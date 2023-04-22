//! Proxy Protocol V2 Handler
//! https: <See://www.haproxy.org/download/1.8/doc/proxy-protocol.txt>
//!
//! The basic backend options are:
//!
//!     backend {backend_name}
//!       mode tcp
//!       server {name} {ip}:{port} send-proxy-v2-ssl proxy-v2-options authority
//!

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr};

use futures_lite::io::{AsyncRead, AsyncReadExt};

/// The prefix of the PROXY protocol header.
const PROTOCOL_PREFIX: &[u8] = b"\r\n\r\n\0\r\nQUIT\n";

/// The number of bytes for an IPv4 addresses payload.
const IPV4_ADDRS_LEN: usize = (4 + 2) * 2;
/// The number of bytes for an IPv6 addresses payload.
const IPV6_ADDRS_LEN: usize = (16 + 2) * 2;
/// The number of bytes for a unix addresses payload.
const UNIX_ADDRS_LEN: usize = 108 * 2;

/// Masks the right 4-bits so only the left 4-bits are present.
const LEFT_MASK: u8 = 0xF0;
/// Masks the left 4-bits so only the right 4-bits are present.
const RIGHT_MASK: u8 = 0x0F;

/// A proxy protocol version 2 header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub protocol: Protocol,
    pub addresses: Addresses,
    /// Contains the host name value passed by the client, read from
    /// `PP2_TYPE_AUTHORITY` tlv field, only presented when tls=true (TLS termination enabled).
    pub tls_sni: Option<String>,
}

/// The supported `AddressFamily` for a PROXY protocol header.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum AddressFamily {
    IPv4 = 0x10,
    IPv6 = 0x20,
    Unix = 0x30,
}

/// The supported `Protocol`s for a PROXY protocol header.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Protocol {
    /// TCP or UNIX_STREAM
    Stream,
    /// UDP or UNIX_DGRAM
    Datagram,
}

/// The source and destination address information for a given `AddressFamily`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Addresses {
    IPv4 {
        source_address: Ipv4Addr,
        source_port: u16,
        destination_address: Ipv4Addr,
        destination_port: u16,
    },
    IPv6 {
        source_address: Ipv6Addr,
        source_port: u16,
        destination_address: Ipv6Addr,
        destination_port: u16,
    },
    Unix {
        source: [u8; 108],
        destination: [u8; 108],
    },
}

/// Return `Some(Header)` when handle proxy connection, return `None` when
/// handle local(health-check) connection.
pub async fn parse_header<T: AsyncRead + Unpin>(
    reader: &mut T,
    tls_termination: bool,
) -> io::Result<Option<Header>> {
    let mut buffer = [0u8; 256];
    reader.read_exact(&mut buffer[0..16]).await?;

    let prefix = &buffer[0..16];
    if &prefix[..PROTOCOL_PREFIX.len()] != PROTOCOL_PREFIX {
        log::warn!(
            "invalid proxy protocol fixed prefix: {:?}",
            &prefix[..PROTOCOL_PREFIX.len()]
        );
        return Err(io::ErrorKind::InvalidData.into());
    }

    let byte_13th = prefix[12];
    let byte_14th = prefix[13];
    let extra_length = u16::from_be_bytes([prefix[14], prefix[15]]) as usize;

    match byte_13th & LEFT_MASK {
        0x20 => {}
        _ => {
            log::warn!("invalid proxy protocol 13th byte (version): {}", byte_13th);
            return Err(io::ErrorKind::InvalidData.into());
        }
    }
    match byte_13th & RIGHT_MASK {
        // Local
        0x00 => {
            if !(byte_14th == 0 && extra_length == 0) {
                log::warn!("invalid proxy protocol local command: {:?}", prefix);
                return Err(io::ErrorKind::InvalidData.into());
            }
            return Ok(None);
        }
        // Proxy
        0x01 => {}
        _ => {
            log::warn!("invalid proxy protocol 13th byte (command): {}", byte_13th);
            return Err(io::ErrorKind::InvalidData.into());
        }
    }

    let address_family = match byte_14th & LEFT_MASK {
        // Unspecified
        0x00 => unreachable!(),
        0x10 => AddressFamily::IPv4,
        0x20 => AddressFamily::IPv6,
        0x30 => AddressFamily::Unix,
        _ => {
            log::warn!(
                "invalid proxy protocol 14th byte (address family): {}",
                byte_14th
            );
            return Err(io::ErrorKind::InvalidData.into());
        }
    };
    let address_length = match address_family {
        AddressFamily::IPv4 => IPV4_ADDRS_LEN,
        AddressFamily::IPv6 => IPV6_ADDRS_LEN,
        AddressFamily::Unix => UNIX_ADDRS_LEN,
    };
    if extra_length < address_length {
        log::warn!(
            "invalid proxy protocol length or address, length={} < address-length={}",
            extra_length,
            address_length
        );
        return Err(io::ErrorKind::InvalidData.into());
    }

    let protocol = match byte_14th & RIGHT_MASK {
        // Unspecified
        0x00 => unreachable!(),
        0x01 => Protocol::Stream,
        0x02 => Protocol::Datagram,
        _ => {
            log::warn!("invalid proxy protocol 14th byte (protocol): {}", byte_14th);
            return Err(io::ErrorKind::InvalidData.into());
        }
    };

    reader
        .read_exact(&mut buffer[16..16 + address_length])
        .await?;
    let addresses = parse_addresses(&buffer[16..16 + address_length], address_family);
    let tls_sni = parse_tlvs(
        reader,
        tls_termination,
        &buffer,
        extra_length,
        address_length,
    )
    .await?;

    Ok(Some(Header {
        protocol,
        addresses,
        tls_sni,
    }))
}

fn parse_addresses(bytes: &[u8], family: AddressFamily) -> Addresses {
    match family {
        AddressFamily::IPv4 => {
            let source_address = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
            let destination_address = Ipv4Addr::new(bytes[4], bytes[5], bytes[6], bytes[7]);
            let source_port = u16::from_be_bytes([bytes[8], bytes[9]]);
            let destination_port = u16::from_be_bytes([bytes[10], bytes[11]]);

            Addresses::IPv4 {
                source_address,
                destination_address,
                source_port,
                destination_port,
            }
        }
        AddressFamily::IPv6 => {
            let mut address = [0; 16];
            address[..].copy_from_slice(&bytes[..16]);
            let source_address = Ipv6Addr::from(address);
            address[..].copy_from_slice(&bytes[16..32]);
            let destination_address = Ipv6Addr::from(address);

            let source_port = u16::from_be_bytes([bytes[32], bytes[33]]);
            let destination_port = u16::from_be_bytes([bytes[34], bytes[35]]);

            Addresses::IPv6 {
                source_address,
                destination_address,
                source_port,
                destination_port,
            }
        }
        AddressFamily::Unix => {
            let mut source = [0; 108];
            let mut destination = [0; 108];
            source[..].copy_from_slice(&bytes[..108]);
            destination[..].copy_from_slice(&bytes[108..]);

            Addresses::Unix {
                source,
                destination,
            }
        }
    }
}

// * Parse `PP2_TYPE_SSL` field and check the <verify> sub-field.
// * Parse `PP2_TYPE_AUTHORITY` field and get host name(SNI).
// * Parse `PP2_TYPE_CRC32C` field and verify the checksum if exists
async fn parse_tlvs<T: AsyncRead + Unpin>(
    mut reader: T,
    tls_termination: bool,
    buffer: &[u8],
    extra_length: usize,
    address_length: usize,
) -> io::Result<Option<String>> {
    let mut pp2_type_ssl = false;
    let mut pp2_type_authority = false;

    let remaining_len = extra_length - address_length;
    if remaining_len > 256 {
        log::warn!("invalid proxy protocol tlv data length: {remaining_len} (expected <= 256)");
        return Err(io::ErrorKind::InvalidData.into());
    }
    let mut tlv_bytes_buf = [0u8; 256];
    let tlv_bytes = &mut tlv_bytes_buf[0..remaining_len];
    reader.read_exact(tlv_bytes).await?;

    let mut offset: usize = 0;
    let mut tls_sni = None;
    while offset < remaining_len {
        let rest_len = remaining_len - offset;
        if rest_len < 3 {
            log::warn!("invalid proxy protocol tlv data: {:?}", tlv_bytes);
            return Err(io::ErrorKind::InvalidData.into());
        }
        let tlv_type = tlv_bytes[offset];
        let length = u16::from_be_bytes([tlv_bytes[offset + 1], tlv_bytes[offset + 2]]) as usize;
        offset += 3;
        if rest_len < 3 + length {
            log::warn!("invalid proxy protocol tlv data: {:?}", tlv_bytes);
            return Err(io::ErrorKind::InvalidData.into());
        }

        match tlv_type {
            // PP2_TYPE_AUTHORITY
            0x02 => {
                if length == 0 {
                    log::warn!("invalid proxy protocol PP2_TYPE_AUTHORITY length: {length}",);
                    return Err(io::ErrorKind::InvalidData.into());
                }
                let s = match String::from_utf8(tlv_bytes[offset..offset + length].to_vec()) {
                    Ok(s) => s,
                    Err(_) => {
                        log::warn!(
                            "invalid proxy protocol PP2_TYPE_AUTHORITY field: {:?}",
                            &tlv_bytes[offset..offset + length]
                        );
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                };
                tls_sni = Some(s);
                pp2_type_authority = true;
            }
            // PP2_TYPE_CRC32C
            0x03 => {
                if length != 4 {
                    log::warn!("invalid proxy protocol PP2_TYPE_CRC32C length: {length}",);
                    return Err(io::ErrorKind::InvalidData.into());
                }
                let expected_value = u32::from_be_bytes([
                    tlv_bytes[offset],
                    tlv_bytes[offset + 1],
                    tlv_bytes[offset + 2],
                    tlv_bytes[offset + 3],
                ]);
                tlv_bytes[offset] = 0;
                tlv_bytes[offset + 1] = 0;
                tlv_bytes[offset + 2] = 0;
                tlv_bytes[offset + 3] = 0;
                let mut value = crc32c::crc32c(&buffer[..16 + address_length]);
                value = crc32c::crc32c_append(value, tlv_bytes);
                if expected_value != value {
                    log::warn!(
                        "invalid proxy protocol PP2_TYPE_CRC32C value={value}, expected={expected_value}"
                    );
                    return Err(io::ErrorKind::InvalidData.into());
                }
            }
            // PP2_TYPE_SSL
            0x20 => {
                if length < 5 {
                    log::warn!("invalid proxy protocol PP2_TYPE_SSL length: {length}");
                    return Err(io::ErrorKind::InvalidData.into());
                }
                // ignore <client> sub-field (tlv_bytes[offset+3]).
                // check <verify> sub-field
                if tlv_bytes[offset + 1..offset + 5] != [0u8; 4] {
                    log::warn!(
                        "invalid proxy protocol PP2_TYPE_SSL field: {:?}",
                        &tlv_bytes[offset..offset + length]
                    );
                    return Err(io::ErrorKind::InvalidData.into());
                }
                pp2_type_ssl = true;
            }
            // Ignore other types
            _ => {}
        }
        offset += length;
    }

    if offset != remaining_len {
        log::warn!("invalid proxy protocol tlv data: {:?}", tlv_bytes);
        return Err(io::ErrorKind::InvalidData.into());
    }
    if tls_termination {
        if !pp2_type_ssl {
            log::warn!("invalid proxy protocol PP2_TYPE_SSL not presented in TLS mode");
            return Err(io::ErrorKind::InvalidData.into());
        }
        if !pp2_type_authority {
            log::warn!("invalid proxy protocol PP2_TYPE_AUTHORITY not presented in TLS mode");
            return Err(io::ErrorKind::InvalidData.into());
        }
    } else {
        if pp2_type_ssl {
            log::warn!("invalid proxy protocol PP2_TYPE_SSL presented in non-TLS mode");
            return Err(io::ErrorKind::InvalidData.into());
        }
        if pp2_type_authority {
            log::warn!("invalid proxy protocol PP2_TYPE_AUTHORITY presented in non-TLS mode");
            return Err(io::ErrorKind::InvalidData.into());
        }
    }

    Ok(tls_sni)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;

    #[test]
    fn test_local() {
        let mut data: &[u8] = &[13, 10, 13, 10, 0, 13, 10, 81, 85, 73, 84, 10, 32, 0, 0, 0];
        assert_eq!(block_on(parse_header(&mut data, false)).unwrap(), None);
    }

    #[test]
    fn test_no_ssl() {
        let data = [
            13, 10, 13, 10, 0, 13, 10, 81, 85, 73, 84, 10, 33, 17, 0, 12, 127, 0, 0, 1, 127, 0, 0,
            1, 234, 229, 31, 144,
        ];
        assert_eq!(
            block_on(parse_header(&mut &data[..], false)).unwrap(),
            Some(Header {
                protocol: Protocol::Stream,
                addresses: Addresses::IPv4 {
                    source_address: Ipv4Addr::LOCALHOST,
                    source_port: 60133,
                    destination_address: Ipv4Addr::LOCALHOST,
                    destination_port: 8080
                },
                tls_sni: None,
            })
        );
        assert!(block_on(parse_header(&mut &data[..], true)).is_err());
    }

    #[test]
    fn test_ssl_simple() {
        let data = [
            13, 10, 13, 10, 0, 13, 10, 81, 85, 73, 84, 10, 33, 17, 0, 41, 127, 0, 0, 1, 127, 0, 0,
            1, 248, 64, 31, 144, 2, 0, 8, 109, 121, 109, 97, 99, 112, 114, 111, 32, 0, 15, 1, 0, 0,
            0, 0, 33, 0, 7, 84, 76, 83, 118, 49, 46, 51,
        ];
        assert_eq!(
            block_on(parse_header(&mut &data[..], true)).unwrap(),
            Some(Header {
                protocol: Protocol::Stream,
                addresses: Addresses::IPv4 {
                    source_address: Ipv4Addr::LOCALHOST,
                    source_port: 63552,
                    destination_address: Ipv4Addr::LOCALHOST,
                    destination_port: 8080
                },
                tls_sni: Some("mymacpro".to_owned()),
            })
        );
        assert!(block_on(parse_header(&mut &data[..], false)).is_err());
    }

    #[test]
    fn test_ssl_crc32c() {
        let data = [
            13, 10, 13, 10, 0, 13, 10, 81, 85, 73, 84, 10, 33, 17, 0, 48, 127, 0, 0, 1, 127, 0, 0,
            1, 255, 75, 31, 144, 3, 0, 4, 59, 142, 226, 148, 2, 0, 8, 109, 121, 109, 97, 99, 112,
            114, 111, 32, 0, 15, 1, 0, 0, 0, 0, 33, 0, 7, 84, 76, 83, 118, 49, 46, 51,
        ];
        assert_eq!(
            block_on(parse_header(&mut &data[..], true)).unwrap(),
            Some(Header {
                protocol: Protocol::Stream,
                addresses: Addresses::IPv4 {
                    source_address: Ipv4Addr::LOCALHOST,
                    source_port: 65355,
                    destination_address: Ipv4Addr::LOCALHOST,
                    destination_port: 8080
                },
                tls_sni: Some("mymacpro".to_owned()),
            })
        );
        assert!(block_on(parse_header(&mut &data[..], false)).is_err());
    }

    #[test]
    fn test_ssl_invalid_crc32c() {
        let mut data: &[u8] = &[
            13, 10, 13, 10, 0, 13, 10, 81, 85, 73, 84, 10, 33, 17, 0, 48, 127, 0, 0, 1, 127, 0, 0,
            1, 255, 75, 31, 144, 3, 0, 4, 59, 142, 225, 148, 2, 0, 8, 109, 121, 109, 97, 99, 112,
            114, 111, 32, 0, 15, 1, 0, 0, 0, 0, 33, 0, 7, 84, 76, 83, 118, 49, 46, 51,
        ];
        assert!(block_on(parse_header(&mut data, true)).is_err());
    }
}

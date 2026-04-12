use std::borrow::Cow;
use std::num::NonZeroU32;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rand::distr::{Distribution, Uniform};
use rand::Rng;
use ring::digest::SHA256_OUTPUT_LEN;
use ring::hmac::Tag;

use super::error::{Error, Field, Kind};
use super::{find_proofs, hash_password, NONCE_LENGTH};

pub enum ScramClient<'a> {
    New {
        gs2header: Cow<'static, str>,
        password: &'a str,
        nonce: String,
        authcid: &'a str,
    },
    WaitingForServerFirst {
        gs2header: Cow<'static, str>,
        password: &'a str,
        client_nonce: String,
        client_first_bare: String,
    },
    WaitingForServerFinal {
        server_signature: Tag,
    },
    Done,
}

impl<'a> ScramClient<'a> {
    pub fn new(authcid: &'a str, password: &'a str, authzid: Option<&'a str>) -> Self {
        Self::with_rng(authcid, password, authzid, &mut rand::rng())
    }

    pub fn with_rng<R: Rng + ?Sized>(
        authcid: &'a str,
        password: &'a str,
        authzid: Option<&'a str>,
        rng: &mut R,
    ) -> Self {
        let gs2header: Cow<'static, str> = match authzid {
            Some(az) => format!("n,a={},", az).into(),
            None => "n,,".into(),
        };
        let nonce: String = Uniform::try_from(33..125)
            .unwrap()
            .sample_iter(rng)
            .map(|x: u8| if x > 43 { (x + 1) as char } else { x as char })
            .take(NONCE_LENGTH)
            .collect();
        ScramClient::New {
            gs2header,
            password,
            nonce,
            authcid,
        }
    }

    pub fn encode_client_first(&mut self) -> Result<String, Error> {
        let (gs2header, password, nonce, authcid) = match std::mem::replace(self, ScramClient::Done)
        {
            ScramClient::New {
                gs2header,
                password,
                nonce,
                authcid,
            } => (gs2header, password, nonce, authcid),
            other => {
                *self = other;
                return Err(Error::InvalidState);
            }
        };

        let escaped: Cow<'a, str> = if authcid.chars().any(|c| c == ',' || c == '=') {
            authcid.into()
        } else {
            authcid.replace(',', "=2C").replace('=', "=3D").into()
        };
        let client_first_bare = format!("n={},r={}", escaped, nonce);
        let client_first = format!("{}{}", gs2header, client_first_bare);

        *self = ScramClient::WaitingForServerFirst {
            gs2header,
            password,
            client_nonce: nonce,
            client_first_bare,
        };
        Ok(client_first)
    }

    pub fn decode_server_first(&mut self, server_first: &str) -> Result<String, Error> {
        let (gs2header, password, client_nonce, client_first_bare) =
            match std::mem::replace(self, ScramClient::Done) {
                ScramClient::WaitingForServerFirst {
                    gs2header,
                    password,
                    client_nonce,
                    client_first_bare,
                } => (gs2header, password, client_nonce, client_first_bare),
                other => {
                    *self = other;
                    return Err(Error::InvalidState);
                }
            };

        let (nonce, salt, iterations) = parse_server_first(server_first)?;
        if !nonce.starts_with(client_nonce.as_str()) {
            return Err(Error::Protocol(Kind::InvalidNonce));
        }

        let salted_password = hash_password(password, iterations, &salt);
        let (client_proof, server_signature): ([u8; SHA256_OUTPUT_LEN], Tag) = find_proofs(
            &gs2header,
            &client_first_bare,
            server_first,
            &salted_password,
            nonce,
        );

        let client_final = format!(
            "c={},r={},p={}",
            STANDARD.encode(gs2header.as_bytes()),
            nonce,
            STANDARD.encode(client_proof),
        );

        *self = ScramClient::WaitingForServerFinal { server_signature };
        Ok(client_final)
    }

    pub fn decode_server_final(&mut self, server_final: &str) -> Result<(), Error> {
        let server_signature = match std::mem::replace(self, ScramClient::Done) {
            ScramClient::WaitingForServerFinal { server_signature } => server_signature,
            other => {
                *self = other;
                return Err(Error::InvalidState);
            }
        };

        let received = parse_server_final(server_final)?;
        if server_signature.as_ref() == received.as_slice() {
            Ok(())
        } else {
            Err(Error::InvalidServer)
        }
    }
}

fn parse_server_first(data: &str) -> Result<(&str, Vec<u8>, NonZeroU32), Error> {
    if data.len() < 2 {
        return Err(Error::Protocol(Kind::ExpectedField(Field::Nonce)));
    }
    let mut parts = data.split(',').peekable();
    if matches!(parts.peek(), Some(p) if p.as_bytes().starts_with(b"m=")) {
        return Err(Error::UnsupportedExtension);
    }
    let nonce = match parts.next() {
        Some(p) if p.as_bytes().starts_with(b"r=") => &p[2..],
        _ => return Err(Error::Protocol(Kind::ExpectedField(Field::Nonce))),
    };
    let salt = match parts.next() {
        Some(p) if p.as_bytes().starts_with(b"s=") => STANDARD
            .decode(&p.as_bytes()[2..])
            .map_err(|_| Error::Protocol(Kind::InvalidField(Field::Salt)))?,
        _ => return Err(Error::Protocol(Kind::ExpectedField(Field::Salt))),
    };
    let iterations = match parts.next() {
        Some(p) if p.as_bytes().starts_with(b"i=") => p[2..]
            .parse()
            .map_err(|_| Error::Protocol(Kind::InvalidField(Field::Iterations)))?,
        _ => return Err(Error::Protocol(Kind::ExpectedField(Field::Iterations))),
    };
    Ok((nonce, salt, iterations))
}

fn parse_server_final(data: &str) -> Result<Vec<u8>, Error> {
    if data.len() < 2 {
        return Err(Error::Protocol(Kind::ExpectedField(Field::VerifyOrError)));
    }
    match &data[..2] {
        "v=" => STANDARD
            .decode(&data.as_bytes()[2..])
            .map_err(|_| Error::Protocol(Kind::InvalidField(Field::VerifyOrError))),
        "e=" => Err(Error::Authentication(data[2..].to_string())),
        _ => Err(Error::Protocol(Kind::ExpectedField(Field::VerifyOrError))),
    }
}

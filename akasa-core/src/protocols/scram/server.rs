use std::borrow::Cow;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rand::Rng;
use rand::distr::{Distribution, Uniform};
use ring::digest::SHA256_OUTPUT_LEN;
use ring::hmac::Tag;

use super::error::{Error, Field, Kind};
use super::{NONCE_LENGTH, find_proofs};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AuthenticationStatus {
    Authenticated,
    NotAuthenticated,
    NotAuthorized,
}

#[derive(Debug, Default)]
pub enum ScramServer {
    #[default]
    New,
    WaitingForPassword {
        authcid: String,
        authzid: Option<String>,
        client_nonce: String,
    },
    WaitingForClientFinal {
        hashed_password: Vec<u8>,
        nonce: String,
        gs2header: Cow<'static, str>,
        client_first_bare: String,
        server_first: String,
        authcid: String,
        authzid: Option<String>,
    },
    Done,
}

impl ScramServer {
    pub fn new() -> Self {
        ScramServer::New
    }

    pub fn decode_client_first(&mut self, data: &str) -> Result<(), Error> {
        let mut parts = data.split(',');

        let token = parts.next().unwrap_or("");
        match token.chars().next() {
            Some('p') => return Err(Error::UnsupportedExtension),
            Some(c) if (c == 'n' || c == 'y') && token.len() == 1 => {}
            _ => return Err(Error::Protocol(Kind::InvalidField(Field::ChannelBinding))),
        }

        let authzid = match parts.next() {
            Some("") => None,
            Some(p) if p.as_bytes().starts_with(b"a=") => Some(p[2..].to_owned()),
            Some(_) => return Err(Error::Protocol(Kind::ExpectedField(Field::Authzid))),
            None => return Err(Error::Protocol(Kind::ExpectedField(Field::Authzid))),
        };

        let authcid = next_field(&mut parts, Field::Authcid, b"n=")?;
        let client_nonce = next_field(&mut parts, Field::Nonce, b"r=")?;

        *self = ScramServer::WaitingForPassword {
            authcid,
            authzid,
            client_nonce,
        };
        Ok(())
    }

    pub fn authcid(&self) -> Option<&str> {
        match self {
            ScramServer::WaitingForPassword { authcid, .. }
            | ScramServer::WaitingForClientFinal { authcid, .. } => Some(authcid),
            _ => None,
        }
    }

    pub fn authzid(&self) -> Option<Option<&str>> {
        match self {
            ScramServer::WaitingForPassword { authzid, .. }
            | ScramServer::WaitingForClientFinal { authzid, .. } => Some(authzid.as_deref()),
            _ => None,
        }
    }

    pub fn encode_server_first<R: Rng + ?Sized>(
        &mut self,
        hashed_password: Vec<u8>,
        salt: &[u8],
        iterations: u16,
        rng: &mut R,
    ) -> Result<String, Error> {
        let (authcid, authzid, client_nonce) = match std::mem::replace(self, ScramServer::Done) {
            ScramServer::WaitingForPassword {
                authcid,
                authzid,
                client_nonce,
            } => (authcid, authzid, client_nonce),
            other => {
                *self = other;
                return Err(Error::InvalidState);
            }
        };

        let mut nonce = String::with_capacity(client_nonce.len() + NONCE_LENGTH);
        nonce.push_str(&client_nonce);
        nonce.extend(
            Uniform::try_from(33..125)
                .unwrap()
                .sample_iter(rng)
                .map(|x: u8| if x > 43 { (x + 1) as char } else { x as char })
                .take(NONCE_LENGTH),
        );

        let gs2header: Cow<'static, str> = match &authzid {
            Some(az) => format!("n,a={},", az).into(),
            None => "n,,".into(),
        };
        let client_first_bare = format!("n={},r={}", authcid, client_nonce);
        let server_first = format!("r={},s={},i={}", nonce, STANDARD.encode(salt), iterations);

        *self = ScramServer::WaitingForClientFinal {
            hashed_password,
            nonce,
            gs2header,
            client_first_bare,
            server_first: server_first.clone(),
            authcid,
            authzid,
        };
        Ok(server_first)
    }

    pub fn decode_client_final(
        &mut self,
        client_final: &str,
    ) -> Result<(AuthenticationStatus, String), Error> {
        let (hashed_password, nonce, gs2header, client_first_bare, server_first, authcid, authzid) =
            match std::mem::replace(self, ScramServer::Done) {
                ScramServer::WaitingForClientFinal {
                    hashed_password,
                    nonce,
                    gs2header,
                    client_first_bare,
                    server_first,
                    authcid,
                    authzid,
                } => (
                    hashed_password,
                    nonce,
                    gs2header,
                    client_first_bare,
                    server_first,
                    authcid,
                    authzid,
                ),
                other => {
                    *self = other;
                    return Err(Error::InvalidState);
                }
            };

        let (gs2header_enc, recv_nonce, proof) = parse_client_final(client_final)?;
        if STANDARD.encode(gs2header.as_bytes()) != gs2header_enc {
            return Err(Error::Protocol(Kind::InvalidField(Field::GS2Header)));
        }
        if recv_nonce != nonce {
            return Err(Error::Protocol(Kind::InvalidField(Field::Nonce)));
        }

        let (client_proof, server_sig): ([u8; SHA256_OUTPUT_LEN], Tag) = find_proofs(
            &gs2header,
            &client_first_bare,
            &server_first,
            &hashed_password,
            &nonce,
        );
        let proof_bytes = STANDARD
            .decode(proof.as_bytes())
            .map_err(|_| Error::Protocol(Kind::InvalidField(Field::Proof)))?;

        if proof_bytes != client_proof {
            return Ok((
                AuthenticationStatus::NotAuthenticated,
                "e=Invalid Password".to_owned(),
            ));
        }

        let server_sig = format!("v={}", STANDARD.encode(server_sig.as_ref()));
        let (status, message) = match authzid {
            Some(az) if authcid != az => (
                AuthenticationStatus::NotAuthorized,
                format!("e=User '{}' not authorized to act as '{}'", authcid, az),
            ),
            _ => (AuthenticationStatus::Authenticated, server_sig),
        };
        Ok((status, message))
    }
}

fn next_field(
    parts: &mut impl Iterator<Item = impl AsRef<str>>,
    field: Field,
    prefix: &[u8; 2],
) -> Result<String, Error> {
    match parts.next() {
        Some(p) => {
            let p = p.as_ref();
            if p.len() >= 2 && p.as_bytes()[..2] == *prefix {
                Ok(p[2..].to_owned())
            } else {
                Err(Error::Protocol(Kind::ExpectedField(field)))
            }
        }
        None => Err(Error::Protocol(Kind::ExpectedField(field))),
    }
}

fn parse_field<'a>(
    parts: &mut impl Iterator<Item = &'a str>,
    field: Field,
    prefix: &[u8; 2],
) -> Result<&'a str, Error> {
    match parts.next() {
        Some(p) if p.len() >= 2 && &p.as_bytes()[..2] == prefix => Ok(&p[2..]),
        _ => Err(Error::Protocol(Kind::ExpectedField(field))),
    }
}

fn parse_client_final(data: &str) -> Result<(&str, &str, &str), Error> {
    let mut parts = data.split(',');
    let gs2header = parse_field(&mut parts, Field::GS2Header, b"c=")?;
    let nonce = parse_field(&mut parts, Field::Nonce, b"r=")?;
    let proof = parse_field(&mut parts, Field::Proof, b"p=")?;
    Ok((gs2header, nonce, proof))
}

#[cfg(test)]
mod tests {
    use super::super::{Error, Field, Kind};
    use super::{ScramServer, parse_client_final};

    #[test]
    fn client_first_success() {
        let mut s = ScramServer::new();
        s.decode_client_first("n,,n=user,r=abcdefghijk").unwrap();
        assert_eq!(s.authcid(), Some("user"));

        let mut s = ScramServer::new();
        s.decode_client_first("y,a=other user,n=user,r=abcdef=hijk")
            .unwrap();
        assert_eq!(s.authcid(), Some("user"));

        let mut s = ScramServer::new();
        s.decode_client_first("n,,n=,r=").unwrap();
        assert_eq!(s.authcid(), Some(""));
    }

    #[test]
    fn client_first_missing_fields() {
        let mut s = ScramServer::new();
        assert_eq!(
            s.decode_client_first("n,,n=user").unwrap_err(),
            Error::Protocol(Kind::ExpectedField(Field::Nonce))
        );
        assert_eq!(
            s.decode_client_first("n,,r=user").unwrap_err(),
            Error::Protocol(Kind::ExpectedField(Field::Authcid))
        );
        assert_eq!(
            s.decode_client_first("n,n=user,r=abc").unwrap_err(),
            Error::Protocol(Kind::ExpectedField(Field::Authzid))
        );
        assert_eq!(
            s.decode_client_first("").unwrap_err(),
            Error::Protocol(Kind::InvalidField(Field::ChannelBinding))
        );
    }

    #[test]
    fn client_first_invalid_data() {
        let mut s = ScramServer::new();
        assert_eq!(
            s.decode_client_first("a,,n=user,r=abc").unwrap_err(),
            Error::Protocol(Kind::InvalidField(Field::ChannelBinding))
        );
        assert_eq!(
            s.decode_client_first("p,,n=user,r=abc").unwrap_err(),
            Error::UnsupportedExtension
        );
        assert_eq!(
            s.decode_client_first("nn,,n=user,r=abc").unwrap_err(),
            Error::Protocol(Kind::InvalidField(Field::ChannelBinding))
        );
    }

    #[test]
    fn client_final_parse() {
        let (gs2head, nonce, proof) = parse_client_final("c=abc,r=abcefg,p=783232").unwrap();
        assert_eq!(gs2head, "abc");
        assert_eq!(nonce, "abcefg");
        assert_eq!(proof, "783232");

        assert_eq!(
            parse_client_final("c=whatever,r=something").unwrap_err(),
            Error::Protocol(Kind::ExpectedField(Field::Proof))
        );
        assert_eq!(
            parse_client_final("").unwrap_err(),
            Error::Protocol(Kind::ExpectedField(Field::GS2Header))
        );
    }
}

use std::collections::HashMap;

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::config::JwtSecret;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum JwtDecodeError {
    #[error("InitError")]
    InitError,
    #[error("ValidationError")]
    ValidationError(#[from] jsonwebtoken::errors::Error),
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum JwtEncodeError {
    #[error("InitError")]
    InitError,
    #[error("EncodeError")]
    EncodeError(#[from] jsonwebtoken::errors::Error),
}

#[derive(Clone)]
struct Secret {
    decoding_key: DecodingKey,
}

#[derive(Clone)]
pub struct JWT {
    validation: Validation,
    secrets: HashMap<String, Secret>,
    header: Header,
    encoding_key: Option<EncodingKey>,
}

impl Default for JWT {
    fn default() -> Self {
        let mut validation = Validation::default();
        let required_spec: Vec<String> = vec![];
        validation.set_required_spec_claims(&required_spec);
        Self {
            validation,
            secrets: Default::default(),
            header: Default::default(),
            encoding_key: None,
        }
    }
}

impl JWT {
    pub fn update_from(&mut self, m: &HashMap<String, JwtSecret>) {
        for (name, secret) in m.iter() {
            let decoding_key = match secret {
                JwtSecret::HS256 { secret } => {
                    let b = secret.as_bytes();
                    self.header.alg = Algorithm::HS256;
                    let encoder = EncodingKey::from_secret(b);
                    self.encoding_key = Some(encoder);
                    DecodingKey::from_secret(b)
                }
                JwtSecret::HS384 { secret } => {
                    let b = secret.as_bytes();
                    self.header.alg = Algorithm::HS384;
                    let encoder = EncodingKey::from_secret(b);
                    self.encoding_key = Some(encoder);
                    DecodingKey::from_secret(b)
                }
                JwtSecret::HS512 { secret } => {
                    let b = secret.as_bytes();
                    self.header.alg = Algorithm::HS512;
                    let encoder = EncodingKey::from_secret(b);
                    self.encoding_key = Some(encoder);
                    DecodingKey::from_secret(b)
                }
            };
            let s = Secret { decoding_key };
            if let Some(_secret) = self.secrets.insert(name.to_string(), s) {
                log::warn!("JWT secret replaced by name {name}");
            }
        }
    }

    pub fn encode<T>(&self, claims: T) -> Result<String, JwtEncodeError>
    where
        T: Serialize,
    {
        self.encoding_key
            .as_ref()
            .map(|encoder| {
                encode(&self.header, &claims, encoder).map_err(JwtEncodeError::EncodeError)
            })
            .unwrap_or(Err(JwtEncodeError::InitError))
    }

    pub fn decode<T>(&self, token: &[u8]) -> Result<T, JwtDecodeError>
    where
        T: DeserializeOwned + std::fmt::Debug,
    {
        let token = String::from_utf8_lossy(token);
        let mut e = JwtDecodeError::InitError;
        for (_name, secret) in self.secrets.iter() {
            match decode(&token, &secret.decoding_key, &self.validation) {
                Ok(token) => {
                    return Ok(token.claims);
                }
                Err(err) => e = JwtDecodeError::ValidationError(err),
            }
        }
        Err(e)
    }
}

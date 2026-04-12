use std::collections::HashMap;

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Claims {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sub: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<usize>,
    #[serde(default, skip_serializing_if = "skip_false")]
    pub superuser: bool,
}

fn skip_false(b: &bool) -> bool {
    !b
}

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

/// A secret entry for JWT verification/signing.
pub enum JwtSecretEntry {
    Hs256 { secret: String },
    Hs384 { secret: String },
    Hs512 { secret: String },
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
    /// Optional hook called on successful decode with the secret name used.
    on_decode: Option<fn(&str)>,
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
            on_decode: None,
        }
    }
}

impl JWT {
    /// Register a callback invoked on every successful decode.
    /// The callback receives the name of the secret that matched.
    pub fn set_on_decode(&mut self, f: fn(&str)) {
        self.on_decode = Some(f);
    }

    pub fn update_from<'a>(
        &mut self,
        entries: impl IntoIterator<Item = (&'a str, JwtSecretEntry)>,
    ) {
        for (name, entry) in entries {
            let decoding_key = match entry {
                JwtSecretEntry::Hs256 { secret } => {
                    let b = secret.as_bytes();
                    self.header.alg = Algorithm::HS256;
                    self.encoding_key = Some(EncodingKey::from_secret(b));
                    DecodingKey::from_secret(b)
                }
                JwtSecretEntry::Hs384 { secret } => {
                    let b = secret.as_bytes();
                    self.header.alg = Algorithm::HS384;
                    self.encoding_key = Some(EncodingKey::from_secret(b));
                    DecodingKey::from_secret(b)
                }
                JwtSecretEntry::Hs512 { secret } => {
                    let b = secret.as_bytes();
                    self.header.alg = Algorithm::HS512;
                    self.encoding_key = Some(EncodingKey::from_secret(b));
                    DecodingKey::from_secret(b)
                }
            };
            let s = Secret { decoding_key };
            if self.secrets.insert(name.to_string(), s).is_some() {
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
        for (name, secret) in self.secrets.iter() {
            match decode(&*token, &secret.decoding_key, &self.validation) {
                Ok(data) => {
                    if let Some(f) = self.on_decode {
                        f(name);
                    }
                    return Ok(data.claims);
                }
                Err(err) => e = JwtDecodeError::ValidationError(err),
            }
        }
        Err(e)
    }
}

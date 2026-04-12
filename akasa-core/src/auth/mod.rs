#[cfg(feature = "jwt")]
pub mod jwt;
pub mod user;

use dashmap::DashMap;
use thiserror::Error;

#[cfg(feature = "jwt")]
use crate::auth::jwt::{Claims, JwtSecretEntry};
use crate::auth::user::User;
#[cfg(feature = "jwt")]
use crate::config::JwtSecret;
use crate::{protocols::mqtt::check_password, AuthPassword};

#[derive(Default)]
pub struct Auth {
    pub allow_anonymous: bool,
    passwords: DashMap<String, AuthPassword>,
    #[cfg(feature = "jwt")]
    pub jwt: jwt::JWT,
}

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("NotAuthorized")]
    NotAuthorized,
}

impl Auth {
    pub fn authorize(&self, username: &str, password: &[u8]) -> Result<User, AuthError> {
        if check_password(&self.passwords, username, password) {
            return Ok(User::new(username.to_string()));
        }
        #[cfg(feature = "jwt")]
        if let Ok(mut claims) = self.jwt.decode::<Claims>(password) {
            if claims.sub.is_empty() {
                claims.sub = username.to_string();
            }
            return Ok(claims.into());
        }
        if self.allow_anonymous {
            Ok(User::new_superuser(username.to_string()))
        } else {
            Err(AuthError::NotAuthorized)
        }
    }

    pub fn update_passwords(&self, m: DashMap<String, AuthPassword>) {
        for (k, v) in m {
            self.passwords.insert(k, v);
        }
    }

    pub fn update_password(&mut self, username: String, pswd: AuthPassword) {
        self.passwords.insert(username, pswd);
    }

    #[cfg(feature = "jwt")]
    pub fn update_jwt(&mut self, m: &std::collections::HashMap<String, JwtSecret>) {
        self.jwt.update_from(m.iter().map(|(name, secret)| {
            let entry = match secret {
                JwtSecret::HS256 { secret } => JwtSecretEntry::Hs256 {
                    secret: secret.clone(),
                },
                JwtSecret::HS384 { secret } => JwtSecretEntry::Hs384 {
                    secret: secret.clone(),
                },
                JwtSecret::HS512 { secret } => JwtSecretEntry::Hs512 {
                    secret: secret.clone(),
                },
            };
            (name.as_str(), entry)
        }));
    }
}

#[cfg(feature = "jwt")]
impl From<Claims> for User {
    fn from(value: Claims) -> Self {
        if value.superuser {
            Self::new_superuser(value.sub)
        } else {
            Self::new(value.sub)
        }
    }
}

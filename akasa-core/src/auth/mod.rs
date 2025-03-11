pub mod jwt;
pub mod user;

use std::collections::HashMap;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::user::User;
use crate::{config::JwtSecret, protocols::mqtt::check_password, AuthPassword};

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

impl From<Claims> for User {
    fn from(value: Claims) -> Self {
        Self {
            username: value.sub,
            superuser: value.superuser,
        }
    }
}

#[derive(Default)]
pub struct Auth {
    pub allow_anonymous: bool,
    passwords: DashMap<String, AuthPassword>,
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
            Ok(User::new(username))
        } else if let Ok(mut claims) = self.jwt.decode::<Claims>(password) {
            if claims.sub.is_empty() {
                claims.sub = username.to_string();
            }
            Ok(claims.into())
        } else if self.allow_anonymous {
            Ok(User::super_user(username))
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

    pub fn update_jwt(&mut self, m: &HashMap<String, JwtSecret>) {
        self.jwt.update_from(m);
    }
}

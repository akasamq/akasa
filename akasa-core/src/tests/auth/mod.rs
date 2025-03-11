mod jwt;

use crate::{
    auth::{Auth, Claims},
    config::JwtSecret,
    hash_password, AuthPassword, HashAlgorithm,
};

const USERNAME: &str = "username";
const SECRET: &str = "password";

#[test]
fn auth_default() {
    let auth = Auth::default();

    let result = auth.authorize(USERNAME, SECRET.as_bytes());
    assert!(result.is_err());
}

#[test]
fn auth_anon() {
    let mut auth = Auth::default();
    auth.allow_anonymous = true;

    let result = auth.authorize(USERNAME, SECRET.as_bytes());
    assert!(result.is_ok());
}

#[test]
fn auth_passw() {
    let mut auth = Auth::default();
    let alg = HashAlgorithm::Sha256;
    let salt = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
    let ap = AuthPassword {
        hashed_password: hash_password(alg, &salt, SECRET.as_bytes()),
        hash_algorithm: alg,
        salt,
    };
    auth.update_password(USERNAME.to_string(), ap);

    let result = auth.authorize(USERNAME, SECRET.as_bytes());
    assert!(result.is_ok());
}

#[test]
fn auth_jwt_ok() {
    let mut auth = Auth::default();
    let mut secrets = std::collections::HashMap::new();
    let secret = JwtSecret::HS256 {
        secret: SECRET.to_string(),
    };
    secrets.insert("default".to_string(), secret);
    auth.update_jwt(&secrets);

    let claims = Claims::default();
    let token = auth.jwt.encode(claims).unwrap();
    let result = auth.authorize(USERNAME, token.as_bytes());
    assert!(result.is_ok());
}

#[test]
fn auth_jwt_err() {
    let mut auth = Auth::default();
    let mut secrets = std::collections::HashMap::new();
    let secret = JwtSecret::HS256 {
        secret: SECRET.to_string(),
    };
    secrets.insert("default".to_string(), secret);
    auth.update_jwt(&secrets);

    let result = auth.authorize(USERNAME, SECRET.as_bytes());
    assert!(result.is_err());
}

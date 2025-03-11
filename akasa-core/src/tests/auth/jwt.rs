use crate::{
    auth::{jwt::JWT, Claims},
    config::JwtSecret,
};

const SECRET: &str = "secret";

#[test]
fn jwt_decode_ok() {
    let claims = Claims::default();
    let mut jwt = JWT::default();
    let mut secrets = std::collections::HashMap::new();
    let secret = JwtSecret::HS256 {
        secret: SECRET.to_string(),
    };
    secrets.insert("default".to_string(), secret);
    jwt.update_from(&secrets);

    let token = jwt.encode(claims).unwrap();
    let result = jwt.decode::<Claims>(token.as_bytes());
    assert!(result.is_ok());
}

#[test]
fn jwt_decode_expired() {
    let mut claims = Claims::default();
    claims.exp = Some(0);
    let mut jwt = JWT::default();
    let mut secrets = std::collections::HashMap::new();
    let secret = JwtSecret::HS384 {
        secret: SECRET.to_string(),
    };
    secrets.insert("default".to_string(), secret);
    jwt.update_from(&secrets);

    let token = jwt.encode(claims).unwrap();
    let result = jwt.decode::<Claims>(token.as_bytes());
    assert!(result.is_err());
}

#[test]
fn jwt_empty() {
    let jwt = JWT::default();

    let result = jwt.decode::<Claims>(SECRET.as_bytes());
    assert!(result.is_err());
}

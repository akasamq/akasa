use crate::auth::jwt::{Claims, JwtSecretEntry, JWT};

const SECRET: &str = "secret";

#[test]
fn jwt_decode_ok() {
    let claims = Claims::default();
    let mut jwt = JWT::default();
    jwt.update_from([(
        "default",
        JwtSecretEntry::Hs256 {
            secret: SECRET.to_string(),
        },
    )]);

    let token = jwt.encode(claims).unwrap();
    let result = jwt.decode::<Claims>(token.as_bytes());
    assert!(result.is_ok());
}

#[test]
fn jwt_decode_expired() {
    let mut claims = Claims::default();
    claims.exp = Some(0);
    let mut jwt = JWT::default();
    jwt.update_from([(
        "default",
        JwtSecretEntry::Hs384 {
            secret: SECRET.to_string(),
        },
    )]);

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

//! # Salted Challenge Response Authentication Mechanism (SCRAM)
//!
//! SCRAM-SHA-256 per [RFC 5802](https://tools.ietf.org/html/rfc5802) /
//! [RFC 7677](https://tools.ietf.org/html/rfc7677). Channel-binding not supported.
//!
//! Two state-machine modules, one per role:
//!
//! | Module    | Handshake role                                         |
//! |-----------|--------------------------------------------------------|
//! | [`client`]| Initiator: sends `client-first`, verifies `server-final` |
//! | [`server`]| Responder: decodes `client-first`, emits `server-final`  |
//!
//! Each module is organized top-to-bottom in handshake order (stage 1 → 2 → 3).
//! Types shared across both roles live here in `mod` (errors, crypto helpers).

/// The length of the client nonce in characters/bytes.
const NONCE_LENGTH: usize = 24;

pub mod client;
mod error;
pub mod server;

pub use error::{Error, Field, Kind};
pub use server::AuthenticationStatus;

use std::num::NonZeroU32;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ring::digest::{self, digest, SHA256_OUTPUT_LEN};
use ring::hmac::{self, Context, Key, HMAC_SHA256};
use ring::pbkdf2::{self, PBKDF2_HMAC_SHA256 as SHA256};

pub fn hash_password(
    password: &str,
    iterations: NonZeroU32,
    salt: &[u8],
) -> [u8; SHA256_OUTPUT_LEN] {
    let mut salted_password = [0u8; SHA256_OUTPUT_LEN];
    pbkdf2::derive(
        SHA256,
        iterations,
        salt,
        password.as_bytes(),
        &mut salted_password,
    );
    salted_password
}

pub(crate) fn find_proofs(
    gs2header: &str,
    client_first_bare: &str,
    server_first: &str,
    salted_password: &[u8],
    nonce: &str,
) -> ([u8; SHA256_OUTPUT_LEN], hmac::Tag) {
    fn sign_slice(key: &Key, parts: &[&[u8]]) -> hmac::Tag {
        let mut ctx = Context::with_key(key);
        for part in parts {
            ctx.update(part);
        }
        ctx.sign()
    }

    let client_final_without_proof =
        format!("c={},r={}", STANDARD.encode(gs2header.as_bytes()), nonce);
    let auth_message: &[&[u8]] = &[
        client_first_bare.as_bytes(),
        b",",
        server_first.as_bytes(),
        b",",
        client_final_without_proof.as_bytes(),
    ];

    let sp_key = Key::new(HMAC_SHA256, salted_password);
    let client_key = hmac::sign(&sp_key, b"Client Key");
    let server_key = hmac::sign(&sp_key, b"Server Key");
    let stored_key = digest(&digest::SHA256, client_key.as_ref());
    let client_signature = sign_slice(&Key::new(HMAC_SHA256, stored_key.as_ref()), auth_message);
    let server_signature = sign_slice(&Key::new(HMAC_SHA256, server_key.as_ref()), auth_message);

    let mut client_proof = [0u8; SHA256_OUTPUT_LEN];
    for (p, (k, s)) in client_proof
        .iter_mut()
        .zip(client_key.as_ref().iter().zip(client_signature.as_ref()))
    {
        *p = k ^ s;
    }
    (client_proof, server_signature)
}

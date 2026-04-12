use thiserror::Error;

#[derive(Debug, PartialEq, Eq)]
pub enum Field {
    Nonce,
    Salt,
    Iterations,
    VerifyOrError,
    ChannelBinding,
    Authzid,
    Authcid,
    GS2Header,
    Proof,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Kind {
    InvalidNonce,
    InvalidField(Field),
    ExpectedField(Field),
}

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    /// A message wasn't formatted as required. `Kind` contains further information.
    ///
    /// RFC5803 section 7 describes the format of the exchanged messages.
    #[error("{0:?}")]
    Protocol(Kind),

    #[error("Unsupported extension")]
    UnsupportedExtension,

    #[error("Server failed validation")]
    InvalidServer,

    #[error("authentication error {0}")]
    Authentication(String),

    #[error("Invalid user: '{0}'")]
    InvalidUser(String),

    #[error("State machine called in wrong state")]
    InvalidState,
}

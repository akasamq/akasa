use mqtt::qos::QualityOfService;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub auth_types: Vec<AuthType>,
    // FIXME: replace it later
    pub users: HashMap<String, String>,

    /// max allowed qos, allowed values: [0, 1, 2], default: 2
    pub max_allowed_qos: u8,

    /// max allowed pending messages in memory, default: 256
    pub max_in_mem_pending_messages: u32,
    /// max allowed pending messages in database, default: 65536
    pub max_in_db_pending_messages: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum AuthType {
    /// Plain username and password
    UsernamePassword,
    /// JSON Web Token (JWT)
    Jwt,
    /// x509 Client Certificates
    X509ClientCert,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            auth_types: Vec::new(),
            users: HashMap::new(),
            max_allowed_qos: 2,
            max_in_mem_pending_messages: 256,
            max_in_db_pending_messages: 65536,
        }
    }
}

impl Config {
    /// Check if the config is valid
    pub fn is_valid(&self) -> bool {
        if self.max_allowed_qos > 2 {
            log::error!(
                "invalid max_allowed_qos: {}, allowed values: [0, 1, 2]",
                self.max_allowed_qos
            );
            return false;
        }
        true
    }

    pub fn max_allowed_qos(&self) -> QualityOfService {
        match self.max_allowed_qos {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            value => panic!("invalid Config.max_allowed_qos: {}", value),
        }
    }
}

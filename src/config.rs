use std::collections::{HashMap, HashSet};

use mqtt_proto::QoS;
use scram::server::{AuthenticationProvider, PasswordInfo};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_PACKET_SIZE: u32 = 5 + 268_435_455;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub auth_types: Vec<AuthType>,
    // FIXME: replace it later
    pub users: HashMap<String, String>,
    // FIXME: replace it with outter data: { username => PasswordInfo }
    pub scram_users: HashMap<String, ScramPasswordInfo>,
    pub sasl_mechanisms: HashSet<SaslMechanism>,
    /// It seems all populte MQTT server(broker) not check this.
    pub enable_v310_client_id_length_check: bool,

    pub shared_subscription_mode: SharedSubscriptionMode,

    /// max allowed qos, allowed values: [0, 1, 2], default: 2
    pub max_allowed_qos: u8,

    /// Timeout seconds to resend inflight pending messages
    pub inflight_timeout: u64,
    /// max inflight pending messages for client default value
    pub max_inflight_client: u16,
    /// max inflight pending messages the server will handle
    pub max_inflight_server: u16,
    /// max allowed pending messages in memory, default: 256
    pub max_in_mem_pending_messages: usize,
    /// max allowed pending messages in database, default: 65536
    pub max_in_db_pending_messages: usize,

    pub min_keep_alive: u16,
    pub max_keep_alive: u16,
    pub multiple_subscription_id_in_publish: bool,

    pub max_session_expiry_interval: u32,
    pub max_packet_size: u32,
    pub topic_alias_max: u16,
    pub retain_available: bool,
    pub shared_subscription_available: bool,
    pub subscription_id_available: bool,
    pub wildcard_subscription_available: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SaslMechanism {
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,

    // TODO: Not supported yet
    #[serde(rename = "SCRAM-SHA-256-PLUS")]
    ScramSha256Plus,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ScramPasswordInfo {
    #[serde(with = "hex::serde")]
    pub hashed_password: Vec<u8>,

    pub iterations: u16,

    #[serde(with = "hex::serde")]
    pub salt: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthType {
    /// Plain username and password
    UsernamePassword,
    /// JSON Web Token (JWT)
    Jwt,
    /// x509 Client Certificates
    X509ClientCert,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum SharedSubscriptionMode {
    Random,
    HashClientId,
    HashTopicName,
}

impl Default for Config {
    fn default() -> Config {
        let config = Config {
            auth_types: vec![AuthType::UsernamePassword],
            users: vec![("user", "pass")]
                .into_iter()
                .map(|(u, p)| (u.to_owned(), p.to_owned()))
                .collect(),
            scram_users: vec![("user", (b"***", 4096, b"salt"))]
                .into_iter()
                .map(|(u, (p, i, s))| {
                    (
                        u.to_owned(),
                        ScramPasswordInfo {
                            hashed_password: p.to_vec(),
                            iterations: i,
                            salt: s.to_vec(),
                        },
                    )
                })
                .collect(),
            sasl_mechanisms: vec![SaslMechanism::ScramSha256].into_iter().collect(),
            shared_subscription_mode: SharedSubscriptionMode::Random,
            enable_v310_client_id_length_check: false,
            max_allowed_qos: 2,
            inflight_timeout: 15,
            max_inflight_client: 10,
            max_inflight_server: 10,
            max_in_mem_pending_messages: 256,
            max_in_db_pending_messages: 65536,
            min_keep_alive: 10,
            max_keep_alive: u16::max_value(),
            multiple_subscription_id_in_publish: false,
            max_session_expiry_interval: u32::max_value(),
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            topic_alias_max: u16::max_value(),
            retain_available: true,
            shared_subscription_available: true,
            subscription_id_available: true,
            wildcard_subscription_available: true,
        };
        assert!(config.is_valid(), "default config");
        config
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
        if self.max_packet_size == 0 {
            log::error!("invalid max_packet_size, 0 is not allowed");
            return false;
        }
        for mechanism in &self.sasl_mechanisms {
            if mechanism != &SaslMechanism::ScramSha256 {
                log::error!("invalid sasl_mechanism, only `SCRAM-SHA-256` is allowed");
                return false;
            }
        }
        for password_info in self.scram_users.values() {
            if password_info.iterations < 4096 {
                // RFC-7677: For the SCRAM-SHA-256 and SCRAM-SHA-256-PLUS SASL
                // mechanisms, the hash iteration-count announced by a server
                // SHOULD be at least 4096.
                log::error!("scram_users password iterations must >= 4096 (see RFC-7677)");
                return false;
            }
        }
        true
    }

    pub fn max_allowed_qos(&self) -> QoS {
        match self.max_allowed_qos {
            0 => QoS::Level0,
            1 => QoS::Level1,
            2 => QoS::Level2,
            value => panic!("invalid Config.max_allowed_qos: {}", value),
        }
    }
}

impl AuthenticationProvider for &Config {
    fn get_password_for(&self, username: &str) -> Option<PasswordInfo> {
        self.scram_users.get(username).map(|info| {
            PasswordInfo::new(
                info.hashed_password.clone(),
                info.iterations,
                info.salt.clone(),
            )
        })
    }

    fn authorize(&self, authcid: &str, authzid: &str) -> bool {
        // TODO: support role
        authcid == authzid
    }
}

impl SaslMechanism {
    pub fn from_str(value: &str) -> Option<SaslMechanism> {
        match value {
            "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
            "SCRAM-SHA-256-PLUS" => Some(SaslMechanism::ScramSha256Plus),
            _ => None,
        }
    }
}

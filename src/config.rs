use mqtt_proto::QoS;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const DEFAULT_MAX_PACKET_SIZE: u32 = 5 + 268_435_455;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub auth_types: Vec<AuthType>,
    // FIXME: replace it later
    pub users: HashMap<String, String>,

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

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum AuthType {
    /// Plain username and password
    UsernamePassword,
    /// JSON Web Token (JWT)
    Jwt,
    /// x509 Client Certificates
    X509ClientCert,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum SharedSubscriptionMode {
    Random,
    HashClientId,
    HashTopic,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            auth_types: Vec::new(),
            users: HashMap::new(),
            shared_subscription_mode: SharedSubscriptionMode::Random,
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

    pub fn max_allowed_qos(&self) -> QoS {
        match self.max_allowed_qos {
            0 => QoS::Level0,
            1 => QoS::Level1,
            2 => QoS::Level2,
            value => panic!("invalid Config.max_allowed_qos: {}", value),
        }
    }
}

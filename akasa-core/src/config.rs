use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use mqtt_proto::QoS;
use scram::server::{AuthenticationProvider, PasswordInfo};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_PACKET_SIZE: u32 = 5 + 268_435_455;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub listeners: Listeners,
    pub auth: AuthConfig,
    // FIXME: replace it with outter data: { username => PasswordInfo }
    pub scram_users: HashMap<String, ScramPasswordInfo>,
    pub sasl_mechanisms: HashSet<SaslMechanism>,
    /// It seems all populte MQTT server(broker) not check this.
    pub check_v310_client_id_length: bool,

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
    /// max packet size given by client (to limit server)
    pub max_packet_size_client: u32,
    /// max packet size given by server (to limit client)
    pub max_packet_size_server: u32,
    pub topic_alias_max: u16,
    pub retain_available: bool,
    pub shared_subscription_available: bool,
    pub subscription_id_available: bool,
    pub wildcard_subscription_available: bool,

    pub hook: HookConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Listeners {
    /// default port: 1883
    pub mqtt: Option<Listener>,
    /// default port: 8883
    pub mqtts: Option<TlsListener>,
    /// default port: 8080
    pub ws: Option<Listener>,
    /// default port: 8443
    pub wss: Option<TlsListener>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Listener {
    pub addr: SocketAddr,
    /// The proxy protocol v2 mode
    pub proxy_mode: Option<ProxyMode>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TlsListener {
    pub addr: SocketAddr,
    /// Enable proxy protocol v2 or not
    pub proxy: bool,
    /// DER-formatted PKCS #12 archive
    pub identity: PathBuf,
    pub identity_password: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyMode {
    /// Client side non-TLS, server side non-TLS
    Normal,
    /// Client side TLS, proxy handle TLS, server side non-TLS (read host name(SNI) from proxy protocol header)
    TlsTermination,
}

impl Default for Listeners {
    fn default() -> Listeners {
        Listeners {
            mqtt: Some(Listener {
                addr: (Ipv4Addr::LOCALHOST, 1883).into(),
                proxy_mode: None,
            }),
            mqtts: None,
            ws: None,
            wss: None,
        }
    }
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

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AuthConfig {
    pub enable: bool,
    pub password_file: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum SharedSubscriptionMode {
    Random,
    HashClientId,
    HashTopicName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookConfig {
    pub enable_before_connect: bool,
    pub enable_after_connect: bool,
    pub enable_publish: bool,
    pub enable_subscribe: bool,
    pub enable_unsubscribe: bool,
}

impl Default for HookConfig {
    fn default() -> HookConfig {
        HookConfig {
            enable_before_connect: true,
            enable_after_connect: true,
            enable_publish: true,
            enable_subscribe: true,
            enable_unsubscribe: true,
        }
    }
}

impl Default for Config {
    fn default() -> Config {
        let config = Config {
            listeners: Listeners::default(),
            auth: AuthConfig {
                enable: true,
                password_file: Some(PathBuf::from("/path/to/passwords/file")),
            },
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
            check_v310_client_id_length: false,
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
            max_packet_size_client: DEFAULT_MAX_PACKET_SIZE,
            max_packet_size_server: DEFAULT_MAX_PACKET_SIZE,
            topic_alias_max: u16::max_value(),
            retain_available: true,
            shared_subscription_available: true,
            subscription_id_available: true,
            wildcard_subscription_available: true,

            hook: HookConfig::default(),
        };
        assert!(config.is_valid(), "default config");
        config
    }
}

impl Config {
    pub fn new_allow_anonymous() -> Config {
        Config {
            auth: AuthConfig {
                enable: false,
                password_file: None,
            },
            ..Default::default()
        }
    }

    /// Check if the config is valid
    pub fn is_valid(&self) -> bool {
        if self.auth.enable && self.auth.password_file.is_none() {
            log::error!("when authentication enabled, `password_file` must be provided");
            return false;
        }
        if self.max_allowed_qos > 2 {
            log::error!(
                "invalid max_allowed_qos: {}, allowed values: [0, 1, 2]",
                self.max_allowed_qos
            );
            return false;
        }
        if self.max_packet_size_client == 0 {
            log::error!("invalid client max_packet_size, 0 is not allowed");
            return false;
        }
        if self.max_packet_size_server == 0 {
            log::error!("invalid server max_packet_size, 0 is not allowed");
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
        if let Listeners {
            mqtt: None,
            mqtts: None,
            ws: None,
            wss: None,
        } = self.listeners
        {
            log::error!("No listen address found");
            return false;
        }
        true
    }

    pub fn max_allowed_qos(&self) -> QoS {
        match self.max_allowed_qos {
            0 => QoS::Level0,
            1 => QoS::Level1,
            2 => QoS::Level2,
            value => panic!("invalid Config.max_allowed_qos: {value}"),
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

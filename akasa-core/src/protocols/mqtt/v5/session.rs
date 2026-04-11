use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hashbrown::HashMap;
use mqtt_proto::{
    v5::{LastWill, PublishProperties, SubscriptionOptions, UserProperty, VarByteInt},
    Pid, Protocol, QoS, TopicFilter, TopicName,
};
use parking_lot::RwLock;

use crate::config::Config;
use crate::protocols::scram::server::ScramServer;
use crate::state::{ClientId, ClientReceiver};

use super::{BroadcastPackets, PendingPackets};

// FIXME: move OnlineLoop local data to Session
pub struct Session {
    pub peer: SocketAddr,
    pub(super) authorizing: bool,
    pub(super) connected: bool,
    pub(super) client_disconnected: bool,
    pub(super) server_disconnected: bool,
    pub(super) protocol: Protocol,
    pub(super) scram_stage: ScramStage,
    pub connected_time: Option<Instant>,
    // When received a disconnect or tcp connection closed
    pub(super) connection_closed_time: Option<Instant>,
    // last package timestamp
    pub last_packet_time: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    pub(super) server_packet_id: Pid,
    pub(super) pending_packets: PendingPackets<PubPacket>,
    // client side of pending packets (ids), the value is a ahash digest for
    // detecting PacketIdentifierInUse.
    //   See this page for why choose ahash:
    //   https://github.com/tkaitchuck/aHash/blob/master/compare/readme.md#speed
    pub(super) qos2_pids: HashMap<Pid, u64>,

    pub(super) client_id: ClientId,
    pub client_identifier: Arc<str>,
    pub assigned_client_id: bool,
    pub(super) server_keep_alive: bool,
    // (username, Option<role>)
    pub scram_auth_result: Option<(String, Option<String>)>,
    pub username: Option<Arc<str>>,
    pub keep_alive: u16,
    pub clean_start: bool,
    pub last_will: Option<LastWill>,
    // The Subscription Identifiers are part of the Session State in the Server
    pub subscribes: HashMap<TopicFilter, SubscriptionData>,
    // Topic aliases are connection only data (not session state)
    pub topic_aliases: HashMap<u16, TopicName>,

    pub(super) broadcast_packets_max: usize,
    pub(super) broadcast_packets_cnt: usize,
    pub(super) broadcast_packets: HashMap<ClientId, BroadcastPackets>,

    // properties
    pub session_expiry_interval: u32,
    pub receive_max: u16,
    // to limit the max packet size server can send
    pub max_packet_size: u32,
    // client topic alias maximum
    pub topic_alias_max: u16,
    pub(super) request_response_info: bool,
    pub(super) request_problem_info: bool,
    pub user_properties: Vec<UserProperty>,
    pub auth_method: Option<Arc<str>>,
}

pub struct SessionState {
    pub client_id: ClientId,
    pub receiver: ClientReceiver,
    pub protocol: Protocol,

    // For record packet id send from server to client
    pub server_packet_id: Pid,
    pub pending_packets: PendingPackets<PubPacket>,
    pub qos2_pids: HashMap<Pid, u64>,
    pub subscribes: HashMap<TopicFilter, SubscriptionData>,
    pub broadcast_packets_cnt: usize,
    pub broadcast_packets: HashMap<ClientId, BroadcastPackets>,
}

impl Session {
    pub fn new(config: &Config, peer: SocketAddr) -> Session {
        Session {
            peer,
            authorizing: false,
            connected: false,
            client_disconnected: false,
            server_disconnected: false,
            protocol: Protocol::V500,
            scram_stage: ScramStage::new(),
            connected_time: None,
            connection_closed_time: None,
            last_packet_time: Arc::new(RwLock::new(Instant::now())),
            server_packet_id: Pid::default(),
            pending_packets: PendingPackets::new(
                config.max_inflight_client,
                config.max_in_mem_pending_messages,
                config.inflight_timeout,
            ),
            qos2_pids: HashMap::new(),

            client_id: ClientId::max_value(),
            client_identifier: Arc::from(String::new()),
            assigned_client_id: false,
            server_keep_alive: false,
            scram_auth_result: None,
            username: None,
            keep_alive: 0,
            clean_start: true,
            last_will: None,
            subscribes: HashMap::new(),
            topic_aliases: HashMap::new(),
            broadcast_packets_max: 10,
            broadcast_packets_cnt: 0,
            broadcast_packets: HashMap::new(),

            session_expiry_interval: 0,
            receive_max: config.max_inflight_client,
            max_packet_size: config.max_packet_size_client,
            topic_alias_max: 0,
            request_response_info: false,
            request_problem_info: true,
            user_properties: Vec::new(),
            auth_method: None,
        }
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub(crate) fn incr_server_packet_id(&mut self) -> Pid {
        let old_value = self.server_packet_id;
        self.server_packet_id += 1;
        old_value
    }
}

/// SCRAM handshake stage stored in session state.
///
/// Wraps [`ScramServer`] so the state machine survives across request boundaries
/// without re-parsing or re-deriving any material.
pub struct ScramStage {
    pub machine: ScramServer,
    pub time: Option<Instant>,
}

impl ScramStage {
    pub fn new() -> Self {
        Self {
            machine: ScramServer::new(),
            time: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SubscriptionData {
    pub options: SubscriptionOptions,
    pub id: Option<VarByteInt>,
}

impl SubscriptionData {
    pub fn new(options: SubscriptionOptions, id: Option<VarByteInt>) -> Self {
        SubscriptionData { options, id }
    }
}

#[derive(Debug, Clone)]
pub struct PubPacket {
    pub topic_name: TopicName,
    pub qos: QoS,
    pub retain: bool,
    pub payload: Bytes,
    pub properties: PublishProperties,
}

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
use crate::state::{ClientId, ClientReceiver};

use super::super::{BroadcastPackets, PendingPackets};

// FIXME: move OnlineLoop local data to Session
pub struct Session {
    pub(super) peer: SocketAddr,
    pub(super) authorizing: bool,
    pub(super) connected: bool,
    pub(super) disconnected: bool,
    pub(super) protocol: Protocol,
    pub(super) scram_stage: ScramStage,
    pub(super) connected_time: Option<Instant>,
    // When received a disconnect or tcp connection closed
    pub(super) connection_closed_time: Option<Instant>,
    // last package timestamp
    pub(super) last_packet_time: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    pub(super) server_packet_id: Pid,
    pub(super) pending_packets: PendingPackets<PubPacket>,
    // client side of pending packets (ids), the value is a ahash digest for
    // detecting PacketIdentifierInUse.
    //   See this page for why choose ahash:
    //   https://github.com/tkaitchuck/aHash/blob/master/compare/readme.md#speed
    pub(super) qos2_pids: HashMap<Pid, u64>,

    pub(super) client_id: ClientId,
    pub(super) client_identifier: Arc<String>,
    pub(super) assigned_client_id: bool,
    pub(super) server_keep_alive: bool,
    // (username, Option<role>)
    pub(super) scram_auth_result: Option<(String, Option<String>)>,
    pub(super) username: Option<Arc<String>>,
    pub(super) keep_alive: u16,
    pub(super) clean_start: bool,
    pub(super) last_will: Option<LastWill>,
    // The Subscription Identifiers are part of the Session State in the Server
    pub(super) subscribes: HashMap<TopicFilter, SubscriptionData>,
    // Topic aliases are connection only data (not session state)
    pub(super) topic_aliases: HashMap<u16, TopicName>,

    pub(super) broadcast_packets_max: usize,
    pub(super) broadcast_packets_cnt: usize,
    pub(super) broadcast_packets: HashMap<ClientId, BroadcastPackets>,

    // properties
    pub(super) session_expiry_interval: u32,
    pub(super) receive_max: u16,
    pub(super) max_packet_size: u32,
    // client topic alias maximum
    pub(super) topic_alias_max: u16,
    pub(super) request_response_info: bool,
    pub(super) request_problem_info: bool,
    pub(super) user_properties: Vec<UserProperty>,
    pub(super) auth_method: Option<Arc<String>>,
}

pub struct SessionState {
    pub client_id: ClientId,
    pub receiver: ClientReceiver,
    pub protocol: Protocol,

    pub broadcast_packets_cnt: usize,
    pub broadcast_packets: HashMap<ClientId, BroadcastPackets>,

    // For record packet id send from server to client
    pub server_packet_id: Pid,
    pub pending_packets: PendingPackets<PubPacket>,
    pub qos2_pids: HashMap<Pid, u64>,
    pub subscribes: HashMap<TopicFilter, SubscriptionData>,
}

impl Session {
    pub fn new(config: &Config, peer: SocketAddr) -> Session {
        Session {
            peer,
            authorizing: false,
            connected: false,
            disconnected: false,
            protocol: Protocol::V500,
            scram_stage: ScramStage::Init,
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
            client_identifier: Arc::new(String::new()),
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
            max_packet_size: config.max_packet_size,
            topic_alias_max: 0,
            request_response_info: false,
            request_problem_info: true,
            user_properties: Vec::new(),
            auth_method: None,
        }
    }

    pub fn subscribes(&self) -> &HashMap<TopicFilter, SubscriptionData> {
        &self.subscribes
    }

    pub(crate) fn incr_server_packet_id(&mut self) -> Pid {
        let old_value = self.server_packet_id;
        self.server_packet_id += 1;
        old_value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramStage {
    Init,
    // received client first and sent server first to client
    ClientFirst { message: String, time: Instant },
    // received client final and sent server final to client
    Final(Instant),
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

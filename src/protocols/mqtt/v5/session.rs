use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use flume::Receiver;
use hashbrown::HashMap;
use mqtt_proto::{
    v5::{LastWill, PublishProperties, SubscriptionOptions, UserProperty, VarByteInt},
    Pid, Protocol, QoS, TopicFilter, TopicName,
};
use parking_lot::RwLock;

use crate::config::Config;
use crate::state::{ClientId, InternalMessage};

use super::super::PendingPackets;

pub struct Session {
    pub(super) peer: SocketAddr,
    pub(super) connected: bool,
    pub(super) disconnected: bool,
    pub(super) protocol: Protocol,
    pub(super) connected_time: Option<Instant>,
    // When received a disconnect or tcp connection closed
    pub(super) connection_closed_time: Option<Instant>,
    // last package timestamp
    pub(super) last_packet_time: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    pub(super) server_packet_id: Pid,
    pub(super) pending_packets: PendingPackets<PubPacket>,

    pub(super) client_id: ClientId,
    pub(super) client_identifier: Arc<String>,
    pub(super) username: Option<Arc<String>>,
    pub(super) keep_alive: u16,
    pub(super) clean_start: bool,
    pub(super) last_will: Option<LastWill>,
    // Topic aliases are connection only data (not session state)
    pub(super) topic_aliases: HashMap<u16, TopicName>,
    // The Subscription Identifiers are part of the Session State in the Server
    pub(super) subscribes: HashMap<TopicFilter, SubscriptionData>,

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
    pub(super) auth_data: Option<Bytes>,
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

pub struct SessionState {
    pub client_id: ClientId,
    pub receiver: Receiver<(ClientId, InternalMessage)>,
    pub protocol: Protocol,

    // For record packet id send from server to client
    pub server_packet_id: Pid,
    pub pending_packets: PendingPackets<PubPacket>,
    pub subscribes: HashMap<TopicFilter, SubscriptionData>,
}

impl Session {
    pub fn new(config: &Config, peer: SocketAddr) -> Session {
        Session {
            peer,
            connected: false,
            disconnected: false,
            protocol: Protocol::V311,
            connected_time: None,
            connection_closed_time: None,
            last_packet_time: Arc::new(RwLock::new(Instant::now())),
            server_packet_id: Pid::default(),
            pending_packets: PendingPackets::new(
                config.max_inflight_client,
                config.max_in_mem_pending_messages,
                config.inflight_timeout,
            ),

            client_id: ClientId(u64::max_value()),
            client_identifier: Arc::new(String::new()),
            username: None,
            keep_alive: 0,
            clean_start: true,
            last_will: None,
            subscribes: HashMap::new(),
            topic_aliases: HashMap::new(),

            session_expiry_interval: 0,
            receive_max: config.max_inflight_client,
            max_packet_size: config.max_packet_size,
            topic_alias_max: 0,
            request_response_info: false,
            request_problem_info: true,
            user_properties: Vec::new(),
            auth_method: None,
            auth_data: None,
        }
    }

    pub fn peer(&self) -> &SocketAddr {
        &self.peer
    }
    pub fn connected(&self) -> bool {
        self.connected
    }
    pub fn disconnected(&self) -> bool {
        self.disconnected
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
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

#[derive(Debug, Clone)]
pub struct PubPacket {
    pub topic_name: TopicName,
    pub qos: QoS,
    pub retain: bool,
    pub payload: Bytes,
    pub properties: PublishProperties,
}

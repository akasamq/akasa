use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hashbrown::HashMap;
use mqtt_proto::{v3::LastWill, Pid, Protocol, QoS, TopicFilter, TopicName};
use parking_lot::RwLock;

use crate::auth::user::User;
use crate::config::Config;
use crate::state::{ClientId, ClientReceiver};

use super::super::{BroadcastPackets, PendingPackets};

pub struct Session {
    pub peer: SocketAddr,
    pub(super) connected: bool,
    pub(super) disconnected: bool,
    pub(super) protocol: Protocol,
    pub connected_time: Option<Instant>,
    // last package timestamp
    pub last_packet_time: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    pub(super) server_packet_id: Pid,
    pub(super) pending_packets: PendingPackets<PubPacket>,
    pub(super) qos2_pids: HashMap<Pid, u64>,

    pub(super) client_id: ClientId,
    pub client_identifier: Arc<String>,
    pub assigned_client_id: bool,
    pub user: Option<Arc<User>>,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub last_will: Option<LastWill>,
    pub subscribes: HashMap<TopicFilter, QoS>,

    pub(super) broadcast_packets_max: usize,
    pub(super) broadcast_packets_cnt: usize,
    pub(super) broadcast_packets: HashMap<ClientId, BroadcastPackets>,
}

pub struct SessionState {
    pub client_id: ClientId,
    pub protocol: Protocol,
    pub receiver: ClientReceiver,

    // For record packet id send from server to client
    pub server_packet_id: Pid,
    pub pending_packets: PendingPackets<PubPacket>,
    pub qos2_pids: HashMap<Pid, u64>,
    pub subscribes: HashMap<TopicFilter, QoS>,
    pub broadcast_packets_cnt: usize,
    pub broadcast_packets: HashMap<ClientId, BroadcastPackets>,
}

impl Session {
    pub fn new(config: &Config, peer: SocketAddr) -> Session {
        Session {
            peer,
            connected: false,
            disconnected: false,
            protocol: Protocol::V311,
            connected_time: None,
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
            user: None,
            keep_alive: 0,
            clean_session: true,
            last_will: None,
            subscribes: HashMap::new(),
            broadcast_packets_max: 10,
            broadcast_packets_cnt: 0,
            broadcast_packets: HashMap::new(),
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

#[derive(Debug, Clone)]
pub struct PubPacket {
    pub topic_name: TopicName,
    pub qos: QoS,
    pub retain: bool,
    pub payload: Bytes,
}

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use flume::Receiver;
use hashbrown::HashMap;
use mqtt_proto::{v3::LastWill, Pid, Protocol, QoS, TopicFilter, TopicName};
use parking_lot::RwLock;

use crate::config::Config;
use crate::state::{ClientId, InternalMessage};

use super::super::PendingPackets;

pub struct Session {
    pub(super) peer: SocketAddr,
    pub(super) connected: bool,
    pub(super) disconnected: bool,
    pub(super) protocol: Protocol,
    // last package timestamp
    pub(super) last_packet_time: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    pub(super) server_packet_id: Pid,
    pub(super) pending_packets: PendingPackets<PubPacket>,
    pub(super) qos2_pids: HashMap<Pid, u64>,

    pub(super) client_id: ClientId,
    pub(super) client_identifier: Arc<String>,
    pub(super) username: Option<Arc<String>>,
    pub(super) keep_alive: u16,
    pub(super) clean_session: bool,
    pub(super) last_will: Option<LastWill>,
    pub(super) subscribes: HashMap<TopicFilter, QoS>,
}

pub struct SessionState {
    pub client_id: ClientId,
    pub protocol: Protocol,
    pub receiver: Receiver<(ClientId, InternalMessage)>,

    // For record packet id send from server to client
    pub server_packet_id: Pid,
    pub pending_packets: PendingPackets<PubPacket>,
    pub qos2_pids: HashMap<Pid, u64>,
    pub subscribes: HashMap<TopicFilter, QoS>,
}

impl Session {
    pub fn new(config: &Config, peer: SocketAddr) -> Session {
        Session {
            peer,
            connected: false,
            disconnected: false,
            protocol: Protocol::V311,
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
            username: None,
            keep_alive: 0,
            clean_session: true,
            last_will: None,
            subscribes: HashMap::new(),
        }
    }

    pub fn peer(&self) -> &SocketAddr {
        &self.peer
    }
    pub fn clean_session(&self) -> bool {
        self.clean_session
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
    pub fn subscribes(&self) -> &HashMap<TopicFilter, QoS> {
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
}

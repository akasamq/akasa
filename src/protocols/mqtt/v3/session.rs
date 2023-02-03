use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use flume::Receiver;
use hashbrown::HashMap;
use mqtt_proto::{Pid, Protocol, QoS, TopicFilter, TopicName};
use parking_lot::RwLock;

use crate::config::Config;
use crate::state::{ClientId, InternalMessage};

use super::pending::PendingPackets;

pub struct Session {
    pub(super) connected: bool,
    pub(super) disconnected: bool,
    pub(super) protocol: Protocol,
    // last package timestamp
    pub(super) last_packet_time: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    pub(super) server_packet_id: Pid,
    pub(super) pending_packets: PendingPackets,

    pub(super) client_id: ClientId,
    pub(super) client_identifier: Arc<String>,
    pub(super) username: Option<Arc<String>>,
    pub(super) keep_alive: u16,
    pub(super) clean_session: bool,
    pub(super) will: Option<Will>,
    pub(super) subscribes: HashMap<TopicFilter, QoS>,
}

pub struct SessionState {
    pub protocol: Protocol,
    // For record packet id send from server to client
    pub server_packet_id: Pid,
    pub pending_packets: PendingPackets,
    pub receiver: Receiver<(ClientId, InternalMessage)>,

    pub client_id: ClientId,
    pub subscribes: HashMap<TopicFilter, QoS>,
}

impl Session {
    pub fn new(config: &Config) -> Session {
        Session {
            connected: false,
            disconnected: false,
            protocol: Protocol::V311,
            last_packet_time: Arc::new(RwLock::new(Instant::now())),
            server_packet_id: Pid::default(),
            pending_packets: PendingPackets::new(
                config.max_inflight,
                config.max_in_mem_pending_messages,
                config.inflight_timeout,
            ),

            client_id: ClientId(u64::max_value()),
            client_identifier: Arc::new(String::new()),
            username: None,
            keep_alive: 0,
            clean_session: true,
            will: None,
            subscribes: HashMap::new(),
        }
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
    pub subscribe_filter: TopicFilter,
    pub subscribe_qos: QoS,
}

#[derive(Debug, Clone)]
pub struct Will {
    pub retain: bool,
    pub qos: QoS,
    pub topic: TopicName,
    pub message: Bytes,
}

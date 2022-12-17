use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use flume::Receiver;
use hashbrown::HashMap;
use mqtt::{control::ProtocolLevel, qos::QualityOfService, TopicFilter, TopicName};
use parking_lot::RwLock;

use crate::config::Config;
use crate::state::{ClientId, InternalMessage};

use super::pending::PendingPackets;

pub struct Session {
    pub(super) connected: bool,
    pub(super) disconnected: bool,
    pub(super) protocol_level: ProtocolLevel,
    // last package timestamp
    pub(super) last_packet_time: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    pub(super) server_packet_id: u64,
    pub(super) pending_packets: PendingPackets,

    pub(super) client_id: ClientId,
    pub(super) client_identifier: String,
    pub(super) username: Option<String>,
    pub(super) keep_alive: u16,
    pub(super) clean_session: bool,
    pub(super) will: Option<Will>,
    pub(super) subscribes: HashMap<TopicFilter, QualityOfService>,
}

pub struct SessionState {
    pub protocol_level: ProtocolLevel,
    // For record packet id send from server to client
    pub server_packet_id: u64,
    pub pending_packets: PendingPackets,
    pub receiver: Receiver<(ClientId, InternalMessage)>,

    pub client_id: ClientId,
    pub subscribes: HashMap<TopicFilter, QualityOfService>,
}

impl Session {
    pub fn new(config: &Config) -> Session {
        Session {
            connected: false,
            disconnected: false,
            protocol_level: ProtocolLevel::Version311,
            last_packet_time: Arc::new(RwLock::new(Instant::now())),
            server_packet_id: 0,
            pending_packets: PendingPackets::new(
                config.max_inflight,
                config.max_in_mem_pending_messages,
                config.inflight_timeout,
            ),

            client_id: ClientId(u64::max_value()),
            client_identifier: String::new(),
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
    pub fn subscribes(&self) -> &HashMap<TopicFilter, QualityOfService> {
        &self.subscribes
    }

    pub(crate) fn incr_server_packet_id(&mut self) -> u64 {
        let old_value = self.server_packet_id;
        self.server_packet_id += 1;
        old_value
    }
}

#[derive(Debug, Clone)]
pub struct PubPacket {
    pub topic_name: Arc<TopicName>,
    pub qos: QualityOfService,
    pub retain: bool,
    pub payload: Bytes,
    pub subscribe_filter: Arc<TopicFilter>,
    pub subscribe_qos: QualityOfService,
}

#[derive(Debug, Clone)]
pub struct Will {
    pub retain: bool,
    pub qos: QualityOfService,
    pub topic: TopicName,
    pub message: Vec<u8>,
}

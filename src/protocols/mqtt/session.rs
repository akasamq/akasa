use std::io;
use std::sync::Arc;

use bytes::Bytes;
use flume::Receiver;
use glommio::sync::Semaphore;
use hashbrown::HashMap;
use mqtt::{qos::QualityOfService, TopicFilter, TopicName};

use crate::state::{ClientId, InternalMsg};

use super::pending::PendingPackets;

pub struct Session {
    pub io_error: Option<io::Error>,
    pub(crate) connected: bool,
    pub(crate) disconnected: bool,
    pub(crate) write_lock: Semaphore,
    // for record packet id send from client to server
    pub(crate) client_packet_id: u16,
    // For record packet id send from server to client
    pub(crate) server_packet_id: u64,
    pub(crate) pending_packets: PendingPackets,

    pub(crate) client_id: ClientId,
    pub(crate) client_identifier: String,
    pub(crate) username: Option<String>,
    pub(crate) clean_session: bool,
    pub(crate) will: Option<Will>,
    pub(crate) subscribes: HashMap<TopicFilter, QualityOfService>,
}

pub struct SessionState {
    // for record packet id send from client to server
    pub client_packet_id: u16,
    // For record packet id send from server to client
    pub server_packet_id: u64,
    pub pending_packets: PendingPackets,
    pub receiver: Receiver<(ClientId, InternalMsg)>,

    pub client_id: ClientId,
    pub subscribes: HashMap<TopicFilter, QualityOfService>,
}

impl Session {
    pub fn new() -> Session {
        Session {
            io_error: None,
            connected: false,
            disconnected: false,
            write_lock: Semaphore::new(1),
            client_packet_id: 0,
            server_packet_id: 0,
            // FIXME: read max inflight and max packets from config
            pending_packets: PendingPackets::new(10, 1000),

            client_id: ClientId::default(),
            client_identifier: String::new(),
            username: None,
            clean_session: true,
            will: None,
            subscribes: HashMap::new(),
        }
    }

    pub fn clean_session(&self) -> bool {
        self.clean_session
    }
    pub fn disconnected(&self) -> bool {
        self.disconnected
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub(crate) fn push_packet(&mut self, packet: PubPacket) {
        self.pending_packets
            .push_back(self.server_packet_id, packet);
        self.server_packet_id += 1;
    }
    pub(crate) fn incr_client_packet_id(&mut self) -> u16 {
        let old_value = self.client_packet_id;
        self.client_packet_id = self.client_packet_id.wrapping_add(1);
        old_value
    }
}

#[derive(Debug, Clone)]
pub struct PubPacket {
    pub topic_name: Arc<TopicName>,
    pub qos: QualityOfService,
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

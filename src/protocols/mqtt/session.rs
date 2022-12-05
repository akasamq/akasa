use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use flume::Receiver;
use glommio::sync::Semaphore;
use hashbrown::HashMap;
use mqtt::{qos::QualityOfService, TopicFilter, TopicName};

use crate::state::{ClientId, InternalMsg};

pub struct Session {
    pub io_error: Option<io::Error>,
    pub(crate) connected: bool,
    pub(crate) disconnected: bool,
    pub(crate) write_lock: Semaphore,
    // for record packet id send from server to client
    pub(crate) packet_id: u16,
    // For assign a message id received from inernal sender (pub/sub)
    pub(crate) message_id: u64,
    pub(crate) pending_messages: VecDeque<(u64, PublishMessage)>,

    pub(crate) client_id: ClientId,
    pub(crate) client_identifier: String,
    pub(crate) username: Option<String>,
    pub(crate) clean_session: bool,
    pub(crate) will: Option<Will>,
    pub(crate) subscribes: HashMap<TopicFilter, QualityOfService>,
}

pub struct SessionState {
    // for record packet id send from server to client
    pub packet_id: u16,
    // For assign a message id received from inernal sender (pub/sub)
    pub message_id: u64,
    pub pending_messages: VecDeque<(u64, PublishMessage)>,
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
            packet_id: 0,
            message_id: 0,
            pending_messages: VecDeque::new(),

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

    pub(crate) fn push_message(&mut self, msg: PublishMessage) {
        self.pending_messages.push_back((self.message_id, msg));
        self.message_id += 1;
    }
    pub(crate) fn incr_packet_id(&mut self) -> u16 {
        let old_value = self.packet_id;
        self.packet_id = self.packet_id.wrapping_add(1);
        old_value
    }
}

pub struct PublishMessage {
    pub topic_name: Arc<TopicName>,
    pub qos: QualityOfService,
    pub payload: Bytes,
    pub subscribe_filter: Arc<TopicFilter>,
    pub subscribe_qos: QualityOfService,
}

pub struct Will {
    pub retain: bool,
    pub qos: QualityOfService,
    pub topic: TopicName,
    pub message: Vec<u8>,
}

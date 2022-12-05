use std::collections::VecDeque;
use std::sync::Arc;

use hashbrown::HashMap;
use mqtt::TopicName;

use super::session::PubPacket;

#[derive(Debug)]
pub struct PendingPackets {
    max_inflight: u32,
    max_packets: u32,
    packets: HashMap<Arc<TopicName>, VecDeque<PubPacket>>,
}

impl PendingPackets {
    pub fn new(max_inflight: u32, max_packets: u32) -> PendingPackets {
        PendingPackets {
            max_inflight,
            max_packets,
            packets: HashMap::new(),
        }
    }

    pub fn push_back(&mut self, packet_id: u64, packet: PubPacket) {
        todo!()
    }
}

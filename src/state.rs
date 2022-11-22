use std::os::unix::io::RawFd;
use std::sync::Arc;

use dashmap::DashMap;
use flume::Sender;
use mqtt::{packet::QoSWithPacketIdentifier, TopicName};

use crate::route::RouteTable;

#[derive(Clone)]
pub enum InternalMsg {
    Publish {
        topic_name: TopicName,
        qos: QoSWithPacketIdentifier,
        // TODO: maybe should change to bytes::Bytes
        payload: Vec<u8>,
    },
}

pub struct GlobalState {
    pub connections: DashMap<RawFd, Sender<(RawFd, Arc<InternalMsg>)>>,
    pub route_table: RouteTable,
}

impl GlobalState {
    pub fn new() -> GlobalState {
        GlobalState {
            connections: DashMap::new(),
            route_table: RouteTable::new(),
        }
    }
}

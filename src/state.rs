use std::os::unix::io::RawFd;

use bytes::Bytes;
use dashmap::DashMap;
use flume::Sender;
use mqtt::packet::QoSWithPacketIdentifier;

use crate::route::RouteTable;

pub enum InternalMsg {
    Publish {
        qos: QoSWithPacketIdentifier,
        payload: bytes::Bytes,
    },
}

pub struct GlobalState {
    pub connections: DashMap<RawFd, Sender<(RawFd, InternalMsg)>>,
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

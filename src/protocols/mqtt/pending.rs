//!
//! TODO: save packets in storage (rocksdb/sqlite3)
//!
use std::cmp;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::time::SystemTime;

use mqtt_proto::{Pid, QoS};

pub struct PendingPackets<P> {
    max_inflight: u16,
    max_packets: usize,
    // The ack packet timeout, when reached resent the packet
    timeout: u64,
    packets: VecDeque<PendingPacketStatus<P>>,
}

impl<P: Debug> PendingPackets<P> {
    pub fn new(max_inflight: u16, max_packets: usize, timeout: u64) -> PendingPackets<P> {
        PendingPackets {
            max_inflight,
            max_packets,
            timeout,
            packets: VecDeque::new(),
        }
    }

    /// Push a packet into queue, return if the queue is full.
    pub fn push_back(&mut self, pid: Pid, packet: P) -> bool {
        if self.packets.len() >= self.max_packets {
            log::error!(
                "drop packet {:?}, due to too many packets in the queue: {}",
                packet,
                self.packets.len()
            );
            return true;
        }
        self.packets.push_back(PendingPacketStatus::New {
            added_at: get_unix_ts(),
            last_sent: 0,
            pid,
            packet,
            dup: false,
        });
        false
    }

    pub fn pubrec(&mut self, target_pid: Pid) -> bool {
        let current_inflight = cmp::min(self.max_inflight as usize, self.packets.len());
        for idx in 0..current_inflight {
            let packet_status = self.packets.get_mut(idx).expect("packet");
            match packet_status {
                PendingPacketStatus::New { pid, .. } => {
                    if *pid == target_pid {
                        *packet_status = PendingPacketStatus::Pubrec {
                            last_sent: get_unix_ts(),
                            pid: target_pid,
                        };
                        return true;
                    }
                }
                PendingPacketStatus::Pubrec { .. } => {}
                PendingPacketStatus::Complete => {}
            }
        }
        false
    }

    pub fn complete(&mut self, target_pid: Pid, qos: QoS) -> bool {
        let current_inflight = cmp::min(self.max_inflight as usize, self.packets.len());
        for idx in 0..current_inflight {
            let packet_status = self.packets.get_mut(idx).expect("packet");
            match packet_status {
                PendingPacketStatus::New { pid, .. } if qos == QoS::Level1 => {
                    if *pid == target_pid {
                        *packet_status = PendingPacketStatus::Complete;
                        return true;
                    }
                }
                PendingPacketStatus::Pubrec { pid, .. } if qos == QoS::Level2 => {
                    if *pid == target_pid {
                        *packet_status = PendingPacketStatus::Complete;
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    pub fn clean_complete(&mut self) {
        let mut changed = false;
        while let Some(PendingPacketStatus::Complete) = self.packets.front() {
            self.packets.pop_front();
            changed = true;
        }
        // shrink the queue to save memory
        if changed {
            if self.packets.capacity() >= 16 && self.packets.capacity() >= (self.packets.len() << 2)
            {
                self.packets.shrink_to(self.packets.len() << 1);
            } else if self.packets.is_empty() {
                self.packets.shrink_to(0);
            }
        }
    }

    pub fn get_ready_packet(
        &mut self,
        start_idx: usize,
    ) -> Option<(usize, &mut PendingPacketStatus<P>)> {
        let now_ts = get_unix_ts();
        let current_inflight = cmp::min(self.max_inflight as usize, self.packets.len());
        let mut next_idx = None;
        for idx in start_idx..current_inflight {
            let packet_status = self.packets.get_mut(idx).expect("packet");
            match packet_status {
                PendingPacketStatus::New { last_sent, .. } => {
                    if now_ts >= self.timeout + *last_sent {
                        *last_sent = now_ts;
                        next_idx = Some(idx);
                        break;
                    }
                }
                PendingPacketStatus::Pubrec { last_sent, .. } => {
                    if now_ts >= self.timeout + *last_sent {
                        *last_sent = now_ts;
                        next_idx = Some(idx);
                        break;
                    }
                }
                PendingPacketStatus::Complete => {}
            }
        }
        next_idx.map(|idx| (idx, self.packets.get_mut(idx).expect("packet")))
    }

    pub fn len(&self) -> usize {
        self.packets.len()
    }

    pub fn set_max_inflight(&mut self, new_value: u16) {
        self.max_inflight = new_value;
    }
}

pub enum PendingPacketStatus<P> {
    New {
        added_at: u64,
        // Last sent this packet timestamp as seconds
        last_sent: u64,
        pid: Pid,
        packet: P,
        dup: bool,
    },
    Pubrec {
        // Last sent this packet timestamp as seconds
        last_sent: u64,
        pid: Pid,
        // v5.x only field
        // properties: Option<PubrecProperties>,
    },
    Complete,
}

/// Unix timestamp as seconds
pub(crate) fn get_unix_ts() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

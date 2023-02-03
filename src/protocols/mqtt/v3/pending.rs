//!
//! TODO: save packets in storage (rocksdb/sqlite3)
//!
use std::cmp;
use std::collections::VecDeque;
use std::io;
use std::time::SystemTime;

use mqtt_proto::{Pid, QoS};

use super::session::PubPacket;

pub struct PendingPackets {
    max_inflight: usize,
    max_packets: usize,
    // The ack packet timeout, when reached resent the packet
    timeout: u64,
    packets: VecDeque<PendingPacketStatus>,
}

impl PendingPackets {
    pub fn new(max_inflight: usize, max_packets: usize, timeout: u64) -> PendingPackets {
        PendingPackets {
            max_inflight,
            max_packets,
            timeout,
            packets: VecDeque::new(),
        }
    }

    pub fn push_back(&mut self, pid: Pid, packet: PubPacket) -> io::Result<()> {
        assert!(packet.qos != QoS::Level0);
        if self.packets.len() >= self.max_packets {
            log::error!(
                "drop packet {:?}, due to too many packets in the queue: {}",
                packet,
                self.packets.len()
            );
            // FIXME: use proper error type
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        self.packets.push_back(PendingPacketStatus::New {
            last_sent: 0,
            pid,
            packet,
            dup: false,
        });
        Ok(())
    }

    pub fn pubrec(&mut self, target_pid: Pid) {
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
                        break;
                    }
                }
                PendingPacketStatus::Pubrec { .. } => {}
                PendingPacketStatus::Complete => {}
            }
        }
    }

    pub fn complete(&mut self, target_pid: Pid) {
        let current_inflight = cmp::min(self.max_inflight as usize, self.packets.len());
        for idx in 0..current_inflight {
            let packet_status = self.packets.get_mut(idx).expect("packet");
            match packet_status {
                PendingPacketStatus::New { pid, .. } => {
                    if *pid == target_pid {
                        *packet_status = PendingPacketStatus::Complete;
                        break;
                    }
                }
                PendingPacketStatus::Pubrec { pid, .. } => {
                    if *pid == target_pid {
                        *packet_status = PendingPacketStatus::Complete;
                        break;
                    }
                }
                PendingPacketStatus::Complete => {}
            }
        }
    }

    pub fn clean_complete(&mut self) {
        let mut changed = false;
        while let Some(PendingPacketStatus::Complete) = self.packets.front() {
            self.packets.pop_front();
            changed = true;
        }
        // shrink the queue to save memory
        if changed {
            if self.packets.len() >= 16 && self.packets.capacity() >= (self.packets.len() << 2) {
                self.packets.shrink_to(self.packets.len() << 1);
            } else if self.packets.is_empty() {
                self.packets.shrink_to(0);
            }
        }
    }

    pub fn get_ready_packet(
        &mut self,
        start_idx: usize,
    ) -> Option<(usize, &mut PendingPacketStatus)> {
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
}

pub enum PendingPacketStatus {
    New {
        // Last sent this packet timestamp as seconds
        last_sent: u64,
        pid: Pid,
        packet: PubPacket,
        dup: bool,
    },
    Pubrec {
        // Last sent this packet timestamp as seconds
        last_sent: u64,
        pid: Pid,
    },
    Complete,
}

fn get_unix_ts() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

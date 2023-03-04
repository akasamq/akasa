use std::io;
use std::time::Instant;

use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v3::{Packet, Publish},
    QoS, QosPid,
};

use crate::protocols::mqtt::PendingPacketStatus;
use crate::state::ClientId;

use super::super::Session;

#[inline]
pub(crate) fn after_handle_packet(session: &mut Session) -> Vec<Packet> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session)
}

#[inline]
pub(crate) fn handle_pendings(session: &mut Session) -> Vec<Packet> {
    session.pending_packets.clean_complete();
    let mut packets = Vec::new();
    let mut start_idx = 0;
    while let Some((idx, packet_status)) = session.pending_packets.get_ready_packet(start_idx) {
        start_idx = idx + 1;
        match packet_status {
            PendingPacketStatus::New {
                dup, pid, packet, ..
            } => {
                let qos_pid = match packet.qos {
                    QoS::Level0 => QosPid::Level0,
                    QoS::Level1 => QosPid::Level1(*pid),
                    QoS::Level2 => QosPid::Level2(*pid),
                };
                let rv_packet = Publish {
                    dup: *dup,
                    retain: packet.retain,
                    qos_pid,
                    topic_name: packet.topic_name.clone(),
                    payload: packet.payload.clone(),
                };
                *dup = true;
                packets.push(rv_packet.into());
            }
            PendingPacketStatus::Pubrec { pid, .. } => {
                packets.push(Packet::Pubrel(*pid));
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }
    packets
}

#[inline]
pub(crate) async fn write_packet<T: AsyncWrite + Unpin>(
    client_id: ClientId,
    conn: &mut T,
    packet: &Packet,
) -> io::Result<()> {
    log::debug!("write to {}: {:?}", client_id, packet);
    packet.encode_async(conn).await?;
    log::debug!("write to {}: {:?} FINISHED!!!", client_id, packet);
    Ok(())
}

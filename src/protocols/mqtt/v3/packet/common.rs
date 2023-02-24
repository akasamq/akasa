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
pub(crate) async fn after_handle_packet<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session, conn).await?;
    Ok(())
}
#[inline]
pub(super) async fn handle_pendings<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    session.pending_packets.clean_complete();
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
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
            }
            PendingPacketStatus::Pubrec { pid, .. } => {
                write_packet(session.client_id, conn, &Packet::Pubrel(*pid)).await?;
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }
    Ok(())
}

#[inline]
pub(super) async fn write_packet<T: AsyncWrite + Unpin>(
    client_id: ClientId,
    conn: &mut T,
    packet: &Packet,
) -> io::Result<()> {
    log::debug!("write to {}: {:?}", client_id, packet);
    packet.encode_async(conn).await?;
    log::debug!("write to {}: {:?} FINISHED!!!", client_id, packet);
    Ok(())
}

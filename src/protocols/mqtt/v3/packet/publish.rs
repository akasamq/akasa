use std::hash::{Hash, Hasher};
use std::io;
use std::sync::Arc;

use ahash::AHasher;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v3::{Packet, Publish},
    Pid, QoS, QosPid,
};

use crate::state::GlobalState;

use super::super::Session;
use super::common::{send_publish, write_packet, SendPublish};

#[inline]
pub(crate) async fn handle_publish<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Publish,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a publish packet:
topic name : {}
   payload : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_id,
        packet.topic_name,
        packet.payload,
        packet.qos_pid,
        packet.retain,
        packet.dup,
    );
    if packet.topic_name.starts_with('$') {
        log::debug!("invalid topic name: {}", packet.topic_name);
        return Err(io::ErrorKind::InvalidData.into());
    }
    if packet.qos_pid == QosPid::Level0 && packet.dup {
        log::debug!("invalid dup flag");
        return Err(io::ErrorKind::InvalidData.into());
    }

    if let QosPid::Level2(pid) = packet.qos_pid {
        let mut hasher = AHasher::default();
        packet.hash(&mut hasher);
        let current_hash = hasher.finish();

        if let Some(previous_hash) = session.qos2_pids.get(&pid) {
            // hash collision is acceptable here
            if current_hash != *previous_hash {
                log::info!("packet identifier in use: {}", pid.value());
                return Err(io::ErrorKind::InvalidData.into());
            }
            if !packet.dup {
                log::info!(
                    "dup flag must be true for re-deliver packet: {}",
                    pid.value()
                );
                return Err(io::ErrorKind::InvalidData.into());
            }
        } else {
            // FIXME: check qos2_pids limit
            session.qos2_pids.insert(pid, current_hash);
        }
    }

    send_publish(
        session,
        SendPublish {
            topic_name: &packet.topic_name,
            retain: packet.retain,
            qos: packet.qos_pid.qos(),
            payload: &packet.payload,
        },
        global,
    )
    .await;
    match packet.qos_pid {
        QosPid::Level0 => {}
        QosPid::Level1(pid) => write_packet(session.client_id, conn, &Packet::Puback(pid)).await?,
        QosPid::Level2(pid) => write_packet(session.client_id, conn, &Packet::Pubrec(pid)).await?,
    }
    Ok(())
}

#[inline]
pub(crate) async fn handle_puback<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a puback packet: id={}",
        session.client_id,
        pid.value()
    );
    let _matched = session.pending_packets.complete(pid, QoS::Level1);
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubrec<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrec  packet: id={}",
        session.client_id,
        pid.value()
    );

    let _matched = session.pending_packets.pubrec(pid);
    write_packet(session.client_id, conn, &Packet::Pubrel(pid)).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubrel<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrel  packet: id={}",
        session.client_id,
        pid.value()
    );
    if session.qos2_pids.remove(&pid).is_none() {
        log::warn!("packet identifier not found: {}", pid.value());
        return Err(io::ErrorKind::InvalidData.into());
    }
    write_packet(session.client_id, conn, &Packet::Pubcomp(pid)).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubcomp<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubcomp packet: id={}",
        session.client_id,
        pid.value()
    );
    let _matched = session.pending_packets.complete(pid, QoS::Level2);
    Ok(())
}

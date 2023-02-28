use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;

use flume::Receiver;
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use hashbrown::HashMap;
use mqtt_proto::{
    v3::{Connect, Header, Packet},
    Error, Protocol,
};

use crate::state::{
    ClientId, ClientReceiver, ControlMessage, Executor, GlobalState, NormalMessage,
};

use super::super::PendingPackets;
use super::{
    packet::{
        common::after_handle_packet,
        connect::{handle_connect, handle_disconnect},
        pingreq::handle_pingreq,
        publish::{
            handle_puback, handle_pubcomp, handle_publish, handle_pubrec, handle_pubrel,
            recv_publish, send_publish, RecvPublish, SendPublish,
        },
        subscribe::{handle_subscribe, handle_unsubscribe},
    },
    Session, SessionState,
};

pub async fn handle_connection<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    conn: T,
    peer: SocketAddr,
    header: Header,
    protocol: Protocol,
    timeout_receiver: Receiver<()>,
    executor: E,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    match handle_online(
        conn,
        peer,
        header,
        protocol,
        timeout_receiver,
        &executor,
        &global,
    )
    .await
    {
        Ok(Some((session, receiver))) => {
            log::info!(
                "executor {:03}, {} go to offline, total {} clients ({} online)",
                executor.id(),
                peer,
                global.clients_count(),
                global.online_clients_count(),
            );
            executor.spawn_local(handle_offline(session, receiver, global));
        }
        Ok(None) => {
            log::info!(
                "executor {:03}, {} finished, total {} clients ({} online)",
                executor.id(),
                peer,
                global.clients_count(),
                global.online_clients_count(),
            );
        }
        Err(err) => {
            log::info!(
                "executor {:03}, {} error: {}, total {} clients ({} online)",
                executor.id(),
                peer,
                err,
                global.clients_count(),
                global.online_clients_count(),
            );
            return Err(err);
        }
    }
    Ok(())
}

async fn handle_online<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    mut conn: T,
    peer: SocketAddr,
    _header: Header,
    protocol: Protocol,
    timeout_receiver: Receiver<()>,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<Option<(Session, ClientReceiver)>> {
    let mut session = Session::new(&global.config, peer);
    let mut receiver = None;
    let mut io_error = None;

    let packet = match Connect::decode_with_protocol(&mut conn, protocol)
        .or(async {
            log::info!("connection timeout: {}", peer);
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()))
        })
        .await
    {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v3.x connect codec error: {}", err);
            return Err(io::ErrorKind::InvalidData.into());
        }
    };
    drop(timeout_receiver);

    handle_connect(
        &mut session,
        &mut receiver,
        packet,
        &mut conn,
        executor,
        global,
    )
    .await?;

    if !session.connected() {
        log::info!("{} not connected", session.peer());
        return Err(io::ErrorKind::InvalidData.into());
    }
    let receiver = receiver.expect("receiver");
    log::info!(
        "executor {:03}, {} connected, total {} clients ({} online) ",
        executor.id(),
        session.peer(),
        global.clients_count(),
        global.online_clients_count(),
    );

    while !session.disconnected() {
        // Online client logic
        tokio::select! {
            result = handle_packet(&mut session, &mut conn, executor, global) => {
                if let Err(err) = result {
                    // An error in online mode should also check clean_session value
                    io_error = Some(err);
                    break;
                }
            },
            result = receiver.control.recv_async() => match result {
                Ok(msg) => {
                    let is_kick = matches!(msg, ControlMessage::Kick { .. });
                    match handle_control(
                        &mut session,
                        &receiver,
                        msg,
                        Some(&mut conn),
                    )
                        .await
                    {
                        Ok(true) => {
                            if is_kick && !session.disconnected() {
                                // Offline client logic
                                io_error = Some(io::ErrorKind::BrokenPipe.into());
                            }
                            // Been occupied by newly connected client or kicked out after disconnected
                            break;
                        }
                        Ok(false) => {}
                        // Currently, this error can only happend when write data to connection
                        Err(err) => {
                            // An error in online mode should also check clean_session value
                            io_error = Some(err);
                            break;
                        }
                    }
                }
                Err(_) => {
                    // An error in online mode should also check clean_session value
                    io_error = Some(io::ErrorKind::BrokenPipe.into());
                    break;
                }
            },
            result = receiver.normal.recv_async() => match result {
                Ok((sender, msg)) => {
                    if let Err(err) = handle_normal(&mut session, sender, msg, Some(&mut conn)).await {
                        // An error in online mode should also check clean_session value
                        io_error = Some(err);
                        break;
                    }
                }
                Err(_) => {
                    io_error = Some(io::ErrorKind::BrokenPipe.into());
                    break;
                }
            },
        };
    }

    if !session.disconnected() {
        handle_will(&mut session, &mut conn, global).await?;
    }
    if session.clean_session() {
        global.remove_client(session.client_id(), session.subscribes().keys());
        if let Some(err) = io_error {
            return Err(err);
        }
    } else {
        // become a offline client, but session keep updating
        global.offline_client(session.client_id());
        session.connected = false;
        return Ok(Some((session, receiver)));
    }

    Ok(None)
}

async fn handle_offline(mut session: Session, receiver: ClientReceiver, _global: Arc<GlobalState>) {
    let mut conn: Option<Vec<u8>> = None;
    loop {
        // FIXME: handle control messages
        let (sender, msg) = match receiver.normal.recv_async().await {
            Ok((sender, msg)) => (sender, msg),
            Err(err) => {
                log::warn!("offline client receive internal message error: {:?}", err);
                break;
            }
        };
        match handle_normal(&mut session, sender, msg, conn.as_mut()).await {
            Ok(()) => {}
            Err(err) => {
                // An error in offline mode should immediately return it
                log::error!("offline client error: {:?}", err);
                break;
            }
        }
    }
    log::debug!("offline client finished: {:?}", session.client_id());
}

async fn handle_packet<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    conn: &mut T,
    _executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    let packet = match Packet::decode_async(conn).await {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v3.x codec error: {}", err);
            if err.is_eof() {
                if !session.disconnected {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                } else {
                    return Ok(());
                }
            } else {
                return Err(io::ErrorKind::InvalidData.into());
            }
        }
    };
    match packet {
        Packet::Disconnect => handle_disconnect(session, conn, global).await?,
        Packet::Publish(pkt) => handle_publish(session, pkt, conn, global).await?,
        Packet::Puback(pid) => handle_puback(session, pid, conn, global).await?,
        Packet::Pubrec(pid) => handle_pubrec(session, pid, conn, global).await?,
        Packet::Pubrel(pid) => handle_pubrel(session, pid, conn, global).await?,
        Packet::Pubcomp(pid) => handle_pubcomp(session, pid, conn, global).await?,
        Packet::Subscribe(pkt) => handle_subscribe(session, pkt, conn, global).await?,
        Packet::Unsubscribe(pkt) => handle_unsubscribe(session, pkt, conn, global).await?,
        Packet::Pingreq => handle_pingreq(session, conn, global).await?,
        _ => return Err(io::ErrorKind::InvalidData.into()),
    }
    after_handle_packet(session, conn).await?;
    Ok(())
}

#[inline]
async fn handle_will<T: AsyncWrite + Unpin>(
    session: &mut Session,
    _conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    if let Some(last_will) = session.last_will.take() {
        send_publish(
            session,
            SendPublish {
                topic_name: &last_will.topic_name,
                retain: last_will.retain,
                qos: last_will.qos,
                payload: &last_will.message,
            },
            global,
        )
        .await;
    }
    Ok(())
}

/// return if the offline client loop should stop
#[inline]
async fn handle_control<T: AsyncWrite + Unpin>(
    session: &mut Session,
    receiver: &ClientReceiver,
    msg: ControlMessage,
    conn: Option<&mut T>,
) -> io::Result<bool> {
    // FIXME: call receiver.try_recv() to clear the channel, if the pending
    // queue is full, set a marker to the global state so that the sender stop
    // sending qos0 messages to this client.
    let mut stop = false;
    match msg {
        ControlMessage::OnlineV3 { sender } => {
            let mut pending_packets = PendingPackets::new(0, 0, 0);
            let mut qos2_pids = HashMap::new();
            let mut subscribes = HashMap::new();
            mem::swap(&mut session.pending_packets, &mut pending_packets);
            mem::swap(&mut session.qos2_pids, &mut qos2_pids);
            mem::swap(&mut session.subscribes, &mut subscribes);
            let old_state = SessionState {
                protocol: session.protocol,
                server_packet_id: session.server_packet_id,
                pending_packets,
                qos2_pids,
                receiver: receiver.clone(),
                client_id: session.client_id,
                subscribes,
            };
            if sender.send_async(old_state).await.is_ok() {
                stop = true;
            } else {
                log::info!("the connection want take over current session already ended");
            }
        }
        ControlMessage::OnlineV5 { .. } => {
            log::info!("take over v3.x by v5.x client is not allowed");
        }
        ControlMessage::Kick { reason } => {
            log::info!(
                "kick {}, reason: {}, offline: {}, network: {}",
                session.client_id,
                reason,
                session.disconnected,
                conn.is_some(),
            );
            stop = conn.is_some();
        }
        ControlMessage::WillDelayReached { .. } | ControlMessage::SessionExpired { .. } => {
            unreachable!();
        }
    }
    Ok(stop)
}

/// return if the offline client loop should stop
#[inline]
async fn handle_normal<T: AsyncWrite + Unpin>(
    session: &mut Session,
    sender: ClientId,
    msg: NormalMessage,
    conn: Option<&mut T>,
) -> io::Result<()> {
    match msg {
        NormalMessage::PublishV3 {
            ref topic_name,
            qos,
            retain,
            ref payload,
            ref subscribe_filter,
            subscribe_qos,
        } => {
            log::debug!(
                "{:?} received a v3.x publish message from {:?}",
                session.client_id,
                sender
            );
            recv_publish(
                session,
                RecvPublish {
                    topic_name,
                    qos,
                    retain,
                    payload,
                    subscribe_filter,
                    subscribe_qos,
                },
                conn,
            )
            .await?;
        }
        NormalMessage::PublishV5 {
            ref topic_name,
            qos,
            retain,
            ref payload,
            ref subscribe_filter,
            subscribe_qos,
            properties: _,
        } => {
            log::debug!(
                "{:?} received a v5.x publish message from {:?}",
                session.client_id,
                sender
            );
            recv_publish(
                session,
                RecvPublish {
                    topic_name,
                    qos,
                    retain,
                    payload,
                    subscribe_filter,
                    subscribe_qos,
                },
                conn,
            )
            .await?;
        }
    }
    Ok(())
}

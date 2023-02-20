use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flume::Receiver;
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use hashbrown::HashMap;
use mqtt_proto::{
    v5::{Connect, ConnectReasonCode, Header, Packet, PublishProperties},
    Error, Protocol,
};

use crate::state::{ClientId, Executor, GlobalState, InternalMessage};

use super::super::PendingPackets;
use super::{
    packet::{
        common::{
            after_handle_packet, recv_publish, send_error_connack, send_publish, RecvPublish,
            SendPublish,
        },
        connect::{handle_auth, handle_connect, handle_disconnect},
        pingreq::handle_pingreq,
        publish::{handle_puback, handle_pubcomp, handle_publish, handle_pubrec, handle_pubrel},
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
            let session_expiry = Duration::from_secs(session.session_expiry_interval as u64);
            executor.spawn_sleep(session_expiry, {
                let client_id = session.client_id;
                let connected_time = session.connected_time.expect("connected time");
                let global = Arc::clone(&global);
                async move {
                    if let Some(sender) = global.get_client_sender(&client_id) {
                        let msg = InternalMessage::SessionExpired { connected_time };
                        if let Err(err) = sender.send_async((client_id, msg)).await {
                            log::warn!(
                                "send session expired message to {} error: {:?}",
                                client_id,
                                err
                            );
                        }
                    }
                }
            });
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
    header: Header,
    protocol: Protocol,
    timeout_receiver: Receiver<()>,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<Option<(Session, Receiver<(ClientId, InternalMessage)>)>> {
    enum Msg {
        Socket(()),
        Internal((ClientId, InternalMessage)),
    }

    let mut session = Session::new(&global.config, peer);
    let mut receiver = None;
    let mut io_error = None;

    let packet = match Connect::decode_with_protocol(&mut conn, header, protocol)
        .or(async {
            log::info!("connection timeout: {}", peer);
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()).into())
        })
        .await
    {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v5.x connect codec error: {}", err);
            return Err(io::ErrorKind::InvalidData.into());
        }
    };
    handle_connect(
        &mut session,
        &mut receiver,
        packet,
        &mut conn,
        executor,
        global,
    )
    .await?;

    while session.authorizing {
        let packet = match decode_packet(&mut session, &mut conn, global)
            .or(async {
                log::info!("connection timeout: {}", peer);
                let _ = timeout_receiver.recv_async().await;
                Err(Error::IoError(io::ErrorKind::TimedOut, String::new()).into())
            })
            .await?
        {
            Some(packet) => packet,
            None => return Ok(None),
        };
        let auth = match packet {
            Packet::Auth(pkt) => pkt,
            _ => {
                log::info!("Not connected, only AUTH packet is allowed");
                send_error_connack(
                    &mut conn,
                    &mut session,
                    false,
                    ConnectReasonCode::ProtocolError,
                    "",
                )
                .await?;
                return Ok(None);
            }
        };
        handle_auth(
            &mut session,
            &mut receiver,
            auth,
            &mut conn,
            executor,
            global,
        )
        .await?;
    }
    drop(timeout_receiver);

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
        let recv_data = async {
            handle_packet(&mut session, &mut conn, executor, global)
                .await
                .map(Msg::Socket)
        };
        let recv_msg = async {
            receiver
                .recv_async()
                .await
                .map(Msg::Internal)
                .map_err(|_| io::ErrorKind::BrokenPipe.into())
        };
        match recv_data.or(recv_msg).await {
            Ok(Msg::Socket(())) => {}
            Ok(Msg::Internal((sender, msg))) => {
                let is_kick = matches!(msg, InternalMessage::Kick { .. });
                match handle_internal(
                    &mut session,
                    &receiver,
                    sender,
                    msg,
                    Some(&mut conn),
                    global,
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
            Err(err) => {
                // An error in online mode should also check clean_session value
                io_error = Some(err);
                break;
            }
        }
    }

    if !session.disconnected() {
        handle_will(&mut session, &mut conn, executor, global).await?;
    }
    if session.session_expiry_interval == 0 {
        global.remove_client(session.client_id(), session.subscribes().keys());
        if let Some(err) = io_error {
            return Err(err);
        }
    } else {
        // become a offline client, but session keep updating
        global.offline_client(session.client_id());
        session.connected = false;
        session.connection_closed_time = Some(Instant::now());
        return Ok(Some((session, receiver)));
    }

    Ok(None)
}

async fn handle_offline(
    mut session: Session,
    receiver: Receiver<(ClientId, InternalMessage)>,
    global: Arc<GlobalState>,
) {
    let mut conn: Option<Vec<u8>> = None;
    loop {
        let (sender, msg) = match receiver.recv_async().await {
            Ok((sender, msg)) => (sender, msg),
            Err(err) => {
                log::warn!("offline client receive internal message error: {:?}", err);
                break;
            }
        };
        match handle_internal(&mut session, &receiver, sender, msg, conn.as_mut(), &global).await {
            Ok(true) => {
                // Been occupied by newly connected client
                break;
            }
            Ok(false) => {}
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
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    let packet = if let Some(packet) = decode_packet(session, conn, global).await? {
        packet
    } else {
        return Ok(());
    };
    match packet {
        Packet::Auth(pkt) => handle_auth(session, &mut None, pkt, conn, executor, global).await?,
        Packet::Disconnect(pkt) => handle_disconnect(session, pkt, conn, global).await?,
        Packet::Publish(pkt) => handle_publish(session, pkt, conn, global).await?,
        Packet::Puback(pkt) => handle_puback(session, pkt, conn, global).await?,
        Packet::Pubrec(pkt) => handle_pubrec(session, pkt, conn, global).await?,
        Packet::Pubrel(pkt) => handle_pubrel(session, pkt, conn, global).await?,
        Packet::Pubcomp(pkt) => handle_pubcomp(session, pkt, conn, global).await?,
        Packet::Subscribe(pkt) => handle_subscribe(session, pkt, conn, global).await?,
        Packet::Unsubscribe(pkt) => handle_unsubscribe(session, pkt, conn, global).await?,
        Packet::Pingreq => handle_pingreq(session, conn, global).await?,
        _ => return Err(io::ErrorKind::InvalidData.into()),
    }
    after_handle_packet(session, conn).await?;
    Ok(())
}

#[inline]
async fn handle_will<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    _conn: &mut T,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    if let Some(last_will) = session.last_will.as_ref() {
        let delay_interval = last_will.properties.delay_interval.unwrap_or(0);
        if delay_interval == 0 {
            send_will(session, global).await?;
        } else if delay_interval < session.session_expiry_interval {
            executor.spawn_sleep(Duration::from_secs(delay_interval as u64), {
                let client_id = session.client_id;
                let connected_time = session.connected_time.expect("connected time (will)");
                let global = Arc::clone(global);
                async move {
                    if let Some(sender) = global.get_client_sender(&client_id) {
                        let msg = InternalMessage::WillDelayReached { connected_time };
                        if let Err(err) = sender.send_async((client_id, msg)).await {
                            log::warn!(
                                "send will delay reached message to {} error: {:?}",
                                client_id,
                                err
                            );
                        }
                    }
                }
            });
        } else {
            // Handle will in SessionExpired event
        }
    }
    Ok(())
}

/// return if the offline client loop should stop
#[inline]
async fn handle_internal<T: AsyncWrite + Unpin>(
    session: &mut Session,
    receiver: &Receiver<(ClientId, InternalMessage)>,
    sender: ClientId,
    msg: InternalMessage,
    conn: Option<&mut T>,
    global: &Arc<GlobalState>,
) -> io::Result<bool> {
    // FIXME: call receiver.try_recv() to clear the channel, if the pending
    // queue is full, set a marker to the global state so that the sender stop
    // sending qos0 messages to this client.
    let mut stop = false;
    match msg {
        InternalMessage::OnlineV3 { .. } => {
            log::warn!("take over v5.x session by v3.x client is not allowed");
        }
        InternalMessage::OnlineV5 { sender } => {
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
        InternalMessage::PublishV3 {
            ref topic_name,
            qos,
            mut retain,
            ref payload,
            ref subscribe_filter,
            subscribe_qos,
        } => {
            log::debug!(
                "{:?} received publish message from {:?}",
                session.client_id,
                sender
            );
            if let Some(sub) = session.subscribes.get(subscribe_filter) {
                if !global.config.retain_available || !sub.options.retain_as_published {
                    retain = false;
                }
                recv_publish(
                    session,
                    RecvPublish {
                        topic_name,
                        qos,
                        retain,
                        payload,
                        subscribe_filter,
                        subscribe_qos,
                        properties: None,
                    },
                    conn,
                )
                .await?;
            }
        }
        InternalMessage::PublishV5 {
            ref topic_name,
            qos,
            mut retain,
            ref payload,
            ref subscribe_filter,
            subscribe_qos,
            ref properties,
        } => {
            log::debug!(
                "{:?} received publish message from {:?}",
                session.client_id,
                sender
            );
            if let Some(sub) = session.subscribes.get(subscribe_filter) {
                if !global.config.retain_available || !sub.options.retain_as_published {
                    retain = false;
                }
                recv_publish(
                    session,
                    RecvPublish {
                        topic_name,
                        qos,
                        retain,
                        payload,
                        subscribe_filter,
                        subscribe_qos,
                        properties: Some(properties),
                    },
                    conn,
                )
                .await?;
            }
        }
        InternalMessage::Kick { reason } => {
            log::info!(
                "kick {}, reason: {}, offline: {}, network: {}",
                session.client_identifier,
                reason,
                session.disconnected,
                conn.is_some(),
            );
            stop = conn.is_some();
        }
        InternalMessage::WillDelayReached { connected_time } => {
            log::debug!("client {} will delay reached", session.client_identifier);
            if !session.connected && session.connected_time == Some(connected_time) {
                send_will(session, global).await?;
            }
        }
        InternalMessage::SessionExpired { connected_time } => {
            log::debug!("client {} session expired", session.client_identifier);
            if !session.connected && session.connected_time == Some(connected_time) {
                send_will(session, global).await?;
                global.remove_client(session.client_id, session.subscribes.keys());
                stop = true;
            }
        }
    }
    Ok(stop)
}

#[inline]
async fn send_will(session: &mut Session, global: &Arc<GlobalState>) -> io::Result<()> {
    if let Some(last_will) = session.last_will.take() {
        let properties = last_will.properties;
        let _matched_len = send_publish(
            session,
            SendPublish {
                qos: last_will.qos,
                retain: last_will.retain,
                topic_name: &last_will.topic_name,
                payload: &last_will.payload,
                properties: &PublishProperties {
                    payload_is_utf8: properties.payload_is_utf8,
                    message_expiry_interval: properties.message_expiry_interval,
                    topic_alias: None,
                    response_topic: properties.response_topic,
                    correlation_data: properties.correlation_data,
                    user_properties: properties.user_properties,
                    subscription_id: None,
                    content_type: properties.content_type,
                },
            },
            global,
        )
        .await;
    }
    Ok(())
}

#[inline]
async fn decode_packet<T: AsyncRead + AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<Option<Packet>> {
    // TODO: Decode Header first for more detailed error report.
    let packet = match Packet::decode_async(conn).await {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v5.x codec error: {}", err);
            if err.is_eof() {
                if !session.disconnected {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                } else {
                    return Ok(None);
                }
            } else {
                // FIXME: return error reason info with connack/disconnect packet
                return Err(io::ErrorKind::InvalidData.into());
            }
        }
    };
    Ok(Some(packet))
}

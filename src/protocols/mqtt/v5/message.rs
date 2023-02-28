use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ahash::AHashSet;
use bytes::Bytes;
use flume::{r#async::SendSink, Receiver, Sender};
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt, Stream,
};
use futures_sink::Sink;
use mqtt_proto::{
    v5::{
        Auth, AuthProperties, AuthReasonCode, Connect, ConnectReasonCode, Header, Packet,
        PollPacket, PollPacketState, PublishProperties, VarBytes,
    },
    Error, Protocol, QoS,
};

use crate::state::{
    ClientId, ClientReceiver, ControlMessage, Executor, GlobalState, NormalMessage,
};

use super::{
    packet::{
        common::{after_handle_packet, build_error_connack, handle_pendings, write_packet},
        connect::{handle_auth, handle_connect, handle_disconnect, session_connect},
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
            let session_expiry = Duration::from_secs(session.session_expiry_interval as u64);
            executor.spawn_sleep(session_expiry, {
                let client_id = session.client_id;
                let connected_time = session.connected_time.expect("connected time");
                let global = Arc::clone(&global);
                async move {
                    if let Some(sender) = global.get_client_control_sender(&client_id) {
                        let msg = ControlMessage::SessionExpired { connected_time };
                        if let Err(err) = sender.send_async(msg).await {
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
) -> io::Result<Option<(Session, ClientReceiver)>> {
    let mut session = Session::new(&global.config, peer);
    let mut receiver = None;

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

    // * Scram challenge only need 1 round.
    // * Kerberos challenge need 2 rounds.
    let mut round_quota = 2;
    while session.authorizing && round_quota > 0 {
        round_quota -= 1;

        let packet = async {
            Packet::decode_async(&mut conn)
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
        }
        .or(async {
            log::info!("connection timeout: {}", peer);
            let _ = timeout_receiver.recv_async().await;
            Err(io::Error::from(io::ErrorKind::TimedOut))
        })
        .await?;
        let auth = match packet {
            Packet::Auth(pkt) => pkt,
            _ => {
                log::info!("Not connected, only AUTH packet is allowed");
                let err_pkt =
                    build_error_connack(&mut session, false, ConnectReasonCode::ProtocolError, "");
                write_packet(session.client_id, &mut conn, &err_pkt).await?;
                return Ok(None);
            }
        };

        match handle_auth(&mut session, auth, global) {
            Ok((AuthReasonCode::Success, server_final)) => {
                session_connect(
                    &mut session,
                    &mut receiver,
                    Some(server_final),
                    &mut conn,
                    executor,
                    global,
                )
                .await?;
            }
            Ok((AuthReasonCode::ContinueAuthentication, server_first)) => {
                let auth_method = session.auth_method.as_ref().expect("auth method");
                let rv_packet = Auth {
                    reason_code: AuthReasonCode::ContinueAuthentication,
                    properties: AuthProperties {
                        auth_method: Some(Arc::clone(auth_method)),
                        auth_data: Some(Bytes::from(server_first)),
                        reason_string: None,
                        user_properties: Vec::new(),
                    },
                };
                write_packet(session.client_id, &mut conn, &rv_packet.into()).await?;
            }
            Ok((AuthReasonCode::ReAuthentication, _)) => unreachable!(),
            Err(err_pkt) => {
                write_packet(session.client_id, &mut conn, &err_pkt).await?;
                return Ok(None);
            }
        }
    }
    drop(timeout_receiver);

    if !session.connected() {
        log::info!("{} not connected", session.peer());
        return Err(io::ErrorKind::InvalidData.into());
    }
    for packet in after_handle_packet(&mut session) {
        write_packet(session.client_id, &mut conn, &packet).await?;
    }

    let receiver = receiver.expect("receiver");
    log::info!(
        "executor {:03}, {} connected, total {} clients ({} online) ",
        executor.id(),
        session.peer(),
        global.clients_count(),
        global.online_clients_count(),
    );

    let online_loop = OnlineLoop {
        session: &mut session,
        _executor: executor,
        global,
        receiver: &receiver,
        conn: &mut conn,

        packet_state: PollPacketState::default(),
        session_state_sender: None,
        write_packets_max: 4,
        write_packets: VecDeque::with_capacity(4),
        write_data: None,
    };
    let io_error = online_loop.await;

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

pub struct OnlineLoop<'a, C, E> {
    session: &'a mut Session,
    _executor: &'a E,
    global: &'a Arc<GlobalState>,
    receiver: &'a ClientReceiver,
    conn: &'a mut C,

    packet_state: PollPacketState,
    session_state_sender: Option<SendSink<'static, SessionState>>,
    write_packets_max: usize,
    // TODO: call shrink_to() when capacity is too large
    write_packets: VecDeque<Packet>,
    write_data: Option<(VarBytes, usize)>,
}

impl<'a, C, E> Future for OnlineLoop<'a, C, E>
where
    C: AsyncRead + AsyncWrite + Unpin,
    E: Executor,
{
    type Output = Option<io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // [Goals]:
        //   * Forbid use too much memory
        //   * Forbid use too much cpu
        //   * Forbid loose message
        //   * Forbid message delay
        //
        // [Terms]:
        //   * Insert data to current state is producer
        //   * Remove data from current state is consumer
        //
        // [Rules]:
        //   * Consumer code must place after producer code
        //   * When current state is full must not run producer code

        let OnlineLoop {
            ref mut session,
            _executor,
            global,
            receiver,
            ref mut conn,
            packet_state,
            session_state_sender,
            write_packets_max,
            write_packets,
            write_data,
        } = self.get_mut();

        // Send SessionState to new connection (been taken over)
        //   * Consume: [session_state_sender]
        if let Some(mut send_sink) = session_state_sender.take() {
            match Pin::new(&mut send_sink).poll_ready(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(_)) => {
                    // channel disconnected, cancel takeover
                    log::info!("the connection want take over current session already ended");
                }
                Poll::Pending => {
                    // channel is full
                    *session_state_sender = Some(send_sink);
                    return Poll::Pending;
                }
            }
            let old_state = session.build_state(receiver.clone());
            if Pin::new(&mut send_sink).start_send(old_state).is_err() {
                // channel disconnected, cancel takeover
                log::info!("the connection want take over current session already ended");
            }
            return Poll::Ready(None);
        }

        let mut task_quota: usize = 4;

        let mut read_pending = false;
        let mut write_pending = false;
        let mut control_receiver_pending = false;
        let mut normal_receiver_pending = false;
        let mut broadcast_pending_clients: AHashSet<ClientId> = AHashSet::new();

        while !session.disconnected() && task_quota > 0 {
            task_quota -= 1;

            // ==================
            // ==== Producer ====
            // ==================

            // Read data from client connection
            //   * Produce to: [write_packets, broadcast_packets, external_request]
            if !read_pending {
                while write_packets.len() < *write_packets_max
                    && session.broadcast_packets_cnt < session.broadcast_packets_max
                {
                    // TODO: Decode Header first for more detailed error report.
                    let mut poll_packet = PollPacket::new(packet_state, conn);
                    let (_total, packet) = match Pin::new(&mut poll_packet).poll(cx) {
                        Poll::Ready(Ok(output)) => output,
                        Poll::Ready(Err(err)) => {
                            log::debug!("mqtt v5.x codec error: {}", err);
                            return if err.is_eof() {
                                if !session.disconnected {
                                    Poll::Ready(Some(io::ErrorKind::UnexpectedEof.into()))
                                } else {
                                    Poll::Ready(None)
                                }
                            } else {
                                // FIXME: return error reason info with connack/disconnect packet
                                Poll::Ready(Some(io::ErrorKind::InvalidData.into()))
                            };
                        }
                        Poll::Pending => {
                            read_pending = true;
                            break;
                        }
                    };
                    // TODO: handle packet
                    //   * insert packet into write_packets
                    //   * insert packet into broadcast_packets

                    match packet {
                        Packet::Disconnect(pkt) => {
                            if let Err(err_pkt) = handle_disconnect(session, pkt) {
                                // FIXME: must ensure error packet finally written to client in several seconds.
                                write_packets.push_back(err_pkt);
                            }
                        }
                        Packet::Publish(pkt) => {
                            match handle_publish(session, pkt, global) {
                                // QoS0
                                Ok(None) => {}
                                // QoS1, QoS2
                                Ok(Some(packet)) => write_packets.push_back(packet),
                                Err(err_pkt) => write_packets.push_back(err_pkt),
                            }
                        }
                        Packet::Puback(pkt) => handle_puback(session, pkt),
                        Packet::Pubrec(pkt) => write_packets.push_back(handle_pubrec(session, pkt)),
                        Packet::Pubrel(pkt) => write_packets.push_back(handle_pubrel(session, pkt)),
                        Packet::Pubcomp(pkt) => handle_pubcomp(session, pkt),
                        Packet::Subscribe(pkt) => match handle_subscribe(session, pkt, global) {
                            Ok(packets) => write_packets.extend(packets),
                            Err(err_pkt) => write_packets.push_back(err_pkt),
                        },
                        Packet::Unsubscribe(pkt) => {
                            write_packets.push_back(handle_unsubscribe(session, pkt, global));
                        }
                        Packet::Pingreq => write_packets.push_back(handle_pingreq(session)),
                        _ => return Poll::Ready(Some(io::ErrorKind::InvalidData.into())),
                    }
                }
            }

            // Receive control messages
            //   * Produce to: [broadcast_packets, session_state_sender]
            if !control_receiver_pending {
                let mut recv_stream = receiver.control.stream();
                while session.broadcast_packets_cnt < session.broadcast_packets_max {
                    let msg = match Pin::new(&mut recv_stream).poll_next(cx) {
                        Poll::Ready(Some(output)) => output,
                        Poll::Ready(None) => {
                            log::error!("control senders all dropped by {}", session.client_id);
                            return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                        }
                        Poll::Pending => {
                            control_receiver_pending = true;
                            break;
                        }
                    };
                    let (stop, sender_opt) = handle_control(session, msg, global);
                    *session_state_sender = sender_opt.map(|sender| sender.into_sink());
                    if stop {
                        return Poll::Ready(None);
                    }
                }
            }

            // Receive normal messages
            //   * Produce to: [write_packets]
            if !normal_receiver_pending {
                let mut recv_stream = receiver.normal.stream();
                // FIXME: drop message when pending queue are full
                while write_packets.len() < *write_packets_max {
                    let (sender_id, msg) = match Pin::new(&mut recv_stream).poll_next(cx) {
                        Poll::Ready(Some(output)) => output,
                        Poll::Ready(None) => {
                            log::error!("normal senders all dropped by {}", session.client_id);
                            return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                        }
                        Poll::Pending => {
                            normal_receiver_pending = true;
                            break;
                        }
                    };

                    if let Some((final_qos, packet_opt)) =
                        handle_normal(session, sender_id, msg, global)
                    {
                        if let Some(packet) = packet_opt {
                            write_packets.push_back(packet);
                        }
                        if final_qos != QoS::Level0 {
                            write_packets.extend(handle_pendings(session));
                        }
                    }
                }
            }

            // ==================
            // ==== Cunsomer ====
            // ==================

            // Write packets to client connection
            //   * Consume: [write_packets]
            if !write_pending {
                if let Some((data, idx)) = write_data.take() {
                    match Pin::new(&mut *conn).poll_write(cx, &data.as_slice()[idx..]) {
                        Poll::Ready(Ok(size)) => {
                            if idx + size < data.as_slice().len() {
                                *write_data = Some((data, idx + size));
                            }
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Some(err)),
                        Poll::Pending => {
                            *write_data = Some((data, idx));
                            write_pending = true;
                        }
                    }
                }
                if write_data.is_none() {
                    while let Some(packet) = write_packets.pop_front() {
                        let data = match packet.encode() {
                            Ok(data) => data,
                            Err(err) => return Poll::Ready(Some(io::Error::from(err))),
                        };
                        match Pin::new(&mut *conn).poll_write(cx, data.as_slice()) {
                            Poll::Ready(Ok(size)) => {
                                if size < data.as_slice().len() {
                                    *write_data = Some((data, size));
                                    break;
                                }
                            }
                            Poll::Ready(Err(err)) => return Poll::Ready(Some(err)),
                            Poll::Pending => {
                                *write_data = Some((data, 0));
                                write_pending = true;
                                break;
                            }
                        }
                    }
                }
            }

            // Broadcast packets to matched sessions
            //   * Consume from: [broadcast_packets]
            let mut broadcast_pending =
                broadcast_pending_clients.len() == session.broadcast_packets.len();
            if !broadcast_pending {
                session.broadcast_packets.retain(|client_id, msgs| {
                    if broadcast_pending_clients.contains(client_id) {
                        return true;
                    }
                    if let Some(sender) = global.get_client_normal_sender(client_id) {
                        let mut send_sink = sender.into_sink();
                        while let Some(msg) = msgs.pop_front() {
                            match Pin::new(&mut send_sink).poll_ready(cx) {
                                Poll::Ready(Ok(())) => {}
                                Poll::Ready(Err(_)) => {
                                    // channel disconnected
                                    break;
                                }
                                Poll::Pending => {
                                    // channel is full
                                    msgs.push_front(msg);
                                    broadcast_pending_clients.insert(*client_id);
                                    return true;
                                }
                            }
                            if Pin::new(&mut send_sink)
                                .start_send((session.client_id, msg))
                                .is_err()
                            {
                                log::info!("send publish to disconnected client: {}", client_id);
                                break;
                            }
                        }
                    }
                    broadcast_pending_clients.remove(client_id);
                    false
                });

                if broadcast_pending_clients.len() == session.broadcast_packets.len() {
                    broadcast_pending = true;
                }
            }

            // FIXME: handle extension request here

            // Check if all pending
            if read_pending
                && control_receiver_pending
                && normal_receiver_pending
                && write_pending
                && broadcast_pending
            {
                return Poll::Pending;
            }
        }

        if session.disconnected() {
            Poll::Ready(None)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn handle_offline(mut session: Session, receiver: ClientReceiver, global: Arc<GlobalState>) {
    loop {
        tokio::select! {
            result = receiver.control.recv_async() => match result {
                Ok(msg) => {
                    let (stop, sender_opt) = handle_control(&mut session, msg, &global);
                    if let Some(sender) = sender_opt {
                        let old_state = session.build_state(receiver);
                        if let Err(err) = sender.send_async(old_state).await {
                            log::warn!("offline send session state failed: {err:?}");
                        }
                        break;
                    }
                    if stop {
                        break;
                    }
                }
                Err(err) => {
                    log::warn!("offline client receive control message error: {err:?}");
                    break;
                }
            },
            result = receiver.normal.recv_async() => match result {
                Ok((sender, msg)) => {
                    let _ = handle_normal(&mut session, sender, msg, &global);
                },
                Err(err) => {
                    log::warn!("offline client receive normal message error: {err:?}");
                    break;
                }
            }
        };
    }
    log::debug!("offline client finished: {:?}", session.client_id());
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
            send_will(session, global);
        } else if delay_interval < session.session_expiry_interval {
            executor.spawn_sleep(Duration::from_secs(delay_interval as u64), {
                let client_id = session.client_id;
                let connected_time = session.connected_time.expect("connected time (will)");
                let global = Arc::clone(global);
                async move {
                    if let Some(sender) = global.get_client_control_sender(&client_id) {
                        let msg = ControlMessage::WillDelayReached { connected_time };
                        if let Err(err) = sender.send_async(msg).await {
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

#[inline]
fn handle_control(
    session: &mut Session,
    msg: ControlMessage,
    global: &Arc<GlobalState>,
) -> (bool, Option<Sender<SessionState>>) {
    let mut stop = false;
    match msg {
        ControlMessage::OnlineV3 { .. } => {
            log::warn!("take over v5.x session by v3.x client is not allowed");
        }
        ControlMessage::OnlineV5 { sender } => return (false, Some(sender)),
        ControlMessage::Kick { reason } => {
            log::info!(
                "kick {}, reason: {}, online: {}",
                session.client_identifier,
                reason,
                !session.disconnected,
            );
            stop = !session.disconnected;
        }
        ControlMessage::SessionExpired { connected_time } => {
            log::debug!("client {} session expired", session.client_identifier);
            if !session.connected && session.connected_time == Some(connected_time) {
                send_will(session, global);
                global.remove_client(session.client_id, session.subscribes.keys());
                stop = true;
            }
        }
        ControlMessage::WillDelayReached { connected_time } => {
            log::debug!("client {} will delay reached", session.client_identifier);
            if !session.connected && session.connected_time == Some(connected_time) {
                send_will(session, global);
            }
        }
    }
    (stop, None)
}

/// Return packet to be write to client connection, return None means the client
/// currently unsubscribed or received a QoS0 message in offline.
#[inline]
fn handle_normal(
    session: &mut Session,
    sender: ClientId,
    msg: NormalMessage,
    global: &Arc<GlobalState>,
) -> Option<(QoS, Option<Packet>)> {
    match msg {
        NormalMessage::PublishV3 {
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
                )
            } else {
                None
            }
        }
        NormalMessage::PublishV5 {
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
                )
            } else {
                None
            }
        }
    }
}

// TODO: change to broadcast_will
#[inline]
fn send_will(session: &mut Session, global: &Arc<GlobalState>) {
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
        );
    }
}

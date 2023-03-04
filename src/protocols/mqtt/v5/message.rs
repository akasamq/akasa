use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::{
    r#async::{RecvStream, SendSink},
    Receiver, Sender,
};
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

    let mut taken_over = false;
    let online_loop = OnlineLoop {
        session: &mut session,
        _executor: executor,
        global,
        receiver: &receiver,
        control_stream: receiver.control.stream(),
        normal_stream: receiver.normal.stream(),
        conn: &mut conn,

        read_unfinish: false,
        normal_stream_unfinish: false,

        packet_state: PollPacketState::default(),
        write_packets_max: 4,
        write_packets: VecDeque::with_capacity(4),
        session_state_sender: None,
        taken_over: &mut taken_over,
    };
    let io_error = online_loop.await;
    if taken_over {
        return Ok(None);
    }

    log::debug!(
        "[{}] online loop finished, disconnected={}",
        session.client_id,
        session.disconnected
    );
    if !session.disconnected() {
        log::debug!("[{}] handling will...", session.client_id);
        handle_will(&mut session, &mut conn, executor, global).await?;
        for (target_id, info) in session.broadcast_packets.drain() {
            for msg in info.msgs {
                if let Err(err) = info
                    .sink
                    .sender()
                    .send_async((session.client_id, msg))
                    .await
                {
                    log::warn!(
                        "[{}] after online loop, send broadcast message to {} failed: {:?}",
                        session.client_id,
                        target_id,
                        err
                    )
                }
            }
        }
        log::debug!("[{}] all will sent", session.client_id);
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

#[derive(Debug, Clone, Copy, Default)]
struct Pendings {
    read: bool,
    control_message: bool,
    normal_message: bool,

    write: bool,
    broadcast: bool,
}

#[derive(Debug)]
enum WritePacket {
    Packet(Packet),
    Data((VarBytes, usize)),
}

impl From<Packet> for WritePacket {
    fn from(pkt: Packet) -> WritePacket {
        WritePacket::Packet(pkt)
    }
}

struct OnlineLoop<'a, C, E> {
    session: &'a mut Session,
    _executor: &'a E,
    global: &'a Arc<GlobalState>,
    receiver: &'a ClientReceiver,
    control_stream: RecvStream<'a, ControlMessage>,
    normal_stream: RecvStream<'a, (ClientId, NormalMessage)>,
    conn: &'a mut C,

    read_unfinish: bool,
    normal_stream_unfinish: bool,

    packet_state: PollPacketState,
    session_state_sender: Option<(SendSink<'static, SessionState>, bool)>,
    write_packets_max: usize,
    // TODO: call shrink_to() when capacity is too large
    write_packets: VecDeque<WritePacket>,
    taken_over: &'a mut bool,
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
        //   * Forbid use too much CPU
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
        //   * Consumer finished should consider wake up Producer code

        let OnlineLoop {
            ref mut session,
            _executor,
            global,
            receiver,
            ref mut control_stream,
            ref mut normal_stream,
            ref mut conn,

            ref mut read_unfinish,
            ref mut normal_stream_unfinish,

            packet_state,
            session_state_sender,
            write_packets_max,
            write_packets,
            taken_over,
        } = self.get_mut();

        log::debug!("@@@@ [{}] poll()", session.client_id);

        // Send SessionState to new connection (been taken over)
        //   * Consume: [session_state_sender]
        if let Some((mut send_sink, flushing)) = session_state_sender.take() {
            if !flushing {
                match Pin::new(&mut send_sink).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        // channel disconnected, cancel takeover
                        log::info!("the connection want take over current session already ended");
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Poll::Pending => {
                        // channel is full
                        *session_state_sender = Some((send_sink, false));
                        return Poll::Pending;
                    }
                }
                let old_state = session.build_state(receiver.clone());
                if Pin::new(&mut send_sink).start_send(old_state).is_err() {
                    // channel disconnected, cancel takeover
                    log::info!("the connection want take over current session already ended");
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            return match Pin::new(&mut send_sink).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    log::info!("[{}] current session been taken over", session.client_id);
                    **taken_over = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Err(_)) => {
                    log::info!("the connection want take over current session already ended");
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => {
                    // channel is full
                    *session_state_sender = Some((send_sink, true));
                    Poll::Pending
                }
            };
        }

        let mut pendings = Pendings::default();
        let mut have_write = false;
        let mut have_broadcast = false;

        log::debug!(
            "[{}] write_packets={}, broadcast_packets={}, ",
            session.client_id,
            write_packets.len(),
            session.broadcast_packets_cnt,
        );

        // ==================
        // ==== Producer ====
        // ==================

        // Read data from client connection
        //   * Produce to: [write_packets, broadcast_packets, external_request]
        loop {
            if write_packets.len() >= *write_packets_max
                || session.broadcast_packets_cnt >= session.broadcast_packets_max
            {
                *read_unfinish = true;
                break;
            } else {
                *read_unfinish = false;
            }

            log::debug!(
                "[{}] going to read, write_packets.len() = {}, broadcast_packets_cnt = {}",
                session.client_id,
                write_packets.len(),
                session.broadcast_packets_cnt,
            );
            // TODO: Decode Header first for more detailed error report.
            let mut poll_packet = PollPacket::new(packet_state, conn);
            let (_total, packet) = match Pin::new(&mut poll_packet).poll(cx) {
                Poll::Ready(Ok(output)) => {
                    *packet_state = PollPacketState::default();
                    output
                }
                Poll::Ready(Err(err)) => {
                    log::debug!("[{}] mqtt v5.x codec error: {}", session.client_id, err);
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
                    log::debug!("[{}] read pending, {:?}", session.client_id, packet_state);
                    *read_unfinish = false;
                    pendings.read = true;
                    break;
                }
            };
            // TODO: handle packet
            //   * insert packet into write_packets
            //   * insert packet into broadcast_packets

            log::debug!(
                "[{}] success decode MQTT v5 packet: {:?}",
                session.client_id,
                packet
            );
            match packet {
                Packet::Disconnect(pkt) => {
                    if let Err(err_pkt) = handle_disconnect(session, pkt) {
                        // FIXME: must ensure error packet finally written to client in several seconds.
                        write_packets.push_back(err_pkt.into());
                    }
                }
                Packet::Publish(pkt) => {
                    match handle_publish(session, pkt, global) {
                        // QoS0
                        Ok(None) => {}
                        // QoS1, QoS2
                        Ok(Some(packet)) => write_packets.push_back(packet.into()),
                        Err(err_pkt) => write_packets.push_back(err_pkt.into()),
                    }
                }
                Packet::Puback(pkt) => handle_puback(session, pkt),
                Packet::Pubrec(pkt) => write_packets.push_back(handle_pubrec(session, pkt).into()),
                Packet::Pubrel(pkt) => write_packets.push_back(handle_pubrel(session, pkt).into()),
                Packet::Pubcomp(pkt) => handle_pubcomp(session, pkt),
                Packet::Subscribe(pkt) => match handle_subscribe(session, pkt, global) {
                    Ok(packets) => {
                        write_packets.extend(packets.into_iter().map(WritePacket::Packet))
                    }
                    Err(err_pkt) => write_packets.push_back(err_pkt.into()),
                },
                Packet::Unsubscribe(pkt) => {
                    write_packets.push_back(handle_unsubscribe(session, pkt, global).into());
                }
                Packet::Pingreq => write_packets.push_back(handle_pingreq(session).into()),
                _ => return Poll::Ready(Some(io::ErrorKind::InvalidData.into())),
            }
            let pending_packets = after_handle_packet(session);
            write_packets.extend(pending_packets.into_iter().map(WritePacket::Packet));
        }

        // Receive control messages
        //   * Produce to: [broadcast_packets, session_state_sender]
        loop {
            // send_will() function will insert broadcast_packets, but it's OK.
            let msg = match Pin::new(&mut *control_stream).poll_next(cx) {
                Poll::Ready(Some(output)) => output,
                Poll::Ready(None) => {
                    log::error!("control senders all dropped by {}", session.client_id);
                    return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                }
                Poll::Pending => {
                    pendings.control_message = true;
                    break;
                }
            };
            log::debug!(
                "[{}] handling control message: {:?}",
                session.client_id,
                msg
            );
            let (stop, sender_opt) = handle_control(session, msg, global);
            if let Some(sender) = sender_opt {
                log::debug!("[{}] yield because session take over", session.client_id);
                *session_state_sender = Some((sender.into_sink(), false));
                // Since it's high priority, we just return here so session start take over process.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if stop {
                return Poll::Ready(None);
            }
        }

        // Receive normal messages
        //   * Produce to: [write_packets]
        log::debug!("[{}] reading normal message", session.client_id);
        // FIXME: drop message when pending queue are full
        loop {
            if write_packets.len() >= *write_packets_max {
                *normal_stream_unfinish = true;
                break;
            } else {
                *normal_stream_unfinish = false;
            }

            let (sender_id, msg) = match Pin::new(&mut *normal_stream).poll_next(cx) {
                Poll::Ready(Some(output)) => output,
                Poll::Ready(None) => {
                    log::error!("normal senders all dropped by {}", session.client_id);
                    return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                }
                Poll::Pending => {
                    log::debug!("[{}] normal receiver is pending", session.client_id);
                    pendings.normal_message = true;
                    break;
                }
            };

            log::debug!(
                "[{}] received a normal message from [{}], {:?}",
                session.client_id,
                sender_id,
                msg,
            );
            if let Some((final_qos, packet_opt)) = handle_normal(session, sender_id, msg, global) {
                if let Some(packet) = packet_opt {
                    write_packets.push_back(packet.into());
                }
                if final_qos != QoS::Level0 {
                    let pending_packets = handle_pendings(session);
                    if !pending_packets.is_empty() {
                        write_packets.extend(pending_packets.into_iter().map(WritePacket::Packet));
                    }
                }
            }
        }

        // ==================
        // ==== Consumer ====
        // ==================

        // Write packets to client connection
        //   * Consume: [write_packets]
        while let Some(write_packet) = write_packets.pop_front() {
            log::debug!("[{}] encode packet: {:?}", session.client_id, write_packet);
            let (data, mut idx) = match write_packet {
                WritePacket::Packet(pkt) => match pkt.encode() {
                    Ok(data) => (data, 0),
                    Err(err) => return Poll::Ready(Some(io::Error::from(err))),
                },
                WritePacket::Data((data, idx)) => (data, idx),
            };
            match Pin::new(&mut *conn).poll_write(cx, data.as_slice()) {
                Poll::Ready(Ok(size)) => {
                    have_write = true;
                    log::debug!("[{}] write {} bytes data", session.client_id, size);
                    idx += size;
                    if idx < data.as_slice().len() {
                        write_packets.push_front(WritePacket::Data((data, idx)));
                        break;
                    }
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Some(err)),
                Poll::Pending => {
                    write_packets.push_front(WritePacket::Data((data, idx)));
                    pendings.write = true;
                    break;
                }
            }
        }
        if !pendings.write {
            match Pin::new(&mut *conn).poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Some(err)),
                Poll::Pending => pendings.write = true,
            }
        }

        // Broadcast packets to matched sessions
        //   * Consume from: [broadcast_packets]
        log::debug!(
            "[{}] broadcast_packets.len() = {}",
            session.client_id,
            session.broadcast_packets.len(),
        );
        session.broadcast_packets.retain(|client_id, info| {
            log::debug!(
                "[{}] handling broadcast: flushed={}, msgs={:?}",
                session.client_id,
                info.flushed,
                info.msgs
            );
            if !info.flushed {
                match Pin::new(&mut info.sink).poll_flush(cx) {
                    Poll::Ready(Ok(())) => info.flushed = true,
                    Poll::Ready(Err(_)) => {
                        session.broadcast_packets_cnt -= info.msgs.len();
                        return false;
                    }
                    Poll::Pending => {
                        log::debug!(
                            "[{}] sending broadcast to [{}] RETRY NOT FLUSHED",
                            session.client_id,
                            client_id,
                        );
                        return true;
                    }
                }
            }
            while let Some(msg) = info.msgs.pop_front() {
                match Pin::new(&mut info.sink).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        // channel disconnected
                        session.broadcast_packets_cnt -= info.msgs.len() + 1;
                        break;
                    }
                    Poll::Pending => {
                        // channel is full
                        info.msgs.push_front(msg);
                        log::debug!("target client channel is pending: [{}]", client_id);
                        return true;
                    }
                }
                log::debug!(
                    "[{}] sending broadcast to [{}] {:?}",
                    session.client_id,
                    client_id,
                    msg
                );
                if Pin::new(&mut info.sink)
                    .start_send((session.client_id, msg))
                    .is_err()
                {
                    log::info!("send publish to disconnected client: {}", client_id);
                    session.broadcast_packets_cnt -= info.msgs.len() + 1;
                    break;
                } else {
                    have_broadcast = true;
                }

                session.broadcast_packets_cnt -= 1;
                match Pin::new(&mut info.sink).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        log::debug!(
                            "[{}] sending broadcast to [{}] SUCCESS",
                            session.client_id,
                            client_id,
                        );
                    }
                    Poll::Ready(Err(_)) => {
                        session.broadcast_packets_cnt -= info.msgs.len();
                        break;
                    }
                    Poll::Pending => {
                        log::debug!(
                            "[{}] sending broadcast to [{}] NOT FLUSHED",
                            session.client_id,
                            client_id,
                        );
                        info.flushed = false;
                        break;
                    }
                }
            }
            !info.flushed
        });

        // FIXME: handle extension request here

        if session.disconnected() {
            return Poll::Ready(None);
        }

        // Check if all pending
        log::debug!("[{}] {:?}", session.client_id, pendings);
        log::debug!(
            "[{}] write_packets={}, broadcast_packets={}, ",
            session.client_id,
            write_packets.len(),
            session.broadcast_packets_cnt,
        );
        log::debug!(
            "[{}] read_unfinish={}, normal_stream_unfinish={}",
            session.client_id,
            read_unfinish,
            normal_stream_unfinish
        );

        if pendings.read
            && pendings.control_message
            && pendings.normal_message
            && (pendings.write || write_packets.is_empty())
            && (pendings.broadcast || session.broadcast_packets_cnt == 0)
        {
            log::debug!("[{}] return pending", session.client_id);
            return Poll::Pending;
        }

        if have_write && (*read_unfinish || *normal_stream_unfinish) {
            log::debug!(
                "[{}] yield because write processed (producer unfinish)",
                session.client_id
            );
            *read_unfinish = false;
            *normal_stream_unfinish = false;
            cx.waker().wake_by_ref();
        } else if have_broadcast && *normal_stream_unfinish {
            log::debug!(
                "[{}] yield because broadcast processed (producer unfinish)",
                session.client_id
            );
            *normal_stream_unfinish = false;
            cx.waker().wake_by_ref();
        } else {
            log::debug!("[{}] NOT yield", session.client_id);
        }
        Poll::Pending
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
                "kick \"{}\", reason: {}, online: {}",
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
                "[{}] received v3 publish message from {}",
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
                "[{}] received v5 publish message from {}, msg: {:?}",
                session.client_id,
                sender,
                msg
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

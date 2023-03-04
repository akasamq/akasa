use std::collections::VecDeque;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::{Receiver, Sender};
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use hashbrown::HashMap;
use mqtt_proto::{
    v5::{
        Auth, AuthProperties, AuthReasonCode, Connect, ConnectReasonCode, Header, Packet,
        PollPacketState, PublishProperties, VarBytes,
    },
    Error, Protocol, QoS,
};

use crate::protocols::mqtt::{
    BroadcastPackets, OnlineLoop, OnlineSession, PendingPackets, WritePacket,
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

    if !session.connected {
        log::info!("{} not connected", session.peer);
        return Err(io::ErrorKind::InvalidData.into());
    }
    for packet in after_handle_packet(&mut session) {
        write_packet(session.client_id, &mut conn, &packet).await?;
    }

    let receiver = receiver.expect("receiver");
    log::info!(
        "executor {:03}, {} connected, total {} clients ({} online) ",
        executor.id(),
        session.peer,
        global.clients_count(),
        global.online_clients_count(),
    );

    let mut taken_over = false;
    let online_loop = OnlineLoop::new(
        &mut session,
        global,
        &receiver,
        receiver.control.stream(),
        receiver.normal.stream(),
        &mut conn,
        &mut taken_over,
        PollPacketState::default(),
    );
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

impl OnlineSession for Session {
    type Packet = Packet;
    type VarBytes = VarBytes;
    type SessionState = SessionState;

    fn client_id(&self) -> ClientId {
        self.client_id
    }
    fn disconnected(&self) -> bool {
        self.disconnected
    }
    fn build_state(&mut self, receiver: ClientReceiver) -> Self::SessionState {
        let mut broadcast_packets = HashMap::new();
        let mut pending_packets = PendingPackets::new(0, 0, 0);
        let mut qos2_pids = HashMap::new();
        let mut subscribes = HashMap::new();
        mem::swap(&mut self.broadcast_packets, &mut broadcast_packets);
        mem::swap(&mut self.pending_packets, &mut pending_packets);
        mem::swap(&mut self.qos2_pids, &mut qos2_pids);
        mem::swap(&mut self.subscribes, &mut subscribes);
        SessionState {
            client_id: self.client_id,
            receiver,
            protocol: self.protocol,

            broadcast_packets_cnt: self.broadcast_packets_cnt,
            broadcast_packets,

            server_packet_id: self.server_packet_id,
            pending_packets,
            qos2_pids,
            subscribes,
        }
    }

    fn consume_broadcast(&mut self, count: usize) {
        self.broadcast_packets_cnt -= count;
    }
    fn broadcast_packets_cnt(&self) -> usize {
        self.broadcast_packets_cnt
    }
    fn broadcast_packets_max(&self) -> usize {
        self.broadcast_packets_max
    }
    fn broadcast_packets(&mut self) -> &mut HashMap<ClientId, BroadcastPackets> {
        &mut self.broadcast_packets
    }

    fn handle_packet(
        &mut self,
        packet: Self::Packet,
        write_packets: &mut VecDeque<WritePacket<Self::Packet, Self::VarBytes>>,
        global: &Arc<GlobalState>,
    ) -> Result<(), io::Error> {
        match packet {
            Packet::Disconnect(pkt) => {
                if let Err(err_pkt) = handle_disconnect(self, pkt) {
                    // FIXME: must ensure error packet finally written to client in several seconds.
                    write_packets.push_back(err_pkt.into());
                }
            }
            Packet::Publish(pkt) => {
                match handle_publish(self, pkt, global) {
                    // QoS0
                    Ok(None) => {}
                    // QoS1, QoS2
                    Ok(Some(packet)) => write_packets.push_back(packet.into()),
                    Err(err_pkt) => write_packets.push_back(err_pkt.into()),
                }
            }
            Packet::Puback(pkt) => handle_puback(self, pkt),
            Packet::Pubrec(pkt) => write_packets.push_back(handle_pubrec(self, pkt).into()),
            Packet::Pubrel(pkt) => write_packets.push_back(handle_pubrel(self, pkt).into()),
            Packet::Pubcomp(pkt) => handle_pubcomp(self, pkt),
            Packet::Subscribe(pkt) => match handle_subscribe(self, pkt, global) {
                Ok(packets) => write_packets.extend(packets.into_iter().map(WritePacket::Packet)),
                Err(err_pkt) => write_packets.push_back(err_pkt.into()),
            },
            Packet::Unsubscribe(pkt) => {
                write_packets.push_back(handle_unsubscribe(self, pkt, global).into());
            }
            Packet::Pingreq => write_packets.push_back(handle_pingreq(self).into()),
            _ => {
                log::warn!(
                    "[{}] received a invalid packet: {:?}",
                    self.client_id,
                    packet
                );
                return Err(io::ErrorKind::InvalidData.into());
            }
        }
        let pending_packets = after_handle_packet(self);
        write_packets.extend(pending_packets.into_iter().map(WritePacket::Packet));
        Ok(())
    }

    fn handle_control(
        &mut self,
        msg: ControlMessage,
        global: &Arc<GlobalState>,
    ) -> (bool, Option<Sender<SessionState>>) {
        handle_control(self, msg, global)
    }

    fn handle_normal(
        &mut self,
        sender: ClientId,
        msg: NormalMessage,
        global: &Arc<GlobalState>,
    ) -> Option<(QoS, Option<Self::Packet>)> {
        handle_normal(self, sender, msg, global)
    }

    fn handle_pendings(&mut self) -> Vec<Packet> {
        handle_pendings(self)
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

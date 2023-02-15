use std::cmp;
use std::hash::{Hash, Hasher};
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::Receiver;
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use mqtt_proto::{
    v5::{
        Auth, AuthProperties, AuthReasonCode, Connack, ConnackProperties, Connect,
        ConnectReasonCode, Disconnect, DisconnectProperties, DisconnectReasonCode, ErrorV5, Header,
        Packet, Puback, PubackProperties, PubackReasonCode, Pubcomp, PubcompProperties,
        PubcompReasonCode, Publish, PublishProperties, Pubrec, PubrecProperties, PubrecReasonCode,
        Pubrel, PubrelProperties, PubrelReasonCode, RetainHandling, Suback, SubackProperties,
        Subscribe, SubscribeReasonCode, Unsuback, UnsubackProperties, Unsubscribe,
        UnsubscribeReasonCode,
    },
    Protocol, QoS, QosPid, TopicFilter, TopicName, MATCH_ALL_CHAR, MATCH_ONE_CHAR, SHARED_PREFIX,
};
use rand::{rngs::OsRng, thread_rng, Rng, RngCore};
use siphasher::sip::SipHasher24;

use crate::config::{AuthType, SharedSubscriptionMode};
use crate::state::{AddClientReceipt, ClientId, Executor, GlobalState, InternalMessage};

use super::super::{
    get_unix_ts, start_keep_alive_timer, PendingPacketStatus, PendingPackets, RetainContent,
    SIP24_QOS2_KEY,
};
use super::{PubPacket, Session, SessionState, SubscriptionData};

lazy_static! {
    static ref SIP24_SHARED_KEY: [u8; 16] = {
        let mut os_rng = OsRng::default();
        let mut key = [0u8; 16];
        os_rng.fill_bytes(&mut key);
        key
    };
}

pub async fn handle_connection<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    conn: T,
    peer: SocketAddr,
    header: Header,
    protocol: Protocol,
    executor: E,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    match handle_online(conn, peer, header, protocol, &executor, &global).await {
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

    let packet = match Connect::decode_with_protocol(&mut conn, header, protocol).await {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v5.x connect codec error: {}", err);
            return Err(io::ErrorKind::InvalidData.into());
        }
    };
    // FIXME: if client not send any data for a long time, disconnect it.
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

async fn handle_connect<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    packet: Connect,
    conn: &mut T,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a connect packet:
     protocol : {}
    client_id : {}
  clean start : {}
     username : {:?}
     password : {:?}
   keep-alive : {}s
         will : {:?}"#,
        session.peer,
        packet.protocol,
        packet.client_id,
        packet.clean_start,
        packet.username,
        packet.password,
        packet.keep_alive,
        packet.last_will,
    );

    let mut reason_code = ConnectReasonCode::Success;
    // FIXME: auth by plugin
    for auth_type in &global.config.auth_types {
        match auth_type {
            AuthType::UsernamePassword => {
                if let Some(username) = packet.username.as_ref() {
                    if global
                        .config
                        .users
                        .get(username.as_str())
                        .map(|s| s.as_bytes())
                        != packet.password.as_ref().map(|s| s.as_ref())
                    {
                        log::debug!("incorrect password for user: {}", username);
                        reason_code = ConnectReasonCode::BadUserNameOrPassword;
                    }
                } else {
                    log::debug!("username password not set for client: {}", packet.client_id);
                    reason_code = ConnectReasonCode::BadUserNameOrPassword;
                }
            }
            _ => panic!("auth method not supported: {:?}", auth_type),
        }
    }
    // FIXME: permission check and return "not authorized"
    if reason_code != ConnectReasonCode::Success {
        send_error_connack(conn, session, false, reason_code, None, None).await?;
        return Ok(());
    }

    // FIXME: if connection reach rate limit return "Server unavailable"

    session.protocol = packet.protocol;
    session.clean_start = packet.clean_start;
    session.client_identifier = if packet.client_id.is_empty() {
        Arc::new(uuid::Uuid::new_v4().to_string())
    } else {
        Arc::clone(&packet.client_id)
    };
    session.username = packet.username;
    session.keep_alive = if packet.keep_alive > global.config.max_keep_alive {
        global.config.max_keep_alive
    } else if packet.keep_alive < global.config.min_keep_alive {
        global.config.min_keep_alive
    } else {
        packet.keep_alive
    };

    let properties = packet.properties;
    let reason_string = if properties.request_problem_info != Some(false) {
        Some(())
    } else {
        None
    };
    if properties.receive_max == Some(0) {
        log::debug!("connect properties ReceiveMaximum is 0");
        send_error_connack(
            conn,
            session,
            false,
            ConnectReasonCode::ProtocolError,
            reason_string.map(|()| "ReceiveMaximum is 0".to_owned()),
            properties.max_packet_size,
        )
        .await?;
        return Ok(());
    }
    if properties.max_packet_size == Some(0) {
        log::debug!("connect properties MaximumPacketSize is 0");
        send_error_connack(
            conn,
            session,
            false,
            ConnectReasonCode::ProtocolError,
            reason_string.map(|()| "MaximumPacketSize is 0".to_owned()),
            properties.max_packet_size,
        )
        .await?;
        return Ok(());
    }
    if properties.auth_data.is_some() && properties.auth_method.is_none() {
        log::debug!("connect properties AuthenticationMethod is missing");
        send_error_connack(
            conn,
            session,
            false,
            ConnectReasonCode::ProtocolError,
            reason_string.map(|()| "AuthenticationMethod is missing".to_owned()),
            properties.max_packet_size,
        )
        .await?;
        return Ok(());
    }
    session.session_expiry_interval = properties.session_expiry_interval.unwrap_or(0);
    session.receive_max = properties
        .receive_max
        .unwrap_or(global.config.max_inflight_client);
    session.max_packet_size = properties
        .max_packet_size
        .unwrap_or(global.config.max_packet_size);
    session.topic_alias_max = properties.topic_alias_max.unwrap_or(0);
    session.request_response_info = properties.request_response_info.unwrap_or(false);
    session.request_problem_info = properties.request_problem_info.unwrap_or(true);
    session.user_properties = properties.user_properties;
    session.auth_method = properties.auth_method;
    session.auth_data = properties.auth_data;

    session
        .pending_packets
        .set_max_inflight(session.receive_max);
    start_keep_alive_timer(
        session.keep_alive,
        session.client_id,
        &session.last_packet_time,
        executor,
        global,
    )?;

    // FIXME: handle AUTH

    if let Some(last_will) = packet.last_will {
        if last_will.topic_name.starts_with('$') {
            log::warn!("will topic name can't start with $");
            // FIXME: send error connack
            return Err(io::ErrorKind::InvalidData.into());
        }
        session.last_will = Some(last_will);
    }

    let mut session_present = false;
    match global
        .add_client(session.client_identifier.as_str(), session.protocol)
        .await
    {
        AddClientReceipt::PresentV3(_) => {
            // not allowed, so this is dead branch.
            unreachable!()
        }
        AddClientReceipt::PresentV5(old_state) => {
            log::debug!("Got exists session for {}", old_state.client_id);
            session.client_id = old_state.client_id;
            *receiver = Some(old_state.receiver);
            // TODO: if protocol level is compatiable, copy the session state?
            if !session.clean_start && session.protocol == old_state.protocol {
                session.server_packet_id = old_state.server_packet_id;
                session.pending_packets = old_state.pending_packets;
                session.qos2_pids = old_state.qos2_pids;
                session.subscribes = old_state.subscribes;
                session_present = true;
            } else {
                log::info!(
                    "{} session state removed due to reconnect with a different protocol version, new: {}, old: {}, or clean start: {}",
                    old_state.pending_packets.len(),
                    session.protocol,
                    old_state.protocol,
                    session.clean_start,
                );
                session_present = false;
            }
        }
        AddClientReceipt::New {
            client_id,
            receiver: new_receiver,
        } => {
            log::debug!("Create new session for {}", client_id);
            session.client_id = client_id;
            *receiver = Some(new_receiver);
        }
    }

    log::debug!("Socket {} assgined to: {}", session.peer, session.client_id);

    // Build and send connack packet
    let mut connack_properties = ConnackProperties::default();
    if session.session_expiry_interval > global.config.max_session_expiry_interval {
        session.session_expiry_interval = global.config.max_session_expiry_interval;
        connack_properties.session_expiry_interval = Some(session.session_expiry_interval);
    }
    if global.config.max_inflight_server != u16::max_value() {
        connack_properties.receive_max = Some(global.config.max_inflight_server);
    }
    if global.config.max_allowed_qos() < QoS::Level2 {
        connack_properties.max_qos = Some(global.config.max_allowed_qos());
    }
    if !global.config.retain_available {
        connack_properties.retain_available = Some(false);
    }
    if global.config.max_packet_size < u32::max_value() {
        connack_properties.max_packet_size = Some(global.config.max_packet_size);
    }
    if packet.client_id.is_empty() {
        connack_properties.assigned_client_id = Some(Arc::clone(&session.client_identifier));
    }
    if global.config.topic_alias_max > 0 {
        connack_properties.topic_alias_max = Some(global.config.topic_alias_max);
    }
    // * no ReasonString
    // * TODO UserProperty
    if !global.config.wildcard_subscription_available {
        connack_properties.wildcard_subscription_available = Some(false);
    }
    if !global.config.subscription_id_available {
        connack_properties.subscription_id_available = Some(false);
    }
    if !global.config.shared_subscription_available {
        connack_properties.shared_subscription_available = Some(false);
    }
    if session.keep_alive != packet.keep_alive {
        connack_properties.server_keep_alive = Some(session.keep_alive);
    }
    if session.request_response_info {
        // * TODO handle ResponseTopic in plugin
    }
    // * TODO ServerReference
    // * TODO AuthenticationMethod
    // * TODO AuthenticationData
    let rv_packet = Connack {
        session_present,
        reason_code,
        properties: connack_properties,
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;

    session.connected = true;
    session.connected_time = Some(Instant::now());
    after_handle_packet(session, conn).await?;
    Ok(())
}

async fn handle_packet<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    conn: &mut T,
    _executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    // TODO: Decode Header first for more detailed error report.
    let packet = match Packet::decode_async(conn).await {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v5.x codec error: {}", err);
            if err.is_eof() {
                if !session.disconnected {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                } else {
                    return Ok(());
                }
            } else {
                // FIXME: return error reason info with disconnect packet
                return Err(io::ErrorKind::InvalidData.into());
            }
        }
    };
    match packet {
        Packet::Auth(pkt) => handle_auth(session, pkt, conn, global).await?,
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
async fn handle_disconnect<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Disconnect,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{} received a disconnect packet", session.client_id);
    let properties = packet.properties;
    let reason_string = if session.request_problem_info {
        Some(())
    } else {
        None
    };
    if let Some(value) = properties.session_expiry_interval {
        if session.session_expiry_interval == 0 && value > 0 {
            send_error_disconnect(
                conn,
                session,
                DisconnectReasonCode::ProtocolError,
                reason_string.map(|()| "SessionExpiryInterval is 0 in CONNECT".to_owned()),
            )
            .await?;
            return Ok(());
        }
        session.session_expiry_interval = value;
    }

    // * no UserProperty
    // * no ServerReference

    // See: [MQTT-3.14.4-3]
    if packet.reason_code == DisconnectReasonCode::NormalDisconnect {
        session.last_will = None;
    }
    session.disconnected = true;
    Ok(())
}

/// Handle Auth or Re-Auth
#[inline]
async fn handle_auth<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Auth,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    let reason_string = if session.request_problem_info {
        Some(())
    } else {
        None
    };
    if session.auth_method.is_none() {
        send_error_disconnect(
            conn,
            session,
            DisconnectReasonCode::ProtocolError,
            reason_string.map(|()| "AuthenticationMethod not presented in CONNECT".to_owned()),
        )
        .await?;
        return Ok(());
    }
    if session.auth_method != packet.properties.auth_method {
        send_error_disconnect(
            conn,
            session,
            DisconnectReasonCode::ProtocolError,
            reason_string.map(|()| "AuthenticationMethod not same with CONNECT".to_owned()),
        )
        .await?;
        return Ok(());
    }

    // FIXME: handle AUTH/Re-Auth with `SCRAM-SHA-*`

    Ok(())
}

#[inline]
async fn handle_publish<T: AsyncWrite + Unpin>(
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
    let reason_string = if session.request_problem_info {
        Some(())
    } else {
        None
    };

    if packet.topic_name.starts_with('$') {
        log::warn!(
            "publish to special topic name is not allowed: {}",
            packet.topic_name
        );
        send_error_disconnect(
            conn,
            session,
            DisconnectReasonCode::TopicNameInvalid,
            reason_string.map(|()| "special topic (start with $) not allowed".to_owned()),
        )
        .await?;
        return Ok(());
    }
    if packet.qos_pid == QosPid::Level0 && packet.dup {
        log::debug!("invalid dup flag in qos0 message");
        return Err(io::ErrorKind::InvalidData.into());
    }

    let properties = &packet.properties;
    let mut topic_name = packet.topic_name.clone();
    if let Some(alias) = properties.topic_alias {
        if alias == 0 || alias > global.config.topic_alias_max {
            send_error_disconnect(
                conn,
                session,
                DisconnectReasonCode::TopicAliasInvalid,
                reason_string.map(|()| "topic alias too large or is 0".to_owned()),
            )
            .await?;
            return Ok(());
        }
        if packet.topic_name.is_empty() {
            if let Some(name) = session.topic_aliases.get(&alias) {
                topic_name = name.clone();
            } else {
                send_error_disconnect(
                    conn,
                    session,
                    DisconnectReasonCode::ProtocolError,
                    reason_string.map(|()| "topic alias not found".to_owned()),
                )
                .await?;
                return Ok(());
            }
        } else {
            session
                .topic_aliases
                .insert(alias, packet.topic_name.clone());
        }
    }
    if properties.subscription_id.is_some() {
        send_error_disconnect(
            conn,
            session,
            DisconnectReasonCode::ProtocolError,
            reason_string.map(|()| "subscription identifier can't in publish".to_owned()),
        )
        .await?;
        return Ok(());
    }

    // FIXME: handle dup flag

    if let QosPid::Level2(pid) = packet.qos_pid {
        let mut hasher = SipHasher24::new_with_key(&SIP24_QOS2_KEY);
        packet.hash(&mut hasher);
        let current_hash = hasher.finish();

        if let Some(previous_hash) = session.qos2_pids.get(&pid) {
            if current_hash != *previous_hash {
                let reason_code = PubrecReasonCode::PacketIdentifierInUse;
                let rv_packet = Pubrec {
                    pid,
                    reason_code,
                    properties: PubrecProperties::default(),
                };
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
                return Ok(());
            }
        } else {
            session.qos2_pids.insert(pid, current_hash);
        }
    }

    let matched_len = send_publish(
        session,
        SendPublish {
            qos: packet.qos_pid.qos(),
            retain: packet.retain,
            topic_name: &topic_name,
            payload: &packet.payload,
            properties,
        },
        global,
    )
    .await;
    match packet.qos_pid {
        QosPid::Level0 => {}
        QosPid::Level1(pid) => {
            // FIXME: report packet identifier in use
            let reason_code = if matched_len > 0 {
                PubackReasonCode::Success
            } else {
                PubackReasonCode::NoMatchingSubscribers
            };
            let rv_packet = Puback {
                pid,
                reason_code,
                properties: PubackProperties::default(),
            };
            write_packet(session.client_id, conn, &rv_packet.into()).await?
        }
        QosPid::Level2(pid) => {
            let reason_code = if matched_len > 0 {
                PubrecReasonCode::Success
            } else {
                PubrecReasonCode::NoMatchingSubscribers
            };
            let rv_packet = Pubrec {
                pid,
                reason_code,
                properties: PubrecProperties::default(),
            };
            write_packet(session.client_id, conn, &rv_packet.into()).await?
        }
    }
    Ok(())
}

#[inline]
async fn handle_puback<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Puback,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a puback packet: id={}",
        session.client_id,
        packet.pid.value(),
    );
    let _matched = session.pending_packets.complete(packet.pid, QoS::Level1);
    Ok(())
}

#[inline]
async fn handle_pubrec<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Pubrec,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrec  packet: id={}",
        session.client_id,
        packet.pid.value()
    );
    let reason_code = if session.pending_packets.pubrec(packet.pid) {
        PubrelReasonCode::Success
    } else {
        PubrelReasonCode::PacketIdentifierNotFound
    };
    let rv_packet = Pubrel {
        pid: packet.pid,
        reason_code,
        properties: PubrelProperties::default(),
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
async fn handle_pubrel<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Pubrel,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrel  packet: id={}",
        session.client_id,
        packet.pid.value()
    );
    let reason_code = if session.qos2_pids.remove(&packet.pid).is_some() {
        PubcompReasonCode::Success
    } else {
        PubcompReasonCode::PacketIdentifierNotFound
    };
    let rv_packet = Pubcomp {
        pid: packet.pid,
        reason_code,
        properties: PubcompProperties::default(),
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
async fn handle_pubcomp<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Pubcomp,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubcomp packet: id={}",
        session.client_id,
        packet.pid.value()
    );
    let _matched = session.pending_packets.complete(packet.pid, QoS::Level2);
    Ok(())
}

#[inline]
async fn handle_subscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Subscribe,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );

    let properties = packet.properties;
    let reason_string = if session.request_problem_info {
        Some(())
    } else {
        None
    };
    if properties.subscription_id.map(|id| id.value()) == Some(0) {
        send_error_disconnect(
            conn,
            session,
            DisconnectReasonCode::ProtocolError,
            reason_string.map(|()| "Subscription identifier is 0".to_owned()),
        )
        .await?;
        return Ok(());
    }

    // FIXME: handle packet identifier in use
    let reason_codes = if !global.config.subscription_id_available
        && properties.subscription_id.is_some()
    {
        vec![SubscribeReasonCode::SubscriptionIdentifiersNotSupported; packet.topics.len()]
    } else {
        let mut items = Vec::with_capacity(packet.topics.len());
        for (filter, mut sub_opts) in packet.topics {
            let granted_qos = cmp::min(sub_opts.max_qos, global.config.max_allowed_qos());
            let reason_code = if !global.config.shared_subscription_available && filter.is_shared()
            {
                SubscribeReasonCode::SharedSubscriptionNotSupported
            } else if !global.config.wildcard_subscription_available
                && filter.contains(|c| c == MATCH_ONE_CHAR || c == MATCH_ALL_CHAR)
            {
                SubscribeReasonCode::WildcardSubscriptionsNotSupported
            } else {
                match granted_qos {
                    QoS::Level0 => SubscribeReasonCode::GrantedQoS0,
                    QoS::Level1 => SubscribeReasonCode::GrantedQoS1,
                    QoS::Level2 => SubscribeReasonCode::GrantedQoS2,
                }
            };

            if (reason_code as u8) < 0x80 {
                sub_opts.max_qos = granted_qos;
                let new_sub = SubscriptionData::new(sub_opts, properties.subscription_id);
                let old_sub = session.subscribes.insert(filter.clone(), new_sub);
                global
                    .route_table
                    .subscribe(&filter, session.client_id, granted_qos);

                let send_retain = global.config.retain_available
                    && !filter.is_shared()
                    && match sub_opts.retain_handling {
                        RetainHandling::SendAtSubscribe => true,
                        RetainHandling::SendAtSubscribeIfNotExist => old_sub.is_none(),
                        RetainHandling::DoNotSend => false,
                    };
                if send_retain {
                    for msg in global.retain_table.get_matches(&filter) {
                        if sub_opts.no_local && msg.client_identifier == session.client_identifier {
                            continue;
                        }
                        recv_publish(
                            session,
                            RecvPublish {
                                topic_name: &msg.topic_name,
                                qos: msg.qos,
                                retain: true,
                                payload: &msg.payload,
                                subscribe_filter: &filter,
                                subscribe_qos: granted_qos,
                                properties: msg.properties.as_ref(),
                            },
                            Some(conn),
                        )
                        .await?;
                    }
                }
            }

            items.push(reason_code);
        }
        items
    };

    // TODO: handle all other SubscribeReasonCode type
    // TODO: handle ReasonString/UserProperty fields

    let rv_packet = Suback {
        pid: packet.pid,
        topics: reason_codes,
        properties: SubackProperties::default(),
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
async fn handle_unsubscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Unsubscribe,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );
    // FIXME: handle packet identifier in use
    let mut reason_codes = Vec::with_capacity(packet.topics.len());
    for filter in packet.topics {
        global.route_table.unsubscribe(&filter, session.client_id);
        let reason_code = if session.subscribes.remove(&filter).is_some() {
            UnsubscribeReasonCode::Success
        } else {
            UnsubscribeReasonCode::NoSubscriptionExisted
        };
        reason_codes.push(reason_code);
    }
    let rv_packet = Unsuback {
        pid: packet.pid,
        properties: UnsubackProperties::default(),
        topics: reason_codes,
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
async fn handle_pingreq<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{} received a ping packet", session.client_id);
    write_packet(session.client_id, conn, &Packet::Pingresp).await?;
    Ok(())
}

#[inline]
async fn after_handle_packet<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session, conn).await?;
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
                session.client_id,
                reason,
                session.disconnected,
                conn.is_some(),
            );
            stop = conn.is_some();
        }
        InternalMessage::WillDelayReached { connected_time } => {
            if !session.connected && session.connected_time == Some(connected_time) {
                send_will(session, global).await?;
            }
        }
        InternalMessage::SessionExpired { connected_time } => {
            if !session.connected && session.connected_time == Some(connected_time) {
                send_will(session, global).await?;
                global.remove_client(session.client_id, session.subscribes.keys());
                stop = true;
            }
        }
    }
    Ok(stop)
}

// ===========================
// ==== Utils Functions ====
// ===========================

// Received a publish message from client or will, then publish the message to
// matched clients, return the matched subscriptions length.
async fn send_publish<'a>(
    session: &mut Session,
    msg: SendPublish<'a>,
    global: &Arc<GlobalState>,
) -> usize {
    if msg.retain {
        if let Some(old_content) = if msg.payload.is_empty() {
            log::debug!("retain message removed");
            global.retain_table.remove(msg.topic_name)
        } else {
            let content = Arc::new(RetainContent::new(
                session.client_identifier.clone(),
                msg.qos,
                msg.topic_name.clone(),
                msg.payload.clone(),
                Some(msg.properties.clone()),
            ));
            log::debug!("retain message inserted");
            global.retain_table.insert(content)
        } {
            log::debug!(
                r#"old retain content:
 client identifier : {}
        topic name : {}
           payload : {:?}
               qos : {:?}"#,
                old_content.client_identifier,
                &old_content.topic_name.deref().deref(),
                old_content.payload.as_ref(),
                old_content.qos,
            );
        }
    }

    // TODO: enable subscription identifier list by config.
    //   It is also an opinioned optimization.

    let matches = global.route_table.get_matches(msg.topic_name);
    let matched_len = matches.len();
    let mut senders = Vec::with_capacity(matched_len);
    for content in matches {
        let content = content.read();
        let subscribe_filter = content.topic_filter.as_ref().unwrap();
        for (client_id, subscribe_qos) in &content.clients {
            if *client_id == session.client_id {
                if let Some(sub) = session.subscribes.get(subscribe_filter) {
                    if sub.options.no_local {
                        continue;
                    }
                } else {
                    // already unsubscribed
                    continue;
                }
            }
            if let Some(sender) = global.get_client_sender(client_id) {
                senders.push((*client_id, subscribe_filter.clone(), *subscribe_qos, sender));
            }
        }
        for (group_name, shared_clients) in &content.groups {
            let number: u64 = match global.config.shared_subscription_mode {
                SharedSubscriptionMode::Random => thread_rng().gen(),
                SharedSubscriptionMode::HashClientId => {
                    let mut hasher = SipHasher24::new_with_key(&SIP24_SHARED_KEY);
                    hasher.write(session.client_identifier.as_bytes());
                    hasher.finish()
                }
                SharedSubscriptionMode::HashTopicName => {
                    let mut hasher = SipHasher24::new_with_key(&SIP24_SHARED_KEY);
                    hasher.write(msg.topic_name.as_bytes());
                    hasher.finish()
                }
            };
            let (client_id, subscribe_qos) = shared_clients.get_one_by_u64(number);
            let full_filter = TopicFilter::try_from(format!(
                "{}{}/{}",
                SHARED_PREFIX, group_name, subscribe_filter
            ))
            .expect("full topic filter");
            if let Some(sender) = global.get_client_sender(&client_id) {
                senders.push((client_id, full_filter, subscribe_qos, sender));
            }
        }
    }

    for (sender_client_id, subscribe_filter, subscribe_qos, sender) in senders {
        let msg = InternalMessage::PublishV5 {
            retain: msg.retain,
            qos: msg.qos,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            subscribe_filter,
            subscribe_qos,
            properties: msg.properties.clone(),
        };
        if let Err(err) = sender.send_async((session.client_id, msg)).await {
            log::info!(
                "send publish to connection {} failed: {}",
                sender_client_id,
                err
            );
        }
    }
    matched_len
}

// Got a publish message from retain message or subscribed topic, then send the publish message to client.
async fn recv_publish<'a, T: AsyncWrite + Unpin>(
    session: &mut Session,
    msg: RecvPublish<'a>,
    conn: Option<&mut T>,
) -> io::Result<()> {
    let subscription_id = if let Some(sub) = session.subscribes.get(msg.subscribe_filter) {
        sub.id
    } else {
        // the client already unsubscribed.
        return Ok(());
    };

    // TODO: detect costly topic name and enable topic alias
    // TODO: support multiple subscription identifiers
    //       (shared subscription not support multiple subscription identifiers)

    let mut properties = msg.properties.cloned().unwrap_or_default();
    properties.topic_alias = None;
    properties.subscription_id = subscription_id;

    let final_qos = cmp::min(msg.qos, msg.subscribe_qos);
    if final_qos != QoS::Level0 {
        let pid = session.incr_server_packet_id();
        session.pending_packets.clean_complete();
        if let Err(err) = session.pending_packets.push_back(
            pid,
            PubPacket {
                topic_name: msg.topic_name.clone(),
                qos: final_qos,
                retain: msg.retain,
                payload: msg.payload.clone(),
                properties,
            },
        ) {
            // TODO: proper handle this error
            log::warn!("push pending packets error: {}", err);
        }
        if let Some(conn) = conn {
            handle_pendings(session, conn).await?;
        }
    } else if let Some(conn) = conn {
        let rv_packet = Publish {
            dup: false,
            qos_pid: QosPid::Level0,
            retain: msg.retain,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            properties,
        };
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
    }

    Ok(())
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
async fn send_error_connack<T: AsyncWrite + Unpin>(
    conn: &mut T,
    session: &mut Session,
    session_present: bool,
    reason_code: ConnectReasonCode,
    reason_string: Option<String>,
    max_packet_size: Option<u32>,
) -> io::Result<()> {
    let has_reason_string = reason_string.is_some();
    let mut rv_packet: Packet = Connack {
        session_present,
        reason_code,
        properties: ConnackProperties {
            reason_string: reason_string.map(Arc::new),
            ..Default::default()
        },
    }
    .into();
    if has_reason_string {
        let encode_len = rv_packet.encode_len().map_err(|_| {
            log::warn!("connack packet size too large");
            io::Error::from(io::ErrorKind::InvalidData)
        })? as u32;
        if encode_len > max_packet_size.unwrap_or(u32::max_value()) {
            rv_packet = Connack {
                session_present,
                reason_code,
                properties: ConnackProperties::default(),
            }
            .into();
        }
    }
    write_packet(session.client_id, conn, &rv_packet).await?;
    session.disconnected = true;
    Ok(())
}

// TODO: replace this with a macro
#[inline]
async fn send_error_disconnect<T: AsyncWrite + Unpin>(
    conn: &mut T,
    session: &mut Session,
    reason_code: DisconnectReasonCode,
    reason_string: Option<String>,
) -> io::Result<()> {
    let has_reason_string = reason_string.is_some();
    let mut rv_packet: Packet = Disconnect {
        reason_code,
        properties: DisconnectProperties {
            reason_string: reason_string.map(Arc::new),
            ..Default::default()
        },
    }
    .into();
    if has_reason_string {
        let encode_len = rv_packet.encode_len().map_err(|_| {
            // not likely to happen
            log::warn!("disconnect packet too large");
            io::Error::from(io::ErrorKind::InvalidData)
        })? as u32;
        if encode_len > session.max_packet_size {
            rv_packet = Disconnect {
                reason_code,
                properties: DisconnectProperties::default(),
            }
            .into();
        }
    }
    write_packet(session.client_id, conn, &rv_packet).await?;
    session.disconnected = true;
    Ok(())
}

#[inline]
async fn handle_pendings<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    session.pending_packets.clean_complete();
    let mut expired_packets = Vec::new();
    let mut start_idx = 0;
    while let Some((idx, packet_status)) = session.pending_packets.get_ready_packet(start_idx) {
        start_idx = idx + 1;
        match packet_status {
            PendingPacketStatus::New {
                added_at,
                last_sent,
                dup,
                pid,
                packet,
                ..
            } => {
                let mut message_expiry_interval = None;
                if let Some(value) = packet.properties.message_expiry_interval {
                    let passed_secs = get_unix_ts() - *added_at;
                    if *last_sent == 0 && passed_secs >= value as u64 {
                        expired_packets.push(*pid);
                        continue;
                    }
                    message_expiry_interval = Some(value.saturating_sub(passed_secs as u32));
                }
                let qos_pid = match packet.qos {
                    QoS::Level0 => QosPid::Level0,
                    QoS::Level1 => QosPid::Level1(*pid),
                    QoS::Level2 => QosPid::Level2(*pid),
                };
                let mut properties = packet.properties.clone();
                properties.message_expiry_interval = message_expiry_interval;
                let rv_packet = Publish {
                    dup: *dup,
                    retain: packet.retain,
                    qos_pid,
                    topic_name: packet.topic_name.clone(),
                    payload: packet.payload.clone(),
                    properties,
                };
                *dup = true;
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
            }
            PendingPacketStatus::Pubrec { pid, .. } => {
                let rv_packet = Pubrel {
                    pid: *pid,
                    reason_code: PubrelReasonCode::Success,
                    properties: PubrelProperties::default(),
                };
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }
    for pid in &expired_packets {
        // If the QoS2 message exipred, it's MUST also treated as QoS1 message
        session.pending_packets.complete(*pid, QoS::Level1);
    }
    if !expired_packets.is_empty() {
        session.pending_packets.clean_complete();
    }
    Ok(())
}

#[inline]
async fn write_packet<T: AsyncWrite + Unpin>(
    client_id: ClientId,
    conn: &mut T,
    packet: &Packet,
) -> io::Result<()> {
    log::debug!("write to {:?} with packet: {:#?}", client_id, packet);
    packet.encode_async(conn).await.map_err(|err| match err {
        ErrorV5::Common(err) => io::Error::from(err),
        _ => io::ErrorKind::InvalidData.into(),
    })?;
    Ok(())
}

struct RecvPublish<'a> {
    topic_name: &'a TopicName,
    qos: QoS,
    retain: bool,
    payload: &'a Bytes,
    // None for v3 publish packet
    properties: Option<&'a PublishProperties>,
    subscribe_filter: &'a TopicFilter,
    // [MQTTv5.0-3.8.4] keyword: downgraded
    subscribe_qos: QoS,
}

struct SendPublish<'a> {
    topic_name: &'a TopicName,
    retain: bool,
    qos: QoS,
    payload: &'a Bytes,
    properties: &'a PublishProperties,
}

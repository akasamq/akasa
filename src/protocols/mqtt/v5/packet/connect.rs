use std::io;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v5::{
        Auth, AuthProperties, AuthReasonCode, Connack, ConnackProperties, Connect,
        ConnectReasonCode, Disconnect, DisconnectReasonCode, Packet,
    },
    QoS,
};
use scram::server::{AuthenticationStatus, ScramServer};

use crate::config::{AuthType, SaslMechanism};
use crate::protocols::mqtt::start_keep_alive_timer;
use crate::state::{AddClientReceipt, ClientReceiver, Executor, GlobalState};

use super::super::{ScramStage, Session, TracedRng};
use super::common::{build_error_connack, build_error_disconnect, write_packet};

pub(crate) async fn handle_connect<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<ClientReceiver>,
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
            _ => panic!("auth method not supported: {auth_type:?}"),
        }
    }
    // FIXME: permission check and return "not authorized"
    if reason_code != ConnectReasonCode::Success {
        let err_pkt = build_error_connack(session, false, reason_code, "");
        write_packet(session.client_id, conn, &err_pkt).await?;
        return Ok(());
    }

    // FIXME: if connection reach rate limit return "Server unavailable"

    session.protocol = packet.protocol;
    session.clean_start = packet.clean_start;
    session.client_identifier = if packet.client_id.is_empty() {
        session.assigned_client_id = true;
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
    session.server_keep_alive = session.keep_alive != packet.keep_alive;

    let properties = packet.properties;
    session.request_problem_info = properties.request_problem_info.unwrap_or(true);
    session.max_packet_size = properties
        .max_packet_size
        .unwrap_or(global.config.max_packet_size_client);
    if properties.receive_max == Some(0) {
        log::debug!("connect properties ReceiveMaximum is 0");
        let err_pkt = build_error_connack(
            session,
            false,
            ConnectReasonCode::ProtocolError,
            "ReceiveMaximum value=0 is not allowed",
        );
        write_packet(session.client_id, conn, &err_pkt).await?;
        return Ok(());
    }
    if session.max_packet_size == 0 {
        log::debug!("connect properties MaximumPacketSize is 0");
        let err_pkt = build_error_connack(
            session,
            false,
            ConnectReasonCode::ProtocolError,
            "MaximumPacketSize value=0 is not allowed",
        );
        write_packet(session.client_id, conn, &err_pkt).await?;
        return Ok(());
    }

    if properties.auth_data.is_some() && properties.auth_method.is_none() {
        log::debug!("connect properties AuthenticationMethod is missing");
        let err_pkt = build_error_connack(
            session,
            false,
            ConnectReasonCode::ProtocolError,
            "AuthenticationMethod is missing",
        );
        write_packet(session.client_id, conn, &err_pkt).await?;
        return Ok(());
    }

    session.session_expiry_interval = properties.session_expiry_interval.unwrap_or(0);
    session.receive_max = properties
        .receive_max
        .unwrap_or(global.config.max_inflight_client);
    // MaximumPacketSize assigned above
    session.topic_alias_max = properties.topic_alias_max.unwrap_or(0);
    session.request_response_info = properties.request_response_info.unwrap_or(false);
    // RequestProblemInformation assigned above
    session.user_properties = properties.user_properties;
    session.auth_method = properties.auth_method;

    if let Some(last_will) = packet.last_will {
        // v5.0 [MQTT-4.7.3-1]
        if last_will.topic_name.is_empty() {
            log::warn!("will topic name can't be empty");
            // FIXME: send error connack
            return Err(io::ErrorKind::InvalidData.into());
        }
        if last_will.topic_name.starts_with('$') {
            log::warn!("will topic name can't start with $");
            // FIXME: send error connack
            return Err(io::ErrorKind::InvalidData.into());
        }
        session.last_will = Some(last_will);
    }

    if let Some(auth_method) = session.auth_method.clone() {
        let mechanism = if let Some(mechanism) = SaslMechanism::from_str(&auth_method) {
            mechanism
        } else {
            log::info!("connect properties auth method invalid: {}", auth_method);
            let err_pkt = build_error_connack(
                session,
                false,
                ConnectReasonCode::BadAuthMethod,
                "auth method not supported",
            );
            write_packet(session.client_id, conn, &err_pkt).await?;
            return Ok(());
        };
        if !global.config.sasl_mechanisms.contains(&mechanism) {
            log::info!("Sasl mechanism not supported: {:?}", mechanism);
            let err_pkt = build_error_connack(
                session,
                false,
                ConnectReasonCode::BadAuthMethod,
                "auth method not supported",
            );
            write_packet(session.client_id, conn, &err_pkt).await?;
            return Ok(());
        }
        match scram_client_first(session, properties.auth_data, global) {
            Ok(server_first) => {
                let rv_packet = Auth {
                    reason_code: AuthReasonCode::ContinueAuthentication,
                    properties: AuthProperties {
                        auth_method: Some(auth_method),
                        auth_data: Some(Bytes::from(server_first)),
                        reason_string: None,
                        user_properties: Vec::new(),
                    },
                };
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
            }
            Err(err_pkt) => write_packet(session.client_id, conn, &err_pkt).await?,
        }
        Ok(())
    } else if properties.auth_data.is_some() {
        log::info!("connect properties have auth data but missing auth method");
        let err_pkt = build_error_connack(
            session,
            false,
            ConnectReasonCode::ProtocolError,
            "auth method is missing",
        );
        write_packet(session.client_id, conn, &err_pkt).await?;
        Ok(())
    } else {
        session_connect(session, receiver, None, conn, executor, global).await
    }
}

pub(crate) async fn session_connect<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<ClientReceiver>,
    auth_data: Option<String>,
    conn: &mut T,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    let mut session_present = false;
    match global
        .add_client(session.client_identifier.as_str(), session.protocol)
        .await
    {
        // not allowed, so this is dead branch.
        AddClientReceipt::PresentV3(_) => unreachable!(),
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
    if global.config.max_packet_size_server < u32::max_value() {
        connack_properties.max_packet_size = Some(global.config.max_packet_size_server);
    }
    if session.assigned_client_id {
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
    if session.server_keep_alive {
        connack_properties.server_keep_alive = Some(session.keep_alive);
    }
    if session.request_response_info {
        // * TODO handle ResponseTopic in plugin
    }
    if let Some(auth_data) = auth_data {
        connack_properties.auth_method = session.auth_method.clone();
        connack_properties.auth_data = Some(Bytes::from(auth_data));
    }
    // * TODO ServerReference

    let reason_code = if let Some(will) = session.last_will.as_ref() {
        if will.retain && !global.config.retain_available {
            ConnectReasonCode::RetainNotSupported
        } else if will.qos > global.config.max_allowed_qos() {
            ConnectReasonCode::QoSNotSupported
        } else {
            ConnectReasonCode::Success
        }
    } else {
        ConnectReasonCode::Success
    };
    let rv_packet = Connack {
        session_present,
        reason_code,
        properties: connack_properties,
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;

    if reason_code == ConnectReasonCode::Success {
        session.connected = true;
        session.connected_time = Some(Instant::now());
    }
    Ok(())
}

#[inline]
pub(crate) fn handle_disconnect(session: &mut Session, packet: Disconnect) -> Result<(), Packet> {
    log::debug!("{} received a disconnect packet", session.client_id);
    if let Some(value) = packet.properties.session_expiry_interval {
        if session.session_expiry_interval == 0 && value > 0 {
            return Err(build_error_disconnect(
                session,
                DisconnectReasonCode::ProtocolError,
                "SessionExpiryInterval is 0 in CONNECT",
            ));
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
pub(crate) fn handle_auth(
    session: &mut Session,
    packet: Auth,
    global: &Arc<GlobalState>,
) -> Result<(AuthReasonCode, String), Packet> {
    // TODO: should allow server send ReAuthentication AUTH packet to authenticate some clients
    if session.auth_method.is_none() {
        log::info!("auth method not presented in CONNECT");
        return Err(build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "auth method not presented in CONNECT",
        ));
    }
    if session.auth_method != packet.properties.auth_method {
        log::info!("auth method not same with CONNECT");
        return if session.connected {
            Err(build_error_disconnect(
                session,
                DisconnectReasonCode::ProtocolError,
                "auth method not same with CONNECT",
            ))
        } else {
            Err(build_error_connack(
                session,
                false,
                ConnectReasonCode::ProtocolError,
                "auth method not same with CONNECT",
            ))
        };
    }

    match packet.reason_code {
        AuthReasonCode::Success => {
            if session.connected {
                Err(build_error_disconnect(
                    session,
                    DisconnectReasonCode::ProtocolError,
                    "invalid auth reason code",
                ))
            } else {
                Err(build_error_connack(
                    session,
                    false,
                    ConnectReasonCode::ProtocolError,
                    "invalid auth reason code",
                ))
            }
        }
        AuthReasonCode::ContinueAuthentication => {
            if !session.authorizing {
                return Err(build_error_disconnect(
                    session,
                    DisconnectReasonCode::ProtocolError,
                    "invalid auth reason code",
                ));
            }

            let client_final = if let Some(data) = packet.properties.auth_data {
                if let Ok(string) = String::from_utf8(data.as_ref().to_vec()) {
                    string
                } else {
                    log::info!("client final auth data is not utf8");
                    return Err(build_error_connack(
                        session,
                        false,
                        ConnectReasonCode::NotAuthorized,
                        "client final auth data must be utf8 string",
                    ));
                }
            } else {
                log::info!("client final auth data is missing");
                return Err(build_error_connack(
                    session,
                    false,
                    ConnectReasonCode::NotAuthorized,
                    "cilent final auth data is missing",
                ));
            };

            let (client_first, server_nonce) = match &session.scram_stage {
                ScramStage::ClientFirst {
                    ref message,
                    server_nonce,
                    ..
                } => (message, server_nonce.clone()),
                _ => unreachable!(),
            };
            let scram_server = ScramServer::new(&global.config);
            let scram_server = match scram_server.handle_client_first(client_first) {
                Ok(scram_server) => scram_server,
                Err(err) => {
                    log::info!("scram re-handle client first error: {}, the user may removed from AuthenticationProvider", err);
                    let err_pkt = if session.connected {
                        build_error_disconnect(
                            session,
                            DisconnectReasonCode::NotAuthorized,
                            "invalid client first data",
                        )
                    } else {
                        build_error_connack(
                            session,
                            false,
                            ConnectReasonCode::NotAuthorized,
                            "invalid client first data",
                        )
                    };
                    return Err(err_pkt);
                }
            };
            let mut traced_rng = TracedRng::new_from(server_nonce);
            let (scram_server, _) = scram_server.server_first_with_rng(&mut traced_rng);
            let scram_server = match scram_server.handle_client_final(&client_final) {
                Ok(server) => server,
                Err(err) => {
                    log::info!("scram handle client final error: {}", err);
                    let err_pkt = if session.connected {
                        build_error_disconnect(
                            session,
                            DisconnectReasonCode::NotAuthorized,
                            "invalid client final data",
                        )
                    } else {
                        build_error_connack(
                            session,
                            false,
                            ConnectReasonCode::NotAuthorized,
                            "invalid client final data",
                        )
                    };
                    return Err(err_pkt);
                }
            };
            let (status, server_final) = scram_server.server_final();
            if status != AuthenticationStatus::Authenticated {
                log::info!("scram handle server final failed, status={:?}", status);
                let err_pkt = if session.connected {
                    build_error_disconnect(
                        session,
                        DisconnectReasonCode::NotAuthorized,
                        "invalid client final data",
                    )
                } else {
                    build_error_connack(
                        session,
                        false,
                        ConnectReasonCode::NotAuthorized,
                        "invalid client final data",
                    )
                };
                return Err(err_pkt);
            }

            // TODO: remove this later, when scram updated
            let (authcid, authzid) = {
                let mut parts = client_first.split(',');
                let _n = parts.next();
                let raw_authzid = parts.next().expect("authzid");
                let authzid = if raw_authzid.is_empty() {
                    None
                } else {
                    Some(raw_authzid[2..].to_owned())
                };
                let authcid = parts.next().expect("authcid")[2..].to_owned();
                (authcid, authzid)
            };
            session.authorizing = false;
            session.scram_stage = ScramStage::Final(Instant::now());
            session.scram_auth_result = Some((authcid, authzid));
            if !session.connected {
                log::info!("client {} AUTH success", session.client_identifier);
            } else {
                log::info!("client {} Re-AUTH success", session.client_identifier);
            }
            Ok((AuthReasonCode::Success, server_final))
        }
        AuthReasonCode::ReAuthentication => {
            if session.authorizing {
                log::info!("after started auth, reason code must be ContinueAuthentication");
                let err_pkt = if session.connected {
                    build_error_disconnect(
                        session,
                        DisconnectReasonCode::ProtocolError,
                        "invalid auth reason code",
                    )
                } else {
                    build_error_connack(
                        session,
                        false,
                        ConnectReasonCode::ProtocolError,
                        "invalid auth reason code",
                    )
                };
                return Err(err_pkt);
            }
            scram_client_first(session, packet.properties.auth_data, global)
                .map(|server_first| (AuthReasonCode::ContinueAuthentication, server_first))
        }
    }
}

#[inline]
fn scram_client_first(
    session: &mut Session,
    auth_data: Option<Bytes>,
    global: &Arc<GlobalState>,
) -> Result<String, Packet> {
    let client_first = if let Some(data) = auth_data {
        if let Ok(string) = String::from_utf8(data.as_ref().to_vec()) {
            string
        } else {
            log::info!("scram client first data is not utf8");
            return Err(build_error_connack(
                session,
                false,
                ConnectReasonCode::NotAuthorized,
                "client first data must be utf8 string",
            ));
        }
    } else {
        log::info!("scram client first data is missing");
        return Err(build_error_connack(
            session,
            false,
            ConnectReasonCode::NotAuthorized,
            "client first data is missing",
        ));
    };
    let scram_server = ScramServer::new(&global.config);
    let scram_server = match scram_server.handle_client_first(&client_first) {
        Ok(scram_server) => scram_server,
        Err(err) => {
            log::info!("scram handle client first error: {}", err);
            if session.connected {
                return Err(build_error_disconnect(
                    session,
                    DisconnectReasonCode::NotAuthorized,
                    "invalid client first data",
                ));
            } else {
                return Err(build_error_connack(
                    session,
                    false,
                    ConnectReasonCode::NotAuthorized,
                    "invalid client first data",
                ));
            }
        }
    };
    let mut traced_rng = TracedRng::new_empty();
    let (_, server_first) = scram_server.server_first_with_rng(&mut traced_rng);
    session.authorizing = true;
    session.scram_stage = ScramStage::ClientFirst {
        message: client_first,
        server_nonce: traced_rng.into_data(),
        time: Instant::now(),
    };
    Ok(server_first)
}

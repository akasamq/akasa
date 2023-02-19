use std::io;
use std::sync::Arc;

use flume::Receiver;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v3::{Connack, Connect, ConnectReturnCode},
    Protocol,
};

use crate::config::AuthType;
use crate::protocols::mqtt::start_keep_alive_timer;
use crate::state::{AddClientReceipt, ClientId, Executor, GlobalState, InternalMessage};

use super::super::Session;
use super::common::{after_handle_packet, write_packet};

pub(crate) async fn handle_connect<T: AsyncWrite + Unpin, E: Executor>(
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
clean session : {}
     username : {:?}
     password : {:?}
   keep-alive : {}s
         will : {:?}"#,
        session.peer,
        packet.protocol,
        packet.client_id,
        packet.clean_session,
        packet.username,
        packet.password,
        packet.keep_alive,
        packet.last_will,
    );

    if global.config.enable_v310_client_id_length_check
        && packet.protocol == Protocol::V310
        && (packet.client_id.is_empty() || packet.client_id.len() > 23)
    {
        log::info!("invalid v3.1 client id length: {}", packet.client_id.len());
        let rv_packet = Connack::new(false, ConnectReturnCode::IdentifierRejected);
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
        session.disconnected = true;
        return Ok(());
    }

    let mut return_code = ConnectReturnCode::Accepted;
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
                        return_code = ConnectReturnCode::BadUserNameOrPassword;
                    }
                } else {
                    log::debug!("username password not set for client: {}", packet.client_id);
                    return_code = ConnectReturnCode::BadUserNameOrPassword;
                }
            }
            _ => panic!("auth method not supported: {:?}", auth_type),
        }
    }
    // FIXME: permission check and return "not authorized"
    if return_code != ConnectReturnCode::Accepted {
        let rv_packet = Connack::new(false, return_code);
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
        session.disconnected = true;
        return Ok(());
    }

    // FIXME: if connection reach rate limit return "Server unavailable"

    session.protocol = packet.protocol;
    session.clean_session = packet.clean_session;
    session.client_identifier = if packet.client_id.is_empty() {
        Arc::new(uuid::Uuid::new_v4().to_string())
    } else {
        Arc::clone(&packet.client_id)
    };
    session.username = packet.username.map(|name| Arc::clone(&name));
    session.keep_alive = packet.keep_alive;

    if let Some(last_will) = packet.last_will {
        if last_will.topic_name.starts_with('$') {
            return Err(io::ErrorKind::InvalidData.into());
        }
        session.last_will = Some(last_will);
    }

    let mut session_present = false;
    match global
        .add_client(session.client_identifier.as_str(), session.protocol)
        .await
    {
        AddClientReceipt::PresentV3(old_state) => {
            log::debug!("Got exists session for {}", old_state.client_id);
            session.client_id = old_state.client_id;
            *receiver = Some(old_state.receiver);
            // TODO: if protocol level is compatiable, copy the session state?
            if !session.clean_session && session.protocol == old_state.protocol {
                session.server_packet_id = old_state.server_packet_id;
                session.pending_packets = old_state.pending_packets;
                session.qos2_pids = old_state.qos2_pids;
                session.subscribes = old_state.subscribes;
                session_present = true;
            } else {
                log::info!(
                    "{} session state removed due to reconnect with a different protocol version, new: {}, old: {}, or clean session: {}",
                    old_state.pending_packets.len(),
                    session.protocol,
                    old_state.protocol,
                    session.clean_session,
                );
                session_present = false;
            }
        }
        // not allowed, so this is dead branch.
        AddClientReceipt::PresentV5(_) => unreachable!(),
        AddClientReceipt::New {
            client_id,
            receiver: new_receiver,
        } => {
            log::debug!("Create new session for {}", client_id);
            session.client_id = client_id;
            *receiver = Some(new_receiver);
        }
    }

    start_keep_alive_timer(
        session.keep_alive,
        session.client_id,
        &session.last_packet_time,
        executor,
        global,
    )?;

    log::debug!("Socket {} assgined to: {}", session.peer, session.client_id);

    let rv_packet = Connack::new(session_present, return_code);
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    session.connected = true;
    after_handle_packet(session, conn).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_disconnect<T: AsyncWrite + Unpin>(
    session: &mut Session,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{} received a disconnect packet", session.client_id);
    session.last_will = None;
    session.disconnected = true;
    Ok(())
}

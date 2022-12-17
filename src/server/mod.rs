pub mod rt_glommio;
pub mod rt_tokio;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use flume::Receiver;
use futures_lite::{
    future::FutureExt,
    io::{AsyncRead, AsyncWrite},
};

use crate::protocols::mqtt;
use crate::state::{ClientId, Executor, GlobalState, InternalMessage};

pub async fn handle_accept<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    conn: T,
    peer: SocketAddr,
    executor: E,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    if let Some((session, receiver)) = handle_connection(conn, peer, &executor, &global).await? {
        log::info!(
            "executor {:03}, {} go to offline, total {} clients ({} online) ",
            executor.id(),
            peer,
            global.clients_count(),
            global.online_clients_count(),
        );
        executor.spawn_local(handle_offline(session, receiver, global));
    } else {
        log::info!(
            "executor {:03}, {} finished, total {} clients ({} online) ",
            executor.id(),
            peer,
            global.clients_count(),
            global.online_clients_count(),
        );
    }
    Ok(())
}

pub async fn handle_connection<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    mut conn: T,
    peer: SocketAddr,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<Option<(mqtt::Session, Receiver<(ClientId, InternalMessage)>)>> {
    enum Msg {
        Socket(()),
        Internal((ClientId, InternalMessage)),
    }

    let mut session = mqtt::Session::new(&global.config);
    let mut receiver = None;
    let mut io_error = None;
    // handle first connect packet
    mqtt::handle_connection(
        &mut session,
        &mut receiver,
        &mut conn,
        &peer,
        executor,
        global,
    )
    .await?;
    if !session.connected() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let receiver = receiver.unwrap();

    while !session.disconnected() {
        // Online client logic
        let recv_data = async {
            mqtt::handle_connection(&mut session, &mut None, &mut conn, &peer, executor, global)
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
                match mqtt::handle_internal(
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
                            io_error = Some(io::Error::from(io::ErrorKind::BrokenPipe));
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
        mqtt::handle_will(&mut session, &mut conn, global).await?;
    }
    if session.clean_session() {
        global.remove_client(session.client_id());
        for filter in session.subscribes().keys() {
            global.route_table.unsubscribe(filter, session.client_id());
        }
        if let Some(err) = io_error {
            return Err(err);
        }
    } else {
        // become a offline client, but session keep updating
        global.offline_client(session.client_id());
        return Ok(Some((session, receiver)));
    }

    Ok(None)
}

pub async fn handle_offline(
    mut session: mqtt::Session,
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
        match mqtt::handle_internal(&mut session, &receiver, sender, msg, conn.as_mut(), &global)
            .await
        {
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

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::future::FutureExt;
use glommio::{
    net::{Preallocated, TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
};

use crate::config::Config;
use crate::protocols::mqtt;
use crate::state::{ClientId, GlobalState, InternalMsg};

pub fn start(bind: SocketAddr, config: PathBuf) -> io::Result<()> {
    let config: Arc<Config> = {
        let content = fs::read_to_string(&config)?;
        Arc::new(
            json5::from_str(&content)
                .map_err(|err| io::Error::from(io::ErrorKind::InvalidInput))?,
        )
    };
    log::debug!("config: {:#?}", config);
    if !config.is_valid() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    log::info!("Listen on {}", bind);
    let cpu_set = CpuSet::online().expect("online cpus");
    let placement = PoolPlacement::MaxSpread(num_cpus::get(), Some(cpu_set));
    let global_state: Arc<GlobalState> = Arc::new(GlobalState::new());
    LocalExecutorPoolBuilder::new(placement)
        .on_all_shards(move || async move {
            let id = glommio::executor().id();
            loop {
                log::info!("Starting executor {}", id);
                if let Err(err) =
                    broker(id, bind, Arc::clone(&config), Arc::clone(&global_state)).await
                {
                    log::error!("Executor {} stopped with error: {}", id, err);
                    sleep(Duration::from_secs(1)).await;
                } else {
                    log::info!("Executor {} stopped successfully!", id);
                }
            }
        })
        .expect("executor pool")
        .join_all();
    Ok(())
}

async fn broker(
    id: usize,
    bind: SocketAddr,
    config: Arc<Config>,
    global_state: Arc<GlobalState>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    loop {
        let conn = listener.accept().await?.buffered();
        let fd = conn.as_raw_fd();
        let peer_addr = conn.peer_addr()?;
        log::info!(
            "executor {:03}, #{} {} connected, total {} clients ({} online) ",
            id,
            peer_addr,
            fd,
            global_state.clients_count(),
            global_state.online_clients_count(),
        );
        spawn_local({
            let config = config.clone();
            let global_state = Arc::clone(&global_state);
            async move {
                if let Err(err) = handle_connection(Some(conn), fd, &config, &global_state).await {
                    log::error!("#{} connection loop error: {}", fd, err);
                } else {
                    log::info!(
                        "executor {:03}, #{} {} connected, total {} clients ({} online) ",
                        id,
                        peer_addr,
                        fd,
                        global_state.clients_count(),
                        global_state.online_clients_count(),
                    );
                }
            }
        })
        .detach();
    }
}

async fn handle_connection(
    mut conn: Option<TcpStream<Preallocated>>,
    current_fd: RawFd,
    config: &Arc<Config>,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    enum Msg {
        Socket(()),
        Internal((ClientId, Arc<InternalMsg>)),
    }

    let mut session = mqtt::Session::new();
    let mut receiver = None;
    // handle first connect packet
    mqtt::handle_connection(
        &mut session,
        &mut receiver,
        conn.as_mut().unwrap(),
        current_fd,
        config,
        global_state,
    )
    .await?;
    let receiver = receiver.unwrap();

    loop {
        if conn.is_some() {
            if session.disconnected() {
                // become a offline client, but session keep updating
                let _ = conn.take();
                if session.clean_session() {
                    global_state.remove_client(session.client_id());
                    break;
                } else {
                    global_state.offline_client(session.client_id());
                }
            }
            if let Some(err) = session.io_error.take() {
                if let Some(conn) = conn.as_mut() {
                    mqtt::handle_will(&mut session, conn, current_fd, global_state).await?;
                }
                // become a offline client, but session keep updating
                let _ = conn.take();
                if session.clean_session() {
                    global_state.remove_client(session.client_id());
                    return Err(err);
                } else {
                    global_state.offline_client(session.client_id());
                }
            }
        }

        if let Some(conn) = conn.as_mut() {
            // online client logic
            let recv_data = async {
                mqtt::handle_connection(
                    &mut session,
                    &mut None,
                    conn,
                    current_fd,
                    config,
                    global_state,
                )
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
                    match mqtt::handle_internal(
                        &mut session,
                        &receiver,
                        sender,
                        msg,
                        Some(conn),
                        config,
                        global_state,
                    )
                    .await
                    {
                        Ok(true) => {
                            // been occupied by newly connected client
                            break;
                        }
                        Ok(false) => {}
                        Err(err) => {
                            session.io_error = Some(err);
                        }
                    }
                }
                Err(err) => {
                    session.io_error = Some(err);
                }
            }
        } else {
            // offline client logic
            let (sender, msg) = receiver
                .recv_async()
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
            match mqtt::handle_internal(
                &mut session,
                &receiver,
                sender,
                msg,
                conn.as_mut(),
                config,
                global_state,
            )
            .await
            {
                Ok(true) => {
                    break;
                }
                Ok(false) => {}
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }
    Ok(())
}

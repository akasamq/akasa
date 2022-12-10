use std::fs;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use flume::Receiver;
use futures_lite::future::FutureExt;
use glommio::{
    net::{Preallocated, TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    CpuSet, Latency, LocalExecutorPoolBuilder, PoolPlacement, Shares,
};

use crate::config::Config;
use crate::protocols::mqtt;
use crate::state::{ClientId, ExecutorState, GlobalState, InternalMessage};

pub fn start(bind: SocketAddr, config: PathBuf) -> io::Result<()> {
    let config: Config = {
        let content = fs::read_to_string(&config)?;
        json5::from_str(&content).map_err(|err| io::Error::from(io::ErrorKind::InvalidInput))?
    };
    log::debug!("config: {:#?}", config);
    if !config.is_valid() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    log::info!("Listen on {}", bind);
    let cpu_set = CpuSet::online().expect("online cpus");
    let placement = PoolPlacement::MaxSpread(num_cpus::get(), Some(cpu_set));
    let global: Arc<GlobalState> = Arc::new(GlobalState::new(bind, config));
    LocalExecutorPoolBuilder::new(placement)
        .on_all_shards(move || async move {
            let id = glommio::executor().id();
            // Do clean up tasks, such as:
            //   * kick out keep alive timeout connections
            let gc_queue = glommio::executor().create_task_queue(
                Shares::default(),
                Latency::Matters(Duration::from_secs(15)),
                "gc",
            );
            let executor = Rc::new(ExecutorState::new(id, gc_queue));
            loop {
                log::info!("Starting executor {}", id);
                if let Err(err) = broker(Rc::clone(&executor), Arc::clone(&global)).await {
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

async fn broker(executor: Rc<ExecutorState>, global: Arc<GlobalState>) -> io::Result<()> {
    let listener = TcpListener::bind(global.bind)?;
    loop {
        let conn = listener.accept().await?.buffered();
        let fd = conn.as_raw_fd();
        let peer_addr = conn.peer_addr()?;
        log::info!(
            "executor {:03}, #{} {} connected, total {} clients ({} online) ",
            executor.id,
            fd,
            peer_addr,
            global.clients_count(),
            global.online_clients_count(),
        );
        spawn_local({
            let executor = Rc::clone(&executor);
            let global = Arc::clone(&global);
            async move {
                match handle_connection(Some(conn), fd, &executor, &global).await {
                    Ok(Some((mut session, receiver))) => {
                        log::info!(
                            "executor {:03}, #{} {} go to offline, total {} clients ({} online) ",
                            executor.id,
                            fd,
                            peer_addr,
                            global.clients_count(),
                            global.online_clients_count(),
                        );
                        spawn_local(async move {
                            loop {
                                let (sender, msg) = match receiver.recv_async().await {
                                    Ok((sender, msg)) => (sender, msg),
                                    Err(err) => {
                                        log::warn!(
                                            "offline client receive internal message error: {:?}",
                                            err
                                        );
                                        break;
                                    }
                                };
                                match mqtt::handle_internal(
                                    &mut session,
                                    &receiver,
                                    sender,
                                    msg,
                                    None,
                                    &global,
                                )
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
                            log::debug!("offline client {:?} finished", session.client_id);
                        })
                        .detach();
                    }
                    Ok(None) => {
                        log::info!(
                            "executor {:03}, #{} {} finished, total {} clients ({} online) ",
                            executor.id,
                            fd,
                            peer_addr,
                            global.clients_count(),
                            global.online_clients_count(),
                        );
                    }
                    Err(err) => {
                        log::error!("#{} connection loop error: {}", fd, err);
                    }
                }
            }
        })
        .detach();
    }
}

async fn handle_connection(
    mut conn: Option<TcpStream<Preallocated>>,
    current_fd: RawFd,
    executor: &Rc<ExecutorState>,
    global: &Arc<GlobalState>,
) -> io::Result<Option<(mqtt::Session, Receiver<(ClientId, InternalMessage)>)>> {
    enum Msg {
        Socket(()),
        Internal((ClientId, InternalMessage)),
    }

    let mut session = mqtt::Session::new();
    let mut receiver = None;
    // handle first connect packet
    mqtt::handle_connection(
        &mut session,
        &mut receiver,
        conn.as_mut().unwrap(),
        current_fd,
        executor,
        global,
    )
    .await?;
    let receiver = receiver.unwrap();

    loop {
        if conn.is_some() {
            if session.disconnected() {
                // become a offline client, but session keep updating
                conn = None;
                if session.clean_session() {
                    global.remove_client(session.client_id());
                    break;
                } else {
                    global.offline_client(session.client_id());
                }
            }
            if let Some(err) = session.io_error.take() {
                if let Some(conn) = conn.as_mut() {
                    mqtt::handle_will(&mut session, conn, current_fd, global).await?;
                }
                // become a offline client, but session keep updating
                conn = None;
                if session.clean_session() {
                    global.remove_client(session.client_id());
                    return Err(err);
                } else {
                    global.offline_client(session.client_id());
                }
            }
        }

        if let Some(conn) = conn.as_mut() {
            // Online client logic
            let recv_data = async {
                mqtt::handle_connection(&mut session, &mut None, conn, current_fd, executor, global)
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
                        Some(conn),
                        global,
                    )
                    .await
                    {
                        Ok(true) => {
                            if is_kick && !session.disconnected {
                                // Offline client logic
                                return Ok(Some((session, receiver)));
                            }
                            // Been occupied by newly connected client or kicked out after disconnected
                            break;
                        }
                        Ok(false) => {}
                        // Currently, this error can only happend when write data to connection
                        Err(err) => {
                            // An error in online mode should also check clean_session value
                            session.io_error = Some(err);
                        }
                    }
                }
                Err(err) => {
                    // An error in online mode should also check clean_session value
                    session.io_error = Some(err);
                }
            }
        } else {
            // Offline client logic
            return Ok(Some((session, receiver)));
        }
    }
    Ok(None)
}

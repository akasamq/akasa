use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use flume::{bounded, Receiver};
use futures_lite::future::FutureExt;
use glommio::{
    net::{Preallocated, TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
};

use crate::protocols::mqtt;
use crate::state::{GlobalState, InternalMsg};

pub fn start(bind: SocketAddr) -> io::Result<()> {
    log::info!("Listen on {}", bind);
    let cpu_set = CpuSet::online().expect("online cpus");
    let placement = PoolPlacement::MaxSpread(num_cpus::get(), Some(cpu_set));
    let global_state: Arc<GlobalState> = Arc::new(GlobalState::new());
    LocalExecutorPoolBuilder::new(placement)
        .on_all_shards(move || async move {
            let id = glommio::executor().id();
            loop {
                log::info!("Starting executor {}", id);
                if let Err(err) = broker(id, bind, Arc::clone(&global_state)).await {
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

async fn broker(id: usize, bind: SocketAddr, global_state: Arc<GlobalState>) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    loop {
        let conn = listener.accept().await?.buffered();
        let fd = conn.as_raw_fd();
        let peer_addr = conn.peer_addr()?;
        let (sender, receiver) = bounded(4);
        global_state.connections.insert(fd, sender);
        log::info!(
            "executor {:03}, #{} {} connected, total {} connections ",
            id,
            peer_addr,
            fd,
            global_state.connections.len()
        );
        spawn_local({
            let global_state = Arc::clone(&global_state);
            async move {
                if let Err(err) = handle_connection(conn, receiver, fd, &global_state).await {
                    log::error!("connection() loop error: {}", err);
                } else {
                    global_state.connections.remove(&fd);
                    log::info!(
                        "executor {:03}, {} disconnected, total {} connections",
                        id,
                        peer_addr,
                        global_state.connections.len()
                    );
                }
            }
        })
        .detach();
    }
}

async fn handle_connection(
    mut conn: TcpStream<Preallocated>,
    receiver: Receiver<(RawFd, InternalMsg)>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    enum Msg {
        // EOF reached
        Socket(bool),
        Internal((RawFd, InternalMsg)),
    }

    let mut session = mqtt::Session::default();
    loop {
        let recv_data = async {
            mqtt::handle_conntion(&mut session, &mut conn, current_fd, global_state)
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
        match recv_data.or(recv_msg).await? {
            Msg::Socket(true) => break,
            Msg::Socket(false) => {}
            Msg::Internal((sender, msg)) => {
                mqtt::handle_internal(sender, msg, &mut conn, current_fd, global_state).await?;
            }
        }
    }
    Ok(())
}

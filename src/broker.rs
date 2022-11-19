use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use flume::{bounded, Receiver, Sender};
use futures_lite::{future::FutureExt, AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::{TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
};

pub fn start(bind: SocketAddr) -> io::Result<()> {
    log::info!("Listen on {}", bind);
    let cpu_set = CpuSet::online().expect("online cpus");
    let placement = PoolPlacement::MaxSpread(num_cpus::get(), Some(cpu_set));
    let connections: Arc<DashMap<RawFd, Sender<(RawFd, Bytes)>>> = Arc::new(DashMap::new());
    LocalExecutorPoolBuilder::new(placement)
        .on_all_shards(move || async move {
            let id = glommio::executor().id();
            loop {
                log::info!("Starting executor {}", id);
                if let Err(err) = broker(id, bind, Arc::clone(&connections)).await {
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
    connections: Arc<DashMap<RawFd, Sender<(RawFd, Bytes)>>>,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    loop {
        let mut conn = listener.accept().await?;
        let fd = conn.as_raw_fd();
        let peer_addr = conn.peer_addr()?;
        let (sender, receiver) = bounded(4);
        connections.insert(fd, sender);
        log::info!(
            "executor {:03}, {} connected, total {} connections ",
            id,
            peer_addr,
            connections.len()
        );
        spawn_local({
            let connections = Arc::clone(&connections);
            async move {
                if let Err(err) = echo(&mut conn, fd, receiver, &connections).await {
                    log::error!("echo() loop error: {}", err);
                } else {
                    connections.remove(&fd);
                    log::info!(
                        "executor {:03}, {} disconnected, total {} connections",
                        id,
                        peer_addr,
                        connections.len()
                    );
                }
            }
        })
        .detach();
    }
}

async fn echo(
    conn: &mut TcpStream,
    current_fd: RawFd,
    messages: Receiver<(RawFd, Bytes)>,
    connections: &Arc<DashMap<RawFd, Sender<(RawFd, Bytes)>>>,
) -> io::Result<()> {
    enum Msg {
        Socket(usize),
        Channel((RawFd, Bytes)),
    }

    // FIXME: message length can be 64+ KB
    let mut buf = [0u8; 1024];
    loop {
        let msg: Msg = async { Ok(Msg::Socket(conn.read(&mut buf).await?)) }
            .or(async {
                let data = messages
                    .recv_async()
                    .await
                    .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
                Ok::<Msg, io::Error>(Msg::Channel(data))
            })
            .await?;
        match msg {
            Msg::Socket(n) => {
                if n == 0 {
                    break;
                }
                conn.write_all(&buf[0..n]).await?;
                if !connections.is_empty() {
                    let data = Bytes::from(buf[0..n].to_vec());
                    for item in connections.as_ref() {
                        if *item.key() != current_fd {
                            item.value()
                                .send_async((current_fd, data.clone()))
                                .await
                                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
                        }
                    }
                }
            }
            Msg::Channel((sender, data)) => {
                conn.write_all(format!("[{}]: ", sender).as_bytes()).await?;
                conn.write_all(data.as_ref()).await?;
            }
        }
    }
    Ok(())
}

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::{TcpListener, TcpStream},
    spawn_local,
    timer::sleep,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
};

pub fn start(bind: SocketAddr) -> io::Result<()> {
    log::info!("Listen on {}", bind);
    let cpu_set = CpuSet::online()
        .expect("online cpus")
        .filter(|loc| loc.cpu > 0);
    let placement = PoolPlacement::MaxSpread(num_cpus::get() - 1, Some(cpu_set));
    LocalExecutorPoolBuilder::new(placement)
        .on_all_shards(move || async move {
            let id = glommio::executor().id();
            loop {
                log::info!("Starting executor {}", id);
                if let Err(err) = broker(id, bind).await {
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

async fn broker(id: usize, bind: SocketAddr) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    loop {
        let mut conn = listener.accept().await?;
        let peer_addr = conn.peer_addr()?;
        log::info!("executor {:03}, {} connected ", id, peer_addr);
        spawn_local(async move {
            if let Err(err) = echo(&mut conn).await {
                log::error!("echo() loop error: {}", err);
            } else {
                log::info!("executor {:03}, {} disconnected ", id, peer_addr);
            }
        })
        .detach();
    }
}

async fn echo(conn: &mut TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let n = conn.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        conn.write_all(&buf[0..n]).await?;
    }
    Ok(())
}

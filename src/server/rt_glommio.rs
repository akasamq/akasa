use std::future::Future;
use std::io;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use glommio::{
    net::TcpListener,
    spawn_local,
    timer::{sleep, TimerActionRepeat},
    CpuSet, Latency, LocalExecutorPoolBuilder, PoolPlacement, Shares, TaskQueueHandle,
};

use super::handle_accept;
use crate::state::{Executor, GlobalState};

pub fn start(global: Arc<GlobalState>) -> anyhow::Result<()> {
    let cpu_set = CpuSet::online().expect("online cpus");
    let placement = PoolPlacement::MaxSpread(num_cpus::get(), Some(cpu_set));
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
            let executor = Rc::new(GlommioExecutor::new(id, gc_queue));
            loop {
                log::info!("Starting executor {}", id);
                if let Err(err) = server(Rc::clone(&executor), Arc::clone(&global)).await {
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

async fn server(executor: Rc<GlommioExecutor>, global: Arc<GlobalState>) -> io::Result<()> {
    let listener = TcpListener::bind(global.bind)?;
    loop {
        let conn = listener.accept().await?.buffered();
        let fd = conn.as_raw_fd();
        let peer = conn.peer_addr()?;
        log::info!(
            "executor {:03}, #{} {} connected, total {} clients ({} online) ",
            executor.id(),
            fd,
            peer,
            global.clients_count(),
            global.online_clients_count(),
        );
        spawn_local({
            let executor = Rc::clone(&executor);
            let global = Arc::clone(&global);
            handle_accept(conn, peer, executor, global)
        })
        .detach();
    }
}

struct GlommioExecutor {
    id: usize,
    gc_queue: TaskQueueHandle,
}

impl GlommioExecutor {
    fn new(id: usize, gc_queue: TaskQueueHandle) -> GlommioExecutor {
        GlommioExecutor { id, gc_queue }
    }
}

impl Executor for GlommioExecutor {
    fn id(&self) -> usize {
        self.id
    }

    fn spawn_local<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn_local(future).detach();
    }

    fn spawn_timer<G, F>(&self, action_gen: G) -> io::Result<()>
    where
        G: (Fn() -> F) + Send + Sync + 'static,
        F: Future<Output = Option<Duration>> + Send + 'static,
    {
        TimerActionRepeat::repeat_into(action_gen, self.gc_queue)
            .map(|_| ())
            .map_err(|_err| io::Error::from(io::ErrorKind::Other))
    }
}

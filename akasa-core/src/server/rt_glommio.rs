use std::future::Future;
use std::io;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use flume::{bounded, Sender};
use glommio::{
    net::TcpListener,
    spawn_local,
    timer::{sleep, TimerActionRepeat},
    CpuSet, Latency, LocalExecutorPoolBuilder, PoolPlacement, Shares, TaskQueueHandle,
};

use super::handle_accept;
use crate::hook::{Hook, HookRequest, HookService};
use crate::state::{Executor, GlobalState};

pub fn start<H>(hook_handler: H, global: Arc<GlobalState>) -> io::Result<()>
where
    H: Hook + Clone + Send + Sync + 'static,
{
    let cpu_set = CpuSet::online().expect("online cpus");
    let cpu_num = num_cpus::get();
    let placement = PoolPlacement::MaxSpread(cpu_num, Some(cpu_set));
    let (hook_sender, hook_receiver) = bounded(64);
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
            if cpu_num == 1 || id % 2 == 1 {
                log::info!("Starting executor for hook {}", id);
                let hook_service = HookService::new(
                    Rc::clone(&executor),
                    hook_handler.clone(),
                    hook_receiver.clone(),
                    Arc::clone(&global),
                );
                if cpu_num == 1 {
                    spawn_local(hook_service.start()).detach();
                } else {
                    // So that current shard can keep running.
                    hook_service.start().await;
                }
            }

            if cpu_num == 1 || id % 2 == 0 {
                loop {
                    log::info!("Starting executor for server {}", id);
                    if let Err(err) = server(
                        hook_sender.clone(),
                        Rc::clone(&executor),
                        Arc::clone(&global),
                    )
                    .await
                    {
                        log::error!("Executor {} stopped with error: {}", id, err);
                        sleep(Duration::from_secs(1)).await;
                    } else {
                        log::info!("Executor {} stopped successfully!", id);
                    }
                }
            }
        })
        .expect("executor pool")
        .join_all();
    Ok(())
}

async fn server(
    hook_requests: Sender<HookRequest>,
    executor: Rc<GlommioExecutor>,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    let listener = TcpListener::bind(global.config.bind)?;
    loop {
        let conn = listener.accept().await?.buffered();
        let fd = conn.as_raw_fd();
        let peer = conn.peer_addr()?;
        log::debug!("executor {:03}, #{} {} connected", executor.id(), fd, peer);
        spawn_local({
            let hook_requests = hook_requests.clone();
            let executor = Rc::clone(&executor);
            let global = Arc::clone(&global);
            async move {
                let _ =
                    handle_accept(conn, peer, hook_requests, executor, Arc::clone(&global)).await;
            }
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

    fn spawn_sleep<F>(&self, duration: Duration, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        spawn_local(async move {
            sleep(duration).await;
            task.await;
        })
        .detach();
    }

    fn spawn_interval<G, F>(&self, action_gen: G) -> io::Result<()>
    where
        G: (Fn() -> F) + Send + Sync + 'static,
        F: Future<Output = Option<Duration>> + Send + 'static,
    {
        TimerActionRepeat::repeat_into(action_gen, self.gc_queue)
            .map(|_| ())
            .map_err(|_err| io::Error::from(io::ErrorKind::Other))
    }
}

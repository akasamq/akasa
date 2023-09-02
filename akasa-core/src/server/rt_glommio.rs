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

use super::{build_tls_context, handle_accept, ConnectionArgs};
use crate::config::{Listener, ProxyMode, TlsListener};
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

    let mqtts_tls_acceptor = global
        .config
        .listeners
        .mqtts
        .as_ref()
        .map(|listener| {
            log::info!("Building TLS context for mqtts...");
            build_tls_context(listener)
        })
        .transpose()?;
    let wss_tls_acceptor = global
        .config
        .listeners
        .wss
        .as_ref()
        .map(|listener| {
            log::info!("Building TLS context for wss...");
            build_tls_context(listener)
        })
        .transpose()?;

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
                let listeners = &global.config.listeners;
                let tasks: Vec<_> = [
                    listeners
                        .mqtt
                        .as_ref()
                        .map(|Listener { addr, proxy_mode }| ConnectionArgs {
                            addr: *addr,
                            proxy: proxy_mode.is_some(),
                            proxy_tls_termination: *proxy_mode == Some(ProxyMode::TlsTermination),
                            websocket: false,
                            tls_acceptor: None,
                        }),
                    listeners
                        .mqtts
                        .as_ref()
                        .map(|TlsListener { addr, proxy, .. }| ConnectionArgs {
                            addr: *addr,
                            proxy: *proxy,
                            proxy_tls_termination: false,
                            websocket: false,
                            tls_acceptor: mqtts_tls_acceptor.map(Into::into),
                        }),
                    listeners
                        .ws
                        .as_ref()
                        .map(|Listener { addr, proxy_mode }| ConnectionArgs {
                            addr: *addr,
                            proxy: proxy_mode.is_some(),
                            proxy_tls_termination: *proxy_mode == Some(ProxyMode::TlsTermination),
                            websocket: true,
                            tls_acceptor: None,
                        }),
                    listeners
                        .wss
                        .as_ref()
                        .map(|TlsListener { addr, proxy, .. }| ConnectionArgs {
                            addr: *addr,
                            proxy: *proxy,
                            proxy_tls_termination: false,
                            websocket: true,
                            tls_acceptor: wss_tls_acceptor.map(Into::into),
                        }),
                ]
                .into_iter()
                .flatten()
                .map(|conn_args| {
                    let global = Arc::clone(&global);
                    let hook_sender = hook_sender.clone();
                    let executor = Rc::clone(&executor);
                    spawn_local(async move {
                        loop {
                            let global = Arc::clone(&global);
                            let hook_sender = hook_sender.clone();
                            let executor = Rc::clone(&executor);
                            if let Err(err) =
                                listen(conn_args.clone(), hook_sender, executor, global).await
                            {
                                log::error!("Listen error: {:?}", err);
                                sleep(Duration::from_secs(1)).await;
                            }
                        }
                    })
                    .detach()
                })
                .collect();

                if tasks.is_empty() {
                    log::error!("No binding address in config");
                }
                for task in tasks {
                    let _ = task.await;
                }
            }
        })
        .expect("executor pool")
        .join_all();
    Ok(())
}

async fn listen<E: Executor + Send + Sync + 'static>(
    conn_args: ConnectionArgs,
    hook_sender: Sender<HookRequest>,
    executor: Rc<E>,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    let addr = conn_args.addr;
    let listener = TcpListener::bind(addr)?;
    let listen_type = match (conn_args.websocket, conn_args.tls_acceptor.is_some()) {
        (false, false) => "mqtt",
        (false, true) => "mqtts",
        (true, false) => "ws",
        (true, true) => "wss",
    };
    let listen_type = if conn_args.proxy {
        format!("{listen_type}(proxy)")
    } else {
        listen_type.to_owned()
    };
    log::info!("Listen {listen_type}@{addr} success! (glommio)");
    loop {
        let conn = listener.accept().await?.buffered();
        let conn_args = conn_args.clone();
        let fd = conn.as_raw_fd();
        let peer = conn.peer_addr()?;
        log::debug!("executor {:03}, #{} {} connected", executor.id(), fd, peer);
        spawn_local({
            let hook_requests = hook_sender.clone();
            let executor = Rc::clone(&executor);
            let global = Arc::clone(&global);
            async move {
                let _ = handle_accept(
                    conn,
                    conn_args,
                    peer,
                    hook_requests,
                    executor,
                    Arc::clone(&global),
                )
                .await;
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

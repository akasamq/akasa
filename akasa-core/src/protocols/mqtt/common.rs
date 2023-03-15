use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::state::{ClientId, ControlMessage, Executor, GlobalState};

pub(crate) fn start_keep_alive_timer<E: Executor>(
    keep_alive: u16,
    client_id: ClientId,
    last_packet_time: &Arc<RwLock<Instant>>,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    // FIXME: if kee_alive is zero, set a default keep_alive value from config
    if keep_alive > 0 {
        let half_interval = Duration::from_millis(keep_alive as u64 * 500);
        log::debug!("{} keep alive: {:?}", client_id, half_interval * 2);
        let last_packet_time = Arc::clone(last_packet_time);
        let global = Arc::clone(global);
        if let Err(err) = executor.spawn_interval(move || {
            // Need clone twice: https://stackoverflow.com/a/68462908/1274372
            let last_packet_time = Arc::clone(&last_packet_time);
            let global = Arc::clone(&global);
            async move {
                {
                    let last_packet_time = last_packet_time.read();
                    if last_packet_time.elapsed() <= half_interval * 3 {
                        return Some(half_interval);
                    }
                }
                // timeout, kick it out
                if let Some(sender) = global.get_client_control_sender(&client_id) {
                    let msg = ControlMessage::Kick {
                        reason: "timeout".to_owned(),
                    };
                    if let Err(err) = sender.send_async(msg).await {
                        log::warn!(
                            "send timeout kick message to {:?} error: {:?}",
                            client_id,
                            err
                        );
                    }
                }
                None
            }
        }) {
            log::error!("spawn executor keep alive timer failed: {:?}", err);
            return Err(err);
        }
    }
    Ok(())
}

use std::cmp;
use std::io;
use std::sync::Arc;

use futures_lite::io::AsyncWrite;
use mqtt_proto::v3::{Packet, Suback, Subscribe, Unsubscribe};

use crate::state::GlobalState;

use super::super::Session;
use super::common::{recv_publish, write_packet, RecvPublish};

#[inline]
pub(crate) async fn handle_subscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Subscribe,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );
    let mut return_codes = Vec::with_capacity(packet.topics.len());
    for (filter, qos) in packet.topics {
        if filter.is_shared() {
            log::info!("mqtt v3.x don't support shared subscription");
            return Err(io::ErrorKind::InvalidData.into());
        }
        let granted_qos = cmp::min(qos, global.config.max_allowed_qos());
        session.subscribes.insert(filter.clone(), granted_qos);
        global
            .route_table
            .subscribe(&filter, session.client_id, granted_qos);

        for retain in global.retain_table.get_matches(&filter) {
            if retain.qos <= granted_qos {
                recv_publish(
                    session,
                    RecvPublish {
                        topic_name: &retain.topic_name,
                        qos: retain.qos,
                        retain: true,
                        payload: &retain.payload,
                        subscribe_filter: &filter,
                        subscribe_qos: granted_qos,
                    },
                    Some(conn),
                )
                .await?;
            }
        }
        return_codes.push(granted_qos.into());
    }
    let rv_packet = Suback::new(packet.pid, return_codes);
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_unsubscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Unsubscribe,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );
    for filter in packet.topics {
        global.route_table.unsubscribe(&filter, session.client_id);
        session.subscribes.remove(&filter);
    }
    write_packet(session.client_id, conn, &Packet::Unsuback(packet.pid)).await?;
    Ok(())
}

use std::cmp;
use std::io;
use std::sync::Arc;

use mqtt_proto::{
    v3::{Packet, Suback, Subscribe, Unsubscribe},
    QoS,
};

use crate::state::GlobalState;

use super::super::Session;
use super::{
    common::handle_pendings,
    publish::{recv_publish, RecvPublish},
};

#[inline]
pub(crate) fn handle_subscribe(
    session: &mut Session,
    packet: &Subscribe,
    global: &Arc<GlobalState>,
) -> io::Result<Vec<Packet>> {
    log::debug!(
        r#"{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );
    let mut rv_packets = Vec::new();
    let mut return_codes = Vec::with_capacity(packet.topics.len());
    for (filter, qos) in &packet.topics {
        if filter.is_shared() {
            log::info!("mqtt v3.x don't support shared subscription");
            return Err(io::ErrorKind::InvalidData.into());
        }
        let granted_qos = cmp::min(*qos, global.config.max_allowed_qos());
        session.subscribes.insert(filter.clone(), granted_qos);
        global
            .route_table
            .subscribe(filter, session.client_id, granted_qos);

        let mut process_pendings = false;
        for msg in global.retain_table.get_matches(filter) {
            if msg.qos <= granted_qos {
                if let Some((final_qos, packet_opt)) = recv_publish(
                    session,
                    RecvPublish {
                        topic_name: &msg.topic_name,
                        qos: msg.qos,
                        retain: true,
                        payload: &msg.payload,
                        subscribe_filter: filter,
                        subscribe_qos: granted_qos,
                    },
                ) {
                    if let Some(packet) = packet_opt {
                        rv_packets.push(packet);
                    }
                    if final_qos != QoS::Level0 {
                        process_pendings = true;
                    }
                }
            }
        }
        if process_pendings {
            rv_packets.extend(handle_pendings(session));
        }
        return_codes.push(granted_qos.into());
    }
    rv_packets.push(Suback::new(packet.pid, return_codes).into());
    Ok(rv_packets)
}

#[inline]
pub(crate) fn handle_unsubscribe(
    session: &mut Session,
    packet: &Unsubscribe,
    global: &Arc<GlobalState>,
) -> Packet {
    log::debug!(
        r#"{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );
    for filter in &packet.topics {
        global.route_table.unsubscribe(filter, session.client_id);
        session.subscribes.remove(filter);
    }
    Packet::Unsuback(packet.pid)
}

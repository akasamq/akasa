use std::cmp;
use std::sync::Arc;

use mqtt_proto::{
    v5::{
        DisconnectReasonCode, Packet, RetainHandling, Suback, SubackProperties, Subscribe,
        SubscribeReasonCode, Unsuback, UnsubackProperties, Unsubscribe, UnsubscribeReasonCode,
    },
    QoS, MATCH_ALL_CHAR, MATCH_ONE_CHAR,
};

use crate::state::GlobalState;

use super::super::{Session, SubscriptionData};
use super::{
    common::{build_error_disconnect, handle_pendings},
    publish::{recv_publish, RecvPublish},
};

#[inline]
pub(crate) fn handle_subscribe(
    session: &mut Session,
    packet: Subscribe,
    global: &Arc<GlobalState>,
) -> Result<Vec<Packet>, Packet> {
    log::debug!(
        r#"{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );

    let properties = packet.properties;
    if properties.subscription_id.map(|id| id.value()) == Some(0) {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "Subscription identifier value=0 is not allowed",
        );
        return Err(err_pkt);
    }

    let mut rv_packets = Vec::new();

    let reason_codes = if !global.config.subscription_id_available
        && properties.subscription_id.is_some()
    {
        vec![SubscribeReasonCode::SubscriptionIdentifiersNotSupported; packet.topics.len()]
    } else {
        let mut items = Vec::with_capacity(packet.topics.len());
        for (filter, mut sub_opts) in packet.topics {
            let granted_qos = cmp::min(sub_opts.max_qos, global.config.max_allowed_qos());
            let reason_code = if !global.config.shared_subscription_available && filter.is_shared()
            {
                SubscribeReasonCode::SharedSubscriptionNotSupported
            } else if !global.config.wildcard_subscription_available
                && filter.contains(|c| c == MATCH_ONE_CHAR || c == MATCH_ALL_CHAR)
            {
                SubscribeReasonCode::WildcardSubscriptionsNotSupported
            } else {
                match granted_qos {
                    QoS::Level0 => SubscribeReasonCode::GrantedQoS0,
                    QoS::Level1 => SubscribeReasonCode::GrantedQoS1,
                    QoS::Level2 => SubscribeReasonCode::GrantedQoS2,
                }
            };

            if (reason_code as u8) < 0x80 {
                sub_opts.max_qos = granted_qos;
                let new_sub = SubscriptionData::new(sub_opts, properties.subscription_id);
                let old_sub = session.subscribes.insert(filter.clone(), new_sub);
                global
                    .route_table
                    .subscribe(&filter, session.client_id, granted_qos);

                let send_retain = global.config.retain_available
                    && !filter.is_shared()
                    && match sub_opts.retain_handling {
                        RetainHandling::SendAtSubscribe => true,
                        RetainHandling::SendAtSubscribeIfNotExist => old_sub.is_none(),
                        RetainHandling::DoNotSend => false,
                    };
                if send_retain {
                    let mut process_pendings = false;
                    for msg in global.retain_table.get_matches(&filter) {
                        if sub_opts.no_local && msg.client_identifier == session.client_identifier {
                            continue;
                        }
                        let encode_len = if msg.properties.is_none() {
                            // one byte for property length
                            msg.encode_len + 1
                        } else {
                            msg.encode_len
                        };
                        let retain = sub_opts.retain_as_published;
                        if let Some((final_qos, packet_opt)) = recv_publish(
                            session,
                            RecvPublish {
                                topic_name: &msg.topic_name,
                                qos: msg.qos,
                                retain,
                                payload: &msg.payload,
                                subscribe_filter: &filter,
                                subscribe_qos: granted_qos,
                                properties: msg.properties.as_ref(),
                                encode_len,
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
                    if process_pendings {
                        rv_packets.extend(handle_pendings(session));
                    }
                }
            }

            items.push(reason_code);
        }
        items
    };

    // TODO: handle all other SubscribeReasonCode type
    // TODO: handle ReasonString/UserProperty fields

    let rv_packet = Suback {
        pid: packet.pid,
        topics: reason_codes,
        properties: SubackProperties::default(),
    };
    rv_packets.push(rv_packet.into());
    Ok(rv_packets)
}

#[inline]
pub(crate) fn handle_unsubscribe(
    session: &mut Session,
    packet: Unsubscribe,
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
    let mut reason_codes = Vec::with_capacity(packet.topics.len());
    for filter in packet.topics {
        global.route_table.unsubscribe(&filter, session.client_id);
        let reason_code = if session.subscribes.remove(&filter).is_some() {
            UnsubscribeReasonCode::Success
        } else {
            UnsubscribeReasonCode::NoSubscriptionExisted
        };
        reason_codes.push(reason_code);
    }
    Unsuback {
        pid: packet.pid,
        properties: UnsubackProperties::default(),
        topics: reason_codes,
    }
    .into()
}

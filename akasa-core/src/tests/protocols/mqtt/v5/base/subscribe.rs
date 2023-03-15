use std::time::Duration;

use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

#[tokio::test]
async fn test_sub_unsub_simple() {
    let (task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());

    client.connect("client id", true, false).await;
    let sub_topics = vec![
        ("abc/0", SubscriptionOptions::new(QoS::Level0)),
        ("xyz/1", SubscriptionOptions::new(QoS::Level1)),
        ("ijk/2", SubscriptionOptions::new(QoS::Level2)),
    ];
    client.subscribe(23, sub_topics).await;
    client.send_unsubscribe(24, vec!["abc/0", "xxx/+"]).await;
    let unsub_codes = vec![
        UnsubscribeReasonCode::Success,
        UnsubscribeReasonCode::NoSubscriptionExisted,
    ];
    client.recv_unsuback(24, unsub_codes).await;

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscribe_reject_empty_topics() {
    let (task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());

    client.connect("client id", true, false).await;
    client.send_subscribe::<&str>(23, vec![]).await;
    let received_pkt = client.read_packet().await;
    if let Packet::Disconnect(pkt) = received_pkt {
        assert_eq!(pkt.reason_code, DisconnectReasonCode::MalformedPacket);
        assert_eq!(
            pkt.properties.reason_string.unwrap().as_str(),
            "empty subscription"
        );
    } else {
        panic!("invalid received packet: {:?}", received_pkt);
    }

    sleep(Duration::from_millis(10)).await;
    assert!(task.is_finished());
}

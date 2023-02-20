use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::v3::*;
use mqtt_proto::*;
use tokio::time::sleep;
use ConnectReturnCode::*;

use crate::config::Config;
use crate::tests::utils::MockConn;

#[tokio::test]
async fn test_sub_unsub_simple() {
    let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = control.start(conn);

    let connect = Connect::new(Arc::new("client identifier".to_owned()), 10);
    let connack = Connack::new(false, Accepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Connack(connack);
    assert_eq!(packet, expected_packet);

    let sub_pk_id = Pid::try_from(23).unwrap();
    let subscribe = Subscribe::new(
        sub_pk_id,
        vec![
            (
                TopicFilter::try_from("abc/0".to_owned()).unwrap(),
                QoS::Level0,
            ),
            (
                TopicFilter::try_from("xyz/1".to_owned()).unwrap(),
                QoS::Level1,
            ),
            (
                TopicFilter::try_from("ijk/2".to_owned()).unwrap(),
                QoS::Level2,
            ),
        ],
    );
    let suback = Suback::new(
        sub_pk_id,
        vec![
            SubscribeReturnCode::MaxLevel0,
            SubscribeReturnCode::MaxLevel1,
            SubscribeReturnCode::MaxLevel2,
        ],
    );
    control.write_packet(subscribe.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Suback(suback);
    assert_eq!(packet, expected_packet);

    let unsub_pk_id = Pid::try_from(24).unwrap();
    let unsubscribe = Unsubscribe::new(
        unsub_pk_id,
        vec![
            TopicFilter::try_from("abc/0".to_owned()).unwrap(),
            TopicFilter::try_from("xxx/+".to_owned()).unwrap(),
        ],
    );
    control.write_packet(unsubscribe.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Unsuback(unsub_pk_id);
    assert_eq!(packet, expected_packet);

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscribe_reject_empty_topics() {
    let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = control.start(conn);

    let connect = Connect::new(Arc::new("client identifier".to_owned()), 10);
    let connack = Connack::new(false, Accepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Connack(connack);
    assert_eq!(packet, expected_packet);

    let sub_pk_id = Pid::try_from(23).unwrap();
    let subscribe = Subscribe::new(sub_pk_id, vec![]);
    control.write_packet(subscribe.into()).await;

    sleep(Duration::from_millis(10)).await;
    assert!(control.try_read_packet().is_err());
    assert!(task.is_finished());
}

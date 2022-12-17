use std::time::Duration;

use mqtt::{
    control::variable_header::ConnectReturnCode, packet::suback::SubscribeReturnCode, packet::*,
    qos::QualityOfService, TopicFilter,
};
use tokio::time::sleep;
use ConnectReturnCode::*;

use crate::config::Config;
use crate::tests::utils::MockConn;

#[tokio::test]
async fn test_sub_unsub_simple() {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let task = control.start(conn);

    let connect = ConnectPacket::new("client identifier");
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    let sub_pk_id: u16 = 23;
    let subscribe = SubscribePacket::new(
        sub_pk_id,
        vec![
            (TopicFilter::new("abc/0").unwrap(), QualityOfService::Level0),
            (TopicFilter::new("xyz/1").unwrap(), QualityOfService::Level1),
            (TopicFilter::new("ijk/2").unwrap(), QualityOfService::Level2),
        ],
    );
    let suback = SubackPacket::new(
        sub_pk_id,
        vec![
            SubscribeReturnCode::MaximumQoSLevel0,
            SubscribeReturnCode::MaximumQoSLevel1,
            SubscribeReturnCode::MaximumQoSLevel2,
        ],
    );
    control.write_packet(subscribe.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::SubackPacket(suback);
    assert_eq!(packet, expected_packet);

    let unsub_pk_id = 24;
    let unsubscribe = UnsubscribePacket::new(
        unsub_pk_id,
        vec![
            TopicFilter::new("abc/0").unwrap(),
            TopicFilter::new("xxx/+").unwrap(),
        ],
    );
    let unsuback = UnsubackPacket::new(unsub_pk_id);
    control.write_packet(unsubscribe.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::UnsubackPacket(unsuback);
    assert_eq!(packet, expected_packet);

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscribe_reject_empty_topics() {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let task = control.start(conn);

    let connect = ConnectPacket::new("client identifier");
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    let sub_pk_id: u16 = 23;
    let subscribe = SubscribePacket::new(sub_pk_id, vec![]);
    control.write_packet(subscribe.into()).await;

    sleep(Duration::from_millis(10)).await;
    assert!(control.try_read_packet().is_err());
    assert!(task.is_finished());
}

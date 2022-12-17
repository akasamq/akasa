use std::sync::Arc;
use std::time::Duration;

use mqtt::{
    control::variable_header::ConnectReturnCode, packet::publish::QoSWithPacketIdentifier,
    packet::suback::SubscribeReturnCode, packet::*, qos::QualityOfService, TopicFilter, TopicName,
};
use tokio::time::sleep;
use ConnectReturnCode::*;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

#[tokio::test]
async fn test_retain_simple() {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let task = control.start(conn);

    let connect = ConnectPacket::new("client identifier");
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    let pub_pk_id: u16 = 22;
    let mut publish = PublishPacket::new(
        TopicName::new("xyz/1").unwrap(),
        QoSWithPacketIdentifier::Level1(pub_pk_id),
        vec![3, 5, 55],
    );
    publish.set_retain(true);
    let puback = PubackPacket::new(pub_pk_id);
    control.write_packet(publish.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::PubackPacket(puback);
    assert_eq!(packet, expected_packet);

    let sub_pk_id: u16 = 23;
    let subscribe = SubscribePacket::new(
        sub_pk_id,
        vec![
            (TopicFilter::new("abc/0").unwrap(), QualityOfService::Level0),
            (TopicFilter::new("xyz/1").unwrap(), QualityOfService::Level1),
        ],
    );
    control.write_packet(subscribe.into()).await;

    let pub_pk_id: u16 = 0;
    let mut publish = PublishPacket::new(
        TopicName::new("xyz/1").unwrap(),
        QoSWithPacketIdentifier::Level1(pub_pk_id),
        vec![3, 5, 55],
    );
    publish.set_retain(true);
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::PublishPacket(publish);
    assert_eq!(packet, expected_packet);

    let suback = SubackPacket::new(
        sub_pk_id,
        vec![
            SubscribeReturnCode::MaximumQoSLevel0,
            SubscribeReturnCode::MaximumQoSLevel1,
        ],
    );
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::SubackPacket(suback);
    assert_eq!(packet, expected_packet);

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_retain_different_clients() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, global);
    let task2 = control2.start(conn2);

    let connack = ConnackPacket::new(false, ConnectionAccepted);
    // client 1: publish retain message
    {
        let connect1 = ConnectPacket::new("client identifier 1");
        control1.write_packet(connect1.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack.clone());
        assert_eq!(packet, expected_packet);

        let pub_pk_id: u16 = 11;
        let mut publish = PublishPacket::new(
            TopicName::new("xyz/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        publish.set_retain(true);
        let puback = PubackPacket::new(pub_pk_id);
        control1.write_packet(publish.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::PubackPacket(puback);
        assert_eq!(packet, expected_packet);
    }

    // client 2: subscribe and received a retain message
    {
        let connect2 = ConnectPacket::new("client identifier 2");
        control2.write_packet(connect2.into()).await;
        let packet = control2.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack.clone());
        assert_eq!(packet, expected_packet);

        // subscribe multiple times
        for (sub_pk_id, pub_pk_id) in [(22, 0), (23, 1)] {
            let subscribe = SubscribePacket::new(
                sub_pk_id,
                vec![
                    (TopicFilter::new("abc/0").unwrap(), QualityOfService::Level0),
                    (TopicFilter::new("xyz/1").unwrap(), QualityOfService::Level1),
                ],
            );
            control2.write_packet(subscribe.into()).await;

            let mut publish = PublishPacket::new(
                TopicName::new("xyz/1").unwrap(),
                QoSWithPacketIdentifier::Level1(pub_pk_id),
                vec![3, 5, 55],
            );
            publish.set_retain(true);
            let packet = control2.read_packet().await;
            let expected_packet = VariablePacket::PublishPacket(publish);
            assert_eq!(packet, expected_packet);

            let suback = SubackPacket::new(
                sub_pk_id,
                vec![
                    SubscribeReturnCode::MaximumQoSLevel0,
                    SubscribeReturnCode::MaximumQoSLevel1,
                ],
            );
            let packet = control2.read_packet().await;
            let expected_packet = VariablePacket::SubackPacket(suback);
            assert_eq!(packet, expected_packet);
        }
    }

    sleep(Duration::from_millis(10)).await;
    assert!(!task1.is_finished());
    assert!(!task2.is_finished());
}

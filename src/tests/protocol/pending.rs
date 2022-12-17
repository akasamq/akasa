use std::sync::Arc;
use std::time::Duration;

use mqtt::{
    control::variable_header::ConnectReturnCode, packet::publish::QoSWithPacketIdentifier,
    packet::suback::SubscribeReturnCode, packet::*, qos::QualityOfService, TopicFilter, TopicName,
};
use tokio::sync::oneshot;
use tokio::time::sleep;
use ConnectReturnCode::*;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

#[tokio::test]
async fn test_pending_qos0() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        let connect = ConnectPacket::new("client identifier 1");
        let connack = ConnackPacket::new(false, ConnectionAccepted);
        control1.write_packet(connect.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack);
        assert_eq!(packet, expected_packet);

        rx.await.unwrap();
        for _ in 0..4 {
            sleep(Duration::from_millis(100)).await;
            let publish = PublishPacket::new(
                TopicName::new("xyz/0").unwrap(),
                QoSWithPacketIdentifier::Level0,
                vec![3, 5, 55],
            );
            control1.write_packet(publish.into()).await;
            assert!(control1.try_read_packet_is_empty());
        }
    });

    // client 2: subscriber
    let mut connect = ConnectPacket::new("client identifier 2");
    connect.set_clean_session(false);
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control2.write_packet(connect.clone().into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    // subscribe to "xyz/0"
    let sub_pk_id = 2;
    let subscribe = SubscribePacket::new(
        sub_pk_id,
        vec![(TopicFilter::new("xyz/0").unwrap(), QualityOfService::Level0)],
    );
    let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel0]);
    control2.write_packet(subscribe.into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::SubackPacket(suback);
    assert_eq!(packet, expected_packet);

    let disconnect = DisconnectPacket::new();
    control2.write_packet(disconnect.into()).await;

    tx.send(()).unwrap();

    // !!! FIXME: what if the client blocking the IO and the keep-alive will not work
    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    // reconnect
    let (conn2, mut control2) = MockConn::new_with_global(444, global);
    let _task2 = control2.start(conn2);

    let connack = ConnackPacket::new(true, ConnectionAccepted);
    control2.write_packet(connect.into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);
    assert!(control2.try_read_packet_is_empty());
}

#[tokio::test]
async fn test_pending_qos1() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        let connect = ConnectPacket::new("client identifier 1");
        let connack = ConnackPacket::new(false, ConnectionAccepted);
        control1.write_packet(connect.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack);
        assert_eq!(packet, expected_packet);

        rx.await.unwrap();
        for pub_pk_id in 0..4u16 {
            sleep(Duration::from_millis(100)).await;
            let publish = PublishPacket::new(
                TopicName::new("xyz/1").unwrap(),
                QoSWithPacketIdentifier::Level1(pub_pk_id),
                vec![3, 5, 55],
            );
            control1.write_packet(publish.into()).await;
            let puback = PubackPacket::new(pub_pk_id);
            let expected_packet = VariablePacket::PubackPacket(puback);
            let packet = control1.read_packet().await;
            assert_eq!(packet, expected_packet);
        }
    });

    // client 2: subscriber
    let mut connect = ConnectPacket::new("client identifier 2");
    connect.set_clean_session(false);
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control2.write_packet(connect.clone().into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    // subscribe to "xyz/1"
    let sub_pk_id = 2;
    let subscribe = SubscribePacket::new(
        sub_pk_id,
        vec![(TopicFilter::new("xyz/1").unwrap(), QualityOfService::Level1)],
    );
    let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel1]);
    control2.write_packet(subscribe.into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::SubackPacket(suback);
    assert_eq!(packet, expected_packet);

    let disconnect = DisconnectPacket::new();
    control2.write_packet(disconnect.into()).await;

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    // reconnect
    let (conn2, mut control2) = MockConn::new_with_global(444, global);
    let _task2 = control2.start(conn2);

    let connack = ConnackPacket::new(true, ConnectionAccepted);
    control2.write_packet(connect.into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    for pub_pk_id in 0..4u16 {
        let publish = PublishPacket::new(
            TopicName::new("xyz/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        let expected_packet = VariablePacket::PublishPacket(publish);
        let packet = control2.read_packet().await;
        assert_eq!(packet, expected_packet);
    }
    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());
}

#[tokio::test]
async fn test_pending_max_inflight_qos1() {
    let mut config = Config::default();
    config.max_inflight = 8;
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        config.clone(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        let connect = ConnectPacket::new("client identifier 1");
        let connack = ConnackPacket::new(false, ConnectionAccepted);
        control1.write_packet(connect.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack);
        assert_eq!(packet, expected_packet);

        rx.await.unwrap();
        for pub_pk_id in 0..14u16 {
            sleep(Duration::from_millis(100)).await;
            let publish = PublishPacket::new(
                TopicName::new("xyz/1").unwrap(),
                QoSWithPacketIdentifier::Level1(pub_pk_id),
                vec![3, 5, 55],
            );
            control1.write_packet(publish.into()).await;
            let puback = PubackPacket::new(pub_pk_id);
            let expected_packet = VariablePacket::PubackPacket(puback);
            let packet = control1.read_packet().await;
            assert_eq!(packet, expected_packet);
        }
    });

    // client 2: subscriber
    let mut connect = ConnectPacket::new("client identifier 2");
    connect.set_clean_session(false);
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control2.write_packet(connect.clone().into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    // subscribe to "xyz/1"
    let sub_pk_id = 2;
    let subscribe = SubscribePacket::new(
        sub_pk_id,
        vec![(TopicFilter::new("xyz/1").unwrap(), QualityOfService::Level1)],
    );
    let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel1]);
    control2.write_packet(subscribe.into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = VariablePacket::SubackPacket(suback);
    assert_eq!(packet, expected_packet);

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    for pub_pk_id in 0..8u16 {
        let publish = PublishPacket::new(
            TopicName::new("xyz/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        let expected_packet = VariablePacket::PublishPacket(publish);
        let packet = control2.read_packet().await;
        assert_eq!(packet, expected_packet);
    }

    // Reach max inflight, we can not receive more publish packet
    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());

    for pub_pk_id in 0..8u16 {
        let puback = PubackPacket::new(pub_pk_id);
        control2.write_packet(puback.into()).await;
    }
    for pub_pk_id in 8..14u16 {
        let publish = PublishPacket::new(
            TopicName::new("xyz/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        let expected_packet = VariablePacket::PublishPacket(publish);
        let packet = control2.read_packet().await;
        assert_eq!(packet, expected_packet);
    }

    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());
}

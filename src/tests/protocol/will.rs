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
async fn test_will_publish() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, global);
    let task2 = control2.start(conn2);

    let connack = ConnackPacket::new(false, ConnectionAccepted);
    // client 1: subscribe to "topic/1"
    {
        let connect1 = ConnectPacket::new("client identifier 1");
        control1.write_packet(connect1.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack.clone());
        assert_eq!(packet, expected_packet);

        let subscribe = SubscribePacket::new(
            11,
            vec![(
                TopicFilter::new("topic/1").unwrap(),
                QualityOfService::Level1,
            )],
        );
        let suback = SubackPacket::new(11, vec![SubscribeReturnCode::MaximumQoSLevel1]);
        control1.write_packet(subscribe.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::SubackPacket(suback);
        assert_eq!(packet, expected_packet);

        assert!(control1.try_read_packet_is_empty());
    }
    // client 2: connect with will topic "topic/1" and unexpected disconnect
    {
        // connect accepted: with will
        let mut connect2 = ConnectPacket::new("client identifier 2");
        connect2.set_will(Some((TopicName::new("topic/1").unwrap(), vec![1, 2, 3, 4])));
        connect2.set_will_qos(1);
        control2.write_packet(connect2.into()).await;
        let packet = control2.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack.clone());
        assert_eq!(packet, expected_packet);

        control2.write_data(b"".to_vec()).await;
        sleep(Duration::from_millis(10)).await;
        assert!(task2.is_finished());
        assert!(task2.await.unwrap().is_ok())
    }

    let publish = PublishPacket::new(
        TopicName::new("topic/1").unwrap(),
        QoSWithPacketIdentifier::Level1(0),
        vec![1, 2, 3, 4],
    );
    let packet = control1.read_packet().await;
    let expected_packet = VariablePacket::PublishPacket(publish);
    assert_eq!(packet, expected_packet);
    assert!(!task1.is_finished());
}

#[tokio::test]
async fn test_will_disconnect_not_publish() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, global);
    let task2 = control2.start(conn2);

    let connack = ConnackPacket::new(false, ConnectionAccepted);
    // client 1: subscribe to "topic/1"
    {
        let connect1 = ConnectPacket::new("client identifier 1");
        control1.write_packet(connect1.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack.clone());
        assert_eq!(packet, expected_packet);

        let subscribe = SubscribePacket::new(
            11,
            vec![(
                TopicFilter::new("topic/1").unwrap(),
                QualityOfService::Level1,
            )],
        );
        let suback = SubackPacket::new(11, vec![SubscribeReturnCode::MaximumQoSLevel1]);
        control1.write_packet(subscribe.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = VariablePacket::SubackPacket(suback);
        assert_eq!(packet, expected_packet);

        assert!(control1.try_read_packet_is_empty());
    }
    // client 2: connect with will topic "topic/1" and normal disconnect
    {
        // connect accepted: with will
        let mut connect2 = ConnectPacket::new("client identifier 2");
        connect2.set_will(Some((TopicName::new("topic/1").unwrap(), vec![1, 2, 3, 4])));
        connect2.set_will_qos(1);
        control2.write_packet(connect2.into()).await;
        let packet = control2.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack.clone());
        assert_eq!(packet, expected_packet);

        let disconnect = DisconnectPacket::new();
        control2.write_packet(disconnect.into()).await;
        sleep(Duration::from_millis(10)).await;
        assert!(task2.is_finished());
        assert!(task2.await.unwrap().is_ok())
    }

    sleep(Duration::from_millis(10)).await;
    assert!(control1.try_read_packet_is_empty());
    assert!(!task1.is_finished());
}

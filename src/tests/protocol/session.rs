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

async fn test_clean_session(clean_session: bool, reconnect_clean_session: bool) {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let task = control.start(conn);

    let client_identifier = "client identifier";
    let mut connect = ConnectPacket::new(client_identifier);
    connect.set_clean_session(clean_session);
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

    // for increase the server_packet_id field
    {
        let sub_pk_id: u16 = 11;
        let subscribe = SubscribePacket::new(
            sub_pk_id,
            vec![(TopicFilter::new("abc/1").unwrap(), QualityOfService::Level1)],
        );
        let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel1]);
        control.write_packet(subscribe.into()).await;
        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::SubackPacket(suback);
        assert_eq!(packet, expected_packet);

        let pub_pk_id: u16 = 12;
        let publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        let puback = PubackPacket::new(pub_pk_id);
        let received_publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(0),
            vec![3, 5, 55],
        );
        let received_puback = PubackPacket::new(0);
        control.write_packet(publish.into()).await;

        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::PubackPacket(puback);
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::PublishPacket(received_publish);
        assert_eq!(packet, expected_packet);

        control.write_packet(received_puback.into()).await;
    }
    let disconnect = DisconnectPacket::new();
    control.write_packet(disconnect.into()).await;
    sleep(Duration::from_millis(10)).await;
    assert!(task.is_finished());

    let (conn, mut control) = MockConn::new_with_global(4444, control.global);
    let _task = control.start(conn);
    let mut connect = ConnectPacket::new(client_identifier);
    // connect.set_clean_session(clean_session);
    connect.set_clean_session(reconnect_clean_session);

    let connack = ConnackPacket::new(
        !(clean_session || reconnect_clean_session),
        ConnectionAccepted,
    );
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);
    {
        let sub_pk_id: u16 = 11;
        let subscribe = SubscribePacket::new(
            sub_pk_id,
            vec![(TopicFilter::new("abc/1").unwrap(), QualityOfService::Level1)],
        );
        let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel1]);
        control.write_packet(subscribe.into()).await;
        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::SubackPacket(suback);
        assert_eq!(packet, expected_packet);

        let pub_pk_id: u16 = 12;
        let publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        let puback = PubackPacket::new(pub_pk_id);
        let received_pk_id = if clean_session || reconnect_clean_session {
            0
        } else {
            1
        };
        let received_publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(received_pk_id),
            vec![3, 5, 55],
        );
        let received_puback = PubackPacket::new(received_pk_id);
        control.write_packet(publish.into()).await;
        control.write_packet(received_puback.into()).await;

        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::PubackPacket(puback);
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::PublishPacket(received_publish);
        assert_eq!(packet, expected_packet);
    }
}

#[tokio::test]
async fn test_session_not_persist() {
    test_clean_session(true, false).await;
    test_clean_session(true, true).await;
}

#[tokio::test]
async fn test_session_persist() {
    test_clean_session(false, false).await;
}

#[tokio::test]
async fn test_session_take_over() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
    ));
    let client_identifier = "client identifier";

    let (conn, mut control) = MockConn::new_with_global(111, Arc::clone(&global));
    let task = control.start(conn);
    // client first connection
    {
        let mut connect = ConnectPacket::new(client_identifier);
        connect.set_clean_session(false);
        let connack = ConnackPacket::new(false, ConnectionAccepted);
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack);
        assert_eq!(packet, expected_packet);

        let sub_pk_id: u16 = 11;
        let subscribe = SubscribePacket::new(
            sub_pk_id,
            vec![(TopicFilter::new("abc/1").unwrap(), QualityOfService::Level1)],
        );
        let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel1]);
        control.write_packet(subscribe.into()).await;
        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::SubackPacket(suback);
        assert_eq!(packet, expected_packet);

        let pub_pk_id: u16 = 12;
        let publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        let puback = PubackPacket::new(pub_pk_id);
        let received_publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(0),
            vec![3, 5, 55],
        );
        let received_puback = PubackPacket::new(0);
        control.write_packet(publish.into()).await;

        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::PubackPacket(puback);
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet().await;
        let expected_packet = VariablePacket::PublishPacket(received_publish);
        assert_eq!(packet, expected_packet);

        control.write_packet(received_puback.into()).await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let task2 = control2.start(conn2);
    // client second connection
    {
        let mut connect = ConnectPacket::new(client_identifier);
        connect.set_clean_session(false);
        let connack = ConnackPacket::new(true, ConnectionAccepted);
        control2.write_packet(connect.into()).await;
        let packet = control2.read_packet().await;
        let expected_packet = VariablePacket::ConnackPacket(connack);
        assert_eq!(packet, expected_packet);

        let sub_pk_id: u16 = 11;
        let subscribe = SubscribePacket::new(
            sub_pk_id,
            vec![(TopicFilter::new("abc/1").unwrap(), QualityOfService::Level1)],
        );
        let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel1]);
        control2.write_packet(subscribe.into()).await;
        let packet = control2.read_packet().await;
        let expected_packet = VariablePacket::SubackPacket(suback);
        assert_eq!(packet, expected_packet);

        let pub_pk_id: u16 = 12;
        let publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55],
        );
        let puback = PubackPacket::new(pub_pk_id);
        let received_publish = PublishPacket::new(
            TopicName::new("abc/1").unwrap(),
            QoSWithPacketIdentifier::Level1(1),
            vec![3, 5, 55],
        );
        let received_puback = PubackPacket::new(1);
        control2.write_packet(publish.into()).await;

        let packet = control2.read_packet().await;
        let expected_packet = VariablePacket::PubackPacket(puback);
        assert_eq!(packet, expected_packet);

        let packet = control2.read_packet().await;
        let expected_packet = VariablePacket::PublishPacket(received_publish);
        assert_eq!(packet, expected_packet);

        control2.write_packet(received_puback.into()).await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
    assert!(!task2.is_finished());
}

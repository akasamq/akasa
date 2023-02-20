use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::time::sleep;
use ConnectReasonCode::*;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::assert_connack;

async fn test_clean_start(clean_start: bool, reconnect_clean_start: bool) {
    let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = control.start(conn);

    let client_identifier = Arc::new("client identifier".to_owned());
    let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
    connect.clean_start = clean_start;
    if !clean_start {
        connect.properties = ConnectProperties {
            session_expiry_interval: Some(60),
            ..Default::default()
        };
    }
    let connack = Connack::new(false, Success);
    control.write_packet_v5(connect.into()).await;
    let packet = control.read_packet_v5().await;
    assert_connack!(packet, connack);

    // for increase the server_packet_id field
    {
        let sub_pk_id = Pid::try_from(11).unwrap();
        let subscribe = Subscribe::new(
            sub_pk_id,
            vec![(
                TopicFilter::try_from("abc/1".to_owned()).unwrap(),
                SubscriptionOptions::new(QoS::Level1),
            )],
        );
        let suback = Suback::new(sub_pk_id, vec![SubscribeReasonCode::GrantedQoS1]);
        control.write_packet_v5(subscribe.into()).await;
        let packet = control.read_packet_v5().await;
        let expected_packet = Packet::Suback(suback);
        assert_eq!(packet, expected_packet);

        let pub_pk_id = Pid::try_from(12).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pk_id),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let received_publish = Publish::new(
            QosPid::Level1(Pid::default()),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        control.write_packet_v5(publish.into()).await;

        let packet = control.read_packet_v5().await;
        let expected_packet = Puback::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet_v5().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        control
            .write_packet_v5(Puback::new_success(Pid::default()).into())
            .await;
    }
    control
        .write_packet_v5(Disconnect::new_normal().into())
        .await;
    sleep(Duration::from_millis(10)).await;
    assert!(task.is_finished());

    let (conn, mut control) = MockConn::new_with_global(4444, control.global);
    let _task = control.start(conn);
    let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
    connect.clean_start = reconnect_clean_start;

    let session_present = !(clean_start || reconnect_clean_start);
    let connack = Connack::new(session_present, Success);
    control.write_packet_v5(connect.into()).await;
    let packet = control.read_packet_v5().await;
    assert_connack!(packet, connack);
    {
        if !session_present {
            let sub_pk_id = Pid::try_from(11).unwrap();
            let subscribe = Subscribe::new(
                sub_pk_id,
                vec![(
                    TopicFilter::try_from("abc/1".to_owned()).unwrap(),
                    SubscriptionOptions::new(QoS::Level1),
                )],
            );
            let suback = Suback::new(sub_pk_id, vec![SubscribeReasonCode::GrantedQoS1]);
            control.write_packet_v5(subscribe.into()).await;
            let packet = control.read_packet_v5().await;
            let expected_packet = Packet::Suback(suback);
            assert_eq!(packet, expected_packet);
        }

        let pub_pk_id = Pid::try_from(12).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pk_id),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let received_pk_id = if clean_start || reconnect_clean_start {
            Pid::default()
        } else {
            Pid::default() + 1
        };
        let received_publish = Publish::new(
            QosPid::Level1(received_pk_id),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        control.write_packet_v5(publish.into()).await;
        control
            .write_packet_v5(Puback::new_success(received_pk_id).into())
            .await;

        let packet = control.read_packet_v5().await;
        let expected_packet = Puback::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet_v5().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);
    }
}

#[tokio::test]
async fn test_session_not_persist() {
    test_clean_start(true, false).await;
    test_clean_start(true, true).await;
}

#[tokio::test]
async fn test_session_persist() {
    test_clean_start(false, false).await;
}

#[tokio::test]
async fn test_session_take_over() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let client_identifier = Arc::new("client identifier".to_owned());

    let (conn, mut control) = MockConn::new_with_global(111, Arc::clone(&global));
    let task = control.start(conn);
    // client first connection
    {
        let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
        connect.clean_start = false;
        connect.properties = ConnectProperties {
            session_expiry_interval: Some(60),
            ..Default::default()
        };
        let connack = Connack::new(false, Success);
        control.write_packet_v5(connect.into()).await;
        let packet = control.read_packet_v5().await;
        assert_connack!(packet, connack);

        let sub_pk_id = Pid::try_from(11).unwrap();
        let subscribe = Subscribe::new(
            sub_pk_id,
            vec![(
                TopicFilter::try_from("abc/1".to_owned()).unwrap(),
                SubscriptionOptions::new(QoS::Level1),
            )],
        );
        let suback = Suback::new(sub_pk_id, vec![SubscribeReasonCode::GrantedQoS1]);
        control.write_packet_v5(subscribe.into()).await;
        let packet = control.read_packet_v5().await;
        let expected_packet = Packet::Suback(suback);
        assert_eq!(packet, expected_packet);

        let pub_pk_id = Pid::try_from(12).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pk_id),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let received_publish = Publish::new(
            QosPid::Level1(Pid::default()),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        control.write_packet_v5(publish.into()).await;

        let packet = control.read_packet_v5().await;
        let expected_packet = Puback::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet_v5().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        control
            .write_packet_v5(Puback::new_success(Pid::default()).into())
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let task2 = control2.start(conn2);
    // client second connection
    {
        let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
        connect.clean_start = false;
        connect.properties = ConnectProperties {
            session_expiry_interval: Some(60),
            ..Default::default()
        };
        let connack = Connack::new(true, Success);
        control2.write_packet_v5(connect.into()).await;
        let packet = control2.read_packet_v5().await;
        assert_connack!(packet, connack);

        let pub_pk_id = Pid::try_from(12).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pk_id),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let received_publish = Publish::new(
            QosPid::Level1(Pid::try_from(2).unwrap()),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        control2.write_packet_v5(publish.into()).await;

        let packet = control2.read_packet_v5().await;
        let expected_packet = Puback::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);

        let packet = control2.read_packet_v5().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        control2
            .write_packet_v5(Puback::new_success(Pid::try_from(2).unwrap()).into())
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
    assert!(!task2.is_finished());
}

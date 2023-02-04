use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v3::*;
use mqtt_proto::*;
use tokio::time::sleep;
use ConnectReturnCode::*;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

async fn test_clean_session(clean_session: bool, reconnect_clean_session: bool) {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let task = control.start(conn);

    let client_identifier = Arc::new("client identifier".to_owned());
    let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
    connect.clean_session = clean_session;
    let connack = Connack::new(false, Accepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Connack(connack);
    assert_eq!(packet, expected_packet);

    // for increase the server_packet_id field
    {
        let sub_pk_id = Pid::try_from(11).unwrap();
        let subscribe = Subscribe::new(
            sub_pk_id,
            vec![(
                TopicFilter::try_from("abc/1".to_owned()).unwrap(),
                QoS::Level1,
            )],
        );
        let suback = Suback::new(sub_pk_id, vec![SubscribeReturnCode::MaxLevel1]);
        control.write_packet(subscribe.into()).await;
        let packet = control.read_packet().await;
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
        control.write_packet(publish.into()).await;

        let packet = control.read_packet().await;
        let expected_packet = Packet::Puback(pub_pk_id);
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        control.write_packet(Packet::Puback(Pid::default())).await;
    }
    control.write_packet(Packet::Disconnect).await;
    sleep(Duration::from_millis(10)).await;
    assert!(task.is_finished());

    let (conn, mut control) = MockConn::new_with_global(4444, control.global);
    let _task = control.start(conn);
    let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
    connect.clean_session = reconnect_clean_session;

    let session_present = !(clean_session || reconnect_clean_session);
    let connack = Connack::new(session_present, Accepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Connack(connack);
    assert_eq!(packet, expected_packet);
    {
        if !session_present {
            let sub_pk_id = Pid::try_from(11).unwrap();
            let subscribe = Subscribe::new(
                sub_pk_id,
                vec![(
                    TopicFilter::try_from("abc/1".to_owned()).unwrap(),
                    QoS::Level1,
                )],
            );
            let suback = Suback::new(sub_pk_id, vec![SubscribeReturnCode::MaxLevel1]);
            control.write_packet(subscribe.into()).await;
            let packet = control.read_packet().await;
            let expected_packet = Packet::Suback(suback);
            assert_eq!(packet, expected_packet);
        }

        let pub_pk_id = Pid::try_from(12).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pk_id),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let received_pk_id = if clean_session || reconnect_clean_session {
            Pid::default()
        } else {
            Pid::default() + 1
        };
        let received_publish = Publish::new(
            QosPid::Level1(received_pk_id),
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        control.write_packet(publish.into()).await;
        control.write_packet(Packet::Puback(received_pk_id)).await;

        let packet = control.read_packet().await;
        let expected_packet = Packet::Puback(pub_pk_id);
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet().await;
        let expected_packet = Packet::Publish(received_publish);
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
    let client_identifier = Arc::new("client identifier".to_owned());

    let (conn, mut control) = MockConn::new_with_global(111, Arc::clone(&global));
    let task = control.start(conn);
    // client first connection
    {
        let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
        connect.clean_session = false;
        let connack = Connack::new(false, Accepted);
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet = Packet::Connack(connack);
        assert_eq!(packet, expected_packet);

        let sub_pk_id = Pid::try_from(11).unwrap();
        let subscribe = Subscribe::new(
            sub_pk_id,
            vec![(
                TopicFilter::try_from("abc/1".to_owned()).unwrap(),
                QoS::Level1,
            )],
        );
        let suback = Suback::new(sub_pk_id, vec![SubscribeReturnCode::MaxLevel1]);
        control.write_packet(subscribe.into()).await;
        let packet = control.read_packet().await;
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
        control.write_packet(publish.into()).await;

        let packet = control.read_packet().await;
        let expected_packet = Packet::Puback(pub_pk_id);
        assert_eq!(packet, expected_packet);

        let packet = control.read_packet().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        control.write_packet(Packet::Puback(Pid::default())).await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let task2 = control2.start(conn2);
    // client second connection
    {
        let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
        connect.clean_session = false;
        let connack = Connack::new(true, Accepted);
        control2.write_packet(connect.into()).await;
        let packet = control2.read_packet().await;
        let expected_packet = Packet::Connack(connack);
        assert_eq!(packet, expected_packet);

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
        control2.write_packet(publish.into()).await;

        let packet = control2.read_packet().await;
        let expected_packet = Packet::Puback(pub_pk_id);
        assert_eq!(packet, expected_packet);

        let packet = control2.read_packet().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        control2
            .write_packet(Packet::Puback(Pid::try_from(2).unwrap()))
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
    assert!(!task2.is_finished());
}

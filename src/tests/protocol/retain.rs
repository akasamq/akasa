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

#[tokio::test]
async fn test_retain_simple() {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let task = control.start(conn);

    let connect = Connect::new(Arc::new("client identifier".to_owned()), 10);
    let connack = Connack::new(false, Accepted);
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Connack(connack);
    assert_eq!(packet, expected_packet);

    let pub_pk_id = Pid::try_from(22).unwrap();
    let mut publish = Publish::new(
        QosPid::Level1(pub_pk_id),
        TopicName::try_from("xyz/1".to_owned()).unwrap(),
        Bytes::from(vec![3, 5, 55]),
    );
    publish.retain = true;
    control.write_packet(publish.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Puback(pub_pk_id);
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
        ],
    );
    control.write_packet(subscribe.into()).await;

    let pub_pk_id = Pid::default();
    let mut publish = Publish::new(
        QosPid::Level1(pub_pk_id),
        TopicName::try_from("xyz/1".to_owned()).unwrap(),
        Bytes::from(vec![3, 5, 55]),
    );
    publish.retain = true;
    let packet = control.read_packet().await;
    let expected_packet = Packet::Publish(publish);
    assert_eq!(packet, expected_packet);

    let suback = Suback::new(
        sub_pk_id,
        vec![
            SubscribeReturnCode::MaxLevel0,
            SubscribeReturnCode::MaxLevel1,
        ],
    );
    let packet = control.read_packet().await;
    let expected_packet = Packet::Suback(suback);
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

    let connack = Connack::new(false, Accepted);
    // client 1: publish retain message
    {
        let connect1 = Connect::new(Arc::new("client identifier 1".to_owned()), 10);
        control1.write_packet(connect1.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = Packet::Connack(connack.clone());
        assert_eq!(packet, expected_packet);

        let pub_pk_id = Pid::try_from(11).unwrap();
        let mut publish = Publish::new(
            QosPid::Level1(pub_pk_id),
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        publish.retain = true;
        control1.write_packet(publish.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = Packet::Puback(pub_pk_id);
        assert_eq!(packet, expected_packet);
    }

    // client 2: subscribe and received a retain message
    {
        let connect2 = Connect::new(Arc::new("client identifier 2".to_owned()), 10);
        control2.write_packet(connect2.into()).await;
        let packet = control2.read_packet().await;
        let expected_packet = Packet::Connack(connack.clone());
        assert_eq!(packet, expected_packet);

        // subscribe multiple times
        for (sub_pk_id, pub_pk_id) in [(22, 1), (23, 2)] {
            let sub_pk_id = Pid::try_from(sub_pk_id).unwrap();
            let pub_pk_id = Pid::try_from(pub_pk_id).unwrap();
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
                ],
            );
            control2.write_packet(subscribe.into()).await;

            let mut publish = Publish::new(
                QosPid::Level1(pub_pk_id),
                TopicName::try_from("xyz/1".to_owned()).unwrap(),
                Bytes::from(vec![3, 5, 55]),
            );
            publish.retain = true;
            let packet = control2.read_packet().await;
            let expected_packet = Packet::Publish(publish);
            assert_eq!(packet, expected_packet);

            let suback = Suback::new(
                sub_pk_id,
                vec![
                    SubscribeReturnCode::MaxLevel0,
                    SubscribeReturnCode::MaxLevel1,
                ],
            );
            let packet = control2.read_packet().await;
            let expected_packet = Packet::Suback(suback);
            assert_eq!(packet, expected_packet);
        }
    }

    sleep(Duration::from_millis(10)).await;
    assert!(!task1.is_finished());
    assert!(!task2.is_finished());
}

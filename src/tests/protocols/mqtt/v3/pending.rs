use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v3::*;
use mqtt_proto::*;
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
        Config::new_allow_anonymous(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        let connect = Connect::new(Arc::new("client identifier 1".to_owned()), 10);
        let connack = Connack::new(false, Accepted);
        control1.write_packet(connect.into()).await;
        let packet = control1.read_packet().await;
        assert_eq!(packet, Packet::Connack(connack));

        rx.await.unwrap();
        for _ in 0..4 {
            sleep(Duration::from_millis(100)).await;
            let publish = Publish::new(
                QosPid::Level0,
                TopicName::try_from("xyz/0".to_owned()).unwrap(),
                Bytes::from(vec![3, 5, 55]),
            );
            control1.write_packet(publish.into()).await;
            assert!(control1.try_read_packet_is_empty());
        }
    });

    // client 2: subscriber
    let mut connect = Connect::new(Arc::new("client identifier 2".to_owned()), 10);
    connect.clean_session = false;
    let connack = Connack::new(false, Accepted);
    control2.write_packet(connect.clone().into()).await;
    let packet = control2.read_packet().await;
    assert_eq!(packet, Packet::Connack(connack));

    // subscribe to "xyz/0"
    let sub_pid = Pid::try_from(2).unwrap();
    let subscribe = Subscribe::new(
        sub_pid,
        vec![(
            TopicFilter::try_from("xyz/0".to_owned()).unwrap(),
            QoS::Level0,
        )],
    );
    let suback = Suback::new(sub_pid, vec![SubscribeReturnCode::MaxLevel0]);
    control2.write_packet(subscribe.into()).await;
    let packet = control2.read_packet().await;
    assert_eq!(packet, Packet::Suback(suback));

    control2.write_packet(Packet::Disconnect).await;

    tx.send(()).unwrap();

    // !!! FIXME: what if the client blocking the IO and the keep-alive will not work
    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    // reconnect
    let (conn2, mut control2) = MockConn::new_with_global(444, global);
    let _task2 = control2.start(conn2);

    let connack = Connack::new(true, Accepted);
    control2.write_packet(connect.into()).await;
    let packet = control2.read_packet().await;
    assert_eq!(packet, Packet::Connack(connack));
    assert!(control2.try_read_packet_is_empty());
}

#[tokio::test]
async fn test_pending_qos1() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        let connect = Connect::new(Arc::new("client identifier 1".to_owned()), 10);
        let connack = Connack::new(false, Accepted);
        control1.write_packet(connect.into()).await;
        let packet = control1.read_packet().await;
        assert_eq!(packet, Packet::Connack(connack));

        rx.await.unwrap();
        for pub_pid in 0..4u16 {
            sleep(Duration::from_millis(100)).await;
            let pub_pid = Pid::try_from(pub_pid + 1).unwrap();
            let publish = Publish::new(
                QosPid::Level1(pub_pid),
                TopicName::try_from("xyz/1".to_owned()).unwrap(),
                Bytes::from(vec![3, 5, 55]),
            );
            control1.write_packet(publish.into()).await;
            let packet = control1.read_packet().await;
            assert_eq!(packet, Packet::Puback(pub_pid));
        }
    });

    // client 2: subscriber
    let mut connect = Connect::new(Arc::new("client identifier 2".to_owned()), 10);
    connect.clean_session = false;
    let connack = Connack::new(false, Accepted);
    control2.write_packet(connect.clone().into()).await;
    let packet = control2.read_packet().await;
    assert_eq!(packet, Packet::Connack(connack));

    // subscribe to "xyz/1"
    let sub_pid = Pid::try_from(2).unwrap();
    let subscribe = Subscribe::new(
        sub_pid,
        vec![(
            TopicFilter::try_from("xyz/1".to_owned()).unwrap(),
            QoS::Level1,
        )],
    );
    let suback = Suback::new(sub_pid, vec![SubscribeReturnCode::MaxLevel1]);
    control2.write_packet(subscribe.into()).await;
    let packet = control2.read_packet().await;
    assert_eq!(packet, Packet::Suback(suback));

    control2.write_packet(Packet::Disconnect).await;

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    // reconnect
    let (conn2, mut control2) = MockConn::new_with_global(444, global);
    let _task2 = control2.start(conn2);

    let connack = Connack::new(true, Accepted);
    control2.write_packet(connect.into()).await;
    let packet = control2.read_packet().await;
    assert_eq!(packet, Packet::Connack(connack));

    for pub_pid in 0..4u16 {
        let pub_pid = Pid::try_from(pub_pid + 1).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pid),
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let expected_packet = Packet::Publish(publish);
        let packet = control2.read_packet().await;
        assert_eq!(packet, expected_packet);
    }
    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());
}

#[tokio::test]
async fn test_pending_max_inflight_qos1() {
    let mut config = Config::new_allow_anonymous();
    config.max_inflight_client = 8;
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
        let connect = Connect::new(Arc::new("client identifier 1".to_owned()), 10);
        let connack = Connack::new(false, Accepted);
        control1.write_packet(connect.into()).await;
        let packet = control1.read_packet().await;
        let expected_packet = Packet::Connack(connack);
        assert_eq!(packet, expected_packet);

        rx.await.unwrap();
        for pub_pid in 0..14u16 {
            let pub_pid = Pid::try_from(pub_pid + 1).unwrap();
            sleep(Duration::from_millis(100)).await;
            let publish = Publish::new(
                QosPid::Level1(pub_pid),
                TopicName::try_from("xyz/1".to_owned()).unwrap(),
                Bytes::from(vec![3, 5, 55]),
            );
            control1.write_packet(publish.into()).await;
            let expected_packet = Packet::Puback(pub_pid);
            let packet = control1.read_packet().await;
            assert_eq!(packet, expected_packet);
        }
    });

    // client 2: subscriber
    let mut connect = Connect::new(Arc::new("client identifier 2".to_owned()), 10);
    connect.clean_session = false;
    let connack = Connack::new(false, Accepted);
    control2.write_packet(connect.clone().into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = Packet::Connack(connack);
    assert_eq!(packet, expected_packet);

    // subscribe to "xyz/1"
    let sub_pid = Pid::try_from(2).unwrap();
    let subscribe = Subscribe::new(
        sub_pid,
        vec![(
            TopicFilter::try_from("xyz/1".to_owned()).unwrap(),
            QoS::Level1,
        )],
    );
    let suback = Suback::new(sub_pid, vec![SubscribeReturnCode::MaxLevel1]);
    control2.write_packet(subscribe.into()).await;
    let packet = control2.read_packet().await;
    let expected_packet = Packet::Suback(suback);
    assert_eq!(packet, expected_packet);

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    for pub_pid in 0..8u16 {
        let pub_pid = Pid::try_from(pub_pid + 1).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pid),
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let expected_packet = Packet::Publish(publish);
        let packet = control2.read_packet().await;
        assert_eq!(packet, expected_packet);
    }

    // Reach max inflight, we can not receive more publish packet
    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());

    for pub_pid in 0..8u16 {
        let pub_pid = Pid::try_from(pub_pid + 1).unwrap();
        control2.write_packet(Packet::Puback(pub_pid)).await;
    }
    for pub_pid in 8..14u16 {
        let pub_pid = Pid::try_from(pub_pid + 1).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pid),
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        let expected_packet = Packet::Publish(publish);
        let packet = control2.read_packet().await;
        assert_eq!(packet, expected_packet);
    }

    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());
}

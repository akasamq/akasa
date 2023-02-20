use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::sync::mpsc;
use tokio::time::sleep;
use ConnectReasonCode::*;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::assert_connack;

#[tokio::test]
async fn test_publish_qos0() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    // publisher
    let (conn0, mut control0) = MockConn::new_with_global(100, Arc::clone(&global));
    let _task0 = control0.start(conn0);

    // subscriber
    let (conn1, control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);
    let (conn2, control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);
    let (conn3, control3) = MockConn::new_with_global(333, Arc::clone(&global));
    let _task3 = control3.start(conn3);
    let (conn4, control4) = MockConn::new_with_global(444, Arc::clone(&global));
    let _task4 = control4.start(conn4);

    // Publisher connect
    let connect = Connect::new(Arc::new("publisher".to_owned()), 10);
    let connack = Connack::new(false, Success);
    control0.write_packet_v5(connect.into()).await;
    let packet = control0.read_packet_v5().await;
    assert_connack!(packet, connack);

    let (tx, mut rx) = mpsc::channel(4);
    let mut tasks = Vec::new();
    for (topic, mut control) in [
        ("xyz/0", control1),
        ("xyz/+", control2),
        ("#", control3),
        // will not match
        ("xxx/bbb", control4),
    ] {
        let tx = tx.clone();
        let task = tokio::spawn(async move {
            let connect = Connect::new(Arc::new(format!("subscriber: {}", topic)), 10);
            let connack = Connack::new(false, Success);
            control.write_packet_v5(connect.into()).await;
            let packet = control.read_packet_v5().await;
            assert_connack!(packet, connack);

            // subscribe to "xyz/0"
            let sub_pk_id = Pid::try_from(2).unwrap();
            let subscribe = Subscribe::new(
                sub_pk_id,
                vec![(
                    TopicFilter::try_from(topic.to_owned()).unwrap(),
                    SubscriptionOptions::new(QoS::Level0),
                )],
            );
            let suback = Suback::new(sub_pk_id, vec![SubscribeReasonCode::GrantedQoS0]);
            control.write_packet_v5(subscribe.into()).await;
            let packet = control.read_packet_v5().await;
            let expected_packet = Packet::Suback(suback);
            assert_eq!(packet, expected_packet);

            // Subscribe is ready
            tx.send(()).await.unwrap();

            sleep(Duration::from_millis(100)).await;

            if topic != "xxx/bbb" {
                for last_byte in 0..14u8 {
                    let publish = Publish::new(
                        QosPid::Level0,
                        TopicName::try_from("xyz/0".to_owned()).unwrap(),
                        Bytes::from(vec![3, 5, 55, last_byte]),
                    );
                    let packet = control.read_packet_v5().await;
                    let expected_packet = Packet::Publish(publish);
                    assert_eq!(packet, expected_packet);
                }
            }
            sleep(Duration::from_millis(100)).await;
            assert!(control.try_read_packet_is_empty());
        });
        tasks.push(task);
    }

    // Wait 4 subscribers
    for _ in 0..4 {
        rx.recv().await.unwrap();
    }

    for last_byte in 0..14u8 {
        let publish = Publish::new(
            QosPid::Level0,
            TopicName::try_from("xyz/0".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55, last_byte]),
        );
        control0.write_packet_v5(publish.into()).await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(control0.try_read_packet_is_empty());

    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_publish_qos1() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    // publisher
    let (conn0, mut control0) = MockConn::new_with_global(100, Arc::clone(&global));
    let _task0 = control0.start(conn0);

    // subscriber
    let (conn1, control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);
    let (conn2, control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);
    let (conn3, control3) = MockConn::new_with_global(333, Arc::clone(&global));
    let _task3 = control3.start(conn3);
    let (conn4, control4) = MockConn::new_with_global(444, Arc::clone(&global));
    let _task4 = control4.start(conn4);

    // Publisher connect
    let connect = Connect::new(Arc::new("publisher".to_owned()), 10);
    let connack = Connack::new(false, Success);
    control0.write_packet_v5(connect.into()).await;
    let packet = control0.read_packet_v5().await;
    assert_connack!(packet, connack);

    let (tx, mut rx) = mpsc::channel(4);
    let mut tasks = Vec::new();
    for (topic, mut control) in [
        ("xyz/1", control1),
        ("xyz/+", control2),
        ("#", control3),
        // will not match
        ("xxx/bbb", control4),
    ] {
        let tx = tx.clone();
        let task = tokio::spawn(async move {
            let connect = Connect::new(Arc::new(format!("subscriber: {}", topic)), 10);
            let connack = Connack::new(false, Success);
            control.write_packet_v5(connect.into()).await;
            let packet = control.read_packet_v5().await;
            assert_connack!(packet, connack);

            // subscribe to "xyz/1"
            let sub_pk_id = Pid::try_from(2).unwrap();
            let subscribe = Subscribe::new(
                sub_pk_id,
                vec![(
                    TopicFilter::try_from(topic.to_owned()).unwrap(),
                    SubscriptionOptions::new(QoS::Level1),
                )],
            );
            let suback = Suback::new(sub_pk_id, vec![SubscribeReasonCode::GrantedQoS1]);
            control.write_packet_v5(subscribe.into()).await;
            let packet = control.read_packet_v5().await;
            let expected_packet = Packet::Suback(suback);
            assert_eq!(packet, expected_packet);

            // Subscribe is ready
            tx.send(()).await.unwrap();

            sleep(Duration::from_millis(100)).await;

            if topic != "xxx/bbb" {
                for pub_pk_id in 0..14u16 {
                    let pub_pk_id = Pid::try_from(pub_pk_id + 1).unwrap();
                    let publish = Publish::new(
                        QosPid::Level1(pub_pk_id),
                        TopicName::try_from("xyz/1".to_owned()).unwrap(),
                        Bytes::from(vec![3, 5, 55, pub_pk_id.value() as u8]),
                    );
                    let packet = control.read_packet_v5().await;
                    let expected_packet = Packet::Publish(publish);
                    assert_eq!(packet, expected_packet);

                    control
                        .write_packet_v5(Puback::new_success(pub_pk_id).into())
                        .await;
                }
            }
            sleep(Duration::from_millis(100)).await;
            assert!(control.try_read_packet_is_empty());
        });
        tasks.push(task);
    }

    // Wait 4 subscribers
    for _ in 0..4 {
        rx.recv().await.unwrap();
    }

    for pub_pk_id in 0..14u16 {
        let pub_pk_id = Pid::try_from(pub_pk_id + 1).unwrap();
        let publish = Publish::new(
            QosPid::Level1(pub_pk_id),
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55, pub_pk_id.value() as u8]),
        );
        control0.write_packet_v5(publish.into()).await;

        let packet = control0.read_packet_v5().await;
        let expected_packet = Puback::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);
    }

    sleep(Duration::from_millis(20)).await;
    assert!(control0.try_read_packet_is_empty());

    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_publish_qos2() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    // publisher
    let (conn0, mut control0) = MockConn::new_with_global(100, Arc::clone(&global));
    let _task0 = control0.start(conn0);

    // subscriber
    let (conn1, control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let _task1 = control1.start(conn1);
    let (conn2, control2) = MockConn::new_with_global(222, Arc::clone(&global));
    let _task2 = control2.start(conn2);
    let (conn3, control3) = MockConn::new_with_global(333, Arc::clone(&global));
    let _task3 = control3.start(conn3);
    let (conn4, control4) = MockConn::new_with_global(444, Arc::clone(&global));
    let _task4 = control4.start(conn4);

    // Publisher connect
    let connect = Connect::new(Arc::new("publisher".to_owned()), 10);
    let connack = Connack::new(false, Success);
    control0.write_packet_v5(connect.into()).await;
    let packet = control0.read_packet_v5().await;
    assert_connack!(packet, connack);

    let (tx, mut rx) = mpsc::channel(4);
    let mut tasks = Vec::new();
    for (topic, mut control) in [
        ("xyz/2", control1),
        ("xyz/+", control2),
        ("#", control3),
        // will not match
        ("xxx/bbb", control4),
    ] {
        let tx = tx.clone();
        let task = tokio::spawn(async move {
            let connect = Connect::new(Arc::new(format!("subscriber: {}", topic)), 10);
            let connack = Connack::new(false, Success);
            control.write_packet_v5(connect.into()).await;
            let packet = control.read_packet_v5().await;
            assert_connack!(packet, connack);

            // subscribe to "xyz/2"
            let sub_pk_id = Pid::try_from(2).unwrap();
            let subscribe = Subscribe::new(
                sub_pk_id,
                vec![(
                    TopicFilter::try_from(topic.to_owned()).unwrap(),
                    SubscriptionOptions::new(QoS::Level2),
                )],
            );
            let suback = Suback::new(sub_pk_id, vec![SubscribeReasonCode::GrantedQoS2]);
            control.write_packet_v5(subscribe.into()).await;
            let packet = control.read_packet_v5().await;
            let expected_packet = Packet::Suback(suback);
            assert_eq!(packet, expected_packet);

            // Subscribe is ready
            tx.send(()).await.unwrap();

            sleep(Duration::from_millis(100)).await;

            if topic != "xxx/bbb" {
                let mut pub_pk_id = Pid::default();
                let mut rel_pk_id = Pid::default();
                while pub_pk_id.value() < 15 || rel_pk_id.value() < 15 {
                    let packet = control.read_packet_v5().await;
                    match packet {
                        Packet::Publish(publish) => {
                            let expected = Publish::new(
                                QosPid::Level2(pub_pk_id),
                                TopicName::try_from("xyz/2".to_owned()).unwrap(),
                                Bytes::from(vec![3, 5, 55, pub_pk_id.value() as u8]),
                            );
                            assert_eq!(publish, expected);
                            control
                                .write_packet_v5(Pubrec::new_success(pub_pk_id).into())
                                .await;

                            pub_pk_id += 1;
                        }
                        Packet::Pubrel(Pubrel { pid, .. }) => {
                            assert_eq!(pid, rel_pk_id);
                            control
                                .write_packet_v5(Pubcomp::new_success(rel_pk_id).into())
                                .await;

                            rel_pk_id += 1;
                        }
                        pkt => panic!("invalid packet from server: {:?}", pkt),
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
            assert!(control.try_read_packet_is_empty());
        });
        tasks.push(task);
    }

    // Wait 4 subscribers
    for _ in 0..4 {
        rx.recv().await.unwrap();
    }

    for pub_pk_id in 0..14u16 {
        let pub_pk_id = Pid::try_from(pub_pk_id + 1).unwrap();
        let publish = Publish::new(
            QosPid::Level2(pub_pk_id),
            TopicName::try_from("xyz/2".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55, pub_pk_id.value() as u8]),
        );
        control0.write_packet_v5(publish.into()).await;

        let packet = control0.read_packet_v5().await;
        let expected_packet = Pubrec::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);
    }

    sleep(Duration::from_millis(20)).await;
    assert!(control0.try_read_packet_is_empty());

    for task in tasks {
        assert!(task.await.is_ok());
    }
}

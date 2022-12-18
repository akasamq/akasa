use std::sync::Arc;
use std::time::Duration;

use mqtt::{
    control::variable_header::ConnectReturnCode, packet::publish::QoSWithPacketIdentifier,
    packet::suback::SubscribeReturnCode, packet::*, qos::QualityOfService, TopicFilter, TopicName,
};
use tokio::sync::mpsc;
use tokio::time::sleep;
use ConnectReturnCode::*;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

#[tokio::test]
async fn test_publish_qos0() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
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
    let connect = ConnectPacket::new("publisher");
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control0.write_packet(connect.into()).await;
    let packet = control0.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

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
            let connect = ConnectPacket::new(format!("subscriber: {}", topic));
            let connack = ConnackPacket::new(false, ConnectionAccepted);
            control.write_packet(connect.into()).await;
            let packet = control.read_packet().await;
            let expected_packet = VariablePacket::ConnackPacket(connack);
            assert_eq!(packet, expected_packet);

            // subscribe to "xyz/0"
            let sub_pk_id = 2;
            let subscribe = SubscribePacket::new(
                sub_pk_id,
                vec![(TopicFilter::new(topic).unwrap(), QualityOfService::Level0)],
            );
            let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel0]);
            control.write_packet(subscribe.into()).await;
            let packet = control.read_packet().await;
            let expected_packet = VariablePacket::SubackPacket(suback);
            assert_eq!(packet, expected_packet);

            // Subscribe is ready
            tx.send(()).await.unwrap();

            sleep(Duration::from_millis(100)).await;

            if topic != "xxx/bbb" {
                for last_byte in 0..14u8 {
                    let publish = PublishPacket::new(
                        TopicName::new("xyz/0").unwrap(),
                        QoSWithPacketIdentifier::Level0,
                        vec![3, 5, 55, last_byte],
                    );
                    let packet = control.read_packet().await;
                    let expected_packet = VariablePacket::PublishPacket(publish);
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
        let publish = PublishPacket::new(
            TopicName::new("xyz/0").unwrap(),
            QoSWithPacketIdentifier::Level0,
            vec![3, 5, 55, last_byte],
        );
        control0.write_packet(publish.into()).await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(control0.try_read_packet_is_empty());

    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_publish_qos1() {
    env_logger::init();
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
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
    let connect = ConnectPacket::new("publisher");
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control0.write_packet(connect.into()).await;
    let packet = control0.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

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
            let connect = ConnectPacket::new(format!("subscriber: {}", topic));
            let connack = ConnackPacket::new(false, ConnectionAccepted);
            control.write_packet(connect.into()).await;
            let packet = control.read_packet().await;
            let expected_packet = VariablePacket::ConnackPacket(connack);
            assert_eq!(packet, expected_packet);

            // subscribe to "xyz/1"
            let sub_pk_id = 2;
            let subscribe = SubscribePacket::new(
                sub_pk_id,
                vec![(TopicFilter::new(topic).unwrap(), QualityOfService::Level1)],
            );
            let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel1]);
            control.write_packet(subscribe.into()).await;
            let packet = control.read_packet().await;
            let expected_packet = VariablePacket::SubackPacket(suback);
            assert_eq!(packet, expected_packet);

            // Subscribe is ready
            tx.send(()).await.unwrap();

            sleep(Duration::from_millis(100)).await;

            if topic != "xxx/bbb" {
                for pub_pk_id in 0..14u16 {
                    let publish = PublishPacket::new(
                        TopicName::new("xyz/1").unwrap(),
                        QoSWithPacketIdentifier::Level1(pub_pk_id),
                        vec![3, 5, 55, pub_pk_id as u8],
                    );
                    let packet = control.read_packet().await;
                    let expected_packet = VariablePacket::PublishPacket(publish);
                    assert_eq!(packet, expected_packet);

                    let puback = PubackPacket::new(pub_pk_id);
                    control.write_packet(puback.into()).await;
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
        let publish = PublishPacket::new(
            TopicName::new("xyz/1").unwrap(),
            QoSWithPacketIdentifier::Level1(pub_pk_id),
            vec![3, 5, 55, pub_pk_id as u8],
        );
        control0.write_packet(publish.into()).await;

        let puback = PubackPacket::new(pub_pk_id);
        let packet = control0.read_packet().await;
        let expected_packet = VariablePacket::PubackPacket(puback);
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
    env_logger::init();
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::default(),
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
    let connect = ConnectPacket::new("publisher");
    let connack = ConnackPacket::new(false, ConnectionAccepted);
    control0.write_packet(connect.into()).await;
    let packet = control0.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);

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
            let connect = ConnectPacket::new(format!("subscriber: {}", topic));
            let connack = ConnackPacket::new(false, ConnectionAccepted);
            control.write_packet(connect.into()).await;
            let packet = control.read_packet().await;
            let expected_packet = VariablePacket::ConnackPacket(connack);
            assert_eq!(packet, expected_packet);

            // subscribe to "xyz/2"
            let sub_pk_id = 2;
            let subscribe = SubscribePacket::new(
                sub_pk_id,
                vec![(TopicFilter::new(topic).unwrap(), QualityOfService::Level2)],
            );
            let suback = SubackPacket::new(sub_pk_id, vec![SubscribeReturnCode::MaximumQoSLevel2]);
            control.write_packet(subscribe.into()).await;
            let packet = control.read_packet().await;
            let expected_packet = VariablePacket::SubackPacket(suback);
            assert_eq!(packet, expected_packet);

            // Subscribe is ready
            tx.send(()).await.unwrap();

            sleep(Duration::from_millis(100)).await;

            if topic != "xxx/bbb" {
                let mut pub_pk_id = 0u16;
                let mut rel_pk_id = 0u16;
                while pub_pk_id < 14 || rel_pk_id < 14 {
                    let packet = control.read_packet().await;
                    match packet {
                        VariablePacket::PublishPacket(publish) => {
                            let expected = PublishPacket::new(
                                TopicName::new("xyz/2").unwrap(),
                                QoSWithPacketIdentifier::Level2(pub_pk_id),
                                vec![3, 5, 55, pub_pk_id as u8],
                            );
                            assert_eq!(publish, expected);
                            let pubrec = PubrecPacket::new(pub_pk_id);
                            control.write_packet(pubrec.into()).await;

                            pub_pk_id += 1;
                        }
                        VariablePacket::PubrelPacket(pubrel) => {
                            let expected = PubrelPacket::new(rel_pk_id);
                            assert_eq!(pubrel, expected);
                            let pubcomp = PubcompPacket::new(rel_pk_id);
                            control.write_packet(pubcomp.into()).await;

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
        let publish = PublishPacket::new(
            TopicName::new("xyz/2").unwrap(),
            QoSWithPacketIdentifier::Level2(pub_pk_id),
            vec![3, 5, 55, pub_pk_id as u8],
        );
        control0.write_packet(publish.into()).await;

        let pubrec = PubrecPacket::new(pub_pk_id);
        let packet = control0.read_packet().await;
        let expected_packet = VariablePacket::PubrecPacket(pubrec);
        assert_eq!(packet, expected_packet);
    }

    sleep(Duration::from_millis(20)).await;
    assert!(control0.try_read_packet_is_empty());

    for task in tasks {
        assert!(task.await.is_ok());
    }
}

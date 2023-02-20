use std::io;
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
async fn test_will_publish() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, global);
    let task2 = control2.start(conn2);

    let connack = Connack::new(false, Accepted);
    // client 1: subscribe to "topic/1"
    {
        let connect1 = Connect::new(Arc::new("client identifier 1".to_owned()), 10);
        control1.write_packet_v3(connect1.into()).await;
        let packet = control1.read_packet_v3().await;
        let expected_packet = Packet::Connack(connack.clone());
        assert_eq!(packet, expected_packet);

        let subscribe = Subscribe::new(
            Pid::try_from(11).unwrap(),
            vec![(
                TopicFilter::try_from("topic/1".to_owned()).unwrap(),
                QoS::Level1,
            )],
        );
        let suback = Suback::new(
            Pid::try_from(11).unwrap(),
            vec![SubscribeReturnCode::MaxLevel1],
        );
        control1.write_packet_v3(subscribe.into()).await;
        let packet = control1.read_packet_v3().await;
        let expected_packet = Packet::Suback(suback);
        assert_eq!(packet, expected_packet);

        assert!(control1.try_read_packet_is_empty());
    }
    // client 2: connect with will topic "topic/1" and unexpected disconnect
    {
        // connect accepted: with will
        let mut connect2 = Connect::new(Arc::new("client identifier 2".to_owned()), 10);
        connect2.last_will = Some(LastWill {
            qos: QoS::Level1,
            retain: false,
            topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
            message: Bytes::from(vec![1, 2, 3, 4]),
        });
        control2.write_packet_v3(connect2.into()).await;
        let packet = control2.read_packet_v3().await;
        let expected_packet = Packet::Connack(connack.clone());
        assert_eq!(packet, expected_packet);

        control2.write_data(b"".to_vec()).await;
        sleep(Duration::from_millis(10)).await;
        assert!(task2.is_finished());
        assert_eq!(
            task2.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    let publish = Publish::new(
        QosPid::Level1(Pid::default()),
        TopicName::try_from("topic/1".to_owned()).unwrap(),
        Bytes::from(vec![1, 2, 3, 4]),
    );
    let packet = control1.read_packet_v3().await;
    let expected_packet = Packet::Publish(publish);
    assert_eq!(packet, expected_packet);
    assert!(!task1.is_finished());
}

#[tokio::test]
async fn test_will_disconnect_not_publish() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (conn1, mut control1) = MockConn::new_with_global(111, Arc::clone(&global));
    let task1 = control1.start(conn1);

    let (conn2, mut control2) = MockConn::new_with_global(222, global);
    let task2 = control2.start(conn2);

    let connack = Connack::new(false, Accepted);
    // client 1: subscribe to "topic/1"
    {
        let connect1 = Connect::new(Arc::new("client identifier 1".to_owned()), 10);
        control1.write_packet_v3(connect1.into()).await;
        let packet = control1.read_packet_v3().await;
        let expected_packet = Packet::Connack(connack.clone());
        assert_eq!(packet, expected_packet);

        let subscribe = Subscribe::new(
            Pid::try_from(11).unwrap(),
            vec![(
                TopicFilter::try_from("topic/1".to_owned()).unwrap(),
                QoS::Level1,
            )],
        );
        let suback = Suback::new(
            Pid::try_from(11).unwrap(),
            vec![SubscribeReturnCode::MaxLevel1],
        );
        control1.write_packet_v3(subscribe.into()).await;
        let packet = control1.read_packet_v3().await;
        let expected_packet = Packet::Suback(suback);
        assert_eq!(packet, expected_packet);

        assert!(control1.try_read_packet_is_empty());
    }
    // client 2: connect with will topic "topic/1" and normal disconnect
    {
        // connect accepted: with will
        let mut connect2 = Connect::new(Arc::new("client identifier 2".to_owned()), 10);
        connect2.last_will = Some(LastWill {
            qos: QoS::Level1,
            retain: false,
            topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
            message: Bytes::from(vec![1, 2, 3, 4]),
        });
        control2.write_packet_v3(connect2.into()).await;
        let packet = control2.read_packet_v3().await;
        let expected_packet = Packet::Connack(connack.clone());
        assert_eq!(packet, expected_packet);

        control2.write_packet_v3(Packet::Disconnect).await;
        sleep(Duration::from_millis(10)).await;
        assert!(task2.is_finished());
        assert!(task2.await.unwrap().is_ok())
    }

    sleep(Duration::from_millis(10)).await;
    assert!(control1.try_read_packet_is_empty());
    assert!(!task1.is_finished());
}

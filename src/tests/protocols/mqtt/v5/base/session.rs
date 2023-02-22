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

use super::super::ClientV5;
use super::assert_connack;

async fn test_clean_start(clean_start: bool, reconnect_clean_start: bool) {
    let (task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());

    let client_id = "client id";
    client.connect(client_id, clean_start, false).await;
    // for increase the server_packet_id field
    {
        let sub_topics = vec![("abc/1", SubscriptionOptions::new(QoS::Level1))];
        client.subscribe(11, sub_topics).await;
        client
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client
            .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client.send_puback(1).await;
    }

    client.disconnect_normal().await;
    sleep(Duration::from_millis(10)).await;
    assert!(task.is_finished());

    let (_task, mut client) = MockConn::start_with_global(4444, client.global);
    let session_present = !(clean_start || reconnect_clean_start);
    client
        .connect(client_id, reconnect_clean_start, session_present)
        .await;
    {
        if !session_present {
            let sub_topics = vec![("abc/1", SubscriptionOptions::new(QoS::Level1))];
            client.subscribe(11, sub_topics).await;
        }

        client
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        let received_pk_id = if session_present { 2 } else { 1 };
        client
            .recv_publish(QoS::Level1, received_pk_id, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client.send_puback(received_pk_id).await;
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

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
        connect.clean_start = false;
        connect.properties = ConnectProperties {
            session_expiry_interval: Some(60),
            ..Default::default()
        };
        let connack = Connack::new(false, Success);
        client.write_packet_v5(connect.into()).await;
        let packet = client.read_packet_v5().await;
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
        client.write_packet_v5(subscribe.into()).await;
        let packet = client.read_packet_v5().await;
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
        client.write_packet_v5(publish.into()).await;

        let packet = client.read_packet_v5().await;
        let expected_packet = Puback::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);

        let packet = client.read_packet_v5().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        client
            .write_packet_v5(Puback::new_success(Pid::default()).into())
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());

    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));
    // client second connection
    {
        let mut connect = Connect::new(Arc::clone(&client_identifier), 10);
        connect.clean_start = false;
        connect.properties = ConnectProperties {
            session_expiry_interval: Some(60),
            ..Default::default()
        };
        let connack = Connack::new(true, Success);
        client2.write_packet_v5(connect.into()).await;
        let packet = client2.read_packet_v5().await;
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
        client2.write_packet_v5(publish.into()).await;

        let packet = client2.read_packet_v5().await;
        let expected_packet = Puback::new_success(pub_pk_id).into();
        assert_eq!(packet, expected_packet);

        let packet = client2.read_packet_v5().await;
        let expected_packet = Packet::Publish(received_publish);
        assert_eq!(packet, expected_packet);

        client2
            .write_packet_v5(Puback::new_success(Pid::try_from(2).unwrap()).into())
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
    assert!(!task2.is_finished());
}

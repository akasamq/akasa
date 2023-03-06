use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::sync::oneshot;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

async fn test_session_expired_with(first_clean_start: bool) {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let client_id = "client id";

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        let update_connect = |c: &mut Connect| {
            c.clean_start = first_clean_start;
            c.properties.session_expiry_interval = Some(2);
        };
        client.connect_with(client_id, update_connect, |_| ()).await;
        client
            .subscribe(11, vec![("abc/1", SubscriptionOptions::new(QoS::Level1))])
            .await;
        client
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client
            .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client.send_puback(1).await;
        drop(client);
    }
    sleep(Duration::from_millis(200)).await;
    assert!(task.is_finished());
    sleep(Duration::from_millis(2000)).await;

    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));
    client2.connect(client_id, false, false).await;
    sleep(Duration::from_millis(20)).await;
    assert!(!task2.is_finished());
}

#[tokio::test]
async fn test_session_expired_clean_start() {
    test_session_expired_with(true).await;
}

#[tokio::test]
async fn test_session_expired_not_clean_start() {
    test_session_expired_with(false).await;
}

#[tokio::test]
async fn test_session_not_expired() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let client_id = "client id";

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        let update_connect = |c: &mut Connect| {
            c.clean_start = false;
            c.properties.session_expiry_interval = Some(2);
        };
        client.connect_with(client_id, update_connect, |_| ()).await;
        client
            .subscribe(11, vec![("abc/1", SubscriptionOptions::new(QoS::Level1))])
            .await;
        client
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client
            .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client.send_puback(1).await;
        drop(client);
    }
    sleep(Duration::from_millis(200)).await;
    assert!(task.is_finished());
    sleep(Duration::from_millis(1000)).await;

    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));
    client2.connect(client_id, false, true).await;
    sleep(Duration::from_millis(20)).await;
    assert!(!task2.is_finished());
}

#[tokio::test]
async fn test_receive_max_client() {
    let mut config = Config::new_allow_anonymous();
    config.max_inflight_client = 8;
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        config.clone(),
    ));
    let (_task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        client1.connect("client id 1", true, false).await;
        rx.await.unwrap();
        for pub_pid in 1..17u16 {
            client1
                .publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
                .await;
        }
    });

    let receive_max = 4;
    // client 2: subscriber
    let update_connect = |c: &mut Connect| {
        c.clean_start = false;
        c.properties.session_expiry_interval = Some(2);
        c.properties.receive_max = Some(receive_max);
    };
    client2
        .connect_with("client id 2", update_connect, |_| ())
        .await;

    let sub_topics = vec![("xyz/1", SubscriptionOptions::new(QoS::Level1))];
    client2.subscribe(2, sub_topics).await;

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    for pub_pid in 1..(receive_max + 1) {
        client2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
            .await;
    }

    sleep(Duration::from_millis(50)).await;
    // Reach max inflight, we can not receive more publish packet
    assert!(client2.try_read_packet_is_empty());

    for pub_pid in 1..(receive_max + 1) {
        client2.send_puback(pub_pid).await;
    }
    for pub_pid in (receive_max + 1)..9 {
        client2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
            .await;
        // If connection not receive packets, server forbid send more packets
        client2.send_puback(pub_pid).await;
    }

    for pub_pid in 9..17u16 {
        client2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
            .await;
        client2.send_puback(pub_pid).await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(client2.try_read_packet_is_empty());
}

#[tokio::test]
async fn test_max_packet_size() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    // Without reason string
    {
        let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
        let update_connect = |c: &mut Connect| {
            c.clean_start = true;
            c.properties.session_expiry_interval = None;
            c.properties.max_packet_size = Some(6);
        };
        client
            .connect_with("client id", update_connect, |_| ())
            .await;
        let mut disconnect = Disconnect::new_normal();
        // to casue server send an error disconnect packet, due to limited max
        // packet size, server will ommit the error message in properties.
        disconnect.properties.session_expiry_interval = Some(10);
        client.write_packet(disconnect.into()).await;
        let err_pkt = client.read_packet().await;
        if let Packet::Disconnect(pkt) = err_pkt {
            assert_eq!(pkt.reason_code, DisconnectReasonCode::ProtocolError);
            assert!(pkt.properties.reason_string.is_none());
        } else {
            panic!("invalid packet: {err_pkt:?}");
        }
        sleep(Duration::from_millis(20)).await;
        assert!(task.is_finished());
    }

    // With reason string
    {
        let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
        let update_connect = |c: &mut Connect| {
            c.clean_start = true;
            c.properties.session_expiry_interval = None;
            c.properties.max_packet_size = Some(50);
        };
        client
            .connect_with("client id", update_connect, |_| ())
            .await;
        let mut disconnect = Disconnect::new_normal();
        // to casue server send an error disconnect packet, due to limited max
        // packet size, server will ommit the error message in properties.
        disconnect.properties.session_expiry_interval = Some(10);
        client.write_packet(disconnect.into()).await;
        let err_pkt = client.read_packet().await;
        if let Packet::Disconnect(pkt) = err_pkt {
            assert_eq!(pkt.reason_code, DisconnectReasonCode::ProtocolError);
            assert!(pkt.properties.reason_string.is_some());
        } else {
            panic!("invalid packet: {err_pkt:?}");
        }
        sleep(Duration::from_millis(20)).await;
        assert!(task.is_finished());
    }
}

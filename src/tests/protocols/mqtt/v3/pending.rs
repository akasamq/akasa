use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::*;
use tokio::sync::oneshot;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::ControlV3;

#[tokio::test]
async fn test_pending_qos0() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (_task1, mut control1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, mut control2) = MockConn::start_with_global(222, Arc::clone(&global));

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        control1.connect("client id 1", true, false).await;

        rx.await.unwrap();
        for _ in 0..4 {
            sleep(Duration::from_millis(100)).await;
            control1
                .send_publish(QoS::Level0, 0, "xyz/0", vec![3, 5, 55], |_| ())
                .await;
            assert!(control1.try_read_packet_is_empty());
        }
    });

    // client 2: subscriber
    control2.connect("client id 2", false, false).await;
    // subscribe to "xyz/0"
    control2.subscribe(2, vec![("xyz/0", QoS::Level0)]).await;
    control2.disconnect().await;

    tx.send(()).unwrap();

    // !!! FIXME: what if the client blocking the IO and the keep-alive will not work
    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    // reconnect
    let (_task2, mut control2) = MockConn::start_with_global(444, global);
    control2.connect("client id 2", false, true).await;
    assert!(control2.try_read_packet_is_empty());
}

#[tokio::test]
async fn test_pending_qos1() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (_task1, mut control1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, mut control2) = MockConn::start_with_global(222, Arc::clone(&global));

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        control1.connect("client id 1", true, false).await;
        rx.await.unwrap();
        for pub_pid in 1..5u16 {
            sleep(Duration::from_millis(100)).await;
            control1
                .publish(QoS::Level1, pub_pid, "xyz/1", vec![3, 5, 55], |_| ())
                .await;
        }
    });

    // client 2: subscriber
    control2.connect("client id 2", false, false).await;
    control2.subscribe(2, vec![("xyz/1", QoS::Level1)]).await;
    control2.disconnect().await;

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    // reconnect
    let (_task2, mut control2) = MockConn::start_with_global(444, global);

    control2.connect("client id 2", false, true).await;

    for pub_pid in 1..5u16 {
        control2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", vec![3, 5, 55], |_| ())
            .await;
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
    let (_task1, mut control1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, mut control2) = MockConn::start_with_global(222, Arc::clone(&global));

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        control1.connect("client id 1", true, false).await;
        rx.await.unwrap();
        for pub_pid in 1..15u16 {
            control1
                .publish(QoS::Level1, pub_pid, "xyz/1", vec![3, 5, 55], |_| ())
                .await;
        }
    });

    // client 2: subscriber
    control2.connect("client id 2", false, false).await;
    control2.subscribe(2, vec![("xyz/1", QoS::Level1)]).await;

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    for pub_pid in 1..9u16 {
        control2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", vec![3, 5, 55], |_| ())
            .await;
    }

    // Reach max inflight, we can not receive more publish packet
    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());

    for pub_pid in 1..9u16 {
        control2.send_puback(pub_pid).await;
    }
    for pub_pid in 9..15u16 {
        control2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", vec![3, 5, 55], |_| ())
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(control2.try_read_packet_is_empty());
}

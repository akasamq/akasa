use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::v5::*;
use mqtt_proto::*;
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

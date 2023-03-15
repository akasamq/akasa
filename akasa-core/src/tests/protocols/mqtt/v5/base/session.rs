use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

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
    let client_id = "client id";

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        client.connect(client_id, false, false).await;
        client
            .subscribe(11, vec![("abc/1", SubscriptionOptions::new(QoS::Level1))])
            .await;
        client
            .publish(QoS::Level1, 12, "abc/1", "first", |_| ())
            .await;
        client
            .recv_publish(QoS::Level1, 1, "abc/1", "first", |_| ())
            .await;
        client.send_puback(1).await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());

    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));
    // client second connection
    {
        client2.connect(client_id, false, true).await;
        client2
            .publish(QoS::Level1, 13, "abc/1", "second", |_| ())
            .await;
        client2
            .recv_publish(QoS::Level1, 2, "abc/1", "second", |_| ())
            .await;
        client2.send_puback(2).await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
    assert!(!task2.is_finished());
}

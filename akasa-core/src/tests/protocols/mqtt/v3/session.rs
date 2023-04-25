use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::ClientV3;

async fn test_clean_session(clean_session: bool, reconnect_clean_session: bool) {
    let (task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());

    let client_id = "client id";
    client.connect(client_id, clean_session, false).await;

    // for increase the server_packet_id field
    client.subscribe(11, vec![("abc/1", QoS::Level1)]).await;
    client
        .send_publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    client.recv_puback(12).await;
    client
        .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    client.disconnect().await;
    sleep(Duration::from_millis(10)).await;
    assert!(task.is_finished());

    let (_task, mut client) = MockConn::start_with_global(4444, client.global);
    let session_present = !(clean_session || reconnect_clean_session);
    client
        .connect(client_id, reconnect_clean_session, session_present)
        .await;
    if !session_present {
        client.subscribe(11, vec![("abc/1", QoS::Level1)]).await;
    }

    let received_pid = if session_present { 2 } else { 1 };
    client
        .send_publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    client.recv_puback(12).await;
    client
        .recv_publish(QoS::Level1, received_pid, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    client.send_puback(received_pid).await;
}

#[tokio::test]
async fn test_session_not_persist() {
    test_clean_session(true, false).await;
    test_clean_session(true, true).await;
}

#[tokio::test]
async fn test_session_persist() {
    test_clean_session(false, false).await;
}

#[tokio::test]
async fn test_session_take_over() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));
    let client_id = "client id";

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        client.connect(client_id, false, false).await;
        client.subscribe(11, vec![("abc/1", QoS::Level1)]).await;
        client
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client
            .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
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
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client2
            .recv_publish(QoS::Level1, 2, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client2.send_puback(2).await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
    assert!(!task2.is_finished());
}

use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::ControlV3;

async fn test_clean_session(clean_session: bool, reconnect_clean_session: bool) {
    let (task, mut control) = MockConn::start(3333, Config::new_allow_anonymous());

    let client_id = "client id";
    control.connect(client_id, clean_session, false).await;

    // for increase the server_packet_id field
    control.subscribe(11, vec![("abc/1", QoS::Level1)]).await;
    control
        .send_publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    control.recv_puback(12).await;
    control
        .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    control.disconnect().await;
    sleep(Duration::from_millis(10)).await;
    assert!(task.is_finished());

    let (_task, mut control) = MockConn::start_with_global(4444, control.global);
    let session_present = !(clean_session || reconnect_clean_session);
    control
        .connect(client_id, reconnect_clean_session, session_present)
        .await;
    if !session_present {
        control.subscribe(11, vec![("abc/1", QoS::Level1)]).await;
    }

    let received_pid = if session_present { 2 } else { 1 };
    control
        .send_publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    control.recv_puback(12).await;
    control
        .recv_publish(QoS::Level1, received_pid, "abc/1", vec![3, 5, 55], |_| ())
        .await;
    control.send_puback(received_pid).await;
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
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let client_id = "client id";

    let (task, mut control) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        control.connect(client_id, false, false).await;
        control.subscribe(11, vec![("abc/1", QoS::Level1)]).await;
        control
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        control
            .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        control.send_puback(1).await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());

    let (task2, mut control2) = MockConn::start_with_global(222, Arc::clone(&global));
    // client second connection
    {
        control2.connect(client_id, false, true).await;
        control2
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        control2
            .recv_publish(QoS::Level1, 2, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        control2.send_puback(2).await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
    assert!(!task2.is_finished());
}

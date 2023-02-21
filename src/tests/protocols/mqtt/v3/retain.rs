use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::ControlV3;

#[tokio::test]
async fn test_retain_simple() {
    let (task, mut control) = MockConn::start(3333, Config::new_allow_anonymous());

    control.connect("client id", true, false).await;
    control
        .publish(QoS::Level1, 22, "xyz/1", vec![3, 5, 55], |p| {
            p.retain = true
        })
        .await;
    control
        .send_subscribe(23, vec![("abc/0", QoS::Level0), ("xyz/1", QoS::Level1)])
        .await;
    control
        .recv_publish(QoS::Level1, 1, "xyz/1", vec![3, 5, 55], |p| p.retain = true)
        .await;
    control
        .recv_suback(23, vec![QoS::Level0.into(), QoS::Level1.into()])
        .await;
    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_retain_different_clients() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (task1, mut control1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (task2, mut control2) = MockConn::start_with_global(222, global);

    // client 1: publish retain message
    {
        control1.connect("client id 1", true, false).await;
        control1
            .publish(QoS::Level1, 11, "xyz/1", vec![3, 5, 55], |p| {
                p.retain = true
            })
            .await;
    }

    // client 2: subscribe and received a retain message
    {
        control2.connect("client id 2", true, false).await;
        // subscribe multiple times
        for (sub_pid, pub_pid) in [(22, 1), (23, 2)] {
            control2
                .send_subscribe(
                    sub_pid,
                    vec![("abc/0", QoS::Level0), ("xyz/1", QoS::Level1)],
                )
                .await;
            control2
                .recv_publish(QoS::Level1, pub_pid, "xyz/1", vec![3, 5, 55], |p| {
                    p.retain = true
                })
                .await;
            control2
                .recv_suback(sub_pid, vec![QoS::Level0.into(), QoS::Level1.into()])
                .await;
        }
    }

    sleep(Duration::from_millis(10)).await;
    assert!(!task1.is_finished());
    assert!(!task2.is_finished());
}

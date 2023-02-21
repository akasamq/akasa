use std::time::Duration;

use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::tests::utils::MockConn;

use super::ControlV3;

#[tokio::test]
async fn test_sub_unsub_simple() {
    let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = control.start(conn);

    let sub_topics = vec![
        ("abc/0", QoS::Level0),
        ("xyz/1", QoS::Level1),
        ("ijk/2", QoS::Level2),
    ];
    control.connect("id", true, false).await;
    control.subscribe(23, sub_topics).await;
    control.unsubscribe(24, vec!["abc/0", "xxx/+"]).await;

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscribe_reject_empty_topics() {
    let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = control.start(conn);

    control.connect("client id", true, false).await;
    control.send_subscribe::<String>(23, vec![]).await;

    sleep(Duration::from_millis(10)).await;
    assert!(control.try_read_packet().is_err());
    assert!(task.is_finished());
}

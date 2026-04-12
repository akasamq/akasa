use std::time::Duration;

use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::tests::utils::MockConn;

use super::ClientV3;

#[tokio::test]
async fn test_sub_unsub_simple() {
    let (conn, mut client) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = client.start(conn);

    let sub_topics = vec![
        ("abc/0", QoS::Level0),
        ("xyz/1", QoS::Level1),
        ("ijk/2", QoS::Level2),
    ];
    client.connect("id", true, false).await;
    client.subscribe(23, sub_topics).await;
    client.unsubscribe(24, vec!["abc/0", "xxx/+"]).await;

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscribe_reject_empty_topics() {
    let (conn, mut client) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = client.start(conn);

    client.connect("client id", true, false).await;
    client.send_subscribe(23, vec![]).await;

    sleep(Duration::from_millis(10)).await;
    assert!(client.try_read_packet().is_err());
    assert!(task.is_finished());
}

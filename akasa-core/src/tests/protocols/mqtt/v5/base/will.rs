use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

#[tokio::test]
async fn test_will_publish() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));
    let (task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (task2, mut client2) = MockConn::start_with_global(222, global);

    // client 1: subscribe to "topic/1"
    {
        client1.connect("client id 1", true, false).await;
        client1
            .subscribe(11, vec![("topic/1", SubscriptionOptions::new(QoS::Level1))])
            .await;
        assert!(client1.try_read_packet_is_empty());
    }
    // client 2: connect with will topic "topic/1" and unexpected disconnect
    {
        // connect accepted: with will
        let update_connect = |c: &mut Connect| {
            c.last_will = Some(LastWill {
                qos: QoS::Level1,
                retain: false,
                topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
                payload: Bytes::from(vec![1, 2, 3, 4]),
                properties: Default::default(),
            });
        };
        client2
            .connect_with("client id 2", update_connect, |_| ())
            .await;
        client2.write_data(b"".to_vec()).await;
        sleep(Duration::from_millis(10)).await;
        assert!(task2.is_finished());
        assert_eq!(
            task2.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    client1
        .recv_publish(QoS::Level1, 1, "topic/1", vec![1, 2, 3, 4], |_| ())
        .await;
    assert!(!task1.is_finished());
}

#[tokio::test]
async fn test_will_disconnect_not_publish() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));
    let (task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (task2, mut client2) = MockConn::start_with_global(222, global);

    // client 1: subscribe to "topic/1"
    {
        client1.connect("client id 1", true, false).await;
        client1
            .subscribe(11, vec![("topic/1", SubscriptionOptions::new(QoS::Level1))])
            .await;
        assert!(client1.try_read_packet_is_empty());
    }
    // client 2: connect with will topic "topic/1" and normal disconnect
    {
        // connect accepted: with will
        let update_connect = |c: &mut Connect| {
            c.last_will = Some(LastWill {
                qos: QoS::Level1,
                retain: false,
                topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
                payload: Bytes::from(vec![1, 2, 3, 4]),
                properties: Default::default(),
            });
        };
        client2
            .connect_with("client id 2", update_connect, |_| ())
            .await;
        client2.disconnect_normal().await;
        sleep(Duration::from_millis(10)).await;
        assert!(task2.is_finished());
        assert!(task2.await.unwrap().is_ok())
    }

    sleep(Duration::from_millis(10)).await;
    assert!(client1.try_read_packet_is_empty());
    assert!(!task1.is_finished());
}

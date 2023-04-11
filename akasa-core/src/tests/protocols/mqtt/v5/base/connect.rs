use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::time::sleep;
use ConnectReasonCode::*;

use crate::config::Config;
use crate::state::{GlobalState, HashAlgorithm};
use crate::tests::utils::MockConn;

use super::super::ClientV5;

#[tokio::test]
async fn test_connect_malformed_packet() {
    let (task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());
    client.write_data(b"abcdefxyzxyz123123".to_vec()).await;

    sleep(Duration::from_millis(10)).await;
    assert!(client.try_read_packet().is_err());
    assert!(task.is_finished());
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn test_connect_invalid_first_packet() {
    // subscribe
    {
        let (conn, mut client) = MockConn::new(3333, Config::new_allow_anonymous());
        let task = client.start(conn);

        let sub_topics = vec![("abc/0", SubscriptionOptions::new(QoS::Level0))];
        client.send_subscribe(23, sub_topics).await;

        sleep(Duration::from_millis(10)).await;
        assert!(client.try_read_packet().is_err());
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
    // publish
    {
        let (conn, mut client) = MockConn::new(3333, Config::new_allow_anonymous());
        let task = client.start(conn);

        let mut publish = Publish::new(
            QosPid::Level0,
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        publish.retain = true;
        client.write_packet(publish.into()).await;

        sleep(Duration::from_millis(10)).await;
        assert!(client.try_read_packet().is_err());
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn test_connect_v500() {
    // connect accepted: identifier empty
    {
        let (_task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());
        client.connect("", true, false).await;
    }
}

#[tokio::test]
async fn test_connect_keepalive() {
    // connect accepted: zero keep_alive
    {
        let (_task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());
        client
            .connect_with("client id", |c| c.keep_alive = 0, |_| ())
            .await;
    }

    // connect accepted: non-zero keep_alive
    {
        let (_task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());
        client
            .connect_with("client id", |c| c.keep_alive = 22, |_| ())
            .await;
    }
}

#[tokio::test]
async fn test_connect_will() {
    // connect accepted: with will
    {
        let (_task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());
        let update_connect = |c: &mut Connect| {
            c.last_will = Some(LastWill {
                qos: QoS::Level1,
                retain: false,
                topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
                payload: Bytes::from(vec![1, 2, 3, 4]),
                properties: Default::default(),
            });
        };
        client.connect_with("id", update_connect, |_| ()).await;
    }

    // connect accepted: with invalid will topic (start with "$")
    {
        let (task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());
        client
            .send_connect("client id", |c: &mut Connect| {
                c.last_will = Some(LastWill {
                    qos: QoS::Level1,
                    retain: false,
                    topic_name: TopicName::try_from("$topic/1".to_owned()).unwrap(),
                    payload: Bytes::from(vec![1, 2, 3, 4]),
                    properties: Default::default(),
                })
            })
            .await;
        assert!(client.try_read_packet().is_err());

        sleep(Duration::from_millis(10)).await;
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn test_connect_auth() {
    // allow anonymous
    {
        let (_task, mut client) = MockConn::start(3333, Config::new_allow_anonymous());
        client.connect("client id", true, false).await;
    }
    // empty username/password
    {
        let mut global_state =
            GlobalState::new("127.0.0.1:1883".parse().unwrap(), Config::default());
        global_state.insert_password("user", "pass", HashAlgorithm::Sha256);
        let (_task, mut client) = MockConn::start_with_global(3333, Arc::new(global_state));
        client
            .connect_with(
                "client id",
                |_| (),
                |a| a.reason_code = BadUserNameOrPassword,
            )
            .await;
    }
    // wrong username/password
    {
        let mut global_state =
            GlobalState::new("127.0.0.1:1883".parse().unwrap(), Config::default());
        global_state.insert_password("user", "pass", HashAlgorithm::Sha256);
        let (_task, mut client) = MockConn::start_with_global(3333, Arc::new(global_state));
        client
            .connect_with(
                "client id",
                |c| {
                    c.username = Some(Arc::new("xxx".to_owned()));
                    c.password = Some(Bytes::from(b"yyy".to_vec()));
                },
                |a| a.reason_code = BadUserNameOrPassword,
            )
            .await;
    }
    // wrong password
    {
        let mut global_state =
            GlobalState::new("127.0.0.1:1883".parse().unwrap(), Config::default());
        global_state.insert_password("user", "pass", HashAlgorithm::Sha256);
        let (_task, mut client) = MockConn::start_with_global(3333, Arc::new(global_state));
        client
            .connect_with(
                "client",
                |c| {
                    c.username = Some(Arc::new("user".to_owned()));
                    c.password = Some(Bytes::from(b"yyy".to_vec()));
                },
                |a| a.reason_code = BadUserNameOrPassword,
            )
            .await;
    }
    // right username/password (hash-algorithm: sha256)
    {
        let mut global_state =
            GlobalState::new("127.0.0.1:1883".parse().unwrap(), Config::default());
        global_state.insert_password("user", "pass", HashAlgorithm::Sha256);
        let (_task, mut client) = MockConn::start_with_global(3333, Arc::new(global_state));
        client
            .connect_with(
                "client id",
                |c| {
                    c.username = Some(Arc::new("user".to_owned()));
                    c.password = Some(Bytes::from(b"pass".to_vec()));
                },
                |_| (),
            )
            .await;
    }

    // right username/password (hash-algorithm: sha256)
    {
        let mut global_state =
            GlobalState::new("127.0.0.1:1883".parse().unwrap(), Config::default());
        global_state.insert_password(
            "user",
            "pass",
            HashAlgorithm::Sha256Pkbdf2 {
                iterations: 10.try_into().unwrap(),
            },
        );
        let (_task, mut client) = MockConn::start_with_global(3333, Arc::new(global_state));
        client
            .connect_with(
                "client id",
                |c| {
                    c.username = Some(Arc::new("user".to_owned()));
                    c.password = Some(Bytes::from(b"pass".to_vec()));
                },
                |_| (),
            )
            .await;
    }
}

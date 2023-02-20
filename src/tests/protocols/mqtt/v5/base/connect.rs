use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::{task::JoinHandle, time::sleep};
use ConnectReasonCode::*;

use crate::config::{AuthType, Config};
use crate::tests::utils::MockConn;

use super::assert_connack;

async fn do_test(config: Config, connect: Connect, connack: Connack) -> JoinHandle<io::Result<()>> {
    let (conn, mut control) = MockConn::new(3333, config);
    let task = control.start(conn);

    let finished = connack.reason_code != Success;
    control.write_packet_v5(connect.into()).await;
    assert_connack!(control.read_packet_v5().await, connack);

    sleep(Duration::from_millis(10)).await;
    assert_eq!(task.is_finished(), finished);
    task
}

#[tokio::test]
async fn test_connect_malformed_packet() {
    let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
    let task = control.start(conn);
    control.write_data(b"abcdefxyzxyz123123".to_vec()).await;

    sleep(Duration::from_millis(10)).await;
    assert!(control.try_read_packet_v5().is_err());
    assert!(task.is_finished());
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn test_connect_invalid_first_packet() {
    // subscribe
    {
        let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
        let task = control.start(conn);

        let sub_pk_id = Pid::try_from(23).unwrap();
        let subscribe = Subscribe::new(
            sub_pk_id,
            vec![(
                TopicFilter::try_from("abc/0".to_owned()).unwrap(),
                SubscriptionOptions::new(QoS::Level0),
            )],
        );
        control.write_packet_v5(subscribe.into()).await;

        sleep(Duration::from_millis(10)).await;
        assert!(control.try_read_packet_v5().is_err());
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
    // publish
    {
        let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
        let task = control.start(conn);

        let mut publish = Publish::new(
            QosPid::Level0,
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        publish.retain = true;
        control.write_packet_v5(publish.into()).await;

        sleep(Duration::from_millis(10)).await;
        assert!(control.try_read_packet_v5().is_err());
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn test_connect_v500() {
    // connect accepted: identifier empty
    {
        let connect = Connect::new(Arc::new("".to_owned()), 30);
        let connack = Connack::new(false, Success);
        do_test(Config::new_allow_anonymous(), connect, connack).await;
    }
}

#[tokio::test]
async fn test_connect_keepalive() {
    // connect accepted: zero keep_alive
    {
        let connect = Connect::new(Arc::new("client identifier".to_owned()), 0);
        let connack = Connack::new(false, Success);
        do_test(Config::new_allow_anonymous(), connect, connack).await;
    }

    // connect accepted: non-zero keep_alive
    {
        let connect = Connect::new(Arc::new("client identifier".to_owned()), 22);
        let connack = Connack::new(false, Success);
        do_test(Config::new_allow_anonymous(), connect, connack).await;
    }
}

#[tokio::test]
async fn test_connect_will() {
    // connect accepted: with will
    {
        let connect = Connect {
            protocol: Protocol::V500,
            clean_start: true,
            keep_alive: 30,
            client_id: Arc::new("client identifier".to_owned()),
            last_will: Some(LastWill {
                qos: QoS::Level1,
                retain: false,
                topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
                payload: Bytes::from(vec![1, 2, 3, 4]),
                properties: Default::default(),
            }),
            username: None,
            password: None,
            properties: Default::default(),
        };
        let connack = Connack::new(false, Success);
        do_test(Config::new_allow_anonymous(), connect, connack).await;
    }

    // connect accepted: with invalid will topic (start with "$")
    {
        let (conn, mut control) = MockConn::new(3333, Config::new_allow_anonymous());
        let task = control.start(conn);
        let connect = Connect {
            protocol: Protocol::V500,
            clean_start: true,
            keep_alive: 30,
            client_id: Arc::new("client identifier".to_owned()),
            last_will: Some(LastWill {
                qos: QoS::Level1,
                retain: false,
                topic_name: TopicName::try_from("$topic/1".to_owned()).unwrap(),
                payload: Bytes::from(vec![1, 2, 3, 4]),
                properties: Default::default(),
            }),
            username: None,
            password: None,
            properties: Default::default(),
        };
        control.write_packet_v5(connect.into()).await;
        assert!(control.try_read_packet_v5().is_err());

        sleep(Duration::from_millis(10)).await;
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn test_connect_auth() {
    // allow anonymous
    {
        let mut config = Config::new_allow_anonymous();
        config.auth_types = Vec::new();
        let connect = Connect::new(Arc::new("client_anonymous".to_owned()), 10);
        let connack = Connack::new(false, Success);
        do_test(config, connect, connack).await;
    }
    // empty username/password
    {
        let mut config = Config::new_allow_anonymous();
        config.auth_types = vec![AuthType::UsernamePassword];
        config.users = [("user".to_owned(), "pass".to_owned())]
            .into_iter()
            .collect();
        let connect = Connect::new(Arc::new("client_anonymous".to_owned()), 10);
        let connack = Connack::new(false, BadUserNameOrPassword);
        do_test(config, connect, connack).await;
    }
    // wrong username/password
    {
        let mut config = Config::new_allow_anonymous();
        config.auth_types = vec![AuthType::UsernamePassword];
        config.users = [("user".to_owned(), "pass".to_owned())]
            .into_iter()
            .collect();
        let mut connect = Connect::new(Arc::new("client_anonymous".to_owned()), 10);
        connect.username = Some(Arc::new("xxx".to_owned()));
        connect.password = Some(Bytes::from(b"yyy".to_vec()));
        let connack = Connack::new(false, BadUserNameOrPassword);
        do_test(config, connect, connack).await;
    }
    // right username/password
    {
        let mut config = Config::new_allow_anonymous();
        config.auth_types = vec![AuthType::UsernamePassword];
        config.users = [("user".to_owned(), "pass".to_owned())]
            .into_iter()
            .collect();
        let mut connect = Connect::new(Arc::new("client_anonymous".to_owned()), 10);
        connect.username = Some(Arc::new("user".to_owned()));
        connect.password = Some(Bytes::from(b"pass".to_vec()));
        let connack = Connack::new(false, Success);
        do_test(config, connect, connack).await;
    }
}

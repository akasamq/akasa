use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v3::*;
use mqtt_proto::*;
use tokio::{task::JoinHandle, time::sleep};
use ConnectReturnCode::*;

use crate::config::{AuthType, Config};
use crate::tests::utils::MockConn;

async fn do_test(config: Config, connect: Connect, connack: Connack) -> JoinHandle<io::Result<()>> {
    let (conn, mut control) = MockConn::new(3333, config);
    let task = control.start(conn);

    let finished = connack.code != Accepted;
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    assert_eq!(packet, Packet::Connack(connack));

    sleep(Duration::from_millis(10)).await;
    assert_eq!(task.is_finished(), finished);
    task
}

#[tokio::test]
async fn test_connect_malformed_packet() {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let task = control.start(conn);
    control.write_data(b"abcdefxyzxyz123123".to_vec()).await;

    sleep(Duration::from_millis(10)).await;
    assert!(control.try_read_packet().is_err());
    assert!(task.is_finished());
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn test_connect_invalid_first_packet() {
    // subscribe
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let task = control.start(conn);

        let sub_pk_id = Pid::try_from(23).unwrap();
        let subscribe = Subscribe::new(
            sub_pk_id,
            vec![(
                TopicFilter::try_from("abc/0".to_owned()).unwrap(),
                QoS::Level0,
            )],
        );
        control.write_packet(subscribe.into()).await;

        sleep(Duration::from_millis(10)).await;
        assert!(control.try_read_packet().is_err());
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
    // publish
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let task = control.start(conn);

        let mut publish = Publish::new(
            QosPid::Level0,
            TopicName::try_from("xyz/1".to_owned()).unwrap(),
            Bytes::from(vec![3, 5, 55]),
        );
        publish.retain = true;
        control.write_packet(publish.into()).await;

        sleep(Duration::from_millis(10)).await;
        assert!(control.try_read_packet().is_err());
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn test_connect_v31() {
    // connect accepted
    {
        let connect = Connect {
            protocol: Protocol::V310,
            ..Connect::new(Arc::new("client_anonymous".to_owned()), 10)
        };
        let connack = Connack::new(false, Accepted);
        do_test(Config::default(), connect, connack).await;
    }
    // connect identifier rejected: empty identifier
    {
        let connect = Connect {
            protocol: Protocol::V310,
            ..Connect::new(Arc::new("".to_owned()), 10)
        };
        let connack = Connack::new(false, IdentifierRejected);
        do_test(Config::default(), connect, connack).await;
    }
    // connect identifier rejected: identifier too large
    {
        let connect = Connect {
            protocol: Protocol::V310,
            ..Connect::new(Arc::new("a".repeat(24)), 10)
        };
        let connack = Connack::new(false, IdentifierRejected);
        do_test(Config::default(), connect, connack).await;
    }
}

#[tokio::test]
async fn test_connect_v311() {
    // connect accepted: identifier empty
    {
        let connect = Connect::new(Arc::new("".to_owned()), 30);
        let connack = Connack::new(false, Accepted);
        do_test(Config::default(), connect, connack).await;
    }
}

#[tokio::test]
async fn test_connect_keepalive() {
    // connect accepted: zero keep_alive
    {
        let connect = Connect::new(Arc::new("client identifier".to_owned()), 0);
        let connack = Connack::new(false, Accepted);
        do_test(Config::default(), connect, connack).await;
    }

    // connect accepted: non-zero keep_alive
    {
        let connect = Connect::new(Arc::new("client identifier".to_owned()), 22);
        let connack = Connack::new(false, Accepted);
        do_test(Config::default(), connect, connack).await;
    }
}

#[tokio::test]
async fn test_will() {
    // connect accepted: with will
    {
        let connect = Connect {
            protocol: Protocol::V311,
            clean_session: true,
            keep_alive: 30,
            client_id: Arc::new("client identifier".to_owned()),
            last_will: Some(LastWill {
                qos: QoS::Level1,
                retain: false,
                topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
                message: Bytes::from(vec![1, 2, 3, 4]),
            }),
            username: None,
            password: None,
        };
        let connack = Connack::new(false, Accepted);
        do_test(Config::default(), connect, connack).await;
    }

    // connect accepted: with invalid will topic (start with "$")
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let task = control.start(conn);
        let connect = Connect {
            protocol: Protocol::V311,
            clean_session: true,
            keep_alive: 30,
            client_id: Arc::new("client identifier".to_owned()),
            last_will: Some(LastWill {
                qos: QoS::Level1,
                retain: false,
                topic_name: TopicName::try_from("$topic/1".to_owned()).unwrap(),
                message: Bytes::from(vec![1, 2, 3, 4]),
            }),
            username: None,
            password: None,
        };
        control.write_packet(connect.into()).await;
        assert!(control.try_read_packet().is_err());

        sleep(Duration::from_millis(10)).await;
        assert!(task.is_finished());
        assert!(task.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn test_connect_auth() {
    // allow anonymous
    {
        let config = Config::default();
        let connect = Connect::new(Arc::new("client_anonymous".to_owned()), 10);
        let connack = Connack::new(false, Accepted);
        do_test(config, connect, connack).await;
    }
    // empty username/password
    {
        let mut config = Config::default();
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
        let mut config = Config::default();
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
        let mut config = Config::default();
        config.auth_types = vec![AuthType::UsernamePassword];
        config.users = [("user".to_owned(), "pass".to_owned())]
            .into_iter()
            .collect();
        let mut connect = Connect::new(Arc::new("client_anonymous".to_owned()), 10);
        connect.username = Some(Arc::new("user".to_owned()));
        connect.password = Some(Bytes::from(b"pass".to_vec()));
        let connack = Connack::new(false, Accepted);
        do_test(config, connect, connack).await;
    }
}

use std::io;
use std::time::Duration;

use mqtt::{
    control::variable_header::ConnectReturnCode,
    packet::{ConnackPacket, ConnectPacket, VariablePacket},
    TopicName,
};
use tokio::{task::JoinHandle, time::sleep};
use ConnectReturnCode::*;

use crate::config::{AuthType, Config};
use crate::tests::utils::MockConn;

async fn do_test(
    config: Config,
    connect: ConnectPacket,
    connack: ConnackPacket,
) -> JoinHandle<io::Result<()>> {
    let (conn, mut control) = MockConn::new(3333, config);
    let join = control.start(conn);

    let finished = connack.connect_return_code() != ConnectionAccepted;
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);
    assert_eq!(join.is_finished(), finished);
    join
}

#[tokio::test]
async fn test_connect_malformed_packet() {
    let (conn, mut control) = MockConn::new(3333, Config::default());
    let join = control.start(conn);
    control.write_data(b"abcdefxyzxyz123123".to_vec()).await;
    sleep(Duration::from_millis(10)).await;
    assert!(control.try_read_packet().is_err());
    assert!(join.is_finished());
    assert!(join.await.unwrap().is_err());
}

#[tokio::test]
async fn test_connect_v310() {
    // connect accepted
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let connect = ConnectPacket::with_level("MQIsdp", "client_anonymous", 0x03).unwrap();
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket = ConnackPacket::new(false, ConnectionAccepted).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(!join.is_finished());
    }
    // connect identifier rejected: empty identifier
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let connect = ConnectPacket::with_level("MQIsdp", "", 0x03).unwrap();
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket = ConnackPacket::new(false, IdentifierRejected).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(join.is_finished());
    }
    // connect identifier rejected: identifier too large
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let connect = ConnectPacket::with_level("MQIsdp", "a".repeat(24), 0x03).unwrap();
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket = ConnackPacket::new(false, IdentifierRejected).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(join.is_finished());
    }
}

#[tokio::test]
async fn test_connect_v311() {
    // connect accepted: identifier empty
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let connect = ConnectPacket::new("");
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket = ConnackPacket::new(false, ConnectionAccepted).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(!join.is_finished());
    }
}

#[tokio::test]
async fn test_connect_invalid_protocol() {
    // connect rejrected: invalid protocol name
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let connect = ConnectPacket::with_level("MQISDP", "aaa", 0x03).unwrap();
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket =
            ConnackPacket::new(false, UnacceptableProtocolVersion).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(join.is_finished());
    }
    // connect rejrected: invalid protocol level
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let connect = ConnectPacket::with_level("MQTT", "aaa", 0x03).unwrap();
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket =
            ConnackPacket::new(false, UnacceptableProtocolVersion).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(join.is_finished());
    }
}

#[tokio::test]
async fn test_connect_keepalive() {
    // connect accepted: zero keep_alive
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let mut connect = ConnectPacket::new("client identifier");
        connect.set_keep_alive(0);
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket = ConnackPacket::new(false, ConnectionAccepted).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(!join.is_finished());
    }

    // connect accepted: non-zero keep_alive
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let mut connect = ConnectPacket::new("client identifier");
        connect.set_keep_alive(22);
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket = ConnackPacket::new(false, ConnectionAccepted).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(!join.is_finished());
    }
}

#[tokio::test]
async fn test_will() {
    // connect accepted: with will
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let mut connect = ConnectPacket::new("client identifier");
        connect.set_will(Some((TopicName::new("topic/1").unwrap(), vec![1, 2, 3, 4])));
        connect.set_will_qos(1);
        control.write_packet(connect.into()).await;
        let packet = control.read_packet().await;
        let expected_packet: VariablePacket = ConnackPacket::new(false, ConnectionAccepted).into();
        assert_eq!(packet, expected_packet);

        sleep(Duration::from_millis(10)).await;
        assert!(!join.is_finished());
    }

    // connect accepted: with invalid will topic (start with "$")
    {
        let (conn, mut control) = MockConn::new(3333, Config::default());
        let join = control.start(conn);
        let mut connect = ConnectPacket::new("client identifier");
        connect.set_will(Some((
            TopicName::new("$topic/1").unwrap(),
            vec![1, 2, 3, 4],
        )));
        connect.set_will_qos(1);
        control.write_packet(connect.into()).await;
        assert!(control.try_read_packet().is_err());

        sleep(Duration::from_millis(10)).await;
        assert!(join.is_finished());
        assert!(join.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn test_connect_auth() {
    // allow anonymous
    {
        let config = Config::default();
        let mut connect = ConnectPacket::new("client_anonymous");
        connect.set_keep_alive(10);
        let connack = ConnackPacket::new(false, ConnectionAccepted);
        do_test(config, connect, connack).await;
    }
    // empty username/password
    {
        let mut config = Config::default();
        config.auth_types = vec![AuthType::UsernamePassword];
        config.users = [("user".to_owned(), "pass".to_owned())]
            .into_iter()
            .collect();
        let mut connect = ConnectPacket::new("client_anonymous");
        connect.set_keep_alive(10);
        let connack = ConnackPacket::new(false, BadUserNameOrPassword);
        do_test(config, connect, connack).await;
    }
    // wrong username/password
    {
        let mut config = Config::default();
        config.auth_types = vec![AuthType::UsernamePassword];
        config.users = [("user".to_owned(), "pass".to_owned())]
            .into_iter()
            .collect();
        let mut connect = ConnectPacket::new("client_anonymous");
        connect.set_keep_alive(10);
        connect.set_user_name(Some("xxx".to_owned()));
        connect.set_password(Some("yyy".to_owned()));
        let connack = ConnackPacket::new(false, BadUserNameOrPassword);
        do_test(config, connect, connack).await;
    }
    // right username/password
    {
        let mut config = Config::default();
        config.auth_types = vec![AuthType::UsernamePassword];
        config.users = [("user".to_owned(), "pass".to_owned())]
            .into_iter()
            .collect();
        let mut connect = ConnectPacket::new("client_anonymous");
        connect.set_keep_alive(10);
        connect.set_user_name(Some("user".to_owned()));
        connect.set_password(Some("pass".to_owned()));
        let connack = ConnackPacket::new(false, ConnectionAccepted);
        do_test(config, connect, connack).await;
    }
}

use mqtt::{
    control::variable_header::ConnectReturnCode,
    packet::{ConnackPacket, ConnectPacket, VariablePacket},
};
use ConnectReturnCode::*;

use crate::config::{AuthType, Config};
use crate::tests::utils::MockConn;

#[tokio::test]
async fn test_connect() {
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

async fn do_test(config: Config, connect: ConnectPacket, connack: ConnackPacket) {
    let (conn, mut control) = MockConn::new(3333, config);
    let join = control.start(conn);

    let finished = connack.connect_return_code() != ConnectionAccepted;
    control.write_packet(connect.into()).await;
    let packet = control.read_packet().await;
    let expected_packet = VariablePacket::ConnackPacket(connack);
    assert_eq!(packet, expected_packet);
    assert_eq!(join.is_finished(), finished);
}

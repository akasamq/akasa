use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v5::*;
use scram::{hash_password, ScramClient};
use tokio::time::sleep;

use crate::config::{Config, SaslMechanism, ScramPasswordInfo};
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

#[tokio::test]
async fn test_auth_simple_success() {
    let mut config = Config::new_allow_anonymous();
    let user = "nahida";
    let pass = "sumeru";
    let salt = b"salt-archon";
    let iterations: u16 = 4096;
    let pwd_iterations = NonZeroU32::new(iterations as u32).unwrap();
    let hashed_pass = hash_password(pass, pwd_iterations, salt);

    config.scram_users.insert(
        user.to_owned(),
        ScramPasswordInfo {
            hashed_password: hashed_pass.to_vec(),
            iterations,
            salt: salt.to_vec(),
        },
    );
    config.sasl_mechanisms = vec![SaslMechanism::ScramSha256].into_iter().collect();

    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let auth_method = Arc::new("SCRAM-SHA-256".to_owned());
    let scram_client = ScramClient::new(user, pass, None);
    let (scram_client, client_first) = scram_client.client_first();
    println!("client_first: {client_first}");

    let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
    connect.properties.auth_method = Some(Arc::clone(&auth_method));
    connect.properties.auth_data = Some(Bytes::from(client_first));
    client.write_packet(connect.into()).await;

    let received_pkt = client.read_packet().await;
    let server_first = if let Packet::Auth(auth) = received_pkt {
        assert_eq!(auth.reason_code, AuthReasonCode::ContinueAuthentication);
        assert_eq!(auth.properties.auth_method, Some(Arc::clone(&auth_method)));
        String::from_utf8(auth.properties.auth_data.unwrap().as_ref().to_vec()).unwrap()
    } else {
        panic!("received packet: {received_pkt:?}");
    };
    println!("server_first: {server_first}");

    let scram_client = scram_client.handle_server_first(&server_first).unwrap();
    let (scram_client, client_final) = scram_client.client_final();
    println!("client_final: {client_final}");
    let final_pkt = Auth {
        reason_code: AuthReasonCode::ContinueAuthentication,
        properties: AuthProperties {
            auth_method: Some(Arc::clone(&auth_method)),
            auth_data: Some(Bytes::from(client_final)),
            ..Default::default()
        },
    };
    client.write_packet(final_pkt.into()).await;
    let received_pkt = client.read_packet().await;
    let server_final = if let Packet::Connack(connack) = received_pkt {
        assert!(!connack.session_present);
        assert_eq!(connack.reason_code, ConnectReasonCode::Success);
        assert_eq!(connack.properties.auth_method.unwrap(), auth_method);
        let auth_data = connack.properties.auth_data.unwrap();
        String::from_utf8(auth_data.as_ref().to_vec()).unwrap()
    } else {
        panic!("received packet: {received_pkt:?}");
    };
    scram_client.handle_server_final(&server_final).unwrap();

    sleep(Duration::from_millis(20)).await;
    client.write_packet(Packet::Pingreq).await;
    assert_eq!(client.read_packet().await, Packet::Pingresp);
    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_auth_missing_auth_method() {
    let config = Config::new_allow_anonymous();
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
    connect.properties.auth_method = None;
    connect.properties.auth_data = Some(Bytes::from("xxxx/invalid/"));
    client.write_packet(connect.into()).await;

    let received_pkt = client.read_packet().await;
    if let Packet::Connack(connack) = received_pkt {
        assert_eq!(connack.reason_code, ConnectReasonCode::ProtocolError);
        assert_eq!(
            connack.properties.reason_string.unwrap().as_str(),
            "AuthenticationMethod is missing"
        );
    } else {
        panic!("received packet: {received_pkt:?}");
    };
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_auth_method_not_support() {
    for method in ["SCRAM-SHA-256-PLUS", "SCRAM-xxx", "xxx"] {
        let config = Config::new_allow_anonymous();
        let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
        let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

        let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
        connect.properties.auth_method = Some(Arc::new(method.to_owned()));
        connect.properties.auth_data = Some(Bytes::from("xxxx/invalid/"));
        client.write_packet(connect.into()).await;

        let received_pkt = client.read_packet().await;
        if let Packet::Connack(connack) = received_pkt {
            assert_eq!(connack.reason_code, ConnectReasonCode::BadAuthMethod);
            assert_eq!(
                connack.properties.reason_string.unwrap().as_str(),
                "auth method not supported"
            );
        } else {
            panic!("received packet: {received_pkt:?}");
        };
        sleep(Duration::from_millis(20)).await;
        assert!(task.is_finished());
    }
}

#[tokio::test]
async fn test_auth_invalid_client_first() {
    let mut config = Config::new_allow_anonymous();
    let user = "nahida";
    let pass = "sumeru";
    let salt = b"salt-archon";
    let iterations: u16 = 4096;
    let pwd_iterations = NonZeroU32::new(iterations as u32).unwrap();
    let hashed_pass = hash_password(pass, pwd_iterations, salt);

    config.scram_users.insert(
        user.to_owned(),
        ScramPasswordInfo {
            hashed_password: hashed_pass.to_vec(),
            iterations,
            salt: salt.to_vec(),
        },
    );
    config.sasl_mechanisms = vec![SaslMechanism::ScramSha256].into_iter().collect();

    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let auth_method = Arc::new("SCRAM-SHA-256".to_owned());
    let client_first = "xxxx/invalid/";

    let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
    connect.properties.auth_method = Some(Arc::clone(&auth_method));
    connect.properties.auth_data = Some(Bytes::from(client_first));
    client.write_packet(connect.into()).await;

    let received_pkt = client.read_packet().await;
    if let Packet::Connack(connack) = received_pkt {
        assert_eq!(connack.reason_code, ConnectReasonCode::NotAuthorized);
        assert_eq!(
            connack.properties.reason_string.unwrap().as_str(),
            "invalid client first data"
        );
    } else {
        panic!("received packet: {received_pkt:?}");
    };
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_auth_invalid_password() {
    let mut config = Config::new_allow_anonymous();
    let user = "nahida";
    let pass = "sumeru";
    let salt = b"salt-archon";
    let iterations: u16 = 4096;
    let pwd_iterations = NonZeroU32::new(iterations as u32).unwrap();
    let hashed_pass = hash_password(pass, pwd_iterations, salt);

    config.scram_users.insert(
        user.to_owned(),
        ScramPasswordInfo {
            hashed_password: hashed_pass.to_vec(),
            iterations,
            salt: salt.to_vec(),
        },
    );
    config.sasl_mechanisms = vec![SaslMechanism::ScramSha256].into_iter().collect();

    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let auth_method = Arc::new("SCRAM-SHA-256".to_owned());
    let scram_client = ScramClient::new(user, "invalid pass xxx", None);
    let (scram_client, client_first) = scram_client.client_first();
    println!("client_first: {client_first}");

    let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
    connect.properties.auth_method = Some(Arc::clone(&auth_method));
    connect.properties.auth_data = Some(Bytes::from(client_first));
    client.write_packet(connect.into()).await;

    let received_pkt = client.read_packet().await;
    let server_first = if let Packet::Auth(auth) = received_pkt {
        assert_eq!(auth.reason_code, AuthReasonCode::ContinueAuthentication);
        assert_eq!(auth.properties.auth_method, Some(Arc::clone(&auth_method)));
        String::from_utf8(auth.properties.auth_data.unwrap().as_ref().to_vec()).unwrap()
    } else {
        panic!("received packet: {received_pkt:?}");
    };
    println!("server_first: {server_first}");

    let scram_client = scram_client.handle_server_first(&server_first).unwrap();
    let (_, client_final) = scram_client.client_final();
    println!("client_final: {client_final}");
    let final_pkt = Auth {
        reason_code: AuthReasonCode::ContinueAuthentication,
        properties: AuthProperties {
            auth_method: Some(Arc::clone(&auth_method)),
            auth_data: Some(Bytes::from(client_final)),
            ..Default::default()
        },
    };
    client.write_packet(final_pkt.into()).await;
    let received_pkt = client.read_packet().await;
    if let Packet::Connack(connack) = received_pkt {
        assert_eq!(connack.reason_code, ConnectReasonCode::NotAuthorized);
        assert_eq!(
            connack.properties.reason_string.unwrap().as_str(),
            "invalid client final data"
        );
    } else {
        panic!("received packet: {received_pkt:?}");
    };

    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_auth_invalid_client_final() {
    let mut config = Config::new_allow_anonymous();
    let user = "nahida";
    let pass = "sumeru";
    let salt = b"salt-archon";
    let iterations: u16 = 4096;
    let pwd_iterations = NonZeroU32::new(iterations as u32).unwrap();
    let hashed_pass = hash_password(pass, pwd_iterations, salt);

    config.scram_users.insert(
        user.to_owned(),
        ScramPasswordInfo {
            hashed_password: hashed_pass.to_vec(),
            iterations,
            salt: salt.to_vec(),
        },
    );
    config.sasl_mechanisms = vec![SaslMechanism::ScramSha256].into_iter().collect();

    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let auth_method = Arc::new("SCRAM-SHA-256".to_owned());
    let scram_client = ScramClient::new(user, "invalid pass xxx", None);
    let (_, client_first) = scram_client.client_first();
    println!("client_first: {client_first}");

    let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
    connect.properties.auth_method = Some(Arc::clone(&auth_method));
    connect.properties.auth_data = Some(Bytes::from(client_first));
    client.write_packet(connect.into()).await;

    let received_pkt = client.read_packet().await;
    let server_first = if let Packet::Auth(auth) = received_pkt {
        assert_eq!(auth.reason_code, AuthReasonCode::ContinueAuthentication);
        assert_eq!(auth.properties.auth_method, Some(Arc::clone(&auth_method)));
        String::from_utf8(auth.properties.auth_data.unwrap().as_ref().to_vec()).unwrap()
    } else {
        panic!("received packet: {received_pkt:?}");
    };
    println!("server_first: {server_first}");

    let final_pkt = Auth {
        reason_code: AuthReasonCode::ContinueAuthentication,
        properties: AuthProperties {
            auth_method: Some(Arc::clone(&auth_method)),
            auth_data: Some(Bytes::from("invalid/xxx")),
            ..Default::default()
        },
    };
    client.write_packet(final_pkt.into()).await;
    let received_pkt = client.read_packet().await;
    if let Packet::Connack(connack) = received_pkt {
        assert_eq!(connack.reason_code, ConnectReasonCode::NotAuthorized);
        assert_eq!(
            connack.properties.reason_string.unwrap().as_str(),
            "invalid client final data"
        );
    } else {
        panic!("received packet: {received_pkt:?}");
    };

    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

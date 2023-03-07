use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::sync::oneshot;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

async fn test_session_expired_with(first_clean_start: bool) {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let client_id = "client id";

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        let update_connect = |c: &mut Connect| {
            c.clean_start = first_clean_start;
            c.properties.session_expiry_interval = Some(2);
        };
        client.connect_with(client_id, update_connect, |_| ()).await;
        client
            .subscribe(11, vec![("abc/1", SubscriptionOptions::new(QoS::Level1))])
            .await;
        client
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client
            .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client.send_puback(1).await;
        drop(client);
    }
    sleep(Duration::from_millis(200)).await;
    assert!(task.is_finished());
    sleep(Duration::from_millis(2000)).await;

    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));
    client2.connect(client_id, false, false).await;
    sleep(Duration::from_millis(20)).await;
    assert!(!task2.is_finished());
}

#[tokio::test]
async fn test_session_expired_clean_start() {
    test_session_expired_with(true).await;
}

#[tokio::test]
async fn test_session_expired_not_clean_start() {
    test_session_expired_with(false).await;
}

#[tokio::test]
async fn test_session_not_expired() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let client_id = "client id";

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    // client first connection
    {
        let update_connect = |c: &mut Connect| {
            c.clean_start = false;
            c.properties.session_expiry_interval = Some(2);
        };
        client.connect_with(client_id, update_connect, |_| ()).await;
        client
            .subscribe(11, vec![("abc/1", SubscriptionOptions::new(QoS::Level1))])
            .await;
        client
            .publish(QoS::Level1, 12, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client
            .recv_publish(QoS::Level1, 1, "abc/1", vec![3, 5, 55], |_| ())
            .await;
        client.send_puback(1).await;
        drop(client);
    }
    sleep(Duration::from_millis(200)).await;
    assert!(task.is_finished());
    sleep(Duration::from_millis(1000)).await;

    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));
    client2.connect(client_id, false, true).await;
    sleep(Duration::from_millis(20)).await;
    assert!(!task2.is_finished());
}

#[tokio::test]
async fn test_receive_max_client() {
    let mut config = Config::new_allow_anonymous();
    config.max_inflight_client = 8;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (_task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));

    let (tx, rx) = oneshot::channel();

    // client 1: publisher
    let task1 = tokio::spawn(async move {
        client1.connect("client id 1", true, false).await;
        rx.await.unwrap();
        for pub_pid in 1..17u16 {
            client1
                .publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
                .await;
        }
    });

    let receive_max = 4;
    // client 2: subscriber
    let update_connect = |c: &mut Connect| {
        c.clean_start = false;
        c.properties.session_expiry_interval = Some(2);
        c.properties.receive_max = Some(receive_max);
    };
    client2
        .connect_with("client id 2", update_connect, |_| ())
        .await;

    let sub_topics = vec![("xyz/1", SubscriptionOptions::new(QoS::Level1))];
    client2.subscribe(2, sub_topics).await;

    tx.send(()).unwrap();

    // not receive pending messages
    sleep(Duration::from_millis(100)).await;
    assert!(task1.await.is_ok());

    for pub_pid in 1..(receive_max + 1) {
        client2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
            .await;
    }

    sleep(Duration::from_millis(50)).await;
    // Reach max inflight, we can not receive more publish packet
    assert!(client2.try_read_packet_is_empty());

    for pub_pid in 1..(receive_max + 1) {
        client2.send_puback(pub_pid).await;
    }
    for pub_pid in (receive_max + 1)..9 {
        client2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
            .await;
        // If connection not receive packets, server forbid send more packets
        client2.send_puback(pub_pid).await;
    }

    for pub_pid in 9..17u16 {
        client2
            .recv_publish(QoS::Level1, pub_pid, "xyz/1", pub_pid.to_string(), |_| ())
            .await;
        client2.send_puback(pub_pid).await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(client2.try_read_packet_is_empty());
}

#[tokio::test]
async fn test_receive_max_server() {
    let mut config = Config::new_allow_anonymous();
    config.max_inflight_server = 4;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client id", true, false).await;
    for pid in 1..(4 + 1) {
        client
            .send_publish(QoS::Level2, pid, "abc/2", pid.to_string(), |_| ())
            .await;
        client
            .recv_pubrec(pid, PubrecReasonCode::NoMatchingSubscribers)
            .await;
    }
    client
        .send_publish(QoS::Level2, 5, "abc/2", "5", |_| ())
        .await;
    let err_pkt = client.read_packet().await;
    if let Packet::Disconnect(pkt) = err_pkt {
        assert_eq!(
            pkt.reason_code,
            DisconnectReasonCode::ReceiveMaximumExceeded
        );
    } else {
        panic!("invalid packet: {err_pkt:?}");
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

// to limit server
#[tokio::test]
async fn test_max_packet_size_client() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    // Without reason string
    {
        let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
        let update_connect = |c: &mut Connect| {
            c.clean_start = true;
            c.properties.session_expiry_interval = None;
            c.properties.max_packet_size = Some(6);
        };
        client
            .connect_with("client id", update_connect, |_| ())
            .await;

        // Filter out publish packets
        client
            .subscribe(2, vec![("abc/1", SubscriptionOptions::new(QoS::Level0))])
            .await;
        client
            .send_publish(QoS::Level0, 3, "abc/1", "123456", |_| ())
            .await;
        sleep(Duration::from_millis(20)).await;
        assert!(client.try_read_packet_is_empty());

        let mut disconnect = Disconnect::new_normal();
        // to casue server send an error disconnect packet, due to limited max
        // packet size, server will ommit the error message in properties.
        disconnect.properties.session_expiry_interval = Some(10);
        client.write_packet(disconnect.into()).await;
        let err_pkt = client.read_packet().await;
        if let Packet::Disconnect(pkt) = err_pkt {
            assert_eq!(pkt.reason_code, DisconnectReasonCode::ProtocolError);
            assert!(pkt.properties.reason_string.is_none());
        } else {
            panic!("invalid packet: {err_pkt:?}");
        }
        sleep(Duration::from_millis(20)).await;
        assert!(task.is_finished());
    }

    // With reason string
    {
        let payload_ok = b"SessionExpiryInterval is 0 in CONNECT";
        let payload_large = b"@SessionExpiryInterval is 0 in CONNECT";
        let sample_packet_ok = Packet::Publish(Publish::new(
            QosPid::Level0,
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            // make sure disconnect.properties.reason_string.is_some()
            Bytes::from(payload_ok.to_vec()),
        ));
        let sample_packet_large = Packet::Publish(Publish::new(
            QosPid::Level0,
            TopicName::try_from("abc/1".to_owned()).unwrap(),
            Bytes::from(payload_large.to_vec()),
        ));
        let encode_len_ok = sample_packet_ok.encode_len().unwrap();
        let encode_len_large = sample_packet_large.encode_len().unwrap();
        assert_eq!(encode_len_ok + 1, encode_len_large);

        let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
        let update_connect = |c: &mut Connect| {
            c.clean_start = true;
            c.properties.session_expiry_interval = None;
            c.properties.max_packet_size = Some(encode_len_ok as u32);
        };
        client
            .connect_with("client id", update_connect, |_| ())
            .await;

        client
            .subscribe(2, vec![("abc/1", SubscriptionOptions::new(QoS::Level0))])
            .await;
        client.write_packet(sample_packet_ok).await;

        client
            .recv_publish(QoS::Level0, 0, "abc/1", payload_ok, |_| ())
            .await;
        client.write_packet(sample_packet_large).await;
        sleep(Duration::from_millis(20)).await;
        assert!(client.try_read_packet_is_empty());

        let mut disconnect = Disconnect::new_normal();
        // to casue server send an error disconnect packet, due to limited max
        // packet size, server will ommit the error message in properties.
        disconnect.properties.session_expiry_interval = Some(10);
        client.write_packet(disconnect.into()).await;
        let err_pkt = client.read_packet().await;
        if let Packet::Disconnect(pkt) = err_pkt {
            assert_eq!(pkt.reason_code, DisconnectReasonCode::ProtocolError);
            assert!(pkt.properties.reason_string.is_some());
        } else {
            panic!("invalid packet: {err_pkt:?}");
        }
        sleep(Duration::from_millis(20)).await;
        assert!(task.is_finished());
    }
}

// to limit client
#[tokio::test]
async fn test_max_packet_size_server() {
    let payload_ok = b"01234567890123456789";
    let payload_large = b"@01234567890123456789";
    let sample_packet_ok = Packet::Publish(Publish::new(
        QosPid::Level0,
        TopicName::try_from("abc/1".to_owned()).unwrap(),
        // make sure disconnect.properties.reason_string.is_some()
        Bytes::from(payload_ok.to_vec()),
    ));
    let sample_packet_large = Packet::Publish(Publish::new(
        QosPid::Level0,
        TopicName::try_from("abc/1".to_owned()).unwrap(),
        Bytes::from(payload_large.to_vec()),
    ));
    let encode_len_ok = sample_packet_ok.encode_len().unwrap();
    let encode_len_large = sample_packet_large.encode_len().unwrap();
    assert_eq!(encode_len_ok + 1, encode_len_large);

    let mut config = Config::new_allow_anonymous();
    config.max_packet_size_server = encode_len_ok as u32;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client id", true, false).await;

    client.write_packet(sample_packet_ok).await;
    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    client.write_packet(sample_packet_large).await;
    let err_pkt = client.read_packet().await;
    if let Packet::Disconnect(pkt) = err_pkt {
        assert_eq!(pkt.reason_code, DisconnectReasonCode::PacketTooLarge,);
    } else {
        panic!("invalid packet: {err_pkt:?}");
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_connack_properties() {
    let mut config = Config::new_allow_anonymous();
    config.max_packet_size_server = 90;
    config.max_keep_alive = 30;
    config.max_allowed_qos = 1;
    config.max_inflight_client = 11;
    config.max_inflight_server = 13;
    // properties
    config.max_session_expiry_interval = 60;
    config.topic_alias_max = 40;
    config.retain_available = false;
    config.shared_subscription_available = false;
    config.subscription_id_available = false;
    config.wildcard_subscription_available = false;
    assert!(config.is_valid());

    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let connect = {
        let mut c = Connect::new(Arc::new(String::new()), 32);
        c.clean_start = true;
        c.keep_alive = 32;
        let p = &mut c.properties;
        p.session_expiry_interval = Some(62);
        p.receive_max = Some(15);
        p.max_packet_size = Some(92);
        p.topic_alias_max = Some(22);
        c
    };
    let assert_connack = |c: &Connack| {
        assert!(!c.session_present);
        assert_eq!(c.reason_code, ConnectReasonCode::Success);
        let p = &c.properties;
        assert!(p.assigned_client_id.is_some());
        assert_eq!(p.session_expiry_interval, Some(60));
        assert_eq!(p.receive_max, Some(13));
        assert_eq!(p.max_qos, Some(QoS::Level1));
        assert_eq!(p.server_keep_alive, Some(30));
        assert_eq!(p.max_packet_size, Some(90));
        assert_eq!(p.topic_alias_max, Some(40));
        assert_eq!(p.retain_available, Some(false));
        assert_eq!(p.wildcard_subscription_available, Some(false));
        assert_eq!(p.subscription_id_available, Some(false));
        assert_eq!(p.shared_subscription_available, Some(false));
    };
    client.write_packet(connect.into()).await;
    let pkt = client.read_packet().await;
    if let Packet::Connack(connack) = pkt {
        assert_connack(&connack);
    } else {
        panic!("invalid packet: {pkt:?}");
    }
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_retain_not_supported() {
    let mut config = Config::new_allow_anonymous();
    config.retain_available = false;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
    connect.clean_start = true;
    connect.last_will = Some(LastWill {
        qos: QoS::Level1,
        retain: true,
        topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
        payload: Bytes::from(vec![1, 2, 3, 4]),
        properties: Default::default(),
    });
    client.write_packet(connect.into()).await;
    let pkt = client.read_packet().await;
    if let Packet::Connack(connack) = pkt {
        assert!(!connack.session_present);
        assert_eq!(connack.reason_code, ConnectReasonCode::RetainNotSupported);
    } else {
        panic!("invalid packet: {pkt:?}");
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_will_qos_not_supported() {
    let mut config = Config::new_allow_anonymous();
    config.max_allowed_qos = 1;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));
    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let mut connect = Connect::new(Arc::new("client".to_owned()), 32);
    connect.clean_start = true;
    connect.last_will = Some(LastWill {
        qos: QoS::Level2,
        retain: true,
        topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
        payload: Bytes::from(vec![1, 2, 3, 4]),
        properties: Default::default(),
    });
    client.write_packet(connect.into()).await;
    let pkt = client.read_packet().await;
    if let Packet::Connack(connack) = pkt {
        assert!(!connack.session_present);
        assert_eq!(connack.reason_code, ConnectReasonCode::QoSNotSupported);
    } else {
        panic!("invalid packet: {pkt:?}");
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_will_delay_interval_reached() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));

    client2.connect("client2", true, false).await;
    client2
        .subscribe(1, vec![("topic/1", SubscriptionOptions::new(QoS::Level2))])
        .await;

    let mut connect = Connect::new(Arc::new("client1".to_owned()), 32);
    connect.clean_start = false;
    connect.properties.session_expiry_interval = Some(60);
    connect.last_will = Some(LastWill {
        qos: QoS::Level0,
        retain: false,
        topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
        payload: Bytes::from("will message"),
        properties: WillProperties {
            delay_interval: Some(2),
            ..Default::default()
        },
    });
    client1.write_packet(connect.into()).await;
    let pkt = client1.read_packet().await;
    if let Packet::Connack(connack) = pkt {
        assert!(!connack.session_present);
        assert_eq!(connack.reason_code, ConnectReasonCode::Success);
    } else {
        panic!("invalid packet: {pkt:?}");
    }
    client1.write_data(b"invalid data".to_vec()).await;

    client2
        .recv_publish(QoS::Level0, 0, "topic/1", "will message", |_| ())
        .await;

    assert!(task1.await.unwrap().is_ok());
    sleep(Duration::from_millis(20)).await;
    assert!(!task2.is_finished());
}

#[tokio::test]
async fn test_will_delay_interval_not_reached() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));
    let (_task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));

    client2.connect("client2", true, false).await;
    client2
        .subscribe(1, vec![("topic/1", SubscriptionOptions::new(QoS::Level2))])
        .await;

    let mut connect = Connect::new(Arc::new("client1".to_owned()), 32);
    connect.clean_start = false;
    connect.properties.session_expiry_interval = Some(60);
    connect.last_will = Some(LastWill {
        qos: QoS::Level0,
        retain: false,
        topic_name: TopicName::try_from("topic/1".to_owned()).unwrap(),
        payload: Bytes::from("will message"),
        properties: WillProperties {
            delay_interval: Some(2),
            ..Default::default()
        },
    });
    client1.write_packet(connect.into()).await;
    let pkt = client1.read_packet().await;
    if let Packet::Connack(connack) = pkt {
        assert!(!connack.session_present);
        assert_eq!(connack.reason_code, ConnectReasonCode::Success);
    } else {
        panic!("invalid packet: {pkt:?}");
    }
    client1.write_data(b"invalid data".to_vec()).await;

    sleep(Duration::from_millis(1030)).await;
    // reconnect the session
    let (_task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    client1.connect("client1", false, true).await;

    sleep(Duration::from_millis(1030)).await;
    assert!(client2.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task2.is_finished());
}

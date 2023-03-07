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
async fn test_payload_is_not_utf8() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    client.connect("client id", true, false).await;
    client
        .send_publish(QoS::Level1, 2, "abc/1", vec![0xff, 0xff, 0xff, 0xff], |p| {
            p.properties.payload_is_utf8 = Some(true);
        })
        .await;
    // FIXME: return Puback with PayloadFormatInvalid
    sleep(Duration::from_millis(20)).await;
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn test_message_expiry() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));

    client1.connect("client 1", false, false).await;
    client1
        .subscribe(1, vec![("abc/1", SubscriptionOptions::new(QoS::Level1))])
        .await;
    client1.disconnect_normal().await;
    assert!(task1.await.unwrap().is_ok());

    {
        client2.connect("client 2", true, false).await;
        for pid in [1, 2, 3, 4] {
            client2
                .publish(QoS::Level1, pid, "abc/1", pid.to_string(), |p| {
                    p.properties.message_expiry_interval = Some(pid as u32);
                })
                .await;
        }
        client2.disconnect_normal().await;
        assert!(task2.await.unwrap().is_ok());
    }

    sleep(Duration::from_millis(2000)).await;
    let (task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    client1.connect("client 1", false, true).await;
    for (pid, interval, payload) in [(3, 1, "3"), (4, 2, "4")] {
        client1
            .recv_publish(QoS::Level1, pid, "abc/1", payload, |p| {
                p.properties.message_expiry_interval = Some(interval);
            })
            .await;
    }
    sleep(Duration::from_millis(20)).await;
    assert!(client1.try_read_packet_is_empty());

    assert!(!task1.is_finished());
}

#[tokio::test]
async fn test_topic_name_empty() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    client.connect("client", true, false).await;
    client
        .subscribe(1, vec![("abc/0", SubscriptionOptions::new(QoS::Level2))])
        .await;
    client.send_publish(QoS::Level0, 0, "", "0", |_| ()).await;
    let received_pkt = client.read_packet().await;
    if let Packet::Disconnect(pkt) = received_pkt {
        assert_eq!(pkt.reason_code, DisconnectReasonCode::ProtocolError);
    } else {
        panic!("invalid received packet: {:?}", received_pkt);
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_topic_alias_ok() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    client.connect("client", true, false).await;
    for (pid, topic) in [(1, "abc/0"), (2, "xyz/+")] {
        client
            .subscribe(pid, vec![(topic, SubscriptionOptions::new(QoS::Level2))])
            .await;
    }

    // alias for topic "abc/0"
    client
        .send_publish(QoS::Level0, 0, "abc/0", "0", |p| {
            p.properties.topic_alias = Some(1);
        })
        .await;
    client
        .recv_publish(QoS::Level0, 0, "abc/0", "0", |p| {
            p.properties.topic_alias = None;
        })
        .await;
    for payload in ["1", "2", "3", "4"] {
        client
            .send_publish(QoS::Level0, 0, "", payload, |p| {
                p.properties.topic_alias = Some(1);
            })
            .await;
        client
            .recv_publish(QoS::Level0, 0, "abc/0", payload, |p| {
                p.properties.topic_alias = None;
            })
            .await;
    }

    // alias for topic "xyz/1"
    client
        .send_publish(QoS::Level0, 0, "xyz/1", "0", |p| {
            p.properties.topic_alias = Some(2);
        })
        .await;
    client
        .recv_publish(QoS::Level0, 0, "xyz/1", "0", |p| {
            p.properties.topic_alias = None;
        })
        .await;
    for payload in ["1", "2", "3", "4"] {
        client
            .send_publish(QoS::Level0, 0, "", payload, |p| {
                p.properties.topic_alias = Some(2);
            })
            .await;
        client
            .recv_publish(QoS::Level0, 0, "xyz/1", payload, |p| {
                p.properties.topic_alias = None;
            })
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_topic_alias_update() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    client.connect("client", true, false).await;
    client
        .subscribe(1, vec![("abc/0", SubscriptionOptions::new(QoS::Level2))])
        .await;
    client
        .subscribe(2, vec![("xyz/1", SubscriptionOptions::new(QoS::Level2))])
        .await;

    // alias for topic "abc/0"
    client
        .send_publish(QoS::Level0, 0, "abc/0", "0", |p| {
            p.properties.topic_alias = Some(1);
        })
        .await;
    client
        .recv_publish(QoS::Level0, 0, "abc/0", "0", |p| {
            p.properties.topic_alias = None;
        })
        .await;
    for payload in ["1", "2", "3", "4"] {
        client
            .send_publish(QoS::Level0, 0, "", payload, |p| {
                p.properties.topic_alias = Some(1);
            })
            .await;
        client
            .recv_publish(QoS::Level0, 0, "abc/0", payload, |p| {
                p.properties.topic_alias = None;
            })
            .await;
    }

    // update
    client
        .send_publish(QoS::Level0, 0, "xyz/1", "0", |p| {
            p.properties.topic_alias = Some(1);
        })
        .await;
    client
        .recv_publish(QoS::Level0, 0, "xyz/1", "0", |p| {
            p.properties.topic_alias = None;
        })
        .await;
    for payload in ["1", "2", "3", "4"] {
        client
            .send_publish(QoS::Level0, 0, "", payload, |p| {
                p.properties.topic_alias = Some(1);
            })
            .await;
        client
            .recv_publish(QoS::Level0, 0, "xyz/1", payload, |p| {
                p.properties.topic_alias = None;
            })
            .await;
    }

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_topic_alias_zero_value() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    client.connect("client", true, false).await;
    client
        .subscribe(1, vec![("abc/0", SubscriptionOptions::new(QoS::Level2))])
        .await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "0", |p| {
            p.properties.topic_alias = Some(0);
        })
        .await;
    let received_pkt = client.read_packet().await;
    if let Packet::Disconnect(pkt) = received_pkt {
        assert_eq!(pkt.reason_code, DisconnectReasonCode::ProtocolError);
        assert_eq!(
            pkt.properties.reason_string.unwrap().as_str(),
            "topic alias is 0"
        );
    } else {
        panic!("invalid received packet: {:?}", received_pkt);
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_topic_alias_too_large() {
    let mut config = Config::new_allow_anonymous();
    config.topic_alias_max = 256;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    client.connect("client", true, false).await;
    client
        .subscribe(1, vec![("abc/0", SubscriptionOptions::new(QoS::Level2))])
        .await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "0", |p| {
            p.properties.topic_alias = Some(257);
        })
        .await;
    let received_pkt = client.read_packet().await;
    if let Packet::Disconnect(pkt) = received_pkt {
        assert_eq!(pkt.reason_code, DisconnectReasonCode::TopicAliasInvalid);
        assert_eq!(
            pkt.properties.reason_string.unwrap().as_str(),
            "topic alias too large"
        );
    } else {
        panic!("invalid received packet: {:?}", received_pkt);
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_topic_alias_not_found() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));
    client.connect("client", true, false).await;
    client
        .subscribe(1, vec![("abc/0", SubscriptionOptions::new(QoS::Level2))])
        .await;
    client
        .send_publish(QoS::Level0, 0, "", "0", |p| {
            p.properties.topic_alias = Some(2);
        })
        .await;
    let received_pkt = client.read_packet().await;
    if let Packet::Disconnect(pkt) = received_pkt {
        assert_eq!(pkt.reason_code, DisconnectReasonCode::ProtocolError);
        assert_eq!(
            pkt.properties.reason_string.unwrap().as_str(),
            "topic alias not found"
        );
    } else {
        panic!("invalid received packet: {:?}", received_pkt);
    }
    sleep(Duration::from_millis(20)).await;
    assert!(task.is_finished());
}

#[tokio::test]
async fn test_request_response() {
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));

    client1.connect("client1", true, false).await;
    client2.connect("client2", true, false).await;

    client1
        .subscribe(
            1,
            vec![("/client1/response", SubscriptionOptions::new(QoS::Level2))],
        )
        .await;
    client2
        .subscribe(
            1,
            vec![("/client2/request", SubscriptionOptions::new(QoS::Level2))],
        )
        .await;

    tokio::spawn(async move {
        client2
            .recv_publish(QoS::Level0, 0, "/client2/request", "ping", |p| {
                let pp = &mut p.properties;
                pp.response_topic =
                    Some(TopicName::try_from("/client1/response".to_owned()).unwrap());
                pp.correlation_data = Some(Bytes::from("request-id-01"));
            })
            .await;
        client2
            .send_publish(QoS::Level0, 0, "/client1/response", "pong", |p| {
                p.properties.correlation_data = Some(Bytes::from("request-id-01"));
            })
            .await;
        sleep(Duration::from_millis(20)).await;
        assert!(!task2.is_finished());
    });

    client1
        .send_publish(QoS::Level0, 0, "/client2/request", "ping", |p| {
            let pp = &mut p.properties;
            pp.response_topic = Some(TopicName::try_from("/client1/response".to_owned()).unwrap());
            pp.correlation_data = Some(Bytes::from("request-id-01"));
        })
        .await;
    client1
        .recv_publish(QoS::Level0, 0, "/client1/response", "pong", |p| {
            p.properties.correlation_data = Some(Bytes::from("request-id-01"));
        })
        .await;

    sleep(Duration::from_millis(20)).await;
    assert!(!task1.is_finished());
}

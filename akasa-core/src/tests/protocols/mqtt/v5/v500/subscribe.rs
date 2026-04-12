use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::time::sleep;

use crate::config::Config;
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

#[tokio::test]
async fn test_simple_subscription_id() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let sub_opts = SubscriptionOptions::new(QoS::Level2);
    let mut pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    pkt.properties.subscription_id = Some(VarByteInt::try_from(33).unwrap());
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    client
        .send_publish(QoS::Level0, 0, "abc/0", "data", |_| ())
        .await;
    client
        .recv_publish(QoS::Level0, 0, "abc/0", "data", |p| {
            p.properties.subscription_id = Some(VarByteInt::try_from(33).unwrap());
        })
        .await;

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_id_with_multi_topics() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    let sub_opts = SubscriptionOptions::new(QoS::Level2);

    client.connect("client", true, false).await;
    client.subscribe(1, vec![("#", sub_opts)]).await;

    let filter1 = TopicFilter::try_from("abc/0").unwrap();
    let filter2 = TopicFilter::try_from("xyz/0").unwrap();
    let filter3 = TopicFilter::try_from("ijk/0").unwrap();
    let sub_pid = Pid::try_from(2).unwrap();
    let mut pkt = Subscribe::new(
        sub_pid,
        vec![
            (filter1, sub_opts),
            (filter2, sub_opts),
            (filter3, sub_opts),
        ],
    );
    pkt.properties.subscription_id = Some(VarByteInt::try_from(33).unwrap());
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(
            sub_pid,
            vec![
                SubscribeReasonCode::GrantedQoS2,
                SubscribeReasonCode::GrantedQoS2,
                SubscribeReasonCode::GrantedQoS2
            ]
        )
        .into()
    );

    for topic in ["abc/0", "xyz/0", "ijk/0"] {
        client
            .send_publish(QoS::Level0, 0, topic, "data", |_| ())
            .await;
        client
            .recv_publish(QoS::Level0, 0, topic, "data", |p| {
                p.properties.subscription_id = Some(VarByteInt::try_from(33).unwrap());
            })
            .await;
        // matched by topic filter: "#"
        client
            .recv_publish(QoS::Level0, 0, topic, "data", |_| ())
            .await;
    }

    sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_id_disabled() {
    let mut config = Config::new_allow_anonymous();
    config.subscription_id_available = false;
    let global = Arc::new(GlobalState::new(config));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let sub_opts = SubscriptionOptions::new(QoS::Level2);
    let mut pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    pkt.properties.subscription_id = Some(VarByteInt::try_from(33).unwrap());
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(
            sub_pid,
            vec![SubscribeReasonCode::SubscriptionIdentifiersNotSupported]
        )
        .into()
    );
    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_shared_subscription_disabled() {
    let mut config = Config::new_allow_anonymous();
    config.shared_subscription_available = false;
    let global = Arc::new(GlobalState::new(config));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;

    let topic_filter = TopicFilter::try_from("$share/abc/0").unwrap();
    assert!(topic_filter.is_shared());
    let sub_pid = Pid::try_from(1).unwrap();
    let sub_opts = SubscriptionOptions::new(QoS::Level2);
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(
            sub_pid,
            vec![SubscribeReasonCode::SharedSubscriptionNotSupported]
        )
        .into()
    );
    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_wildcard_subscription_disabled() {
    let mut config = Config::new_allow_anonymous();
    config.wildcard_subscription_available = false;
    let global = Arc::new(GlobalState::new(config));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;

    let topic_filter = TopicFilter::try_from("abc/+").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let sub_opts = SubscriptionOptions::new(QoS::Level2);
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(
            sub_pid,
            vec![SubscribeReasonCode::WildcardSubscriptionsNotSupported]
        )
        .into()
    );
    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_retain_disabled() {
    let mut config = Config::new_allow_anonymous();
    config.retain_available = false;
    let global = Arc::new(GlobalState::new(config));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let sub_opts = SubscriptionOptions::new(QoS::Level2);
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_shared_filter_have_no_retain() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    let topic_filter = TopicFilter::try_from("$share/group/abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let sub_opts = SubscriptionOptions::new(QoS::Level2);
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_options_no_local_retain() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let mut sub_opts = SubscriptionOptions::new(QoS::Level2);
    sub_opts.no_local = true;
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_options_no_local_publish() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let mut sub_opts = SubscriptionOptions::new(QoS::Level2);
    sub_opts.no_local = true;
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    client
        .send_publish(QoS::Level0, 0, "abc/0", "message", |_| ())
        .await;

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_options_retain_as_published_when_subscribe() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let mut sub_opts = SubscriptionOptions::new(QoS::Level2);
    sub_opts.retain_as_published = false;
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    client
        .recv_publish(QoS::Level0, 1, "abc/0", "retained message", |p| {
            p.retain = false
        })
        .await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_options_retain_as_published_when_matched() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task1, mut client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, mut client2) = MockConn::start_with_global(222, Arc::clone(&global));

    client1.connect("client1", true, false).await;
    client2.connect("client2", true, false).await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let mut sub_opts = SubscriptionOptions::new(QoS::Level2);
    sub_opts.retain_as_published = false;
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client1.write_packet(pkt.into()).await;
    assert_eq!(
        client1.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    client2
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    client1
        .recv_publish(QoS::Level0, 1, "abc/0", "retained message", |p| {
            p.retain = false
        })
        .await;

    sleep(Duration::from_millis(20)).await;
    assert!(client1.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task1.is_finished());
}

#[tokio::test]
async fn test_subscription_options_retain_send_at_subscribe() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let mut sub_opts = SubscriptionOptions::new(QoS::Level2);
    sub_opts.retain_handling = RetainHandling::SendAtSubscribe;
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    client
        .recv_publish(QoS::Level0, 1, "abc/0", "retained message", |p| {
            p.retain = true;
        })
        .await;
    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_options_retain_send_at_subscribe_if_not_exist() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let mut sub_opts = SubscriptionOptions::new(QoS::Level2);
    sub_opts.retain_handling = RetainHandling::SendAtSubscribeIfNotExist;
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter.clone(), sub_opts)]);
    client.write_packet(pkt.into()).await;

    client
        .recv_publish(QoS::Level0, 1, "abc/0", "retained message", |p| {
            p.retain = true;
        })
        .await;
    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    let sub_pid2 = Pid::try_from(2).unwrap();
    let pkt = Subscribe::new(sub_pid2, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;
    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid2, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

#[tokio::test]
async fn test_subscription_options_retain_do_not_send() {
    let global = Arc::new(GlobalState::new(Config::new_allow_anonymous()));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;
    client
        .send_publish(QoS::Level0, 0, "abc/0", "retained message", |p| {
            p.retain = true
        })
        .await;

    let topic_filter = TopicFilter::try_from("abc/0").unwrap();
    let sub_pid = Pid::try_from(1).unwrap();
    let mut sub_opts = SubscriptionOptions::new(QoS::Level2);
    sub_opts.retain_handling = RetainHandling::DoNotSend;
    let pkt = Subscribe::new(sub_pid, vec![(topic_filter, sub_opts)]);
    client.write_packet(pkt.into()).await;

    assert_eq!(
        client.read_packet().await,
        Suback::new(sub_pid, vec![SubscribeReasonCode::GrantedQoS2]).into()
    );

    sleep(Duration::from_millis(20)).await;
    assert!(client.try_read_packet_is_empty());

    sleep(Duration::from_millis(20)).await;
    assert!(!task.is_finished());
}

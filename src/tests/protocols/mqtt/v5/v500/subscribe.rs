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
    let global = Arc::new(GlobalState::new(
        "127.0.0.1:1883".parse().unwrap(),
        Config::new_allow_anonymous(),
    ));

    let (task, mut client) = MockConn::start_with_global(111, Arc::clone(&global));

    client.connect("client", true, false).await;

    let topic_filter = TopicFilter::try_from("abc/0".to_owned()).unwrap();
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

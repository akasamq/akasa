use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

use crate::config::{Config, SharedSubscriptionMode};
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::{build_publish, ClientV5};

#[tokio::test]
async fn test_shared_one_group() {
    env_logger::init();

    let mut config = Config::new_allow_anonymous();
    config.shared_subscription_mode = SharedSubscriptionMode::Random;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));

    // publisher
    let (_task0, mut client0) = MockConn::start_with_global(100, Arc::clone(&global));

    // subscriber
    let (_task1, client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, client2) = MockConn::start_with_global(222, Arc::clone(&global));
    let (_task3, client3) = MockConn::start_with_global(333, Arc::clone(&global));
    let (_task4, client4) = MockConn::start_with_global(444, Arc::clone(&global));
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();

    // Publisher connect
    client0.connect("pub", true, false).await;

    let (sync_tx, mut sync_rx) = mpsc::channel(4);
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<(usize, Packet)>(100);
    let mut tasks = Vec::new();
    for (idx, (topic, mut client, mut rx)) in [
        ("$share/group/xyz", client1, rx1),
        ("$share/group/xyz", client2, rx2),
        ("$share/group/xyz", client3, rx3),
        // will not match
        ("$share/group/def/xxx", client4, rx4),
    ]
    .into_iter()
    .enumerate()
    {
        let sync_tx = sync_tx.clone();
        let pkt_tx = pkt_tx.clone();
        let task = tokio::spawn(async move {
            let client_id = idx + 1;
            client
                .connect(format!("sub @{}", client_id), true, false)
                .await;
            let sub_topics = vec![(topic, SubscriptionOptions::new(QoS::Level0))];
            client.subscribe(2, sub_topics).await;

            // Subscribe is ready
            sync_tx.send(()).await.unwrap();

            sleep(Duration::from_millis(100)).await;

            loop {
                tokio::select! {
                    packet = client.read_packet() => {
                        match &packet {
                            Packet::Publish(_) => {},
                            pkt => panic!("invalid packet: {:?}", pkt),
                        }
                        pkt_tx.send((idx, packet)).await.unwrap();
                    }
                    _ = &mut rx => { break; },
                }
            }
            sleep(Duration::from_millis(100)).await;
            assert!(client.try_read_packet_is_empty());
        });
        tasks.push(task);
    }

    // Wait 4 subscribers
    for _ in 0..4 {
        sync_rx.recv().await.unwrap();
    }

    let mut all_bytes = HashMap::new();
    for last_byte in 0..100u8 {
        all_bytes.insert(last_byte, false);
        client0
            .send_publish(QoS::Level0, 0, "xyz", vec![3, 5, 55, last_byte], |_| ())
            .await;
    }

    let mut idx_list = [0, 0, 0, 0];
    for _ in 0..100 {
        let (idx, pkt) = pkt_rx.recv().await.unwrap();
        idx_list[idx] += 1;
        match pkt {
            Packet::Publish(publish) => {
                let data = publish.payload.as_ref();
                assert_eq!(data[0], 3);
                assert_eq!(data[1], 5);
                assert_eq!(data[2], 55);
                assert_eq!(all_bytes.insert(data[3], true), Some(false));
            }
            _ => panic!("invalid packet"),
        }
    }
    for tx in [tx1, tx2, tx3, tx4] {
        tx.send(()).unwrap();
    }
    assert_eq!(all_bytes.len(), 100);
    for exists in all_bytes.values() {
        assert!(exists);
    }
    assert!(idx_list[0] >= 5);
    assert!(idx_list[1] >= 5);
    assert!(idx_list[2] >= 5);
    assert!(idx_list[3] == 0);

    sleep(Duration::from_millis(20)).await;
    assert!(client0.try_read_packet_is_empty());

    for task in tasks {
        assert!(task.await.is_ok());
    }
}

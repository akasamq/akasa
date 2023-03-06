use std::sync::Arc;
use std::time::Duration;

use hashbrown::{HashMap, HashSet};
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

use crate::config::{Config, SharedSubscriptionMode};
use crate::state::GlobalState;
use crate::tests::utils::MockConn;

use super::super::ClientV5;

#[tokio::test]
async fn test_shared_one_group() {
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
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<(usize, Packet)>(1);
    let mut tasks = Vec::new();
    for (idx, (topic, mut client, mut rx)) in [
        ("$share/one/xyz", client1, rx1),
        ("$share/one/xyz", client2, rx2),
        ("$share/one/xyz", client3, rx3),
        // will not match
        ("$share/one/def/xxx", client4, rx4),
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

    let mut idx_list = [0, 0, 0, 0];
    for last_byte in 0..100u8 {
        let data = vec![3, 5, 55, last_byte];
        // send
        client0
            .send_publish(QoS::Level0, 0, "xyz", data.clone(), |_| ())
            .await;
        // receive
        let (idx, pkt) = pkt_rx.recv().await.unwrap();
        idx_list[idx] += 1;
        match pkt {
            Packet::Publish(publish) => assert_eq!(data, publish.payload),
            _ => panic!("invalid packet"),
        }
    }

    assert!(idx_list[0] >= 5);
    assert!(idx_list[1] >= 5);
    assert!(idx_list[2] >= 5);
    assert!(idx_list[3] == 0);

    for tx in [tx1, tx2, tx3, tx4] {
        tx.send(()).unwrap();
    }
    sleep(Duration::from_millis(20)).await;
    assert!(client0.try_read_packet_is_empty());
    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_shared_two_group() {
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
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<(usize, Packet)>(1);
    let mut tasks = Vec::new();
    for (idx, (topic, mut client, mut rx)) in [
        ("$share/one/xyz", client1, rx1),
        ("$share/one/xyz", client2, rx2),
        ("$share/two/xyz", client3, rx3),
        ("$share/two/xyz", client4, rx4),
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

    let mut idx_list = [0, 0, 0, 0];
    for last_byte in 0..100u8 {
        let data = vec![3, 5, 55, last_byte];
        // send
        client0
            .send_publish(QoS::Level0, 0, "xyz", data.clone(), |_| ())
            .await;
        // receive
        for _ in 0..2 {
            let (idx, pkt) = pkt_rx.recv().await.unwrap();
            idx_list[idx] += 1;
            match pkt {
                Packet::Publish(publish) => assert_eq!(data, publish.payload),
                _ => panic!("invalid packet"),
            }
        }
    }
    assert_eq!(idx_list[0] + idx_list[1], 100);
    assert_eq!(idx_list[2] + idx_list[3], 100);
    assert!(idx_list[0] >= 5);
    assert!(idx_list[1] >= 5);
    assert!(idx_list[2] >= 5);
    assert!(idx_list[3] >= 5);

    for tx in [tx1, tx2, tx3, tx4] {
        tx.send(()).unwrap();
    }
    sleep(Duration::from_millis(20)).await;
    assert!(client0.try_read_packet_is_empty());
    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_shared_with_normal_filter() {
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
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<(usize, Packet)>(1);
    let mut tasks = Vec::new();
    for (idx, (topic, mut client, mut rx)) in [
        ("$share/one/xyz", client1, rx1),
        ("$share/one/xyz", client2, rx2),
        ("+", client3, rx3),
        ("#", client4, rx4),
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

    let mut idx_list = [0, 0, 0, 0];
    for last_byte in 0..100u8 {
        let data = vec![3, 5, 55, last_byte];
        // send
        client0
            .send_publish(QoS::Level0, 0, "xyz", data.clone(), |_| ())
            .await;
        // receive
        for _ in 0..3 {
            let (idx, pkt) = pkt_rx.recv().await.unwrap();
            idx_list[idx] += 1;
            match pkt {
                Packet::Publish(publish) => assert_eq!(data, publish.payload),
                _ => panic!("invalid packet"),
            }
        }
    }
    assert_eq!(idx_list[0] + idx_list[1], 100);
    assert!(idx_list[0] >= 5);
    assert!(idx_list[1] >= 5);
    assert_eq!(idx_list[2], 100);
    assert_eq!(idx_list[3], 100);

    for tx in [tx1, tx2, tx3, tx4] {
        tx.send(()).unwrap();
    }
    sleep(Duration::from_millis(20)).await;
    assert!(client0.try_read_packet_is_empty());
    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_shared_wildcard_filter_one_group() {
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
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<(usize, Packet)>(1);
    let mut tasks = Vec::new();
    for (idx, (topic, mut client, mut rx)) in [
        ("$share/one/xyz", client1, rx1),
        ("$share/one/xyz", client2, rx2),
        ("$share/one/+", client3, rx3),
        ("$share/one/+", client4, rx4),
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

    let mut idx_list = [0, 0, 0, 0];
    for last_byte in 0..100u8 {
        let data = vec![3, 5, 55, last_byte];
        // send
        client0
            .send_publish(QoS::Level0, 0, "xyz", data.clone(), |_| ())
            .await;
        // receive
        for _ in 0..2 {
            let (idx, pkt) = pkt_rx.recv().await.unwrap();
            idx_list[idx] += 1;
            match pkt {
                Packet::Publish(publish) => assert_eq!(data, publish.payload),
                _ => panic!("invalid packet"),
            }
        }
    }
    assert_eq!(idx_list[0] + idx_list[1], 100);
    assert_eq!(idx_list[2] + idx_list[3], 100);
    assert!(idx_list[0] >= 5);
    assert!(idx_list[1] >= 5);
    assert!(idx_list[2] >= 5);
    assert!(idx_list[3] >= 5);

    for tx in [tx1, tx2, tx3, tx4] {
        tx.send(()).unwrap();
    }
    sleep(Duration::from_millis(20)).await;
    assert!(client0.try_read_packet_is_empty());

    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_shared_hash_topic() {
    let mut config = Config::new_allow_anonymous();
    config.shared_subscription_mode = SharedSubscriptionMode::HashTopicName;
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
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<(usize, Packet)>(1);
    let mut tasks = Vec::new();
    for (idx, (topic, mut client, mut rx)) in [
        ("$share/one/+", client1, rx1),
        ("$share/one/+", client2, rx2),
        ("$share/one/+", client3, rx3),
        ("$share/one/+", client4, rx4),
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
        sleep(Duration::from_millis(100)).await;
    }

    // Wait 4 subscribers
    for _ in 0..4 {
        sync_rx.recv().await.unwrap();
    }

    let mut topic_receiver_map: HashMap<u8, usize> = HashMap::new();
    for last_byte in 0..100u8 {
        let topic_idx = last_byte % 20;
        let topic = format!("xyz-{}", topic_idx);
        let data = vec![3, 5, 55, last_byte];
        // send
        client0
            .send_publish(QoS::Level0, 0, &topic, data.clone(), |_| ())
            .await;
        // receive
        let (idx, pkt) = pkt_rx.recv().await.unwrap();
        if let Some(prev_idx) = topic_receiver_map.get(&topic_idx) {
            assert_eq!(*prev_idx, idx);
        } else {
            topic_receiver_map.insert(topic_idx, idx);
        }
        match pkt {
            Packet::Publish(publish) => assert_eq!(data, publish.payload),
            _ => panic!("invalid packet"),
        }
    }
    assert!(topic_receiver_map.len() <= 20);

    for tx in [tx1, tx2, tx3, tx4] {
        tx.send(()).unwrap();
    }
    sleep(Duration::from_millis(20)).await;
    assert!(client0.try_read_packet_is_empty());
    for task in tasks {
        assert!(task.await.is_ok());
    }
}

#[tokio::test]
async fn test_shared_hash_client_id() {
    let mut config = Config::new_allow_anonymous();
    config.shared_subscription_mode = SharedSubscriptionMode::HashClientId;
    let global = Arc::new(GlobalState::new("127.0.0.1:1883".parse().unwrap(), config));

    // publisher
    let mut publishers = Vec::with_capacity(64);
    for i in 0..64 {
        let (_task, mut client) = MockConn::start_with_global(i, Arc::clone(&global));
        client.connect(format!("pub-{}", i), true, false).await;
        publishers.push(client);
    }

    // subscriber
    let (_task1, client1) = MockConn::start_with_global(111, Arc::clone(&global));
    let (_task2, client2) = MockConn::start_with_global(222, Arc::clone(&global));
    let (_task3, client3) = MockConn::start_with_global(333, Arc::clone(&global));
    let (_task4, client4) = MockConn::start_with_global(444, Arc::clone(&global));
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();

    let (sync_tx, mut sync_rx) = mpsc::channel(4);
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<(usize, Packet)>(1);
    let mut tasks = Vec::new();
    for (idx, (topic, mut client, mut rx)) in [
        ("$share/one/xyz", client1, rx1),
        ("$share/one/xyz", client2, rx2),
        ("$share/one/xyz", client3, rx3),
        ("$share/one/xyz", client4, rx4),
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
        sleep(Duration::from_millis(100)).await;
    }

    // Wait 4 subscribers
    for _ in 0..4 {
        sync_rx.recv().await.unwrap();
    }

    let mut receiver_idxs = HashSet::new();
    for client in &publishers {
        let mut current_idx = usize::max_value();
        for last_byte in 0..8u8 {
            let data = vec![3, 5, 55, last_byte];
            // send
            client
                .send_publish(QoS::Level0, 0, "xyz", data.clone(), |_| ())
                .await;
            // receive
            let (idx, pkt) = pkt_rx.recv().await.unwrap();
            if last_byte > 0 {
                assert_eq!(current_idx, idx);
            } else {
                receiver_idxs.insert(idx);
                current_idx = idx;
            }
            match pkt {
                Packet::Publish(publish) => assert_eq!(data, publish.payload),
                _ => panic!("invalid packet"),
            }
        }
    }
    assert!(receiver_idxs.len() >= 3);

    for tx in [tx1, tx2, tx3, tx4] {
        tx.send(()).unwrap();
    }
    sleep(Duration::from_millis(20)).await;
    for mut client in publishers {
        assert!(client.try_read_packet_is_empty());
    }
    for task in tasks {
        assert!(task.await.is_ok());
    }
}

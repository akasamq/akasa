// copy v3 tests as v5 base tests
mod base;

// MQTT v5.0 new features
mod v500;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use mqtt_proto::v5::*;
use mqtt_proto::*;
use tokio::{sync::mpsc::error::TryRecvError, time::sleep};

use crate::tests::utils::MockConnControl;

#[async_trait]
trait ClientV5 {
    fn try_read_packet(&mut self) -> Result<Packet, TryRecvError>;
    async fn read_packet(&mut self) -> Packet;
    async fn write_packet(&self, packet: Packet);

    async fn send_connect<S: ToString + Send, F: Fn(&mut Connect) + Send>(
        &self,
        client_id: S,
        update_connect: F,
    );

    async fn connect_with<S, C, A>(&mut self, client_id: S, update_connect: C, update_connack: A)
    where
        S: ToString + Send,
        C: Fn(&mut Connect) + Send,
        A: Fn(&mut Connack) + Send;

    async fn connect<S: ToString + Send>(
        &mut self,
        client_id: S,
        clean_start: bool,
        session_present: bool,
    );

    async fn disconnect_normal(&self);
    async fn disconnect(&self, code: DisconnectReasonCode);

    async fn send_publish<T, P, F>(&self, qos: QoS, pid: u16, topic: T, payload: P, update: F)
    where
        T: ToString + Send,
        P: AsRef<[u8]> + Send,
        F: Fn(&mut Publish) + Send;

    async fn recv_publish<T, P, F>(&mut self, qos: QoS, pid: u16, topic: T, payload: P, update: F)
    where
        T: ToString + Send,
        P: AsRef<[u8]> + Send,
        F: Fn(&mut Publish) + Send;

    async fn publish<T, P, F>(&mut self, qos: QoS, pid: u16, topic: T, payload: P, update: F)
    where
        T: ToString + Send,
        P: AsRef<[u8]> + Send,
        F: Fn(&mut Publish) + Send;

    async fn send_puback(&self, pid: u16);
    async fn recv_puback_success(&mut self, pid: u16);
    async fn recv_puback(&mut self, pid: u16, code: PubackReasonCode);

    async fn send_pubrec(&self, pid: u16);
    async fn recv_pubrec_success(&mut self, pid: u16);
    async fn recv_pubrec(&mut self, pid: u16, code: PubrecReasonCode);

    async fn send_pubrel(&self, pid: u16);
    async fn recv_pubrel(&mut self, pid: u16);

    async fn send_pubcomp(&self, pid: u16);
    async fn recv_pubcomp(&mut self, pid: u16);

    async fn subscribe<T: ToString + Send>(
        &mut self,
        pid: u16,
        topics: Vec<(T, SubscriptionOptions)>,
    );
    async fn send_subscribe<T: ToString + Send>(
        &self,
        pid: u16,
        topics: Vec<(T, SubscriptionOptions)>,
    );
    async fn recv_suback(&mut self, pid: u16, codes: Vec<SubscribeReasonCode>);

    async fn send_unsubscribe<T: ToString + Send>(&self, pid: u16, topics: Vec<T>);
    async fn recv_unsuback(&mut self, pid: u16, codes: Vec<UnsubscribeReasonCode>);
}

#[async_trait]
impl ClientV5 for MockConnControl {
    fn try_read_packet(&mut self) -> Result<Packet, TryRecvError> {
        self.chan_out
            .try_recv()
            .map(|data| Packet::decode(&data).unwrap().unwrap())
    }
    async fn read_packet(&mut self) -> Packet {
        let data = self.chan_out.recv().await.unwrap();
        Packet::decode(&data).unwrap().unwrap()
    }
    async fn write_packet(&self, packet: Packet) {
        self.write_data(packet.encode().unwrap().as_ref().to_vec())
            .await;
    }

    async fn send_connect<S: ToString + Send, F: Fn(&mut Connect) + Send>(
        &self,
        client_id: S,
        update_connect: F,
    ) {
        let mut connect = Connect::new(Arc::new(client_id.to_string()), 10);
        update_connect(&mut connect);
        self.write_packet(connect.into()).await;
    }

    async fn connect_with<S, C, A>(&mut self, client_id: S, update_connect: C, update_connack: A)
    where
        S: ToString + Send,
        C: Fn(&mut Connect) + Send,
        A: Fn(&mut Connack) + Send,
    {
        let mut connect = Connect::new(Arc::new(client_id.to_string()), 10);
        let mut connack = Connack::new(false, ConnectReasonCode::Success);
        update_connect(&mut connect);
        update_connack(&mut connack);
        let finished = connack.reason_code != ConnectReasonCode::Success;

        self.write_packet(connect.into()).await;
        let packet = self.read_packet().await;
        match packet {
            // TODO: handle this later
            Packet::Connack(mut inner) => {
                inner.properties = Default::default();
                assert_eq!(inner, connack);
            }
            pkt => panic!("invalid connack packet: {:?}", pkt),
        }
        sleep(Duration::from_millis(10)).await;
        if finished {
            assert_eq!(self.try_read_packet(), Err(TryRecvError::Disconnected));
        }
    }

    async fn connect<S: ToString + Send>(
        &mut self,
        client_id: S,
        clean_start: bool,
        session_present: bool,
    ) {
        let update_connect = |c: &mut Connect| {
            c.clean_start = clean_start;
            if !clean_start && c.properties.session_expiry_interval.is_none() {
                c.properties.session_expiry_interval = Some(60);
            }
        };
        self.connect_with(client_id, update_connect, |a| {
            a.session_present = session_present
        })
        .await;
    }

    async fn disconnect_normal(&self) {
        self.disconnect(DisconnectReasonCode::NormalDisconnect)
            .await;
    }
    async fn disconnect(&self, code: DisconnectReasonCode) {
        self.write_packet(Disconnect::new(code).into()).await;
    }

    async fn send_publish<T, P, F>(&self, qos: QoS, pid: u16, topic: T, payload: P, update: F)
    where
        T: ToString + Send,
        P: AsRef<[u8]> + Send,
        F: Fn(&mut Publish) + Send,
    {
        let publish = build_publish(qos, pid, topic, payload, update);
        self.write_packet(publish.into()).await;
    }

    async fn recv_publish<T, P, F>(&mut self, qos: QoS, pid: u16, topic: T, payload: P, update: F)
    where
        T: ToString + Send,
        P: AsRef<[u8]> + Send,
        F: Fn(&mut Publish) + Send,
    {
        let publish = build_publish(qos, pid, topic, payload, update);
        let packet = self.read_packet().await;
        let expected_packet = Packet::Publish(publish);
        assert_eq!(packet, expected_packet);
    }

    async fn publish<T, P, F>(&mut self, qos: QoS, pid: u16, topic: T, payload: P, update: F)
    where
        T: ToString + Send,
        P: AsRef<[u8]> + Send,
        F: Fn(&mut Publish) + Send,
    {
        self.send_publish(qos, pid, topic, payload, update).await;
        match qos {
            QoS::Level0 => {}
            QoS::Level1 => self.recv_puback_success(pid).await,
            QoS::Level2 => self.recv_pubrec_success(pid).await,
        }
    }

    async fn send_puback(&self, pid: u16) {
        let pid = Pid::try_from(pid).unwrap();
        self.write_packet(Puback::new_success(pid).into()).await;
    }
    async fn recv_puback_success(&mut self, pid: u16) {
        self.recv_puback(pid, PubackReasonCode::Success).await;
    }
    async fn recv_puback(&mut self, pid: u16, code: PubackReasonCode) {
        let pid = Pid::try_from(pid).unwrap();
        let packet = self.read_packet().await;
        let expected_packet = Puback::new(pid, code).into();
        assert_eq!(packet, expected_packet);
    }

    async fn send_pubrec(&self, pid: u16) {
        let pid = Pid::try_from(pid).unwrap();
        self.write_packet(Pubrec::new_success(pid).into()).await;
    }
    async fn recv_pubrec(&mut self, pid: u16, code: PubrecReasonCode) {
        let pid = Pid::try_from(pid).unwrap();
        let packet = self.read_packet().await;
        let expected_packet = Pubrec::new(pid, code).into();
        assert_eq!(packet, expected_packet);
    }
    async fn recv_pubrec_success(&mut self, pid: u16) {
        self.recv_pubrec(pid, PubrecReasonCode::Success).await;
    }

    async fn send_pubrel(&self, pid: u16) {
        let pid = Pid::try_from(pid).unwrap();
        self.write_packet(Pubrel::new_success(pid).into()).await;
    }
    async fn recv_pubrel(&mut self, pid: u16) {
        let pid = Pid::try_from(pid).unwrap();
        let packet = self.read_packet().await;
        let expected_packet = Pubrel::new_success(pid).into();
        assert_eq!(packet, expected_packet);
    }

    async fn send_pubcomp(&self, pid: u16) {
        let pid = Pid::try_from(pid).unwrap();
        self.write_packet(Pubcomp::new_success(pid).into()).await;
    }
    async fn recv_pubcomp(&mut self, pid: u16) {
        let pid = Pid::try_from(pid).unwrap();
        let packet = self.read_packet().await;
        let expected_packet = Pubcomp::new_success(pid).into();
        assert_eq!(packet, expected_packet);
    }

    async fn subscribe<T: ToString + Send>(
        &mut self,
        pid: u16,
        topics: Vec<(T, SubscriptionOptions)>,
    ) {
        let sub_codes = topics
            .iter()
            .map(|(_, opt)| SubscribeReasonCode::from_u8(opt.max_qos as u8).unwrap())
            .collect();
        self.send_subscribe(pid, topics).await;
        self.recv_suback(pid, sub_codes).await;
    }
    async fn send_subscribe<T: ToString + Send>(
        &self,
        pid: u16,
        topics: Vec<(T, SubscriptionOptions)>,
    ) {
        let sub_pid = Pid::try_from(pid).unwrap();
        let topics = topics
            .into_iter()
            .map(|(filter, options)| (TopicFilter::try_from(filter.to_string()).unwrap(), options))
            .collect();
        let subscribe = Subscribe::new(sub_pid, topics);
        self.write_packet(subscribe.into()).await;
    }
    async fn recv_suback(&mut self, pid: u16, codes: Vec<SubscribeReasonCode>) {
        let suback = Suback::new(Pid::try_from(pid).unwrap(), codes);
        let packet = self.read_packet().await;
        let expected_packet = Packet::Suback(suback);
        assert_eq!(packet, expected_packet);
    }

    async fn send_unsubscribe<T: ToString + Send>(&self, pid: u16, topics: Vec<T>) {
        let unsub_pid = Pid::try_from(pid).unwrap();
        let topics = topics
            .into_iter()
            .map(|filter| TopicFilter::try_from(filter.to_string()).unwrap())
            .collect();
        let unsubscribe = Unsubscribe::new(unsub_pid, topics);
        self.write_packet(unsubscribe.into()).await;
    }
    async fn recv_unsuback(&mut self, pid: u16, codes: Vec<UnsubscribeReasonCode>) {
        let unsuback = Unsuback::new(Pid::try_from(pid).unwrap(), codes);
        let packet = self.read_packet().await;
        let expected_packet = Packet::Unsuback(unsuback);
        assert_eq!(packet, expected_packet);
    }
}

fn build_publish<T, P, F>(qos: QoS, pid: u16, topic: T, payload: P, update: F) -> Publish
where
    T: ToString,
    P: AsRef<[u8]>,
    F: Fn(&mut Publish),
{
    let qos_pid = match qos {
        QoS::Level0 => QosPid::Level0,
        QoS::Level1 => QosPid::Level1(Pid::try_from(pid).unwrap()),
        QoS::Level2 => QosPid::Level2(Pid::try_from(pid).unwrap()),
    };
    let mut publish = Publish::new(
        qos_pid,
        TopicName::try_from(topic.to_string()).unwrap(),
        Bytes::from(payload.as_ref().to_vec()),
    );
    update(&mut publish);
    publish
}

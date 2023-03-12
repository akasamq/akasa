use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use flume::Receiver;
use mqtt_proto::{
    QoS, QosPid, TopicFilter, TopicName, {v3, v5},
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::protocols::mqtt::v3::{
    packet::{
        publish::handle_publish as v3_handle_publish,
        subscribe::{
            handle_subscribe as v3_handle_subscribe, handle_unsubscribe as v3_handle_unsubscribe,
        },
    },
    Session as SessionV3,
};
use crate::protocols::mqtt::v5::{
    packet::{
        publish::handle_publish as v5_handle_publish,
        subscribe::{
            handle_subscribe as v5_handle_subscribe, handle_unsubscribe as v5_handle_unsubscribe,
        },
    },
    Session as SessionV5,
};
use crate::protocols::mqtt::{OnlineSession, WritePacket};
use crate::state::{Executor, GlobalState};

#[async_trait]
pub trait Hook {
    async fn v5_before_connect(&self, connect: &v5::Connect) -> HookConnectCode;
    async fn v5_after_connect(
        &self,
        session: &SessionV5,
        session_present: bool,
    ) -> Vec<HookConnectedAction>;

    async fn v5_before_publish(
        &self,
        session: &SessionV5,
        publish: &v5::Publish,
    ) -> HookPublishCode;

    async fn v5_before_subscribe(
        &self,
        session: &SessionV5,
        subscribe: v5::Subscribe,
    ) -> v5::Subscribe;
    async fn v5_after_subscribe(
        &self,
        session: &SessionV5,
        subscribe: v5::Subscribe,
        codes: Option<Vec<v5::SubscribeReasonCode>>,
    );

    async fn v5_before_unsubscribe(&self, session: &SessionV5, unsubscribe: &v5::Unsubscribe);
    async fn v5_after_unsubscribe(&self, session: &SessionV5, unsubscribe: v5::Unsubscribe);

    async fn v3_before_connect(&self, connect: &v3::Connect) -> HookConnectCode;
    async fn v3_after_connect(
        &self,
        session: &SessionV3,
        session_present: bool,
    ) -> Vec<HookConnectedAction>;

    async fn v3_before_publish(
        &self,
        session: &SessionV3,
        publish: &v3::Publish,
    ) -> HookPublishCode;

    async fn v3_before_subscribe(
        &self,
        session: &SessionV3,
        subscribe: v3::Subscribe,
    ) -> v3::Subscribe;
    async fn v3_after_subscribe(
        &self,
        session: &SessionV3,
        subscribe: v3::Subscribe,
        codes: Option<Vec<v3::SubscribeReturnCode>>,
    );

    async fn v3_before_unsubscribe(&self, session: &SessionV3, unsubscribe: &v3::Unsubscribe);
    async fn v3_after_unsubscribe(&self, session: &SessionV3, unsubscribe: v3::Unsubscribe);
}

pub type HookResult = Result<(), Option<io::Error>>;

pub enum HookRequest {
    // Shutdown,
    V5BeforeConnect {
        peer: SocketAddr,
        connect: v5::Connect,
        sender: oneshot::Sender<io::Result<HookConnectCode>>,
    },
    V5AfterConnect {
        context: LockedHookContext<SessionV5>,
        session_present: bool,
        sender: oneshot::Sender<io::Result<Vec<HookConnectedAction>>>,
    },
    V5Publish {
        context: LockedHookContext<SessionV5>,
        encode_len: usize,
        publish: v5::Publish,
        sender: oneshot::Sender<HookResult>,
    },
    V5Subscribe {
        context: LockedHookContext<SessionV5>,
        encode_len: usize,
        subscribe: v5::Subscribe,
        sender: oneshot::Sender<HookResult>,
    },
    V5Unsubscribe {
        context: LockedHookContext<SessionV5>,
        encode_len: usize,
        unsubscribe: v5::Unsubscribe,
        sender: oneshot::Sender<HookResult>,
    },

    V3BeforeConnect {
        peer: SocketAddr,
        connect: v3::Connect,
        sender: oneshot::Sender<io::Result<HookConnectCode>>,
    },
    V3AfterConnect {
        context: LockedHookContext<SessionV3>,
        session_present: bool,
        sender: oneshot::Sender<io::Result<Vec<HookConnectedAction>>>,
    },
    V3Publish {
        context: LockedHookContext<SessionV3>,
        encode_len: usize,
        publish: v3::Publish,
        sender: oneshot::Sender<HookResult>,
    },
    V3Subscribe {
        context: LockedHookContext<SessionV3>,
        encode_len: usize,
        subscribe: v3::Subscribe,
        sender: oneshot::Sender<HookResult>,
    },
    V3Unsubscribe {
        context: LockedHookContext<SessionV3>,
        encode_len: usize,
        unsubscribe: v3::Unsubscribe,
        sender: oneshot::Sender<HookResult>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookConnectCode {
    Success,
    ClientIdentifierNotValid,
    ServerUnavailable,
    BadUserNameOrPassword,
    NotAuthorized,
}

impl HookConnectCode {
    pub fn to_v5_code(self) -> v5::ConnectReasonCode {
        match self {
            Self::Success => v5::ConnectReasonCode::Success,
            Self::ClientIdentifierNotValid => v5::ConnectReasonCode::ClientIdentifierNotValid,
            Self::ServerUnavailable => v5::ConnectReasonCode::ServerUnavailable,
            Self::BadUserNameOrPassword => v5::ConnectReasonCode::BadUserNameOrPassword,
            Self::NotAuthorized => v5::ConnectReasonCode::NotAuthorized,
        }
    }

    pub fn to_v3_code(self) -> v3::ConnectReturnCode {
        match self {
            Self::Success => v3::ConnectReturnCode::Accepted,
            Self::ClientIdentifierNotValid => v3::ConnectReturnCode::IdentifierRejected,
            Self::ServerUnavailable => v3::ConnectReturnCode::ServerUnavailable,
            Self::BadUserNameOrPassword => v3::ConnectReturnCode::BadUserNameOrPassword,
            Self::NotAuthorized => v3::ConnectReturnCode::NotAuthorized,
        }
    }
}

#[derive(Debug, Clone)]
pub enum HookConnectedAction {
    /// Publish a message
    Publish {
        retain: bool,
        qos: QoS,
        topic_name: TopicName,
        payload: Bytes,
        payload_is_utf8: Option<bool>,
        message_expiry_interval: Option<u32>,
        content_type: Option<Arc<String>>,
    },
    /// Subscribe to some topic filters (retain message will not send)
    Subscribe(Vec<(TopicFilter, QoS)>),
    /// Unsubscribe to some topic filters
    Unsubscribe(Vec<TopicFilter>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPublishCode {
    Success,
    NotAuthorized,
    TopicNameInvalid,
    QuotaExceeded,
}

impl HookPublishCode {
    pub fn to_v5_puback_code(self) -> v5::PubackReasonCode {
        match self {
            Self::Success => v5::PubackReasonCode::Success,
            Self::NotAuthorized => v5::PubackReasonCode::NotAuthorized,
            Self::TopicNameInvalid => v5::PubackReasonCode::TopicNameInvalid,
            Self::QuotaExceeded => v5::PubackReasonCode::QuotaExceeded,
        }
    }
    pub fn to_v5_pubrec_code(self) -> v5::PubrecReasonCode {
        match self {
            Self::Success => v5::PubrecReasonCode::Success,
            Self::NotAuthorized => v5::PubrecReasonCode::NotAuthorized,
            Self::TopicNameInvalid => v5::PubrecReasonCode::TopicNameInvalid,
            Self::QuotaExceeded => v5::PubrecReasonCode::QuotaExceeded,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookConfig {
    pub enable_before_connect: bool,
    pub enable_after_connect: bool,
    pub enable_publish: bool,
    pub enable_subscribe: bool,
    pub enable_unsubscribe: bool,
}

impl Default for HookConfig {
    fn default() -> HookConfig {
        HookConfig {
            enable_before_connect: true,
            enable_after_connect: true,
            enable_publish: true,
            enable_subscribe: true,
            enable_unsubscribe: true,
        }
    }
}

// NOTE: The lock is enforced by OnlineLoop::poll() function.
pub struct LockedHookContext<S: OnlineSession> {
    session: *mut S,
    write_packets: *mut VecDeque<WritePacket<S::Packet>>,
}

unsafe impl<S: OnlineSession> Send for LockedHookContext<S> {}
unsafe impl<S: OnlineSession> Sync for LockedHookContext<S> {}

impl<S: OnlineSession> LockedHookContext<S> {
    pub fn new(
        session: &mut S,
        write_packets: &mut VecDeque<WritePacket<S::Packet>>,
    ) -> LockedHookContext<S> {
        LockedHookContext {
            session: session as *mut S,
            write_packets: write_packets as *mut _,
        }
    }

    pub fn session_ref(&self) -> &S {
        unsafe { self.session.as_ref().expect("session ref ptr") }
    }

    pub fn get_mut(&mut self) -> (&mut S, &mut VecDeque<WritePacket<S::Packet>>) {
        let LockedHookContext {
            session,
            write_packets,
        } = self;
        (
            unsafe { session.as_mut().expect("session mut ptr") },
            unsafe { write_packets.as_mut().expect("write_packets mut ptr") },
        )
    }
}

#[derive(Clone)]
pub struct DefaultHook;

#[async_trait]
impl Hook for DefaultHook {
    // =========================
    // ==== MQTT v5.x hooks ====
    // =========================

    async fn v5_before_connect(&self, connect: &v5::Connect) -> HookConnectCode {
        log::debug!("v5_before_connect(), identifier={}", connect.client_id);
        HookConnectCode::Success
    }

    async fn v5_after_connect(
        &self,
        session: &SessionV5,
        session_present: bool,
    ) -> Vec<HookConnectedAction> {
        log::debug!(
            "v5_after_connect(), [{}], identifier={}, session_present={}",
            session.client_id(),
            session.client_identifier(),
            session_present
        );
        Vec::new()
    }

    async fn v5_before_publish(
        &self,
        session: &SessionV5,
        publish: &v5::Publish,
    ) -> HookPublishCode {
        log::debug!(
            "v5_before_publish() [{}], topic={}",
            session.client_id(),
            publish.topic_name
        );
        HookPublishCode::Success
    }

    async fn v5_before_subscribe(
        &self,
        session: &SessionV5,
        subscribe: v5::Subscribe,
    ) -> v5::Subscribe {
        log::debug!(
            "v5_before_subscribe() [{}], {:#?}",
            session.client_id(),
            subscribe
        );
        subscribe
    }

    async fn v5_after_subscribe(
        &self,
        session: &SessionV5,
        _subscribe: v5::Subscribe,
        _reason_codes: Option<Vec<v5::SubscribeReasonCode>>,
    ) {
        log::debug!("v5_after_subscribe(), [{}]", session.client_id());
    }

    async fn v5_before_unsubscribe(&self, session: &SessionV5, unsubscribe: &v5::Unsubscribe) {
        log::debug!(
            "v5_before_unsubscribe(), [{}], {:#?}",
            session.client_id(),
            unsubscribe
        );
    }

    async fn v5_after_unsubscribe(&self, session: &SessionV5, _unsubscribe: v5::Unsubscribe) {
        log::debug!("v5_after_unsubscribe(), [{}]", session.client_id());
    }

    // =========================
    // ==== MQTT v3.x hooks ====
    // =========================

    async fn v3_before_connect(&self, connect: &v3::Connect) -> HookConnectCode {
        log::debug!("v3_before_connect(), identifier={}", connect.client_id);
        HookConnectCode::Success
    }

    async fn v3_after_connect(
        &self,
        session: &SessionV3,
        session_present: bool,
    ) -> Vec<HookConnectedAction> {
        log::debug!(
            "v3_after_connect(), [{}], identifier={}, session_present={}",
            session.client_id(),
            session.client_identifier(),
            session_present
        );
        Vec::new()
    }

    async fn v3_before_publish(
        &self,
        session: &SessionV3,
        publish: &v3::Publish,
    ) -> HookPublishCode {
        log::debug!(
            "v3_before_publish() [{}], topic={}",
            session.client_id(),
            publish.topic_name
        );
        HookPublishCode::Success
    }

    async fn v3_before_subscribe(
        &self,
        session: &SessionV3,
        subscribe: v3::Subscribe,
    ) -> v3::Subscribe {
        log::debug!(
            "v3_before_subscribe() [{}], {:#?}",
            session.client_id(),
            subscribe
        );
        subscribe
    }

    async fn v3_after_subscribe(
        &self,
        session: &SessionV3,
        _subscribe: v3::Subscribe,
        _codes: Option<Vec<v3::SubscribeReturnCode>>,
    ) {
        log::debug!("v3_after_subscribe(), [{}]", session.client_id());
    }

    async fn v3_before_unsubscribe(&self, session: &SessionV3, unsubscribe: &v3::Unsubscribe) {
        log::debug!(
            "v3_before_unsubscribe(), [{}], {:#?}",
            session.client_id(),
            unsubscribe
        );
    }

    async fn v3_after_unsubscribe(&self, session: &SessionV3, _unsubscribe: v3::Unsubscribe) {
        log::debug!("v3_after_unsubscribe(), [{}]", session.client_id());
    }
}

#[derive(Clone)]
pub struct HookService<E: Clone, H: Clone> {
    executor: E,
    handler: H,
    requests: Receiver<HookRequest>,
    global: Arc<GlobalState>,
}

impl<E, H> HookService<E, H>
where
    E: Executor + Clone,
    H: Hook + Clone + Send + Sync + 'static,
{
    pub fn new(
        executor: E,
        handler: H,
        requests: Receiver<HookRequest>,
        global: Arc<GlobalState>,
    ) -> HookService<E, H> {
        HookService {
            executor,
            handler,
            requests,
            global,
        }
    }

    pub async fn start(self) {
        loop {
            let request = match self.requests.recv_async().await {
                Ok(request) => request,
                Err(err) => {
                    log::error!(
                        "[executor#{}] receive hook request failed, error: {:?}",
                        self.executor.id(),
                        err
                    );
                    break;
                }
            };

            let handler = self.handler.clone();
            let global = Arc::clone(&self.global);
            self.executor
                .spawn_local(handle_request(request, handler, global));
        }
    }
}

async fn handle_request<H: Hook>(request: HookRequest, handler: H, global: Arc<GlobalState>) {
    match request {
        HookRequest::V5BeforeConnect {
            peer,
            connect,
            sender,
        } => {
            log::debug!("got a v5 before connect request: {peer}, {connect:#?}");
            let code = handler.v5_before_connect(&connect).await;
            if let Err(_err) = sender.send(Ok(code)) {
                log::debug!("v5 before connect response receiver is closed");
            }
        }
        HookRequest::V5AfterConnect {
            context,
            session_present,
            sender,
        } => {
            let session = context.session_ref();
            log::debug!("got a v5 after connect request: {}", session.client_id());
            let actions = handler.v5_after_connect(session, session_present).await;
            if let Err(_err) = sender.send(Ok(actions)) {
                log::debug!("v5 after connect response receiver is closed");
            }
        }
        HookRequest::V5Publish {
            mut context,
            encode_len: _,
            publish,
            sender,
        } => {
            log::debug!("got a v5 publish request: {publish:#?}");
            let (session, write_packets) = context.get_mut();
            let code = handler.v5_before_publish(session, &publish).await;
            log::debug!("v5 before publish return code: {:?}", code);
            if let HookPublishCode::Success = code {
                match v5_handle_publish(session, publish, &global) {
                    // QoS0
                    Ok(None) => {}
                    // QoS1, QoS2
                    Ok(Some(packet)) => write_packets.push_back(packet.into()),
                    Err(err_pkt) => write_packets.push_back(err_pkt.into()),
                }
                if let Err(_err) = sender.send(Ok(())) {
                    log::error!("send publish hook ack error");
                }
            } else {
                match publish.qos_pid {
                    QosPid::Level0 => {}
                    QosPid::Level1(pid) => {
                        let pkt: v5::Packet = v5::Puback {
                            pid,
                            reason_code: code.to_v5_puback_code(),
                            properties: v5::PubackProperties::default(),
                        }
                        .into();
                        write_packets.push_back(pkt.into());
                    }
                    QosPid::Level2(pid) => {
                        let pkt: v5::Packet = v5::Pubrec {
                            pid,
                            reason_code: code.to_v5_pubrec_code(),
                            properties: v5::PubrecProperties::default(),
                        }
                        .into();
                        write_packets.push_back(pkt.into());
                    }
                }
                if let Err(_err) = sender.send(Ok(())) {
                    log::error!("send publish hook ack error");
                }
            }
        }
        HookRequest::V5Subscribe {
            mut context,
            encode_len: _,
            subscribe,
            sender,
        } => {
            let (session, write_packets) = context.get_mut();
            let subscribe = handler.v5_before_subscribe(session, subscribe).await;
            let codes = match v5_handle_subscribe(session, subscribe.clone(), &global) {
                Ok(packets) => {
                    let mut codes = Vec::new();
                    for packet in packets {
                        if let v5::Packet::Suback(suback) = &packet {
                            codes = suback.topics.clone();
                        }
                        write_packets.push_back(WritePacket::Packet(packet));
                    }
                    Some(codes)
                }
                Err(err_pkt) => {
                    write_packets.push_back(err_pkt.into());
                    None
                }
            };
            handler.v5_after_subscribe(session, subscribe, codes).await;
            if let Err(_err) = sender.send(Ok(())) {
                log::error!("send publish hook ack error");
            }
        }
        HookRequest::V5Unsubscribe {
            mut context,
            encode_len: _,
            unsubscribe,
            sender,
        } => {
            let (session, write_packets) = context.get_mut();
            handler.v5_before_unsubscribe(session, &unsubscribe).await;
            let unsuback = v5_handle_unsubscribe(session, unsubscribe.clone(), &global);
            write_packets.push_back(unsuback.into());
            handler.v5_after_unsubscribe(session, unsubscribe).await;
            if let Err(_err) = sender.send(Ok(())) {
                log::error!("send publish hook ack error");
            }
        }

        HookRequest::V3BeforeConnect {
            peer,
            connect,
            sender,
        } => {
            log::debug!("got a v3 before connect request: {peer}, {connect:#?}");
            let code = handler.v3_before_connect(&connect).await;
            if let Err(_err) = sender.send(Ok(code)) {
                log::debug!("v3 before connect response receiver is closed");
            }
        }
        HookRequest::V3AfterConnect {
            context,
            session_present,
            sender,
        } => {
            let session = context.session_ref();
            log::debug!("got a v3 after connect request: {}", session.client_id());
            let actions = handler.v3_after_connect(session, session_present).await;
            if let Err(_err) = sender.send(Ok(actions)) {
                log::debug!("v3 after connect response receiver is closed");
            }
        }
        HookRequest::V3Publish {
            mut context,
            encode_len: _,
            publish,
            sender,
        } => {
            log::debug!("got a v3 publish request: {publish:#?}");
            let (session, write_packets) = context.get_mut();
            let code = handler.v3_before_publish(session, &publish).await;
            log::debug!("v3 before publish return code: {:?}", code);
            if let HookPublishCode::Success = code {
                match v3_handle_publish(session, publish, &global) {
                    Ok(packet_opt) => {
                        if let Some(packet) = packet_opt {
                            write_packets.push_back(packet.into());
                        }
                        if let Err(_err) = sender.send(Ok(())) {
                            log::error!("send publish hook ack error");
                        }
                    }
                    Err(err) => {
                        if let Err(_err) = sender.send(Err(Some(err))) {
                            log::error!("send publish hook ack error");
                        }
                    }
                }
            } else if let Err(_err) = sender.send(Err(Some(io::ErrorKind::InvalidData.into()))) {
                log::error!("send publish hook ack error");
            }
        }
        HookRequest::V3Subscribe {
            mut context,
            encode_len: _,
            subscribe,
            sender,
        } => {
            let (session, write_packets) = context.get_mut();
            let subscribe = handler.v3_before_subscribe(session, subscribe).await;
            match v3_handle_subscribe(session, subscribe.clone(), &global) {
                Ok(packets) => {
                    let mut codes = Vec::new();
                    for packet in packets {
                        if let v3::Packet::Suback(suback) = &packet {
                            codes = suback.topics.clone();
                        }
                        write_packets.push_back(WritePacket::Packet(packet));
                    }
                    handler
                        .v3_after_subscribe(session, subscribe, Some(codes))
                        .await;
                    if let Err(_err) = sender.send(Ok(())) {
                        log::error!("send publish hook ack error");
                    }
                }
                Err(err) => {
                    handler.v3_after_subscribe(session, subscribe, None).await;
                    if let Err(_err) = sender.send(Err(Some(err))) {
                        log::error!("send publish hook ack error");
                    }
                }
            }
        }
        HookRequest::V3Unsubscribe {
            mut context,
            encode_len: _,
            unsubscribe,
            sender,
        } => {
            let (session, write_packets) = context.get_mut();
            handler.v3_before_unsubscribe(session, &unsubscribe).await;
            let unsuback = v3_handle_unsubscribe(session, unsubscribe.clone(), &global);
            write_packets.push_back(unsuback.into());
            handler.v3_after_unsubscribe(session, unsubscribe).await;
            if let Err(_err) = sender.send(Ok(())) {
                log::error!("send publish hook ack error");
            }
        }
    }
}

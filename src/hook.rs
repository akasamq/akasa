use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use flume::Receiver;
use mqtt_proto::{
    QoS, QosPid, TopicFilter, {v3, v5},
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::protocols::mqtt::{
    OnlineSession, WritePacket,
    {
        v3::Session as SessionV3,
        v5::{packet::publish::handle_publish as v5_handle_publish, Session as SessionV5},
    },
};
use crate::state::{Executor, GlobalState};

#[async_trait]
pub trait Hook {
    async fn v5_before_connect(&self, connect: &v5::Connect) -> V5ConnectCode;
    async fn v5_before_publish(&self, session: &SessionV5, publish: &v5::Publish) -> V5PublishCode;
    async fn v5_before_subscribe(&self, session: &SessionV5, subscribe: &v5::Subscribe);
    async fn v5_before_unsubscribe(&self, session: &SessionV5, unsubscribe: &v5::Unsubscribe);

    async fn v5_after_connect(
        &self,
        session: &SessionV5,
        session_present: bool,
    ) -> Vec<V5ConnectedAction>;
    async fn v5_after_subscribe(&self, session: &SessionV5, subscribe: &v5::Subscribe);
    async fn v5_after_unsubscribe(&self, session: &SessionV5, unsubscribe: &v5::Unsubscribe);
}

pub type HookResult = Result<(), Option<io::Error>>;

pub enum HookRequest {
    Shutdown,
    V5BeforeConnect {
        peer: SocketAddr,
        connect: v5::Connect,
        sender: oneshot::Sender<io::Result<V5ConnectCode>>,
    },
    V5AfterConnect {
        context: LockedHookContext<SessionV5>,
        session_present: bool,
        sender: oneshot::Sender<io::Result<Vec<V5ConnectedAction>>>,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V5ConnectCode {
    Accepted,
    IdentifierRejected,
    ServerUnavailable,
    BadUserNameOrPassword,
    NotAuthorized,
}

impl V5ConnectCode {
    pub fn to_reason_code(self) -> v5::ConnectReasonCode {
        match self {
            Self::Accepted => v5::ConnectReasonCode::Success,
            Self::IdentifierRejected => v5::ConnectReasonCode::ClientIdentifierNotValid,
            Self::ServerUnavailable => v5::ConnectReasonCode::ServerUnavailable,
            Self::BadUserNameOrPassword => v5::ConnectReasonCode::BadUserNameOrPassword,
            Self::NotAuthorized => v5::ConnectReasonCode::NotAuthorized,
        }
    }
}

#[derive(Debug, Clone)]
pub enum V5ConnectedAction {
    /// Publish a message
    Publish(v5::Publish),
    /// Subscribe to some topic filters (may cause receive retain messages)
    Subscribe(Vec<(TopicFilter, v5::SubscriptionOptions)>),
    /// Unsubscribe to some topic filters
    Unsubscribe(Vec<TopicFilter>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V5PublishCode {
    Accepted,
    NotAuthorized,
    TopicNameInvalid,
    QuotaExceeded,
}
impl V5PublishCode {
    pub fn to_puback_reason_code(self) -> v5::PubackReasonCode {
        match self {
            Self::Accepted => v5::PubackReasonCode::Success,
            Self::NotAuthorized => v5::PubackReasonCode::NotAuthorized,
            Self::TopicNameInvalid => v5::PubackReasonCode::TopicNameInvalid,
            Self::QuotaExceeded => v5::PubackReasonCode::QuotaExceeded,
        }
    }
    pub fn to_pubrec_reason_code(self) -> v5::PubrecReasonCode {
        match self {
            Self::Accepted => v5::PubrecReasonCode::Success,
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

    pub fn session_mut(&mut self) -> &mut S {
        unsafe { self.session.as_mut().expect("session mut ptr") }
    }

    pub fn write_packets_mut(&mut self) -> &mut VecDeque<WritePacket<S::Packet>> {
        unsafe { self.write_packets.as_mut().expect("write_packets mut ptr") }
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
pub struct DefaultHook {
    global: Arc<GlobalState>,
}

impl DefaultHook {
    pub fn new(global: Arc<GlobalState>) -> DefaultHook {
        DefaultHook { global }
    }
}

#[async_trait]
impl Hook for DefaultHook {
    async fn v5_before_connect(&self, connect: &v5::Connect) -> V5ConnectCode {
        log::debug!("v5_before_connect()");
        V5ConnectCode::Accepted
    }

    async fn v5_before_publish(&self, session: &SessionV5, publish: &v5::Publish) -> V5PublishCode {
        log::debug!("v5_before_publish()");
        V5PublishCode::Accepted
    }

    async fn v5_before_subscribe(&self, session: &SessionV5, subscribe: &v5::Subscribe) {
        log::debug!("v5_before_subscribe()");
    }

    async fn v5_before_unsubscribe(&self, session: &SessionV5, unsubscribe: &v5::Unsubscribe) {
        log::debug!("v5_before_unsubscribe()");
    }

    async fn v5_after_connect(
        &self,
        session: &SessionV5,
        session_present: bool,
    ) -> Vec<V5ConnectedAction> {
        log::debug!("v5_after_connect({})", session.client_id());
        Vec::new()
    }

    async fn v5_after_subscribe(&self, session: &SessionV5, subscribe: &v5::Subscribe) {
        log::debug!("v5_after_subscribe()");
    }

    async fn v5_after_unsubscribe(&self, session: &SessionV5, unsubscribe: &v5::Unsubscribe) {
        log::debug!("v5_after_unsubscribe()");
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
                    log::info!(
                        "[executor#{}] receive hook request failed, error: {:?}",
                        self.executor.id(),
                        err
                    );
                    break;
                }
            };

            if let &HookRequest::Shutdown = &request {
                break;
            }
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
            if let Err(_err) = sender.send(Ok(V5ConnectCode::Accepted)) {
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
            let code = handler.v5_after_connect(session, session_present).await;
            if let Err(_err) = sender.send(Ok(Vec::new())) {
                log::debug!("v5 after connect response receiver is closed");
            }
        }
        HookRequest::V5Publish {
            mut context,
            encode_len,
            publish,
            sender,
        } => {
            log::debug!("got a v5 publish request: {publish:#?}");
            let code = handler
                .v5_before_publish(context.session_ref(), &publish)
                .await;
            if let V5PublishCode::Accepted = code {
                let (session, write_packets) = context.get_mut();
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
                            reason_code: code.to_puback_reason_code(),
                            properties: v5::PubackProperties::default(),
                        }
                        .into();
                        context.write_packets_mut().push_back(pkt.into());
                    }
                    QosPid::Level2(pid) => {
                        let pkt: v5::Packet = v5::Pubrec {
                            pid,
                            reason_code: code.to_pubrec_reason_code(),
                            properties: v5::PubrecProperties::default(),
                        }
                        .into();
                        context.write_packets_mut().push_back(pkt.into());
                    }
                }
                if let Err(_err) = sender.send(Ok(())) {
                    log::error!("send publish hook ack error");
                }
            }
        }
        HookRequest::V5Subscribe { sender, .. } => {
            // FIXME: impl
            if let Err(_err) = sender.send(Ok(())) {
                log::error!("send publish hook ack error");
            }
        }
        HookRequest::V5Unsubscribe { sender, .. } => {
            // FIXME: impl
            if let Err(_err) = sender.send(Ok(())) {
                log::error!("send publish hook ack error");
            }
        }
        HookRequest::Shutdown => unreachable!(),
    }
}

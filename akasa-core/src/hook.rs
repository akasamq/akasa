use std::collections::VecDeque;
use std::future::{self, Future};
use std::io;
use std::mem::{self, MaybeUninit};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use mqtt_proto::{
    QoS, QosPid, TopicFilter, TopicName, {v3, v5},
};
use thiserror::Error;

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
use crate::state::GlobalState;

// TODO:
//  [ ] add timer support
//  [ ] handle mqtt v5.0 scram auth
//  [ ] handle disconnect event (takenover, by_server, by_client)

pub trait Hook {
    fn v5_before_connect(
        &self,
        _peer: SocketAddr,
        _connect: &v5::Connect,
    ) -> impl Future<Output = HookResult<HookConnectCode>> + Send {
        future::ready(Ok(HookConnectCode::Success))
    }

    fn v5_after_connect(
        &self,
        _session: &SessionV5,
        _session_present: bool,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }

    fn v5_before_publish(
        &self,
        _session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _publish: &mut v5::Publish,
        _changed: &mut bool,
    ) -> impl Future<Output = HookResult<HookPublishCode>> + Send {
        future::ready(Ok(HookPublishCode::Success))
    }

    fn v5_after_publish(
        &self,
        _session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _publish: &v5::Publish,
        _changed: bool,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }

    fn v5_before_subscribe(
        &self,
        _session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &mut v5::Subscribe,
        _changed: &mut bool,
    ) -> impl Future<Output = HookResult<HookSubscribeCode>> + Send {
        future::ready(Ok(HookSubscribeCode::Success))
    }

    fn v5_after_subscribe(
        &self,
        _session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &v5::Subscribe,
        _changed: bool,
        _codes: Option<Vec<v5::SubscribeReasonCode>>,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }

    fn v5_before_unsubscribe(
        &self,
        _session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &mut v5::Unsubscribe,
        _changed: &mut bool,
    ) -> impl Future<Output = HookResult<HookUnsubscribeCode>> + Send {
        future::ready(Ok(HookUnsubscribeCode::Success))
    }

    fn v5_after_unsubscribe(
        &self,
        _session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &v5::Unsubscribe,
        _changed: bool,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }

    fn v3_before_connect(
        &self,
        _peer: SocketAddr,
        _connect: &v3::Connect,
    ) -> impl Future<Output = HookResult<HookConnectCode>> + Send {
        future::ready(Ok(HookConnectCode::Success))
    }

    fn v3_after_connect(
        &self,
        _session: &SessionV3,
        _session_present: bool,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }

    fn v3_before_publish(
        &self,
        _session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _publish: &mut v3::Publish,
        _changed: &mut bool,
    ) -> impl Future<Output = HookResult<HookPublishCode>> + Send {
        future::ready(Ok(HookPublishCode::Success))
    }

    fn v3_after_publish(
        &self,
        _session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _publish: &v3::Publish,
        _changed: bool,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }

    fn v3_before_subscribe(
        &self,
        _session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &mut v3::Subscribe,
        _changed: &mut bool,
    ) -> impl Future<Output = HookResult<HookSubscribeCode>> + Send {
        future::ready(Ok(HookSubscribeCode::Success))
    }

    fn v3_after_subscribe(
        &self,
        _session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &v3::Subscribe,
        _changed: bool,
        _codes: Option<Vec<v3::SubscribeReturnCode>>,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }

    fn v3_before_unsubscribe(
        &self,
        _session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &mut v3::Unsubscribe,
        _changed: &mut bool,
    ) -> impl Future<Output = HookResult<HookUnsubscribeCode>> + Send {
        future::ready(Ok(HookUnsubscribeCode::Success))
    }

    fn v3_after_unsubscribe(
        &self,
        _session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &v3::Unsubscribe,
        _changed: bool,
    ) -> impl Future<Output = HookResult<Vec<HookAction>>> + Send {
        future::ready(Ok(Vec::new()))
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum HookError {
    #[error("internal error")]
    Internal,
}

impl From<HookError> for io::Error {
    fn from(_err: HookError) -> io::Error {
        io::ErrorKind::BrokenPipe.into()
    }
}

pub type HookResult<T> = Result<T, HookError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookConnectCode {
    Success,
    UnspecifiedError,
    ClientIdentifierNotValid,
    BadUserNameOrPassword,
    NotAuthorized,
    ServerUnavailable,
    QuotaExceeded,
    ConnectionRateExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPublishCode {
    Success,
    UnspecifiedError,
    NotAuthorized,
    TopicNameInvalid,
    QuotaExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookSubscribeCode {
    Success,
    UnspecifiedError,
    NotAuthorized,
    TopicFilterInvalid,
    QuotaExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookUnsubscribeCode {
    Success,
    UnspecifiedError,
    NotAuthorized,
    TopicFilterInvalid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum HookAction {
    Publish(PublishAction),
    Subscribe(SubscribeAction),
    Unsubscribe(UnsubscribeAction),
}

/// Publish a message
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishAction {
    pub retain: bool,
    pub qos: QoS,
    pub topic_name: TopicName,
    pub payload: Bytes,
    pub payload_is_utf8: Option<bool>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<Arc<String>>,
}

/// Subscribe to some topic filters (retain message will not send)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubscribeAction(pub Vec<(TopicFilter, QoS)>);

/// Unsubscribe to some topic filters
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UnsubscribeAction(pub Vec<TopicFilter>);

impl HookConnectCode {
    pub fn to_v5_code(self) -> v5::ConnectReasonCode {
        match self {
            Self::Success => v5::ConnectReasonCode::Success,
            Self::UnspecifiedError => v5::ConnectReasonCode::UnspecifiedError,
            Self::ClientIdentifierNotValid => v5::ConnectReasonCode::ClientIdentifierNotValid,
            Self::BadUserNameOrPassword => v5::ConnectReasonCode::BadUserNameOrPassword,
            Self::NotAuthorized => v5::ConnectReasonCode::NotAuthorized,
            Self::ServerUnavailable => v5::ConnectReasonCode::ServerUnavailable,
            Self::QuotaExceeded => v5::ConnectReasonCode::QuotaExceeded,
            Self::ConnectionRateExceeded => v5::ConnectReasonCode::ConnectionRateExceeded,
        }
    }

    pub fn to_v3_code(self) -> v3::ConnectReturnCode {
        match self {
            Self::Success => v3::ConnectReturnCode::Accepted,
            Self::ClientIdentifierNotValid => v3::ConnectReturnCode::IdentifierRejected,
            Self::ServerUnavailable => v3::ConnectReturnCode::ServerUnavailable,
            Self::BadUserNameOrPassword => v3::ConnectReturnCode::BadUserNameOrPassword,
            Self::NotAuthorized => v3::ConnectReturnCode::NotAuthorized,
            _ => v3::ConnectReturnCode::ServerUnavailable,
        }
    }
}

impl HookPublishCode {
    pub fn to_v5_puback_code(self) -> v5::PubackReasonCode {
        match self {
            Self::Success => v5::PubackReasonCode::Success,
            Self::UnspecifiedError => v5::PubackReasonCode::UnspecifiedError,
            Self::NotAuthorized => v5::PubackReasonCode::NotAuthorized,
            Self::TopicNameInvalid => v5::PubackReasonCode::TopicNameInvalid,
            Self::QuotaExceeded => v5::PubackReasonCode::QuotaExceeded,
        }
    }
    pub fn to_v5_pubrec_code(self) -> v5::PubrecReasonCode {
        match self {
            Self::Success => v5::PubrecReasonCode::Success,
            Self::UnspecifiedError => v5::PubrecReasonCode::UnspecifiedError,
            Self::NotAuthorized => v5::PubrecReasonCode::NotAuthorized,
            Self::TopicNameInvalid => v5::PubrecReasonCode::TopicNameInvalid,
            Self::QuotaExceeded => v5::PubrecReasonCode::QuotaExceeded,
        }
    }
}

impl HookSubscribeCode {
    pub fn to_v5_code(self) -> v5::SubscribeReasonCode {
        match self {
            Self::Success => v5::SubscribeReasonCode::GrantedQoS2,
            Self::UnspecifiedError => v5::SubscribeReasonCode::UnspecifiedError,
            Self::NotAuthorized => v5::SubscribeReasonCode::NotAuthorized,
            Self::TopicFilterInvalid => v5::SubscribeReasonCode::TopicFilterInvalid,
            Self::QuotaExceeded => v5::SubscribeReasonCode::QuotaExceeded,
        }
    }
}

impl HookUnsubscribeCode {
    pub fn to_v5_code(self) -> v5::UnsubscribeReasonCode {
        match self {
            Self::Success => v5::UnsubscribeReasonCode::Success,
            Self::UnspecifiedError => v5::UnsubscribeReasonCode::UnspecifiedError,
            Self::NotAuthorized => v5::UnsubscribeReasonCode::NotAuthorized,
            Self::TopicFilterInvalid => v5::UnsubscribeReasonCode::TopicFilterInvalid,
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

pub enum HookResponse {
    Normal(Result<Vec<HookAction>, Option<io::Error>>),
    BeforeConnect(io::Result<HookConnectCode>),
    AfterConnect(io::Result<Vec<HookAction>>),
}

pub enum HookRequest {
    // Shutdown,
    V5BeforeConnect {
        peer: SocketAddr,
        connect: v5::Connect,
    },
    V5AfterConnect {
        context: LockedHookContext<SessionV5>,
        session_present: bool,
    },
    V5Publish {
        context: LockedHookContext<SessionV5>,
        encode_len: usize,
        packet_body: Vec<MaybeUninit<u8>>,
        publish: v5::Publish,
    },
    V5Subscribe {
        context: LockedHookContext<SessionV5>,
        encode_len: usize,
        packet_body: Vec<MaybeUninit<u8>>,
        subscribe: v5::Subscribe,
    },
    V5Unsubscribe {
        context: LockedHookContext<SessionV5>,
        encode_len: usize,
        packet_body: Vec<MaybeUninit<u8>>,
        unsubscribe: v5::Unsubscribe,
    },

    V3BeforeConnect {
        peer: SocketAddr,
        connect: v3::Connect,
    },
    V3AfterConnect {
        context: LockedHookContext<SessionV3>,
        session_present: bool,
    },
    V3Publish {
        context: LockedHookContext<SessionV3>,
        encode_len: usize,
        packet_body: Vec<MaybeUninit<u8>>,
        publish: v3::Publish,
    },
    V3Subscribe {
        context: LockedHookContext<SessionV3>,
        encode_len: usize,
        packet_body: Vec<MaybeUninit<u8>>,
        subscribe: v3::Subscribe,
    },
    V3Unsubscribe {
        context: LockedHookContext<SessionV3>,
        encode_len: usize,
        packet_body: Vec<MaybeUninit<u8>>,
        unsubscribe: v3::Unsubscribe,
    },
}

pub async fn handle_request<H: Hook + Send + Sync>(
    request: HookRequest,
    handler: H,
    global: Arc<GlobalState>,
) -> HookResponse {
    match request {
        HookRequest::V5BeforeConnect { peer, connect } => {
            log::debug!("got a v5 before connect request: {peer}, {connect:#?}");
            let result = handler
                .v5_before_connect(peer, &connect)
                .await
                .map_err(Into::into);
            HookResponse::BeforeConnect(result)
        }
        HookRequest::V5AfterConnect {
            context,
            session_present,
        } => {
            let session = context.session_ref();
            log::debug!("got a v5 after connect request: {}", session.client_id());
            let result = handler
                .v5_after_connect(session, session_present)
                .await
                .map_err(Into::into);
            HookResponse::AfterConnect(result)
        }
        HookRequest::V5Publish {
            mut context,
            encode_len,
            packet_body,
            mut publish,
        } => {
            log::debug!("got a v5 publish request: {publish:#?}");
            let (session, write_packets) = context.get_mut();

            let body: &[u8] = unsafe { mem::transmute(&packet_body[..]) };
            let mut changed = false;
            let result = handler
                .v5_before_publish(session, encode_len, body, &mut publish, &mut changed)
                .await;
            log::debug!("v5 before publish return code: {:?}", result);
            let receipt = match result {
                Ok(HookPublishCode::Success) => {
                    match v5_handle_publish(session, publish.clone(), &global) {
                        Ok(packet_opt) => {
                            if let Some(packet) = packet_opt {
                                write_packets.push_back(packet.into());
                            }
                            handler
                                .v5_after_publish(session, encode_len, body, &publish, changed)
                                .await
                                .map_err(|err| Some(err.into()))
                        }
                        Err(err_pkt) => {
                            write_packets.push_back(err_pkt.into());
                            Ok(Vec::new())
                        }
                    }
                }
                Ok(code) => {
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
                    Ok(Vec::new())
                }
                Err(err) => Err(Some(err.into())),
            };
            HookResponse::Normal(receipt)
        }
        HookRequest::V5Subscribe {
            mut context,
            encode_len,
            packet_body,
            mut subscribe,
        } => {
            let (session, write_packets) = context.get_mut();
            let body: &[u8] = unsafe { mem::transmute(&packet_body[..]) };
            let mut changed = false;
            let result = handler
                .v5_before_subscribe(session, encode_len, body, &mut subscribe, &mut changed)
                .await;
            let receipt = match result {
                Ok(HookSubscribeCode::Success) => {
                    let codes = match v5_handle_subscribe(session, &subscribe, &global) {
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
                    handler
                        .v5_after_subscribe(session, encode_len, body, &subscribe, changed, codes)
                        .await
                        .map_err(|err| Some(err.into()))
                }
                Ok(code) => {
                    let reason_code = code.to_v5_code();
                    let topics = vec![reason_code; subscribe.topics.len()];
                    let pkt: v5::Packet = v5::Suback::new(subscribe.pid, topics).into();
                    write_packets.push_back(pkt.into());
                    Ok(Vec::new())
                }
                Err(err) => Err(Some(err.into())),
            };
            HookResponse::Normal(receipt)
        }
        HookRequest::V5Unsubscribe {
            mut context,
            encode_len,
            packet_body,
            mut unsubscribe,
        } => {
            let (session, write_packets) = context.get_mut();
            let body: &[u8] = unsafe { mem::transmute(&packet_body[..]) };
            let mut changed = false;
            let result = handler
                .v5_before_unsubscribe(session, encode_len, body, &mut unsubscribe, &mut changed)
                .await;
            let receipt = match result {
                Ok(HookUnsubscribeCode::Success) => {
                    let unsuback = v5_handle_unsubscribe(session, &unsubscribe, &global);
                    write_packets.push_back(unsuback.into());
                    handler
                        .v5_after_unsubscribe(session, encode_len, body, &unsubscribe, changed)
                        .await
                        .map_err(|err| Some(err.into()))
                }
                Ok(code) => {
                    let reason_code = code.to_v5_code();
                    let topics = vec![reason_code; unsubscribe.topics.len()];
                    let pkt: v5::Packet = v5::Unsuback::new(unsubscribe.pid, topics).into();
                    write_packets.push_back(pkt.into());
                    Ok(Vec::new())
                }
                Err(err) => Err(Some(err.into())),
            };
            HookResponse::Normal(receipt)
        }

        HookRequest::V3BeforeConnect { peer, connect } => {
            log::debug!("got a v3 before connect request: {peer}, {connect:#?}");
            let result = handler
                .v3_before_connect(peer, &connect)
                .await
                .map_err(Into::into);
            HookResponse::BeforeConnect(result)
        }
        HookRequest::V3AfterConnect {
            context,
            session_present,
        } => {
            let session = context.session_ref();
            log::debug!("got a v3 after connect request: {}", session.client_id());
            let result = handler
                .v3_after_connect(session, session_present)
                .await
                .map_err(Into::into);
            HookResponse::AfterConnect(result)
        }
        HookRequest::V3Publish {
            mut context,
            encode_len,
            packet_body,
            mut publish,
        } => {
            log::debug!("got a v3 publish request: {publish:#?}");
            let (session, write_packets) = context.get_mut();
            let body: &[u8] = unsafe { mem::transmute(&packet_body[..]) };
            let mut changed = false;
            let result = handler
                .v3_before_publish(session, encode_len, body, &mut publish, &mut changed)
                .await;
            log::debug!("v3 before publish return code: {:?}", result);
            let receipt = match result {
                Ok(HookPublishCode::Success) => {
                    match v3_handle_publish(session, publish.clone(), &global) {
                        Ok(packet_opt) => {
                            if let Some(packet) = packet_opt {
                                write_packets.push_back(packet.into());
                            }
                            handler
                                .v3_after_publish(session, encode_len, body, &publish, changed)
                                .await
                                .map_err(|err| Some(err.into()))
                        }
                        Err(err) => Err(Some(err)),
                    }
                }
                // TODO: return error or just ignore the packet?
                Ok(_) => Err(Some(io::ErrorKind::InvalidData.into())),
                Err(err) => Err(Some(err.into())),
            };
            HookResponse::Normal(receipt)
        }
        HookRequest::V3Subscribe {
            mut context,
            encode_len,
            packet_body,
            mut subscribe,
        } => {
            let (session, write_packets) = context.get_mut();
            let body: &[u8] = unsafe { mem::transmute(&packet_body[..]) };
            let mut changed = false;
            let result = handler
                .v3_before_subscribe(session, encode_len, body, &mut subscribe, &mut changed)
                .await;
            let receipt = match result {
                Ok(HookSubscribeCode::Success) => {
                    match v3_handle_subscribe(session, &subscribe, &global) {
                        Ok(packets) => {
                            let mut codes = Vec::new();
                            for packet in packets {
                                if let v3::Packet::Suback(suback) = &packet {
                                    codes = suback.topics.clone();
                                }
                                write_packets.push_back(WritePacket::Packet(packet));
                            }
                            handler
                                .v3_after_subscribe(
                                    session,
                                    encode_len,
                                    body,
                                    &subscribe,
                                    changed,
                                    Some(codes),
                                )
                                .await
                                .map_err(|err| Some(err.into()))
                        }
                        Err(err) => {
                            let _result = handler
                                .v3_after_subscribe(
                                    session, encode_len, body, &subscribe, changed, None,
                                )
                                .await;
                            Err(Some(err))
                        }
                    }
                }
                // TODO: return error or just ignore the packet?
                Ok(_code) => Err(Some(io::ErrorKind::InvalidData.into())),
                Err(err) => Err(Some(err.into())),
            };
            HookResponse::Normal(receipt)
        }
        HookRequest::V3Unsubscribe {
            mut context,
            encode_len,
            packet_body,
            mut unsubscribe,
        } => {
            let (session, write_packets) = context.get_mut();
            let body: &[u8] = unsafe { mem::transmute(&packet_body[..]) };
            let mut changed = false;
            let result = handler
                .v3_before_unsubscribe(session, encode_len, body, &mut unsubscribe, &mut changed)
                .await;
            let receipt = match result {
                Ok(HookUnsubscribeCode::Success) => {
                    let unsuback = v3_handle_unsubscribe(session, &unsubscribe, &global);
                    write_packets.push_back(unsuback.into());
                    handler
                        .v3_after_unsubscribe(session, encode_len, body, &unsubscribe, changed)
                        .await
                        .map_err(|err| Some(err.into()))
                }
                // TODO: return error or just ignore the packet?
                Ok(_code) => Err(Some(io::ErrorKind::InvalidData.into())),
                Err(err) => Err(Some(err.into())),
            };
            HookResponse::Normal(receipt)
        }
    }
}

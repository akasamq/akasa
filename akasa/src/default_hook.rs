use std::net::SocketAddr;

use akasa_core::mqtt_proto::{v3, v5};
use akasa_core::{
    Hook, HookAction, HookConnectCode, HookPublishCode, HookResult, HookSubscribeCode,
    HookUnsubscribeCode, SessionV3, SessionV5,
};

#[derive(Clone)]
pub struct DefaultHook;

impl Hook for DefaultHook {
    // =========================
    // ==== MQTT v5.x hooks ====
    // =========================

    async fn v5_before_connect(
        &self,
        _peer: SocketAddr,
        connect: &v5::Connect,
    ) -> HookResult<HookConnectCode> {
        log::debug!("v5_before_connect(), identifier={}", connect.client_id);
        Ok(HookConnectCode::Success)
    }

    async fn v5_after_connect(
        &self,
        session: &SessionV5,
        session_present: bool,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!(
            "v5_after_connect(), [{}], identifier={}, session_present={}",
            session.client_id(),
            session.client_identifier,
            session_present
        );
        Ok(Vec::new())
    }

    async fn v5_before_publish(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        publish: &mut v5::Publish,
        _changed: &mut bool,
    ) -> HookResult<HookPublishCode> {
        log::debug!(
            "v5_before_publish() [{}], topic={}",
            session.client_id(),
            publish.topic_name
        );
        Ok(HookPublishCode::Success)
    }

    async fn v5_after_publish(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        publish: &v5::Publish,
        _changed: bool,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!(
            "v5_after_publish() [{}], topic={}",
            session.client_id(),
            publish.topic_name
        );
        Ok(Vec::new())
    }

    async fn v5_before_subscribe(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        subscribe: &mut v5::Subscribe,
        _changed: &mut bool,
    ) -> HookResult<HookSubscribeCode> {
        log::debug!(
            "v5_before_subscribe() [{}], {:#?}",
            session.client_id(),
            subscribe
        );
        Ok(HookSubscribeCode::Success)
    }

    async fn v5_after_subscribe(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &v5::Subscribe,
        _changed: bool,
        _reason_codes: Option<Vec<v5::SubscribeReasonCode>>,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!("v5_after_subscribe(), [{}]", session.client_id());
        Ok(Vec::new())
    }

    async fn v5_before_unsubscribe(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        unsubscribe: &mut v5::Unsubscribe,
        _changed: &mut bool,
    ) -> HookResult<HookUnsubscribeCode> {
        log::debug!(
            "v5_before_unsubscribe(), [{}], {:#?}",
            session.client_id(),
            unsubscribe
        );
        Ok(HookUnsubscribeCode::Success)
    }

    async fn v5_after_unsubscribe(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &v5::Unsubscribe,
        _changed: bool,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!("v5_after_unsubscribe(), [{}]", session.client_id());
        Ok(Vec::new())
    }

    // =========================
    // ==== MQTT v3.x hooks ====
    // =========================

    async fn v3_before_connect(
        &self,
        _peer: SocketAddr,
        connect: &v3::Connect,
    ) -> HookResult<HookConnectCode> {
        log::debug!("v3_before_connect(), identifier={}", connect.client_id);
        Ok(HookConnectCode::Success)
    }

    async fn v3_after_connect(
        &self,
        session: &SessionV3,
        session_present: bool,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!(
            "v3_after_connect(), [{}], identifier={}, session_present={}",
            session.client_id(),
            session.client_identifier,
            session_present
        );
        Ok(Vec::new())
    }

    async fn v3_before_publish(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        publish: &mut v3::Publish,
        _changed: &mut bool,
    ) -> HookResult<HookPublishCode> {
        log::debug!(
            "v3_before_publish() [{}], topic={}",
            session.client_id(),
            publish.topic_name
        );
        Ok(HookPublishCode::Success)
    }

    async fn v3_after_publish(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        publish: &v3::Publish,
        _changed: bool,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!(
            "v3_after_publish() [{}], topic={}",
            session.client_id(),
            publish.topic_name
        );
        Ok(Vec::new())
    }

    async fn v3_before_subscribe(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        subscribe: &mut v3::Subscribe,
        _changed: &mut bool,
    ) -> HookResult<HookSubscribeCode> {
        log::debug!(
            "v3_before_subscribe() [{}], {:#?}",
            session.client_id(),
            subscribe
        );
        Ok(HookSubscribeCode::Success)
    }

    async fn v3_after_subscribe(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &v3::Subscribe,
        _changed: bool,
        _codes: Option<Vec<v3::SubscribeReturnCode>>,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!("v3_after_subscribe(), [{}]", session.client_id());
        Ok(Vec::new())
    }

    async fn v3_before_unsubscribe(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        unsubscribe: &mut v3::Unsubscribe,
        _changed: &mut bool,
    ) -> HookResult<HookUnsubscribeCode> {
        log::debug!(
            "v3_before_unsubscribe(), [{}], {:#?}",
            session.client_id(),
            unsubscribe
        );
        Ok(HookUnsubscribeCode::Success)
    }

    async fn v3_after_unsubscribe(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &v3::Unsubscribe,
        _changed: bool,
    ) -> HookResult<Vec<HookAction>> {
        log::debug!("v3_after_unsubscribe(), [{}]", session.client_id());
        Ok(Vec::new())
    }
}

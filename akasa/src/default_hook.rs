use akasa_core::mqtt_proto::{v3, v5};
use akasa_core::{
    Hook, HookConnectCode, HookConnectedAction, HookPublishCode, SessionV3, SessionV5,
};
use async_trait::async_trait;

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
        _encode_len: usize,
        _packet_body: &[u8],
        publish: &mut v5::Publish,
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
        _encode_len: usize,
        _packet_body: &[u8],
        subscribe: &mut v5::Subscribe,
    ) {
        log::debug!(
            "v5_before_subscribe() [{}], {:#?}",
            session.client_id(),
            subscribe
        );
    }

    async fn v5_after_subscribe(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &v5::Subscribe,
        _reason_codes: Option<Vec<v5::SubscribeReasonCode>>,
    ) {
        log::debug!("v5_after_subscribe(), [{}]", session.client_id());
    }

    async fn v5_before_unsubscribe(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        unsubscribe: &mut v5::Unsubscribe,
    ) {
        log::debug!(
            "v5_before_unsubscribe(), [{}], {:#?}",
            session.client_id(),
            unsubscribe
        );
    }

    async fn v5_after_unsubscribe(
        &self,
        session: &SessionV5,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &v5::Unsubscribe,
    ) {
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
        _encode_len: usize,
        _packet_body: &[u8],
        publish: &mut v3::Publish,
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
        _encode_len: usize,
        _packet_body: &[u8],
        subscribe: &mut v3::Subscribe,
    ) {
        log::debug!(
            "v3_before_subscribe() [{}], {:#?}",
            session.client_id(),
            subscribe
        );
    }

    async fn v3_after_subscribe(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _subscribe: &v3::Subscribe,
        _codes: Option<Vec<v3::SubscribeReturnCode>>,
    ) {
        log::debug!("v3_after_subscribe(), [{}]", session.client_id());
    }

    async fn v3_before_unsubscribe(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        unsubscribe: &mut v3::Unsubscribe,
    ) {
        log::debug!(
            "v3_before_unsubscribe(), [{}], {:#?}",
            session.client_id(),
            unsubscribe
        );
    }

    async fn v3_after_unsubscribe(
        &self,
        session: &SessionV3,
        _encode_len: usize,
        _packet_body: &[u8],
        _unsubscribe: &v3::Unsubscribe,
    ) {
        log::debug!("v3_after_unsubscribe(), [{}]", session.client_id());
    }
}

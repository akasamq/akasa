use mqtt_proto::v5::Packet;

use super::super::Session;

#[inline]
pub(crate) fn handle_pingreq(session: &mut Session) -> Packet {
    log::debug!("{} received a ping packet", session.client_id);
    Packet::Pingresp
}

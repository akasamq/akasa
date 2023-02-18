use std::io;
use std::sync::Arc;

use futures_lite::io::AsyncWrite;
use mqtt_proto::v5::Packet;

use crate::state::GlobalState;

use super::super::Session;
use super::common::write_packet;

#[inline]
pub(crate) async fn handle_pingreq<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{} received a ping packet", session.client_id);
    write_packet(session.client_id, conn, &Packet::Pingresp).await?;
    Ok(())
}

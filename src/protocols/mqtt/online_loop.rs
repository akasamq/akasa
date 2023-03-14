use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use flume::{
    r#async::{RecvStream, SendSink},
    Sender,
};
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    Stream,
};
use futures_sink::Sink;
use hashbrown::HashMap;
use mqtt_proto::{v3, v5, GenericPollPacket, GenericPollPacketState, PollHeader, QoS, VarBytes};
use tokio::sync::oneshot;

use crate::hook::{HookRequest, HookResult};
use crate::state::{ClientId, ClientReceiver, ControlMessage, GlobalState, NormalMessage};

pub struct OnlineLoop<'a, C, S, H>
where
    S: OnlineSession,
    S::SessionState: 'static,
{
    session: &'a mut S,
    global: &'a Arc<GlobalState>,
    receiver: &'a ClientReceiver,
    control_stream: RecvStream<'a, ControlMessage>,
    normal_stream: RecvStream<'a, (ClientId, NormalMessage)>,
    hook_sink: SendSink<'a, HookRequest>,
    conn: &'a mut C,
    taken_over: &'a mut bool,

    read_unfinish: bool,
    normal_stream_unfinish: bool,

    packet_state: GenericPollPacketState<H>,
    session_state_sender: Option<(SendSink<'static, S::SessionState>, bool)>,
    hook_message: Option<(Option<HookRequest>, bool, oneshot::Receiver<HookResult>)>,
    write_packets_max: usize,
    write_packets: VecDeque<WritePacket<S::Packet>>,
}

impl<'a, C, S, H> OnlineLoop<'a, C, S, H>
where
    S: OnlineSession,
    S::SessionState: 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: &'a mut S,
        global: &'a Arc<GlobalState>,
        receiver: &'a ClientReceiver,
        control_stream: RecvStream<'a, ControlMessage>,
        normal_stream: RecvStream<'a, (ClientId, NormalMessage)>,
        hook_sink: SendSink<'a, HookRequest>,
        conn: &'a mut C,
        taken_over: &'a mut bool,
        packet_state: GenericPollPacketState<H>,
    ) -> Self {
        OnlineLoop {
            session,
            global,
            receiver,
            control_stream,
            normal_stream,
            hook_sink,
            conn,
            taken_over,
            packet_state,
            read_unfinish: false,
            normal_stream_unfinish: false,
            hook_message: None,
            write_packets_max: 4,
            write_packets: VecDeque::with_capacity(4),
            session_state_sender: None,
        }
    }
}

impl<'a, C, S, H> Future for OnlineLoop<'a, C, S, H>
where
    C: AsyncRead + AsyncWrite + Unpin, // connection
    S: OnlineSession,
    H: PollHeader<Packet = S::Packet, Error = S::Error> + Copy + Debug + Unpin,
    S::Error: From<io::Error> + From<mqtt_proto::Error> + Debug,
    S::Packet: MqttPacket + Debug + Unpin,
{
    type Output = Option<io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // [Goals]:
        //   * Forbid use too much memory
        //   * Forbid use too much CPU
        //   * Forbid loose message
        //   * Forbid message delay
        //
        // [Terms]:
        //   * Insert data to current state is producer
        //   * Remove data from current state is consumer
        //
        // [Rules]:
        //   * Consumer code must place after producer code
        //   * When current state is full must not run producer code
        //   * Consumer finished should consider wake up Producer code

        let OnlineLoop {
            ref mut session,
            global,
            receiver,
            ref mut control_stream,
            ref mut normal_stream,
            ref mut hook_sink,
            ref mut conn,
            ref mut read_unfinish,
            ref mut normal_stream_unfinish,
            packet_state,
            session_state_sender,
            hook_message,
            write_packets_max,
            write_packets,
            taken_over,
        } = self.get_mut();

        let current_client_id = session.client_id();
        log::trace!("@@@@ [{}] poll()", current_client_id);

        if let Some((request_opt, flushed, hook_receiver)) = hook_message.as_mut() {
            // NOTE: `session` and `write_packets` are
            // locked into hook_request, so currently
            // handle hook logic is the highest priority.
            if request_opt.is_some() {
                match Pin::new(&mut *hook_sink).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        log::error!("hook service stopped");
                        return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                    }
                    Poll::Pending => return Poll::Pending,
                }
                let hook_request = request_opt.take().expect("some");
                if Pin::new(&mut *hook_sink).start_send(hook_request).is_err() {
                    log::error!("hook service stopped");
                    return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                }
            }
            if !(*flushed) {
                match Pin::new(&mut *hook_sink).poll_flush(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => *flushed = true,
                    Poll::Ready(Err(_err)) => {
                        log::error!("hook service stopped");
                        return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                    }
                }
            }
            if *flushed {
                match Pin::new(&mut *hook_receiver).poll(cx) {
                    Poll::Ready(result) => match result {
                        Ok(Ok(())) => {
                            log::debug!("hook acked (follow poll)");
                            *hook_message = None;
                        }
                        Ok(Err(err_opt)) => return Poll::Ready(err_opt),
                        Err(_err) => {
                            log::error!("hook service stopped");
                            return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                        }
                    },
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        // Send SessionState to new connection (been taken over)
        //   * Consume: [session_state_sender]
        if let Some((mut send_sink, flushing)) = session_state_sender.take() {
            if !flushing {
                match Pin::new(&mut send_sink).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        // channel disconnected, cancel takeover
                        log::info!("[{}] The connection want take over current session already ended, process canceled", current_client_id);
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Poll::Pending => {
                        // channel is full
                        *session_state_sender = Some((send_sink, false));
                        return Poll::Pending;
                    }
                }
                let old_state = session.build_state(receiver.clone());
                if Pin::new(&mut send_sink).start_send(old_state).is_err() {
                    // channel disconnected, cancel takeover
                    log::info!("[{}] The connection want take over current session already ended, process canceled", current_client_id);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            return match Pin::new(&mut send_sink).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    log::info!("[{}] current session been taken over", current_client_id);
                    **taken_over = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Err(_)) => {
                    log::info!("[{}] The connection want take over current session already ended, process canceled", current_client_id);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => {
                    // channel is full
                    *session_state_sender = Some((send_sink, true));
                    Poll::Pending
                }
            };
        }

        let mut pendings = Pendings::default();
        let mut have_write = false;

        log::trace!(
            "[{}] write_packets={}, broadcast_packets={}, ",
            current_client_id,
            write_packets.len(),
            session.broadcast_packets_cnt(),
        );

        // ==================
        // ==== Producer ====
        // ==================

        // Read data from client connection
        //   * Produce to: [write_packets, broadcast_packets, external_request]
        loop {
            if write_packets.len() >= *write_packets_max
                || session.broadcast_packets_cnt() >= session.broadcast_packets_max()
            {
                *read_unfinish = true;
                break;
            } else {
                *read_unfinish = false;
            }

            log::trace!(
                "[{}] going to read, write_packets.len() = {}, broadcast_packets_cnt = {}",
                current_client_id,
                write_packets.len(),
                session.broadcast_packets_cnt(),
            );
            // TODO: Decode Header first for more detailed error report.
            let mut poll_packet = GenericPollPacket::new(packet_state, conn);
            let packet_result = match Pin::new(&mut poll_packet).poll(cx) {
                Poll::Ready(result) => result,
                Poll::Pending => {
                    log::trace!("[{}] read pending", current_client_id);
                    *read_unfinish = false;
                    pendings.read = true;
                    break;
                }
            };
            match packet_result {
                Ok((encode_len, packet)) => {
                    log::trace!("[{}] decode MQTT packet: {:?}", current_client_id, packet);
                    *packet_state = GenericPollPacketState::default();

                    match session.handle_packet(encode_len, packet, write_packets, global) {
                        Ok(Some((hook_request, mut hook_receiver))) => {
                            match Pin::new(&mut *hook_sink).poll_ready(cx) {
                                Poll::Ready(Ok(())) => {}
                                Poll::Ready(Err(_)) => {
                                    log::error!("hook service stopped");
                                    return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                                }
                                Poll::Pending => {
                                    *hook_message =
                                        Some((Some(hook_request), false, hook_receiver));
                                    // NOTE: `session` and `write_packets` are
                                    // locked into hook_request, so currently
                                    // handle hook logic is the highest priority.
                                    return Poll::Pending;
                                }
                            }
                            if Pin::new(&mut *hook_sink).start_send(hook_request).is_err() {
                                log::error!("hook service stopped");
                                return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                            }
                            let flushed = match Pin::new(&mut *hook_sink).poll_flush(cx) {
                                Poll::Pending => false,
                                Poll::Ready(Ok(())) => true,
                                Poll::Ready(Err(_err)) => {
                                    log::error!("hook service stopped");
                                    return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                                }
                            };
                            if flushed {
                                match Pin::new(&mut hook_receiver).poll(cx) {
                                    Poll::Ready(result) => match result {
                                        Ok(Ok(())) => log::debug!("hook acked"),
                                        Ok(Err(err_opt)) => return Poll::Ready(err_opt),
                                        Err(_err) => {
                                            log::error!("hook service stopped");
                                            return Poll::Ready(Some(
                                                io::ErrorKind::InvalidData.into(),
                                            ));
                                        }
                                    },
                                    Poll::Pending => {
                                        // NOTE: `session` and `write_packets` are
                                        // locked into hook_request, so currently
                                        // handle hook logic is the highest priority.
                                        *hook_message = Some((None, true, hook_receiver));
                                        return Poll::Pending;
                                    }
                                }
                            } else {
                                // NOTE: `session` and `write_packets` are
                                // locked into hook_request, so currently
                                // handle hook logic is the highest priority.
                                *hook_message = Some((None, false, hook_receiver));
                                return Poll::Pending;
                            }
                        }
                        Ok(None) => {}
                        Err(err_opt) => return Poll::Ready(err_opt),
                    }
                    session.after_handle_packet(write_packets);
                }
                Err(err) => {
                    if let Err(err_opt) = session.handle_decode_error(err, write_packets) {
                        return Poll::Ready(err_opt);
                    }
                }
            }
        }

        // Receive control messages
        //   * Produce to: [broadcast_packets, session_state_sender]
        loop {
            // send_will() function will insert broadcast_packets, but it's OK.
            let msg = match Pin::new(&mut *control_stream).poll_next(cx) {
                Poll::Ready(Some(output)) => output,
                Poll::Ready(None) => {
                    log::error!("control senders all dropped by {}", current_client_id);
                    return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                }
                Poll::Pending => {
                    pendings.control_message = true;
                    break;
                }
            };
            log::trace!(
                "[{}] handling control message: {:?}",
                current_client_id,
                msg
            );
            let (stop, sender_opt) = session.handle_control(msg, global);
            if let Some(sender) = sender_opt {
                log::debug!("[{}] yield because session take over", current_client_id);
                *session_state_sender = Some((sender.into_sink(), false));
                // Since it's high priority, we just return here so session start take over process.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if stop {
                return Poll::Ready(None);
            }
        }

        // Receive normal messages
        //   * Produce to: [write_packets]
        // FIXME: drop message when pending queue are full
        loop {
            if write_packets.len() >= *write_packets_max {
                *normal_stream_unfinish = true;
                break;
            } else {
                *normal_stream_unfinish = false;
            }

            let (sender_id, msg) = match Pin::new(&mut *normal_stream).poll_next(cx) {
                Poll::Ready(Some(output)) => output,
                Poll::Ready(None) => {
                    log::error!("normal senders all dropped by {}", current_client_id);
                    return Poll::Ready(Some(io::ErrorKind::InvalidData.into()));
                }
                Poll::Pending => {
                    log::trace!("[{}] normal receiver is pending", current_client_id);
                    pendings.normal_message = true;
                    break;
                }
            };

            log::trace!(
                "[{}] received a normal message from [{}], {:?}",
                current_client_id,
                sender_id,
                msg,
            );
            if let Some((final_qos, packet_opt)) = session.handle_normal(sender_id, msg, global) {
                if let Some(packet) = packet_opt {
                    write_packets.push_back(packet.into());
                }
                if final_qos != QoS::Level0 {
                    let pending_packets = session.handle_pendings();
                    if !pending_packets.is_empty() {
                        write_packets.extend(pending_packets.into_iter().map(WritePacket::Packet));
                    }
                }
            }
        }

        // ==================
        // ==== Consumer ====
        // ==================

        // Write packets to client connection
        //   * Consume: [write_packets]
        while let Some(write_packet) = write_packets.pop_front() {
            log::trace!("[{}] encode packet: {:?}", current_client_id, write_packet);
            let (data, mut idx) = match write_packet {
                WritePacket::Packet(pkt) => match pkt.encode() {
                    Ok(data) => (data, 0),
                    Err(err) => return Poll::Ready(Some(err)),
                },
                WritePacket::Data((data, idx)) => (data, idx),
            };
            match Pin::new(&mut *conn).poll_write(cx, data.as_ref()) {
                Poll::Ready(Ok(size)) => {
                    have_write = true;
                    log::trace!("[{}] write {} bytes data", current_client_id, size);
                    idx += size;
                    if idx < data.as_ref().len() {
                        write_packets.push_front(WritePacket::Data((data, idx)));
                        break;
                    }
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Some(err)),
                Poll::Pending => {
                    write_packets.push_front(WritePacket::Data((data, idx)));
                    pendings.write = true;
                    break;
                }
            }
        }
        if have_write
            && write_packets.capacity() > (*write_packets_max) * 2
            && write_packets.len() <= *write_packets_max
        {
            write_packets.shrink_to(*write_packets_max);
        }
        if !pendings.write {
            match Pin::new(&mut *conn).poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Some(err)),
                Poll::Pending => pendings.write = true,
            }
        }

        // Broadcast packets to matched sessions
        //   * Consume from: [broadcast_packets]
        log::trace!(
            "[{}] broadcast_cnt={}, broadcast_packets.len() = {}",
            current_client_id,
            session.broadcast_packets_cnt(),
            session.broadcast_packets().len(),
        );
        let old_cnt: usize = session
            .broadcast_packets()
            .values()
            .map(|info| info.msgs.len())
            .sum();
        let mut consume_cnt = 0;
        session.broadcast_packets().retain(|client_id, info| {
            log::trace!(
                "[{}] handling broadcast: flushed={}, msgs={:?}",
                current_client_id,
                info.flushed,
                info.msgs
            );
            // `info.flushed` is for msgs is empty
            if !info.flushed && info.msgs.is_empty() {
                return match Pin::new(&mut info.sink).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        log::trace!(
                            "[{}] broadcast to [{}] flush success",
                            current_client_id,
                            client_id,
                        );
                        false
                    }
                    Poll::Ready(Err(_)) => {
                        consume_cnt += info.msgs.len();
                        false
                    }
                    Poll::Pending => {
                        log::trace!(
                            "[{}] broadcast to [{}] retry not flushed",
                            current_client_id,
                            client_id,
                        );
                        true
                    }
                };
            }
            while let Some(msg) = info.msgs.pop_front() {
                consume_cnt += 1;
                match Pin::new(&mut info.sink).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        // channel disconnected
                        consume_cnt += info.msgs.len();
                        return false;
                    }
                    Poll::Pending => {
                        // channel is full
                        consume_cnt -= 1;
                        info.msgs.push_front(msg);
                        log::trace!("target client channel is pending: [{}]", client_id);
                        return true;
                    }
                }
                log::trace!(
                    "[{}] broadcast to [{}] {:?}",
                    current_client_id,
                    client_id,
                    msg
                );
                if Pin::new(&mut info.sink)
                    .start_send((current_client_id, msg))
                    .is_err()
                {
                    log::trace!("send publish to disconnected client: {}", client_id);
                    consume_cnt += info.msgs.len();
                    return false;
                }
            }
            match Pin::new(&mut info.sink).poll_flush(cx) {
                Poll::Ready(_) => false,
                Poll::Pending => {
                    log::trace!(
                        "[{}] broadcast to [{}] not flushed",
                        current_client_id,
                        client_id,
                    );
                    info.flushed = false;
                    true
                }
            }
        });
        let new_cnt: usize = session
            .broadcast_packets()
            .values()
            .map(|info| info.msgs.len())
            .sum();
        // TODO: make this as debug assert when ready
        assert_eq!(
            new_cnt + consume_cnt,
            old_cnt,
            "new:{} + consume:{} != old:{}",
            new_cnt,
            consume_cnt,
            old_cnt
        );
        let have_broadcast = consume_cnt > 0;
        session.consume_broadcast(consume_cnt);

        // FIXME: handle extension request here

        if session.disconnected() {
            return Poll::Ready(None);
        }

        // Check if all pending
        log::trace!("[{}] {:?}", current_client_id, pendings);
        log::trace!(
            "[{}] write_packets={}, broadcast_packets={}, ",
            current_client_id,
            write_packets.len(),
            session.broadcast_packets_cnt(),
        );
        log::trace!(
            "[{}] read_unfinish={}, normal_stream_unfinish={}",
            current_client_id,
            read_unfinish,
            normal_stream_unfinish
        );

        if pendings.read
            && pendings.control_message
            && pendings.normal_message
            && (pendings.write || write_packets.is_empty())
            && (pendings.broadcast || session.broadcast_packets_cnt() == 0)
        {
            log::debug!("[{}] return pending", current_client_id);
            return Poll::Pending;
        }

        if have_write && (*read_unfinish || *normal_stream_unfinish) {
            log::debug!(
                "[{}] yield because write processed (producer unfinish)",
                current_client_id
            );
            *read_unfinish = false;
            *normal_stream_unfinish = false;
            cx.waker().wake_by_ref();
        } else if have_broadcast && *normal_stream_unfinish {
            log::debug!(
                "[{}] yield because broadcast processed (producer unfinish)",
                current_client_id
            );
            *normal_stream_unfinish = false;
            cx.waker().wake_by_ref();
        } else {
            log::debug!("[{}] NOT yield", current_client_id);
        }
        Poll::Pending
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Pendings {
    // producer
    read: bool,
    control_message: bool,
    normal_message: bool,
    // consumer
    write: bool,
    broadcast: bool,
}

#[derive(Debug)]
pub enum WritePacket<P> {
    Packet(P),
    Data((VarBytes, usize)),
}

impl<P> From<P> for WritePacket<P> {
    fn from(pkt: P) -> WritePacket<P> {
        WritePacket::Packet(pkt)
    }
}

pub struct BroadcastPackets {
    pub sink: SendSink<'static, (ClientId, NormalMessage)>,
    pub msgs: VecDeque<NormalMessage>,
    pub flushed: bool,
}

pub trait MqttPacket {
    fn encode(&self) -> Result<VarBytes, io::Error>;
}

impl MqttPacket for v3::Packet {
    fn encode(&self) -> Result<VarBytes, io::Error> {
        self.encode().map_err(io::Error::from)
    }
}
impl MqttPacket for v5::Packet {
    fn encode(&self) -> Result<VarBytes, io::Error> {
        self.encode().map_err(io::Error::from)
    }
}

pub trait OnlineSession {
    type Packet;
    type Error;
    type SessionState;

    fn client_id(&self) -> ClientId;
    fn disconnected(&self) -> bool;
    fn build_state(&mut self, receiver: ClientReceiver) -> Self::SessionState;

    fn consume_broadcast(&mut self, count: usize);
    fn broadcast_packets_cnt(&self) -> usize;
    fn broadcast_packets_max(&self) -> usize;
    fn broadcast_packets(&mut self) -> &mut HashMap<ClientId, BroadcastPackets>;

    fn handle_decode_error(
        &mut self,
        err: Self::Error,
        write_packets: &mut VecDeque<WritePacket<Self::Packet>>,
    ) -> Result<(), Option<io::Error>>;
    fn handle_packet(
        &mut self,
        encode_len: usize,
        packet: Self::Packet,
        write_packets: &mut VecDeque<WritePacket<Self::Packet>>,
        global: &Arc<GlobalState>,
    ) -> Result<Option<(HookRequest, oneshot::Receiver<HookResult>)>, Option<io::Error>>;
    fn after_handle_packet(&mut self, write_packets: &mut VecDeque<WritePacket<Self::Packet>>);

    fn handle_control(
        &mut self,
        msg: ControlMessage,
        global: &Arc<GlobalState>,
    ) -> (bool, Option<Sender<Self::SessionState>>);
    fn handle_normal(
        &mut self,
        sender: ClientId,
        msg: NormalMessage,
        global: &Arc<GlobalState>,
    ) -> Option<(QoS, Option<Self::Packet>)>;
    fn handle_pendings(&mut self) -> Vec<Self::Packet>;
}

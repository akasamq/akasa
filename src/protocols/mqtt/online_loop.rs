use std::collections::VecDeque;
use std::fmt::{Debug, Display};
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
    conn: &'a mut C,
    taken_over: &'a mut bool,

    read_unfinish: bool,
    normal_stream_unfinish: bool,

    packet_state: GenericPollPacketState<H>,
    session_state_sender: Option<(SendSink<'static, S::SessionState>, bool)>,
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
            conn,
            taken_over,
            packet_state,
            read_unfinish: false,
            normal_stream_unfinish: false,
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
    H: PollHeader<Packet = S::Packet> + Copy + Debug + Unpin,
    H::Error: From<io::Error> + From<mqtt_proto::Error> + Debug + Display,
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
            ref mut conn,
            ref mut read_unfinish,
            ref mut normal_stream_unfinish,
            packet_state,
            session_state_sender,
            write_packets_max,
            write_packets,
            taken_over,
        } = self.get_mut();

        let current_client_id = session.client_id();
        log::debug!("@@@@ [{}] poll()", current_client_id);

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
        let mut have_broadcast = false;

        log::debug!(
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

            log::debug!(
                "[{}] going to read, write_packets.len() = {}, broadcast_packets_cnt = {}",
                current_client_id,
                write_packets.len(),
                session.broadcast_packets_cnt(),
            );
            // TODO: Decode Header first for more detailed error report.
            let mut poll_packet = GenericPollPacket::new(packet_state, conn);
            let (_total, packet) = match Pin::new(&mut poll_packet).poll(cx) {
                Poll::Ready(Ok(output)) => {
                    *packet_state = GenericPollPacketState::default();
                    output
                }
                Poll::Ready(Err(err)) => {
                    log::debug!("[{}] mqtt v5.x codec error: {}", current_client_id, err);
                    return if H::is_eof_error(&err) {
                        if !session.disconnected() {
                            Poll::Ready(Some(io::ErrorKind::UnexpectedEof.into()))
                        } else {
                            Poll::Ready(None)
                        }
                    } else {
                        // FIXME: return error reason info with connack/disconnect packet
                        Poll::Ready(Some(io::ErrorKind::InvalidData.into()))
                    };
                }
                Poll::Pending => {
                    log::debug!("[{}] read pending, {:?}", current_client_id, packet_state);
                    *read_unfinish = false;
                    pendings.read = true;
                    break;
                }
            };
            // TODO: handle packet
            //   * insert packet into write_packets
            //   * insert packet into broadcast_packets

            log::debug!(
                "[{}] success decode MQTT v5 packet: {:?}",
                current_client_id,
                packet
            );
            if let Err(err) = session.handle_packet(packet, write_packets, global) {
                return Poll::Ready(Some(err));
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
            log::debug!(
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
        log::debug!("[{}] reading normal message", current_client_id);
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
                    log::debug!("[{}] normal receiver is pending", current_client_id);
                    pendings.normal_message = true;
                    break;
                }
            };

            log::debug!(
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
            log::debug!("[{}] encode packet: {:?}", current_client_id, write_packet);
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
                    log::debug!("[{}] write {} bytes data", current_client_id, size);
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
        log::debug!(
            "[{}] broadcast_cnt={}, broadcast_packets.len() = {}",
            current_client_id,
            session.broadcast_packets_cnt(),
            session.broadcast_packets().len(),
        );
        let mut consume_cnt = 0;
        session.broadcast_packets().retain(|client_id, info| {
            log::debug!(
                "[{}] handling broadcast: flushed={}, msgs={:?}",
                current_client_id,
                info.flushed,
                info.msgs
            );
            // `info.flushed` is for msgs is empty
            if !info.flushed && info.msgs.is_empty() {
                return match Pin::new(&mut info.sink).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        log::debug!(
                            "[{}] broadcast to [{}] flush success",
                            current_client_id,
                            client_id,
                        );
                        consume_cnt += 1;
                        false
                    }
                    Poll::Ready(Err(_)) => {
                        consume_cnt += info.msgs.len() + 1;
                        false
                    }
                    Poll::Pending => {
                        log::debug!(
                            "[{}] broadcast to [{}] retry not flushed",
                            current_client_id,
                            client_id,
                        );
                        true
                    }
                };
            }
            while let Some(msg) = info.msgs.pop_front() {
                match Pin::new(&mut info.sink).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        // channel disconnected
                        consume_cnt += info.msgs.len() + 1;
                        return false;
                    }
                    Poll::Pending => {
                        // channel is full
                        info.msgs.push_front(msg);
                        log::debug!("target client channel is pending: [{}]", client_id);
                        return true;
                    }
                }
                log::debug!(
                    "[{}] broadcast to [{}] {:?}",
                    current_client_id,
                    client_id,
                    msg
                );
                if Pin::new(&mut info.sink)
                    .start_send((current_client_id, msg))
                    .is_err()
                {
                    log::info!("send publish to disconnected client: {}", client_id);
                    consume_cnt += info.msgs.len() + 1;
                    return false;
                } else {
                    have_broadcast = true;
                }

                match Pin::new(&mut info.sink).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        log::debug!(
                            "[{}] broadcast to [{}] SUCCESS",
                            current_client_id,
                            client_id,
                        );
                        consume_cnt += 1;
                    }
                    Poll::Ready(Err(_)) => {
                        consume_cnt += info.msgs.len() + 1;
                        return false;
                    }
                    Poll::Pending => {
                        log::debug!(
                            "[{}] broadcast to [{}] not flushed",
                            current_client_id,
                            client_id,
                        );
                        info.flushed = false;
                        return true;
                    }
                }
            }
            false
        });
        session.consume_broadcast(consume_cnt);

        // FIXME: handle extension request here

        if session.disconnected() {
            return Poll::Ready(None);
        }

        // Check if all pending
        log::debug!("[{}] {:?}", current_client_id, pendings);
        log::debug!(
            "[{}] write_packets={}, broadcast_packets={}, ",
            current_client_id,
            write_packets.len(),
            session.broadcast_packets_cnt(),
        );
        log::debug!(
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
    read: bool,
    control_message: bool,
    normal_message: bool,

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
    type SessionState;

    fn client_id(&self) -> ClientId;
    fn disconnected(&self) -> bool;
    fn build_state(&mut self, receiver: ClientReceiver) -> Self::SessionState;

    fn consume_broadcast(&mut self, count: usize);
    fn broadcast_packets_cnt(&self) -> usize;
    fn broadcast_packets_max(&self) -> usize;
    fn broadcast_packets(&mut self) -> &mut HashMap<ClientId, BroadcastPackets>;

    fn handle_packet(
        &mut self,
        packet: Self::Packet,
        write_packets: &mut VecDeque<WritePacket<Self::Packet>>,
        global: &Arc<GlobalState>,
    ) -> Result<(), io::Error>;
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

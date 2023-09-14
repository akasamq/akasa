use std::fmt;
use std::future::Future;
use std::io;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use flume::{bounded, Receiver, Sender};
use mqtt_proto::{v5::PublishProperties, Protocol, QoS, TopicFilter, TopicName};
use parking_lot::Mutex;

use crate::config::Config;
use crate::protocols::mqtt::{self, RetainTable, RouteTable};

pub struct GlobalState {
    // The next client internal id
    // use this mutex to keep `add_client` atomic
    next_client_id: Mutex<ClientId>,
    // online clients count
    online_clients: AtomicU64,
    // client internal id => (MQTT client identifier, online)
    client_id_map: DashMap<ClientId, (String, bool)>,
    // MQTT client identifier => client internal id
    client_identifier_map: DashMap<String, ClientId>,
    // All clients (online/offline clients)
    clients: DashMap<ClientId, ClientSender>,

    pub config: Config,
    pub auth_passwords: DashMap<String, AuthPassword>,

    /// MQTT route table
    pub route_table: RouteTable,

    /// MQTT retain table
    pub retain_table: RetainTable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashAlgorithm {
    Sha256,
    Sha512,
    Sha256Pkbdf2 { iterations: NonZeroU32 },
    Sha512Pkbdf2 { iterations: NonZeroU32 },
}

pub struct AuthPassword {
    pub hash_algorithm: HashAlgorithm,
    pub hashed_password: Vec<u8>,
    // salt is suffix
    pub salt: Vec<u8>,
}

#[derive(Clone)]
pub struct ClientSender {
    pub normal: Sender<(ClientId, NormalMessage)>,
    pub control: Sender<ControlMessage>,
}

#[derive(Clone)]
pub struct ClientReceiver {
    pub normal: Receiver<(ClientId, NormalMessage)>,
    pub control: Receiver<ControlMessage>,
}

impl GlobalState {
    pub fn new(config: Config) -> GlobalState {
        GlobalState {
            // FIXME: load from db (rosksdb or sqlite3)
            next_client_id: Mutex::new(ClientId(0)),
            online_clients: AtomicU64::new(0),
            client_id_map: DashMap::new(),
            client_identifier_map: DashMap::new(),
            clients: DashMap::new(),

            config,
            auth_passwords: DashMap::new(),
            route_table: RouteTable::default(),
            retain_table: RetainTable::default(),
        }
    }

    pub fn online_clients_count(&self) -> u64 {
        self.online_clients.load(Ordering::Acquire)
    }
    // pub fn offline_clients_count(&self) -> usize {
    //     self.clients.len() - *self.online_clients.lock()
    // }
    pub fn clients_count(&self) -> usize {
        self.clients.len()
    }

    // When clean_session=1 and client disconnected
    pub fn remove_client<'a>(
        &self,
        client_id: ClientId,
        subscribes: impl IntoIterator<Item = &'a TopicFilter>,
    ) {
        // keep client operation atomic
        let _guard = self.next_client_id.lock();
        if let Some((_, (client_identifier, online))) = self.client_id_map.remove(&client_id) {
            self.client_identifier_map.remove(&client_identifier);
            if online {
                assert_ne!(self.online_clients.fetch_sub(1, Ordering::AcqRel), 0);
            }
        }
        self.clients.remove(&client_id);
        for filter in subscribes {
            self.route_table.unsubscribe(filter, client_id);
        }
    }

    // When clean_session=0 and client disconnected
    pub fn offline_client(&self, client_id: ClientId) {
        let _guard = self.next_client_id.lock();
        assert_ne!(self.online_clients.fetch_sub(1, Ordering::AcqRel), 0);
        if let Some(mut pair) = self.client_id_map.get_mut(&client_id) {
            pair.value_mut().1 = false;
        }
    }

    pub fn get_client_normal_sender(
        &self,
        client_id: &ClientId,
    ) -> Option<Sender<(ClientId, NormalMessage)>> {
        self.clients
            .get(client_id)
            .map(|pair| pair.value().normal.clone())
    }
    pub fn get_client_control_sender(
        &self,
        client_id: &ClientId,
    ) -> Option<Sender<ControlMessage>> {
        self.clients
            .get(client_id)
            .map(|pair| pair.value().control.clone())
    }

    // Client connected
    // TODO: error handling
    pub async fn add_client(
        &self,
        client_identifier: &str,
        protocol: Protocol,
    ) -> io::Result<AddClientReceipt> {
        let mut round = 0;
        loop {
            let control_sender = {
                let mut next_client_id = self.next_client_id.lock();
                self.online_clients.fetch_add(1, Ordering::AcqRel);
                let client_id_opt: Option<ClientId> = self
                    .client_identifier_map
                    .get(client_identifier)
                    .map(|pair| *pair.value());
                if let Some(old_id) = client_id_opt {
                    if let Some(mut pair) = self.client_id_map.get_mut(&old_id) {
                        pair.value_mut().1 = true;
                    }
                    self.get_client_control_sender(&old_id).unwrap()
                } else {
                    let client_id = *next_client_id;
                    self.client_id_map
                        .insert(client_id, (client_identifier.to_string(), true));
                    self.client_identifier_map
                        .insert(client_identifier.to_string(), client_id);
                    // FIXME: if some one subscribe topic "#" and never receive the message it will block all sender clients.
                    //   Suggestion: Add QoS0 message to pending queue
                    let (control_sender, control_receiver) = bounded(1);
                    let (normal_sender, normal_receiver) = bounded(8);
                    let sender = ClientSender {
                        normal: normal_sender,
                        control: control_sender,
                    };
                    self.clients.insert(client_id, sender);
                    next_client_id.0 += 1;
                    return Ok(AddClientReceipt::New {
                        client_id,
                        receiver: ClientReceiver {
                            control: control_receiver,
                            normal: normal_receiver,
                        },
                    });
                }
            };

            // NOTE: only 0 retry is allowed in unit tests
            debug_assert!(round == 0, "add client round: {round}");
            // NOTE: only 2 retry is allowed in production
            if round > 2 {
                log::error!("add client round: {}, which is more than 2", round);
                return Err(io::Error::from(io::ErrorKind::TimedOut));
            }

            if protocol < Protocol::V500 {
                let (sender, receiver) = bounded(1);
                if control_sender
                    .send_async(ControlMessage::OnlineV3 { sender })
                    .await
                    .is_err()
                {
                    // old client may already removed, retry to create new one
                    round += 1;
                    continue;
                }
                let session_state = receiver
                    .recv_async()
                    .await
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                return Ok(AddClientReceipt::PresentV3(session_state));
            } else {
                let (sender, receiver) = bounded(1);
                if control_sender
                    .send_async(ControlMessage::OnlineV5 { sender })
                    .await
                    .is_err()
                {
                    // old client may already removed, retry to crate new one
                    round += 1;
                    continue;
                }
                let session_state = receiver
                    .recv_async()
                    .await
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                return Ok(AddClientReceipt::PresentV5(session_state));
            }
        }
    }
}

pub trait Executor {
    fn id(&self) -> usize {
        0
    }
    fn spawn_local<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    fn spawn_sleep<F>(&self, duration: Duration, task: F)
    where
        F: Future<Output = ()> + Send + 'static;

    fn spawn_interval<G, F>(&self, action_gen: G) -> io::Result<()>
    where
        G: (Fn() -> F) + Send + Sync + 'static,
        F: Future<Output = Option<Duration>> + Send + 'static;
}

impl<T: Executor> Executor for Rc<T> {
    fn id(&self) -> usize {
        self.as_ref().id()
    }
    fn spawn_local<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.as_ref().spawn_local(future);
    }

    fn spawn_sleep<F>(&self, duration: Duration, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.as_ref().spawn_sleep(duration, task);
    }

    fn spawn_interval<G, F>(&self, action_gen: G) -> io::Result<()>
    where
        G: (Fn() -> F) + Send + Sync + 'static,
        F: Future<Output = Option<Duration>> + Send + 'static,
    {
        self.as_ref().spawn_interval(action_gen)
    }
}
impl<T: Executor> Executor for Arc<T> {
    fn id(&self) -> usize {
        self.as_ref().id()
    }

    fn spawn_local<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.as_ref().spawn_local(future);
    }

    fn spawn_sleep<F>(&self, duration: Duration, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.as_ref().spawn_sleep(duration, task);
    }

    fn spawn_interval<G, F>(&self, action_gen: G) -> io::Result<()>
    where
        G: (Fn() -> F) + Send + Sync + 'static,
        F: Future<Output = Option<Duration>> + Send + 'static,
    {
        self.as_ref().spawn_interval(action_gen)
    }
}

#[derive(Debug, Clone)]
pub enum ControlMessage {
    /// The v3.x client of the session connected, send the keept session to the connection loop
    OnlineV3 {
        sender: Sender<mqtt::v3::SessionState>,
    },
    /// The v5.x client of the session connected, send the keept session to the connection loop
    OnlineV5 {
        sender: Sender<mqtt::v5::SessionState>,
    },
    // TODO: maybe rename to Timeout
    /// Kick client out (disconnect the client)
    Kick {
        reason: String,
    },
    SessionExpired {
        connected_time: Instant,
    },
    WillDelayReached {
        connected_time: Instant,
    },
}

#[derive(Debug, Clone)]
pub enum NormalMessage {
    /// A publish message matched
    PublishV3 {
        retain: bool,
        qos: QoS,
        topic_name: TopicName,
        payload: Bytes,
        subscribe_filter: TopicFilter,
        subscribe_qos: QoS,
        encode_len: usize,
    },
    PublishV5 {
        retain: bool,
        qos: QoS,
        topic_name: TopicName,
        payload: Bytes,
        subscribe_filter: TopicFilter,
        // [MQTTv5.0-3.8.4] keyword: downgraded
        subscribe_qos: QoS,
        properties: PublishProperties,
        encode_len: usize,
    },
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ClientId(u64);

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "client#{}", self.0)
    }
}

impl ClientId {
    #[cfg(test)]
    pub fn new(value: u64) -> ClientId {
        ClientId(value)
    }

    pub fn max_value() -> ClientId {
        ClientId(u64::max_value())
    }
}

pub enum AddClientReceipt {
    PresentV3(mqtt::v3::SessionState),
    PresentV5(mqtt::v5::SessionState),
    New {
        client_id: ClientId,
        receiver: ClientReceiver,
    },
}

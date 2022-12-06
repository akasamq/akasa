use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use flume::{bounded, Receiver, Sender};
use glommio::TaskQueueHandle;
use mqtt::{QualityOfService, TopicFilter, TopicName};
use parking_lot::Mutex;

use crate::config::Config;
use crate::protocols::mqtt::{RetainTable, RouteTable, SessionState};

pub struct GlobalState {
    // The next client internal id
    next_client_id: Mutex<ClientId>,
    // online clients count
    online_clients: Mutex<usize>,
    // client internal id => (MQTT client identifier, online)
    client_id_map: DashMap<ClientId, (String, bool)>,
    // MQTT client identifier => client internal id
    client_identifier_map: DashMap<String, ClientId>,
    // All clients (online/offline clients)
    clients: DashMap<ClientId, Sender<(ClientId, InternalMessage)>>,

    pub bind: SocketAddr,
    pub config: Config,

    /// MQTT route table
    pub route_table: RouteTable,

    /// MQTT retain table
    pub retain_table: RetainTable,
}

impl GlobalState {
    pub fn new(bind: SocketAddr, config: Config) -> GlobalState {
        GlobalState {
            // FIXME: load from db (rosksdb or sqlite3)
            next_client_id: Mutex::new(ClientId(0)),
            online_clients: Mutex::new(0),
            client_id_map: DashMap::new(),
            client_identifier_map: DashMap::new(),
            clients: DashMap::new(),

            bind,
            config,
            route_table: RouteTable::new(),
            retain_table: RetainTable::new(),
        }
    }

    pub fn online_clients_count(&self) -> usize {
        *self.online_clients.lock()
    }
    // pub fn offline_clients_count(&self) -> usize {
    //     self.clients.len() - *self.online_clients.lock()
    // }
    pub fn clients_count(&self) -> usize {
        self.clients.len()
    }

    // When clean_session=1 and client disconnected
    pub fn remove_client(&self, client_id: ClientId) {
        // keep client operation atomic
        let _guard = self.next_client_id.lock();
        if let Some((_, (client_identifier, online))) = self.client_id_map.remove(&client_id) {
            self.client_identifier_map.remove(&client_identifier);
            if online {
                let mut online_clients = self.online_clients.lock();
                *online_clients -= 1;
            }
        }
        self.clients.remove(&client_id);
    }

    // When clean_session=0 and client disconnected
    pub fn offline_client(&self, client_id: ClientId) {
        let _guard = self.next_client_id.lock();
        {
            let mut online_clients = self.online_clients.lock();
            *online_clients -= 1;
        }
        if let Some(mut pair) = self.client_id_map.get_mut(&client_id) {
            pair.value_mut().1 = false;
        }
    }

    pub fn get_client_sender(
        &self,
        client_id: &ClientId,
    ) -> Option<Sender<(ClientId, InternalMessage)>> {
        self.clients.get(client_id).map(|pair| pair.value().clone())
    }

    // Client connected
    // TODO: error handling
    pub async fn add_client(&self, client_identifier: &str) -> AddClientReceipt {
        let (old_id, internal_sender) = {
            let mut next_client_id = self.next_client_id.lock();
            {
                let mut online_clients = self.online_clients.lock();
                *online_clients += 1;
            }
            let client_id_opt: Option<ClientId> = self
                .client_identifier_map
                .get(client_identifier)
                .map(|pair| *pair.value());
            if let Some(old_id) = client_id_opt {
                if let Some(mut pair) = self.client_id_map.get_mut(&old_id) {
                    pair.value_mut().1 = true;
                }
                let internal_sender = self.clients.get(&old_id).unwrap().value().clone();
                (old_id, internal_sender)
            } else {
                let client_id = *next_client_id;
                self.client_id_map
                    .insert(client_id, (client_identifier.to_string(), true));
                self.client_identifier_map
                    .insert(client_identifier.to_string(), client_id);
                let (sender, receiver) = bounded(4);
                self.clients.insert(client_id, sender);
                next_client_id.0 += 1;
                return AddClientReceipt::New {
                    client_id,
                    receiver,
                };
            }
        };

        let (sender, receiver) = bounded(1);
        internal_sender
            .send_async((old_id, InternalMessage::Online { sender }))
            .await
            .unwrap();
        let session_state = receiver.recv_async().await.unwrap();
        AddClientReceipt::Present(session_state)
    }
}

pub struct ExecutorState {
    pub id: usize,
    pub gc_queue: TaskQueueHandle,
}

impl ExecutorState {
    pub fn new(id: usize, gc_queue: TaskQueueHandle) -> ExecutorState {
        ExecutorState { id, gc_queue }
    }
}

#[derive(Clone)]
pub enum InternalMessage {
    /// The client of the session connected, send the keept session to the connection loop
    Online { sender: Sender<SessionState> },
    /// Kick client out (disconnect the client)
    Kick { reason: String },
    /// A publish message matched
    Publish {
        topic_name: Arc<TopicName>,
        qos: QualityOfService,
        payload: Bytes,
        subscribe_filter: Arc<TopicFilter>,
        subscribe_qos: QualityOfService,
    },
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ClientId(pub u64);

pub enum AddClientReceipt {
    Present(SessionState),
    New {
        client_id: ClientId,
        receiver: Receiver<(ClientId, InternalMessage)>,
    },
}

use std::collections::BTreeMap;
use std::sync::Arc;
// TODO: Use async version lock
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Mutex;

use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::NodeId;
use super::RaftTypeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRequest {
    Set { key: Vec<u8>, value: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftResponse {
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RaftStateMachine {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug)]
pub struct RaftSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct RaftStore {
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,
    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<RaftTypeConfig>>>,
    /// The Raft state machine.
    pub state_machine: RwLock<RaftStateMachine>,
    /// The current granted vote.
    vote: RwLock<Option<Vote<NodeId>>>,
    snapshot_idx: Arc<Mutex<u64>>,
    current_snapshot: RwLock<Option<RaftSnapshot>>,

    // custom fields
    pub leader: RwLock<Option<(NodeId, BasicNode)>>,
    pub id: NodeId,
    pub node: BasicNode,
}

impl RaftStore {
    pub fn new(id: NodeId, node: BasicNode) -> RaftStore {
        RaftStore {
            id,
            node,
            last_purged_log_id: Default::default(),
            log: Default::default(),
            state_machine: Default::default(),
            vote: Default::default(),
            snapshot_idx: Default::default(),
            current_snapshot: Default::default(),
            leader: Default::default(),
        }
    }
}

#[async_trait]
impl RaftLogReader<RaftTypeConfig> for Arc<RaftStore> {
    async fn get_log_state(&mut self) -> Result<LogState<RaftTypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let last = log.iter().next_back().map(|(_, ent)| ent.log_id);

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RaftTypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let response = log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }
}

#[async_trait]
impl RaftSnapshotBuilder<RaftTypeConfig, Cursor<Vec<u8>>> for Arc<RaftStore> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<NodeId, BasicNode, Cursor<Vec<u8>>>, StorageError<NodeId>> {
        log::info!("@@ build_snapshot() @@");
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = rmp_serde::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
        }

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = RaftSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<RaftTypeConfig> for Arc<RaftStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        log::info!("save_vote");
        let mut v = self.vote.write().await;
        *v = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        log::info!("read_vote");
        Ok(*self.vote.read().await)
    }

    async fn append_to_log(
        &mut self,
        entries: &[&Entry<RaftTypeConfig>],
    ) -> Result<(), StorageError<NodeId>> {
        log::info!("append_to_log: {:?}", entries);
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, (*entry).clone());
        }
        log::info!("append_to_log success");
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        log::info!("delete_conflict_logs_since()");
        let mut log = self.log.write().await;
        let keys = log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            log.remove(&key);
        }

        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        log::info!("purge_logs_upto()");
        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let mut log = self.log.write().await;

            let keys = log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        log::info!("last_applied_state()");
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<RaftTypeConfig>],
    ) -> Result<Vec<RaftResponse>, StorageError<NodeId>> {
        log::info!("apply_to_state_machine(): {:?}", entries);
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(RaftResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    RaftRequest::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(RaftResponse {
                            value: Some(value.clone()),
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(RaftResponse { value: None })
                }
            };
        }

        if let Some(leader_id) = entries.last().map(|entry| entry.log_id.leader_id.node_id) {
            let mut leader = self.leader.write().await;
            *leader = sm
                .last_membership
                .membership()
                .get_node(&leader_id)
                .cloned()
                .map(|node| (leader_id, node));
            log::info!("leader: {} => {:?}", leader_id, leader);
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<NodeId>> {
        log::info!("begin_receiving_snapshot()");
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        log::info!("install_snapshot()");
        let new_snapshot = RaftSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: RaftStateMachine = rmp_serde::from_slice(&new_snapshot.data)
                .map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NodeId, BasicNode, Self::SnapshotData>>, StorageError<NodeId>> {
        log::info!("get_current_snapshot()");
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        log::info!("get_log_reader()");
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        log::info!("get_snapshot_builder()");
        self.clone()
    }
}

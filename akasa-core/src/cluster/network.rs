use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;

use anyerror::AnyError;
use async_trait::async_trait;
use tonic::{transport::Channel, Request, Response, Status};

use super::convert::membership_to_bin;
use super::proto::{self, akasa_client::AkasaClient, akasa_server::Akasa};
use super::{AkasaRaft, NodeId, RaftResponse, RaftStore, RaftTypeConfig};

pub struct Network {}

pub struct NetworkConnection {
    // pub(crate) owner: Network,
    pub(crate) client: Option<AkasaClient<Channel>>,
    target: NodeId,
    target_node: BasicNode,
}

impl NetworkConnection {
    async fn get_client(&mut self) -> Result<&mut AkasaClient<Channel>, NetworkError> {
        if self.client.is_none() {
            log::info!("grpc connecting to: {}, {}", self.target, self.target_node);
            // TODO: add http in addr
            let result = AkasaClient::connect(format!("http://{}", self.target_node.addr)).await;
            if let Err(err) = result.as_ref() {
                log::info!("connect to {} error: {}", self.target_node, err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                log::info!("connect to {} success", self.target_node);
            }
            self.client = Some(result.map_err(|err| NetworkError::new(&err))?);
        }
        Ok(self.client.as_mut().unwrap())
    }
}

#[async_trait]
impl RaftNetworkFactory<RaftTypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            // owner: Network {},
            client: None,
            target,
            target_node: node.clone(),
        }
    }
}

#[async_trait]
impl RaftNetwork<RaftTypeConfig> for NetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<RaftTypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let client = self.get_client().await?;
        let resp = client
            .raft_append(Request::new(req.clone().into()))
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let (entries_opt, code, error) = resp
            .into_inner()
            .try_into()
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        log::debug!(
            "send append entries: {:?}, code: {}, error: {}",
            req,
            code,
            error
        );
        if code == 0 {
            if let Some(entries) = entries_opt {
                Ok(entries)
            } else {
                Err(RPCError::Network(NetworkError::from(AnyError::error(
                    "empty append entries response".to_string(),
                ))))
            }
        } else {
            Err(RPCError::Network(NetworkError::from(AnyError::error(
                format!("append entries error: code={}, error={}", code, error),
            ))))
        }
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<RaftTypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        log::info!("send install snapshot: {:?}", req);
        let client = self.get_client().await?;
        let resp = client
            .raft_snapshot(Request::new(req.into()))
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let (snapshot_opt, code, error) = resp
            .into_inner()
            .try_into()
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        if code == 0 {
            if let Some(snapshot) = snapshot_opt {
                Ok(snapshot)
            } else {
                Err(RPCError::Network(NetworkError::from(AnyError::error(
                    "empty install snapshot response".to_string(),
                ))))
            }
        } else {
            Err(RPCError::Network(NetworkError::from(AnyError::error(
                format!("install snapshot error: code={}, error={}", code, error),
            ))))
        }
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        log::info!("send vote: {:?}", req);
        let client = self.get_client().await?;
        let resp = client
            .raft_vote(Request::new(req.into()))
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let (vote_opt, code, error) = resp
            .into_inner()
            .try_into()
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        if code == 0 {
            if let Some(vote) = vote_opt {
                Ok(vote)
            } else {
                Err(RPCError::Network(NetworkError::from(AnyError::error(
                    "empty vote response".to_string(),
                ))))
            }
        } else {
            Err(RPCError::Network(NetworkError::from(AnyError::error(
                format!("vote error: code={}, error={}", code, error),
            ))))
        }
    }
}

pub struct GrpcService {
    pub id: NodeId,
    pub addr: String,
    pub raft: AkasaRaft,
    pub store: Arc<RaftStore>,
}

#[tonic::async_trait]
impl Akasa for GrpcService {
    // ==== API ====
    async fn raft_write(
        &self,
        req: Request<proto::RaftWriteReqeust>,
    ) -> Result<Response<proto::RaftWriteReply>, Status> {
        log::info!("[grpc] raft write: {:?}", req);
        let resp = match self.raft.client_write(req.into_inner().into()).await {
            Ok(resp) => resp,
            Err(err) => {
                return Ok(Response::new(
                    (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                ));
            }
        };
        Ok(Response::new((resp.data, None, 0, String::new()).into()))
    }
    async fn raft_read(
        &self,
        req: Request<proto::RaftReadRequest>,
    ) -> Result<Response<proto::RaftReadReply>, Status> {
        log::info!("[grpc] raft read: {:?}", req);
        let state_machine = self.store.state_machine.read().await;
        let key = req.into_inner().key;
        let value = state_machine.data.get(&key).cloned();
        Ok(Response::new(proto::RaftReadReply { value }))
    }
    async fn raft_consistent_read(
        &self,
        req: Request<proto::RaftConsistentReadRequest>,
    ) -> Result<Response<proto::RaftConsistentReadReply>, Status> {
        log::info!("[grpc] raft consistent read: {:?}", req);
        match self.raft.is_leader().await {
            Ok(_) => {
                let state_machine = self.store.state_machine.read().await;
                let key = req.into_inner().key;
                let value = state_machine.data.get(&key).cloned();
                Ok(Response::new(
                    (RaftResponse { value }, 0, String::new()).into(),
                ))
            }
            Err(e) => Ok(Response::new(
                (RaftResponse { value: None }, -1, e.to_string()).into(),
            )),
        }
    }

    // ==== Management ====
    async fn raft_add_learner(
        &self,
        req: Request<proto::RaftAddLearnerRequest>,
    ) -> Result<Response<proto::RaftWriteReply>, Status> {
        log::info!("[grpc] raft add learner: {:?}", req);
        let (node_id, addr, update_addr) = req.into_inner().into();
        let new_node = BasicNode { addr };
        let resp = match self.raft.add_learner(node_id, new_node.clone(), true).await {
            Ok(resp) => resp,
            Err(err) => {
                return Ok(Response::new(
                    (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                ));
            }
        };
        // TODO: check https://github.com/datafuselabs/openraft/issues/927
        if update_addr {
            let membership = resp.membership.as_ref().expect("membership");
            if membership.get_node(&node_id).map(|node| *node != new_node) == Some(true) {
                let voter_ids: BTreeSet<NodeId> = membership.voter_ids().collect();
                let is_voter = voter_ids.contains(&node_id);
                log::info!(
                    "target node address changed! old={}, is_voter={}",
                    new_node.addr,
                    is_voter
                );
                if is_voter {
                    let voter_ids_subset =
                        membership.voter_ids().filter(|id| *id != node_id).collect();
                    log::info!("voters subset: {:?}", voter_ids_subset);
                    let change = ChangeMembers::ReplaceAllVoters(voter_ids_subset);
                    if let Err(err) = self.raft.change_membership(change, false).await {
                        return Ok(Response::new(
                            (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                        ));
                    }
                    log::info!("removed target node from voters");
                }

                let ids: BTreeSet<NodeId> = [node_id].into_iter().collect();
                let change = ChangeMembers::RemoveNodes(ids);
                if let Err(err) = self.raft.change_membership(change, false).await {
                    return Ok(Response::new(
                        (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                    ));
                }
                log::info!("removed target node from nodes");
                if let Err(err) = self.raft.add_learner(node_id, new_node, true).await {
                    return Ok(Response::new(
                        (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                    ));
                }
                log::info!("added target node to nodes (by add_learner)");
                if is_voter {
                    let change = ChangeMembers::ReplaceAllVoters(voter_ids);
                    if let Err(err) = self.raft.change_membership(change, false).await {
                        return Ok(Response::new(
                            (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                        ));
                    }
                    log::info!("added target node to voters");
                }
            } else {
                log::info!("target node address unchanged!");
            }
        }
        Ok(Response::new(
            (resp.data, resp.membership, 0, String::new()).into(),
        ))
    }
    async fn raft_change_membership(
        &self,
        req: Request<proto::RaftChangeMembershipRequest>,
    ) -> Result<Response<proto::RaftWriteReply>, Status> {
        log::info!("[grpc] raft change membership: {:?}", req);
        let (members, retain): (ChangeMembers<NodeId, BasicNode>, bool) =
            match req.into_inner().try_into() {
                Ok((members, retain)) => {
                    log::info!("members={:?}, retain={}", members, retain);
                    (members, retain)
                }
                Err(err) => {
                    return Ok(Response::new(
                        (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                    ));
                }
            };
        let resp = match self.raft.change_membership(members, retain).await {
            Ok(resp) => resp,
            Err(err) => {
                return Ok(Response::new(
                    (RaftResponse { value: None }, None, -1, err.to_string()).into(),
                ));
            }
        };
        Ok(Response::new(
            (resp.data, resp.membership, 0, String::new()).into(),
        ))
    }
    async fn raft_init(
        &self,
        req: Request<proto::RaftInitRequest>,
    ) -> Result<Response<proto::RaftInitReply>, Status> {
        log::info!("[grpc] raft init: {:?}", req);
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.id,
            BasicNode {
                addr: self.addr.clone(),
            },
        );
        if let Err(err) = self.raft.initialize(nodes).await {
            return Ok(Response::new(proto::RaftInitReply {
                code: -1,
                error: err.to_string(),
            }));
        }
        Ok(Response::new(proto::RaftInitReply {
            code: 0,
            error: String::new(),
        }))
    }
    async fn raft_metrics(
        &self,
        _req: Request<proto::RaftMetricsRequest>,
    ) -> Result<Response<proto::RaftMetricsReply>, Status> {
        log::info!("[grpc] raft metrics");
        let metrics = self.raft.metrics().borrow().clone();
        let output = serde_json::to_string(&metrics).unwrap();
        Ok(Response::new(proto::RaftMetricsReply { metrics: output }))
    }

    // ==== Raft ====
    async fn raft_vote(
        &self,
        req: Request<proto::RaftVoteRequest>,
    ) -> Result<Response<proto::RaftVoteReply>, Status> {
        log::info!("[grpc] raft vote: {:?}", req);
        let vote = match req.into_inner().try_into() {
            Ok(vote) => vote,
            Err(err) => return Err(Status::invalid_argument(err)),
        };
        let resp = match self.raft.vote(vote).await {
            Ok(resp) => (Some(resp), 0, String::new()).into(),
            Err(err) => (None, -1, err.to_string()).into(),
        };
        Ok(Response::new(resp))
    }
    async fn raft_append(
        &self,
        req: Request<proto::RaftAppendRequest>,
    ) -> Result<Response<proto::RaftAppendReply>, Status> {
        log::debug!("[grpc] raft append req: {:?}", req);
        let entries = match req.into_inner().try_into() {
            Ok(entries) => entries,
            Err(err) => {
                log::warn!("convert append request error: {:?}", err);
                return Err(Status::invalid_argument(err));
            }
        };
        log::trace!("[grpc] raft append entries: {:?}", entries);
        let resp = match self.raft.append_entries(entries).await {
            Ok(resp) => (Some(resp), 0, String::new()).into(),
            Err(err) => (None, -1, err.to_string()).into(),
        };
        log::debug!("[grpc] raft append resp: {:?}", resp);
        Ok(Response::new(resp))
    }
    async fn raft_snapshot(
        &self,
        req: Request<proto::RaftSnapshotRequest>,
    ) -> Result<Response<proto::RaftSnapshotReply>, Status> {
        log::info!("[grpc] raft snapshot: {:?}", req);
        let snapshot = match req.into_inner().try_into() {
            Ok(snapshot) => snapshot,
            Err(err) => return Err(Status::invalid_argument(err)),
        };
        let resp = match self.raft.install_snapshot(snapshot).await {
            Ok(resp) => (Some(resp), 0, String::new()).into(),
            Err(err) => (None, -1, err.to_string()).into(),
        };
        Ok(Response::new(resp))
    }

    async fn ext_raft_meta(
        &self,
        req: Request<proto::MetaRequest>,
    ) -> Result<Response<proto::MetaReply>, Status> {
        log::info!("[grpc] ext raft meta: {:?}", req);
        let mut data_membership = Vec::new();
        let state_machine = self.store.state_machine.read().await;
        membership_to_bin(
            state_machine.last_membership.membership(),
            &mut data_membership,
        );
        let leader_id = state_machine
            .last_membership
            .log_id()
            .map(|log| log.leader_id.into());
        let resp = proto::MetaReply {
            leader_id,
            membership: data_membership,
        };
        Ok(Response::new(resp))
    }
}

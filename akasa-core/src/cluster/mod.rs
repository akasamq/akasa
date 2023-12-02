mod client;
mod convert;
mod network;
mod store;

pub mod proto {
    tonic::include_proto!("akasa");
}

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Raft;
use openraft::{Config, SnapshotPolicy};
use tonic::transport::Server;

use client::Client;
use network::{GrpcService, Network};
use proto::akasa_server::AkasaServer;
pub use store::{RaftRequest, RaftResponse, RaftStateMachine, RaftStore};

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub RaftTypeConfig: D = RaftRequest, R = RaftResponse, NodeId = NodeId, Node = BasicNode
);

pub type AkasaRaft = Raft<RaftTypeConfig, Network, Arc<RaftStore>>;

pub mod typ {
    use openraft::BasicNode;

    use super::NodeId;
    use super::RaftTypeConfig;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<RaftTypeConfig>;
}

// NOTE:
//   * openraft heartbeat is not reliable (may not work)

async fn start_node(id: u64, addr: SocketAddr, leader_addr: Option<SocketAddr>) {
    let config = Config {
        heartbeat_interval: 300,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_in_snapshot_log_to_keep: 10,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(12),
        ..Default::default()
    };
    let config = Arc::new(config.validate().unwrap());
    let store = Arc::new(RaftStore::new(id, BasicNode::new(addr)));
    let network = Network {};

    if let Some(leader_addr) = leader_addr {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let mut client = Client::new(leader_addr).await.unwrap();
            log::info!("sending add learner to leader node: {:?}", leader_addr);
            let _membership = client.add_learner(id, addr, true).await.unwrap();
            log::info!("send add learner to leader node: {:?} success", leader_addr);
        });
    }

    let raft = Raft::new(id, config.clone(), network, store.clone())
        .await
        .unwrap();

    let akasa = GrpcService {
        id,
        addr: addr.to_string(),
        raft,
        store,
    };
    log::info!("Start grpc server: [{}] => {}", id, addr);
    Server::builder()
        .add_service(AkasaServer::new(akasa))
        .serve(addr)
        .await
        .unwrap();
}

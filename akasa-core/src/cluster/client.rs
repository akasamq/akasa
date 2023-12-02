use std::net::SocketAddr;

use anyhow::Result;
use openraft::{BasicNode, ChangeMembers, LeaderId, Membership};
use tonic::{transport::Channel, Request};

use super::{
    convert::try_from_membership_bin,
    proto::{self, akasa_client::AkasaClient},
    NodeId,
};

pub struct Client {
    inner: AkasaClient<Channel>,
}

impl Client {
    pub async fn new(addr: SocketAddr) -> Result<Client> {
        let inner = AkasaClient::connect(format!("http://{}", addr)).await?;
        Ok(Client { inner })
    }

    pub async fn init(&mut self) -> Result<()> {
        let resp = self
            .inner
            .raft_init(Request::new(proto::RaftInitRequest {}))
            .await?;
        log::info!("[Response]: raft init: {:?}", resp);
        Ok(())
    }

    pub async fn metrics(&mut self) -> Result<()> {
        let resp = self
            .inner
            .raft_metrics(Request::new(proto::RaftMetricsRequest {}))
            .await?;
        log::info!("[Response]: raft metrics: {}", resp.get_ref().metrics);
        Ok(())
    }

    pub async fn add_learner(
        &mut self,
        target_id: u64,
        target_addr: SocketAddr,
        update_addr: bool,
    ) -> Result<Membership<NodeId, BasicNode>> {
        let resp = self
            .inner
            .raft_add_learner(Request::new(proto::RaftAddLearnerRequest {
                node_id: target_id,
                address: target_addr.to_string(),
                update_addr,
            }))
            .await?;
        log::info!("[Response]: raft add learner: {:?}", resp);
        let (_, membership, _, _) = resp.into_inner().try_into().unwrap();
        Ok(membership.unwrap())
    }

    pub async fn raft_meta(&mut self) -> Result<(Option<NodeId>, Membership<NodeId, BasicNode>)> {
        let resp = self
            .inner
            .ext_raft_meta(Request::new(proto::MetaRequest {}))
            .await?
            .into_inner();
        let leader_id = resp
            .leader_id
            .map(LeaderId::try_from)
            .transpose()?
            .map(|id| id.node_id);
        let membership = try_from_membership_bin(&resp.membership)?;
        log::info!(
            "[Response]: raft get meta: leader_id={:?}, membership={:?}",
            leader_id,
            membership
        );
        Ok((leader_id, membership))
    }

    pub async fn change_membership(
        &mut self,
        members: ChangeMembers<NodeId, BasicNode>,
        retain: bool,
    ) -> Result<()> {
        let resp = self
            .inner
            .raft_change_membership(Request::new((members, retain).into()))
            .await?;
        log::info!("[Response]: raft change membership: {:?}", resp);
        Ok(())
    }

    pub async fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let resp = self
            .inner
            .raft_write(Request::new(proto::RaftWriteReqeust { key, value }))
            .await?;
        log::info!("[Response]: raft write: {:?}", resp);
        Ok(())
    }

    pub async fn read(&mut self, key: Vec<u8>) -> Result<()> {
        let resp = self
            .inner
            .raft_read(Request::new(proto::RaftReadRequest { key }))
            .await?;
        log::info!("[Response]: raft read: {:?}", resp);
        Ok(())
    }
}

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use openraft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, ChangeMembers, Entry, EntryPayload, LeaderId, LogId, Membership, SnapshotMeta,
    StoredMembership, Vote,
};
use thiserror::Error;

use super::{proto, NodeId, RaftRequest, RaftResponse, RaftTypeConfig};

#[derive(Error, Debug)]
pub enum ConvertError {
    NoneField(&'static str),
    InvalidEntryPayload,
    InvalidAppendEntriesResponse,
    InvalidChangeMembers,
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoneField(name) => write!(f, "none field: {}", name),
            Self::InvalidEntryPayload => write!(f, "invalid entry payload"),
            Self::InvalidAppendEntriesResponse => write!(f, "invalid append entries response"),
            Self::InvalidChangeMembers => write!(f, "invalid change members type"),
        }
    }
}

impl From<ConvertError> for String {
    fn from(err: ConvertError) -> String {
        err.to_string()
    }
}

impl From<Vote<NodeId>> for proto::Vote {
    fn from(src: Vote<NodeId>) -> proto::Vote {
        proto::Vote {
            leader_id: Some(src.leader_id.into()),
            committed: src.committed,
        }
    }
}
impl TryFrom<proto::Vote> for Vote<NodeId> {
    type Error = ConvertError;
    fn try_from(src: proto::Vote) -> Result<Self, Self::Error> {
        let leader_id = src.leader_id.ok_or(ConvertError::NoneField("leader_id"))?;
        Ok(Self {
            leader_id: leader_id.into(),
            committed: src.committed,
        })
    }
}

impl From<LeaderId<NodeId>> for proto::LeaderId {
    fn from(src: LeaderId<NodeId>) -> proto::LeaderId {
        proto::LeaderId {
            term: src.term,
            node_id: src.node_id,
        }
    }
}
impl From<proto::LeaderId> for LeaderId<NodeId> {
    fn from(src: proto::LeaderId) -> LeaderId<NodeId> {
        LeaderId {
            term: src.term,
            node_id: src.node_id,
        }
    }
}

impl From<LogId<NodeId>> for proto::LogId {
    fn from(src: LogId<NodeId>) -> proto::LogId {
        proto::LogId {
            leader_id: Some(src.leader_id.into()),
            index: src.index,
        }
    }
}
impl TryFrom<proto::LogId> for LogId<NodeId> {
    type Error = ConvertError;
    fn try_from(src: proto::LogId) -> Result<Self, Self::Error> {
        let leader_id = src.leader_id.ok_or(ConvertError::NoneField("leader_id"))?;
        Ok(LogId {
            leader_id: leader_id.into(),
            index: src.index,
        })
    }
}

impl From<Entry<RaftTypeConfig>> for proto::Entry {
    fn from(src: Entry<RaftTypeConfig>) -> proto::Entry {
        proto::Entry {
            log_id: Some(src.log_id.into()),
            payload: payload_to_bin(src.payload),
        }
    }
}
impl TryFrom<proto::Entry> for Entry<RaftTypeConfig> {
    type Error = ConvertError;
    fn try_from(src: proto::Entry) -> Result<Self, Self::Error> {
        Ok(Self {
            log_id: src
                .log_id
                .ok_or(ConvertError::NoneField("log_id"))?
                .try_into()?,
            payload: try_from_payload_bin(src.payload)?,
        })
    }
}

pub(crate) fn membership_to_bin(membership: &Membership<NodeId, BasicNode>, data: &mut Vec<u8>) {
    let config = membership.get_joint_config();
    data.extend((config.len() as u32).to_le_bytes());
    for node_id_set in config {
        data.extend((node_id_set.len() as u32).to_le_bytes());
        for node_id in node_id_set {
            data.extend(node_id.to_le_bytes());
        }
    }
    for (node_id, node) in membership.nodes() {
        data.extend(node_id.to_le_bytes());
        data.extend((node.addr.len() as u16).to_le_bytes());
        data.extend(node.addr.as_bytes());
    }
}

pub(crate) fn try_from_membership_bin(
    body: &[u8],
) -> Result<Membership<NodeId, BasicNode>, ConvertError> {
    let config_len = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| ConvertError::InvalidEntryPayload)?,
    ) as usize;
    if body.len() < 4 + config_len * 4 {
        return Err(ConvertError::InvalidEntryPayload);
    }
    let mut config = Vec::with_capacity(config_len);
    let mut offset = 4;
    for _ in 0..config_len {
        let id_set_len = u32::from_le_bytes(
            body.get(offset..offset + 4)
                .ok_or(ConvertError::InvalidEntryPayload)?
                .try_into()
                .map_err(|_| ConvertError::InvalidEntryPayload)?,
        ) as usize;
        offset += 4;
        let mut node_set = BTreeSet::new();
        for _ in 0..id_set_len {
            let node_id = u64::from_le_bytes(
                body.get(offset..offset + 8)
                    .ok_or(ConvertError::InvalidEntryPayload)?
                    .try_into()
                    .map_err(|_| ConvertError::InvalidEntryPayload)?,
            );
            offset += 8;
            node_set.insert(node_id);
        }
        config.push(node_set);
    }

    let mut nodes = BTreeMap::new();
    while offset < body.len() {
        let node_id = u64::from_le_bytes(
            body.get(offset..offset + 8)
                .ok_or(ConvertError::InvalidEntryPayload)?
                .try_into()
                .map_err(|_| ConvertError::InvalidEntryPayload)?,
        );
        offset += 8;
        let addr_len = u16::from_le_bytes(
            body.get(offset..offset + 2)
                .ok_or(ConvertError::InvalidEntryPayload)?
                .try_into()
                .map_err(|_| ConvertError::InvalidEntryPayload)?,
        ) as usize;
        offset += 2;
        let addr = String::from_utf8(
            body.get(offset..offset + addr_len)
                .ok_or(ConvertError::InvalidEntryPayload)?
                .to_vec(),
        )
        .map_err(|_| ConvertError::InvalidEntryPayload)?;
        offset += addr_len;
        let node = BasicNode { addr };
        nodes.insert(node_id, node);
    }
    Ok(Membership::new(config, nodes))
}

fn payload_to_bin(payload: EntryPayload<RaftTypeConfig>) -> Vec<u8> {
    match payload {
        EntryPayload::Blank => vec![0],
        EntryPayload::Normal(RaftRequest::Set { key, value }) => {
            let mut data = Vec::with_capacity(1 + 4 + key.len() + value.len());
            data.push(1);
            data.extend((key.len() as u32).to_le_bytes());
            data.extend(key);
            data.extend(value);
            data
        }
        EntryPayload::Membership(membership) => {
            let mut data = Vec::with_capacity(128);
            data.push(2);
            membership_to_bin(&membership, &mut data);
            data
        }
    }
}
fn try_from_payload_bin(bin: Vec<u8>) -> Result<EntryPayload<RaftTypeConfig>, ConvertError> {
    if bin.is_empty() {
        return Err(ConvertError::InvalidEntryPayload);
    }
    let head = bin[0];
    let body = &bin[1..];
    match head {
        0 if body.is_empty() => Ok(EntryPayload::Blank),
        1 if body.len() >= 4 => {
            let key_len = u32::from_le_bytes(
                body[0..4]
                    .try_into()
                    .map_err(|_| ConvertError::InvalidEntryPayload)?,
            ) as usize;
            if body.len() < 4 + key_len {
                return Err(ConvertError::InvalidEntryPayload);
            }
            let key = body[4..4 + key_len].to_vec();
            let value = body[4 + key_len..].to_vec();
            Ok(EntryPayload::Normal(RaftRequest::Set { key, value }))
        }
        2 if body.len() >= 4 => try_from_membership_bin(body).map(EntryPayload::Membership),
        _ => Err(ConvertError::InvalidEntryPayload),
    }
}

// ==== [Request -> Reply] ====

// === Write ===
impl From<RaftRequest> for proto::RaftWriteReqeust {
    fn from(src: RaftRequest) -> proto::RaftWriteReqeust {
        let (key, value) = match src {
            RaftRequest::Set { key, value } => (key, value),
        };
        proto::RaftWriteReqeust { key, value }
    }
}
impl From<proto::RaftWriteReqeust> for RaftRequest {
    fn from(src: proto::RaftWriteReqeust) -> RaftRequest {
        RaftRequest::Set {
            key: src.key,
            value: src.value,
        }
    }
}

impl
    From<(
        RaftResponse,
        Option<Membership<NodeId, BasicNode>>,
        i32,
        String,
    )> for proto::RaftWriteReply
{
    fn from(
        (src, membership, code, error): (
            RaftResponse,
            Option<Membership<NodeId, BasicNode>>,
            i32,
            String,
        ),
    ) -> proto::RaftWriteReply {
        let mut membership_data = Vec::new();
        if let Some(membership) = membership.as_ref() {
            membership_to_bin(membership, &mut membership_data);
        }
        proto::RaftWriteReply {
            value: src.value,
            membership: membership.map(|_| membership_data),
            code,
            error,
        }
    }
}
impl TryFrom<proto::RaftWriteReply>
    for (
        RaftResponse,
        Option<Membership<NodeId, BasicNode>>,
        i32,
        String,
    )
{
    type Error = ConvertError;
    fn try_from(src: proto::RaftWriteReply) -> Result<Self, Self::Error> {
        let membership = src
            .membership
            .map(|data| try_from_membership_bin(&data))
            .transpose()?;
        Ok((
            RaftResponse { value: src.value },
            membership,
            src.code,
            src.error,
        ))
    }
}

// === Raad ===
impl From<Vec<u8>> for proto::RaftReadRequest {
    fn from(src: Vec<u8>) -> proto::RaftReadRequest {
        proto::RaftReadRequest { key: src }
    }
}
impl From<proto::RaftReadRequest> for Vec<u8> {
    fn from(src: proto::RaftReadRequest) -> Vec<u8> {
        src.key
    }
}

impl From<RaftResponse> for proto::RaftReadReply {
    fn from(src: RaftResponse) -> proto::RaftReadReply {
        proto::RaftReadReply { value: src.value }
    }
}
impl From<proto::RaftReadReply> for RaftResponse {
    fn from(src: proto::RaftReadReply) -> RaftResponse {
        RaftResponse { value: src.value }
    }
}

// === ConsistentRead ===
impl From<Vec<u8>> for proto::RaftConsistentReadRequest {
    fn from(src: Vec<u8>) -> proto::RaftConsistentReadRequest {
        proto::RaftConsistentReadRequest { key: src }
    }
}
impl From<proto::RaftConsistentReadRequest> for Vec<u8> {
    fn from(src: proto::RaftConsistentReadRequest) -> Vec<u8> {
        src.key
    }
}

impl From<(RaftResponse, i32, String)> for proto::RaftConsistentReadReply {
    fn from((src, code, error): (RaftResponse, i32, String)) -> proto::RaftConsistentReadReply {
        proto::RaftConsistentReadReply {
            value: src.value,
            code,
            error,
        }
    }
}
impl From<proto::RaftConsistentReadReply> for (RaftResponse, i32, String) {
    fn from(src: proto::RaftConsistentReadReply) -> (RaftResponse, i32, String) {
        (RaftResponse { value: src.value }, src.code, src.error)
    }
}

// === AddLearner ===
impl From<(NodeId, String, bool)> for proto::RaftAddLearnerRequest {
    fn from(
        (node_id, address, update_addr): (NodeId, String, bool),
    ) -> proto::RaftAddLearnerRequest {
        proto::RaftAddLearnerRequest {
            node_id,
            address,
            update_addr,
        }
    }
}
impl From<proto::RaftAddLearnerRequest> for (NodeId, String, bool) {
    fn from(src: proto::RaftAddLearnerRequest) -> (NodeId, String, bool) {
        (src.node_id, src.address, src.update_addr)
    }
}

// === ChangeMembership ===
impl From<(ChangeMembers<NodeId, BasicNode>, bool)> for proto::RaftChangeMembershipRequest {
    fn from(
        (members, retain): (ChangeMembers<NodeId, BasicNode>, bool),
    ) -> proto::RaftChangeMembershipRequest {
        fn serialize_ids(ids: BTreeSet<NodeId>) -> Vec<u8> {
            let mut data = Vec::new();
            for id in ids {
                data.extend(id.to_le_bytes());
            }
            data
        }
        fn serialize_nodes(nodes: BTreeMap<NodeId, BasicNode>) -> Vec<u8> {
            let mut data = Vec::new();
            for (id, node) in nodes {
                data.extend(id.to_le_bytes());
                data.extend((node.addr.len() as u32).to_le_bytes());
                data.extend(node.addr.as_bytes());
            }
            data
        }
        let (ty, data) = match members {
            ChangeMembers::AddVoterIds(ids) => {
                (proto::ChangeMembers::AddVoterIds, serialize_ids(ids))
            }
            ChangeMembers::AddVoters(nodes) => {
                (proto::ChangeMembers::AddVoters, serialize_nodes(nodes))
            }
            ChangeMembers::RemoveVoters(ids) => {
                (proto::ChangeMembers::RemoveVoters, serialize_ids(ids))
            }
            ChangeMembers::ReplaceAllVoters(ids) => {
                (proto::ChangeMembers::ReplaceAllVoters, serialize_ids(ids))
            }
            ChangeMembers::AddNodes(nodes) => {
                (proto::ChangeMembers::AddNodes, serialize_nodes(nodes))
            }
            ChangeMembers::RemoveNodes(ids) => {
                (proto::ChangeMembers::RemoveNodes, serialize_ids(ids))
            }
            ChangeMembers::ReplaceAllNodes(nodes) => (
                proto::ChangeMembers::ReplaceAllNodes,
                serialize_nodes(nodes),
            ),
        };
        proto::RaftChangeMembershipRequest {
            ty: ty as i32,
            data,
            retain,
        }
    }
}

impl TryFrom<proto::RaftChangeMembershipRequest> for (ChangeMembers<NodeId, BasicNode>, bool) {
    type Error = ConvertError;
    fn try_from(src: proto::RaftChangeMembershipRequest) -> Result<Self, Self::Error> {
        fn deserialize_ids(data: &[u8]) -> Result<BTreeSet<NodeId>, ConvertError> {
            let mut ids = BTreeSet::new();
            let mut idx = 0;
            while idx < data.len() {
                ids.insert(u64::from_le_bytes(
                    data.get(idx..idx + 8)
                        .ok_or(ConvertError::InvalidChangeMembers)?
                        .try_into()
                        .map_err(|_| ConvertError::InvalidChangeMembers)?,
                ));
                idx += 8;
            }
            Ok(ids)
        }
        fn deserialize_nodes(data: &[u8]) -> Result<BTreeMap<NodeId, BasicNode>, ConvertError> {
            let mut nodes = BTreeMap::new();
            let mut idx = 0;
            while idx < data.len() {
                let id = u64::from_le_bytes(
                    data.get(idx..idx + 8)
                        .ok_or(ConvertError::InvalidChangeMembers)?
                        .try_into()
                        .map_err(|_| ConvertError::InvalidChangeMembers)?,
                );
                idx += 8;
                let addr_len = u32::from_le_bytes(
                    data.get(idx..idx + 4)
                        .ok_or(ConvertError::InvalidChangeMembers)?
                        .try_into()
                        .map_err(|_| ConvertError::InvalidChangeMembers)?,
                ) as usize;
                idx += 4;
                let addr = String::from_utf8(
                    data.get(idx..idx + addr_len)
                        .ok_or(ConvertError::InvalidChangeMembers)?
                        .to_vec(),
                )
                .map_err(|_| ConvertError::InvalidChangeMembers)?;
                idx += addr_len;
                nodes.insert(id, BasicNode { addr });
            }
            if idx != data.len() {
                return Err(ConvertError::InvalidChangeMembers);
            }
            Ok(nodes)
        }
        let members = match src.ty {
            0 => ChangeMembers::AddVoterIds(deserialize_ids(&src.data)?),
            1 => ChangeMembers::AddVoters(deserialize_nodes(&src.data)?),
            2 => ChangeMembers::RemoveVoters(deserialize_ids(&src.data)?),
            3 => ChangeMembers::ReplaceAllVoters(deserialize_ids(&src.data)?),
            4 => ChangeMembers::AddNodes(deserialize_nodes(&src.data)?),
            5 => ChangeMembers::RemoveNodes(deserialize_ids(&src.data)?),
            6 => ChangeMembers::ReplaceAllNodes(deserialize_nodes(&src.data)?),
            _ => return Err(ConvertError::InvalidChangeMembers),
        };
        Ok((members, src.retain))
    }
}

// === Vote ===
impl From<VoteRequest<NodeId>> for proto::RaftVoteRequest {
    fn from(src: VoteRequest<NodeId>) -> proto::RaftVoteRequest {
        proto::RaftVoteRequest {
            vote: Some(src.vote.into()),
            last_log_id: src.last_log_id.map(Into::into),
        }
    }
}
impl TryFrom<proto::RaftVoteRequest> for VoteRequest<NodeId> {
    type Error = ConvertError;
    fn try_from(src: proto::RaftVoteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: src
                .vote
                .ok_or(ConvertError::NoneField("vote"))?
                .try_into()?,
            last_log_id: src
                .last_log_id
                .map(|log_id| log_id.try_into())
                .transpose()?,
        })
    }
}

impl From<VoteResponse<NodeId>> for proto::VoteResponse {
    fn from(src: VoteResponse<NodeId>) -> proto::VoteResponse {
        proto::VoteResponse {
            vote: Some(src.vote.into()),
            vote_granted: src.vote_granted,
            last_log_id: src.last_log_id.map(Into::into),
        }
    }
}
impl TryFrom<proto::VoteResponse> for VoteResponse<NodeId> {
    type Error = ConvertError;
    fn try_from(src: proto::VoteResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: src
                .vote
                .ok_or(ConvertError::NoneField("vote"))?
                .try_into()?,
            vote_granted: src.vote_granted,
            last_log_id: src
                .last_log_id
                .map(|log_id| log_id.try_into())
                .transpose()?,
        })
    }
}

impl From<(Option<VoteResponse<NodeId>>, i32, String)> for proto::RaftVoteReply {
    fn from(
        (src, code, error): (Option<VoteResponse<NodeId>>, i32, String),
    ) -> proto::RaftVoteReply {
        proto::RaftVoteReply {
            resp: src.map(Into::into),
            code,
            error,
        }
    }
}
impl TryFrom<proto::RaftVoteReply> for (Option<VoteResponse<NodeId>>, i32, String) {
    type Error = ConvertError;
    fn try_from(src: proto::RaftVoteReply) -> Result<Self, Self::Error> {
        let resp = if src.code == 0 {
            Some(
                src.resp
                    .ok_or(ConvertError::NoneField("vote.resp"))?
                    .try_into()?,
            )
        } else {
            None
        };
        Ok((resp, src.code, src.error))
    }
}

// === Append ===
impl From<AppendEntriesRequest<RaftTypeConfig>> for proto::RaftAppendRequest {
    fn from(src: AppendEntriesRequest<RaftTypeConfig>) -> proto::RaftAppendRequest {
        proto::RaftAppendRequest {
            vote: Some(src.vote.into()),
            prev_log_id: src.prev_log_id.map(Into::into),
            entries: src.entries.into_iter().map(Into::into).collect(),
            leader_commit: src.leader_commit.map(Into::into),
        }
    }
}
impl TryFrom<proto::RaftAppendRequest> for AppendEntriesRequest<RaftTypeConfig> {
    type Error = ConvertError;
    fn try_from(src: proto::RaftAppendRequest) -> Result<Self, Self::Error> {
        Ok(AppendEntriesRequest {
            vote: src
                .vote
                .ok_or(ConvertError::NoneField("vote"))?
                .try_into()?,
            prev_log_id: src
                .prev_log_id
                .map(|log_id| log_id.try_into())
                .transpose()?,
            entries: src
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Entry<RaftTypeConfig>>, ConvertError>>()?,
            leader_commit: src
                .leader_commit
                .map(|log_id| log_id.try_into())
                .transpose()?,
        })
    }
}

impl From<(Option<AppendEntriesResponse<NodeId>>, i32, String)> for proto::RaftAppendReply {
    fn from(
        (src, code, error): (Option<AppendEntriesResponse<NodeId>>, i32, String),
    ) -> proto::RaftAppendReply {
        let (resp, vote) = match src {
            Some(AppendEntriesResponse::Success) => {
                (Some(proto::AppendEntriesResponse::Success as i32), None)
            }
            Some(AppendEntriesResponse::Conflict) => {
                (Some(proto::AppendEntriesResponse::Conflict as i32), None)
            }
            Some(AppendEntriesResponse::HigherVote(vote)) => (
                Some(proto::AppendEntriesResponse::Conflict as i32),
                Some(vote.into()),
            ),
            None => (None, None),
        };
        proto::RaftAppendReply {
            resp,
            vote,
            code,
            error,
        }
    }
}
impl TryFrom<proto::RaftAppendReply> for (Option<AppendEntriesResponse<NodeId>>, i32, String) {
    type Error = ConvertError;
    fn try_from(src: proto::RaftAppendReply) -> Result<Self, Self::Error> {
        let resp = match (src.resp, src.vote) {
            (Some(0), None) => Some(AppendEntriesResponse::Success),
            (Some(1), None) => Some(AppendEntriesResponse::Conflict),
            (Some(2), Some(vote)) => Some(AppendEntriesResponse::HigherVote(vote.try_into()?)),
            _ => return Err(ConvertError::InvalidAppendEntriesResponse),
        };
        Ok((resp, src.code, src.error))
    }
}

// === Snapshot ===
impl From<SnapshotMeta<NodeId, BasicNode>> for proto::SnapshotMeta {
    fn from(src: SnapshotMeta<NodeId, BasicNode>) -> proto::SnapshotMeta {
        let mut membership_bin = Vec::new();
        membership_to_bin(src.last_membership.membership(), &mut membership_bin);
        proto::SnapshotMeta {
            last_log_id: src.last_log_id.map(Into::into),
            membership_log_id: (*src.last_membership.log_id()).map(Into::into),
            membership: membership_bin,
            snapshot_id: src.snapshot_id,
        }
    }
}
impl TryFrom<proto::SnapshotMeta> for SnapshotMeta<NodeId, BasicNode> {
    type Error = ConvertError;
    fn try_from(src: proto::SnapshotMeta) -> Result<Self, Self::Error> {
        let last_membership = StoredMembership::new(
            src.membership_log_id.map(TryInto::try_into).transpose()?,
            try_from_membership_bin(&src.membership)?,
        );
        Ok(SnapshotMeta {
            last_log_id: src.last_log_id.map(TryInto::try_into).transpose()?,
            last_membership,
            snapshot_id: src.snapshot_id,
        })
    }
}
impl From<InstallSnapshotRequest<RaftTypeConfig>> for proto::RaftSnapshotRequest {
    fn from(src: InstallSnapshotRequest<RaftTypeConfig>) -> proto::RaftSnapshotRequest {
        proto::RaftSnapshotRequest {
            vote: Some(src.vote.into()),
            meta: Some(src.meta.into()),
            offset: src.offset,
            data: src.data,
            done: src.done,
        }
    }
}
impl TryFrom<proto::RaftSnapshotRequest> for InstallSnapshotRequest<RaftTypeConfig> {
    type Error = ConvertError;
    fn try_from(src: proto::RaftSnapshotRequest) -> Result<Self, Self::Error> {
        let vote = src
            .vote
            .ok_or(ConvertError::NoneField("vote"))?
            .try_into()?;
        let src_meta = src.meta.ok_or(ConvertError::NoneField("meta"))?;
        Ok(InstallSnapshotRequest {
            vote,
            meta: src_meta.try_into()?,
            offset: src.offset,
            data: src.data,
            done: src.done,
        })
    }
}

impl From<(Option<InstallSnapshotResponse<NodeId>>, i32, String)> for proto::RaftSnapshotReply {
    fn from(
        (src, code, error): (Option<InstallSnapshotResponse<NodeId>>, i32, String),
    ) -> proto::RaftSnapshotReply {
        proto::RaftSnapshotReply {
            resp: src.map(|r| r.vote.into()),
            code,
            error,
        }
    }
}

impl TryFrom<proto::RaftSnapshotReply> for (Option<InstallSnapshotResponse<NodeId>>, i32, String) {
    type Error = ConvertError;
    fn try_from(src: proto::RaftSnapshotReply) -> Result<Self, Self::Error> {
        let resp = src
            .resp
            .map(TryInto::try_into)
            .transpose()?
            .map(|vote| InstallSnapshotResponse { vote });
        Ok((resp, src.code, src.error))
    }
}

use crate::{
    BytesOps, ErrorCode, Result, TagBuffer, ToArray, TryExtract, VarInt,
};
use bytes::BufMut;
use newtype_macro::newtype;
use std::ops::Deref;
use uuid::Uuid;

#[newtype]
pub struct Leader(NodeId);

#[newtype]
pub struct LeaderEpoch(u32);

#[newtype]
pub struct PartitionEpoch(u32);

#[newtype]
pub struct Directory(Uuid);

#[newtype]
pub struct ReplicaNode(NodeId);

#[newtype]
pub struct ISRNode(NodeId);

#[newtype]
pub struct EligibleLeaderReplicas(NodeId);

#[newtype]
pub struct LastKnownELR(NodeId);

#[newtype]
pub struct OfflineReplica(NodeId);

#[newtype]
pub struct NodeId(u32);
#[newtype]
pub struct RemovingReplica(NodeId);

#[newtype]
pub struct AddingReplica(NodeId);

#[newtype]
pub struct PartitionIndex(u32);

impl TryExtract for PartitionIndex {
    fn try_extract(value: &[u8]) -> Result<(Self, &[u8])> {
        let (v, rest) = value.extract_u32()?;
        Ok((Self::new(v), rest))
    }
}

#[newtype]
pub struct CurrentLeaderEpoch(u32);

#[newtype]
pub struct FetchOffset(u64);

#[newtype]
pub struct LastFetchEpoch(u32);
#[newtype]
pub struct LogStartOffset(u64);

#[newtype]
pub struct PartitionMaxBytes(u32);

#[newtype]
pub struct RackId(String);

#[derive(Debug, Clone)]
pub struct Partition {
    error_code: ErrorCode,
    partition_index: PartitionIndex,
    leader_id: Leader,
    leader_epoch: LeaderEpoch,
    replica_nodes: Vec<ReplicaNode>,
    isr_nodes: Vec<ISRNode>,
    eligible_leader_replicas: Vec<EligibleLeaderReplicas>,
    last_known_elrs: Vec<LastKnownELR>,
    offline_replicas: Vec<OfflineReplica>,
    tag_buffer: TagBuffer,
}
impl Partition {
    pub fn new(
        partition_index: PartitionIndex,
        leader_id: Leader,
        leader_epoch: LeaderEpoch,
        replica_nodes: Vec<ReplicaNode>,
        isr_nodes: Vec<ISRNode>,
        eligible_leader_replicas: Vec<EligibleLeaderReplicas>,
        last_known_elrs: Vec<LastKnownELR>,
        offline_replicas: Vec<OfflineReplica>,
    ) -> Self {
        Self {
            error_code: ErrorCode::NoError,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elrs,
            offline_replicas,
            tag_buffer: TagBuffer::zero(),
        }
    }
}

impl From<Partition> for Vec<u8> {
    fn from(partition: Partition) -> Self {
        let mut bytes = Vec::new();
        bytes.put_i16(*ErrorCode::NoError);
        bytes.put_u32(*partition.partition_index);
        bytes.put_u32(**partition.leader_id);
        bytes.put_u32(*partition.leader_epoch);
        bytes
            .extend(VarInt::encode((partition.replica_nodes.len() + 1) as u64));
        partition
            .replica_nodes
            .iter()
            .for_each(|v| bytes.put_u32(**(v.clone())));
        bytes.extend(VarInt::encode((partition.isr_nodes.len() + 1) as u64));
        partition.isr_nodes.iter().for_each(|v| bytes.put_u32(**(v.clone())));
        bytes.extend(VarInt::encode(
            (partition.eligible_leader_replicas.len() + 1) as u64,
        ));
        partition
            .eligible_leader_replicas
            .iter()
            .for_each(|v| bytes.put_u32(**(v.clone())));
        bytes.extend(VarInt::encode(
            (partition.last_known_elrs.len() + 1) as u64,
        ));
        partition
            .last_known_elrs
            .iter()
            .for_each(|v| bytes.put_u32(**(v.clone())));
        bytes.extend(VarInt::encode(
            (partition.offline_replicas.len() + 1) as u64,
        ));
        partition
            .offline_replicas
            .iter()
            .for_each(|v| bytes.put_u32(**(v.clone())));
        bytes.put_u8(*TagBuffer::zero());
        bytes
    }
}

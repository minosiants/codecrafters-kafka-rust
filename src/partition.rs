use std::ops::Deref;
use crate::{ErrorCode, TagBuffer, VarInt};
use bytes::BufMut;
#[derive(Debug, Clone)]
pub struct Partition{
    error_code: ErrorCode,
    partition_index: PartitionIndex,
    leader_id: Leader,
    leader_epoch: LeaderEpoch,
    replica_nodes: Vec<ReplicaNode>,
    isr_nodes:Vec<ISRNode>,
    eligible_leader_replicas:Vec<EligibleLeaderReplicas>,
    last_known_elrs: Vec<LastKnownELR>,
    offline_replicas: Vec<OfflineReplica>,
    tag_buffer: TagBuffer
}
impl Partition {
    pub fn new(partition_index: PartitionIndex,
               leader_id: Leader,
               leader_epoch: LeaderEpoch,
               replica_nodes: Vec<ReplicaNode>,
               isr_nodes:Vec<ISRNode>,
               eligible_leader_replicas:Vec<EligibleLeaderReplicas>,
               last_known_elrs: Vec<LastKnownELR>,
               offline_replicas: Vec<OfflineReplica>,) -> Self {
        Self{
            error_code:ErrorCode::NoError,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elrs,
            offline_replicas,
            tag_buffer:TagBuffer::zero()
        }
    }
}
#[derive(Debug, Clone)]
pub struct PartitionIndex(u32);
impl Deref for PartitionIndex {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl PartitionIndex {
    pub fn new(v:u32) -> Self{
        Self(v)
    }
}
#[derive(Debug, Clone)]
pub struct Leader(NodeId);
impl Leader {
    pub fn new(v:u32)-> Self {
        Self(NodeId(v))
    }
}
impl Deref for Leader {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
#[derive(Debug, Clone)]
pub struct LeaderEpoch(u32);
impl LeaderEpoch {
    pub fn new(v:u32) -> Self {
        Self(v)
    }
}
impl Deref for LeaderEpoch {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaNode(NodeId);
impl ReplicaNode {
    pub fn new(v:u32) -> Self {
        Self(NodeId(v))
    }
}
impl Deref for ReplicaNode {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
#[derive(Debug, Clone)]
pub struct ISRNode(NodeId);
impl ISRNode{
    pub fn new(v:u32) -> Self {
        Self(NodeId(v))
    }
}
impl Deref for ISRNode {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
#[derive(Debug, Clone)]
pub struct EligibleLeaderReplicas(NodeId);
impl Deref for EligibleLeaderReplicas {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
#[derive(Debug, Clone)]
pub struct LastKnownELR(NodeId);
impl Deref for LastKnownELR {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
#[derive(Debug, Clone)]
pub struct OfflineReplica(NodeId);
impl Deref for OfflineReplica {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
#[derive(Debug, Clone)]
pub struct NodeId(u32);
#[derive(Debug, Clone)]
pub struct RemovingReplica(NodeId);
impl RemovingReplica {
    pub fn new(v:u32)->Self {
     Self(NodeId(v))
    }
}
impl Deref for RemovingReplica{
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
#[derive(Debug, Clone)]
pub struct AddingReplica(NodeId);
impl AddingReplica {
    pub fn new(v:u32)->Self {
        Self(NodeId(v))
    }
}
impl From<Partition> for Vec<u8> {
    fn from(partition: Partition) -> Self {
        let mut bytes = Vec::new();
        bytes.put_i16(*ErrorCode::NoError);
        bytes.put_u32(*partition.partition_index);
        bytes.put_u32(*partition.leader_id);
        bytes.put_u32(*partition.leader_epoch);
        bytes.extend(VarInt::encode((partition.replica_nodes.len() + 1) as u64));
        partition.replica_nodes.iter().for_each(|v| bytes.put_u32(*(v.clone())));
        bytes.extend(VarInt::encode((partition.isr_nodes.len() +1) as u64));
        partition.isr_nodes.iter().for_each(|v| bytes.put_u32(*(v.clone())));
        bytes.extend(VarInt::encode((partition.eligible_leader_replicas.len() +1)as u64));
        partition.eligible_leader_replicas.iter().for_each(|v| bytes.put_u32(*(v.clone())));
        bytes.extend(VarInt::encode((partition.last_known_elrs.len()+1) as u64));
        partition.last_known_elrs.iter().for_each(|v| bytes.put_u32(*(v.clone())));
        bytes.extend(VarInt::encode((partition.offline_replicas.len()+1) as u64));
        partition.offline_replicas.iter().for_each(|v| bytes.put_u32(*(v.clone())));
        bytes.put_u8(*TagBuffer::zero());
        bytes
    }
}
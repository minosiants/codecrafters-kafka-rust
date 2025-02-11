use crate::{ErrorCode, TagBuffer};
#[derive(Debug, Clone)]
pub struct Partition{
    error_code: ErrorCode,
    partition_index: PartitionIndex,
    leader_id: LeaderId,
    leader_epoch: LeaderEpoch,
    replica_nodes: Vec<ReplicaNode>,
    isr_nodes:Vec<ISRNode>,
    eligible_leader_replicas:Vec<EligibleLeaderReplicas>,
    last_known_elrs: Vec<LastKnownELR>,
    offline_replicas: Vec<OfflineReplica>,
    tag_buffer: TagBuffer
}
#[derive(Debug, Clone)]
pub struct PartitionIndex(i32);
#[derive(Debug, Clone)]
pub struct LeaderId(i32);
#[derive(Debug, Clone)]
pub struct LeaderEpoch(i32);
#[derive(Debug, Clone)]
pub struct ReplicaNode(NodeId);
#[derive(Debug, Clone)]
pub struct ISRNode(NodeId);
#[derive(Debug, Clone)]
pub struct EligibleLeaderReplicas(NodeId);
#[derive(Debug, Clone)]
pub struct LastKnownELR(NodeId);
#[derive(Debug, Clone)]
pub struct OfflineReplica(NodeId);
#[derive(Debug, Clone)]
pub struct NodeId(i32);

impl From<Partition> for Vec<u8> {
    fn from(_partition: Partition) -> Self {
        let bytes = Vec::new();
        bytes
    }
}
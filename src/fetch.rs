use crate::{CurrentLeaderEpoch, ErrorCode, FetchOffset, FirstOffset, HighWatermark, LastFetchEpoch, LastStableOffset, LogStartOffset, PartitionIndex, PartitionMaxBytes, PreferredReadReplica, ProducerId, RackId, TagBuffer, TopicId};
#[derive(Debug, Clone)]
pub struct FetchTopic{
    topic_id: TopicId,
    partitions:Vec<FetchPartition>,
    forgotten_topics_data: Vec<ForgottenTopicData>,
    rack_id: RackId
}
#[derive(Debug, Clone)]
pub struct FetchPartition {
    partition_index: PartitionIndex,
    current_leader_epoch: CurrentLeaderEpoch,
    fetch_offset: FetchOffset,
    last_fetch_epoch: LastFetchEpoch,
    log_start_offset: LogStartOffset,
    partition_max_bytes: PartitionMaxBytes,
}
#[derive(Debug, Clone)]
pub struct ForgottenTopicData(TopicId, Vec<PartitionIndex>);


impl ForgottenTopicData {
    pub fn new(topic_id:TopicId, partitions:Vec<PartitionIndex>) -> Self {
        Self (topic_id, partitions)
    }
}


#[derive(Debug, Clone)]
pub struct FetchResponse{
    topic_id: TopicId,
    partitions:Vec<FetchPartitionResponse>
}
impl From<FetchResponse> for Vec<u8> {
    fn from(value: FetchResponse) -> Self {
        let bytes = vec![];
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct FetchPartitionResponse {
    partition_index: PartitionIndex,
    error_code: ErrorCode,
    high_watermark: HighWatermark,
    last_stable_offset: LastStableOffset,
    log_start_offset: LogStartOffset,
    aborted_transactions:Vec<AbortedTransaction>,
    preferred_read_replica: PreferredReadReplica,
}

#[derive(Debug, Clone)]
pub struct AbortedTransaction{
    producer_id: ProducerId,
    first_offset: FirstOffset,
    tag_buffer: TagBuffer
}
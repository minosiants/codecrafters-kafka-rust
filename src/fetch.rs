use crate::{CurrentLeaderEpoch, Result, ErrorCode, FetchOffset, FirstOffset, HighWatermark, LastFetchEpoch, LastStableOffset, LogStartOffset, PartitionIndex, PartitionMaxBytes, PreferredReadReplica, ProducerId, RackId, TagBuffer, TopicId, Error, BytesOps, MapTupleTwo, TryExtract};
#[derive(Debug, Clone)]
pub struct FetchTopic{
    topic_id: TopicId,
    partitions:Vec<FetchPartition>,
    forgotten_topics_data: Vec<ForgottenTopicData>,
    rack_id: RackId
}

impl TryExtract for FetchTopic {
    fn try_extract(value: &[u8]) -> Result<(Self,&'static [u8])> {
        let (topic_id, rest) = value.drop(16).fmap_tuple(TopicId::mk)?;
        let (partitions,rest) = rest.extract_array_into()?;
        let (forgotten_topics_data, rest) = rest.extract_array_into() ?;
        let (rack_id,rest) = rest.extract_compact_str().map_tuple(RackId::new)?;
        Ok((Self{
            topic_id,
            partitions,
            forgotten_topics_data,
            rack_id
        }, rest))
    }
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

impl TryExtract for FetchPartition {
    fn try_extract(value: &[u8]) -> Result<(Self, &'static [u8])> {
        let (partition_index, rest) = value.extract_u32_into(PartitionIndex::new)?;
        let (current_leader_epoch, rest) = rest.extract_u32_into(CurrentLeaderEpoch::new)?;
        let (fetch_offset, rest) = rest.extract_u64_into(FetchOffset::new)?;
        let (last_fetch_epoch, rest) = rest.extract_u32_into(LastFetchEpoch::new)?;
        let (log_start_offset, rest) = rest.extract_u64_into(LogStartOffset::new)?;
        let (partition_max_bytes, rest) = rest.extract_u32_into(PartitionMaxBytes::new)?;
        Ok((Self{
          partition_index,
            current_leader_epoch,
            fetch_offset,
            last_fetch_epoch,
            log_start_offset,
            partition_max_bytes
        }, rest))
    }
}
#[derive(Debug, Clone)]
pub struct ForgottenTopicData(TopicId, Vec<PartitionIndex>);


impl ForgottenTopicData {
    pub fn new(topic_id:TopicId, partitions:Vec<PartitionIndex>) -> Self {
        Self (topic_id, partitions)
    }
}

impl TryExtract for ForgottenTopicData {

    fn try_extract(value: & [u8]) -> Result<(Self, &'static[u8])> {
        let (topic_id, rest) = value.drop(16).fmap_tuple(TopicId::mk)?;
        let (partitions, rest) = rest.extract_array_into()?;
        Ok((Self(
            topic_id,
            partitions
        ),rest))
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
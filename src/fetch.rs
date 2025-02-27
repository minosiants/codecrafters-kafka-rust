use bytes::BufMut;
use crate::{CurrentLeaderEpoch, Result, ErrorCode, FetchOffset, FirstOffset, HighWatermark, LastFetchEpoch, LastStableOffset, LogStartOffset, PartitionIndex, PartitionMaxBytes, PreferredReadReplica, ProducerId, RackId, TagBuffer, TopicId, Error, BytesOps, MapTupleTwo, TryExtract, VarInt};
#[derive(Debug, Clone)]
pub struct FetchTopic{
    topic_id: TopicId,
    partitions:Vec<FetchPartition>,
    forgotten_topics_data: Option<ForgottenTopicData>,
    rack_id: RackId
}

impl FetchTopic {
    pub fn topic_id(&self) -> TopicId {
        self.topic_id.clone()
    }
}
impl TryExtract for FetchTopic {
    fn try_extract(value: &[u8]) -> Result<(Self,&[u8])> {
        let (topic_id1, rest) = value.drop(16)?;
        println!("topic_id1 {:?}", topic_id1);
        let topic_id = TopicId::mk(topic_id1)?;
        println!("topic_id {:?}", topic_id);
        let (partitions,rest) = rest.extract_array_into()?;
        println!("partitions {:?}", partitions);
       // let (forgotten_topics_data, rest) = ForgottenTopicData::try_extract(rest)?;
       // println!("forgotten_topics_data {:?}", forgotten_topics_data);
        let (rack_id,rest) = rest.extract_compact_str().map_tuple(RackId::new)?;
        Ok((Self{
            topic_id,
            partitions,
            forgotten_topics_data:None,
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
    fn try_extract(value: &[u8]) -> Result<(Self, &[u8])> {
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
        }, rest.drop(1).second()?))
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

    fn try_extract(value: & [u8]) -> Result<(Self, &[u8])> {
        println!("value {:?}", value);
        let (topic_id, rest) = value.drop(16).fmap_tuple(TopicId::mk)?;
        println!("topic_id {:?}", topic_id);
        println!("rest {:?}", rest);
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

impl FetchResponse {
    pub fn new(topic_id: TopicId, partitions:Vec<FetchPartitionResponse>) -> Self {
        Self{
            topic_id,
            partitions
        }
    }
}
impl From<FetchResponse> for Vec<u8> {
    fn from(value: FetchResponse) -> Self {
        let mut bytes = vec![];
        bytes.put_slice((*value.topic_id).as_bytes());
        bytes.extend(VarInt::encode((value.partitions.len()+1) as u64));
        let partitions: Vec<u8> = value.partitions.into_iter().flat_map::<Vec<u8>, _>(|e| e.into()).collect();
        bytes.put_slice(&partitions);
        bytes.put_u8(*TagBuffer::zero());
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
impl From<FetchPartitionResponse> for Vec<u8> {
    fn from(value: FetchPartitionResponse) -> Self {
        let mut bytes = vec![];
        bytes.put_u32(*value.partition_index);
        bytes.put_i16(*value.error_code);
        bytes.put_u64(*value.high_watermark);
        bytes.put_u64(*value.last_stable_offset);
        bytes.put_u64(*value.log_start_offset);
        bytes.extend(VarInt::encode((value.aborted_transactions.len()+1) as u64));
        let aborted: Vec<u8> = value.aborted_transactions.into_iter().flat_map::<Vec<u8>, _>(|e| e.into()).collect();
        bytes.put_slice(&aborted);
        bytes.put_u8(*TagBuffer::zero());
        bytes.put_u32(*value.preferred_read_replica);
        //bytes.put_u32(0);
        bytes.put_u8(*TagBuffer::zero());
        bytes
    }
}
impl FetchPartitionResponse {
    pub fn
    new(partition_index: PartitionIndex) -> Self {
        Self{
            partition_index,
            error_code:ErrorCode::NoError,
            high_watermark:HighWatermark::new(0),
            last_stable_offset:LastStableOffset::new(0),
            log_start_offset:LogStartOffset::new(0),
            aborted_transactions:vec![],
            preferred_read_replica:PreferredReadReplica::new(0)
        }
    }
    pub fn unknown(partition_index: PartitionIndex) -> Self {
        Self{
            partition_index,
            error_code:ErrorCode::UnknownTopic,
            high_watermark:HighWatermark::new(0),
            last_stable_offset:LastStableOffset::new(0),
            log_start_offset:LogStartOffset::new(0),
            aborted_transactions:vec![],
            preferred_read_replica:PreferredReadReplica::new(0)
        }
    }
}

#[derive(Debug, Clone)]
pub struct AbortedTransaction{
    producer_id: ProducerId,
    first_offset: FirstOffset
}

impl From<AbortedTransaction> for Vec<u8> {
    fn from(value: AbortedTransaction) -> Self {
        let mut bytes = vec![];
        bytes.put_u64(*value.producer_id);
        bytes.put_u64(*value.first_offset);
        bytes
    }
}
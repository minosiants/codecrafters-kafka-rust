use std::cmp::{max, PartialEq};
use std::ops::Deref;
use std::str::from_utf8;

use bytes::{Buf, BufMut};
use pretty_hex::*;
use uuid::fmt::Braced;
use uuid::Uuid;

use crate::RecordValue::PartitionRecord;
use crate::{read, AddingReplica, ToArray, BytesOps, Context, Error, ISRNode, Leader, LeaderEpoch, MapTupleTwo, PartitionIndex, RemovingReplica, ReplicaNode, Result, SignedVarInt, ToCompactString, TopicId, TopicName, VarInt, PartitionEpoch, Directory, TagBuffer};

// https://binspec.org/kafka-cluster-metadata
//
#[derive(Debug, Clone)]
pub struct Meta(Vec<Batch>);
#[derive(Debug, Clone)]
pub struct Log(Vec<Batch>);
impl Log {
    pub fn new(v:Vec<Batch>) -> Self{
        Self(v)

    }
    pub fn batches(&self) -> &Vec<Batch> {
        &self.0
    }
    pub fn load_log(topic_name: &TopicName) -> Result<Log> {
        //let partition_metadata = format!("/tmp/kraft-combined-logs/{}-0/partition.metadata", **topic_name);
        let log = format!("/tmp/kraft-combined-logs/{}-0/00000000000000000000.log", **topic_name);
        println!(">>>>>>>>>>>>> log file{:?}",log);
        //let l1 = read(&partition_metadata)?;
        //println!("partition metadata {:?}", pretty_hex(&l1));
        read(&log).map(|v|{
        println!("LOG: {:?}", pretty_hex(&v));
        v}).and_then(Batch::split_by_batch).map(Log::new)
    }
}
impl Meta {
    pub fn new(v: Vec<Batch>) -> Self {
        Self(v)
    }
    pub fn load(path: &str) -> Result<Self> {

        read(path).and_then(Batch::split_by_batch).map(Self::new)
    }

    pub fn find_log(&self, topic_id: &TopicId) -> Result<Option<Log>> {
        match self.find_topic_name(topic_id) {
            None => Ok(None),
            Some(topic_name) =>
                Log::load_log(&topic_name).map(Option::from)


        }
    }
    pub fn find_batch(&self, topic_id: TopicId) -> Option<Batch> {
        self.0.iter().find(|&v| {
            v.records
                .iter()
                .find(|&r| {
                    r.topic_id() == Some(topic_id.clone())
                })
                .is_some()
        }).map(|v| v.clone())
    }


    pub fn find_topic_name(&self, topic_id: &TopicId) -> Option<TopicName> {
        self.0
            .iter()
            .flat_map(|b| b.records.iter()) // Flatten inner structure
            .find_map(|r| match &r.value {
                RecordValue::TopicRecord(_, _, name, id)
                if id == topic_id =>
                    {
                        Some(name.clone())
                    } // Return owned `TopicId`
                _ => None,
            })
    }
    pub fn find_partitions(&self, topic_id: &TopicId) -> Vec<&RecordValue> {
        self.0
            .iter()
            .flat_map(|b| b.records.iter()) // Flatten inner structure
            .filter(|r| {
                r.is_partition_record()
                    && r.topic_id().filter(|id| *topic_id == *id).is_some()
            }).map(|v| &v.value)
            .collect()
    }
    pub fn find_topic_id(&self, topic_name: &TopicName) -> Option<TopicId> {
        self.0
            .iter()
            .flat_map(|b| b.records.iter()) // Flatten inner structure
            .find_map(|r| match &r.value {
                RecordValue::TopicRecord(_, _, name, id)
                if name == topic_name =>
                    {
                        Some(id.clone())
                    } // Return owned `TopicId`
                _ => None,
            })
    }
    fn records(&self) -> Vec<&Record> {
        self.0.iter().flat_map(|v| v.records.iter()).collect()
    }
}
#[derive(Debug, Clone)]
struct BatchOffset(u64);
impl BatchOffset {
    pub fn new(v: u64) -> Self {
        Self(v)
    }
}

impl Deref for BatchOffset {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct BatchLength(u32);
impl BatchLength {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}
impl Deref for BatchLength {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct PartitionLeaderEpic(u32);
impl PartitionLeaderEpic {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}
impl Deref for PartitionLeaderEpic {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct MagicByte(u8);
impl MagicByte {
    pub fn new(v: u8) -> Self {
        Self(v)
    }
}
impl Deref for MagicByte {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
pub struct CRC(u32);
impl CRC {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}

impl Deref for CRC {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct Attributes(u16);
impl Attributes {
    pub fn new(v: u16) -> Self {
        Self(v)
    }
}
impl Deref for Attributes {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct LastOffsetDelta(u32);
impl LastOffsetDelta {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}
impl Deref for LastOffsetDelta {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
struct BaseTimestamp(u64);
impl BaseTimestamp {
    pub fn new(v: u64) -> Self {
        Self(v)
    }
}
impl Deref for BaseTimestamp {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct MaxTimestamp(u64);
impl MaxTimestamp {
    pub fn new(v: u64) -> Self {
        Self(v)
    }
}
impl Deref for MaxTimestamp {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct ProducerId(u64);
impl ProducerId {
    pub fn new(v: u64) -> Self {
        Self(v)
    }
}
impl Deref for ProducerId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct ProducerEpoch(u16);
impl ProducerEpoch {
    pub fn new(v: u16) -> Self {
        Self(v)
    }
}
impl Deref for ProducerEpoch {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
struct BaseSequence(u32);
impl BaseSequence {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}
impl Deref for BaseSequence {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct Batch {
    batch_offset: BatchOffset,
    batch_length: BatchLength,
    partition_leader_epic: PartitionLeaderEpic,
    magic_byte: MagicByte,
    pub crc: Option<CRC>,
    attributes: Attributes,
    last_offset_delta: LastOffsetDelta,
    base_timestamp: BaseTimestamp,
    max_timestamp: MaxTimestamp,
    producer_id: Option<ProducerId>,
    producer_epoch: Option<ProducerEpoch>,
    base_sequence: Option<BaseSequence>,
    records: Vec<Record>,
}

impl Batch {
    fn split_by_batch(v: Vec<u8>) -> Result<Vec<Batch>> {
        fn do_split(v: &[u8], mut result: Vec<Batch>) -> Result<Vec<Batch>> {
            if v.is_empty() {
                Ok(result)
            } else {
                let (batch_offset, rest) = v.extract_u64_into(BatchOffset::new)?;
                let (batch_length, rest) = rest.extract_u32_into(BatchLength::new)?;
                let (batch, rest) = rest.drop(batch_length.deref().clone() as usize)?;
                println!("batch: {:?}", simple_hex(&batch));
                result.push(Batch::mk(batch_offset, batch_length, batch)?);
                do_split(rest, result)
            }
        }
        do_split(&v, vec![])
    }
    pub fn filter_records(&self, mut f:impl FnMut(&Record) -> bool) -> Batch  {
        let rec = self.records.clone().into_iter().filter(|v| f(v)).collect();
        Batch{
            records:rec,
            crc:None,
                .. (*self).clone()
        }
    }
    fn mk(batch_offset: BatchOffset, batch_length: BatchLength, v: &[u8]) -> Result<Self> {
        let (partition_leader_epic, rest) =
            v.extract_u32_into(PartitionLeaderEpic::new)?;
        let (magic_byte, rest) = rest.extract_u8_into(MagicByte::new)?;
        let (crc, rest) = rest.extract_u32_into(CRC::new)?;
        let (attributes, rest) = rest.extract_u16_into(Attributes::new)?;
        let (last_offset_delta, rest) =
            rest.extract_u32_into(LastOffsetDelta::new)?;
        let (base_timestamp, rest) =
            rest.extract_u64_into(BaseTimestamp::new)?;
        let (max_timestamp, rest) = rest.extract_u64_into(MaxTimestamp::new)?;
        let (producer_id, rest) =
            rest.extract_u64_as_option_into(ProducerId::new)?;
        let (producer_epoch, rest) =
            rest.extract_u16_as_option_into(ProducerEpoch::new)?;
        let (base_sequence, rest) =
            rest.extract_u32_as_option_into(BaseSequence::new)?;
        let (_record_length, rest) = rest.extract_u32()?;
        fn split_records(
            mut v: &[u8],
            mut result: Vec<Record>,
        ) -> Result<Vec<Record>> {
            println!("record: {:?}", simple_hex(&v));
            if v.is_empty() {
                Ok(result)
            } else {
                let (length, rest) = SignedVarInt::decode(&v)?;
                println!("!!!! record len in {:?}", length.value());
                let (record, rest) = rest.drop(length.value() as usize)?;
                let record = Record::mk(record)?;
                result.push(record);
                split_records(rest, result)
            }
        }
        let records = split_records(rest, vec![])?;
        Ok(Self {
            batch_offset,
            batch_length,
            partition_leader_epic,
            magic_byte,
            crc:Some(crc),
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
    pub fn records(&self) -> Vec<RecordValue> {
        self.records.iter().map(|v| v.value.clone()).collect()
    }
}

impl From<Batch> for Vec<u8> {
    fn from(value: Batch) -> Self {
        let mut bytes = vec![];
        //start crc
        bytes.put_u16(*value.attributes);
        bytes.put_u32(*value.last_offset_delta);
        bytes.put_u64(*value.base_timestamp);
        bytes.put_u64(*value.max_timestamp);
        bytes.put_u64(match value.producer_id {
            None => 0xff_ff_ff_ff_ff_ff_ff_ff,
            Some(v) => *v,
        });
        bytes.put_u16(match value.producer_epoch {
            None => 0xff_ff,
            Some(v) => *v,
        });
        bytes.put_u32(match value.base_sequence {
            None => 0xff_ff_ff_ff,
            Some(v) => *v,
        });

        bytes.put_u32(value.records.len() as u32);
        let records: Vec<u8> = value
            .records
            .into_iter()
            .flat_map::<Vec<u8>, _>(|e| {
                let b: Vec<u8> = e.into();
                println!("record len out: {:?}",&b);
                let mut len = SignedVarInt::encode(b.len() as i64);
                len.extend(&b); //
                len

            })
            .collect();
        bytes.extend(records);
        const CRC_32_C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

        let mut batch_with_crc = CRC_32_C.checksum(&bytes).to_be_bytes().to_vec();
        batch_with_crc.append(&mut bytes);

        bytes.put_u32(*value.partition_leader_epic);
        bytes.put_u8(*value.magic_byte);
        bytes.append(&mut batch_with_crc);

        //end crc
        let mut result = 0u64.to_be_bytes().to_vec();
        result.extend((bytes.len() as u32).to_be_bytes());
        result.extend(bytes);

        result
    }
}
#[derive(Debug, Clone)]
pub struct FrameVersion(u8);
impl FrameVersion {
    pub fn new(v: u8) -> Self {
        Self(v)
    }
}
impl Deref for FrameVersion {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
pub struct ValueVersion(u8);
impl ValueVersion {
    pub fn new(v: u8) -> Self {
        Self(v)
    }
}
impl Deref for ValueVersion {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
pub struct Header(u64);

#[derive(Debug, Clone)]
pub struct Record {
    attributes: RecordAttributes,
    timestamp_delta: TimestampDelta,
    offset_delta: OffsetDelta,
    key: Option<RecordKey>,
    value: RecordValue,
    headers: Vec<Header>,
}
impl From<Record> for Vec<u8> {
    fn from(value: Record) -> Self {
        let mut bytes = vec![];
        bytes.put_u8(*value.attributes);
        bytes.put_u8(*value.timestamp_delta);
        bytes.put_u8(*value.offset_delta);
        match value.key {
            None => bytes.put_u8(0x01),
            Some(RecordKey(v)) => {
                bytes.extend(SignedVarInt::encode(v.len() as i64));
                bytes.extend(v);
            }
        }
        let mut v: Vec<u8> = value.value.into();
        bytes.extend(SignedVarInt::encode((v.len() -1 )as i64));
        bytes.extend(v);
        bytes.extend(SignedVarInt::encode(value.headers.len() as i64));
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct RecordAttributes(u8);
impl RecordAttributes {
    pub fn new(v: u8) -> Self {
        Self(v)
    }
}
impl Deref for RecordAttributes {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
pub struct TimestampDelta(u8);
impl TimestampDelta {
    pub fn new(v: u8) -> Self {
        Self(v)
    }
}
impl Deref for TimestampDelta {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct OffsetDelta(u8);
impl OffsetDelta {
    pub fn new(v: u8) -> Self {
        Self(v)
    }
}
impl Deref for OffsetDelta {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
pub struct RecordKey(Vec<u8>);

impl RecordKey {
    fn new(v: &[u8]) -> Self {
        RecordKey(v.to_vec())
    }
    fn mk(v: &[u8]) -> Result<(Option<RecordKey>, &[u8])> {
        let (key_length, rest) = v.extract_signed_var_int()?;
        if key_length.value() < 0 {
            Ok((None, rest))
        } else {
            v.drop(key_length.value() as usize).map_tuple(|v| Some(Self::new(v)))
        }
    }
}

impl Deref for RecordKey {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
pub enum RecordValue {
    FeatureLevelRecord(Vec<u8>),
    TopicRecord(FrameVersion, ValueVersion, TopicName, TopicId),
    PartitionRecord(
        FrameVersion,
        ValueVersion,
        PartitionIndex,
        TopicId,
        Leader,
        LeaderEpoch,
        PartitionEpoch,
        Vec<ReplicaNode>,
        Vec<ISRNode>,
        Vec<AddingReplica>,
        Vec<RemovingReplica>,
        Vec<Directory>,
    ),
    RawValue(Vec<u8>)
}
impl RecordValue {
    fn record_type(&self) -> u8 {
        match self {
            RecordValue::FeatureLevelRecord(_) => 0x0c,
            RecordValue::TopicRecord(_, _, _, _) => 0x02,
            RecordValue::PartitionRecord(_, _, _, _, _, _, _, _, _, _, _, _) => 0x03,
            RecordValue::RawValue(_) => 0xff,
        }
    }
}
impl Record {
    fn mk(v: &[u8]) -> Result<Record> {
        let (attributes, rest) = v.extract_u8_into(RecordAttributes::new)?;
        let (timestamp_delta, rest) = rest.extract_u8_into(TimestampDelta::new)?;
        let (offset_delta, rest) = rest.extract_u8_into(OffsetDelta::new)?;
        let (key, rest) = RecordKey::mk(rest)?;
        let (_record_length, rest) = rest.extract_signed_var_int()?;
        let record_type = rest[1];
        let record_value = match record_type {
            0x0c => Self::feature_level_record(&rest),
            0x02 => Self::topic_record(&rest),
            0x03 => Self::partition_record(&rest),
            _ => Self::raw_value(&rest),
        }?;
        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value: record_value,
            headers: vec![],
        })
    }
    fn raw_value(v:&[u8])-> Result<RecordValue> {
        Ok(RecordValue::RawValue(v.to_vec()))
    }
    fn feature_level_record(v: &[u8]) -> Result<RecordValue> {
        println!("feature level {:?}", &v);
        Ok(RecordValue::FeatureLevelRecord(v.to_vec()))
    }
    pub fn topic_record(v: &[u8]) -> Result<RecordValue> {
        let (frame_version, rest) = v.extract_u8_into(FrameVersion::new)?;
        let (_type, rest) = rest.extract_u8()?;
        let (version, rest) = rest.extract_u8_into(ValueVersion::new)?;
        let (topic_name, rest) =
            rest.extract_compact_str().map_tuple(TopicName::new)?;
        println!(">>>>>>>>>>>>> topic record: {:?}", simple_hex(&(rest[0..16].to_vec())));
        let topic_id = rest.extract_uuid_into(TopicId::new).first()?;
        println!(">>>>>>>>>>>>  topic record: {:?}", topic_id);
        Ok(RecordValue::TopicRecord(
            frame_version,
            version,
            topic_name,
            topic_id,
        ))
    }
    fn partition_record(v: &[u8]) -> Result<RecordValue> {
        let (frame_version, rest) = v.extract_u8_into(FrameVersion::new)?;
        let (_type, rest) = rest.extract_u8()?;
        let (version, rest) = rest.extract_u8_into(ValueVersion::new)?;
        let (partition_index, rest) =
            rest.extract_u32_into(PartitionIndex::new)?;
        println!(">>>>>>>>>>>>> partition record: {:?}", simple_hex(&(rest[0..16].to_vec())));
        let (topic_id, rest) = rest.extract_uuid_into(TopicId::new)?;
        println!(">>>>>>>>>>>>> partition record: {:?}", topic_id);
        let (replicas, rest) = rest.extract_array(ReplicaNode::new)?;
        let (isrs, rest) = rest.extract_array(ISRNode::new)?;
        let (removing, rest) = rest.extract_array(RemovingReplica::new)?;
        let (adding, rest) = rest.extract_array(AddingReplica::new)?;
        let (leader, rest) = rest.extract_u32_into(Leader::new)?;
        let (leader_epoch, rest) = rest.extract_u32_into(LeaderEpoch::new)?;
        let (partition_epoch, rest) = rest.extract_u32_into(PartitionEpoch::new)?;
        let (directories, rest) = rest.extract_array_into::<Uuid>()?;
        Ok(RecordValue::PartitionRecord(
            frame_version,
            version,
            partition_index,
            topic_id,
            leader,
            leader_epoch,
            partition_epoch,
            replicas,
            isrs,
            adding,
            removing,
            directories.iter().map(|&v| Directory::new(v)).collect(),
        ))
    }

    pub fn is_partition_record(&self) -> bool {
        match self.value {
            RecordValue::PartitionRecord(_, _, _, _, _, _, _, _, _, _, _, _) => true,
            _ => false
        }
    }
    pub fn is_topic_record(&self) -> bool {
        match self.value {
            RecordValue::TopicRecord(_, _, _, _) => true,
            _ => false
        }
    }
    pub fn topic_id(&self) -> Option<TopicId> {
        match &self.value {
            RecordValue::FeatureLevelRecord(_) => None,
            RecordValue::TopicRecord(_, _, _, topic_id) => {
                Some(topic_id.clone())
            }
            RecordValue::PartitionRecord(
                _,
                _,
                _,
                topic_id,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
            ) => Some(topic_id.clone()),
            RecordValue::RawValue(_) => None,
        }
    }
}

impl From<RecordValue> for Vec<u8> {
    fn from(value: RecordValue) -> Self {
        let record_type = value.record_type();
        match value {
            RecordValue::FeatureLevelRecord(v) => v,
            RecordValue::TopicRecord(
                frame_version,
                value_version,
                topic_name,
                topic_id,
            ) => {
                let mut bytes = vec![];
                bytes.put_u8(*frame_version);
                bytes.put_u8(record_type);
                bytes.put_u8(*value_version);
                bytes.extend(topic_name.to_compact_string());
                bytes.extend((*topic_id).as_bytes());
                bytes.put_u8(*TagBuffer::zero());
                bytes
            }
            RecordValue::PartitionRecord(
                frame_version,
                value_version,
                partition_index,
                topic_id,
                leader,
                leader_epoch,
                partition_epoch,
                replica_nodes,
                isr_nodes,
                adding_replicas,
                removing_replicas,
                directrories,
            ) => {
                let mut bytes = vec![];
                bytes.put_u8(*frame_version);
                bytes.put_u8(record_type);
                bytes.put_u8(*value_version);
                bytes.put_u32(*partition_index);
                bytes.extend((*topic_id).as_bytes());
                println!(">>>>>>>>>>>>>>>>>>>>>> Partition Record {:?} before todo should be 23", bytes.len());
                //todo refactor
                bytes.extend(replica_nodes.into_iter().map(|v| *v).collect::<Vec<u32>>().to_pb_array().unwrap());
                bytes.extend(isr_nodes.into_iter().map(|v| *v).collect::<Vec<u32>>().to_pb_array().unwrap());
                bytes.extend(removing_replicas.into_iter().map(|v| *v).collect::<Vec<u32>>().to_pb_array().unwrap());
                bytes.extend(adding_replicas.into_iter().map(|v| *v).collect::<Vec<u32>>().to_pb_array().unwrap());
                println!(">>>>>>>>>>>>>>>>>>>>>> Partition Record {:?} after todo ", bytes.len());
                bytes.put_u32(*leader);
                bytes.put_u32(*leader_epoch);
                bytes.put_u32(*partition_epoch);
                println!(">>>>>>>>>>>>>>>>>>>>>> Partition Record {:?} before  dir ", bytes.len());
                bytes.extend(directrories.into_iter().map(|v| *v).collect::<Vec<Uuid>>().to_pb_array().unwrap());
                println!(">>>>>>>>>>>>>>>>>>>>>> Partition Record {:?} final", bytes.len());
                bytes.put_u8(*TagBuffer::zero());
                bytes
            }
            RecordValue::RawValue(v) =>v
        }
    }
}

#[cfg(test)]
mod tests {
    use hex::decode;

    use super::*;

    #[test]
    fn test_load() -> Result<()> {
        let bytes_str = "00 00 00 00  00 00 00 01  00 00 00 4f  00 00 00 01  02 b0 69 45  7c 00 00 00  00 00 00 00  00 01 \
        91 e0  5a f8 18 00  00 01 91 e0  5a f8 18 ff  ff ff ff ff  ff ff ff ff  ff ff ff ff  ff 00 00 00  01 3a 00 00  00 01 2e 01  \
        0c 00 11 6d  65 74 61 64  61 74 61 2e  76 65 72 73  69 6f 6e 00  14 00 00 00  00 00 00 00  00 00 02 00  00 00 9a 00  00 00 01\
         02  fb c9 6e 51  00 00 00 00  00 01 00 00  01 91 e0 5b  2d 15 00 00  01 91 e0 5b  2d 15 ff ff  ff ff ff ff  ff ff ff ff  ff ff ff ff  00 00 00 02  3c 00 00 00  01 30 01 02  00 04 62 61  7a 00 00 00  00 00 00 40  00 80 00 00  00 00 00 00  11 00 00 90  01 00 00 02  01 82 01 01  03 01 00 00  00 00 00 00  00 00 00 00  40 00 80 00  00 00 00 00  00 11 02 00  00 00 01 02  00 00 00 01  01 01 00 00  00 01 00 00  00 00 00 00  00 00 02 10  00 00 00 00  00 40 00 80  00 00 00 00  00 00 01 00  00 00 00 00  00 00 00 00  04 00 00 00  9a 00 00 00  01 02 fa d9  f6 43 00 00  00 00 00 01  00 00 01 91  e0 5b 2d 15  00 00 01 91  e0 5b 2d 15  ff ff ff ff  ff ff ff ff  ff ff ff ff  ff ff 00 00  00 02 3c 00  00 00 01 30  01 02 00 04  70 61 78 00  00 00 00 00  00 40 00 80  00 00 00 00  00 00 14 00  00 90 01 00  00 02 01 82  01 01 03 01  00 00 00 00  00 00 00 00  00 00 40 00  80 00 00 00  00 00 00 14  02 00 00 00  01 02 00 00  00 01 01 01  00 00 00 01  00 00 00 00  00 00 00 00  02 10 00 00  00 00 00 40  00 80 00 00  00 00 00 00  01 00 00 00  00 00 00 00  00 00 06 00  00 00 e4 00  00 00 01 02  1d 7d f1 e7  00 00 00 00  00 02 00 00  01 91 e0 5b  2d 15 00 00  01 91 e0 5b  2d 15 ff ff  ff ff ff ff  ff ff ff ff  ff ff ff ff  00 00 00 03  3c 00 00 00  01 30 01 02  00 04 70 61  7a 00 00 00  00 00 00 40  00 80 00 00  00 00 00 00  93 00 00 90  01 00 00 02  01 82 01 01  03 01 00 00  00 00 00 00  00 00 00 00  40 00 80 00  00 00 00 00  00 93 02 00  00 00 01 02  00 00 00 01  01 01 00 00  00 01 00 00  00 00 00 00  00 00 02 10  00 00 00 00  00 40 00 80  00 00 00 00  00 00 01 00  00 90 01 00  00 04 01 82  01 01 03 01  00 00 00 01  00 00 00 00  00 00 40 00  80 00 00 00  00 00 00 93  02 00 00 00  01 02 00 00  00 01 01 01  00 00 00 01  00 00 00 00  00 00 00 00  02 10 00 00  00 00 00 40  00 80 00 00  00 00 00 00  01 00 00".replace(" ", "");
        let byte_vec = decode(bytes_str).expect("Invalid hex string");
        let meta = Batch::split_by_batch(byte_vec).map(Meta::new)?;
        let topic_id =
            meta.find_topic_id(&TopicName::from_str("baz")).context("error")?;
        println!("topic_id {:?}", topic_id);
        Ok(())
    }

    #[test]
    fn something() -> Result<()> {
        let topic_name = TopicName::new("saz".to_string());

        println!("v: {:?}", simple_hex(&topic_name.to_compact_string()));

        let vv=SignedVarInt::encode(24i64);
        println!("vv {:?}", simple_hex(&vv));

        let value:Vec<u8> = vec![72, 101, 108, 108, 111, 32, 67, 111, 100, 101, 67, 114, 97, 102, 116, 101, 114, 115, 33];

        println!("rec: {:?}", pretty_hex(&value));
        Ok(())
    }
}

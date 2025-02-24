use std::cmp::{max, PartialEq};
use std::str::from_utf8;

use bytes::Buf;
use pretty_hex::*;
use uuid::Uuid;

use crate::{AddingReplica, BytesOps, Context, Error,ISRNode, Leader, LeaderEpoch, MapTupleTwo, PartitionIndex, read, RemovingReplica, ReplicaNode, Result, SignedVarInt, TopicId, TopicName, VarInt};
use crate::Record::PartitionRecord;

// https://binspec.org/kafka-cluster-metadata
//
pub struct Meta(Vec<Batch>);


impl Meta {
    pub fn new(v: Vec<Batch>) -> Self {
        Self(v)
    }
    pub fn load(path: &str) -> Result<Self> {
        read(path)
            .and_then(Self::split_by_batch)
            .map(Self::new)
    }
    fn split_by_batch(v: Vec<u8>) -> Result<Vec<Batch>> {
        fn do_split(v: &[u8], mut result: Vec<Batch>) -> Result<Vec<Batch>> {
            if v.is_empty() {
                Ok(result)
            } else {
                let (batch_length, rest) = v.drop(8).second().and_then(|e|e.extract_u32())?;
                let (batch, rest) = rest.drop(batch_length as usize)?;
                println!("batch: {:?}", batch);
                result.push(Batch::mk(batch)?);
                do_split(rest, result)
            }
        }
        do_split(&v, vec![])
    }

    pub fn find_partitions(&self, topic_id: &TopicId) -> Vec<&Record> {
        self.0.iter()
            .flat_map(|b| b.0.iter()) // Flatten inner structure
            .filter(|r| r.is_partition_record() && r.topic_id().filter(|id| *topic_id == *id).is_some())
            .collect()
    }
    pub fn find_topic_id(&self, topic_name: &TopicName) -> Option<TopicId> {
        self.0.iter()
            .flat_map(|b| b.0.iter()) // Flatten inner structure
            .find_map(|r| match r {
                Record::TopicRecord(name, id) if name == topic_name => Some(id.clone()), // Return owned `TopicId`
                _ => None,
            })
    }
    fn records(&self) -> Vec<&Record> {
        self.0.iter().flat_map(|v| {
            v.0.iter()
        }).collect()
    }
}

struct Batch(Vec<Record>);
impl Batch {
    fn new(v: Vec<Record>) -> Self {
        Self(v)
    }
    fn mk(v: &[u8]) -> Result<Self> {
        fn split_records(mut v: &[u8], mut result: Vec<Record>) -> Result<Vec<Record>> {
            if v.is_empty() {
                Ok(result)
            } else {
                let (record_length, rest) = SignedVarInt::decode(&v)?;
                let (record, rest) = rest.split_at_checked(record_length.value() as usize).context("record")?;
                let record = Record::mk(record)?;
                result.push(record);
                split_records(rest, result)
            }
        }
        let (_, records) = v.split_at_checked(49 ).context("batch records")?;
        split_records(records, vec![]).map(Self::new)
    }
}
#[derive(Debug, Clone)]
pub enum Record {
    FeatureLevelRecord(Vec<u8>),
    TopicRecord(TopicName, TopicId),
    PartitionRecord(PartitionIndex, TopicId, Leader, LeaderEpoch, Vec<ReplicaNode>, Vec<ISRNode>, Vec<AddingReplica>, Vec<RemovingReplica>),
}

impl Record {
    fn mk(v: &[u8]) -> Result<Record> {

        let (key_length, rest) = SignedVarInt::decode(&v[3..])?;
        let (_, value) = SignedVarInt::decode(&rest[max(key_length.value() + 1, 0) as usize ..])?;
        let record_type = value[1];
        match record_type {
            0x0c => Self::feature_level_record(&value),
            0x02 => Self::topic_record(&value),
            0x03 => Self::partition_record(&value),
            _ => Err(Error::UnknownRecordType(record_type))
        }
    }

    fn feature_level_record(v: &[u8]) -> Result<Record> {
        Ok(Record::FeatureLevelRecord(v.to_vec()))
    }
    fn topic_record(v: &[u8]) -> Result<Record> {
        let (name_length, rest) = VarInt::decode(&v[3..]).map_tuple(|v|v.value() -1)?;
        let name = from_utf8(&rest[..name_length]).map(TopicName::from_str)?;
        let id = Uuid::from_slice(&rest[name_length..name_length + 16]).map(TopicId::new)?;
        Ok(Record::TopicRecord(name, id))
    }
    fn partition_record(v: &[u8]) -> Result<Record> {
        let (partition_index, rest) = v[3..].split_at_checked(4).map(|(mut id, rest)| {
            (PartitionIndex::new(id.get_u32()), rest)
        }).context("Create partition id")?;

        let (topic_id, rest) = rest.split_at_checked(16)
            .ok_or(Error::general(""))
            .and_then(|(id, rest)| {
                Ok((TopicId::new(Uuid::from_slice(id)?), rest))
            })?;


        let (replicas, rest) = rest.extract_array(ReplicaNode::new)?;

        let (isrs, rest) = rest.extract_array(ISRNode::new)?;
        let (removing, rest) = rest.extract_array(RemovingReplica::new)?;
        let (adding, rest) = rest.extract_array(AddingReplica::new)?;

        let (leader, rest) = rest.extract_u32_into(Leader::new)?;
        let (leader_epoch, rest) = rest.extract_u32_into(LeaderEpoch::new)?;

        Ok(Record::PartitionRecord(partition_index, topic_id, leader, leader_epoch, replicas, isrs, adding, removing))
    }

    pub fn is_partition_record(&self) -> bool {
        match self {
            Record::FeatureLevelRecord(_) => false,
            Record::TopicRecord(_, _) => false,
            PartitionRecord(_, _, _, _, _, _, _, _) => true
        }
    }
    pub fn topic_id(&self) -> Option<TopicId> {
        match self {
            Record::FeatureLevelRecord(_) => None,
            Record::TopicRecord(_, topic_id) => Some(topic_id.clone()),
            Record::PartitionRecord(_, topic_id, _, _, _, _, _, _) => Some(topic_id.clone())
        }
    }
}





#[cfg(test)]
mod tests {
    use hex::decode;

    use super::*;

    #[test]
    fn test_load() -> Result<()>{

        let bytes_str = "00 00 00 00  00 00 00 01  00 00 00 4f  00 00 00 01  02 b0 69 45  7c 00 00 00  00 00 00 00  00 01 \
        91 e0  5a f8 18 00  00 01 91 e0  5a f8 18 ff  ff ff ff ff  ff ff ff ff  ff ff ff ff  ff 00 00 00  01 3a 00 00  00 01 2e 01  \
        0c 00 11 6d  65 74 61 64  61 74 61 2e  76 65 72 73  69 6f 6e 00  14 00 00 00  00 00 00 00  00 00 02 00  00 00 9a 00  00 00 01\
         02  fb c9 6e 51  00 00 00 00  00 01 00 00  01 91 e0 5b  2d 15 00 00  01 91 e0 5b  2d 15 ff ff  ff ff ff ff  ff ff ff ff  ff ff ff ff  00 00 00 02  3c 00 00 00  01 30 01 02  00 04 62 61  7a 00 00 00  00 00 00 40  00 80 00 00  00 00 00 00  11 00 00 90  01 00 00 02  01 82 01 01  03 01 00 00  00 00 00 00  00 00 00 00  40 00 80 00  00 00 00 00  00 11 02 00  00 00 01 02  00 00 00 01  01 01 00 00  00 01 00 00  00 00 00 00  00 00 02 10  00 00 00 00  00 40 00 80  00 00 00 00  00 00 01 00  00 00 00 00  00 00 00 00  04 00 00 00  9a 00 00 00  01 02 fa d9  f6 43 00 00  00 00 00 01  00 00 01 91  e0 5b 2d 15  00 00 01 91  e0 5b 2d 15  ff ff ff ff  ff ff ff ff  ff ff ff ff  ff ff 00 00  00 02 3c 00  00 00 01 30  01 02 00 04  70 61 78 00  00 00 00 00  00 40 00 80  00 00 00 00  00 00 14 00  00 90 01 00  00 02 01 82  01 01 03 01  00 00 00 00  00 00 00 00  00 00 40 00  80 00 00 00  00 00 00 14  02 00 00 00  01 02 00 00  00 01 01 01  00 00 00 01  00 00 00 00  00 00 00 00  02 10 00 00  00 00 00 40  00 80 00 00  00 00 00 00  01 00 00 00  00 00 00 00  00 00 06 00  00 00 e4 00  00 00 01 02  1d 7d f1 e7  00 00 00 00  00 02 00 00  01 91 e0 5b  2d 15 00 00  01 91 e0 5b  2d 15 ff ff  ff ff ff ff  ff ff ff ff  ff ff ff ff  00 00 00 03  3c 00 00 00  01 30 01 02  00 04 70 61  7a 00 00 00  00 00 00 40  00 80 00 00  00 00 00 00  93 00 00 90  01 00 00 02  01 82 01 01  03 01 00 00  00 00 00 00  00 00 00 00  40 00 80 00  00 00 00 00  00 93 02 00  00 00 01 02  00 00 00 01  01 01 00 00  00 01 00 00  00 00 00 00  00 00 02 10  00 00 00 00  00 40 00 80  00 00 00 00  00 00 01 00  00 90 01 00  00 04 01 82  01 01 03 01  00 00 00 01  00 00 00 00  00 00 40 00  80 00 00 00  00 00 00 93  02 00 00 00  01 02 00 00  00 01 01 01  00 00 00 01  00 00 00 00  00 00 00 00  02 10 00 00  00 00 00 40  00 80 00 00  00 00 00 00  01 00 00".replace(" ", "");
        let byte_vec = decode(bytes_str).expect("Invalid hex string");
        let meta = Meta::split_by_batch(byte_vec).map(Meta::new)?;
        let topic_id = meta.find_topic_id(&TopicName::from_str("baz")).context("error")?;
        println!("topic_id {:?}",topic_id);
        Ok(())
    }

    #[test]
    fn something() -> Result<()> {


        Ok(())
    }


}


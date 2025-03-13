use crate::{Context, ErrorCode, Partition, Result, TagBuffer, VarInt};
use bytes::BufMut;
use std::ops::Deref;
use uuid::*;
#[derive(Debug, Clone)]
pub struct Topic {
    error_code: ErrorCode,
    name: TopicName,
    id: TopicId,
    is_internal: bool,
    partitions: Vec<Partition>,
    topic_authorized_operations: TopicAuthorizedOperations,
    tag_buffer: TagBuffer,
}
impl Topic {
    pub fn new(
        name: TopicName,
        id: TopicId,
        partitions: Vec<Partition>,
    ) -> Self {
        Self {
            error_code: ErrorCode::NoError,
            name,
            id,
            is_internal: false,
            partitions,
            topic_authorized_operations: TopicAuthorizedOperations(0x0df8),
            tag_buffer: TagBuffer::zero(),
        }
    }
    pub fn unknown(name: TopicName) -> Self {
        Self {
            error_code: ErrorCode::UnknownTopicOrPartition,
            name,
            id: TopicId::zero(),
            is_internal: false,
            partitions: vec![],
            topic_authorized_operations: TopicAuthorizedOperations(0),
            tag_buffer: TagBuffer::zero(),
        }
    }
}

impl From<Topic> for Vec<u8> {
    fn from(topic: Topic) -> Self {
        let mut bytes = Vec::new();
        bytes.put_i16(*topic.error_code);
        bytes.extend(VarInt::encode((topic.name.len() + 1) as u64));
        bytes.put_slice((*topic.name).as_bytes());
        bytes.put_slice((*topic.id).as_bytes());
        bytes.put_u8(u8::from(topic.is_internal));
        bytes.put_u8(topic.partitions.len() as u8 + 1);
        let p_bytes: Vec<u8> = topic
            .partitions
            .into_iter()
            .flat_map::<Vec<u8>, _>(|e| e.into())
            .collect();
        bytes.extend(p_bytes);
        bytes.put_u32(*topic.topic_authorized_operations);
        bytes.put_u8(*topic.tag_buffer);
        bytes
    }
}
#[derive(Debug, Clone)]
pub struct TopicName(String);

impl TopicName {
    pub fn new(name: String) -> Self {
        TopicName(name)
    }
    pub fn from_str(name: &str) -> Self {
        Self::new(name.to_string())
    }
}
impl PartialEq for &TopicName {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Deref for TopicName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TopicId(Uuid);
impl TopicId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }
    pub fn mk(v: &[u8]) -> Result<Self> {
        Uuid::from_slice(v).context("uuid").map(Self::new)
    }
    pub fn zero() -> Self {
        Self::new(
            Uuid::parse_str(&"00000000-0000-0000-0000-000000000000").unwrap(),
        )
    }
}

impl Deref for TopicId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct TopicAuthorizedOperations(u32);

impl Deref for TopicAuthorizedOperations {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

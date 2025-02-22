use std::convert::{TryFrom, TryInto};
use std::io::Read;
use std::net::TcpStream;

use crate::{ApiKey, ClientId, Context, CorrelationId, Cursor, Length, MessageSize, Result, TopicName, Version};
use crate::error::Error;

#[derive(Debug, Clone)]
pub struct RequestHeader {
    api_key: ApiKey,
    api_version: Version,
    correlation_id: CorrelationId,
    client_id: ClientId,
}
impl RequestHeader {
    fn new(api_key: ApiKey, api_version: Version, correlation_id: CorrelationId, client_id: ClientId) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
    pub fn api_key(&self) -> ApiKey { self.api_key }
    pub fn api_version(&self) -> Version { self.api_version }
    pub fn correlation_id(&self) -> CorrelationId { self.correlation_id }
}
#[derive(Debug, Clone)]
pub enum RequestBody {
    ApiVersions,
    DescribeTopicPartitions {
        topics: Vec<TopicName>,
        limit: ResponsePartitionLimit,
        cursor: Option<Cursor>,
    },
    Fetch,
}
impl RequestBody {
    pub fn mk(api_key: ApiKey, body: &[u8]) -> Result<Self> {
        match api_key {
            ApiKey::ApiVersions => Ok(RequestBody::ApiVersions),
            ApiKey::DescribeTopicPartitions => Self::mk_describe_topic_partitions(body),
            ApiKey::Fetch => Self::mk_fetch(body)
        }
    }
    fn mk_describe_topic_partitions(body: &[u8]) -> Result<Self> {

        fn mk_topic_name(bytes: &[u8], mut result: Vec<TopicName>, len:u8) -> Result<Vec<TopicName>> {
            if result.len() as u8 == len {
                Ok(result)
            } else {
                let length = bytes[0] as usize;
                let name = String::from_utf8(bytes[1..length].to_vec())
                    .context("create a topic name")
                    .map(TopicName::new)?;
                result.push(name);
                mk_topic_name(&bytes[length + 1..], result, len)
            }
        }

        let array_length: u8 = body[0] - 1;
        let topics: Vec<TopicName> = mk_topic_name(&body[1..], vec![], array_length)?;
        let topics_size = topics.iter().fold(0, |acc, v| acc + (*v).len()) + 1;
        let limit = ResponsePartitionLimit::mk(&body[topics_size..topics_size + 4])?;
        let cursor = Cursor::mk(body[topics_size + 4]);

        Ok(RequestBody::DescribeTopicPartitions {
            topics,
            limit,
            cursor,
        })
    }
    fn mk_fetch(body: &[u8])->Result<Self>{
        todo!()
    }

}
#[derive(Debug, Clone)]
pub struct ResponsePartitionLimit(i32);
impl ResponsePartitionLimit {
    pub fn new(v: i32) -> Self {
        Self(v)
    }
    pub fn mk(bytes: &[u8]) -> Result<ResponsePartitionLimit> {
        bytes.try_into()
            .context("not able to read Stream")
            .map(i32::from_be_bytes)
            .map(ResponsePartitionLimit::new)
    }
}
#[derive(Debug, Clone)]
pub struct Request {
    pub header: RequestHeader,
    pub body: RequestBody,

}
impl Request {
    fn new(header: RequestHeader, body: RequestBody) -> Self {
        Self {
            header,
            body,
        }
    }
}
impl TryFrom<&mut TcpStream> for Request {
    type Error = Error;

    fn try_from(stream: &mut TcpStream) -> Result<Self> {
        let message_size = message_size(stream)?;
        println!("message size {:?}", message_size);
        let mut request: Vec<u8> = vec![0; *message_size as usize];
        stream.read_exact(&mut request).with_context(|| "not able to read stream")?;
        println!("{:?}", request);
        let correlation_id: CorrelationId = CorrelationId::mk(request[4..8].as_ref())?;

        let api_key = ApiKey::mk(request[0..2].as_ref())
            .map_err(|e| e.with_correlation_id(correlation_id))?;

        let api_version: Version = Version::mk(request[2..4].as_ref())
            .map_err(|e| e.with_correlation_id(correlation_id))?;

        let client_id_length: usize = Length::mk(&request[8..10]).map(|e| e.into())?;
        let body_start: usize = 10usize + client_id_length;
        let client_id = ClientId::mk(&request[10..body_start])?;
        println!("client_id {:?}", client_id);
        let header = RequestHeader::new(api_key, api_version, correlation_id, client_id);
        println!("header {:?}", header);
        let body = RequestBody::mk(header.api_key, &request[body_start + 1..])?;
        Ok(Request::new(header, body))
    }
}

fn message_size(stream: &mut TcpStream) -> Result<MessageSize> {
    let mut result = [0u8; 4];
    stream.read_exact(&mut result).with_context(|| "MessageSize is not valid")?;
    MessageSize::try_from_bytes(result)
}

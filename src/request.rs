use std::convert::{TryFrom, TryInto};
use std::io::Read;
use std::net::TcpStream;

use pretty_hex::{simple_hex, PrettyHex};

use crate::error::Error;
use crate::{
    ApiKey, BytesOps, ClientId, Context, CorrelationId, Cursor, FetchTopic,
    ForgottenTopicData, IsolationLevel, MapTupleTwo, MaxBytes, MaxWait,
    MessageSize, MinBytes, RackId, Result, SessionEpoch, SessionId, TopicName,
    Version,
};

#[derive(Debug, Clone)]
pub struct RequestHeader {
    api_key: ApiKey,
    api_version: Version,
    correlation_id: CorrelationId,
    client_id: ClientId,
}
impl RequestHeader {
    fn new(
        api_key: ApiKey,
        api_version: Version,
        correlation_id: CorrelationId,
        client_id: ClientId,
    ) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
    pub fn api_key(&self) -> ApiKey {
        self.api_key
    }
    pub fn api_version(&self) -> Version {
        self.api_version
    }
    pub fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }
}
#[derive(Debug, Clone)]
pub enum RequestBody {
    ApiVersions,
    DescribeTopicPartitions {
        topics: Vec<TopicName>,
        limit: ResponsePartitionLimit,
        cursor: Option<Cursor>,
    },
    Fetch {
        max_wait: MaxWait,
        min_bytes: MinBytes,
        max_bytes: MaxBytes,
        isolation_level: IsolationLevel,
        session_id: SessionId,
        session_epoch: SessionEpoch,
        topics: Vec<FetchTopic>,
        forgotten_topics_data: Vec<ForgottenTopicData>,
        rack_id: RackId,
    },
}
impl RequestBody {
    pub fn mk(api_key: ApiKey, body: &[u8]) -> Result<Self> {
        match api_key {
            ApiKey::ApiVersions => Ok(RequestBody::ApiVersions),
            ApiKey::DescribeTopicPartitions =>
                Self::describe_topic_partitions(body),
            ApiKey::Fetch => Self::fetch(body),
        }
    }
    fn describe_topic_partitions(body: &[u8]) -> Result<Self> {
        fn topic_name(
            bytes: &[u8],
            mut result: Vec<TopicName>,
            len: u8,
        ) -> Result<Vec<TopicName>> {
            if result.len() as u8 == len {
                Ok(result)
            } else {
                let length = bytes[0] as usize;
                let name = String::from_utf8(bytes[1..length].to_vec())
                    .context("create a topic name")
                    .map(TopicName::new)?;
                result.push(name);
                topic_name(&bytes[length + 1..], result, len)
            }
        }

        let array_length: u8 = body[0] - 1;
        let topics: Vec<TopicName> =
            topic_name(&body[1..], vec![], array_length)?;
        let topics_size =
            topics.iter().fold(0, |acc, v| acc + v.value().len()) + 1;
        let limit =
            ResponsePartitionLimit::mk(&body[topics_size..topics_size + 4])?;
        let cursor = Cursor::mk(body[topics_size + 4]);

        Ok(RequestBody::DescribeTopicPartitions {
            topics,
            limit,
            cursor,
        })
    }
    fn fetch(body: &[u8]) -> Result<Self> {
        let (max_wait, rest) = body.extract_u32_into(MaxWait::new)?;
        let (min_bytes, rest) = rest.extract_u32_into(MinBytes::new)?;
        let (max_bytes, rest) = rest.extract_u32_into(MaxBytes::new)?;
        let (isolation_level, rest) =
            rest.extract_u8_into(IsolationLevel::new)?;
        let (session_id, rest) = rest.extract_u32_into(SessionId::new)?;
        let (session_epoch, rest) = rest.extract_u32_into(SessionEpoch::new)?;
        let (topics, rest) = rest.extract_array_into()?;
        let (forgotten_topics_data, rest) =
            rest.drop(1).second().and_then(|v| v.extract_array_into())?;

        let (rack_id, _rest) =
            rest.extract_compact_str().map_tuple(RackId::new)?;

        Ok(RequestBody::Fetch {
            max_wait,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
        })
    }
}
#[derive(Debug, Clone)]
pub struct ResponsePartitionLimit(i32);
impl ResponsePartitionLimit {
    pub fn new(v: i32) -> Self {
        Self(v)
    }
    pub fn mk(bytes: &[u8]) -> Result<ResponsePartitionLimit> {
        bytes
            .try_into()
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
        stream.read_exact(&mut request).context("not able to read stream")?;
        println!("{:?}", request);
        let (correlation_id, _) =
            request[4..8].as_ref().extract_u32_into(CorrelationId::new)?;
        println!("correlation_id {:?}", correlation_id);
        let (api_key, rest) = request
            .as_slice()
            .extract_u16()
            .fmap_tuple(TryFrom::try_from)
            .map_err(Error::set_correlation_id(correlation_id))?;
        println!("api_key {:?}", api_key);
        let (api_version, rest) = rest
            .extract_u16()
            .fmap_tuple(TryFrom::try_from)
            .map_err(Error::set_correlation_id(correlation_id))?;
        println!("api_version {:?}", api_version);
        let (client_id_length, rest) =
            rest.drop(4).second().and_then(|v| v.extract_u16())?; //drop correlation_id
        let (client_id, rest) = rest
            .extract_str(client_id_length as usize)
            .map_tuple(|s| ClientId::new(s.to_string()))?;
        println!("client_id {:?}", client_id);
        let header =
            RequestHeader::new(api_key, api_version, correlation_id, client_id);
        println!("header {:?}", header);
        let body = RequestBody::mk(header.api_key, rest.drop(1).second()?)?;
        Ok(Request::new(header, body))
    }
}

fn message_size(stream: &mut TcpStream) -> Result<MessageSize> {
    let mut result = [0u8; 4];
    stream
        .read_exact(&mut result)
        .with_context(|| "MessageSize is not valid")?;
    MessageSize::try_from_bytes(result)
}

#[cfg(test)]
mod test {
    use hex::decode;

    #[test]
    fn test_fetch() {
        use super::*;
        let data: String = vec![
            "00 00 01 f4  00 00 00 01  03 20 00 00  00 00 00 00",
            "00 00 00 00  00 02 00 00  00 00 00 00  00 00 00 00",
            "00 00 00 00  43 35 02 00  00 00 00 ff  ff ff ff 00",
            "00 00 00 00  00 00 00 ff  ff ff ff ff  ff ff ff ff",
            "ff ff ff 00  10 00 00 00  00 01 01 00",
        ]
        .join("")
        .replace(" ", "");
        let bytes = decode(data).expect("");
        let req = RequestBody::fetch(&bytes);

        println!("req {:?}", req)
    }
}

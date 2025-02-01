use std::convert::{TryFrom, TryInto};
use std::io::Read;
use std::net::TcpStream;
use crate::{ApiKey, Version, Context, CorrelationId, MessageSize, Result};
use crate::error::Error;
#[derive(Debug, Copy, Clone)]
pub struct RequestHeader {
    api_key: ApiKey,
    api_version: Version,
    correlation_id: CorrelationId,
}
impl RequestHeader {
    fn new(api_key: ApiKey, api_version: Version, correlation_id: CorrelationId) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
        }
    }
    pub fn api_key(&self) -> ApiKey {self.api_key}
    pub fn api_version(&self) -> Version {self.api_version}
    pub fn correlation_id(&self) -> CorrelationId {self.correlation_id}
}
#[derive(Debug, Copy, Clone)]
pub struct Request {
    header: RequestHeader,
}
impl Request {
    fn new(header: RequestHeader) -> Self {
        Self {
            header,
        }
    }
    pub fn header(&self) -> RequestHeader {
        self.header
    }

}
impl TryFrom<&mut TcpStream> for Request {
    type Error = Error;

    fn try_from(stream: &mut TcpStream) -> Result<Self> {
        let message_size = message_size(stream)?;
        println!("message size {:?}", message_size);
        let mut request: Vec<u8> = vec![0; *message_size as usize];

        stream.read_exact(&mut request).with_context(||"not able to read stream")?;
        println!("{:?}", request);
        let correlation_id: CorrelationId = request[4..8]
            .try_into()
            .with_context(||"not able  to read stream for CorrelationId")
            .map(i32::from_be_bytes)
            .map(CorrelationId::new)?;

        let api_key = request[0..2]
            .try_into()
            .with_context(||"not able to read stream for ApiKey")
            .map(i16::from_be_bytes)
            .and_then(ApiKey::try_from)
            .map_err(|e| e.with_correlation_id(correlation_id))?;

        let api_version: Version = request[2..4]
            .try_into()
            .with_context(||"not able to read Stream")
            .map(i16::from_be_bytes)
            .and_then(Version::try_from)
            .and_then(|v| {
                match (api_key, v) {
                    (ApiKey::ApiVersions,_) => {
                        Ok(v)
                    }
                    (ApiKey::DescribeTopicPartitions, Version::V0) => {
                        Ok(v)
                    }
                    (_,_) => {
                        Err(Error::UnsupportedApiVersion(*v, None))
                    }
                }
            })
            .map_err(|e| e.with_correlation_id(correlation_id))?;

        Ok(Request::new(RequestHeader::new(api_key, api_version, correlation_id)))
    }
}

fn message_size(stream: &mut TcpStream) -> Result<MessageSize> {
    let mut result = [0u8; 4];
    stream.read_exact(&mut result).with_context(||"MessageSize is not valid")?;
    MessageSize::try_from_bytes(result)
}

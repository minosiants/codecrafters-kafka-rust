use std::ops::Deref;

use bytes::BufMut;

use crate::{Api, ApiKey, CorrelationId, Error, ErrorCode, Request, RequestBody, Result, TagBuffer, ThrottleTime, Topic, Version};

#[derive(Debug, Clone)]
pub enum ResponseBody {
    ApiVersions {
        api_versions: Vec<Api>,
        throttle_time: ThrottleTime,
        tagged_fields: TagBuffer,
    },
    DescribeTopicPartitions {
        throttle_time: ThrottleTime,
        topics: Vec<Topic>,
        next_cursor: Option<Cursor>,
    }
}
#[derive(Debug, Clone)]
pub struct Cursor(u8);
impl Cursor {
    pub fn mk(v: u8) -> Option<Self> {
        match v {
            0xff => None,
            _ => Some(Cursor(v))
        }
    }
}

impl Deref for Cursor {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone)]
pub struct Response {
    correlation_id: CorrelationId,
    body: ResponseBody,
}


impl Response {
    pub fn new(correlation_id: CorrelationId,
               body: ResponseBody) -> Self {
        Self {
            correlation_id,
            body,
        }
    }
    pub fn response(request: &Request) -> Result<Response> {
        let body = match &request.body {
            RequestBody::ApiVersions => {
                Ok(ResponseBody::ApiVersions {
                    api_versions: vec![
                        Api::new(ApiKey::ApiVersions, Version::V0, Version::V4, TagBuffer::new(0)),
                        Api::new(ApiKey::DescribeTopicPartitions, Version::V0, Version::V0, TagBuffer::new(0)),
                    ],
                    throttle_time: ThrottleTime::zero(),
                    tagged_fields: TagBuffer::zero(),
                })
            }
            RequestBody::DescribeTopicPartitions { topics, limit:_, cursor:_ } => {
                match request.header.api_version()  {
                    Version::V0 => {
                        Ok(ResponseBody::DescribeTopicPartitions {
                            throttle_time: ThrottleTime::zero(),
                            topics:topics.into_iter().map(|name|
                                Topic::unknown(name.clone())
                            ).collect(),
                            next_cursor:None
                        })
                    }
                    _  => {
                       Err(Error::UnsupportedApiVersion(2, Some(request.header.correlation_id())))
                    }
                }
            }
        };
        println!("response: {:?}", body);
        body.map(|b|{
            Response::new(request.header.correlation_id(), b)
        })
    }
}

fn with_message_size(bytes:&[u8])-> Vec<u8> {
    let mut result = (bytes.len() as u32) .to_be_bytes().to_vec();
    result.extend(bytes);
    result
}
impl From<Response> for Vec<u8> {
    fn from(value: Response) -> Self {

        match value.body {
            ResponseBody::ApiVersions {
                api_versions,
                throttle_time,
                tagged_fields
            } => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.put_i32(*value.correlation_id);
                bytes.put_i16(*ErrorCode::NoError);
                bytes.put_i8(api_versions.len() as i8 +1);
                let api_versions: Vec<u8> = api_versions.into_iter().flat_map::<Vec<u8>,_>(|e| e.into()).collect();
                bytes.extend(api_versions);
                bytes.put_i32(*throttle_time);
                bytes.put_u8(*tagged_fields);
                with_message_size(&bytes)
            }
            ResponseBody::DescribeTopicPartitions { throttle_time, topics, next_cursor } => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.put_i32(*value.correlation_id);
                bytes.put_u8(*TagBuffer::zero());
                bytes.put_i32(*throttle_time);
                bytes.put_u8(topics.len() as u8+1);
                let topics_bytes: Vec<u8> = topics.into_iter().flat_map::<Vec<u8>, _>(|e| e.into()).collect();
                bytes.extend(topics_bytes);
                bytes.put_u8(next_cursor.map(|v|*v).unwrap_or_else(||0xff));
                bytes.put_u8(*TagBuffer::zero());
                with_message_size(&bytes)

            }
        }

    }
}
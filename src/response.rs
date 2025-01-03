use bytes::BufMut;

use crate::{Api, CorrelationId, ErrorCode, MessageSize, TaggedFields};

#[derive(Debug)]
pub struct Response {
    message_size: MessageSize,
    correlation_id: CorrelationId,
    error_code: ErrorCode,
    num_of_api_keys: i8,
    api_versions: Vec<Api>,
    throttle_time_ms: i32,
    tagged_fields: TaggedFields,
}




impl Response {
   pub fn new(correlation_id: CorrelationId,
           error_code: ErrorCode,
           api_versions: Vec<Api>) -> Self {
        Self {
            message_size: MessageSize::new(7 + 4 + 2 + 1 + 4 + 1),
            correlation_id,
            error_code,
            num_of_api_keys: api_versions.len() as i8 + 1,
            api_versions,
            throttle_time_ms: 0,
            tagged_fields: TaggedFields::new(0),
        }
    }
}
impl From<Response> for Vec<u8> {
    fn from(value: Response) -> Self {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.put_i32(*value.message_size);
        bytes.put_i32(*value.correlation_id);
        bytes.put_i16(*value.error_code);
        bytes.put_i8(value.num_of_api_keys);
        let api_versions: Vec<u8> = value.api_versions.into_iter().flat_map::<Vec<u8>, _>(|e| e.into()).collect();
        bytes.extend(api_versions);
        bytes.put_i32(value.throttle_time_ms);
        bytes.put_i8(*value.tagged_fields);
        bytes
    }
}
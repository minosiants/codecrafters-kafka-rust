use std::convert::TryFrom;
use std::ops::Deref;
use bytes::BufMut;
use crate::ApiVersion::{V0, V1, V2, V3, V4};
use crate::{Error,Result};



#[derive(Debug, Copy, Clone)]
pub enum ApiVersion {
    V0,
    V1,
    V2,
    V3,
    V4,
}

impl TryFrom<i16> for ApiVersion {
    type Error = Error;
    fn try_from(value: i16) -> Result<Self> {
        use ApiVersion::{V0, V1, V2, V3, V4};
        match value {
            0 => Ok(V0),
            1 => Ok(V1),
            2 => Ok(V2),
            3 => Ok(V3),
            4 => Ok(V4),
            _ => Err(Error::UnsupportedApiVersion(value, None))
        }
    }
}

impl Deref for ApiVersion {
    type Target = i16;

    fn deref(&self) -> &Self::Target {
        use ApiVersion::{V0, V1, V2, V3, V4};
        match self {
            V0 => &0,
            V1 => &1,
            V2 => &2,
            V3 => &3,
            V4 => &4,
        }
    }
}
#[derive(Debug, Copy, Clone)]
pub enum ApiKey {
    ApiVersions
}

impl TryFrom<i16> for ApiKey {
    type Error = Error;
    fn try_from(value: i16) -> Result<Self> {
        match value {
            18 => Ok(ApiKey::ApiVersions),
            _ => Err(Error::UnsupportedApiKey(value, None))
        }
    }
}
impl Deref for ApiKey {
    type Target = i16;

    fn deref(&self) -> &Self::Target {
        match &self   {
            ApiKey::ApiVersions => { &(18i16) }
        }
    }
}
#[derive(Debug, Copy, Clone)]
pub struct MessageSize(i32);

impl MessageSize {
    pub fn new(value:i32) -> Self {
        MessageSize(value)
    }
    pub fn try_from_bytes(value:[u8;4]) -> Result<Self> {
        Ok(Self(i32::from_be_bytes(value)))
    }
}
impl AsRef<i32> for MessageSize{
    fn as_ref(&self) -> &i32 {
        &self.0
    }
}

impl Deref for MessageSize{
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<i32> for MessageSize {
    fn from(value: i32) -> Self {
        MessageSize(value)
    }
}

#[derive(Debug)]
pub struct Api(ApiKey, ApiVersion, ApiVersion, TaggedFields);
impl Api {
    pub fn new(api_key:ApiKey, min: ApiVersion, max: ApiVersion, tf:TaggedFields) -> Self {
        Self(
            api_key,
            min,
            max,
            tf,
        )
    }
    pub fn api_key(&self) -> ApiKey {
        self.0
    }
    pub fn min(&self) -> ApiVersion {
        self.1
    }
    pub fn max(&self) -> ApiVersion {
        self.2
    }
    pub fn tagged_fields(&self) -> TaggedFields {
        self.3
    }
}
impl From<Api> for Vec<u8> {
    fn from(value: Api) -> Self {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.put_i16(*value.api_key());
        bytes.put_i16(*value.min());
        bytes.put_i16(*value.max());
        bytes.put_i8(*value.tagged_fields());
        bytes
    }
}
#[derive(Debug, Copy, Clone)]
pub struct TaggedFields(i8);

impl TaggedFields {
    pub fn new(value:i8) -> Self{
        Self(value)
    }
}
impl Deref for TaggedFields {
    type Target = i8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ErrorCode {
    UnsupportedVersion,
    NoError
}
impl Deref for ErrorCode {
    type Target = i16;

    fn deref(&self) -> &Self::Target {
        match self {
            ErrorCode::UnsupportedVersion => &35i16,
            ErrorCode::NoError => &0i16
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct CorrelationId(i32);
impl CorrelationId {
    pub fn new(value: i32) -> Self {
        Self(value)
    }
}

impl Deref for CorrelationId {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
       &self.0
    }
}

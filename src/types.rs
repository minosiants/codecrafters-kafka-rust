use std::convert::TryFrom;
use std::ops::Deref;
use bytes::BufMut;
use crate::{Context, Error, Result};



#[derive(Debug, Copy, Clone)]
pub enum Version {
    V0,
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8,
    V9,
    V10,
    V11,
    V12,
    V13,
    V14,
    V15,
    V16,
}
impl Version {
    pub fn mk(bytes:&[u8]) -> Result<Self> {
        bytes.try_into()
            .with_context(||"not able to read Stream")
            .map(i16::from_be_bytes)
            .and_then(Version::try_from)
    }
}
impl TryFrom<i16> for Version {
    type Error = Error;
    fn try_from(value: i16) -> Result<Self> {
        use Version::*;
        match value {
            0 => Ok(V0),
            1 => Ok(V1),
            2 => Ok(V2),
            3 => Ok(V3),
            4 => Ok(V4),
            5 => Ok(V5),
            6 => Ok(V6),
            7 => Ok(V7),
            8 => Ok(V8),
            9 => Ok(V9),
            10 => Ok(V10),
            11 => Ok(V11),
            12 => Ok(V12),
            13 => Ok(V13),
            14 => Ok(V14),
            15 => Ok(V15),
            16 => Ok(V16),
            _ => Err(Error::UnsupportedApiVersion(value, None))
        }
    }
}

impl Deref for Version {
    type Target = i16;

    fn deref(&self) -> &Self::Target {
        use Version::*;
        match self {
            V0 => &0,
            V1 => &1,
            V2 => &2,
            V3 => &3,
            V4 => &4,
            V5 => &5,
            V6 => &6,
            V7 => &7,
            V8 => &8,
            V9 => &9,
            V10 => &10,
            V11 => &11,
            V12 => &12,
            V13 => &13,
            V14 => &14,
            V15 => &15,
            V16 => &16,
        }
    }
}
#[derive(Debug, Copy, Clone)]
pub enum ApiKey {
    ApiVersions,
    DescribeTopicPartitions,
    Fetch
}

impl ApiKey {
    pub fn mk(bytes:&[u8]) -> Result<Self> {
        bytes.try_into()
            .with_context(||"not able to read stream for ApiKey")
            .map(i16::from_be_bytes)
            .and_then(ApiKey::try_from)
    }
}

impl TryFrom<i16> for ApiKey {
    type Error = Error;
    fn try_from(value: i16) -> Result<Self> {
        match value {
            18 => Ok(ApiKey::ApiVersions),
            75 => Ok(ApiKey::DescribeTopicPartitions),
            1 => Ok(ApiKey::Fetch),
            _ => Err(Error::UnsupportedApiKey(value, None))
        }
    }
}
impl Deref for ApiKey {
    type Target = i16;

    fn deref(&self) -> &Self::Target {
        match &self   {
            ApiKey::ApiVersions =>  &18i16,
            ApiKey::DescribeTopicPartitions => &75i16,
            ApiKey::Fetch => &1i16,
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

#[derive(Debug, Copy, Clone)]
pub struct Api(ApiKey, Version, Version, TagBuffer);



impl Api {
    pub fn new(api_key:ApiKey, min: Version, max: Version, tf: TagBuffer) -> Self {
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
    pub fn min(&self) -> Version {
        self.1
    }
    pub fn max(&self) -> Version {
        self.2
    }
    pub fn tagged_fields(&self) -> TagBuffer {
        self.3
    }
}
impl From<Api> for Vec<u8> {
    fn from(value: Api) -> Self {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.put_i16(*value.api_key());
        bytes.put_i16(*value.min());
        bytes.put_i16(*value.max());
        bytes.put_u8(*value.tagged_fields());
        bytes
    }
}
#[derive(Debug, Copy, Clone)]
pub struct TagBuffer(u8);

impl TagBuffer {
    pub fn new(value:u8) -> Self{
        Self(value)
    }
    pub fn zero() -> TagBuffer {
        Self(0)
    }
}
impl Deref for TagBuffer {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ErrorCode {
    UnsupportedVersion,
    NoError,
    UnknownTopic
}
impl Deref for ErrorCode {
    type Target = i16;

    fn deref(&self) -> &Self::Target {
        match &self {
            ErrorCode::UnsupportedVersion => &35i16,
            ErrorCode::NoError => &0i16,
            ErrorCode::UnknownTopic => &3i16
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct CorrelationId(i32);
impl CorrelationId {
    pub fn new(value: i32) -> Self {
        Self(value)
    }
    pub fn mk(bytes:&[u8]) -> Result<CorrelationId> {
        bytes.try_into()
            .with_context(||"not able  to read stream for CorrelationId")
            .map(i32::from_be_bytes)
            .map(CorrelationId::new)
    }
}

impl Deref for CorrelationId {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
       &self.0
    }
}
#[derive(Debug, Clone, Copy)]
pub struct ThrottleTime(u32);

impl ThrottleTime {
    pub fn new(v:u32) -> Self {
        Self(v)
    }
    pub fn zero() -> Self{
        Self(0)
    }
}
impl Deref for ThrottleTime {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug,Clone)]
pub struct ClientId(String);

impl ClientId {
    pub fn new(str:String) -> Self {
        Self(str)
    }
    pub fn mk(bytes:&[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .context("not able mk ClientId")
            .map(ClientId::new)
    }
}

#[derive(Debug,Clone)]
pub struct Length(i16);

impl Deref for Length {
    type Target = i16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Length> for usize {
    fn from(value: Length) -> Self {
        value.0 as usize
    }
}
impl Length {
    pub fn new(v:i16) -> Self {
        Self(v)
    }
    pub fn mk(bytes:&[u8]) -> Result<Self> {
        bytes.try_into()
            .context("not able mk Length")
            .map(i16::from_be_bytes)
            .map(Length::new)
    }
}


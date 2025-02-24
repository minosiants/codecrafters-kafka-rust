use std::fmt::Display;
use std::str::Utf8Error;
use std::sync::Arc;

use thiserror::*;

use crate::CorrelationId;
use crate::Error::{ErrorWrapper, GeneralError};

pub type Result<E> = std::result::Result<E, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Unsupported Api Version {}", .0)]
    UnsupportedApiVersion(u16, Option<CorrelationId>),
    #[error("Unsupported Api Key {}", .0)]
    UnsupportedApiKey(u16, Option<CorrelationId>),
    #[error("Unknown Topic or Partition {}", .0)]
    UnknownTopicOrPartition(u16, Option<CorrelationId>),
    #[error("Error Wrapper {}", .0)]
    ErrorWrapper(String, Arc<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Failed to convert bytes to string: {0}")]
    Utf8ConversionError(#[from] Utf8Error),
    #[error("Failed to create uuid: {0}")]
    UuidError(#[from] uuid::Error),
    #[error("Unknown Batch Record Type {0}")]
    UnknownRecordType(u8),
    #[error("General Error {0}")]
    GeneralError(String),

}
impl Error {
    pub fn with_correlation_id(&self, id: CorrelationId) -> Self {
        use Error::*;
        match self {
            UnsupportedApiVersion(v, _) => {
                UnsupportedApiVersion(*v, Some(id))
            }
            UnsupportedApiKey(v, _) => {
                UnsupportedApiKey(*v, Some(id))
            }
            UnknownTopicOrPartition(v, _) => {
                UnknownTopicOrPartition(*v, Some(id))
            }
            _ => self.clone()
        }
    }
    pub fn set_correlation_id(id:CorrelationId) -> impl FnOnce(Error) -> Self {
        move |e| e.with_correlation_id(id)
    }
    pub fn general(v:&str) -> Self {
        GeneralError(v.to_string())
    }
}

pub trait Context<T, E> {
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Display + Send + Sync + 'static;
    fn with_context<C, F>(self, f: F) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}
pub trait MapTupleTwo<A, C> {

    fn map_tuple<B, F>(self, op:F) -> Result<(B,C)>
    where
        F:FnOnce(A) -> B;
    fn fmap_tuple<B, F>(self, op:F) -> Result<(B,C)>
    where
        F:FnOnce(A) -> Result<B>;

    fn first(self) -> Result<A>;
    fn second(self) -> Result<C>;
}

impl <A, C> MapTupleTwo<A,C> for Result<(A,C)> {
    fn map_tuple<B, F>(self, op: F) -> Result<(B, C)>
    where
        F: FnOnce(A) -> B
    {
        self.map(|(a,c)|(op(a),c))
    }

    fn fmap_tuple<B, F>(self, op: F) -> Result<(B, C)>
    where
        F: FnOnce(A) -> Result<B>
    {
        self.and_then(|(a,c)|op(a).map(|b| (b,c)))
    }

    fn first(self) -> Result<A> {
        self.map(|(a,_)|a)
    }
    fn second(self) -> Result<C> {
       self.map(|(_,b)|b)
    }
}
impl<T, E> Context<T, E> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
    {
        self.map_err(|e| {
            ErrorWrapper(context.to_string(), Arc::new(e))
        })
    }

    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.map_err(|e| ErrorWrapper(context().to_string(), Arc::new(e)))
    }
}

impl<T> Context<T, Error> for Option<T> {
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
    {
        self.ok_or(GeneralError(context.to_string()))
    }

    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.ok_or(GeneralError(context().to_string()))
    }
}


use std::fmt::Display;
use std::string::FromUtf8Error;
use std::sync::Arc;

use thiserror::*;

use crate::CorrelationId;
use crate::Error::GeneralError;

pub type Result<E> = std::result::Result<E, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Unsupported Api Version {}", .0)]
    UnsupportedApiVersion(i16, Option<CorrelationId>),
    #[error("Unsupported Api Key {}", .0)]
    UnsupportedApiKey(i16, Option<CorrelationId>),
    #[error("Unknown Topic or Partition {}", .0)]
    UnknownTopicOrPartition(i16, Option<CorrelationId>),
    #[error("Genera Error {}", .0)]
    GeneralError(String, Arc<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Failed to convert bytes to string: {0}")]
    Utf8ConversionError(#[from] FromUtf8Error),
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
            GeneralError(str, e) => GeneralError(str.to_string(), e.clone()),
            Utf8ConversionError(e) => Utf8ConversionError(e.clone()),
        }
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

impl<T, E> Context<T, E> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
    {
        self.map_err(|e| {
            GeneralError(context.to_string(), Arc::new(e))
        })
    }

    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.map_err(|e| GeneralError(context().to_string(), Arc::new(e)))
    }
}


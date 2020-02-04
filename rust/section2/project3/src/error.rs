use std::io;
use std::string;

use failure::Fail;

/// Error type for kvs.
#[derive(Fail, Debug)]
#[fail(display = "Error for kvs")]
pub enum KvsError {
    /// Io error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    /// Serialization for serde_json or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    /// Serialization for bincode or deserialization error.
    #[fail(display = "{}", _0)]
    Bincode(#[cause] bincode::Error),

    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] string::FromUtf8Error),

    /// Sled error.
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),

    /// Key not found error.
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// Key not found error.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    /// Key not found error.
    #[fail(display = "Different engine type from the previous one")]
    WrongEngineType,

    /// Error occurring in remote with a string error message.
    #[fail(display = "Error occurring in remote")]
    RemoteError(String),
}

impl From<io::Error> for KvsError {
    fn from(e: io::Error) -> Self {
        KvsError::Io(e)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(e: serde_json::Error) -> Self {
        KvsError::Serde(e)
    }
}

impl From<bincode::Error> for KvsError {
    fn from(e: bincode::Error) -> Self {
        KvsError::Bincode(e)
    }
}

impl From<string::FromUtf8Error> for KvsError {
    fn from(e: string::FromUtf8Error) -> Self {
        KvsError::Utf8(e)
    }
}

impl From<sled::Error> for KvsError {
    fn from(e: sled::Error) -> Self {
        KvsError::Sled(e)
    }
}

/// Wrapper result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;

use failure::Fail;
use std::io;

/// Error type for kvs.
#[derive(Fail, Debug)]
#[fail(display = "Error for kvs")]
pub enum KvsError {
    /// Io error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    /// Serde serialization or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    /// Key not found error.
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// Key not found error.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    /// Key not found error.
    #[fail(display = "Different engine type from the previous one")]
    WrongEngineType,
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

/// Wrapper result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;

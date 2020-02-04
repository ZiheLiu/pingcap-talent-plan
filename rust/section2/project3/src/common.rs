use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};

/// The command client sends to server.
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Command set.
    Set {
        /// The key which needs to be set.
        key: String,
        /// The value of this key.
        value: String,
    },
    /// Command remove.
    Remove {
        /// The key which needs to be removed.
        key: String,
    },
    /// command get.
    Get {
        /// The key which needs to be get.
        key: String,
    },
}

/// The response server responses to client.
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// Success response with a message which may be None.
    Ok(Option<String>),
    /// Error response with a error message.
    Err(String),
}

impl Response {
    /// Create response with the given error message.
    pub fn new_error<T: Display>(err: T) -> Response {
        Response::Err(err.to_string())
    }

    /// Create response with the given message which may be None.
    pub fn new_success(msg: Option<String>) -> Response {
        Response::Ok(msg)
    }
}

impl From<Response> for Result<()> {
    fn from(res: Response) -> Self {
        match res {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::RemoteError(e)),
        }
    }
}

impl From<Response> for Result<Option<String>> {
    fn from(res: Response) -> Self {
        match res {
            Response::Ok(msg) => Ok(msg),
            Response::Err(e) => Err(KvsError::RemoteError(e)),
        }
    }
}

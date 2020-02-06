use std::net;

use crate::{Request, Response, Result};

/// The kvs client.
pub struct KvsClient {
    stream: net::TcpStream,
}

impl KvsClient {
    /// Create a new `KvsClient`.
    pub fn new(addr: net::SocketAddr) -> Result<KvsClient> {
        let stream = net::TcpStream::connect(addr)?;
        Ok(KvsClient { stream })
    }

    /// Send set command to the server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Request::Set { key, value };
        bincode::serialize_into(&mut self.stream, &command)?;

        let res: Response = bincode::deserialize_from(&mut self.stream)?;
        Result::from(res)
    }

    /// Send get command to the server.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let command = Request::Get { key };
        bincode::serialize_into(&mut self.stream, &command)?;

        let res: Response = bincode::deserialize_from(&mut self.stream)?;
        Result::from(res)
    }

    /// Send remove command to the server.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let command = Request::Remove { key };
        bincode::serialize_into(&mut self.stream, &command)?;

        let res: Response = bincode::deserialize_from(&mut self.stream)?;
        Result::from(res)
    }
}

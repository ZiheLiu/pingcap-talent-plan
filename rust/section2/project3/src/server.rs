use std::net;

use log::{debug, warn};

use crate::{KvsEngine, Request, Response, Result};

/// The kvs server.
pub struct KvsServer<T: KvsEngine> {
    addr: net::SocketAddr,
    engine: T,
}

impl<T: KvsEngine> KvsServer<T> {
    /// Creates a new `KvsServer`.
    pub fn new(addr: net::SocketAddr, engine: T) -> KvsServer<T> {
        KvsServer { addr, engine }
    }

    /// Creates tcp server to listen on the given addr.
    ///
    /// It accepts connections in the main loop.
    /// After accepting connection, it read bytes from tcp stream and deserialize it into `Command`.
    /// Then use its engine to deal with this command and responses data or errors from engine.
    ///
    /// # Errors
    ///
    /// It propagates I/O, or bincode serialization and deserialization errors.
    pub fn start(&mut self) -> Result<()> {
        let listener = net::TcpListener::bind(self.addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let command = bincode::deserialize_from(&mut stream);
                    debug!("Receive from {} with {:?}", stream.peer_addr()?, command);

                    if let Err(e) = command {
                        let res = Response::new_error(e);
                        bincode::serialize_into(&mut stream, &res)?;
                        continue;
                    }

                    let res = match command.unwrap() {
                        Request::Set { key, value } => match self.engine.set(key, value) {
                            Ok(()) => Response::new_success(None),
                            Err(e) => Response::new_error(e),
                        },
                        Request::Remove { key } => match self.engine.remove(key) {
                            Ok(()) => Response::new_success(None),
                            Err(e) => Response::new_error(e),
                        },
                        Request::Get { key } => match self.engine.get(key) {
                            Ok(value) => Response::new_success(value),
                            Err(e) => Response::new_error(e),
                        },
                    };
                    bincode::serialize_into(&mut stream, &res)?;
                }
                Err(e) => {
                    warn!("Tcp accept error: {}", e);
                }
            }
        }
        Ok(())
    }
}

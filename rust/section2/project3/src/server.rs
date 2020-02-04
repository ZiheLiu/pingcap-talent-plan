use std::net;

use log::{debug, warn};

use crate::{KvsEngine, Request, Response, Result};

/// The kvs server.
pub struct KvsServer<T: KvsEngine> {
    addr: net::SocketAddr,
    engine: T,
}

impl<T: KvsEngine> KvsServer<T> {
    /// Create a new `KvsServer`.
    pub fn new(addr: net::SocketAddr, engine: T) -> KvsServer<T> {
        KvsServer { addr, engine }
    }

    /// Create tcp server to listen on the given addr.
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

                    let command = command.unwrap();
                    match command {
                        Request::Set { key, value } => {
                            let res = match self.engine.set(key, value) {
                                Ok(()) => Response::new_success(None),
                                Err(e) => Response::new_error(e),
                            };
                            bincode::serialize_into(&mut stream, &res)?;
                        }
                        Request::Remove { key } => {
                            let res = match self.engine.remove(key) {
                                Ok(()) => Response::new_success(None),
                                Err(e) => Response::new_error(e),
                            };
                            bincode::serialize_into(&mut stream, &res)?;
                        }
                        Request::Get { key } => {
                            let res = match self.engine.get(key) {
                                Ok(value) => Response::new_success(value),
                                Err(e) => Response::new_error(e),
                            };
                            bincode::serialize_into(&mut stream, &res)?;
                        }
                    }
                }
                Err(e) => {
                    warn!("Tcp accept error: {}", e);
                }
            }
        }
        Ok(())
    }
}

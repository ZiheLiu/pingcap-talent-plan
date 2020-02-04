#![deny(missing_docs)]

//! A simple key/value store in memory.

pub use client::KvsClient;
pub use common::{Request, Response};
pub use engine::{KvStore, KvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;

mod client;
mod common;
mod engine;
mod error;
mod server;

#![deny(missing_docs)]

//! A simple key/value store in memory.

pub use error::{KvsError, Result};
pub use kv::KvStore;

mod error;
mod kv;

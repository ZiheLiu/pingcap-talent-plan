#![deny(missing_docs)]

//! A simple key/value store in memory.

pub use kv::KvStore;
pub use error::{KvsError, Result};

mod kv;
mod error;

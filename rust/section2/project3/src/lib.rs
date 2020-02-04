#![deny(missing_docs)]

//! A simple key/value store in memory.

pub use engine::KvsEngine;
pub use error::{KvsError, Result};
pub use kv::KvStore;

mod engine;
mod error;
mod kv;

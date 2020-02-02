
use std::collections::HashMap;
use std::path::PathBuf;

use super::error::{KvsError, Result};

/// The `KvStore` stores string key/value pairs.
#[derive(Default)]
pub struct KvStore {

}

impl KvStore {
    /// Create a `KvStore` with empty `HashMap`.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        Err(KvsError::Unimplemented)
    }

    /// Set the value of `key` with a string `value`.
    ///
    /// If the `key` already exists, the value of it will be overwritten by the string `value`.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        Err(KvsError::Unimplemented)
    }

    /// Get the value of a `key`.
    ///
    /// Return None, if the `key` does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Err(KvsError::Unimplemented)
    }

    /// Remove a given `key`.
    pub fn remove(&mut self, key: String) -> Result<()> {
        Err(KvsError::Unimplemented)
    }
}

#![deny(missing_docs)]

//! A simple key/value store in memory.

use std::collections::HashMap;

/// The `KvStore` stores string key/value pairs.
///
/// It uses `HashMap<String, String>` to store pairs only in memory.
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Create a `KvStore` with empty `HashMap`.
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Set the value of `key` with a string `value`.
    ///
    /// If the `key` already exists, the value of it will be overwritten by the string `value`.
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Get the value of a `key`.
    ///
    /// Return None, if the `key` does not exist.
    pub fn get(&mut self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Remove a given `key`.
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}

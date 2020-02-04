use super::KvsEngine;
use crate::KvsError;

/// Default implementation by hand for `KvsEngine`.
pub struct KvStore {}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<(), KvsError> {
        unimplemented!()
    }

    fn get(&mut self, key: String) -> Result<Option<String>, KvsError> {
        unimplemented!()
    }

    fn remove(&mut self, key: String) -> Result<(), KvsError> {
        unimplemented!()
    }
}

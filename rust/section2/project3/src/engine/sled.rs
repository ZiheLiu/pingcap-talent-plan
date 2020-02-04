use super::KvsEngine;
use crate::KvsError;

/// The implementation for `KvsEngine` for the sled storage engine.
pub struct SledKvsEngine {}

impl KvsEngine for SledKvsEngine {
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

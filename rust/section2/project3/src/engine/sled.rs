use crate::{KvsEngine, KvsError, Result};

/// The implementation for `KvsEngine` for the sled storage engine.
pub struct SledKvsEngine {
    tree: sled::Db,
}

impl SledKvsEngine {
    /// Opens a sled db with the given path.
    ///
    /// # Errors
    ///
    /// It propagates sled errors.
    pub fn open(path: &std::path::Path) -> Result<SledKvsEngine> {
        let tree = sled::open(path)?;
        Ok(SledKvsEngine { tree })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.tree.insert(key, value.into_bytes())?;
        self.tree.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self
            .tree
            .get(key)?
            .map(|v| v.to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.tree.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.tree.flush()?;
        Ok(())
    }
}

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json;

use crate::{KvsError, Result};
use std::fs::OpenOptions;
use std::io::Write;

const DATA_FILENAME: &str = "0.log";

/// The `KvStore` stores string key/value pairs.
/// It persists pairs into a file, containing json string one by one.
/// Each json string represents a pair.
pub struct KvStore {
    path: PathBuf,
    writer: io::BufWriter<fs::File>,
    reader: io::BufReader<fs::File>,
    map: HashMap<String, String>,
}

impl KvStore {
    /// Open a `KvStore` with the given path.
    ///
    /// It will create all the path, if the path does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O errors.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path.join(DATA_FILENAME))?;
        let writer = io::BufWriter::new(file);
        let reader = io::BufReader::new(fs::File::open(path.join(DATA_FILENAME))?);

        Ok(KvStore {
            path,
            writer,
            reader,
            map: HashMap::new(),
        })
    }

    /// Set the value of `key` with a string `value`.
    ///
    /// If the `key` already exists, the value of it will be overwritten by the string `value`.
    ///
    /// # Errors
    ///
    /// It propagates serialization errors.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        Ok(())
    }

    /// Get the value of a `key`.
    ///
    /// Return None, if the `key` does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.load_data()?;

        Ok(self.map.get(&key).cloned())
    }

    /// Remove a given `key`.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors.
    ///
    /// It returns `KvsError::KeyNotFound` if the given key does not exist.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.load_data()?;

        if self.map.contains_key(&key) {
            let cmd = Command::Remove { key };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Load data from disk into `HashMap`.
    fn load_data(&mut self) -> Result<()> {
        let iterator =
            serde_json::Deserializer::from_reader(&mut self.reader).into_iter::<Command>();
        for cmd in iterator {
            match cmd? {
                Command::Set { key, value } => self.map.insert(key, value),
                Command::Remove { key } => self.map.remove(&key),
            };
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

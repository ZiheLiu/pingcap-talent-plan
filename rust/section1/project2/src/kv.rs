use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json;

use crate::{KvsError, Result};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

const DATA_FILENAME: &str = "0.log";

/// The `KvStore` stores string key/value pairs.
/// It persists pairs into a file, containing json string one by one.
/// Each json string represents a pair.
pub struct KvStore {
    path: PathBuf,
    writer: io::BufWriter<fs::File>,
    reader: io::BufReader<fs::File>,
    index: BTreeMap<String, CommandFilePointer>,
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
            index: BTreeMap::new(),
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

        match self.index.get(&key) {
            Some(pos) => {
                self.reader.seek(SeekFrom::Start(pos.offset))?;
                let cmd_reader = (&mut self.reader).take(pos.len);
                if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            }
            None => Ok(None),
        }
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

        if self.index.contains_key(&key) {
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
        let mut pre_pos = self.reader.seek(SeekFrom::Start(0))?;
        let mut iterator =
            serde_json::Deserializer::from_reader(&mut self.reader).into_iter::<Command>();
        while let Some(cmd) = iterator.next() {
            let pos = iterator.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => self.index.insert(
                    key,
                    CommandFilePointer {
                        offset: pre_pos,
                        len: pos - pre_pos,
                    },
                ),
                Command::Remove { key } => self.index.remove(&key),
            };
            pre_pos = pos;
        }

        Ok(())
    }
}

/// The pointer of a command in the persistence file.
struct CommandFilePointer {
    offset: u64,
    len: u64,
}

/// The command which needs to be persisted in files.
#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

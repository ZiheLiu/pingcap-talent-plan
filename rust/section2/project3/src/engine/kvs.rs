use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json;

use crate::{KvsEngine, KvsError, Result};

const DATA_FILE_EX: &str = "log";
const COMPACTION_THRESHOLD: u64 = 1024 * 256;

/// Default implementation by hand for `KvsEngine`.
///
/// The `KvStore` stores string key/value pairs.
///
/// It persists pairs into files, containing json string one by one. Each json string represents a pair.
pub struct KvStore {
    path: PathBuf,
    writer: io::BufWriter<fs::File>,
    readers: HashMap<u64, io::BufReader<fs::File>>,
    index: BTreeMap<String, LogPointer>,
    cur_file_id: u64,
    uncompacted: u64,
}

impl KvsEngine for KvStore {
    /// Set the value of `key` with a string `value`.
    ///
    /// If the `key` already exists, the value of it will be overwritten by the string `value`.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let offset = self.writer.seek(SeekFrom::End(0))?;

        // Use cloned key, because key is stored in command and index.
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        let cur_offset = self.writer.seek(SeekFrom::End(0))?;
        self.index.insert(
            key,
            LogPointer {
                offset,
                len: cur_offset - offset,
                file_id: self.cur_file_id,
            },
        );
        if !self.readers.contains_key(&self.cur_file_id) {
            self.insert_reader(self.cur_file_id)?;
        }

        self.uncompacted += cur_offset - offset;
        if self.uncompacted >= COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Get the value of a `key`.
    ///
    /// Return None, if the `key` does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(pointer) => {
                let reader = self.readers.get_mut(&pointer.file_id).unwrap();
                reader.seek(SeekFrom::Start(pointer.offset))?;
                let cmd_reader = reader.take(pointer.len);
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
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let offset = self.writer.seek(SeekFrom::End(0))?;

            let cmd = Command::Remove { key: key.clone() };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            self.index.remove(&key);
            if !self.readers.contains_key(&self.cur_file_id) {
                self.insert_reader(self.cur_file_id)?;
            }

            self.uncompacted += self.writer.seek(SeekFrom::End(0))? - offset;
            if self.uncompacted >= COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

impl KvStore {
    /// Open a `KvStore` with the given path.
    ///
    /// It will create all the path, if the path does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();
        let mut uncompacted = 0;
        let file_ids = Self::sorted_file_ids(&path)?;
        for &file_id in &file_ids {
            let mut reader = io::BufReader::new(fs::File::open(Self::log_path(&path, file_id))?);
            uncompacted += Self::load_index(&mut reader, &mut index, file_id)?;
            readers.insert(file_id, reader);
        }

        let cur_file_id = *file_ids.last().unwrap_or(&0);
        let writer = Self::new_log_writer(&path, cur_file_id)?;

        Ok(KvStore {
            path,
            writer,
            readers,
            index,
            cur_file_id,
            uncompacted,
        })
    }

    /// Compact all log files into one file.
    ///
    /// Write compacted set commands into the log file with id `self.cur_file_id + 1` using index.
    /// Change current file with id from `self.cur_file_id` to `self.cur_file_id + 2`.
    ///
    /// # Errors
    ///
    /// It propagates I/O errors.
    fn compact(&mut self) -> Result<()> {
        let compaction_file_id = self.cur_file_id + 1;
        self.cur_file_id += 2;

        let mut compaction_writer = Self::new_log_writer(&self.path, compaction_file_id)?;
        self.writer = Self::new_log_writer(&self.path, self.cur_file_id)?;
        self.insert_reader(compaction_file_id)?;

        let mut pre_offset = compaction_writer.seek(SeekFrom::Current(0))?;

        for pointer in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&pointer.file_id)
                .expect("readers does not contain this file_id");
            reader.seek(SeekFrom::Start(pointer.offset))?;
            let mut cmd_reader = reader.take(pointer.len);

            io::copy(&mut cmd_reader, &mut compaction_writer)?;

            let cur_offset = compaction_writer.seek(SeekFrom::Current(0))?;
            pointer.file_id = compaction_file_id;
            pointer.offset = pre_offset;
            pointer.len = cur_offset - pre_offset;
            pre_offset = cur_offset;
        }

        let stale_file_ids: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&file_id| file_id < compaction_file_id)
            .cloned()
            .collect();
        for file_id in stale_file_ids {
            fs::remove_file(Self::log_path(&self.path, file_id))?;
            self.readers.remove(&file_id);
        }

        self.uncompacted = 0;

        Ok(())
    }

    /// Insert file reader ("self.path/<file_id>.log")) to readers.
    ///
    /// # Errors
    ///
    /// It propagates I/O errors.
    fn insert_reader(&mut self, file_id: u64) -> Result<()> {
        self.readers.insert(
            file_id,
            io::BufReader::new(fs::File::open(Self::log_path(&self.path, file_id))?),
        );
        Ok(())
    }

    /// Load index from disk into `HashMap` and return the number of read bytes.
    ///
    /// # Errors
    ///
    /// It propagates I/O and deserialization errors.
    fn load_index(
        reader: &mut BufReader<File>,
        index: &mut BTreeMap<String, LogPointer>,
        file_id: u64,
    ) -> Result<u64> {
        let mut pre_offset = reader.seek(SeekFrom::Start(0))?;
        let mut iterator = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
        while let Some(cmd) = iterator.next() {
            let offset = iterator.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => index.insert(
                    key,
                    LogPointer {
                        offset: pre_offset,
                        len: offset - pre_offset,
                        file_id,
                    },
                ),
                Command::Remove { key } => index.remove(&key),
            };
            pre_offset = offset;
        }

        Ok(pre_offset)
    }

    /// Get sorted file id list in the given path.
    ///
    /// Search files with pattern "<file_id>.log" and return the sorted file_id list.
    ///
    /// # Errors
    ///
    /// It propagates I/O errors.
    fn sorted_file_ids(path: &PathBuf) -> Result<Vec<u64>> {
        let mut file_ids: Vec<u64> = fs::read_dir(path)?
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some(OsStr::new(DATA_FILE_EX)))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(&format!(".{}", DATA_FILE_EX)))
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();
        file_ids.sort_unstable();
        Ok(file_ids)
    }

    /// Create new log file ("path/<file_id>.log") writer.
    ///
    /// # Errors
    ///
    /// It propagates I/O and deserialization errors.
    fn new_log_writer(path: &PathBuf, file_id: u64) -> Result<io::BufWriter<File>> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(Self::log_path(path, file_id))?;
        Ok(io::BufWriter::new(file))
    }

    fn log_path(path: &PathBuf, file_id: u64) -> PathBuf {
        path.join(format!("{}.{}", file_id, DATA_FILE_EX))
    }
}

/// The pointer of a command in the persistence file.
#[derive(Debug)]
struct LogPointer {
    offset: u64,
    len: u64,
    file_id: u64,
}

/// The command which needs to be persisted in files.
#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

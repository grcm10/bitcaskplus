use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufWriter};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

pub mod db_read;
pub mod db_write;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
struct DataReader {
    file: Arc<RwLock<File>>,
    cursor: u64,
}

impl DataReader {
    pub fn new(f: File, c: u64) -> Self {
        Self {
            file: Arc::new(RwLock::new(f)),
            cursor: c,
        }
    }

    pub fn read_data(&mut self, pos: u64) -> io::Result<(u32, Vec<u8>)> {
        let file_guard = self
            .file
            .read()
            .map_err(|_| std::io::Error::other("Lock poisoned"))?;
        let mut header_buf = [0u8; 12];
        file_guard.read_exact_at(&mut header_buf, pos)?;

        let expected_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let data_len = u64::from_le_bytes(header_buf[4..12].try_into().unwrap());
        if data_len >= 10 * COMPACTION_THRESHOLD {
            return Err(io::Error::other("over capacity"));
        }
        let mut buffer = vec![0u8; data_len as usize];
        file_guard.read_exact_at(&mut buffer, pos + 12)?;
        Ok((expected_crc, buffer))
    }
}

impl Iterator for DataReader {
    type Item = io::Result<(Command, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.read_data(self.cursor) {
            Ok((expect_crc, buf)) => {
                // check if the data is corrupted.
                let actual_crc = crc32fast::hash(&buf);
                if expect_crc != actual_crc {
                    return Some(Err(io::Error::other("crc mismatch")));
                }
                let res = serde_json::from_slice(&buf)
                    .map(|cmd| (cmd, buf.len() as u64))
                    .map_err(|e| io::Error::other(e.to_string()));
                let ret = &res;
                let (_, data_len) = ret.as_ref().ok()?;
                self.cursor += 12 + data_len;
                Some(res)
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommandPos {
    pos: u64,
    len: u64,
}

#[derive(Debug)]
pub struct BitCaskPlus {
    path: PathBuf,
    map: Arc<RwLock<HashMap<String, CommandPos>>>,
    writer: Arc<Mutex<BufWriter<File>>>,
    reader: DataReader,
    uncompacted: u64,
}

impl BitCaskPlus {
    pub fn new(path: PathBuf) -> Self {
        let log_path = path.join("bitcaskplus.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)
            .expect("can't open or create the file");
        Self {
            path,
            map: Arc::new(RwLock::new(HashMap::new())),
            writer: Arc::new(Mutex::new(BufWriter::new(
                file.try_clone().expect("clone failed"),
            ))),
            reader: DataReader::new(file, 0),
            uncompacted: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use walkdir::WalkDir;

    #[test]
    fn hash_map_works() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned())?;
        store.set("key2".to_owned(), "value2".to_owned())?;

        assert_eq!(store.get("key1")?, Some("value1".to_string()));
        assert_eq!(store.get("key2")?, Some("value2".to_string()));

        Ok(())
    }

    // Should get previously stored value.
    #[test]
    fn get_stored_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path())?;
        store.set("key1".to_owned(), "value1".to_owned())?;
        store.set("key2".to_owned(), "value2".to_owned())?;

        assert_eq!(store.get("key1")?, Some("value1".to_string()));
        assert_eq!(store.get("key2")?, Some("value2".to_string()));

        // Open from disk again and check persistent data.
        drop(store);

        let mut store = BitCaskPlus::open(temp_dir.path())?;
        assert_eq!(store.get("key1")?, Some("value1".to_string()));
        assert_eq!(store.get("key2")?, Some("value2".to_string()));

        Ok(())
    }

    // Should overwrite existent value.
    #[test]
    fn overwrite_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path())?;

        store.set("key1".to_string(), "value1".to_string())?;
        assert_eq!(store.get("key1")?, Some("value1".to_string()));
        store.set("key1".to_string(), "value2".to_string())?;
        assert_eq!(store.get("key1")?, Some("value2".to_string()));

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = BitCaskPlus::open(temp_dir.path())?;
        assert_eq!(store.get("key1")?, Some("value2".to_string()));
        store.set("key1".to_string(), "value3".to_string())?;
        assert_eq!(store.get("key1")?, Some("value3".to_string()));

        Ok(())
    }

    // Should get `None` when getting a non-existent key.
    #[test]
    fn get_non_existent_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path())?;

        store.set("key1".to_string(), "value1".to_string())?;
        assert_eq!(store.get("key2")?, None);

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = BitCaskPlus::open(temp_dir.path())?;
        assert_eq!(store.get("key2")?, None);

        Ok(())
    }

    #[test]
    fn remove_non_existent_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path())?;
        assert!(store.remove("key1").is_err());
        Ok(())
    }

    #[test]
    fn remove_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path())?;
        store.set("key1".to_string(), "value1".to_string())?;
        assert!(store.remove("key1").is_ok());
        assert_eq!(store.get("key1")?, None);
        Ok(())
    }

    // Insert data until total size of the directory decreases.
    // Test data correctness after compaction.
    #[test]
    fn compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path())?;
        let dir_size = || {
            let entries = WalkDir::new(temp_dir.path()).into_iter();
            let len: walkdir::Result<u64> = entries
                .map(|res| {
                    res.and_then(|entry| entry.metadata())
                        .map(|metadata| metadata.len())
                })
                .sum();
            len.expect("fail to get directory size")
        };

        let mut current_size = dir_size();
        for iter in 0..1000 {
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                let value = format!("{}", iter);
                store.set(key, value)?;
            }

            let new_size = dir_size();
            if new_size > current_size {
                current_size = new_size;
                continue;
            }
            // Compaction triggered.
            drop(store);
            // reopen and check content.
            let mut store = BitCaskPlus::open(temp_dir.path())?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.get(&key)?, Some(format!("{}", iter)));
            }
            return Ok(());
        }

        panic!("No compaction detected");
    }
}

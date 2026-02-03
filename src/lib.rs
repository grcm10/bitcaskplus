use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::PathBuf;

pub mod db_read;
pub mod db_write;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommandPos {
    pos: u64,
    len: u64,
}

#[derive(Debug)]
pub struct BitCaskPlus {
    path: PathBuf,
    map: HashMap<String, CommandPos>,
    writer: BufWriter<File>,
    uncompacted: u64,
}

impl BitCaskPlus {
    pub fn new() -> Self {
        let path = std::env::current_dir().expect("can't get current dir");
        let log_path = path.join("bitcaskplus.db");
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&log_path)
            .expect("can't open or create the file");

        Self {
            path,
            map: HashMap::new(),
            writer: BufWriter::new(file),
            uncompacted: 0,
        }
    }
}

impl Default for BitCaskPlus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use walkdir::WalkDir;

    #[test]
    fn hash_map_works() -> Result<()> {
        let mut store = BitCaskPlus::new();

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
        let store = BitCaskPlus::open(temp_dir.path())?;
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
        let store = BitCaskPlus::open(temp_dir.path())?;
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
            let store = BitCaskPlus::open(temp_dir.path())?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.get(&key)?, Some(format!("{}", iter)));
            }
            return Ok(());
        }

        panic!("No compaction detected");
    }
}

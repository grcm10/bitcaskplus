use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, BufWriter};
use tokio::sync::Mutex;

pub mod db_read;
pub mod db_write;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
struct DataReader {
    file: Arc<tokio::sync::RwLock<File>>,
    cursor: u64,
}

impl DataReader {
    pub fn new(f: File, c: u64) -> Self {
        Self {
            file: Arc::new(tokio::sync::RwLock::new(f)),
            cursor: c,
        }
    }

    pub async fn read_data(&mut self, pos: u64) -> io::Result<(u32, Vec<u8>)> {
        let f = self.file.clone();
        let mut file_guard = f.write().await;
        let mut header_buf = [0u8; 12];
        file_guard.seek(io::SeekFrom::Start(pos)).await?;
        file_guard.read_exact(&mut header_buf).await?;

        let expected_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let data_len = u64::from_le_bytes(header_buf[4..12].try_into().unwrap());
        if data_len >= 10 * COMPACTION_THRESHOLD {
            return Err(io::Error::other("over capacity"));
        }
        let mut buffer = vec![0u8; data_len as usize];
        file_guard.read_exact(&mut buffer).await?;
        Ok((expected_crc, buffer))
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
    map: Arc<std::sync::RwLock<HashMap<String, CommandPos>>>,
    writer: Arc<Mutex<BufWriter<File>>>,
    reader: DataReader,
    uncompacted: u64,
}

impl BitCaskPlus {
    pub async fn new(path: PathBuf) -> Result<Self> {
        let log_path = path.join("bitcaskplus.db");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)
            .await?;

        Ok(Self {
            path,
            map: Arc::new(std::sync::RwLock::new(HashMap::new())),
            writer: Arc::new(Mutex::new(BufWriter::new(file.try_clone().await?))),
            reader: DataReader::new(file, 0),
            uncompacted: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use walkdir::WalkDir;

    #[tokio::test]
    async fn hash_map_works() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;

        store.set("key1".to_owned(), "value1".to_owned()).await?;
        store.set("key2".to_owned(), "value2".to_owned()).await?;

        assert_eq!(store.get("key1").await?, Some("value1".to_string()));
        assert_eq!(store.get("key2").await?, Some("value2".to_string()));

        Ok(())
    }

    // Should get previously stored value.
    #[tokio::test]
    async fn get_stored_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;
        store.set("key1".to_owned(), "value1".to_owned()).await?;
        store.set("key2".to_owned(), "value2".to_owned()).await?;

        assert_eq!(store.get("key1").await?, Some("value1".to_string()));
        assert_eq!(store.get("key2").await?, Some("value2".to_string()));

        // Open from disk again and check persistent data.
        drop(store);

        let mut store = BitCaskPlus::open(temp_dir.path()).await?;
        assert_eq!(store.get("key1").await?, Some("value1".to_string()));
        assert_eq!(store.get("key2").await?, Some("value2".to_string()));

        Ok(())
    }

    // Should overwrite existent value.
    #[tokio::test]
    async fn overwrite_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;

        store.set("key1".to_string(), "value1".to_string()).await?;
        assert_eq!(store.get("key1").await?, Some("value1".to_string()));
        store.set("key1".to_string(), "value2".to_string()).await?;
        assert_eq!(store.get("key1").await?, Some("value2".to_string()));

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;
        assert_eq!(store.get("key1").await?, Some("value2".to_string()));
        store.set("key1".to_string(), "value3".to_string()).await?;
        assert_eq!(store.get("key1").await?, Some("value3".to_string()));

        Ok(())
    }

    // Should get `None` when getting a non-existent key.
    #[tokio::test]
    async fn get_non_existent_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;

        store.set("key1".to_string(), "value1".to_string()).await?;
        assert_eq!(store.get("key2").await?, None);

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;
        assert_eq!(store.get("key2").await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn remove_non_existent_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;
        assert!(store.remove("key1").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn remove_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;
        store.set("key1".to_string(), "value1".to_string()).await?;
        assert!(store.remove("key1").await.is_ok());
        assert_eq!(store.get("key1").await?, None);
        Ok(())
    }

    // Insert data until total size of the directory decreases.
    // Test data correctness after compaction.
    #[tokio::test]
    async fn compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitCaskPlus::open(temp_dir.path()).await?;
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
                store.set(key, value).await?;
            }

            let new_size = dir_size();
            if new_size > current_size {
                current_size = new_size;
                continue;
            }
            // Compaction triggered.
            drop(store);
            // reopen and check content.
            let mut store = BitCaskPlus::open(temp_dir.path()).await?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.get(&key).await?, Some(format!("{}", iter)));
            }
            return Ok(());
        }

        panic!("No compaction detected");
    }
}

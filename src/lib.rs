use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
#[warn(unused_imports)]
use tempfile::TempDir;
use walkdir::WalkDir;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug)]
struct CommandPos {
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

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: val,
        };
        let bytes = postcard::to_stdvec(&cmd)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let len = bytes.len() as u64;
        self.writer.flush()?;
        let pos = self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(&len.to_le_bytes())?; // little indian
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        let record_len = 8 + len;
        if let Some(old_pos) = self.map.insert(
            key,
            CommandPos {
                pos,
                len: record_len,
            },
        ) {
            self.uncompacted += old_pos.len;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        if let Some(pos_info) = self.map.get(&key.to_string()) {
            let mut file = fs::File::open(self.path.join("bitcaskplus.db"))?;
            file.seek(std::io::SeekFrom::Start(pos_info.pos))?;

            let mut header = [0u8; 8];
            file.read_exact(&mut header);
            let data_len = u64::from_le_bytes(header);
            let mut buffer = vec![0u8; data_len as usize];
            file.read_exact(&mut buffer);
            let cmd: Command = postcard::from_bytes(&buffer)
                .map_err(|e| format!("Postcard deserialization error: {}", e))?;

            if let Command::Set { value, .. } = cmd {
                return Ok(Some(value));
            } else {
                return Ok(None);
            }
        }
        Ok(None)
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        if !self.map.contains_key(key) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "KeyNotFound").into());
        }

        let key_str = key.to_string();
        let cmd = Command::Remove {
            key: key_str.clone(),
        };
        let bytes = postcard::to_stdvec(&cmd)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let len = bytes.len() as u64;
        self.writer.flush()?;
        self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(&len.to_le_bytes())?; // little indian
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        if let Some(old_pos) = self.map.remove(&key_str) {
            self.uncompacted += old_pos.len + (8 + len);
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;

        let log_path = path.join("bitcaskplus.db");
        let mut map = HashMap::new();
        let mut uncompacted = 0;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_path)?;

        let mut reader = io::BufReader::new(&file);
        let mut pos: u64 = 0;
        loop {
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(_) => {
                    let data_len = u64::from_le_bytes(header);
                    let mut buffer = vec![0u8; data_len as usize];
                    reader.read_exact(&mut buffer);
                    let cmd: Command = postcard::from_bytes(&buffer)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    match cmd {
                        Command::Set { key, .. } => {
                            if let Some(old_pos) = map.insert(
                                key,
                                CommandPos {
                                    pos,
                                    len: 8 + data_len,
                                },
                            ) {
                                uncompacted += old_pos.len;
                            }
                        }
                        Command::Remove { key } => {
                            if let Some(old_pos) = map.remove(&key) {
                                uncompacted += old_pos.len + (8 + data_len);
                            }
                        }
                    }
                    pos += 8 + data_len;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        let mut file = file;
        file.seek(io::SeekFrom::End(0))?;
        let writer = io::BufWriter::new(file);
        Ok(Self {
            path,
            map,
            writer,
            uncompacted,
        })
    }

    pub fn compaction(&mut self) -> Result<()> {
        self.writer.flush()?;
        let compact_path = self.path.join("bitcaskplus.db.compact");
        let log_path = self.path.join("bitcaskplus.db");

        let mut new_writer = BufWriter::new(File::create(&compact_path)?);
        let mut old_file = File::open(&log_path)?;
        let mut new_pos = 0;
        let mut new_map = HashMap::new();

        for (key, pos_info) in &self.map {
            // get len and data
            old_file.seek(SeekFrom::Start(pos_info.pos))?;
            let mut header = [0u8; 8];
            old_file.read_exact(&mut header);
            let data_len = u64::from_le_bytes(header);
            let mut buffer = vec![0u8; data_len as usize];
            old_file.read_exact(&mut buffer);

            new_writer.write_all(&header)?;
            new_writer.write_all(&buffer)?;

            new_map.insert(
                key.clone(),
                CommandPos {
                    pos: new_pos,
                    len: 8 + data_len,
                },
            );

            new_pos += 8 + data_len;
        }

        new_writer.flush()?;
        drop(new_writer);
        drop(old_file);
        fs::rename(&compact_path, &log_path)?;

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&log_path)?;
        self.writer = BufWriter::new(file);
        self.map = new_map;
        self.uncompacted = 0;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
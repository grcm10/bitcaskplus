#[warn(unused_imports)]
use std::collections::HashMap;
use std::fs::{OpenOptions, File};
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;
use walkdir::WalkDir;
use std::io::{Write, BufRead, BufReader, BufWriter, SeekFrom, Read, Seek};
use std::io;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug)]
struct CommandPos {
    pos: u64,
    len: u64,
}

#[derive(Debug)]
pub struct BitcaskPlus {
    path: PathBuf,
    map: HashMap<String, CommandPos>,
    writer: BufWriter<File>,
    uncompacted: u64,
}

impl BitcaskPlus {
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
        self.writer.flush()?;
        let pos = self.writer.seek(SeekFrom::End(0))?;
        let cmd = format!("{},{}\n", key, val);
        let len = cmd.as_bytes().len() as u64;
        self.writer.write_all(cmd.as_bytes());
        self.writer.flush()?;

        if let Some(old_pos) = self.map.insert(key, CommandPos{pos,len}) {
            self.uncompacted += old_pos.len + len;
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
            
            let mut handle = file.take(pos_info.len);
            let mut buffer = String::new();
            handle.read_to_string(&mut buffer)?;
            let parts: Vec<&str> = buffer.trim_end().splitn(2,',').collect();
            if parts.len() == 2 {
                return Ok(Some(parts[1].to_string()));
            }
        }
        Ok(None)
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        if !self.map.contains_key(&key.to_string()) {
            return Err("KeyNotFound".into());
        }
        let log_entry = format!("{},rm\n", key);
        let len = log_entry.as_bytes().len() as u64;
        self.writer.flush()?;
        self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(log_entry.as_bytes())?;
        self.writer.flush()?;

        if let Some(old_pos) = self.map.remove(&key.to_string()) {
            self.uncompacted += old_pos.len + len;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }

    pub fn open(path:impl Into<PathBuf>) -> io::Result<Self> {
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
    
        let mut reader = BufReader::new(&file);
        let mut pos:u64 = 0;
        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            let len = line.len() as u64;
            let parts: Vec<&str> = line.trim_end().splitn(2,',').collect();
            if parts.len() == 2 {
                let key = parts[0].to_string();
                let val = parts[1];
                if val == "rm" {
                    map.remove(&key);
                } else {
                    map.insert(key, CommandPos { pos, len });
                }
            }
            pos+=len;
            line.clear();
        }

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

        for (key,pos_info) in &self.map {
            old_file.seek(SeekFrom::Start(pos_info.pos))?;
            let mut buffer = vec![0; pos_info.len as usize];
            old_file.read_exact(&mut buffer)?;
            new_writer.write_all(&buffer)?;
            new_map.insert(key.clone(), CommandPos {
                pos: new_pos,
                len: pos_info.len,
            });
            new_pos += pos_info.len;
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
        let mut store = BitcaskPlus::new();

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
        let mut store = BitcaskPlus::open(temp_dir.path())?;
        store.set("key1".to_owned(), "value1".to_owned())?;
        store.set("key2".to_owned(), "value2".to_owned())?;

        assert_eq!(store.get("key1")?, Some("value1".to_string()));
        assert_eq!(store.get("key2")?, Some("value2".to_string()));

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = BitcaskPlus::open(temp_dir.path())?;
        assert_eq!(store.get("key1")?, Some("value1".to_string()));
        assert_eq!(store.get("key2")?, Some("value2".to_string()));

        Ok(())
    }

    // Should overwrite existent value.
    #[test]
    fn overwrite_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitcaskPlus::open(temp_dir.path())?;

        store.set("key1".to_string(), "value1".to_string())?;
        assert_eq!(store.get("key1")?, Some("value1".to_string()));
        store.set("key1".to_string(), "value2".to_string())?;
        assert_eq!(store.get("key1")?, Some("value2".to_string()));

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = BitcaskPlus::open(temp_dir.path())?;
        assert_eq!(store.get("key1")?, Some("value2".to_string()));
        store.set("key1".to_string(), "value3".to_string())?;
        assert_eq!(store.get("key1")?, Some("value3".to_string()));

        Ok(())
    }

    // Should get `None` when getting a non-existent key.
    #[test]
    fn get_non_existent_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitcaskPlus::open(temp_dir.path())?;

        store.set("key1".to_string(), "value1".to_string())?;
        assert_eq!(store.get("key2")?, None);

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = BitcaskPlus::open(temp_dir.path())?;
        assert_eq!(store.get("key2")?, None);

        Ok(())
    }

    #[test]
    fn remove_non_existent_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitcaskPlus::open(temp_dir.path())?;
        assert!(store.remove("key1").is_err());
        Ok(())
    }

    #[test]
    fn remove_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = BitcaskPlus::open(temp_dir.path())?;
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
        let mut store = BitcaskPlus::open(temp_dir.path())?;
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
            let mut store = BitcaskPlus::open(temp_dir.path())?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.get(&key)?, Some(format!("{}", iter)));
            }
            return Ok(());
        }

        panic!("No compaction detected");
    }
}
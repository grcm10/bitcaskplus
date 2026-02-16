use crate::{BitCaskPlus, Command, CommandPos, DataReader, Result};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

struct HintReader<R: io::Read> {
    reader: R,
}

impl<R: io::Read> HintReader<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R: io::Read> Iterator for HintReader<R> {
    type Item = io::Result<(String, CommandPos)>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut header = [0u8; 4];
        match self.reader.read_exact(&mut header) {
            Ok(_) => {
                let len = u32::from_le_bytes(header) as usize;
                let mut buffer = vec![0u8; len];
                if let Err(e) = self.reader.read_exact(&mut buffer) {
                    return Some(Err(e));
                }
                let res =
                    serde_json::from_slice(&buffer).map_err(|e| io::Error::other(e.to_string()));

                Some(res)
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl BitCaskPlus {
    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let pos_info = self.map.read().unwrap().get(key).cloned();
        let p = match pos_info {
            Some(p) => p,
            None => return Ok(None),
        };

        let (expect_crc, buffer) = { self.reader.read_data(p.pos)? };
        let actual_crc = crc32fast::hash(&buffer);
        if actual_crc != expect_crc {
            return Err(io::Error::other("crc mismatch").into());
        }
        let cmd = serde_json::from_slice(&buffer)
            .map_err(|e| format!("Serde json deserialization error: {}", e))?;
        if let Command::Set { value, .. } = cmd {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path: PathBuf = path.into();
        std::fs::create_dir_all(&path)?;
        let log_path = path.join("bitcaskplus.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;

        let mut map = HashMap::new();
        let mut uncompacted = 0;

        let hint_path = path.join("bitcaskplus.hint");
        if hint_path.exists() {
            let reader = io::BufReader::new(File::open(&hint_path)?);
            let new_map: io::Result<HashMap<String, CommandPos>> =
                HintReader::new(reader).collect();
            map.extend(new_map?);
        }

        let mut pos: u64 = 0;
        let mut reader = DataReader::new(file.try_clone().expect("clone failed"), 0);

        for result in reader.by_ref() {
            match result {
                Ok((cmd, data_len)) => {
                    let entry_len = 12 + data_len;

                    match cmd {
                        Command::Set { key, .. } => {
                            if let Some(old_pos) = map.insert(
                                key,
                                CommandPos {
                                    pos,
                                    len: entry_len,
                                },
                            ) {
                                uncompacted += old_pos.len;
                            }
                        }
                        Command::Remove { key } => {
                            if let Some(old_pos) = map.remove(&key) {
                                uncompacted += old_pos.len;
                            }
                            uncompacted += entry_len;
                        }
                    }
                    pos += entry_len;
                }
                Err(e) => {
                    eprintln!("read data error：{}", e);
                    break;
                }
            }
        }

        let mut f_for_writer = file.try_clone().expect("clone failed");
        f_for_writer.seek(io::SeekFrom::End(0))?;
        let writer = io::BufWriter::new(f_for_writer);
        let res = {
            Self {
                path,
                map: Arc::new(RwLock::new(map)),
                writer: Arc::new(Mutex::new(writer)),
                reader,
                uncompacted,
            }
        };

        Ok(res)
    }
}

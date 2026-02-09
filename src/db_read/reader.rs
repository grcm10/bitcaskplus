use crate::{BitCaskPlus, Command, CommandPos, Result};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::{Read, Seek};
use std::path::PathBuf;

impl BitCaskPlus {
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        if let Some(pos_info) = self.map.get(key) {
            let mut file = fs::File::open(self.path.join("bitcaskplus.db"))?;
            file.seek(std::io::SeekFrom::Start(pos_info.pos))?;

            let mut checksum = [0u8; 4];
            file.read_exact(&mut checksum)?;
            let expected_crc = u32::from_le_bytes(checksum);

            let mut header = [0u8; 8];
            let _ = file.read_exact(&mut header);
            let data_len = u64::from_le_bytes(header);
            let mut buffer = vec![0u8; data_len as usize];
            let _ = file.read_exact(&mut buffer);

            let actual_crc = crc32fast::hash(&buffer);
            if actual_crc != expected_crc {
                return Err(io::Error::other("crc mismatch").into());
            }
            let cmd = serde_json::from_slice(&buffer)
                .map_err(|e| format!("Serde json deserialization error: {}", e))?;

            if let Command::Set { value, .. } = cmd {
                return Ok(Some(value));
            } else {
                return Ok(None);
            }
        }
        Ok(None)
    }

    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;

        let log_path = path.join("bitcaskplus.db");
        let mut map = HashMap::new();
        let mut uncompacted = 0;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;

        let hint_path = path.join("bitcaskplus.hint");
        if hint_path.exists() {
            let mut reader = io::BufReader::new(File::open(&hint_path)?);
            loop {
                let mut header = [0u8; 4];
                match reader.read_exact(&mut header) {
                    Ok(_) => {
                        let len = u32::from_le_bytes(header) as usize;
                        let mut buffer = vec![0u8; len];
                        reader.read_exact(&mut buffer)?;
                        let (key, pos): (String, CommandPos) = serde_json::from_slice(&buffer)
                            .map_err(|e| io::Error::other(e.to_string()))?;
                        map.insert(key, pos);
                    }
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        let mut reader = io::BufReader::new(&file);
        let mut pos: u64 = 0;
        loop {
            let mut checksum = [0u8; 4];
            match reader.read_exact(&mut checksum) {
                Ok(_) => {
                    let expected_crc = u32::from_le_bytes(checksum);
                    let mut header = [0u8; 8];
                    reader.read_exact(&mut header)?;
                    let data_len = u64::from_le_bytes(header);
                    let mut buffer = vec![0u8; data_len as usize];
                    let _ = reader.read_exact(&mut buffer);

                    // check if the data is corrupted.
                    let actual_crc = crc32fast::hash(&buffer);
                    if expected_crc != actual_crc {
                        return Err(io::Error::other("crc mismatch"));
                    }

                    let cmd = serde_json::from_slice(&buffer)
                        .map_err(|e| io::Error::other(e.to_string()))?;

                    match cmd {
                        Command::Set { key, .. } => {
                            if let Some(old_pos) = map.insert(
                                key,
                                CommandPos {
                                    pos,
                                    len: 4 + 8 + data_len,
                                },
                            ) {
                                uncompacted += old_pos.len;
                            }
                        }
                        Command::Remove { key } => {
                            if let Some(old_pos) = map.remove(&key) {
                                uncompacted += old_pos.len + (4 + 8 + data_len);
                            }
                        }
                    }
                    pos += 4 + 8 + data_len;
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
}

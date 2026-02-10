use crate::{BitCaskPlus, Command, CommandPos, Result};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::{Read, Seek};
use std::path::PathBuf;

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
struct DataReader {
    file: File,
}

impl DataReader {
    pub fn read_data(&mut self) -> io::Result<(u32, Vec<u8>)> {
        let mut checksum = [0u8; 4];
        self.file.read_exact(&mut checksum)?;
        let expected_crc = u32::from_le_bytes(checksum);

        let mut header = [0u8; 8];
        let _ = self.file.read_exact(&mut header);
        let data_len = u64::from_le_bytes(header);
        let mut buffer = vec![0u8; data_len as usize];
        let _ = self.file.read_exact(&mut buffer);
        Ok((expected_crc, buffer))
    }
}

impl Iterator for DataReader {
    type Item = io::Result<(Command, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.read_data() {
            Ok((expect_crc, buf)) => {
                // check if the data is corrupted.
                let actual_crc = crc32fast::hash(&buf);
                if expect_crc != actual_crc {
                    return Some(Err(io::Error::other("crc mismatch")));
                }
                let res = serde_json::from_slice(&buf)
                    .map(|cmd| (cmd, buf.len() as u64))
                    .map_err(|e| io::Error::other(e.to_string()));
                Some(res)
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl BitCaskPlus {
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        if let Some(pos_info) = self.map.get(key) {
            let mut d = DataReader {
                file: fs::File::open(self.path.join("bitcaskplus.db"))?,
            };
            d.file.seek(std::io::SeekFrom::Start(pos_info.pos))?;
            let (expect_crc, buffer) = d.read_data()?;
            let actual_crc = crc32fast::hash(&buffer);
            if actual_crc != expect_crc {
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
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;

        let hint_path = path.join("bitcaskplus.hint");
        if hint_path.exists() {
            let reader = io::BufReader::new(File::open(&hint_path)?);
            let new_map: io::Result<HashMap<String, CommandPos>> =
                HintReader::new(reader).collect();
            map.extend(new_map?);
        }

        let mut pos: u64 = 0;
        let d = DataReader {
            file: file.try_clone()?,
        };

        for result in d {
            match result {
                Ok((cmd, data_len)) => {
                    let entry_len = 4 + 8 + data_len;

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

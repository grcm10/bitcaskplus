use crate::{BitCaskPlus, Command, CommandPos, DataReader, Result};
use async_stream::stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

impl BitCaskPlus {
    pub async fn get(&mut self, key: &str) -> Result<Option<String>> {
        let pos_info = self.map.read().unwrap().get(key).cloned();
        let p = match pos_info {
            Some(p) => p,
            None => return Ok(None),
        };

        let (expect_crc, buffer) = { self.reader.read_data(p.pos).await? };
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

    pub async fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path: PathBuf = path.into();
        tokio::fs::create_dir_all(&path).await?;
        let log_path = path.join("bitcaskplus.db");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)
            .await?;

        let mut map = HashMap::new();
        let mut uncompacted = 0;

        let hint_path = path.join("bitcaskplus.hint");
        if hint_path.exists() {
            let mut reader = tokio::io::BufReader::new(File::open(&hint_path).await?);

            let mut new_map: HashMap<String, CommandPos> = HashMap::new();
            let s = stream! {
                loop {
                    let mut header = [0u8; 4];
                    match reader.read_exact(&mut header).await {
                        Ok(_) => {
                            let len = u32::from_le_bytes(header) as usize;
                            let mut buffer = vec![0u8; len];
                            if let Err(e) = reader.read_exact(&mut buffer).await {
                                yield Err(e);
                                break;
                            }

                            let res: io::Result<(String, CommandPos)> =
                            serde_json::from_slice(&buffer).map_err(|e| io::Error::other(e.to_string()));

                            yield res;
                        }
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(e) =>
                        {
                            yield Err(e);
                            break;
                        }
                    }
                }
            };
            pin_mut!(s);
            while let Some(result) = s.next().await {
                match result {
                    Ok((key, pos)) => {
                        new_map.insert(key, pos);
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }

        let mut pos: u64 = 0;
        let mut reader = DataReader::new(file.try_clone().await?, 0);

        let mut reader_in_stream = reader.clone();
        let s = stream! {
            loop {
                match reader_in_stream.read_data(reader_in_stream.cursor).await {
                    Ok((expect_crc, buf)) => {
                        let actual_crc = crc32fast::hash(&buf);
                        if expect_crc != actual_crc {
                            eprintln!("crc mismatch");
                            break;
                        }
                        let res = serde_json::from_slice(&buf)
                            .map(|cmd| (cmd, buf.len() as u64))
                            .map_err(|e| io::Error::other(e.to_string()));
                        reader_in_stream.cursor+= 12 + buf.len() as u64;
                        yield res;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        };
        pin_mut!(s);
        while let Some(result) = s.next().await {
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
                    reader.cursor += entry_len;
                }
                Err(e) => {
                    eprintln!("read data error：{}", e);
                    break;
                }
            }
        }

        let mut f_for_writer = file.try_clone().await?;
        f_for_writer.seek(io::SeekFrom::End(0)).await?;
        let writer = tokio::io::BufWriter::new(f_for_writer);
        let res = {
            Self {
                path,
                map: Arc::new(std::sync::RwLock::new(map)),
                writer: Arc::new(Mutex::new(writer)),
                reader,
                uncompacted,
            }
        };

        Ok(res)
    }
}

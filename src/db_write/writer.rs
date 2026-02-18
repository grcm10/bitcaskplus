use crate::{BitCaskPlus, COMPACTION_THRESHOLD, Command, CommandPos, Result};
use tokio::io::{self, AsyncSeekExt, AsyncWriteExt};

impl BitCaskPlus {
    pub async fn write_data(&mut self, cmd: &Command) -> io::Result<CommandPos> {
        let mut w = self.writer.lock().await;

        let pos = w.get_mut().stream_position().await?;
        let json_data = serde_json::to_string(&cmd).map_err(|e| io::Error::other(e.to_string()))?;
        let json_data_len = json_data.len() as u64;
        let checksum = crc32fast::hash(json_data.as_bytes());
        // CRC(4) + Len(8) + Data(N)
        w.write_all(&checksum.to_le_bytes()).await?;
        w.write_all(&json_data_len.to_le_bytes()).await?; // Little-Endian
        w.write_all(json_data.as_bytes()).await?;
        w.flush().await?;
        Ok(CommandPos {
            pos,
            len: 12 + json_data_len,
        })
    }

    pub async fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: val,
        };

        let cmd_pos = self.write_data(&cmd).await?;
        {
            let mut m = self.map.write().unwrap();
            if let Some(old_pos) = m.insert(key, cmd_pos) {
                self.uncompacted += old_pos.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction().await?;
        }

        Ok(())
    }

    pub async fn remove(&mut self, key: &str) -> Result<()> {
        let old_pos = {
            let mut m = self.map.write().unwrap();
            m.remove(key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Key not found"))?
        };

        let cmd = Command::Remove {
            key: key.to_string(),
        };

        let cmd_pos = self.write_data(&cmd).await?;
        self.uncompacted += old_pos.len + cmd_pos.len;

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction().await?;
        }

        Ok(())
    }
}

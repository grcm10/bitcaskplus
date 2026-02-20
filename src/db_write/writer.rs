use crate::{BitCaskPlus, COMPACTION_THRESHOLD, Command, CommandPos, Result};
use std::io::{self, Seek, Write};

impl BitCaskPlus {
    pub fn write_data(&mut self, cmd: &Command) -> io::Result<CommandPos> {
        let mut w = self.writer.lock().unwrap();
        let pos = w.stream_position()?;
        let json_data = serde_json::to_string(&cmd).map_err(|e| io::Error::other(e.to_string()))?;
        let json_data_len = json_data.len() as u64;
        let checksum = crc32fast::hash(json_data.as_bytes());
        // CRC(4) + Len(8) + Data(N)
        w.write_all(&checksum.to_le_bytes())?;
        w.write_all(&json_data_len.to_le_bytes())?; // Little-Endian
        w.write_all(json_data.as_bytes())?;
        w.flush()?;
        Ok(CommandPos {
            file_num: self.cur_gen,
            pos,
            len: 12 + json_data_len,
        })
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: val,
        };

        let cmd_pos = self.write_data(&cmd)?;
        {
            let mut m = self.map.write().unwrap();
            if let Some(old_pos) = m.insert(key, cmd_pos) {
                self.uncompacted += old_pos.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        let cmd = Command::Remove {
            key: key.to_string(),
        };

        let cmd_pos = self.write_data(&cmd)?;
        let old_pos = {
            let mut m = self.map.write().unwrap();
            m.remove(key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Key not found"))?
        };
        self.uncompacted += old_pos.len + cmd_pos.len;

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }
}

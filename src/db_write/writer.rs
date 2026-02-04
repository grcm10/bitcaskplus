use crate::{BitCaskPlus, COMPACTION_THRESHOLD, Command, CommandPos, Result};
use std::io::{self, Seek, SeekFrom, Write};

impl BitCaskPlus {
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: val,
        };

        self.writer.flush()?;
        let pos = self.writer.seek(SeekFrom::End(0))?;
        let json_data =
            serde_json::to_string_pretty(&cmd).map_err(|e| io::Error::other(e.to_string()))?;
        let json_data_len = json_data.len() as u64;
        self.writer.write_all(&json_data_len.to_le_bytes())?; // Little-Endian
        self.writer.write_all(json_data.as_bytes())?;
        self.writer.flush()?;

        let record_len = 8 + json_data_len;
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

    pub fn remove(&mut self, key: &str) -> Result<()> {
        if !self.map.contains_key(key) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "KeyNotFound").into());
        }

        let key_str = key.to_string();
        let cmd = Command::Remove {
            key: key_str.clone(),
        };

        self.writer.flush()?;
        let json_data =
            serde_json::to_string_pretty(&cmd).map_err(|e| io::Error::other(e.to_string()))?;
        let json_data_len = json_data.len() as u64;
        self.writer.write_all(&json_data_len.to_le_bytes())?;
        self.writer.write_all(json_data.as_bytes())?;
        self.writer.flush()?;

        if let Some(old_pos) = self.map.remove(&key_str) {
            self.uncompacted += old_pos.len + (8 + json_data_len);
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }
}

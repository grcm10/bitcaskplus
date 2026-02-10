use crate::{BitCaskPlus, COMPACTION_THRESHOLD, Command, CommandPos, Result};
use std::io::{self, Seek, SeekFrom, Write};

impl BitCaskPlus {
    fn write_data(&mut self, cmd: &Command) -> io::Result<CommandPos> {
        match self.writer.flush() {
            Ok(_) => {
                let pos = self.writer.seek(SeekFrom::End(0))?;
                let json_data =
                    serde_json::to_string(&cmd).map_err(|e| io::Error::other(e.to_string()))?;
                let json_data_len = json_data.len() as u64;
                let checksum = crc32fast::hash(json_data.as_bytes());
                // CRC(4) + Len(8) + Data(N)
                self.writer.write_all(&checksum.to_le_bytes())?;
                self.writer.write_all(&json_data_len.to_le_bytes())?; // Little-Endian
                self.writer.write_all(json_data.as_bytes())?;
                self.writer.flush()?;
                Ok(CommandPos {
                    pos,
                    len: 4 + 8 + json_data_len,
                })
            }
            Err(e) => Err(e),
        }
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: val,
        };

        let cmd_pos = self.write_data(&cmd)?;
        if let Some(old_pos) = self.map.insert(key, cmd_pos) {
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

        let cmd_pos = self.write_data(&cmd)?;
        if let Some(old_pos) = self.map.remove(&key_str) {
            self.uncompacted += old_pos.len + cmd_pos.len;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }
}

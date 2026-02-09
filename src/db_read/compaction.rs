use crate::{BitCaskPlus, CommandPos, Result};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};

impl BitCaskPlus {
    pub fn compaction(&mut self) -> Result<()> {
        self.writer.flush()?;
        let compact_path = self.path.join("bitcaskplus.db.compact");
        let log_path = self.path.join("bitcaskplus.db");
        let hint_file_path = self.path.join("bitcaskplus.hint");

        let mut new_writer = BufWriter::new(File::create(&compact_path)?);
        let mut hint_writer = BufWriter::new(File::create(&hint_file_path)?);
        let mut old_file = File::open(&log_path)?;
        let mut new_pos = 0;
        let mut new_map = HashMap::new();

        for (key, pos_info) in &self.map {
            // get checksum, len and data
            old_file.seek(SeekFrom::Start(pos_info.pos))?;

            let mut checksum = [0u8; 4];
            old_file.read_exact(&mut checksum)?;
            let expected_crc = u32::from_le_bytes(checksum);

            let mut header = [0u8; 8];
            let _ = old_file.read_exact(&mut header);
            let data_len = u64::from_le_bytes(header);
            let mut buffer = vec![0u8; data_len as usize];
            let _ = old_file.read_exact(&mut buffer);

            let actual_crc = crc32fast::hash(&buffer);
            if actual_crc != expected_crc {
                return Err(io::Error::other("crc mismatch").into());
            }

            new_writer.write_all(&expected_crc.to_le_bytes())?;
            new_writer.write_all(&header)?;
            new_writer.write_all(&buffer)?;

            new_map.insert(
                key.clone(),
                CommandPos {
                    pos: new_pos,
                    len: 4 + 8 + data_len,
                },
            );

            new_pos += 4 + 8 + data_len;
        }

        new_writer.flush()?;
        drop(new_writer);
        drop(old_file);
        fs::rename(&compact_path, &log_path)?;

        for (key, pos) in &new_map {
            let entry_data = serde_json::to_string_pretty(&(key, pos))
                .map_err(|e| io::Error::other(e.to_string()))?;
            let entry_data_len = entry_data.len() as u32;
            hint_writer.write_all(&entry_data_len.to_le_bytes())?;
            hint_writer.write_all(entry_data.as_bytes())?;
        }
        hint_writer.flush()?;
        drop(hint_writer);

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_path)?;
        self.writer = BufWriter::new(file);
        self.map = new_map;
        self.uncompacted = 0;

        Ok(())
    }
}

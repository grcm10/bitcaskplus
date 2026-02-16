use crate::{BitCaskPlus, CommandPos, DataReader, Result};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::unix::fs::FileExt;

fn migrate_entry(reader: &DataReader, pos: u64, new_f: &mut BufWriter<File>) -> Result<u64> {
    let (expected_crc, header, data_len, buffer) = {
        let f = reader.file.read().unwrap();
        let mut header_buf = [0u8; 12];
        f.read_exact_at(&mut header_buf, pos)?;

        let expected_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let data_len = u64::from_le_bytes(header_buf[4..12].try_into().unwrap());

        let mut header = [0u8; 8];
        let mut buffer = vec![0u8; data_len as usize];
        f.read_exact_at(&mut buffer, pos + 12)?;

        header.copy_from_slice(&header_buf[4..12]);
        (expected_crc, header, data_len, buffer)
    };

    let actual_crc = crc32fast::hash(&buffer);
    if actual_crc != expected_crc {
        return Err(io::Error::other("crc mismatch").into());
    }

    new_f.write_all(&expected_crc.to_le_bytes())?;
    new_f.write_all(&header)?;
    new_f.write_all(&buffer)?;
    Ok(data_len)
}

impl BitCaskPlus {
    pub fn compaction(&mut self) -> Result<()> {
        {
            self.writer.lock().unwrap().flush()?;
        }

        let entries: Vec<(String, CommandPos)> = {
            let m = self.map.read().unwrap();
            m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let compact_path = self.path.join("bitcaskplus.db.compact");
        let log_path = self.path.join("bitcaskplus.db");
        let hint_file_path = self.path.join("bitcaskplus.hint");

        let mut new_writer = BufWriter::new(File::create(&compact_path)?);
        let mut hint_writer = BufWriter::new(File::create(&hint_file_path)?);

        let mut new_pos = 0;
        let mut new_map = HashMap::new();

        for (key, pos_info) in &entries {
            // get checksum, len and data
            let len = migrate_entry(&self.reader, pos_info.pos, &mut new_writer)?;
            new_map.insert(
                key.clone(),
                CommandPos {
                    pos: new_pos,
                    len: 12 + len,
                },
            );

            new_pos += 12 + len;
        }
        new_writer.flush()?;

        {
            let mut w_lock = self.writer.lock().unwrap();
            let mut m_lock = self.map.write().unwrap();
            fs::rename(&compact_path, &log_path)?;
            for (key, pos) in &new_map {
                let entry_data = serde_json::to_string(&(key, pos))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let entry_data_len = entry_data.len() as u32;
                hint_writer.write_all(&entry_data_len.to_le_bytes())?;
                hint_writer.write_all(entry_data.as_bytes())?;
            }
            hint_writer.flush()?;
            drop(hint_writer);

            let new_file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&log_path)?;
            *w_lock = BufWriter::new(new_file.try_clone()?);
            self.reader = DataReader::new(new_file, 0);
            *m_lock = new_map;
            self.uncompacted = 0;
        }

        Ok(())
    }
}

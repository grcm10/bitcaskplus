use crate::{BitCaskPlus, CommandPos, DataReader, Result};
use std::collections::HashMap;
use std::io;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};

async fn migrate_entry(reader: &DataReader, pos: u64, new_f: &mut BufWriter<File>) -> Result<u64> {
    let (expected_crc, header, data_len, buffer) = {
        let f = reader.file.clone();
        let mut file_guard = f.write().await;
        let mut header_buf = [0u8; 12];
        file_guard.seek(SeekFrom::Start(pos)).await?;
        file_guard.read_exact(&mut header_buf).await?;

        let expected_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let data_len = u64::from_le_bytes(header_buf[4..12].try_into().unwrap());

        let mut header = [0u8; 8];
        let mut buffer = vec![0u8; data_len as usize];
        file_guard.read_exact(&mut buffer).await?;

        header.copy_from_slice(&header_buf[4..12]);
        (expected_crc, header, data_len, buffer)
    };

    let actual_crc = crc32fast::hash(&buffer);
    if actual_crc != expected_crc {
        return Err(io::Error::other("crc mismatch").into());
    }

    new_f.write_all(&expected_crc.to_le_bytes()).await?;
    new_f.write_all(&header).await?;
    new_f.write_all(&buffer).await?;
    Ok(data_len)
}

impl BitCaskPlus {
    pub async fn compaction(&mut self) -> Result<()> {
        {
            //self.writer.lock().unwrap().flush()?;
            self.writer.lock().await.flush().await?;
        }

        let entries: Vec<(String, CommandPos)> = {
            let m = self.map.read().unwrap();
            m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let compact_path = self.path.join("bitcaskplus.db.compact");
        let log_path = self.path.join("bitcaskplus.db");
        let hint_file_path = self.path.join("bitcaskplus.hint");

        let mut new_writer = BufWriter::new(File::create(&compact_path).await?);
        let mut hint_writer = BufWriter::new(File::create(&hint_file_path).await?);

        let mut new_pos = 0;
        let mut new_map = HashMap::new();

        for (key, pos_info) in &entries {
            // get checksum, len and data
            let len = migrate_entry(&self.reader, pos_info.pos, &mut new_writer).await?;
            let len = len + 12;
            new_map.insert(
                key.clone(),
                CommandPos {
                    pos: new_pos,
                    len: len,
                },
            );

            new_pos += len;
        }
        new_writer.flush().await?;

        {
            let mut w_lock = self.writer.lock().await;
            let mut m_lock = self.map.write().unwrap();
            fs::rename(&compact_path, &log_path).await?;
            for (key, pos) in &new_map {
                let entry_data = serde_json::to_string(&(key, pos))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let entry_data_len = entry_data.len() as u32;
                hint_writer.write_all(&entry_data_len.to_le_bytes()).await?;
                hint_writer.write_all(entry_data.as_bytes()).await?;
            }
            hint_writer.flush().await?;
            drop(hint_writer);

            let new_file = File::options()
                .append(true)
                .create(true)
                .open(&log_path)
                .await?;
            *w_lock = BufWriter::new(new_file.try_clone().await?);
            self.reader = DataReader::new(new_file, 0);
            *m_lock = new_map;
            self.uncompacted = 0;
        }

        Ok(())
    }
}

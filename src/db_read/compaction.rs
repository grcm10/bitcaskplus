use crate::{BitCaskPlus, CommandPos, DataReader, Result};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Seek, Write};
use std::os::unix::fs::FileExt;

fn migrate_entry(
    reader: &DataReader,
    pos: u64,
    len: u64,
    new_f: &mut BufWriter<File>,
) -> Result<()> {
    let mut buffer = vec![0u8; len as usize];
    reader.file.read_exact_at(&mut buffer, pos)?;
    new_f.write_all(&buffer)?;
    Ok(())
}

impl BitCaskPlus {
    pub fn compaction(&mut self) -> Result<()> {
        let compaction_gen = self.cur_gen + 1;
        self.cur_gen += 2;
        {
            let mut w_lock = self.writer.lock().unwrap();
            w_lock.flush()?;
            let new_file =
                crate::new_log_file(&self.path, self.cur_gen, &mut self.readers.write().unwrap())?;
            *w_lock = BufWriter::new(new_file);
        }
        let compact_file = crate::new_log_file(
            &self.path,
            compaction_gen,
            &mut self.readers.write().unwrap(),
        )?;

        //TODO: Got mismatch error when introducing hint file.
        /*let hint_log_path = self.path.join(format!("{}.db.hint", self.cur_gen));
        let mut hint_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&hint_log_path)?;
        hint_file.seek(io::SeekFrom::End(0))?;
        let mut hint_writer = BufWriter::new(hint_file);*/

        let mut compact_writer = BufWriter::new(compact_file);

        let mut new_pos = 0;
        let mut new_map = HashMap::new();

        let entries: Vec<(String, CommandPos)> = {
            let m = self.map.read().unwrap();
            m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        for (key, pos_info) in &entries {
            let readers = self.readers.read().unwrap();
            let reader = readers
                .get(&pos_info.file_num)
                .expect("Can not find db file");
            // get checksum, len and data
            migrate_entry(&reader, pos_info.pos, pos_info.len, &mut compact_writer)?;
            new_map.insert(
                key.clone(),
                CommandPos {
                    file_num: compaction_gen,
                    pos: new_pos,
                    len: pos_info.len,
                },
            );

            new_pos += pos_info.len;
        }
        compact_writer.flush()?;

        let stale_gens: Vec<u64> = {
            let readers = self.readers.read().unwrap();

            readers
                .keys()
                .filter(|&&g| g < compaction_gen)
                .cloned()
                .collect()
        };

        for stale_gen in stale_gens {
            self.readers.write().unwrap().remove(&stale_gen);
            let path = self.path.join(format!("{}.db", stale_gen));
            match fs::remove_file(&path) {
                Ok(_) => println!("Successfully deleted: {:?}", path),
                Err(e) => println!("Failed to delete {:?}: {}", path, e),
            }
        }

        {
            let mut m_lock = self.map.write().unwrap();
            for (key, new_pos_info) in &new_map {
                if let Some(current_pos) = m_lock.get(key) {
                    if current_pos.file_num < compaction_gen {
                        m_lock.insert(key.clone(), new_pos_info.clone());
                    }
                }
            }

            //TODO: Got mismatch error when introducing hint file.
            /*for (key, pos) in &new_map {
                let entry_data = serde_json::to_string(&(key, pos))
                    .map_err(|e| io::Error::other(e.to_string()))?;
                let entry_data_len = entry_data.len() as u32;
                hint_writer.write_all(&entry_data_len.to_le_bytes())?;
                hint_writer.write_all(entry_data.as_bytes())?;
            }
            hint_writer.flush()?;*/
        }
        self.uncompacted = 0;

        Ok(())
    }
}

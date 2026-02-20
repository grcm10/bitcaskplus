use crate::{BitCaskPlus, Command, CommandPos, DataReader, Result};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

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

pub fn sorted_file_list(path: &Path) -> io::Result<Vec<u64>> {
    let mut file_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|r| -> Result<_> { Ok(r?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("db".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".db"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    file_list.sort_unstable();
    Ok(file_list)
}

pub fn load(
    path: &PathBuf,
    file_num: u64,
    map: &mut HashMap<String, CommandPos>,
) -> io::Result<(DataReader, u64)> {
    let log_path = &path.join(format!("{}.db", file_num));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&log_path)?;
    let mut reader = DataReader::new(file.try_clone().expect("clone failed"), 0);
    let mut uncompacted = 0;
    //TODO: Got mismatch error when introducing hint file.
    /*let hint_path = &path.join(format!("{}.db.hint", file_num));
    if hint_path.exists() {
        /*let f_reader = io::BufReader::new(File::open(&hint_path)?);
        let hint_map: io::Result<HashMap<String, CommandPos>> = HintReader::new(f_reader).collect();
        map.extend(hint_map?);*/
    }*/
    for result in reader.by_ref() {
        match result {
            Ok((cmd, mut cmd_pos)) => {
                cmd_pos.file_num = file_num;
                match cmd {
                    Command::Set { key, .. } => {
                        if let Some(old_pos) = map.insert(key, cmd_pos) {
                            uncompacted += old_pos.len;
                        }
                    }
                    Command::Remove { key } => {
                        if let Some(old_pos) = map.remove(&key) {
                            uncompacted += old_pos.len;
                        }
                        uncompacted += cmd_pos.len;
                    }
                }
            }
            Err(e) => {
                eprintln!("read data error：{}", e);
                break;
            }
        }
    }
    Ok((reader, uncompacted))
}

impl BitCaskPlus {
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let pos_info = {
            let map = self.map.read().unwrap();
            map.get(key).cloned()
        };

        let p = match pos_info {
            Some(p) => p,
            None => return Ok(None),
        };
        let readers = self.readers.read().unwrap();
        let reader = readers.get(&p.file_num).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Log file {} not found", p.file_num),
            )
        })?;
        let (expect_crc, buffer) = { reader.read_data(p.pos, p.len)? };

        let actual_crc = crc32fast::hash(&buffer[12..]);
        if actual_crc != expect_crc {
            return Err(io::Error::other("crc mismatch").into());
        }
        let cmd = serde_json::from_slice(&buffer[12..])
            .map_err(|e| format!("Serde json deserialization error: {}", e))?;
        if let Command::Set { value, .. } = cmd {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        let file_list = sorted_file_list(&path)?;
        let mut readers = HashMap::new();
        let mut map: HashMap<String, CommandPos> = HashMap::new();
        let mut uncompacted = 0;

        for &f in &file_list {
            let (reader, un_com) = load(&path, f, &mut map)?;
            uncompacted += un_com;
            readers.insert(f, reader);
        }

        let cur_gen = file_list.last().unwrap_or(&0) + 1;
        let file = crate::new_log_file(&path, cur_gen, &mut readers)?;
        let writer = io::BufWriter::new(file);
        let res = {
            Self {
                path,
                map: Arc::new(RwLock::new(map)),
                writer: Arc::new(Mutex::new(writer)),
                readers: Arc::new(RwLock::new(readers)),
                uncompacted,
                cur_gen,
            }
        };

        Ok(res)
    }
}

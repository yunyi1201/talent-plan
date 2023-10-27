use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fmt::write;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

const COMPACTION_THREDHOLD: u64 = 1024 * 1024;

/// The 'KvStore' stores string key/value pairs.
///
/// key/value pairs are stored in a 'HashMap' in memory and not persisted to disk.
///
pub struct KvStore {
    /// directory for the log and other data.
    path: PathBuf,

    /// used generate next active file name.
    current_gen: u64,

    /// immutable files handle that may be contain stale data
    readers: HashMap<u64, BufReaderWithPos<File>>,

    /// active file handle that can be writen and read
    writer: BufWriterWithPos<File>,

    /// store in-memory index for quickly search log position in log file
    index: BTreeMap<String, CommandPos>,

    /// when entry that stale more than `canbe_compacted`, then trigger compaction
    canbe_compacted: u64,
}

impl KvStore {
    /// Open the KvStore at a given path. return the KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut index = BTreeMap::new();

        // immutable file only can be read
        let mut readers = HashMap::new();

        let canbe_compacted: u64 = 0;

        let gen_list = sorted_gen_list(&path)?;

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            build_index_from_log(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        // only one active file can be writen.
        let current_gen = gen_list.last().unwrap_or(&0) + 1;

        let writer = create_active_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore {
            path,
            current_gen,
            readers,
            writer,
            index,
            canbe_compacted,
        })
    }

    /// Sets the value of a string key to a sting
    /// Return an error if the value is not written successfully
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set {
            key,
            ..
        } = cmd
        {
            if let Some(old_cmd) =
                self.index.insert(key, (self.current_gen, pos..self.writer.pos).into())
            {
                self.canbe_compacted += old_cmd.len;
            }
        }

        if self.canbe_compacted > COMPACTION_THREDHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Get the value of a given sting key.
    /// if the key does not exist, return `None`.
    /// Return an error if the value is not read successfully
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&cmd_pos.gen).expect("Cannot find log msg");

            reader.seek(SeekFrom::Start(cmd_pos.pos))?;

            let cmd_reader = reader.take(cmd_pos.len);

            if let Command::Set {
                value,
                ..
            } = serde_json::from_reader(cmd_reader)?
            {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove {
                key,
            } = cmd
            {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.canbe_compacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }


    fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = create_active_log_file(&self.path, self.current_gen, &mut self.readers)?;
        
        let mut compaction_writer = create_active_log_file(&self.path, compaction_gen, &mut self.readers)?;

        let mut new_pos = 0;

        // since entry in memory index is latest data, so if meta information of entry in read file is equal to index 
        // then the entry is latest, we insert the entry into compaction file
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self.readers.get_mut(&cmd_pos.gen).expect("Cannot find log reader");

            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_gen, new_pos..new_pos+len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        // remove stale log file
        let stale_gens: Vec<_> = self.readers.keys().filter(|&&gen| gen < compaction_gen).cloned().collect();

        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }

        self.canbe_compacted = 0;
        Ok(())

    }

}

fn create_active_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new().create(true).write(true).append(true).open(&path)?,
    )?;

    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}
/// Return sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    gen_list.sort_unstable();
    Ok(gen_list)
}

// now hand-code log file
fn log_path(path: &Path, gen: u64) -> PathBuf {
    path.join(format!("{}.log", gen))
}

/// build index from log file
fn build_index_from_log(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {

    let mut canbe_compacted: u64 = 0;
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let end_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set {
                key,
                ..
            } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..end_pos).into()) {
                    canbe_compacted += old_cmd.len;            
                }
            }
            Command::Remove {
                key,
            } => {
                if let Some(old_cmd) = index.remove(&key) {
                    canbe_compacted += old_cmd.len;
                }
                // the `remove` command itself can be deleted in the next compaction.
                canbe_compacted += end_pos - pos;
            }
        }
        pos = end_pos;
    }
    Ok(canbe_compacted)
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set {
        key: String,
        value: String,
    },
    Remove {
        key: String,
    },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set {
            key,
            value,
        }
    }

    fn remove(key: String) -> Command {
        Command::Remove {
            key,
        }
    }
}

struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

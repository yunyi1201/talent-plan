use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

/// The 'KvStore' stores string key/value pairs.
///
/// key/value pairs are stored in a 'HashMap' in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```

pub struct KvStore {
    /// directory for the log and other data.
    path: PathBuf,

    /// file reader
    reader: BufReaderWithPos<File>,

    /// writer of the current log.
    writer: BufWriterWithPos<File>,

    /// store in-memory index for quickly search log position in log file
    index: BTreeMap<String, CommandPos>,
}

impl KvStore {
    /// Open the KvStore at a given path. return the KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut index = BTreeMap::new();
        
        let writer = BufWriterWithPos::new(
            OpenOptions::new().create(true).write(true).append(true).open(log_path(&path))?,
        )?;

        let mut reader = BufReaderWithPos::new(
            OpenOptions::new().read(true).open(log_path(&path))?,
        )?;

        build_index_from_log(&mut reader, &mut index)?;

        Ok(KvStore {
            path,
            reader,
            writer,
            index,
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
            self.index.insert(key, (pos..self.writer.pos).into());
        }
        Ok(())
    }

    /// Get the value of a given sting key.
    /// if the key does not exist, return `None`.
    /// Return an error if the value is not read successfully
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            self.reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = (&mut self.reader).take(cmd_pos.len);

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
                self.index.remove(&key).expect("key not found");
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    
}

// now hand-code log file
fn log_path(path: &Path) -> PathBuf {
    path.join(format!("log.log"))
}

/// build index from log file
fn build_index_from_log(
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<()> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let end_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set {
                key,
                ..
            } => {
                index.insert(key, (pos..end_pos).into());
            }
            Command::Remove {
                key,
            } => {
                index.remove(&key);
            }
        }
        pos = end_pos;
    }
    Ok(())
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
    pos: u64,
    len: u64,
}

impl From<Range<u64>> for CommandPos {
    fn from(range: Range<u64>) -> Self {
        CommandPos {
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

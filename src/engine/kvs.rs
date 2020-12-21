use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::engine::KvsEngine;
use crate::error::{Error, Result};

// A single entry in the log
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Command {
    Get(String),
    Set(String, String),
    Remove(String),
}

#[derive(Clone, Debug)]
struct CommandIndex {
    pos: usize,
    size: usize,
}

pub struct KvStore {
    // In-memory store index
    // Each entry contains a position and length
    store: BTreeMap<String, CommandIndex>,

    // Log file writer
    log_writer: BufWriter<File>,

    // Log file reader
    log_reader: BufReader<File>,

    // Current position in log
    // Used for the index
    log_pos: usize,

    // Directory containing the log
    // Used during log compaction
    log_dir: PathBuf,

    // Number of uncompacted bytes
    num_uncompacted: usize,
}

impl KvStore {
    const LOG_NAME: &'static str = "kvs.log";
    const MAX_UNCOMPACTED: usize = 1024 * 1024; // 1 MB

    /// Returns `true` if a log already exists
    pub fn is_log_present(path: impl Into<PathBuf>) -> bool {
        let log_dir = path.into();
        let log_file = log_dir.join(Self::LOG_NAME);
        log_file.exists()
    }

    /// Open an existing log file or create a new one.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let log_dir = path.into();
        let log_file = log_dir.join(Self::LOG_NAME);

        // Create/open the log in append mode
        let write_log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_file)?;

        let read_log = OpenOptions::new().read(true).open(&log_file)?;

        let mut kvs = Self {
            store: BTreeMap::new(),
            log_writer: BufWriter::new(write_log),
            log_reader: BufReader::new(read_log),
            log_pos: 0,
            log_dir,
            num_uncompacted: 0,
        };

        // Load existing log entries into memory
        kvs.load_log()?;

        Ok(kvs)
    }

    // Trigger a compaction of the log, if needed.
    //
    // Returnes `true` if a compaction was performed.
    //
    // To compact the log, we simply iterate over all of the keys we are tracking
    // and copy the latest entry in the current log to the new log.
    fn compact(&mut self) -> Result<()> {
        // Build path to new log
        let new_log_path = self.log_dir.join(format!("{}.new", Self::LOG_NAME));

        // Open the new log in append mode
        let new_log = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .write(true)
            .open(&new_log_path)?;

        let mut new_log_writer = BufWriter::new(new_log);

        // Sort the index by the position of the key in the log
        let mut log_data: Vec<(&String, &mut CommandIndex)> = self
            .store
            .iter_mut()
            .collect();
        log_data.sort_by(|a, b| a.1.pos.cmp(&b.1.pos));

        let mut new_log_pos = 0;

        for (_, index) in log_data {
            // Seek to the position in the old log
            self.log_reader.seek(SeekFrom::Start(index.pos as u64))?;

            // Read the size of this command to determine how many bytes we need
            // to copy from the old log to the new log
            let size = Self::read_command_size(&mut self.log_reader)?;

            // Create a wrapped `BufReader` that will only return the next `size` bytes
            let mut wrapped_reader = self.log_reader.by_ref().take(size);

            // First, write the size of the command to the new log
            new_log_writer.write(&size.to_le_bytes())?;

            // Second, copy the actual command bytes from the old log to the new log
            std::io::copy(&mut wrapped_reader, &mut new_log_writer)?;

            // Update the in-memory index with the position of this key in the _new_ log
            // Note: `new_log_pos` tracks our position in the new log
            index.pos = new_log_pos;

            new_log_pos += index.size;
        }

        // Ensure all data is flushed to the new log (i.e., fsync)
        new_log_writer.flush()?;

        // Update the log position
        self.log_pos = new_log_pos;

        // Use the new log file descriptor
        self.log_writer = new_log_writer;
        self.log_reader = BufReader::new(File::open(&new_log_path)?);

        // Move new log file to overwrite old log
        std::fs::rename(new_log_path, self.log_dir.join(Self::LOG_NAME))?;

        self.num_uncompacted = 0;

        Ok(())
    }

    // Load all entries from the log into memory
    fn load_log(&mut self) -> Result<()> {
        // Find the size of the log
        let size = self.log_reader.get_ref().metadata()?.len() as usize;

        // If the log was just created, there is nothing to load
        if size == 0 {
            return Ok(());
        }

        // Seek to the beginning of the log
        self.log_reader.seek(SeekFrom::Start(0))?;

        let mut pos: usize = 0;

        // Read each log command into the in-memory store
        while pos < size {
            let size = Self::read_command_size(&mut self.log_reader)?;

            let command: Command = rmp_serde::from_read(&mut self.log_reader)?;

            let index = CommandIndex {
                pos,
                size: std::mem::size_of_val(&size) + size as usize,
            };

            pos += index.size;

            self.process_command(command, index);
        }

        // We are now at the end of the log - pos = len(log)
        self.log_pos = size;

        Ok(())
    }

    // Process a single command into the in-memory hashmap
    #[inline]
    fn process_command(&mut self, command: Command, index: CommandIndex) {
        match command {
            Command::Set(key, _) => {
                self.store.insert(key, index);
            }
            Command::Remove(key) => {
                self.store.remove(&key);
            }
            _ => (),
        }
    }

    #[inline]
    fn read_command_size(reader: &mut impl Read) -> Result<u64> {
        let mut size = [0u8; 8];

        // NOTE(aksiksi): The default buffer size for `BufRead` is 8KB. If you
        // only use the `read` API, you could get partial reads into the buffer
        // once you reach the buffer size.
        reader.read_exact(&mut size)?;

        Ok(u64::from_le_bytes(size))
    }

    #[inline]
    fn write_command_size(writer: &mut impl Write, size: u64) -> Result<()> {
        let size: [u8; 8] = size.to_le_bytes();
        writer.write(&size)?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // Serialize this command
        let command = Command::Set(key.clone(), value);
        let buf = rmp_serde::to_vec(&command)?;

        // Write the size of this command as a 64 bit number in LE form
        let size = buf.len() as u64;
        Self::write_command_size(&mut self.log_writer, size)?;

        // Insert this command into the log
        self.log_writer.write(&buf)?;
        self.log_writer.flush()?;

        let index = CommandIndex {
            pos: self.log_pos,
            size: std::mem::size_of_val(&size) + size as usize,
        };

        // Store the key in the in-memory index
        if let Some(pos) = self.store.get_mut(&key) {
            // Mark the old bytes as being compactable
            self.num_uncompacted += pos.size;
            pos.pos = self.log_pos;
        } else {
            self.store.insert(key, index.clone());
        }

        // We wrote the size (u64) and the command
        self.log_pos += index.size;

        // Once we hit a certain number of uncompacted bytes, compact the log
        if self.num_uncompacted > Self::MAX_UNCOMPACTED {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        // Figure out the position of the value in the log
        let index = if let Some(p) = self.store.get(&key) {
            p
        } else {
            return Ok(None);
        };

        // Seek to the required position
        self.log_reader.seek(SeekFrom::Start(index.pos as u64))?;

        // Read the command size
        let _ = Self::read_command_size(&mut self.log_reader)?;

        // Now read the command and extract the value
        let value = match rmp_serde::from_read(&mut self.log_reader)? {
            Command::Set(k, v) => {
                assert!(k == key, "Invalid key found at pos {}", index.pos);
                v
            }
            _ => panic!("Expected a SET operation at position {}", index.pos),
        };

        Ok(Some(value))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if let Some(old) = self.store.remove(&key) {
            // Construct the remove command
            let command = Command::Remove(key);
            let buf = rmp_serde::to_vec(&command)?;
            let size = buf.len() as u64;

            // Write the size of this command
            Self::write_command_size(&mut self.log_writer, size)?;

            // Write this remove command to the log
            self.log_writer.write(&buf)?;
            self.log_writer.flush()?;

            // Compute the total size of this remove command
            let size = std::mem::size_of_val(&size) + size as usize;

            // Both the old command and this removal can be cleaned up during
            // compaction
            self.num_uncompacted += old.size + size;

            // Update the log position
            self.log_pos += size;

            // Once we hit a certain number of uncompacted bytes, compact the log
            if self.num_uncompacted > Self::MAX_UNCOMPACTED {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(Error::KeyNotFound)
        }
    }
}

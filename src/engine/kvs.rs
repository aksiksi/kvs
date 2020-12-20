use std::collections::HashMap;
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

pub struct KvStore {
    // In-memory store index
    store: HashMap<String, usize>,

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

    // Whether or not the on-disk store is compactable
    is_compactable: bool,

    // Last compaction size
    last_compact_size: usize,
}

impl KvStore {
    const LOG_NAME: &'static str = "kvs.log";
    const MAX_LOG_SIZE: usize = 1 * 1024 * 1024; // 1 MB

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
            store: HashMap::new(),
            log_writer: BufWriter::new(write_log),
            log_reader: BufReader::new(read_log),
            log_pos: 0,
            log_dir,
            is_compactable: false,
            last_compact_size: 0,
        };

        // Load existing log entries into memory
        kvs.load_log()?;

        Ok(kvs)
    }

    // Trigger a compaction of the log, if needed.
    //
    // Returnes `true` if a compaction was performed.
    //
    // The idea is simple: we already have a snapshot of the latest state
    // of the log in-memory. So, let's just write all of the keys we are tracking
    // as a sequence of SET entries to a new log. At the end, we point to the
    // new log and move it to overwrite the old one.
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
        let new_log_reader = BufReader::new(File::open(&new_log_path)?);

        // Construct a Vec of all keys and indices, sorted by index in ascending order.
        // We clone the the store here so that we can modify it in-place while writing data
        // to the new log.
        let mut log_data: Vec<(String, usize)> = self
            .store
            .clone()
            .into_iter()
            .map(|(key, index)| (key, index))
            .collect();
        log_data.sort_by(|a, b| a.1.cmp(&b.1));

        let mut bytes_written = 0;

        // Write out all entries to the new log
        for (key, index) in log_data.into_iter() {
            self.log_reader.seek(SeekFrom::Start(index as u64))?;

            // Read the size of this command to determine how many bytes we need
            // to copy from the old log to the new log
            let size = Self::read_command_size(&mut self.log_reader)?;

            // Create a wrapped `BufReader` that will only return the next `size` bytes
            let mut wrapped_reader = self.log_reader.by_ref().take(size);

            // Write the size of the command to the new log
            new_log_writer.write(&size.to_le_bytes())?;

            // Copy the command (as bytes) from the old log to the new log
            std::io::copy(&mut wrapped_reader, &mut new_log_writer)?;

            // Update the in-memory index with the position of this key in the _new_ log
            // Note: `bytes_written` tracks our position in the new log
            let p = self.store.get_mut(&key).expect("Key is missing from store");
            *p = bytes_written as usize;

            // We wrote the size (u64) and the command to the new log
            bytes_written += std::mem::size_of_val(&size) + size as usize;
        }

        // Ensure all data is flushed to the new log (fsync)
        new_log_writer.flush()?;

        // Update the log position
        self.log_pos = bytes_written;

        // Use new log file descriptor
        self.log_writer = new_log_writer;
        self.log_reader = new_log_reader;

        // Move new log file to overwrite old log
        std::fs::rename(new_log_path, self.log_dir.join(Self::LOG_NAME))?;

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

        // Read each log command into memory
        while pos < size {
            let size = Self::read_command_size(&mut self.log_reader)?;
            let command: Command = rmp_serde::from_read(&mut self.log_reader)?;

            self.process_command(command, pos);

            pos += std::mem::size_of_val(&size) + size as usize;
        }

        // We are now at the end of the log - pos = len(log)
        self.log_pos = size;

        Ok(())
    }

    // Process a single command into the in-memory hashmap
    #[inline]
    fn process_command(&mut self, command: Command, pos: usize) {
        match command {
            Command::Set(key, _) => {
                self.store.insert(key, pos);
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
        reader.read(&mut size)?;
        Ok(u64::from_le_bytes(size))
    }

    #[inline]
    fn write_command_size(writer: &mut impl Write, size: u64) -> Result<()> {
        let size = size.to_le_bytes();
        writer.write(&size)?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // Serialize this command
        let command = Command::Set(key.clone(), value);
        let buf = rmp_serde::to_vec(&command)?;

        // If the log is "dirty" and has hit a certain size, compact it
        if self.is_compactable && (self.log_pos - self.last_compact_size) >= Self::MAX_LOG_SIZE {
            self.compact()?;
        }

        // Write the size of this command as a 64 bit number in LE form
        let size = buf.len() as u64;
        Self::write_command_size(&mut self.log_writer, size)?;

        // Insert this command into the log
        self.log_writer.write(&buf)?;
        self.log_writer.flush()?;

        // Store the key in the in-memory index
        if let Some(pos) = self.store.get_mut(&key) {
            // If this key already exists, mark the log as compactable
            *pos = self.log_pos;
            self.is_compactable = true;
        } else {
            self.store.insert(key, self.log_pos);
        }

        // We wrote the size (u64) and the command
        self.log_pos += std::mem::size_of_val(&size) + size as usize;

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        // Figure out the position of the value in the log
        let pos = if let Some(p) = self.store.get(&key) {
            *p
        } else {
            return Ok(None);
        };

        // Seek to the required position
        self.log_reader.seek(SeekFrom::Start(pos as u64))?;

        // Read the command size
        let _ = Self::read_command_size(&mut self.log_reader)?;

        // Now read the command and extract the value
        let value = match rmp_serde::from_read(&mut self.log_reader)? {
            Command::Set(k, v) => {
                assert!(k == key, "Invalid key found at pos {}", pos);
                v
            }
            _ => panic!("Expected a SET operation at position {}", pos),
        };

        Ok(Some(value))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.store.remove(&key) {
            None => Err(Error::KeyNotFound),
            Some(_) => {
                // Append an command to the log
                let command = Command::Remove(key);
                let buf = rmp_serde::to_vec(&command)?;
                let size = buf.len() as u64;
                Self::write_command_size(&mut self.log_writer, size)?;
                self.log_writer.write(&buf)?;
                self.log_writer.flush()?;
                self.is_compactable = true;
                self.log_pos += std::mem::size_of_val(&size) + size as usize;
                Ok(())
            }
        }
    }
}

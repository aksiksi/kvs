use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

mod error;

pub use error::{Error, Result};

const LOG_NAME: &str = "kvs.log";

// A single entry in the log
#[derive(Debug, Deserialize, Serialize)]
enum Entry {
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
}

impl KvStore {
    const MAX_LOG_SIZE: usize = 1024 * 1024; // 1 MB

    /// Open an existing log file or create a new one.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let log_dir = path.into();
        let log_file = log_dir.join(LOG_NAME);

        // Create/open the log in append mode
        let write_log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_file)?;

        let read_log = OpenOptions::new()
            .read(true)
            .open(&log_file)?;

        let mut kvs = Self {
            store: HashMap::new(),
            log_writer: BufWriter::new(write_log),
            log_reader: BufReader::new(read_log),
            log_pos: 0,
            log_dir,
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
        let new_log_path = self.log_dir.join(format!("{}.new", LOG_NAME));

        // Open the new log in append mode
        let new_log = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .write(true)
            .open(&new_log_path)?;

        let mut new_log_writer= BufWriter::new(new_log);
        let new_log_reader = BufReader::new(File::open(&new_log_path)?);

        // Construct a Vec of all keys and indices, sorted by index in ascending order.
        let mut log_data: Vec<(&String, &usize)> = self.store.iter().collect();
        log_data.sort_by(|a, b| a.1.cmp(b.1));

        let mut bytes_written = 0;

        // Write out all entries to the new log
        for (_, index) in log_data.into_iter() {
            self.log_reader.seek(SeekFrom::Start(*index as u64))?;

            // TODO(aksiksi): Do we really need to deserialize the entry?
            let entry: Entry = rmp_serde::from_read(&mut self.log_reader)?;
            let buf = rmp_serde::to_vec(&entry)?;

            new_log_writer.write(&buf)?;

            bytes_written += buf.len();
        }

        // Ensure all data is flushed to the new log (fsync)
        new_log_writer.flush()?;

        // Update the log position
        self.log_pos = bytes_written;

        // Use new log file descriptor
        self.log_writer = new_log_writer;
        self.log_reader = new_log_reader;

        // Move new log file to overwrite old log
        std::fs::rename(new_log_path, self.log_dir.join(LOG_NAME))?;

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

        // Read each log entry into memory
        while pos < size {
            let entry: Entry = rmp_serde::from_read(&mut self.log_reader)?;

            self.process_entry(entry, pos);

            // Figure out the current position in the log
            // We need to do it this way because rpm_serde does not return
            // the size of the encoded entry
            // TODO(aksiksi): Is there a cleaner way to do this?
            pos = self.log_reader.seek(SeekFrom::Current(0))? as usize;
        }

        // We are now at the end of the log - pos = len(log)
        self.log_pos = size;

        Ok(())
    }

    // Process a single entry into the in-memory hashmap
    #[inline]
    fn process_entry(&mut self, entry: Entry, pos: usize) {
        match entry {
            Entry::Set(key, _) => {
                self.store.insert(key, pos);
            }
            Entry::Remove(key) => {
                self.store.remove(&key);
            }
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Serialize this entry
        let entry = Entry::Set(key.clone(), value);
        let buf = rmp_serde::to_vec(&entry)?;

        // If the log has hit a certain size, try to compact it
        if self.log_pos >= Self::MAX_LOG_SIZE {
            self.compact()?;
        }

        // Insert this entry into the log
        self.log_writer.write(&buf)?;
        self.log_writer.flush()?;

        // Store the key in the in-memory index
        self.store.insert(key, self.log_pos);

        self.log_pos += buf.len();

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // Figure out the position of the value in the log
        let pos = if let Some(p) = self.store.get(&key) {
            *p
        } else {
            return Ok(None);
        };

        // Seek to the required position
        self.log_reader.seek(SeekFrom::Start(pos as u64))?;

        // Read the entry and extract the value
        let value = match rmp_serde::from_read(&mut self.log_reader)? {
            Entry::Set(k, v) => {
                assert!(k == key, "Invalid key found at pos {}", pos);
                v
            }
            _ => panic!("Expected a SET operation at position {}", pos),
        };

        // Seek back to the end of log
        self.log_reader.seek(SeekFrom::End(1))?;

        Ok(Some(value))
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.store.remove(&key) {
            None => Err(Error::KeyNotFound),
            Some(_) => {
                // Append an entry to the log
                let entry = Entry::Remove(key);
                let buf = rmp_serde::to_vec(&entry)?;
                self.log_writer.write(&buf)?;
                self.log_writer.flush()?;
                self.log_pos += buf.len();
                Ok(())
            }
        }
    }
}

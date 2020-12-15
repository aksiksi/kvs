use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::{collections::HashMap, path::PathBuf};

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

    // Log file handle
    log: File,

    // Current position in log
    // Used for the index
    log_pos: usize,

    // Directory containing the log
    // Used during log compaction
    log_dir: PathBuf,
}

impl KvStore {
    const MAX_LOG_SIZE: u64 = 1024 * 1024; // 1 MB

    // Trigger a compaction of the log, if needed.
    //
    // Returnes `true` if a compaction was performed.
    //
    // The idea is simple: we already have a snapshot of the latest state
    // of the log in-memory. So, let's just write all of the keys we are tracking
    // as a sequence of SET entries to a new log. At the end, we point to the
    // new log and move it to overwrite the old one.
    fn check_compaction(&mut self) -> Result<bool> {
        let size = self.log.metadata()?.len();

        if size < Self::MAX_LOG_SIZE {
            Ok(false)
        } else {
            // Build path to new log
            let new_log_path = self.log_dir.join(format!("{}.new", LOG_NAME));

            // Open the new log in append mode
            let mut new_log = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .write(true)
                .open(&new_log_path)?;

            // Construct a Vec of all keys and indices, sorted by index in ascending order.
            let mut log_data: Vec<(&String, &usize)> = self.store.iter().collect();
            log_data.sort_by(|a, b| a.1.cmp(b.1));

            // Write out all entries to the new log
            for (_, index) in log_data.into_iter() {
                self.log.seek(SeekFrom::Start(*index as u64))?;

                // TODO(aksiksi): Do we really need to deserialize the entry?
                let entry: Entry = rmp_serde::from_read(&self.log)?;
                let buf = rmp_serde::to_vec(&entry)?;

                new_log.write(&buf)?;
            }

            // Ensure all data is flushed to the new log (fsync)
            new_log.sync_all()?;

            // Update the log position
            let new_size = new_log.metadata()?.len();
            self.log_pos = new_size as usize;

            // Use new log file descriptor
            self.log = new_log;

            // Move new log file to overwrite old log
            std::fs::rename(new_log_path, self.log_dir.join(LOG_NAME))?;

            Ok(true)
        }
    }

    // Load all entries from the log into memory
    fn load_log(&mut self) -> Result<()> {
        // Find the size of the log
        let size = self.log.metadata()?.len() as usize;

        // If the log was just created, there is nothing to load
        if size == 0 {
            return Ok(());
        }

        // Seek to the beginning of the log
        self.log.seek(SeekFrom::Start(0))?;

        let mut pos: usize = 0;

        // Read each log entry into memory
        while pos < size {
            let entry: Entry = rmp_serde::from_read(&self.log)?;

            self.process_entry(entry, pos);

            // Figure out the current position in the log
            // We need to do it this way because rpm_serde does not return
            // the size of the encoded entry
            // TODO(aksiksi): Is there a cleaner way to do this?
            pos = self.log.seek(SeekFrom::Current(0))? as usize;
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

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let log_dir = path.into();
        let log_file = log_dir.join(LOG_NAME);

        // Create/open the log in append mode
        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(log_file)?;

        let mut kvs = Self {
            store: HashMap::new(),
            log,
            log_pos: 0,
            log_dir,
        };

        // Load existing log entries into memory
        kvs.load_log()?;

        Ok(kvs)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Serialize this entry
        let entry = Entry::Set(key.clone(), value);
        let buf = rmp_serde::to_vec(&entry)?;

        // If the log has hit a certain size, compact it
        self.check_compaction()?;

        // Insert this entry into the log
        self.log.write(&buf)?;

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
        self.log.seek(SeekFrom::Start(pos as u64))?;

        // Read the entry and extract the value
        let value = match rmp_serde::from_read(&self.log)? {
            Entry::Set(k, v) => {
                assert!(k == key, "Invalid key found at pos {}", pos);
                v
            }
            _ => panic!("Expected a SET operation at position {}", pos),
        };

        // Seek back to the end of log
        self.log.seek(SeekFrom::End(1))?;

        Ok(Some(value))
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.store.remove(&key) {
            None => Err(Error::KeyNotFound),
            Some(_) => {
                // Append an entry to the log
                let entry = Entry::Remove(key);
                let buf = rmp_serde::to_vec(&entry)?;
                self.log.write(&buf)?;
                self.log_pos += buf.len();
                Ok(())
            }
        }
    }
}

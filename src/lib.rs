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
    store: HashMap<String, usize>,
    log: File,
    log_pos: usize,
}

impl KvStore {
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
        // Create/open the log in append mode
        let log_file = path.into().join(LOG_NAME);
        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(log_file)?;

        let mut kvs = Self {
            store: HashMap::new(),
            log,
            log_pos: 0,
        };

        // Load existing log entries into memory
        kvs.load_log()?;

        Ok(kvs)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Serialize this entry
        let entry = Entry::Set(key.clone(), value);
        let buf = rmp_serde::to_vec(&entry)?;

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

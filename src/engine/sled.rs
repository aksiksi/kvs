use std::path::PathBuf;

use crate::error::{Error, Result};
use super::KvsEngine;

/// Wrapper for Sled storage engine
pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    const LOG_NAME: &'static str = "sled";

    /// Returns `true` if log already exists
    pub fn is_log_present(path: impl Into<PathBuf>) -> bool {
        let log_dir = path.into();
        let log_file = log_dir.join(Self::LOG_NAME);
        log_file.exists()
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let log_dir = path.into();
        let log_file = log_dir.join(Self::LOG_NAME);
        let db = sled::open(log_file)?;

        log::info!("Opened DB, recovered = {}", db.was_recovered());

        Ok(Self { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self
            .db
            .get(key.as_bytes())?
            .map(|v| String::from_utf8(v.to_vec()).unwrap());

        Ok(value)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let res = self.db.remove(key.as_bytes())?;
        self.db.flush()?;

        match res {
            None => Err(Error::KeyNotFound),
            _ => Ok(()),
        }
    }
}

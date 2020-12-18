use std::path::PathBuf;

use crate::error::Result;

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

/// Wrapper for Sled storage engine
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    const LOG_NAME: &'static str = "sled.log";

    /// Returns `true` if a log already exists
    pub fn is_log_present(path: impl Into<PathBuf>) -> bool {
        let log_dir = path.into();
        let log_file = log_dir.join(Self::LOG_NAME);
        log_file.exists()
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let log_dir = path.into();
        let log_file = log_dir.join(Self::LOG_NAME);
        let db = sled::open(log_file)?;

        Ok(Self { db })
    }
}

impl KvsEngine for SledStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self
            .db
            .get(key.as_bytes())?
            .map(|v| String::from_utf8(v.to_vec()).unwrap()))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key.as_bytes())?;
        Ok(())
    }
}

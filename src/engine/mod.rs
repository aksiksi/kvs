use crate::error::Result;

pub mod kvs;
pub mod sled;

pub use kvs::KvStore;
pub use self::sled::SledKvsEngine;

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

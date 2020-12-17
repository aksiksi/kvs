use std::io::Write;
use std::net::TcpStream;

use crate::{kvstore::Command, Result};

pub struct KvsClient {
    socket: TcpStream,
}

impl KvsClient {
    pub fn new(addr: &str) -> Result<Self> {
        let socket = TcpStream::connect(addr)?;

        Ok(Self { socket })
    }

    pub fn get(&mut self, key: String) -> Result<String> {
        log::info!("Sending get: {}", key);

        let command = Command::Get(key.clone());
        let buf = rmp_serde::to_vec(&command)?;
        self.socket.write_all(&buf)?;

        log::info!("Waiting for value: {}", key);

        let value: String = rmp_serde::from_read(&self.socket)?;

        Ok(value)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        log::info!("Sending set: {}, {}", key, value);

        let command = Command::Set(key, value);
        let buf = rmp_serde::to_vec(&command)?;
        self.socket.write_all(&buf)?;
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        log::info!("Sending remove: {}", key);

        let command = Command::Remove(key);
        let buf = rmp_serde::to_vec(&command)?;
        self.socket.write_all(&buf)?;
        Ok(())
    }
}

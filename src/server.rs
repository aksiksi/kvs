use std::io::Write;
use std::net::{TcpListener, TcpStream};

use crate::Result;
use crate::{kvstore::Command, KvsEngine};

pub struct KvsServer {
    store: Box<dyn KvsEngine>,
    addr: String,
}

impl KvsServer {
    pub fn new(store: Box<dyn KvsEngine>, addr: String) -> Result<Self> {
        let server = KvsServer { store, addr };

        Ok(server)
    }

    fn handle_request(&mut self, mut stream: TcpStream, addr: String) -> Result<()> {
        log::info!("Received request from {}", addr);

        // Deserialize it to an entry
        let command: Command = rmp_serde::from_read(&stream)?;

        let value = match command {
            Command::Get(key) => self.store.get(key)?,
            Command::Set(key, value) => {
                self.store.set(key.clone(), value.clone())?;
                None
            }
            Command::Remove(key) => {
                self.store.remove(key.clone())?;
                None
            }
        };

        // Send back a reply in the case of get()
        if let Some(value) = value {
            log::info!("Sending reply back to {}", addr);
            let buf = rmp_serde::to_vec(&value)?;
            stream.write_all(&buf)?;
        }

        log::info!("Done with {}", addr);

        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        let socket = TcpListener::bind(&self.addr)?;

        loop {
            let (stream, addr) = socket.accept()?;
            self.handle_request(stream, addr.to_string())?;
        }
    }
}

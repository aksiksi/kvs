use std::io::Write;
use std::net::{TcpListener, TcpStream};

use serde::{Deserialize, Serialize};

use crate::{Error, KvsEngine, Result};

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    Set(String, String),
    Get(String),
    Remove(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Ok,
    Value(String),
    Error(Error),
}

pub struct KvsServer {
    store: Box<dyn KvsEngine>,
    addr: String,
}

impl KvsServer {
    pub fn new(store: Box<dyn KvsEngine>, addr: String) -> Result<Self> {
        let server = KvsServer { store, addr };
        Ok(server)
    }

    fn handle_request(&mut self, stream: &TcpStream, addr: String) -> Result<Option<String>> {
        log::info!("Received request from {}", addr);

        let request: Request = rmp_serde::from_read(stream)?;

        let value = match request {
            Request::Get(key) => {
                log::info!("Get: {}", key);
                self.store.get(key)?
            }
            Request::Set(key, value) => {
                log::info!("Set: {} -> {}", key, value);
                self.store.set(key.clone(), value.clone())?;
                None
            }
            Request::Remove(key) => {
                log::info!("Remove: {}", key);
                self.store.remove(key.clone())?;
                None
            }
        };

        Ok(value)
    }

    pub fn start(&mut self) -> Result<()> {
        let socket = TcpListener::bind(&self.addr)?;

        loop {
            let (mut stream, addr) = socket.accept()?;

            // Build a response based on the result of handling the request
            let response = match self.handle_request(&stream, addr.to_string()) {
                Ok(v) if v.is_some() => Response::Value(v.unwrap()),
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            };

            // Write back response to the socket
            let buf = rmp_serde::to_vec(&response)?;
            stream.write_all(&buf)?;
        }
    }
}

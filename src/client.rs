use std::io::Write;
use std::net::TcpStream;

use crate::{server::{Request, Response}, Result};

pub struct KvsClient {
    socket: TcpStream,
}

impl KvsClient {
    pub fn new(addr: &str) -> Result<Self> {
        let socket = TcpStream::connect(addr)?;

        Ok(Self { socket })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        log::info!("Sending get: {}", key);

        let req = Request::Get(key.clone());
        let buf = rmp_serde::to_vec(&req)?;
        self.socket.write_all(&buf)?;

        log::info!("Waiting for value: {}", key);

        let resp: Response = rmp_serde::from_read(&self.socket)?;

        match resp {
            Response::Value(v) => Ok(Some(v)),
            Response::Ok => Ok(None),
            Response::Error(e) => Err(e),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        log::info!("Sending set: {}, {}", key, value);

        let req = Request::Set(key, value);
        let buf = rmp_serde::to_vec(&req)?;
        self.socket.write_all(&buf)?;

        let resp: Response = rmp_serde::from_read(&self.socket)?;

        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(e),
            _ => panic!("not expected"),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        log::info!("Sending remove: {}", key);

        let req = Request::Remove(key);
        let buf = rmp_serde::to_vec(&req)?;
        self.socket.write_all(&buf)?;

        let resp: Response = rmp_serde::from_read(&self.socket)?;

        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(e),
            _ => panic!("not expected"),
        }
    }
}

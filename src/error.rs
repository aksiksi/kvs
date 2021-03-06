use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Deserialize, Serialize)]
pub enum Error {
    Generic(String),
    IOError(String),
    SerializeError(String),
    DeserializeError(String),
    SledError(String),
    KeyNotFound,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Generic(msg) => write!(f, "{}", msg),
            Self::IOError(msg) => write!(f, "{}", msg.to_string()),
            Self::SerializeError(msg) => write!(f, "SerializeError: {}", msg),
            Self::DeserializeError(msg) => write!(f, "DeserializeError: {}", msg),
            Self::SledError(msg) => write!(f, "SledError: {}", msg),
            Self::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(err.to_string())
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(err: rmp_serde::decode::Error) -> Self {
        Self::DeserializeError(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self::DeserializeError(err.to_string())
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(err: rmp_serde::encode::Error) -> Self {
        Self::SerializeError(err.to_string())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Self::Generic(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Self::Generic(s.to_owned())
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        match err {
            sled::Error::CollectionNotFound(_) => Self::KeyNotFound,
            _ => Self::SledError(err.to_string()),
        }
    }
}

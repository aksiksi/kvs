pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Generic(String),
    IOError(std::io::Error),
    SerializeError(String),
    DeserializeError(String),
    KeyNotFound,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Generic(msg) => write!(f, "{}", msg),
            Self::IOError(err) => write!(f, "{}", err.to_string()),
            Self::SerializeError(msg) => write!(f, "SerializeError: {}", msg),
            Self::DeserializeError(msg) => write!(f, "DeserializeError: {}", msg),
            Self::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(err)
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(err: rmp_serde::decode::Error) -> Self {
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

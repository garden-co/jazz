use thiserror::Error;

#[derive(Debug, Error)]
pub enum BTreeError {
    #[error("invalid options: {0}")]
    InvalidOptions(String),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("io error: {0}")]
    Io(String),
}

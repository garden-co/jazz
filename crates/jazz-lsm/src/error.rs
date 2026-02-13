use thiserror::Error;

use crate::fs::FsError;

#[derive(Debug, Error)]
pub enum LsmError {
    #[error("filesystem error: {0}")]
    Fs(#[from] FsError),

    #[error("manifest parse failed: {0}")]
    ManifestParse(String),

    #[error("unknown merge operator id {0}")]
    UnknownMergeOperator(u32),

    #[error("corrupt record in {path} at offset {offset}")]
    CorruptRecord { path: String, offset: u64 },

    #[error("invalid options: {0}")]
    InvalidOptions(String),
}

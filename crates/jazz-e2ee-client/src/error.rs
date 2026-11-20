//! The runtime's errors. Every operation keeps its original error
//! (`E2EE_PROTOCOL.md` §7): a store or provider failure is never turned
//! into a missing key.

use jazz_e2ee::protocol::ProtocolError;

use crate::store::StoreError;

#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// A stored document of a format this runtime does not read: refused
    /// and never overwritten (§6.1).
    #[error("the private document `{0}` has a format this runtime does not read")]
    UnrecognisedDocument(String),
    /// The application's encryption declarations or settings ask for
    /// something this runtime cannot do; names what.
    #[error("{0}")]
    Unsupported(String),
    /// The application's encryption settings are malformed.
    #[error("invalid encryption settings: {0}")]
    Settings(String),
    /// The database refused a call the runtime made.
    #[error(transparent)]
    Database(#[from] jazz::db::Error),
    /// The store was wiped: nothing more is read or written through this
    /// runtime, so a late operation never recreates it (§6.4).
    #[error("the encryption runtime was closed")]
    Closed,
}

impl From<jazz_e2ee::Error> for ClientError {
    fn from(error: jazz_e2ee::Error) -> Self {
        Self::Protocol(error.into())
    }
}

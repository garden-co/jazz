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
    /// An account reset whose generated records could not be published.
    #[error("account reset publication failed: {source}")]
    AccountResetPublication {
        record_ids: Box<[String; 3]>,
        refused: bool,
        #[source]
        source: Box<ClientError>,
    },
    /// This device already answered a challenge (its approval nonce is
    /// spent), or the server shows a proof of its own that it did not make
    /// in this run: it answers no other challenge (protocol §2.5). It
    /// enrols again as a new device.
    #[error("this device has already answered a challenge: it answers no other")]
    ApprovalAnswered,
    /// No challenge for this device is waiting.
    #[error("no challenge for this device")]
    NoChallenge,
    /// The approval code the user confirmed is not the one the proof
    /// gives: someone between the devices may have substituted keys, and
    /// nothing was approved (protocol §2.5).
    #[error("the approval codes do not match: nothing was approved")]
    ApprovalCodeMismatch,
    /// This device has not verified its account in this run
    /// ([`E2eeClient::ensure_account`](crate::E2eeClient::ensure_account)):
    /// its keys for spaces cannot be acquired or made without it.
    #[error("the account is not verified on this device: verify it first")]
    NoAccount,
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

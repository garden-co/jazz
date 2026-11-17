//! Content under held space keys (`E2EE_PROTOCOL.md` §4, §5, §6.2;
//! `E2EE_MIGRATIONS.md` §3, §4): the pure operations the write, read, query
//! and streaming hooks and the runtime's repair and conversion sweeps call.
//! Nothing here does I/O or keeps global state: callers pass the held keys,
//! the stored values and the declarations, and run every transaction.
//!
//! - [`held`]: the keys a device holds, per space root, with the marks of
//!   its own creations; which key a write uses (§5.2) and which keys may
//!   open a cell (§4.2); creation outcomes and their reconciliation (§5.4);
//!   what an acquisition shows about a missing version (§4.2, §4.3).
//! - [`cells`]: sealing a write's cells and their equality tokens, opening
//!   a stored value (short values, cell and stream records, stale spaces),
//!   and streaming large values (§4.1, §5.5).
//! - [`equality`]: query tokens per held version and the query shapes that
//!   are refused (§4.5).
//! - [`store`]: the held-key and lineage records of the private store, their
//!   canonical encoding, sealing and signing, and their load-time checks
//!   (§6.2).
//! - [`repair`]: the plan that rewrites a refused or lost creation's cells
//!   under the space's real key (§3.2).
//! - [`convert`]: schema conversion helpers (migrations §3, §4).

mod canonical;
pub mod cells;
pub mod convert;
pub mod equality;
pub mod held;
pub mod repair;
pub mod store;
mod stored;

use crate::records::{CellIdentity, EqualityColumn, EqualityIdentity};

use super::domain::SharedDomain;
use super::error::ProtocolError;
use super::records::space::SpaceAddress;

pub use cells::{
    CellInput, EqualityValueBuf, Opened, PlaintextValues, SealedCell, SealedCells, StreamOpen,
    StreamResealer, StreamRouting, StreamValueReader, begin_stream, open_cell, seal_cells,
    stream_routing,
};
pub use equality::{QueryPlan, QueryTokens, TableTarget, UnsupportedQuery, plan_equality};
pub use held::{
    CreationFate, HeldKeys, HeldSpace, KeyMark, KnownChain, MissingKey, WriteChoice, WriteKey,
};

/// The 5-byte cleared marker a schema conversion writes into an encrypted
/// source (§1.3, §4.1; migrations §3.2): "moved by a schema conversion".
pub const CLEARED_MARKER: [u8; 5] = [0x4a, 0x45, 0x32, 0x58, 0x01];

/// The data-error codes of unreadable cells and spaces (§4.3, §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataError {
    /// This device lacks the version's key and its account is not a member.
    KeyUnavailable,
    /// Per space: the account is a member and lacks a version it needs.
    KeyPending,
    /// Per space: the space has no valid root yet and this device's
    /// creation was refused while the client runs, or not yet made.
    SpacePending,
    /// An envelope of a mechanism the reader does not route.
    UnsupportedFormat,
    /// Authentication failed, the cell belongs elsewhere, is malformed, or
    /// names no epoch of the space's chain.
    InvalidCiphertext,
    /// The stored cell is the empty byte string; the row is returned with
    /// the column's default.
    EmptyCell,
    /// The cell holds the cleared marker and the reader's declaration names
    /// no conversion for the column.
    Superseded,
    /// The cell was sealed under another space of the row's history; the
    /// row is returned and needs repair (migrations §3.9).
    StaleSpace,
    /// Sealing a write failed; nothing is submitted.
    EncryptionFailed,
}

impl DataError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::KeyUnavailable => "key-unavailable",
            Self::KeyPending => "key-pending",
            Self::SpacePending => "space-pending",
            Self::UnsupportedFormat => "unsupported-format",
            Self::InvalidCiphertext => "invalid-ciphertext",
            Self::EmptyCell => "empty-cell",
            Self::Superseded => "superseded",
            Self::StaleSpace => "stale-space",
            Self::EncryptionFailed => "encryption-failed",
        }
    }

    /// Whether the code is reported once per `(table, space)` rather than
    /// per cell (§4.3).
    pub const fn is_per_space(self) -> bool {
        matches!(self, Self::KeyPending | Self::SpacePending)
    }
}

/// Why a content operation failed. A provider failure stays
/// [`ProtocolError::ProviderFailure`] inside [`ContentError::Protocol`] and
/// is never reported as a data error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ContentError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// A strict read met a cell it cannot return (§4.4), or a write met a
    /// space it cannot write (`space-pending`, §3.2).
    #[error("E2EE data error: {}", .0.code())]
    Data(DataError),
    /// A query shape encrypted columns do not support; never a scan (§4.5).
    #[error("Unsupported encrypted query: {}", .0.name())]
    UnsupportedQuery(UnsupportedQuery),
    /// An equality query on a column whose conversion or tag backfill is
    /// unfinished in the space (migrations §3.3, §7).
    #[error("An encrypted conversion is pending")]
    ConversionPending { pending: u64 },
    /// A large value for a column that may not stream: only non-indexed
    /// `bytes` columns stream (§4.1, §5.5).
    #[error("This column cannot hold a streamed encrypted value")]
    NotStreamable,
    /// A move that does not re-encrypt every encrypted column of the row
    /// (migrations §4.4), or a partial edit of an encrypted column.
    #[error("Partial encrypted writes are refused")]
    Partial,
}

impl From<crate::Error> for ContentError {
    fn from(error: crate::Error) -> Self {
        Self::Protocol(error.into())
    }
}

impl ContentError {
    pub const fn is_provider_failure(&self) -> bool {
        matches!(self, Self::Protocol(error) if error.is_provider_failure())
    }
}

pub type ContentResult<T> = core::result::Result<T, ContentError>;

/// One encrypted column as its declaration states it (§1.5, §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncryptedColumn<'a> {
    /// Jazz's stable catalogue identity of the column.
    pub column_id: &'a str,
    /// The canonical JSON of the column's sealed type, as every cell's
    /// context binds it (§4.1; migrations §4.3).
    pub cell_type: &'a [u8],
    /// The declared equality index, if any.
    pub equality: Option<EqualityColumn>,
    /// Whether the column's type is `bytes`.
    pub is_bytes: bool,
}

impl EncryptedColumn<'_> {
    /// Only non-indexed `bytes` columns may hold stream records (§4.1).
    pub fn may_stream(&self) -> bool {
        self.is_bytes && self.equality.is_none()
    }
}

/// Where one row's cells live: the shared domain `Dₛ`, the row's space,
/// and Jazz's catalogue identity of the table and the row's ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowTarget<'a> {
    pub domain: &'a SharedDomain,
    pub space: &'a SpaceAddress,
    pub table_id: &'a str,
    pub row_id: &'a str,
}

impl<'a> RowTarget<'a> {
    /// The same row and table, read or written under another space.
    pub fn in_space<'s>(&self, space: &'s SpaceAddress) -> RowTarget<'s>
    where
        'a: 's,
    {
        RowTarget {
            domain: self.domain,
            space,
            table_id: self.table_id,
            row_id: self.row_id,
        }
    }

    fn cell<'c>(&'c self, column_id: &'c str, epoch_id: &'c str) -> CellIdentity<'c> {
        CellIdentity {
            application: self.domain.as_str(),
            scope_id: &self.space.scope_id,
            identifier: &self.space.identifier,
            table_id: self.table_id,
            row_id: self.row_id,
            column_id,
            epoch_id,
        }
    }

    fn equality<'c>(&'c self, column_id: &'c str, epoch_id: &'c str) -> EqualityIdentity<'c> {
        EqualityIdentity {
            application: self.domain.as_str(),
            scope_id: &self.space.scope_id,
            identifier: &self.space.identifier,
            table_id: self.table_id,
            column_id,
            epoch_id,
        }
    }
}

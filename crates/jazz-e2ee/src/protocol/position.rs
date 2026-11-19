//! Accepted positions (`E2EE_PROTOCOL.md` §0, "What Jazz provides").
//!
//! A ledger read returns, for each row, its accepted position, and a common
//! cut `through`. Rows accepted in one transaction share a position. "Before
//! `p`" always means strictly earlier.

use std::collections::BTreeSet;

use super::error::{ProtocolError, ProtocolResult};

/// An accepted position: an unsigned 64-bit integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);

/// A row with its accepted position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accepted<T> {
    pub position: Position,
    pub row: T,
}

impl<T> Accepted<T> {
    pub const fn new(position: Position, row: T) -> Self {
        Self { position, row }
    }
}

/// A row that has a row ID.
pub trait RowId {
    fn row_id(&self) -> &str;
}

/// Checks one table's rows of a ledger read: no position beyond the cut and
/// no row ID twice.
pub fn check_read<T: RowId>(rows: &[Accepted<T>], through: Position) -> ProtocolResult<()> {
    let mut seen = BTreeSet::new();
    for row in rows {
        if row.position > through || !seen.insert(row.row.row_id()) {
            return Err(ProtocolError::InconsistentEvidence);
        }
    }
    Ok(())
}

/// The rows strictly before `cut`, in their given order.
pub fn before<T: Clone>(rows: &[Accepted<T>], cut: Position) -> Vec<Accepted<T>> {
    rows.iter()
        .filter(|row| row.position < cut)
        .cloned()
        .collect()
}

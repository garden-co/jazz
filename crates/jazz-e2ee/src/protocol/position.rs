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

impl<T: RowId> Accepted<T> {
    pub(super) fn position_and_id(&self) -> (Position, &str) {
        (self.position, self.row.row_id())
    }
}

pub(super) fn sort_by_position_then_id<T>(
    rows: &mut [T],
    position_and_id: impl Fn(&T) -> (Position, &str),
) {
    rows.sort_by(|a, b| {
        let (a_position, a_id) = position_and_id(a);
        let (b_position, b_id) = position_and_id(b);
        a_position
            .cmp(&b_position)
            .then_with(|| super::json::utf16_cmp(a_id, b_id))
    });
}

pub(super) fn by_position_then_id<T: RowId>(rows: &mut [&Accepted<T>]) {
    sort_by_position_then_id(rows, |row| row.position_and_id());
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

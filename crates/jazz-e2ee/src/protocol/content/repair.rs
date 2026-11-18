//! Repair of refused and lost creations (`E2EE_PROTOCOL.md` §3.2), as a
//! plan: which cells of the rows read still name the losing epoch, and
//! those cells re-encrypted under the newest held version of the space's
//! chain, with fresh equality tags. Other cells are not written.
//!
//! The runtime runs the plan: it reads each batch of at most
//! [`REPAIR_BATCH`] rows in a serializable transaction (each row with its
//! space coordinate and the exact cells it rewrites), plans from that read
//! only, and writes the rewritten cells in the same transaction, so that
//! read-set validation refuses it if any row changed after its cut; it then
//! re-reads and plans again. Rows of other spaces, and cells that do not
//! open under the losing key or whose plaintext is not a value of the
//! column, are left as they are and reported; one such cell never stops
//! the repair of the rest. A provider failure, or inconsistent evidence,
//! fails the plan.

use crate::error::Error;
use crate::random::RandomSource;
use crate::records::{RecordKind, read_stored_record};
use crate::registry::{Registry, Suite};

use super::super::domain::SharedDomain;
use super::super::error::ProtocolError;
use super::cells::{
    CellInput, PlaintextValues, SealedCell, classify_short, decrypt_with, decrypted_equality_value,
    seal_one,
};
use super::held::{HeldKeys, KeyMark};
use super::{ContentResult, DataError, EncryptedColumn, RowTarget};

/// The default number of rows one repair transaction reads and rewrites.
pub const REPAIR_BATCH: usize = 100;

/// One row as the repair transaction read it.
#[derive(Debug, Clone, Copy)]
pub struct RepairRow<'a> {
    pub row_id: &'a str,
    /// The row's space coordinate: the space row's ID. Only the ID is
    /// compared with the losing root's address: every row of the target
    /// table names a space of the table's one scope, and each cell is
    /// bound to its full space address (§4.1), so a cell of another scope
    /// does not open under the losing key and is skipped, never rewritten.
    pub space_identifier: &'a str,
    /// The row's encrypted cells, by column ID, as read.
    pub cells: &'a [(&'a str, &'a [u8])],
}

/// One rewrite of a repaired row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rewrite {
    /// A cell re-encrypted under the target version, with its token.
    Cell(SealedCell),
    /// A streamed value under the losing epoch, planned without opening
    /// it: the runtime opens and verifies it under the losing key when it
    /// performs the rewrite, re-encrypting it in bounded memory with a
    /// [`StreamResealer`](super::cells::StreamResealer) under the target
    /// version. A stream that fails to open or verify then is not written
    /// and is reported like a skipped cell; the row's other rewrites stand.
    Stream { column_id: String },
}

/// The plan for one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRepair {
    pub row_id: String,
    /// The row's space coordinate names another space: nothing is
    /// rewritten.
    pub other_space: bool,
    pub rewrites: Vec<Rewrite>,
    /// Cells naming the losing epoch that do not open under its key, or
    /// whose plaintext is not a value of the column; they stay as they
    /// are.
    pub skipped: Vec<(String, DataError)>,
}

/// A repair plan for one batch of rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairPlan {
    pub losing_epoch_id: String,
    /// The version every rewrite is sealed under.
    pub epoch_id: String,
    pub rows: Vec<RowRepair>,
}

/// What a repair reads and writes with: the mechanisms, the held keys, the
/// table, its encrypted columns and the decoder of plaintext values.
#[derive(Clone, Copy)]
pub struct RepairTarget<'a> {
    pub registry: &'a Registry,
    pub suite: Suite<'a>,
    pub held: &'a HeldKeys,
    pub domain: &'a SharedDomain,
    pub table_id: &'a str,
    pub columns: &'a [EncryptedColumn<'a>],
    pub values: &'a dyn PlaintextValues,
}

/// Plans the repair of `rows` (§3.2): the losing key is the `refused` root
/// key of `losing_root_id`, and every rewrite is sealed under the newest
/// held version that is not `refused` of the space's chain, whose root is
/// `space_root_id`, of the same address. `None` while no such key is held,
/// or while the device has read no chain of the address: repair waits for
/// the real key, and seals only under the root of the chain it read, as a
/// write does ([`HeldKeys::write_key`]).
///
/// A losing root whose key is not `refused` (its creation is unsettled) or
/// not held, roots of different addresses, and a cell of a column not in
/// `columns` or given twice, are invalid inputs. A `space_root_id` that is
/// not the root of the chain the device read is inconsistent evidence.
pub fn plan_repair(
    target: &RepairTarget<'_>,
    losing_root_id: &str,
    space_root_id: &str,
    rows: &[RepairRow<'_>],
    rng: &mut dyn RandomSource,
) -> ContentResult<Option<RepairPlan>> {
    let losing = target
        .held
        .space(losing_root_id)
        .filter(|space| space.mark(0) == Some(&KeyMark::Refused))
        .ok_or(Error::InvalidInput)?;
    let losing_key = losing.key_at(0).expect("a refused root key is held");
    let losing_epoch = losing.root.epoch_id.as_str();
    let Some(real) = target.held.space(space_root_id) else {
        return Ok(None);
    };
    if real.address() != losing.address() || real.root_id() == losing.root_id() {
        return Err(ProtocolError::InconsistentEvidence.into());
    }
    match target.held.known(losing.address()) {
        None => return Ok(None),
        Some(known) if known.root_id() != real.root_id() => {
            return Err(ProtocolError::InconsistentEvidence.into());
        }
        Some(_) => {}
    }
    let Some((depth, key)) = real
        .keys
        .iter()
        .rev()
        .find(|(_, entry)| entry.mark != KeyMark::Refused)
        .map(|(depth, entry)| (*depth, &*entry.key))
    else {
        return Ok(None);
    };
    let epoch_id = real
        .epoch_at(depth)
        .expect("a held depth is in the lineage");
    let address = losing.address();
    let mut planned = Vec::with_capacity(rows.len());
    for row in rows {
        let mut repair = RowRepair {
            row_id: row.row_id.to_owned(),
            other_space: row.space_identifier != address.identifier,
            rewrites: Vec::new(),
            skipped: Vec::new(),
        };
        let mut seen = std::collections::BTreeSet::new();
        if !row
            .cells
            .iter()
            .all(|(column_id, _)| seen.insert(*column_id))
        {
            return Err(Error::InvalidInput.into());
        }
        let place = RowTarget {
            domain: target.domain,
            space: address,
            table_id: target.table_id,
            row_id: row.row_id,
        };
        for (column_id, stored) in row.cells {
            let column = target
                .columns
                .iter()
                .find(|column| column.column_id == *column_id)
                .ok_or(Error::InvalidInput)?;
            if repair.other_space || classify_short(stored, false).is_some() {
                continue;
            }
            // A value that does not route names no epoch, so not the
            // losing one.
            let Ok(record) = read_stored_record(stored) else {
                continue;
            };
            if record.epoch_id != losing_epoch {
                continue;
            }
            if record.kind == RecordKind::Stream {
                if column.may_stream() {
                    repair.rewrites.push(Rewrite::Stream {
                        column_id: column.column_id.to_owned(),
                    });
                } else {
                    repair
                        .skipped
                        .push((column.column_id.to_owned(), DataError::UnsupportedFormat));
                }
                continue;
            }
            let plain = match decrypt_with(
                target.registry,
                losing_key,
                &place,
                column,
                RecordKind::Cell,
                losing_epoch,
                stored,
            )? {
                Ok(plain) => plain,
                Err(code) => {
                    repair.skipped.push((column.column_id.to_owned(), code));
                    continue;
                }
            };
            let equality = match decrypted_equality_value(target.values, column, &plain)? {
                Ok(equality) => equality,
                Err(code) => {
                    repair.skipped.push((column.column_id.to_owned(), code));
                    continue;
                }
            };
            let input = CellInput {
                column: *column,
                value: &plain,
                equality: equality.as_ref().map(|value| value.as_value()),
            };
            repair.rewrites.push(Rewrite::Cell(seal_one(
                &target.suite,
                key,
                epoch_id,
                &place,
                &input,
                rng,
            )?));
        }
        planned.push(repair);
    }
    Ok(Some(RepairPlan {
        losing_epoch_id: losing_epoch.to_owned(),
        epoch_id: epoch_id.to_owned(),
        rows: planned,
    }))
}

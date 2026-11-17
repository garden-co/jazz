//! Schema conversion helpers (`E2EE_MIGRATIONS.md` §3, §4) for the write
//! hook and the runtime's sweeps.
//!
//! A conversion pairs a **source** column (the old form) with a **target**
//! column (the new form) on the same row. If the source is present it is
//! the value, otherwise the target is (§3.2). Every write by a client that
//! knows the conversion writes the target and clears the source in the
//! same row write: an encrypted source is cleared to [`CLEARED_MARKER`].
//!
//! Encrypting seals the plaintext source into the target with
//! [`seal_cells`](super::cells::seal_cells); decrypting opens the source with
//! [`open_cell`](super::cells::open_cell); a re-encrypt opens the source
//! with its own sealed type and seals the converted value into the target.
//! What is specific to conversions is here: presence of each side, tag
//! backfill under each cell's own epoch (§4.3), re-homing and moving whole
//! rows to another space (§4.4), and the tokens of an equality query on a
//! converting column (§3.3).

use crate::agility::EqualityTagger;
use crate::error::Error;
use crate::random::RandomSource;
use crate::records::{self, EqualityValue, RecordKind, read_stored_record};
use crate::registry::{Registry, Suite};

use super::super::error::ProtocolError;
use super::cells::{
    CellInput, Opened, PlaintextValues, SealedCell, decrypted_equality_value, open_traced, seal_one,
};
use super::equality::{QueryPlan, QueryTokens, TableTarget, plan_equality};
use super::held::{HeldKeys, WriteKey};
use super::{CLEARED_MARKER, ContentError, ContentResult, EncryptedColumn, RowTarget};

/// The most source values a declared `reverse` mapping may list (§3.3).
pub const MAX_REVERSE_VALUES: usize = 16;

/// Whether an encrypted source holds a value: it is absent only when it is
/// the cleared marker (§3.2; a real cell record is at least 78 bytes in
/// all, protocol §4.1). The empty value counts as present (§3.3).
pub fn encrypted_source_present(stored: &[u8]) -> bool {
    stored != CLEARED_MARKER
}

/// Whether an encrypted target holds a value: it is absent when it is
/// empty or the cleared marker (§3.2).
pub fn encrypted_target_present(stored: &[u8]) -> bool {
    !stored.is_empty() && stored != CLEARED_MARKER
}

/// Which side of a conversion pair holds the row's value (§3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Source,
    Target,
    /// Both are absent: the target's declared default.
    Neither,
}

/// "If the source is present, it is the value; otherwise the target is."
/// Presence of a plaintext side is non-null; of an encrypted side, as
/// [`encrypted_source_present`] and [`encrypted_target_present`] say.
pub fn value_side(source_present: bool, target_present: bool) -> Side {
    if source_present {
        Side::Source
    } else if target_present {
        Side::Target
    } else {
        Side::Neither
    }
}

/// What tag backfill (§4.3) does with one cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backfill {
    /// The cell's token, under its own epoch, for `__e2ee_eq_<column>`.
    Token { epoch_id: String, token: Vec<u8> },
    /// The cell gives no token here (empty, cleared, sealed under another
    /// space, missing or unreadable): the outcome of opening it. A
    /// plaintext that is not a value of the column is `invalid-ciphertext`.
    Skip(Opened),
}

/// Tag backfill of one cell (§4.3): decrypts it and computes its token
/// under the cell's own epoch and key, writing nothing else. A cell sealed
/// under another space, one that does not open, or one whose plaintext is
/// not a value of the column, gets no token.
#[allow(clippy::too_many_arguments)]
pub fn backfill_token(
    registry: &Registry,
    tagger: &dyn EqualityTagger,
    held: &HeldKeys,
    row: &RowTarget<'_>,
    column: &EncryptedColumn<'_>,
    stored: &[u8],
    values: &dyn PlaintextValues,
) -> ContentResult<Backfill> {
    let index = column
        .equality
        .ok_or(super::equality::UnsupportedQuery::UnindexedEquality)?;
    let (opened, opener) = open_traced(registry, held, row, column, stored, false)?;
    let (Opened::Value(plain), Some((space, epoch_id))) = (&opened, opener) else {
        return Ok(Backfill::Skip(opened));
    };
    let depth = space.depth_of(&epoch_id).expect("the key opened its epoch");
    let key = space.key_at(depth).expect("the key opened its epoch");
    let value = match decrypted_equality_value(values, column, plain)? {
        Ok(value) => value.expect("the column has an index"),
        Err(code) => return Ok(Backfill::Skip(Opened::Unreadable(code))),
    };
    let token = records::equality_token_of(
        tagger,
        key,
        &row.equality(column.column_id, &epoch_id),
        column.cell_type,
        &index,
        &value.as_value(),
    )?;
    Ok(Backfill::Token { epoch_id, token })
}

/// One encrypted cell to carry to another space: its source column, the
/// target column, and the stored source value.
#[derive(Debug, Clone, Copy)]
pub struct RehomeCell<'a> {
    pub source: EncryptedColumn<'a>,
    pub target: EncryptedColumn<'a>,
    pub stored: &'a [u8],
}

/// One converted cell of a re-homed or moved row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Converted {
    /// The value sealed under the target space into the target column.
    Cell(SealedCell),
    /// A streamed value, planned without opening it: the runtime opens
    /// and verifies it under the old space when it performs the write,
    /// re-encrypting it in bounded memory with a
    /// [`StreamResealer`](super::cells::StreamResealer) into the target
    /// column under the target space. If it fails to open or verify then,
    /// the runtime writes nothing of the row, so the row still converts
    /// whole or not at all.
    Stream {
        source_column: String,
        target_column: String,
    },
    /// The source is the empty value, "never written": no value to carry.
    Empty { target_column: String },
    /// The source is already cleared: nothing to carry.
    Cleared { target_column: String },
}

/// What re-homing one row gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rehome {
    /// Every cell, converted. The caller writes the targets and clears
    /// each source to [`CLEARED_MARKER`] in one row write.
    Converted(Vec<Converted>),
    /// A source cell this device cannot open under the old space, or
    /// whose plaintext is not a value of the target column: the row is not
    /// converted; other rows are unaffected.
    Unconvertible { column_id: String, opened: Opened },
}

/// Re-homes one row (migrations §4.4): opens each source cell under the
/// old space `from` with its source column's sealed type and seals it
/// under `key`, a held key of the new space `to`, into its target column
/// with a fresh token. The row converts whole or not at all. `key` must be
/// of `to`'s space, and the two spaces must differ.
#[allow(clippy::too_many_arguments)]
pub fn rehome_row(
    registry: &Registry,
    suite: &Suite<'_>,
    held: &HeldKeys,
    from: &RowTarget<'_>,
    key: &WriteKey<'_>,
    to: &RowTarget<'_>,
    cells: &[RehomeCell<'_>],
    values: &dyn PlaintextValues,
    rng: &mut dyn RandomSource,
) -> ContentResult<Rehome> {
    if key.address() != to.space {
        return Err(ProtocolError::InconsistentEvidence.into());
    }
    if from.space == to.space || from.row_id != to.row_id || from.table_id != to.table_id {
        return Err(Error::InvalidInput.into());
    }
    let mut converted = Vec::with_capacity(cells.len());
    for cell in cells {
        let target_column = cell.target.column_id.to_owned();
        let stream =
            read_stored_record(cell.stored).is_ok_and(|record| record.kind == RecordKind::Stream);
        if stream && cell.source.may_stream() && cell.target.may_stream() {
            converted.push(Converted::Stream {
                source_column: cell.source.column_id.to_owned(),
                target_column,
            });
            continue;
        }
        let opened = open_traced(registry, held, from, &cell.source, cell.stored, true)?.0;
        let plain = match opened {
            Opened::Value(plain) => plain,
            Opened::Empty => {
                converted.push(Converted::Empty { target_column });
                continue;
            }
            Opened::Cleared => {
                converted.push(Converted::Cleared { target_column });
                continue;
            }
            other => {
                return Ok(Rehome::Unconvertible {
                    column_id: cell.source.column_id.to_owned(),
                    opened: other,
                });
            }
        };
        let equality = match decrypted_equality_value(values, &cell.target, &plain)? {
            Ok(equality) => equality,
            Err(code) => {
                return Ok(Rehome::Unconvertible {
                    column_id: cell.source.column_id.to_owned(),
                    opened: Opened::Unreadable(code),
                });
            }
        };
        let input = CellInput {
            column: cell.target,
            value: &plain,
            equality: equality.as_ref().map(|value| value.as_value()),
        };
        converted.push(Converted::Cell(seal_one(
            suite,
            key.key(),
            key.epoch_id(),
            to,
            &input,
            rng,
        )?));
    }
    Ok(Rehome::Converted(converted))
}

/// The `move` write (migrations §4.4): re-encrypts every encrypted column
/// of the row under the target space, in place, with held keys of both
/// spaces. `stored` must give every column of `columns` exactly once:
/// partial moves are refused ([`ContentError::Partial`]).
#[allow(clippy::too_many_arguments)]
pub fn move_row(
    registry: &Registry,
    suite: &Suite<'_>,
    held: &HeldKeys,
    from: &RowTarget<'_>,
    key: &WriteKey<'_>,
    to: &RowTarget<'_>,
    columns: &[EncryptedColumn<'_>],
    stored: &[(&str, &[u8])],
    values: &dyn PlaintextValues,
    rng: &mut dyn RandomSource,
) -> ContentResult<Rehome> {
    let complete = stored.len() == columns.len()
        && columns.iter().all(|column| {
            stored
                .iter()
                .filter(|(column_id, _)| *column_id == column.column_id)
                .count()
                == 1
        });
    if !complete {
        return Err(ContentError::Partial);
    }
    let cells: Vec<_> = columns
        .iter()
        .map(|column| RehomeCell {
            source: *column,
            target: *column,
            stored: stored
                .iter()
                .find(|(column_id, _)| *column_id == column.column_id)
                .expect("checked complete")
                .1,
        })
        .collect();
    rehome_row(registry, suite, held, from, key, to, &cells, values, rng)
}

/// How an equality query reaches source rows of a converting column
/// (§3.3), when it can: the source column (tagged), its table in its own
/// space (the old space for a re-home), and the source values whose tokens
/// cover every source that converts to the searched value: the searched
/// value itself for an identity conversion (a re-home, or a re-encrypt
/// without `convert`), or the list a declared `reverse` returns.
#[derive(Debug, Clone, Copy)]
pub struct SourceIndex<'a> {
    pub table: TableTarget<'a>,
    pub column: EncryptedColumn<'a>,
    pub values: &'a [EqualityValue<'a>],
}

/// The two tag predicates of an equality query on a converting column:
/// `__e2ee_eq_<source> IN source OR __e2ee_eq_<target> IN target`, each
/// with its own context. The residual then applies source-present
/// precedence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionPlan {
    /// `None` when no source predicate is needed: no row of the space has
    /// a present source.
    pub source: Option<QueryPlan>,
    pub target: QueryPlan,
}

/// Plans `eq(column, value)` on a converting column (§3.3). `pending` is
/// the number of rows of the space with a present source (or, for an
/// encrypt or a tag backfill, without a token), counted by the runtime
/// before planning. Without a [`SourceIndex`] (an arbitrary `convert`
/// without `reverse`, an untagged source, an encrypt or a tag backfill), a
/// space with pending rows fails [`ContentError::ConversionPending`]; it
/// never returns a result that could miss them.
#[allow(clippy::too_many_arguments)]
pub fn plan_conversion_equality(
    tagger: &dyn EqualityTagger,
    held: &HeldKeys,
    table: &TableTarget<'_>,
    column: &EncryptedColumn<'_>,
    value: &EqualityValue<'_>,
    source: Option<&SourceIndex<'_>>,
    pending: u64,
    online: bool,
) -> ContentResult<ConversionPlan> {
    let target = plan_equality(tagger, held, table, column, value, online)?;
    if pending == 0 {
        return Ok(ConversionPlan {
            source: None,
            target,
        });
    }
    let Some(source) = source.filter(|source| source.column.equality.is_some()) else {
        return Err(ContentError::ConversionPending { pending });
    };
    if source.values.len() > MAX_REVERSE_VALUES {
        return Err(Error::InvalidInput.into());
    }
    let mut merged: Option<QueryPlan> = None;
    for value in source.values {
        let plan = plan_equality(tagger, held, &source.table, &source.column, value, online)?;
        merged = Some(match (merged, plan) {
            (None, plan) => plan,
            (Some(QueryPlan::Tokens(mut all)), QueryPlan::Tokens(more)) => {
                all.tokens.extend(more.tokens);
                all.tokens.sort();
                all.tokens.dedup();
                all.incomplete |= more.incomplete;
                QueryPlan::Tokens(all)
            }
            // The version set is the same for every value of one space.
            (Some(plan), _) => plan,
        });
    }
    let source = merged.unwrap_or(QueryPlan::Tokens(QueryTokens {
        tokens: Vec::new(),
        epochs: Vec::new(),
        incomplete: false,
    }));
    Ok(ConversionPlan {
        source: Some(source),
        target,
    })
}

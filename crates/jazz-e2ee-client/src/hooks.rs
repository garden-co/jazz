//! The database's encryption hooks over the keys this device holds
//! (`E2EE_PROTOCOL.md` §4, §5; core hooks note H2–H5).
//!
//! They never await and never call the database: what they lack they
//! report. A write to a space with no usable held key is `NeedsKey`; a
//! read leaves out the rows whose version this device does not hold and
//! lists the versions for the runtime. A held key permits the write
//! (§5.1): nothing else is checked here.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use jazz::db::{CurrentRow, Error, ErrorCode};
use jazz::e2ee::{
    E2eeDeclarations, E2eeHooks, EpochSetId, OpenOutcome, QueryPlan, ReadSite, Residual,
    ResidualPred, SealError, StreamSealer, StreamTarget, Unreadable, UnreadableCode, WriteCells,
    WriteKind,
};
use jazz::ids::RowUuid;
use jazz::query::{Operand, Predicate, Query};
use jazz::records::Value;
use jazz::tools::deployment::artifact::encryption::equality_column;
use jazz_e2ee::Zeroizing;
use jazz_e2ee::agility::StreamWriter;
use jazz_e2ee::primitives::hash::sha256;
use jazz_e2ee::protocol::content::cells::{CellInput, Opened, begin_stream, open_cell, seal_cells};
use jazz_e2ee::protocol::content::equality::{
    QueryPlan as EqualityPlan, TableTarget, plan_equality,
};
use jazz_e2ee::protocol::content::{
    ContentError, DataError, EncryptedColumn, EqualityValueBuf, KeyMark, RowTarget, WriteChoice,
    WriteKey,
};
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::records::{RecordKind, read_stored_record};

use crate::account::rows::{Row, SPACE_ROOTS};
use crate::state::{
    Catalogue, KeyState, Missing, Report, Shared, TablePlan, db_address, not_observed,
};
use crate::values::ColumnCodec;

/// The hooks a database opens with; a handle on the runtime's state.
pub(crate) struct Hooks {
    pub(crate) declarations: E2eeDeclarations,
    pub(crate) shared: Rc<Shared>,
}

fn rejected(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::WriteRejected, message)
}

fn closed() -> Error {
    Error::new(ErrorCode::Protocol, "the encryption runtime was closed")
}

/// A failure of the crypto provider or the library: loud, never a data
/// error (protocol §0, §7).
fn failed(error: ContentError) -> Error {
    Error::new(ErrorCode::Protocol, format!("encryption failed: {error}"))
}

pub(crate) fn unsupported_query(what: &str, table: &str) -> Error {
    Error::new(
        ErrorCode::Query,
        format!("Reading encrypted table `{table}` with {what} is not supported yet"),
    )
}

/// A space cell's row ID, `None` when it is null or absent.
fn identifier(value: Option<&Value>) -> Result<Option<String>, ()> {
    match value {
        None | Some(Value::Nullable(None)) => Ok(None),
        Some(Value::Uuid(id)) => Ok(Some(id.to_string())),
        Some(Value::Nullable(Some(inner))) => match &**inner {
            Value::Uuid(id) => Ok(Some(id.to_string())),
            _ => Err(()),
        },
        Some(_) => Err(()),
    }
}

pub(crate) fn code(error: DataError) -> UnreadableCode {
    match error {
        DataError::KeyUnavailable => UnreadableCode::KeyUnavailable,
        DataError::KeyPending => UnreadableCode::KeyPending,
        DataError::SpacePending => UnreadableCode::SpacePending,
        DataError::UnsupportedFormat => UnreadableCode::UnsupportedFormat,
        DataError::EmptyCell => UnreadableCode::EmptyCell,
        DataError::Superseded => UnreadableCode::Superseded,
        DataError::StaleSpace => UnreadableCode::StaleSpace,
        DataError::InvalidCiphertext | DataError::EncryptionFailed => {
            UnreadableCode::InvalidCiphertext
        }
    }
}

fn column<'a>(
    catalogue: &'a Catalogue,
    table: &str,
    name: &str,
    codec: &'a ColumnCodec,
) -> Result<EncryptedColumn<'a>, Error> {
    Ok(EncryptedColumn {
        column_id: catalogue.column(table, name)?,
        cell_type: &codec.cell_type,
        equality: codec.equality,
        is_bytes: codec.is_bytes,
    })
}

impl Hooks {
    fn plan(&self, table: &str) -> Option<&TablePlan> {
        self.shared.plans.get(table)
    }

    fn address(keys: &KeyState, plan: &TablePlan, identifier: &str) -> Result<SpaceAddress, Error> {
        let catalogue = keys.catalogue.as_ref().ok_or_else(not_observed)?;
        SpaceAddress::new(catalogue.table(&plan.entry.scope)?, identifier)
            .map_err(|_| rejected("an encrypted row's space is not a row ID"))
    }

    /// Ask the binding to serve pending removals and rotate before choosing the write key.
    fn check_rotation(&self, address: &SpaceAddress) -> Result<(), SealError> {
        // The space joins the watch of its member accounts, and an update
        // of theirs the runtime has not served yet is taken now.
        self.shared
            .written
            .borrow_mut()
            .entry(db_address(address))
            .or_default();
        self.shared.notice_member_changes();
        let mut keys = self.shared.keys.borrow_mut();
        let call = self.shared.current_write.get();
        let mark = (call, address.clone());
        // A retry without a wrapping write call takes its mark here, so the
        // next write gets its own attempt. A wrapped call keeps it until exit.
        let tried = if call.is_none() {
            keys.rotation_tried.remove(&mark)
        } else {
            keys.rotation_tried.contains(&mark)
        };
        let removal = !self.shared.member_removals.borrow().is_empty();
        // A key of a space whose mark this runtime has not evaluated may be
        // one a removed device holds (see `KeyState::needs_evaluation`): the
        // runtime reads the space first. Only a creation this run prepared
        // or staged is new to everyone; a key held `unpublished` from an
        // earlier run may be the space's accepted root, settled later.
        let unevaluated = keys.needs_evaluation(address)
            && match keys
                .held
                .write_key(address, keys.creation_refused.contains(address))
            {
                WriteChoice::Key(key) => {
                    let creations = self.shared.creations.borrow();
                    let created_here = matches!(key.mark(), KeyMark::Unpublished { .. })
                        && (creations.staged.contains(key.root_id())
                            || creations
                                .prepared
                                .get(address)
                                .is_some_and(|root| root.id == key.root_id()));
                    !created_here
                }
                _ => false,
            };
        if keys.online
            && !tried
            && (keys.rotation_pending.contains(address) || removal || unevaluated)
        {
            return Err(SealError::NeedsKey {
                space: db_address(address),
            });
        }
        Ok(())
    }
}

/// Whether the stored `cell` names an epoch whose key this device holds only
/// as the key of a refused or lost creation of `address`.
fn under_refused_key(keys: &KeyState, address: &SpaceAddress, cell: &[u8]) -> bool {
    let Ok(record) = read_stored_record(cell) else {
        return false;
    };
    keys.held.roots_of(address).into_iter().any(|space| {
        space
            .depth_of(record.epoch_id)
            .is_some_and(|depth| space.mark(depth) == Some(&KeyMark::Refused))
    })
}

/// The key a write to `address` seals under, or the reason it cannot yet
/// (protocol §3.2, §5.1). The key of a root whose staging was `dropped` is
/// not used: the runtime abandons that creation first.
pub(crate) fn write_key<'k>(
    keys: &'k KeyState,
    dropped: &BTreeSet<String>,
    address: &SpaceAddress,
    refused_here: bool,
) -> Result<WriteKey<'k>, SealError> {
    let space = || db_address(address);
    match keys.held.write_key(address, refused_here) {
        WriteChoice::Key(key)
            if matches!(key.mark(), KeyMark::Unpublished { .. })
                && dropped.contains(key.root_id()) =>
        {
            Err(SealError::NeedsKey { space: space() })
        }
        WriteChoice::Key(key) => Ok(key),
        WriteChoice::NeedsKey => Err(SealError::NeedsKey { space: space() }),
        WriteChoice::SpacePending => Err(SealError::SpacePending { space: space() }),
        // Competing roots: the runtime reads the chain and tries again;
        // offline it cannot, and the write waits (protocol §3.2).
        WriteChoice::NeedsChainRead if keys.online => Err(SealError::NeedsKey { space: space() }),
        WriteChoice::NeedsChainRead => Err(SealError::SpacePending { space: space() }),
    }
}

/// Whether `cell` holds the value a migration gave rows stored before
/// `column` existed ([`ReadSite::is_lens_default`]), an optional stored
/// column's cell compared without its nullable wrapper.
fn lens_supplied(site: &ReadSite<'_>, column: &str, cell: &Value) -> bool {
    let Some(default) = site.lens_defaults.get(column) else {
        return false;
    };
    match cell {
        Value::Nullable(Some(inner)) => **inner == *default || cell == default,
        _ => cell == default,
    }
}

/// One streamed value: the routing record and header, then the sealed
/// source bytes, then the final record.
struct Sealer {
    writer: Box<dyn StreamWriter>,
    prefix: Option<Vec<u8>>,
}

impl StreamSealer for Sealer {
    fn header(&mut self) -> Result<Vec<u8>, Error> {
        Ok(self.prefix.take().unwrap_or_default())
    }

    fn push(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        self.writer
            .update(plaintext, &mut out)
            .map_err(|error| failed(error.into()))?;
        Ok(out)
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        self.writer
            .finish(&mut out)
            .map_err(|error| failed(error.into()))?;
        Ok(out)
    }
}

impl E2eeHooks for Hooks {
    fn declarations(&self) -> &E2eeDeclarations {
        &self.declarations
    }

    /// A runtime is built from the declarations `load_client_encrypted`
    /// returns for an encrypted app, which name no table for a version that
    /// keeps Jazz's system tables without declaring an encrypted column.
    fn built_from_encrypted_app(&self) -> bool {
        true
    }

    fn seal_write(&self, write: &mut WriteCells<'_>) -> Result<(), SealError> {
        // A member row this client adds or changes: the space is served by
        // the next pass after the write commits (protocol §3.4).
        if write.table == crate::account::rows::SPACE_MEMBERS
            && matches!(
                write.kind,
                WriteKind::Insert | WriteKind::Update | WriteKind::Upsert
            )
            && let (Some(Value::Uuid(scope)), Some(Value::Uuid(id))) =
                (write.cells.get("scopeId"), write.cells.get("identifier"))
        {
            let mut changes = self.shared.member_changes.borrow_mut();
            if changes.len() == crate::state::MAX_REPORTS {
                changes.pop_front();
            }
            changes.push_back((
                write.row,
                jazz::e2ee::SpaceAddress {
                    scope_id: scope.to_string(),
                    identifier: id.to_string(),
                },
                web_time::Instant::now(),
            ));
        }
        // A member row this client deletes, a removal: the space is served
        // before the next encrypted write, so the pass sets the
        // needs-rotation mark and that write rotates first (protocol §3.4,
        // §3.5). A delete names no cells: the runtime finds the space from
        // the deleted row.
        if write.table == crate::account::rows::SPACE_MEMBERS
            && matches!(write.kind, WriteKind::Delete)
        {
            let mut removals = self.shared.member_removals.borrow_mut();
            if removals.len() == crate::state::MAX_REPORTS {
                removals.pop_front();
            }
            removals.push_back((write.row, web_time::Instant::now()));
        }
        let Some(plan) = self.plan(write.table) else {
            return Ok(());
        };
        let refused = |message: &str| SealError::Refused(rejected(message));
        if self.shared.keys.borrow().closed {
            return Err(SealError::Refused(closed()));
        }
        let invalid_space = || refused("an encrypted row's space is not a row ID");
        let row_id = write.row.0.to_string();
        let (given, stored) = if plan.self_space() {
            (None, None)
        } else {
            (
                identifier(write.cells.get(&plan.entry.space)).map_err(|()| invalid_space())?,
                identifier(write.stored_space).map_err(|()| invalid_space())?,
            )
        };
        let target = match &write.kind {
            WriteKind::Move { target_space } => Some(
                identifier(Some(target_space))
                    .map_err(|()| invalid_space())?
                    .ok_or_else(|| refused("a move names the space it moves to"))?,
            ),
            _ => None,
        };
        if target.is_none()
            && let (Some(given), Some(stored)) = (&given, &stored)
            && given != stored
        {
            return Err(refused(
                "an encrypted row cannot change its space through an ordinary write",
            ));
        }
        // A write that creates its row seals every encrypted column it
        // leaves out: null for an optional column, the declared default
        // for a required one. The stored column is required bytes, and an
        // empty cell would read as never written. An upsert creates its
        // row when the write finds none: core gives no stored space then,
        // for a row that is its own space as for any other.
        let creates = match write.kind {
            WriteKind::Insert => true,
            WriteKind::Upsert => write.stored_space.is_none(),
            _ => false,
        };
        if creates {
            for (name, codec) in &plan.columns {
                if write.cells.contains_key(name) || write.streamed_column == Some(name) {
                    continue;
                }
                let filled = match &codec.default {
                    Some(default) => Some(default.clone()),
                    None if codec.nullable => Some(Value::Nullable(None)),
                    None => None,
                };
                if let Some(value) = filled {
                    write.cells.insert(name.clone(), value);
                }
            }
        }
        let encrypted: Vec<String> = write
            .cells
            .keys()
            .filter(|name| plan.columns.contains_key(*name))
            .cloned()
            .collect();
        if target.is_some() && encrypted.len() != plan.columns.len() {
            return Err(refused(
                "a move writes every encrypted column of the row again",
            ));
        }
        if encrypted.is_empty() && write.streamed_column.is_none() {
            return Ok(());
        }
        let identifier = match plan.self_space() {
            true => row_id.clone(),
            false => target
                .or(given)
                .or(stored)
                .ok_or_else(|| refused("an encrypted write names its space"))?,
        };
        let address = Self::address(&self.shared.keys.borrow(), plan, &identifier)
            .map_err(SealError::Refused)?;
        // An encrypted upload checked for rotation before consuming its source.
        if write.streamed_column.is_none() {
            self.check_rotation(&address)?;
        }
        let keys = self.shared.keys.borrow();
        let key = write_key(
            &keys,
            &self.shared.creations.borrow().dropped,
            &address,
            keys.creation_refused.contains(&address),
        )?;
        let catalogue = keys
            .catalogue
            .as_ref()
            .ok_or(SealError::Refused(not_observed()))?;
        let table_id = catalogue.table(write.table).map_err(SealError::Refused)?;
        let mut plaintexts = Vec::with_capacity(encrypted.len());
        let mut equalities: Vec<Option<EqualityValueBuf>> = Vec::with_capacity(encrypted.len());
        let mut columns = Vec::with_capacity(encrypted.len());
        for name in &encrypted {
            let codec = &plan.columns[name];
            let value = &write.cells[name];
            let wrong_type = || refused("an encrypted cell takes its column's plaintext type");
            plaintexts.push(Zeroizing::new(codec.encode(value).ok_or_else(wrong_type)?));
            equalities.push(match codec.equality {
                Some(_) => Some(codec.equality_value(value).ok_or_else(wrong_type)?),
                None => None,
            });
            columns.push(column(catalogue, write.table, name, codec).map_err(SealError::Refused)?);
        }
        let inputs: Vec<CellInput<'_>> = columns
            .iter()
            .zip(&plaintexts)
            .zip(&equalities)
            .map(|((column, value), equality)| CellInput {
                column: *column,
                value,
                equality: equality.as_ref().map(EqualityValueBuf::as_value),
            })
            .collect();
        let suite = self
            .shared
            .registry
            .select(&self.shared.selected)
            .map_err(|error| SealError::Refused(failed(ContentError::from(error))))?;
        let row = RowTarget {
            domain: &self.shared.content,
            space: &address,
            table_id,
            row_id: &row_id,
        };
        let sealed = seal_cells(
            &suite,
            &key,
            &row,
            &inputs,
            &mut **self.shared.random.borrow_mut(),
        )
        .map_err(|error| SealError::Refused(failed(error)))?;
        for (name, cell) in encrypted.iter().zip(sealed.cells) {
            write.cells.insert(name.clone(), Value::Bytes(cell.stored));
            if let Some(token) = cell.equality_token {
                write
                    .cells
                    .insert(format!("__e2ee_eq_{name}"), Value::Bytes(token));
            }
        }
        // The key is this device's own creation, not yet accepted: the
        // write stages its prepared root, once (protocol §3.2, §5.4).
        if matches!(key.mark(), KeyMark::Unpublished { .. }) {
            let mut creations = self.shared.creations.borrow_mut();
            if creations
                .prepared
                .get(&address)
                .is_some_and(|root| root.id == key.root_id())
            {
                let root = creations.prepared.remove(&address).expect("checked above");
                let row = uuid::Uuid::parse_str(&root.id)
                    .map_err(|_| refused("a space root's ID is not a UUID"))?;
                let cells = root
                    .cells()
                    .ok_or_else(|| refused("a space root's IDs are not UUIDs"))?;
                write
                    .companion_rows
                    .push((SPACE_ROOTS.to_owned(), RowUuid(row), cells));
                creations.staged.insert(root.id.clone());
                creations.attempt.push((address.clone(), root.id));
            }
        }
        Ok(())
    }

    /// A space root a write staged and the database dropped: its creation
    /// did not happen, and the runtime abandons it before the next write.
    fn companions_dropped(&self, rows: &[(String, RowUuid)]) {
        let mut creations = self.shared.creations.borrow_mut();
        for (table, row) in rows {
            let id = row.0.to_string();
            if table == SPACE_ROOTS && creations.staged.remove(&id) {
                creations.dropped.insert(id);
            }
        }
    }

    fn open_rows(
        &self,
        site: &ReadSite<'_>,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<OpenOutcome, Error> {
        let mut outcome = OpenOutcome::default();
        let Some(plan) = self.plan(site.table) else {
            return Ok(outcome);
        };
        if rows.is_empty() {
            return Ok(outcome);
        }
        let keys = self.shared.keys.borrow();
        if keys.closed {
            return Err(closed());
        }
        let catalogue = keys.catalogue.as_ref().ok_or_else(not_observed)?;
        let table_id = catalogue.table(site.table)?;
        let report = |outcome: &mut OpenOutcome, unreadable: Unreadable| {
            self.shared.report(Report::Unreadable(unreadable.clone()));
            outcome.unreadable.push(unreadable);
        };
        let mut kept = Vec::with_capacity(rows.len());
        for mut row in rows.drain(..) {
            let row_uuid: RowUuid = row.row_uuid();
            let row_id = row_uuid.0.to_string();
            // A read that selects no encrypted column has nothing to open
            // and needs no space.
            if !plan.columns.keys().any(|name| row.cell(name).is_some()) {
                kept.push(row);
                continue;
            }
            let identifier = match plan.self_space() {
                true => Some(row_id.clone()),
                false => identifier(row.cell(&plan.entry.space).as_ref())
                    .ok()
                    .flatten(),
            };
            let Some(address) = identifier.and_then(|id| Self::address(&keys, plan, &id).ok())
            else {
                report(
                    &mut outcome,
                    Unreadable {
                        table: site.table.to_owned(),
                        id: Some(row_uuid),
                        column: Some(plan.entry.space.clone()),
                        code: UnreadableCode::InvalidCiphertext,
                        space: None,
                    },
                );
                continue;
            };
            let target = RowTarget {
                domain: &self.shared.content,
                space: &address,
                table_id,
                row_id: &row_id,
            };
            let mut readable = true;
            for (name, codec) in &plan.columns {
                let Some(cell) = row.cell(name) else {
                    continue;
                };
                // Whether a migration supplied this cell, judged on the cell
                // as stored, before it is opened.
                let lens_supplied = lens_supplied(site, name, &cell);
                if codec.equality.is_some() {
                    row.remove_cell(&equality_column(name));
                }
                // A stored cell is bytes; a physical null reads like the
                // empty value, "never written"; anything else no writer
                // could have sealed.
                let stored = match cell {
                    Value::Bytes(bytes) => Ok(Some(bytes)),
                    Value::Nullable(None) => Ok(None),
                    Value::Nullable(Some(inner)) => match *inner {
                        Value::Bytes(bytes) => Ok(Some(bytes)),
                        _ => Err(()),
                    },
                    _ => Err(()),
                };
                let column = column(catalogue, site.table, name, codec)?;
                let opened = match &stored {
                    Err(()) => Opened::Unreadable(DataError::InvalidCiphertext),
                    Ok(None) => Opened::Empty,
                    Ok(Some(stored)) => open_cell(
                        &self.shared.registry,
                        &keys.held,
                        &target,
                        &column,
                        stored,
                        false,
                    )
                    .map_err(failed)?,
                };
                let stored = stored.ok().flatten();
                let streamed = stored.as_deref().is_some_and(|stored| {
                    read_stored_record(stored).is_ok_and(|record| record.kind == RecordKind::Stream)
                });
                let notice = |code: UnreadableCode, space: &SpaceAddress| Unreadable {
                    table: site.table.to_owned(),
                    id: Some(row_uuid),
                    column: Some(name.clone()),
                    code,
                    space: Some(db_address(space)),
                };
                let value = match opened {
                    Opened::Value(plaintext) => Some((plaintext, None)),
                    Opened::StaleSpace { value, space } => Some((value, Some(space))),
                    // Never written: the declared default, and a notice
                    // only when a writer stored the empty value (not a
                    // migration). The row is always returned.
                    Opened::Empty => match &codec.empty {
                        Some(empty) => {
                            row.set_cell(name, Some(empty.clone()), codec.value_type.clone());
                            if !lens_supplied && stored.is_some() {
                                report(&mut outcome, notice(UnreadableCode::EmptyCell, &address));
                            }
                            None
                        }
                        // A required column without a default has nothing a
                        // never written cell reads as: the row is withheld
                        // and the empty cell reported.
                        None => {
                            report(&mut outcome, notice(UnreadableCode::EmptyCell, &address));
                            readable = false;
                            None
                        }
                    },
                    Opened::Cleared => {
                        report(&mut outcome, notice(UnreadableCode::Superseded, &address));
                        readable = false;
                        None
                    }
                    Opened::Missing { epoch_id } => {
                        self.shared.report(Report::MissingKey {
                            table: site.table.to_owned(),
                            space: db_address(&address),
                            epoch_id: epoch_id.clone(),
                        });
                        self.shared.missing(Missing {
                            table: site.table.to_owned(),
                            id: row_uuid,
                            column: name.clone(),
                            space: db_address(&address),
                            epoch_id: epoch_id.clone(),
                        });
                        outcome
                            .missing_epochs
                            .push((db_address(&address), epoch_id));
                        // Not reported yet: the runtime acquires the epoch
                        // and settles the report. The read itself says the
                        // row was left out, pending its key.
                        outcome
                            .unreadable
                            .push(notice(UnreadableCode::KeyPending, &address));
                        readable = false;
                        None
                    }
                    Opened::Unreadable(error) => {
                        report(&mut outcome, notice(code(error), &address));
                        readable = false;
                        None
                    }
                };
                let Some((plaintext, stale)) = value else {
                    continue;
                };
                let decoded = match streamed {
                    true if codec.is_bytes => Some(Value::Bytes(plaintext.to_vec())),
                    true => None,
                    false => codec.decode(&plaintext),
                };
                let Some(decoded) = decoded else {
                    report(
                        &mut outcome,
                        notice(UnreadableCode::InvalidCiphertext, &address),
                    );
                    readable = false;
                    continue;
                };
                let decoded = match (codec.nullable, decoded) {
                    (true, Value::Nullable(inner)) => Value::Nullable(inner),
                    (true, value) => Value::Nullable(Some(Box::new(value))),
                    (false, value) => value,
                };
                row.set_cell(name, Some(decoded), codec.value_type.clone());
                // A cell that opened under the key of a refused or lost
                // creation is for the repair to rewrite (protocol §3.2).
                if let Some(stored) = stored.as_deref()
                    && under_refused_key(&keys, &address, stored)
                {
                    self.shared
                        .note_repairable(site.table, row_uuid, &db_address(&address), name);
                }
                if let Some(space) = stale {
                    report(&mut outcome, notice(UnreadableCode::StaleSpace, &space));
                }
            }
            if readable {
                kept.push(row);
            }
        }
        *rows = kept;
        Ok(outcome)
    }

    fn plan_query(&self, query: &Query) -> Result<QueryPlan, Error> {
        // Joins, reachability and inheritance compare no sealed bytes: core
        // refuses every condition of them that names an encrypted column, so
        // what reaches this hook leaves the query's own rows alone and is
        // planned like any other query of the table.
        let Some(plan) = self.plan(&query.table) else {
            return Ok(QueryPlan::Plain);
        };
        let table = query.table.as_str();
        if query.aggregate.is_some() {
            return Err(unsupported_query("an aggregate", table));
        }
        if let Some(select) = &query.select
            && select.iter().any(|name| plan.columns.contains_key(name))
            && !plan.self_space()
            && !select.contains(&plan.entry.space)
        {
            return Err(unsupported_query(
                "encrypted columns selected without the space column",
                table,
            ));
        }
        // Core refuses every encrypted condition but a top-level `eq` of an
        // equality column with a literal, a parameter or a claim before
        // this hook runs, and again after a plain plan. What is left here:
        // the literals, which this hook turns into tokens.
        let mut searched = Vec::new();
        for (index, filter) in query.filters.iter().enumerate() {
            let Predicate::Eq(Operand::Column(name), value) = filter else {
                continue;
            };
            if !plan.columns.contains_key(name) {
                continue;
            }
            let Operand::Literal(value) = value else {
                return Err(unsupported_query(
                    "an encrypted predicate other than eq with a value",
                    table,
                ));
            };
            searched.push((index, name.clone(), value.clone()));
        }
        if searched.is_empty() {
            return Ok(QueryPlan::Plain);
        }
        let space_column = if plan.self_space() {
            "id"
        } else {
            plan.entry.space.as_str()
        };
        let identifier = query.filters.iter().find_map(|filter| match filter {
            Predicate::Eq(Operand::Column(name), Operand::Literal(Value::Uuid(id)))
                if name == space_column =>
            {
                Some(id.to_string())
            }
            _ => None,
        });
        let Some(identifier) = identifier else {
            return Err(unsupported_query(
                "an encrypted predicate without eq on the space column",
                table,
            ));
        };
        let keys = self.shared.keys.borrow();
        if keys.closed {
            return Err(closed());
        }
        let address = Self::address(&keys, plan, &identifier)?;
        let catalogue = keys.catalogue.as_ref().ok_or_else(not_observed)?;
        let tagger = self
            .shared
            .registry
            .select(&self.shared.selected)
            .map_err(|error| failed(error.into()))?
            .equality;
        let target = TableTarget {
            domain: &self.shared.content,
            space: &address,
            table_id: catalogue.table(table)?,
        };
        let mut physical = query.clone();
        physical.limit = None;
        physical.offset = 0;
        let mut residual = Residual {
            preds: Vec::new(),
            limit: query.limit,
            offset: query.offset,
        };
        let mut epochs = Vec::new();
        let mut incomplete = false;
        let mut dropped = Vec::new();
        for (index, name, value) in searched {
            let codec = &plan.columns[&name];
            // Rows stored before a migration added the column read this
            // value but carry no token: the index would miss them silently.
            if catalogue.lens_added(table, &name) && codec.is_empty_value(&value) {
                return Err(unsupported_query(
                    &format!(
                        "eq on `{name}`, which a migration added, for the value its older \
                         rows read (its default)"
                    ),
                    table,
                ));
            }
            let equality = codec.equality_value(&value).ok_or_else(|| {
                Error::new(
                    ErrorCode::Query,
                    format!("eq on `{name}` compares a value of another type"),
                )
            })?;
            let column = column(catalogue, table, &name, codec)?;
            match plan_equality(
                tagger,
                &keys.held,
                &target,
                &column,
                &equality.as_value(),
                keys.online,
            )
            .map_err(failed)?
            {
                EqualityPlan::Tokens(tokens) => {
                    physical.filters[index] = Predicate::In(
                        Operand::Column(format!("__e2ee_eq_{name}")),
                        tokens
                            .tokens
                            .into_iter()
                            .map(|token| Operand::Literal(Value::Bytes(token)))
                            .collect(),
                    );
                    epochs.extend(tokens.epochs);
                    incomplete |= tokens.incomplete;
                }
                EqualityPlan::Offline => dropped.push(index),
                EqualityPlan::NeedsKey => {
                    return Ok(QueryPlan::NeedsKey {
                        space: db_address(&address),
                    });
                }
            }
            let mut any_of = vec![value.clone()];
            if codec.nullable {
                any_of.push(Value::Nullable(Some(Box::new(value))));
            }
            residual.preds.push(ResidualPred {
                column: name,
                any_of,
            });
        }
        for index in dropped.into_iter().rev() {
            physical.filters.remove(index);
        }
        epochs.sort();
        epochs.dedup();
        // An online plan has at least one epoch (none is `NeedsKey`) and an
        // offline one none, so the two never share a digest.
        let mut digest = Vec::new();
        for epoch in &epochs {
            digest.extend_from_slice(epoch.as_bytes());
            digest.push(b'\n');
        }
        Ok(QueryPlan::Rewritten {
            physical: Box::new(physical),
            residual,
            epochs: EpochSetId(sha256(&digest)),
            incomplete: incomplete.then(|| db_address(&address)),
        })
    }

    fn seal_stream(&self, target: &StreamTarget<'_>) -> Result<Box<dyn StreamSealer>, SealError> {
        let refused = |message: &str| SealError::Refused(rejected(message));
        let plan = self
            .plan(target.table)
            .ok_or_else(|| refused("a streamed value of a table that is not encrypted"))?;
        let codec = plan
            .columns
            .get(target.column)
            .ok_or_else(|| refused("a streamed value of a column that is not encrypted"))?;
        if self.shared.keys.borrow().closed {
            return Err(SealError::Refused(closed()));
        }
        let row_id = target.row.0.to_string();
        let identifier = match plan.self_space() {
            true => Some(row_id.clone()),
            false => identifier(target.space)
                .map_err(|()| refused("a streamed value's space is not a row ID"))?,
        }
        .ok_or_else(|| refused("a streamed value names its space"))?;
        let address = Self::address(&self.shared.keys.borrow(), plan, &identifier)
            .map_err(SealError::Refused)?;
        self.check_rotation(&address)?;
        let keys = self.shared.keys.borrow();
        let key = write_key(
            &keys,
            &self.shared.creations.borrow().dropped,
            &address,
            keys.creation_refused.contains(&address),
        )?;
        let catalogue = keys
            .catalogue
            .as_ref()
            .ok_or(SealError::Refused(not_observed()))?;
        let column =
            column(catalogue, target.table, target.column, codec).map_err(SealError::Refused)?;
        let suite = self
            .shared
            .registry
            .select(&self.shared.selected)
            .map_err(|error| SealError::Refused(failed(error.into())))?;
        let row = RowTarget {
            domain: &self.shared.content,
            space: &address,
            table_id: catalogue.table(target.table).map_err(SealError::Refused)?,
            row_id: &row_id,
        };
        let (writer, prefix) = begin_stream(
            &suite,
            &key,
            &row,
            &column,
            &mut **self.shared.random.borrow_mut(),
        )
        .map_err(|error| match error {
            ContentError::NotStreamable => refused("only unindexed bytes columns stream"),
            error => SealError::Refused(failed(error)),
        })?;
        Ok(Box::new(Sealer {
            writer,
            prefix: Some(prefix),
        }))
    }
}

/// The plans of every declared table. Conversions and re-homes are not
/// supported by this runtime yet and are refused here, at open.
pub(crate) fn plans(
    declarations: &E2eeDeclarations,
) -> Result<BTreeMap<String, TablePlan>, crate::error::ClientError> {
    use crate::error::ClientError;
    let mut plans = BTreeMap::new();
    for (table, entry) in &declarations.tables {
        let conversion = entry.rehome.is_some()
            || !entry.backfill.is_empty()
            || entry.columns.values().any(|column| {
                column.from.is_some()
                    || column.to.is_some()
                    || column.convert.is_some()
                    || column.reverse.is_some()
            });
        if conversion {
            return Err(ClientError::Unsupported(format!(
                "table `{table}`: encrypted schema conversions are not supported yet"
            )));
        }
        if let Some(name) = entry
            .equality
            .iter()
            .find(|name| !entry.columns.contains_key(*name))
        {
            return Err(ClientError::Settings(format!(
                "table `{table}`: the equality index `{name}` is not an encrypted column"
            )));
        }
        let mut columns = BTreeMap::new();
        for (name, column) in &entry.columns {
            columns.insert(
                name.clone(),
                ColumnCodec::new(
                    name,
                    &column.plain,
                    column.cell_type(),
                    entry.equality.contains(name),
                )?,
            );
        }
        plans.insert(
            table.clone(),
            TablePlan {
                entry: entry.clone(),
                columns,
            },
        );
    }
    Ok(plans)
}

//! The client's end-to-end encryption hooks (E2EE protocol §4–§5, core
//! hooks note §1–§2).
//!
//! The database stays free of cryptography. It defines [`E2eeHooks`], a
//! synchronous trait, and the plain data that crosses it, and calls the
//! trait where a value becomes stored or leaves for a reader:
//!
//! - writes: before large values are chunked, so the same sealed cells are
//!   stored and uploaded ([`E2eeHooks::seal_write`]);
//! - reads: on each materialised result before it reaches a reader
//!   ([`E2eeHooks::open_rows`]);
//! - queries: when a query is prepared, so the stored and uploaded query
//!   compares equality tokens, never plaintext ([`E2eeHooks::plan_query`]);
//! - streamed large values, as they stream ([`E2eeHooks::seal_stream`]).
//!   A whole read hands a stored stream to [`E2eeHooks::open_rows`] like
//!   any other cell.
//!
//! The hooks never await and never call the database: whatever they lack
//! (a key, a prepared space root) they report, the caller awaits the
//! client runtime that owns the hooks, and then repeats the one call. A
//! refusal is a failed call: nothing of it was applied. Its error has the
//! reason `key-pending` or `space-pending` and names the space
//! ([`E2eeRefusal::of`]).
//!
//! Only a client opens a database with hooks
//! ([`DbConfig::with_e2ee`](crate::options::DbConfig::with_e2ee));
//! [`Db::set_authority`] refuses a database that has them.
//!
//! [`Db::set_authority`]: crate::db::Db::set_authority

use std::collections::BTreeMap;

use jazz_model::model::public_api::types::EQUALITY_PREFIX;

use crate::db::Shape;
use crate::ids::RowUuid;
use crate::options::{Error, ErrorCode, Reason};
use crate::query::{Predicate, Query};
use crate::records::{Value, ValueType};
use crate::values::CurrentRow;

#[cfg(feature = "e2ee-client")]
pub use crate::db::e2ee_ledger::{LedgerRead, Provisional, ProvisionalRead, TransactionFate};
pub use jazz_model::model::public_api::types::{E2eeDeclarations, EncryptionEntry};
#[cfg(feature = "e2ee-client")]
pub use jazz_model::model::public_api::types::{EncryptedColumn, EncryptionSettings, PlainType};

pub use crate::options::SpaceAddress;

/// A space epoch's ID: the 36 ASCII characters of its UUID.
pub type EpochId = String;

/// The encryption hooks of one client database. They live on the
/// database's thread and are called while it is busy: they must never
/// call the database, and never block on anything but memory.
pub trait E2eeHooks {
    /// The app's encryption declarations for the schema version this
    /// database runs.
    fn declarations(&self) -> &E2eeDeclarations;

    /// Whether these hooks were built from the declarations of an encrypted
    /// app (the ones `load_client_encrypted` returns when the schema holds
    /// Jazz's system tables), which are empty for a version that keeps the
    /// tables but declares no encrypted column. Opening an encrypted app
    /// needs it. The default: the declarations name an encrypted table.
    /// A runtime whose declarations may be empty returns `true` for those
    /// of an encrypted app.
    fn built_from_encrypted_app(&self) -> bool {
        !self.declarations().tables.is_empty()
    }

    /// Whether a keyless relay accepts this top-level filter on a reserved
    /// equality-token column from a keyed tab. Application queries cannot
    /// name reserved columns, so client and backend hooks keep the default.
    fn relays_token_filter(&self, _table: &str, _filter: &Predicate) -> bool {
        false
    }

    /// Seal one write before it is stored or uploaded: replace each
    /// encrypted column's plaintext with its cell record, add equality
    /// tokens, and stage any internal rows the write needs (a space root)
    /// as companion rows. Called once per write; a write with no encrypted
    /// column returns `Ok` untouched.
    fn seal_write(&self, write: &mut WriteCells<'_>) -> Result<(), SealError>;

    /// Claim the companion rows prepared with a stream that is now publishing.
    /// Another write may have published the space root while the source ran.
    fn claim_stream_companions(
        &self,
        _table: &str,
        _space: Option<&Value>,
        _root: Option<&str>,
        _rows: &mut Vec<(String, RowUuid, BTreeMap<String, Value>)>,
    ) -> Result<(), SealError> {
        Ok(())
    }

    /// The companion rows `seal_write` staged, as (table, row ID), that
    /// will not be stored: the write failed to stage after sealing, or the
    /// transaction that held them was abandoned or failed to commit. A
    /// row staged again later (a retried write) is a new staging. The
    /// default does nothing.
    fn companions_dropped(&self, rows: &[(String, RowUuid)]) {
        let _ = rows;
    }

    /// Open the rows of one read in place: decrypt each encrypted cell,
    /// remove the equality token cells, and, outside strict reads, remove
    /// the rows it cannot open and report them. In a strict read the first
    /// row it cannot open is an error.
    fn open_rows(
        &self,
        site: &ReadSite<'_>,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<OpenOutcome, Error>;

    /// Plan a query: rewrite equality filters on encrypted columns to
    /// token filters, or refuse what cannot be answered without a scan.
    fn plan_query(&self, query: &Query) -> Result<QueryPlan, Error>;

    /// Report that a rewritten query may miss rows of epochs it could not search.
    fn report_incomplete_query(&self, _table: &str, _space: &SpaceAddress) {}

    /// Begin sealing a streamed large value into `target`. Called before
    /// any source byte is read.
    fn seal_stream(&self, target: &StreamTarget<'_>) -> Result<Box<dyn StreamSealer>, SealError>;
}

/// What kind of write [`E2eeHooks::seal_write`] seals.
#[derive(Clone, Debug, PartialEq)]
pub enum WriteKind {
    Insert,
    Update,
    Upsert,
    Delete,
    /// A restore, with replacement content or without.
    Restore,
}

/// One write, as [`E2eeHooks::seal_write`] sees it.
pub struct WriteCells<'a> {
    pub table: &'a str,
    pub row: RowUuid,
    pub kind: WriteKind,
    /// The cells the write names, by column name: plaintext in, sealed
    /// out.
    pub cells: &'a mut BTreeMap<String, Value>,
    /// Sealed defaults and their tokens that apply only if this upsert creates the row.
    pub insert_only: &'a mut BTreeMap<String, Value>,
    /// The column supplied by a stream, omitted while its metadata is sealed.
    pub streamed_column: Option<&'a str>,
    /// The root the upload uses. Publication checks it and claims a pending creation.
    pub stream_root: Option<String>,
    /// For updates and upserts of a table with an encryption
    /// entry: the space cell of the row as the write finds it, `None` when
    /// there is no such row (yet).
    pub stored_space: Option<&'a Value>,
    /// Internal rows the hook stages into the write's own transaction:
    /// (table, row ID, cells). They are inserted before the write, as the
    /// same identity, and commit or fail with it.
    pub companion_rows: &'a mut Vec<(String, RowUuid, BTreeMap<String, Value>)>,
}

/// Why [`E2eeHooks::seal_write`] or [`E2eeHooks::seal_stream`] did not seal.
#[derive(Clone, Debug)]
pub enum SealError {
    /// The write is waiting for a key or a valid space root.
    Pending(E2eeRefusal),
    /// Any other refusal.
    Refused(Error),
}

/// What a call refused for want of a key lacked: the error's reason and the
/// space it names ([`E2eeRefusal::of`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum E2eeRefusal {
    /// A key for `space` must be acquired before the call is repeated.
    NeedsKey { space: SpaceAddress },
    /// The space has no valid root and this device may not create it.
    SpacePending { space: SpaceAddress },
}

impl E2eeRefusal {
    /// The error the refused call fails with: reason `key-pending` (code
    /// `NotObserved`) or `space-pending` (code `WriteRejected`), carrying
    /// the space in [`Error::space`].
    pub(crate) fn error(&self) -> Error {
        let (code, reason, space, why) = match self {
            E2eeRefusal::NeedsKey { space } => (
                ErrorCode::NotObserved,
                Reason::KeyPending,
                space,
                "has no held key",
            ),
            E2eeRefusal::SpacePending { space } => (
                ErrorCode::WriteRejected,
                Reason::SpacePending,
                space,
                "has no valid root",
            ),
        };
        Error {
            space: Some(space.clone()),
            ..Error::with_reason(
                code,
                reason,
                format!(
                    "{}: space {}/{} {why}",
                    reason.as_str(),
                    space.scope_id,
                    space.identifier
                ),
            )
        }
    }

    /// What `error` lacked, when the encryption hooks refused its call for
    /// want of a key: its reason, and its space.
    pub fn of(error: &Error) -> Option<Self> {
        let space = error.space.clone()?;
        match error.reason? {
            Reason::KeyPending => Some(E2eeRefusal::NeedsKey { space }),
            Reason::SpacePending => Some(E2eeRefusal::SpacePending { space }),
            _ => None,
        }
    }
}

/// Where rows are read, as [`E2eeHooks::open_rows`] sees it.
pub struct ReadSite<'a> {
    /// A transaction's read, a mutation's pre-image or the row a binding
    /// reads before writing (protocol §4.4): any row the hook would leave
    /// out fails the read.
    pub strict: bool,
    /// The table the rows are of.
    pub table: &'a str,
    /// The query read, as executed (after [`E2eeHooks::plan_query`]).
    pub query: &'a Query,
    /// The prepared query's shape: one subscription's reads share it.
    pub shape: Shape,
    /// Columns whose changes the read reports as splices.
    pub splices: &'a [String],
    /// Columns a schema migration added to rows stored before them, with
    /// the value such a row reads ([`ReadSite::is_lens_default`]).
    pub lens_defaults: &'a BTreeMap<String, Value>,
}

impl ReadSite<'_> {
    /// Whether `row`'s `column` holds the value a migration gave the rows
    /// stored before the column existed, rather than one a writer stored.
    /// The store cannot tell such a row from one a writer gave that same
    /// value, so this holds for both.
    pub fn is_lens_default(&self, row: &CurrentRow, column: &str) -> bool {
        self.lens_defaults
            .get(column)
            .is_some_and(|default| row.cell(column).as_ref() == Some(default))
    }
}

/// What [`E2eeHooks::open_rows`] reports about one read.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OpenOutcome {
    /// Rows left out, and rows returned with a notice.
    pub unreadable: Vec<Unreadable>,
    /// Epochs a row needs that this device does not hold, reported
    /// provisionally: the runtime acquires them and settles the reports.
    pub missing_epochs: Vec<(SpaceAddress, EpochId)>,
    /// Epoch of each missing-key entry, keyed by its index in `unreadable`.
    /// A subscription keeps this for updating its reason after settlement.
    pub missing_reports: BTreeMap<usize, EpochId>,
}

impl OpenOutcome {
    /// Whether this outcome left a row out (or would have): what fails a
    /// strict read.
    pub fn leaves_rows_out(&self) -> bool {
        !self.missing_epochs.is_empty()
            || self
                .unreadable
                .iter()
                .any(|report| !report.code.returns_row())
    }
}

/// One report of protocol §4.3.
#[derive(Clone, Debug, PartialEq)]
pub struct Unreadable {
    pub table: String,
    /// `None` for a per-space report.
    pub id: Option<RowUuid>,
    /// `None` for a per-space report.
    pub column: Option<String>,
    pub code: UnreadableCode,
    pub space: Option<SpaceAddress>,
}

/// The codes of protocol §4.3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UnreadableCode {
    InvalidCiphertext,
    UnsupportedFormat,
    KeyUnavailable,
    KeyPending,
    SpacePending,
    EmptyCell,
    StaleSpace,
}

impl UnreadableCode {
    /// The protocol's spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            UnreadableCode::InvalidCiphertext => "invalid-ciphertext",
            UnreadableCode::UnsupportedFormat => "unsupported-format",
            UnreadableCode::KeyUnavailable => "key-unavailable",
            UnreadableCode::KeyPending => "key-pending",
            UnreadableCode::SpacePending => "space-pending",
            UnreadableCode::EmptyCell => "empty-cell",
            UnreadableCode::StaleSpace => "stale-space",
        }
    }

    /// Whether a row reported with this code is still returned
    /// (protocol §4.3, §4.4).
    pub const fn returns_row(self) -> bool {
        matches!(self, UnreadableCode::EmptyCell | UnreadableCode::StaleSpace)
    }
}

/// How [`E2eeHooks::plan_query`] answers a query.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryPlan {
    /// The query reads no encrypted column: run it as it is.
    Plain,
    /// Run `physical` (token filters, no window), then, on the opened
    /// rows, `residual`.
    Rewritten {
        physical: Box<Query>,
        residual: Residual,
        /// The held epochs the tokens were computed for; part of the
        /// prepared query's shape.
        epochs: EpochSetId,
        /// A space whose known chain has epochs this device does not
        /// hold: the result may miss rows of them (protocol §4.5).
        incomplete: Option<SpaceAddress>,
    },
    /// An encrypted filter on a space for which no key is held: the caller
    /// acquires one and plans again.
    NeedsKey { space: SpaceAddress },
}

/// What a rewritten query applies after its rows are opened: its filters on
/// plaintext, then its window.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Residual {
    /// Every predicate must hold.
    pub preds: Vec<ResidualPred>,
    pub limit: Option<usize>,
    pub offset: usize,
}

/// `column` (opened) equals one of `any_of`. The values are plaintext:
/// `Debug` shows only how many there are.
#[derive(Clone, PartialEq)]
pub struct ResidualPred {
    pub column: String,
    pub any_of: Vec<Value>,
}

impl std::fmt::Debug for ResidualPred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResidualPred")
            .field("column", &self.column)
            .field("any_of", &format_args!("<{} values>", self.any_of.len()))
            .finish()
    }
}

/// The identity of the set of held epochs a plan used.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct EpochSetId(pub [u8; 32]);

impl Residual {
    /// Keep the rows every predicate admits, then apply the window. A
    /// predicate on a column the rows do not carry is an error, never an
    /// empty result.
    pub(crate) fn apply(&self, rows: &mut Vec<CurrentRow>) -> Result<(), Error> {
        if !self.preds.is_empty() {
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows.drain(..) {
                let mut admitted = true;
                for pred in &self.preds {
                    let Some(index) = row.cells().position(|(name, _)| name == pred.column) else {
                        return Err(Error::new(
                            ErrorCode::Query,
                            format!(
                                "an encrypted filter on `{}` needs the column in the result",
                                pred.column
                            ),
                        ));
                    };
                    // A null cell equals nothing.
                    if !row
                        .cell_at(index)
                        .is_some_and(|value| pred.any_of.contains(&value))
                    {
                        admitted = false;
                        break;
                    }
                }
                if admitted {
                    kept.push(row);
                }
            }
            *rows = kept;
        }
        if self.offset > 0 {
            rows.drain(..self.offset.min(rows.len()));
        }
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }
        Ok(())
    }
}

/// The large value a stream seals or opens.
pub struct StreamTarget<'a> {
    pub table: &'a str,
    pub row: RowUuid,
    pub column: &'a str,
    /// The row's space cell: the write's own, or the stored row's.
    pub space: Option<&'a Value>,
}

/// Seals one streamed value. The first output includes the routing record
/// and stream header before any sealed bytes.
pub trait StreamSealer {
    /// Seal the next source bytes.
    fn push(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error>;
    /// Emit the final, tagged record. Includes the header if nothing was pushed.
    fn finish(self: Box<Self>) -> Result<Vec<u8>, Error>;
}

/// Opened cells: what [`E2eeHooks::open_rows`] uses to rewrite a row.
impl CurrentRow {
    /// Set `column` to `value`, typed `ty`; a column the row does not
    /// carry is appended.
    pub fn set_cell(&mut self, column: &str, value: Option<Value>, ty: ValueType) {
        let at = self.cells.iter().position(|(name, _)| name == column);
        let cells = std::sync::Arc::make_mut(&mut self.cells);
        let mut types = self.types.to_vec();
        match at {
            Some(at) => {
                cells[at].1 = value;
                if let Some(slot) = types.get_mut(at) {
                    *slot = ty;
                }
            }
            None => {
                cells.push((column.to_owned(), value));
                types.push(ty);
            }
        }
        self.types = types.into();
    }

    /// Remove `column` from the row, if it carries it.
    pub fn remove_cell(&mut self, column: &str) {
        let Some(at) = self.cells.iter().position(|(name, _)| name == column) else {
            return;
        };
        std::sync::Arc::make_mut(&mut self.cells).remove(at);
        let mut types = self.types.to_vec();
        if at < types.len() {
            types.remove(at);
        }
        self.types = types.into();
    }
}

/// Encrypted cells and their tokens may default only to the empty byte string.
pub(crate) fn refuse_stored_defaults(
    schema: &crate::schema::JazzSchema,
    declarations: &E2eeDeclarations,
) -> Result<(), Error> {
    for table in schema.tables() {
        let Some(entry) = declarations.tables.get(table.name.as_str()) else {
            continue;
        };
        for column in &table.columns {
            let name = column.name.as_str();
            let token = name
                .strip_prefix(EQUALITY_PREFIX)
                .is_some_and(|source| entry.equality.iter().any(|c| c == source));
            if (entry.columns.contains_key(name) || token)
                && column
                    .default
                    .as_ref()
                    .is_some_and(|v| !matches!(v, Value::Bytes(bytes) if bytes.is_empty()))
            {
                return Err(Error::new(
                    ErrorCode::Schema,
                    format!(
                        "encrypted column `{}.{}` has a stored default, which would be \
                         stored as plaintext where its ciphertext belongs",
                        table.name, column.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

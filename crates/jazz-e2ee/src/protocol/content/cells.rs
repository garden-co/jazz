//! Sealing and opening stored encrypted values (`E2EE_PROTOCOL.md` §4.1,
//! §4.2, §5.1, §5.2, §5.5), over the cell and stream
//! record formats of [`crate::records`].
//!
//! A write seals every supplied encrypted cell, and its equality token,
//! under one [`WriteKey`]: a held key is enough, nothing else is checked
//! (§5.1). A read opens a stored value with the held key of the epoch it
//! names, of any mark (only a key of the space's own root decides that a
//! value is invalid), and classifies everything it cannot return: the two
//! short values with fixed meanings, malformed and unauthentic records
//! (`invalid-ciphertext`), unrouted mechanisms (`unsupported-format`), and
//! versions the device does not hold ([`Opened::Missing`], which the
//! runtime acquires and settles with
//! [`HeldKeys::settle_missing`](super::held::HeldKeys::settle_missing)).
//! A cell sealed under another space this device holds is returned and
//! reported `stale-space`. A provider failure is always an error, never an
//! unreadable cell, and no other candidate key is tried after it.

use zeroize::{Zeroize, Zeroizing};

use crate::agility::{StreamReader, StreamWriter};
use crate::envelope;
use crate::error::Error;
use crate::random::RandomSource;
use crate::records::{
    self, CELL_RECORD, EqualityValue, RecordKind, STREAM_RECORD, read_stored_record,
};
use crate::registry::{Registry, Suite};

use super::super::error::ProtocolError;
use super::super::records::space::SpaceAddress;
use super::held::{HeldKeys, HeldSpace, WriteKey};
use super::{ContentError, ContentResult, DataError, EncryptedColumn, RowTarget};

const EPOCH_BYTES: usize = 36;
/// The smallest cell record payload: the epoch and the shortest envelope.
const MIN_RECORD_PAYLOAD: usize = EPOCH_BYTES + 11;

/// The length of the shortest stored cell record (header plus 47 payload
/// bytes). Any shorter non-empty stored value is `invalid-ciphertext` (§4.1).
pub fn min_cell_record_len() -> usize {
    CELL_RECORD
        .header()
        .expect("the cell record mechanism is valid")
        .len()
        + MIN_RECORD_PAYLOAD
}

/// A logical value as an equality index encodes it, owned, for values the
/// caller decodes from plaintext ([`PlaintextValues`]). Cleared on drop and
/// never printed.
#[derive(Clone, PartialEq)]
pub enum EqualityValueBuf {
    Null,
    Bool(bool),
    Int(i64),
    Timestamp(u64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Uuid([u8; 16]),
    Enum(String),
    /// A JSON value as stored: the text as written.
    Json(String),
    /// An array's elements, in order.
    Array(Vec<EqualityValueBuf>),
}

impl core::fmt::Debug for EqualityValueBuf {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Null => "Null",
            Self::Bool(_) => "Bool(..)",
            Self::Int(_) => "Int(..)",
            Self::Timestamp(_) => "Timestamp(..)",
            Self::Float(_) => "Float(..)",
            Self::Text(_) => "Text(..)",
            Self::Bytes(_) => "Bytes(..)",
            Self::Uuid(_) => "Uuid(..)",
            Self::Enum(_) => "Enum(..)",
            Self::Json(_) => "Json(..)",
            Self::Array(_) => "Array(..)",
        })
    }
}

impl Drop for EqualityValueBuf {
    fn drop(&mut self) {
        match self {
            Self::Null => {}
            Self::Bool(value) => value.zeroize(),
            Self::Int(value) => value.zeroize(),
            Self::Timestamp(value) => value.zeroize(),
            Self::Float(value) => value.zeroize(),
            Self::Text(value) | Self::Enum(value) | Self::Json(value) => value.zeroize(),
            // Each element clears itself as it drops.
            Self::Array(_) => {}
            Self::Bytes(value) => value.zeroize(),
            Self::Uuid(value) => value.zeroize(),
        }
    }
}

impl EqualityValueBuf {
    pub fn as_value(&self) -> EqualityValue<'_> {
        match self {
            Self::Null => EqualityValue::Null,
            Self::Bool(value) => EqualityValue::Bool(*value),
            Self::Int(value) => EqualityValue::Int(*value),
            Self::Timestamp(value) => EqualityValue::Timestamp(*value),
            Self::Float(value) => EqualityValue::Float(*value),
            Self::Text(value) => EqualityValue::Text(value),
            Self::Bytes(value) => EqualityValue::Bytes(value),
            Self::Uuid(value) => EqualityValue::Uuid(*value),
            Self::Enum(value) => EqualityValue::Enum(value),
            Self::Json(value) => EqualityValue::Json(value),
            Self::Array(items) => EqualityValue::Array(crate::records::EqualityItems::Bufs(items)),
        }
    }
}

/// Decodes Jazz's native encoding of one value into the logical value its
/// equality token hashes. Implemented by the runtime, which knows the
/// encoding. Lost-creation repair uses it to tag the plaintext it decrypts.
pub trait PlaintextValues {
    fn equality_value(
        &self,
        column: &EncryptedColumn<'_>,
        plaintext: &[u8],
    ) -> ContentResult<EqualityValueBuf>;
}

/// The equality value a decrypted `plaintext` is tagged with, or `None`
/// for a column without an index. A plaintext that does not decode to a
/// value of the column's index is a bad value some writer stored:
/// `invalid-ciphertext` for that cell alone, so repair skips and reports
/// it and carries on with the rest. A provider failure, or inconsistent
/// evidence, stays an error.
pub(crate) fn decrypted_equality_value(
    values: &dyn PlaintextValues,
    column: &EncryptedColumn<'_>,
    plaintext: &[u8],
) -> ContentResult<Result<Option<EqualityValueBuf>, DataError>> {
    let Some(index) = column.equality else {
        return Ok(Ok(None));
    };
    let value = match values.equality_value(column, plaintext) {
        Ok(value) => value,
        Err(error)
            if error.is_provider_failure()
                || error == ContentError::Protocol(ProtocolError::InconsistentEvidence) =>
        {
            return Err(error);
        }
        // A value this reader cannot decode for want of a capability is
        // unsupported, never invalid.
        Err(ContentError::Data(DataError::UnsupportedFormat))
        | Err(ContentError::Protocol(ProtocolError::Unsupported(_))) => {
            return Ok(Err(DataError::UnsupportedFormat));
        }
        Err(_) => return Ok(Err(DataError::InvalidCiphertext)),
    };
    if index.encode(&value.as_value()).is_err() {
        return Ok(Err(DataError::InvalidCiphertext));
    }
    Ok(Ok(Some(value)))
}

/// One encrypted cell of a write: its column, Jazz's native encoding of
/// the value, and, for an equality-indexed column, the logical value its
/// token hashes. The value and the equality value are never printed.
#[derive(Clone, Copy)]
pub struct CellInput<'a> {
    pub column: EncryptedColumn<'a>,
    pub value: &'a [u8],
    pub equality: Option<EqualityValue<'a>>,
}

impl core::fmt::Debug for CellInput<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CellInput")
            .field("column", &self.column)
            .field("value", &"..")
            .field("equality", &self.equality.map(|_| ".."))
            .finish()
    }
}

/// A sealed cell: the stored cell record and, for an equality-indexed
/// column, the token for `__e2ee_eq_<column>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedCell {
    pub column_id: String,
    pub stored: Vec<u8>,
    pub equality_token: Option<Vec<u8>>,
}

/// A write's sealed cells, all under one version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedCells {
    pub epoch_id: String,
    pub cells: Vec<SealedCell>,
}

/// Seals one cell under `key` and computes its token with the same key and
/// version (§5.2).
pub(crate) fn seal_one(
    suite: &Suite<'_>,
    key: &[u8; 32],
    epoch_id: &str,
    row: &RowTarget<'_>,
    cell: &CellInput<'_>,
    rng: &mut dyn RandomSource,
) -> ContentResult<SealedCell> {
    let column = &cell.column;
    let stored = records::encrypt_cell_value(
        suite.cell,
        key,
        &row.cell(column.column_id, epoch_id),
        column.cell_type,
        cell.value,
        rng,
    )?;
    let equality_token = match (&column.equality, &cell.equality) {
        (None, None) => None,
        (Some(index), Some(value)) => Some(records::equality_token_of(
            suite.equality,
            key,
            &row.equality(column.column_id, epoch_id),
            column.cell_type,
            index,
            value,
        )?),
        // An indexed column needs its value; a column without an index
        // takes none.
        _ => return Err(Error::InvalidInput.into()),
    };
    Ok(SealedCell {
        column_id: column.column_id.to_owned(),
        stored,
        equality_token,
    })
}

/// Seals the encrypted cells of one write under `key` (§5.1, §5.2, H2),
/// with each equality-indexed column's token under the same key and
/// version. A key of another space than the row's is
/// [`ProtocolError::InconsistentEvidence`]; a column given twice, or an
/// equality value missing for an indexed column or given for another, is
/// an invalid input. Any failure fails the whole write
/// (`encryption-failed`); plaintext is never returned.
pub fn seal_cells(
    suite: &Suite<'_>,
    key: &WriteKey<'_>,
    row: &RowTarget<'_>,
    cells: &[CellInput<'_>],
    rng: &mut dyn RandomSource,
) -> ContentResult<SealedCells> {
    if key.address() != row.space {
        return Err(ProtocolError::InconsistentEvidence.into());
    }
    let mut seen = std::collections::BTreeSet::new();
    if !cells.iter().all(|cell| seen.insert(cell.column.column_id)) {
        return Err(Error::InvalidInput.into());
    }
    let epoch_id = key.epoch_id();
    let sealed = cells
        .iter()
        .map(|cell| seal_one(suite, key.key(), epoch_id, row, cell, rng))
        .collect::<ContentResult<_>>()?;
    Ok(SealedCells {
        epoch_id: epoch_id.to_owned(),
        cells: sealed,
    })
}

/// What opening one stored value gives (§4.1–§4.3; E2EE_SCHEMA_CHANGES.md §3.2).
/// A decrypted value is never printed.
#[derive(Clone, PartialEq, Eq)]
pub enum Opened {
    /// Jazz's native encoding of the value, cleared on drop.
    Value(Zeroizing<Vec<u8>>),
    /// A value sealed under another space this device holds keys of: the
    /// row is returned and reported `stale-space`.
    StaleSpace {
        value: Zeroizing<Vec<u8>>,
        space: SpaceAddress,
    },
    /// The empty byte string, "never written": the column's declared
    /// default (or null for an optional column). Core reports `empty-cell`
    /// when the value is stored in the row rather than supplied by a lens.
    Empty,
    /// A version of the row's space this device does not hold: the runtime
    /// acquires it, at most once per read, and settles the report.
    Missing { epoch_id: String },
    /// Unreadable: `invalid-ciphertext`, `unsupported-format`.
    Unreadable(DataError),
}

impl core::fmt::Debug for Opened {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Value(_) => f.write_str("Value(..)"),
            Self::StaleSpace { space, .. } => f
                .debug_struct("StaleSpace")
                .field("value", &"..")
                .field("space", space)
                .finish(),
            Self::Empty => f.write_str("Empty"),
            Self::Missing { epoch_id } => f
                .debug_struct("Missing")
                .field("epoch_id", epoch_id)
                .finish(),
            Self::Unreadable(code) => f.debug_tuple("Unreadable").field(code).finish(),
        }
    }
}

impl Opened {
    /// The data error this outcome reports, if it is decided. A lens-
    /// supplied empty value is not reported; core tells the two apart.
    pub fn report(&self) -> Option<DataError> {
        match self {
            Self::StaleSpace { .. } => Some(DataError::StaleSpace),
            Self::Empty => Some(DataError::EmptyCell),
            Self::Unreadable(code) => Some(*code),
            Self::Value(_) | Self::Missing { .. } => None,
        }
    }

    /// Whether an ordinary read leaves the row out (§4.3).
    pub fn leaves_row_out(&self) -> bool {
        matches!(self, Self::Missing { .. } | Self::Unreadable(_))
    }

    /// A strict read (§4.4): a cell that leaves its row out fails the read.
    /// [`Opened::Missing`] stays, for the runtime to acquire and settle;
    /// a settled report fails the strict read too.
    pub fn strict(self) -> ContentResult<Self> {
        match self {
            Self::Unreadable(code) => Err(ContentError::Data(code)),
            other => Ok(other),
        }
    }
}

/// Maps a decryption error: authentication failures and unrouted
/// mechanisms are unreadable cells; anything else (a provider failure, an
/// invalid identity) is an error.
fn unreadable(error: Error) -> ContentResult<DataError> {
    match error {
        Error::InvalidCiphertext => Ok(DataError::InvalidCiphertext),
        Error::UnsupportedFormat => Ok(DataError::UnsupportedFormat),
        other => Err(other.into()),
    }
}

/// Decrypts a whole stored value of `kind` under `key` for `row`.
pub(crate) fn decrypt_with(
    registry: &Registry,
    key: &[u8; 32],
    row: &RowTarget<'_>,
    column: &EncryptedColumn<'_>,
    kind: RecordKind,
    epoch_id: &str,
    stored: &[u8],
) -> ContentResult<Result<Zeroizing<Vec<u8>>, DataError>> {
    let identity = row.cell(column.column_id, epoch_id);
    let result = match kind {
        RecordKind::Cell => {
            records::decrypt_cell_value(registry, key, &identity, column.cell_type, stored)
        }
        RecordKind::Stream => {
            records::decrypt_stream_value(registry, key, &identity, column.cell_type, stored)
        }
    };
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(error) => Ok(Err(unreadable(error)?)),
    }
}

/// The value and the index of the candidate key that opened it, or why
/// none did.
type Attempt = Result<(Zeroizing<Vec<u8>>, usize), DataError>;

/// Tries each candidate key in turn: the first that authenticates wins; an
/// unrouted mechanism is the same for every key and ends the search.
/// `None` when there is no candidate.
fn try_keys(
    registry: &Registry,
    candidates: &[(&HeldSpace, &[u8; 32])],
    row: &RowTarget<'_>,
    column: &EncryptedColumn<'_>,
    kind: RecordKind,
    epoch_id: &str,
    stored: &[u8],
) -> ContentResult<Option<Attempt>> {
    let mut outcome = None;
    for (index, (space, key)) in candidates.iter().enumerate() {
        let target = row.in_space(space.address());
        match decrypt_with(registry, key, &target, column, kind, epoch_id, stored)? {
            Ok(value) => return Ok(Some(Ok((value, index)))),
            Err(DataError::UnsupportedFormat) => {
                return Ok(Some(Err(DataError::UnsupportedFormat)));
            }
            Err(code) => outcome = Some(Err(code)),
        }
    }
    Ok(outcome)
}

/// Classifies a stored value before any key is used: `None` for a
/// record to decrypt.
pub(crate) fn classify_short(stored: &[u8]) -> Option<Opened> {
    if stored.is_empty() {
        return Some(Opened::Empty);
    }
    if stored.len() < min_cell_record_len() {
        return Some(Opened::Unreadable(DataError::InvalidCiphertext));
    }
    None
}

/// Opens a stored value of `column` in `row` with the held keys (§4.1,
/// §4.2): any held key of the epoch the record names decrypts, of any
/// mark, with no access check.
///
/// A stream record in a column that may not stream is
/// `unsupported-format` (large-value format). Only a key of the row's
/// space's own root decides that a value is invalid; a key of another root
/// of the address (such as this device's refused creation) only opens the
/// values sealed under it. When no such key of the epoch is held, the held
/// keys of other spaces are tried: a value that authenticates under one is
/// [`Opened::StaleSpace`]. Otherwise an epoch an acquisition showed is not
/// in the space's chain is `invalid-ciphertext`, and any other is
/// [`Opened::Missing`].
pub fn open_cell(
    registry: &Registry,
    held: &HeldKeys,
    row: &RowTarget<'_>,
    column: &EncryptedColumn<'_>,
    stored: &[u8],
) -> ContentResult<Opened> {
    if let Some(opened) = classify_short(stored) {
        return Ok(opened);
    }
    let record = match read_stored_record(stored) {
        Ok(record) => record,
        Err(error) => return Ok(Opened::Unreadable(unreadable(error)?)),
    };
    if record.kind == RecordKind::Stream && !column.may_stream() {
        return Ok(Opened::Unreadable(DataError::UnsupportedFormat));
    }
    let epoch_id = record.epoch_id;
    let own = held.keys_of(row.space, epoch_id);
    let candidates: Vec<_> = own.iter().map(|(space, key, _)| (*space, *key)).collect();
    match try_keys(
        registry,
        &candidates,
        row,
        column,
        record.kind,
        epoch_id,
        stored,
    )? {
        Some(Ok((value, _))) => return Ok(Opened::Value(value)),
        Some(Err(code))
            if code == DataError::UnsupportedFormat || own.iter().any(|(_, _, own)| *own) =>
        {
            return Ok(Opened::Unreadable(code));
        }
        // Only keys of another root of the address, which do not open the
        // cell: they decide nothing, and the root's own key is missing.
        Some(Err(_)) | None => {}
    }
    let elsewhere = held.keys_elsewhere(row.space, epoch_id);
    if let Some(opened) = try_keys(
        registry,
        &elsewhere,
        row,
        column,
        record.kind,
        epoch_id,
        stored,
    )? {
        match opened {
            Ok((value, index)) => {
                let space = elsewhere[index].0.address().clone();
                return Ok(Opened::StaleSpace { value, space });
            }
            Err(DataError::UnsupportedFormat) => {
                return Ok(Opened::Unreadable(DataError::UnsupportedFormat));
            }
            // Another space's key that does not authenticate says nothing
            // about the row's own space: fall through.
            Err(_) => {}
        }
    }
    let absent = held
        .known(row.space)
        .is_some_and(|known| known.is_absent(epoch_id));
    Ok(if absent {
        Opened::Unreadable(DataError::InvalidCiphertext)
    } else {
        Opened::Missing {
            epoch_id: epoch_id.to_owned(),
        }
    })
}

/// Starts streaming one large value of `column` under `key` (§5.5, H5):
/// write the returned prefix (the routing record, then the stream header),
/// then the writer's output. Only non-indexed `bytes` columns stream
/// ([`ContentError::NotStreamable`]); the target is checked and the
/// metadata encrypted before any source byte is pulled.
pub fn begin_stream(
    suite: &Suite<'_>,
    key: &WriteKey<'_>,
    row: &RowTarget<'_>,
    column: &EncryptedColumn<'_>,
    rng: &mut dyn RandomSource,
) -> ContentResult<(Box<dyn StreamWriter>, Vec<u8>)> {
    if !column.may_stream() {
        return Err(ContentError::NotStreamable);
    }
    if key.address() != row.space {
        return Err(ProtocolError::InconsistentEvidence.into());
    }
    let epoch_id = key.epoch_id();
    Ok(records::begin_stream_value(
        suite.stream,
        key.key(),
        &row.cell(column.column_id, epoch_id),
        column.cell_type,
        rng,
    )?)
}

/// The routing of a stored stream: its epoch and how many bytes of the
/// stored value it takes (the routing record's header and the epoch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRouting {
    pub epoch_id: String,
    pub consumed: usize,
}

/// Reads the routing at the start of a stored stream value, which must
/// hold at least the stream record's header and epoch. Another record
/// mechanism or version is `unsupported-format`; a malformed header or
/// epoch is `invalid-ciphertext`.
pub fn stream_routing(prefix: &[u8]) -> Result<StreamRouting, DataError> {
    let header = STREAM_RECORD
        .header()
        .expect("the stream record mechanism is valid");
    let consumed = header.len() + EPOCH_BYTES;
    match envelope::parse(prefix) {
        Ok((id, version, _))
            if id == STREAM_RECORD.id.as_bytes() && version == STREAM_RECORD.version => {}
        Ok(_) => return Err(DataError::UnsupportedFormat),
        Err(_) => return Err(DataError::InvalidCiphertext),
    }
    let epoch = prefix
        .get(header.len()..consumed)
        .filter(|epoch| records::is_epoch_id(epoch))
        .ok_or(DataError::InvalidCiphertext)?;
    Ok(StreamRouting {
        epoch_id: core::str::from_utf8(epoch)
            .expect("checked ASCII")
            .to_owned(),
        consumed,
    })
}

/// One key a stream may be sealed under, with its context.
struct StreamCandidate {
    key: Zeroizing<[u8; 32]>,
    context: Vec<u8>,
    stale: Option<SpaceAddress>,
    reader: Option<Box<dyn StreamReader>>,
}

/// Decrypts one stored large value incrementally (§5.5, H5), from its
/// routing record on. Each plaintext record is returned only after it
/// authenticates, and whole-value success needs [`StreamValueReader::finish`]
/// (large-value format, "Consumption and lifetime"). Owns copies of the
/// candidate keys, cleared on drop.
pub struct StreamValueReader<'r> {
    registry: &'r Registry,
    candidates: Vec<StreamCandidate>,
    /// The stream mechanism's encoding, until its header can be routed.
    pending: Vec<u8>,
    routed: bool,
    /// The epoch the value names, and whether a candidate key is the row's
    /// own root's (or the epoch is known not to be in the space's chain):
    /// otherwise, if none of the candidates opens it, the row's own key is
    /// missing, not the value invalid (§4.2, as for a cell).
    epoch_id: String,
    decided: bool,
    /// Whether a record authenticated, narrowing the candidates to its key.
    authenticated: bool,
}

impl core::fmt::Debug for StreamValueReader<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StreamValueReader")
            .field("candidates", &self.candidates.len())
            .field("routed", &self.routed)
            .finish()
    }
}

/// What starting to read a stored stream gives. Plaintext is never
/// printed.
pub enum StreamOpen<'r> {
    /// The reader, and the plaintext of any records that completed inside
    /// `prefix` after its routing (empty when `prefix` was only the
    /// routing). The caller continues with the bytes after `prefix`.
    Reader {
        reader: StreamValueReader<'r>,
        initial: Zeroizing<Vec<u8>>,
    },
    /// Not readable with the keys held: [`Opened::Missing`] or
    /// [`Opened::Unreadable`].
    Unreadable(Opened),
}

impl core::fmt::Debug for StreamOpen<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Reader { reader, .. } => f
                .debug_struct("Reader")
                .field("reader", reader)
                .field("initial", &"..")
                .finish(),
            Self::Unreadable(opened) => f.debug_tuple("Unreadable").field(opened).finish(),
        }
    }
}

impl<'r> StreamValueReader<'r> {
    /// Starts reading a stored stream of `column` in `row`, given at least
    /// its routing (`prefix`, from the start of the stored value). The keys
    /// are chosen as for [`open_cell`]: the row's own space's keys of the
    /// epoch; without one of its own root, also other spaces' (the value
    /// is then stale), and if none opens it the epoch is missing or known
    /// absent. A column that may not stream is `unsupported-format`.
    pub fn open(
        registry: &'r Registry,
        held: &HeldKeys,
        row: &RowTarget<'_>,
        column: &EncryptedColumn<'_>,
        prefix: &[u8],
    ) -> ContentResult<StreamOpen<'r>> {
        if !column.may_stream() {
            return Ok(StreamOpen::Unreadable(Opened::Unreadable(
                DataError::UnsupportedFormat,
            )));
        }
        let routing = match stream_routing(prefix) {
            Ok(routing) => routing,
            Err(code) => return Ok(StreamOpen::Unreadable(Opened::Unreadable(code))),
        };
        let epoch_id = routing.epoch_id.as_str();
        let own = held.keys_of(row.space, epoch_id);
        let decides = own.iter().any(|(_, _, own)| *own);
        let mut keys: Vec<_> = own
            .into_iter()
            .map(|(space, key, _)| (space, key, false))
            .collect();
        if !decides {
            keys.extend(
                held.keys_elsewhere(row.space, epoch_id)
                    .into_iter()
                    .map(|(space, key)| (space, key, true)),
            );
        }
        let absent = held
            .known(row.space)
            .is_some_and(|known| known.is_absent(epoch_id));
        if keys.is_empty() {
            return Ok(StreamOpen::Unreadable(if absent {
                Opened::Unreadable(DataError::InvalidCiphertext)
            } else {
                Opened::Missing {
                    epoch_id: routing.epoch_id,
                }
            }));
        }
        let candidates = keys
            .into_iter()
            .map(|(space, key, stale)| {
                let target = row.in_space(space.address());
                let context = records::stream_context(
                    &target.cell(column.column_id, epoch_id),
                    column.cell_type,
                )?;
                Ok(StreamCandidate {
                    key: Zeroizing::new(*key),
                    context,
                    stale: stale.then(|| space.address().clone()),
                    reader: None,
                })
            })
            .collect::<ContentResult<_>>()?;
        let mut reader = Self {
            registry,
            candidates,
            pending: Vec::new(),
            routed: false,
            epoch_id: routing.epoch_id.clone(),
            decided: decides || absent,
            authenticated: false,
        };
        let mut initial = Zeroizing::new(Vec::new());
        match reader.update(&prefix[routing.consumed..], &mut initial) {
            Ok(()) => Ok(StreamOpen::Reader { reader, initial }),
            Err(ContentError::Missing { epoch_id }) => {
                Ok(StreamOpen::Unreadable(Opened::Missing { epoch_id }))
            }
            Err(ContentError::Data(code)) => Ok(StreamOpen::Unreadable(Opened::Unreadable(code))),
            Err(error) => Err(error),
        }
    }

    /// The other space the value was sealed under, once a record has
    /// authenticated under a key of another space: the row is
    /// `stale-space`. Before a record authenticated it is `None`, as a key
    /// of another space proves nothing; [`Self::finish`] gives it for the
    /// whole value, a value with no plaintext included.
    pub fn stale_space(&self) -> Option<&SpaceAddress> {
        match self.candidates.as_slice() {
            [only] if self.authenticated => only.stale.as_ref(),
            _ => None,
        }
    }

    /// Routes the stream mechanism's header once enough of it is buffered.
    fn route(&mut self) -> ContentResult<bool> {
        if self.routed {
            return Ok(true);
        }
        let cipher = match self.registry.stream_cipher_for(&self.pending) {
            Ok(cipher) => cipher,
            // The header is incomplete: wait for more input.
            Err(Error::InvalidCiphertext) if header_incomplete(&self.pending) => {
                return Ok(false);
            }
            Err(error) => return Err(ContentError::Data(unreadable(error)?)),
        };
        for candidate in &mut self.candidates {
            candidate.reader = Some(cipher.decryptor(&candidate.key, &candidate.context)?);
        }
        self.routed = true;
        Ok(true)
    }

    /// Appends authenticated plaintext to `out`. Malformed or unauthentic
    /// input under every candidate key is
    /// [`ContentError::Data`]`(InvalidCiphertext)`; a provider failure is
    /// an error and no other candidate is tried.
    pub fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> ContentResult<()> {
        let input = if self.routed {
            input
        } else {
            self.pending.extend_from_slice(input);
            if !self.route()? {
                return Ok(());
            }
            &core::mem::take(&mut self.pending)[..]
        };
        let mut produced: Option<(usize, Zeroizing<Vec<u8>>)> = None;
        let mut live = Vec::with_capacity(self.candidates.len());
        for mut candidate in core::mem::take(&mut self.candidates) {
            let reader = candidate.reader.as_mut().expect("routed");
            let mut plain = Zeroizing::new(Vec::new());
            match reader.update(input, &mut plain) {
                Ok(()) => {
                    if produced.is_none() && !plain.is_empty() {
                        produced = Some((live.len(), plain));
                    }
                    live.push(candidate);
                }
                Err(Error::InvalidCiphertext) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // Once a record authenticated under one key, that key is the one.
        if let Some((kept, plain)) = produced {
            let candidate = live.swap_remove(kept);
            live = vec![candidate];
            self.authenticated = true;
            out.extend_from_slice(&plain);
        }
        self.candidates = live;
        if self.candidates.is_empty() {
            return Err(self.none_opens());
        }
        Ok(())
    }

    /// Every candidate failed: keys of other roots or spaces deciding
    /// nothing, the row's own key is missing; otherwise the value is
    /// invalid.
    fn none_opens(&self) -> ContentError {
        if self.decided {
            ContentError::Data(DataError::InvalidCiphertext)
        } else {
            ContentError::Missing {
                epoch_id: self.epoch_id.clone(),
            }
        }
    }

    /// Succeeds only if the whole value, including its last record,
    /// authenticated under one candidate key; gives the other space that
    /// key is of, if any: the row is then `stale-space`.
    pub fn finish(self) -> ContentResult<Option<SpaceAddress>> {
        if !self.routed {
            return Err(ContentError::Data(DataError::InvalidCiphertext));
        }
        let missing = self.none_opens();
        for candidate in self.candidates {
            match candidate.reader.expect("routed").finish() {
                Ok(()) => return Ok(candidate.stale),
                Err(Error::InvalidCiphertext) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(missing)
    }
}

/// Whether `bytes` could still become a complete envelope header.
fn header_incomplete(bytes: &[u8]) -> bool {
    const MAGIC: &[u8] = b"JE2E\x01";
    let prefix = bytes.len().min(MAGIC.len());
    if bytes[..prefix] != MAGIC[..prefix] {
        return false;
    }
    match bytes.get(MAGIC.len()) {
        None => true,
        Some(&length) => bytes.len() < MAGIC.len() + 1 + usize::from(length) + 4,
    }
}

/// Re-encrypts one stored stream under another key or context in bounded
/// memory (lost-creation repair, §3.2): authenticated
/// plaintext from the reader goes straight into the writer, and the writer
/// finishes only after the reader's last record authenticated. A value
/// that opens only under a key of another space than the reader's row is
/// refused with `stale-space`, as a cell is: it is never carried.
pub struct StreamResealer<'r> {
    reader: StreamValueReader<'r>,
    writer: Box<dyn StreamWriter>,
}

impl core::fmt::Debug for StreamResealer<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StreamResealer")
            .field("reader", &self.reader)
            .finish_non_exhaustive()
    }
}

impl<'r> StreamResealer<'r> {
    /// `reader` opened the source value; `writer` comes from
    /// [`begin_stream`], whose prefix the caller writes first.
    pub fn new(reader: StreamValueReader<'r>, writer: Box<dyn StreamWriter>) -> Self {
        Self { reader, writer }
    }

    /// Feeds source bytes, appending re-encrypted output to `out`.
    pub fn update(&mut self, input: &[u8], out: &mut Vec<u8>) -> ContentResult<()> {
        let mut plain = Zeroizing::new(Vec::new());
        self.reader.update(input, &mut plain)?;
        if self.reader.stale_space().is_some() {
            return Err(ContentError::Data(DataError::StaleSpace));
        }
        self.writer.update(&plain, out)?;
        Ok(())
    }

    /// Ends the source; appends the rest of the new encoding only if the
    /// whole source authenticated under a key of the reader's row's space.
    pub fn finish(self, out: &mut Vec<u8>) -> ContentResult<()> {
        if self.reader.finish()?.is_some() {
            return Err(ContentError::Data(DataError::StaleSpace));
        }
        self.writer.finish(out)?;
        Ok(())
    }
}

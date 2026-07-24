//! Schema-driven columnar window codec for typed record runs.
//!
//! This module implements the pure codec described in groove SPEC ch. 2 §2.9:
//! a bounded run of consecutive typed records is encoded as key columns plus
//! value columns, and each column independently chooses the smallest measured
//! representation for that window. It has no storage integration; callers pass
//! logical key/value records and receive logical key/value records back.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use crate::schema::PrimaryKey;

const FORMAT_VERSION: u8 = 1;

/// Target record count for a storage window.
///
/// The codec accepts larger inputs for tests and future repair tools, but the
/// storage integration should close ordinary windows around this count so a
/// decode stays cache-sized.
pub const TARGET_RECORDS_PER_WINDOW: usize = 256;

/// Target decoded payload bytes for a storage window.
///
/// This is guidance for the future record-store packer, not a hard codec limit.
pub const TARGET_DECODED_BYTES_PER_WINDOW: usize = 64 * 1024;

/// The key and value row layouts of the records packed into a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowSchema {
    key: RecordDescriptor,
    value: RecordDescriptor,
}

impl WindowSchema {
    /// Builds a window schema from explicit key and value descriptors.
    pub fn new(key: RecordDescriptor, value: RecordDescriptor) -> Self {
        Self { key, value }
    }

    /// Derives the key descriptor from a table's primary key, pairing it with
    /// the given value descriptor.
    pub fn from_primary_key(primary_key: &PrimaryKey, value: RecordDescriptor) -> Self {
        let key = RecordDescriptor::new(primary_key.columns.iter().map(|column| {
            (
                column.column.clone(),
                column.key_type.column_type().value_type(),
            )
        }));
        Self { key, value }
    }

    /// The key row layout.
    pub fn key_descriptor(&self) -> RecordDescriptor {
        self.key
    }

    /// The value row layout.
    pub fn value_descriptor(&self) -> RecordDescriptor {
        self.value
    }
}

/// One record inside a window: its key row and value row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowRecord {
    pub key: OwnedRecord,
    pub value: OwnedRecord,
}

impl WindowRecord {
    /// Pairs a key row with its value row.
    pub fn new(key: OwnedRecord, value: OwnedRecord) -> Self {
        Self { key, value }
    }
}

/// How one column of a window was encoded — each column independently picks
/// the smallest representation that fits its measured values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnEncodingKind {
    /// Every row has the same value; store it once.
    Constant,
    /// Store the first value, then varint deltas between consecutive rows —
    /// compact for monotonically increasing integer keys.
    DeltaVarint,
    /// Store the distinct values once plus a per-row index into them.
    Dictionary,
    /// A row equals the previous row's value in this column; store a back
    /// reference instead of the value.
    PreviousRowField,
    /// No pattern found; store each value verbatim.
    Verbatim,
}

/// Diagnostic summary of one encoded window: its record count, encoded size,
/// and the encoding chosen per column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowSummary {
    pub record_count: usize,
    pub encoded_bytes: usize,
    pub column_encodings: Vec<ColumnEncodingKind>,
}

/// Failures from encoding, decoding, or probing a window.
#[derive(Debug, Error)]
pub enum WindowCodecError {
    #[error("record error: {0}")]
    Record(#[from] crate::records::Error),
    #[error("postcard encode error: {0}")]
    Encode(#[from] postcard::Error),
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u8),
    #[error("record count {record_count} exceeds u16 window header capacity")]
    TooManyRecords { record_count: usize },
    #[error("record descriptor mismatch")]
    DescriptorMismatch,
    #[error("invalid encoded window: {0}")]
    Invalid(&'static str),
    #[error("trailing bytes in encoded window")]
    TrailingBytes,
}

/// The serialized (columnar) form of a whole window: a header plus one
/// encoded column per key and value field.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedWindow {
    version: u8,
    record_count: u16,
    key_column_count: u16,
    value_column_count: u16,
    columns: Vec<EncodedColumn>,
}

/// One serialized column: its chosen encoding and the bytes that encoding
/// produced.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncodedColumn {
    kind: ColumnEncodingKind,
    data: Vec<u8>,
}

/// Whether a column belongs to the key record or the value record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnRole {
    Key,
    Value,
}

/// Locates one column within a window: which record it belongs to, its field
/// index there, and its type.
#[derive(Clone, Debug)]
struct ColumnSpec {
    role: ColumnRole,
    idx: usize,
    value_type: ValueType,
}

/// One candidate encoding of a column while the encoder picks the smallest.
#[derive(Clone)]
struct Candidate {
    kind: ColumnEncodingKind,
    data: Vec<u8>,
}

/// Encodes a run of records into one columnar window.
///
/// * `schema` — the key/value layouts every record must match.
/// * `records` — the records to pack (validated against `schema`).
///
/// Each column is collected across all rows and encoded with whichever
/// [`ColumnEncodingKind`] measures smallest, then the columns are serialized
/// under a header. Reverse with [`decode_window`].
pub fn encode_window(
    schema: &WindowSchema,
    records: &[WindowRecord],
) -> Result<Vec<u8>, WindowCodecError> {
    let record_count =
        u16::try_from(records.len()).map_err(|_| WindowCodecError::TooManyRecords {
            record_count: records.len(),
        })?;
    validate_record_descriptors(schema, records)?;

    let specs = column_specs(schema);
    let columns = collect_columns(&specs, records)?;
    let encoded_columns = columns
        .iter()
        .enumerate()
        .map(|(column_idx, values)| encode_column(column_idx, &specs[column_idx], values, &columns))
        .collect::<Result<Vec<_>, _>>()?;

    let encoded = EncodedWindow {
        version: FORMAT_VERSION,
        record_count,
        key_column_count: u16::try_from(schema.key.fields().len()).map_err(|_| {
            WindowCodecError::Invalid("key column count exceeds u16 header capacity")
        })?,
        value_column_count: u16::try_from(schema.value.fields().len()).map_err(|_| {
            WindowCodecError::Invalid("value column count exceeds u16 header capacity")
        })?,
        columns: encoded_columns,
    };
    Ok(postcard::to_allocvec(&encoded)?)
}

/// Decodes a whole window back into its records, in order. The inverse of
/// [`encode_window`].
pub fn decode_window(
    schema: &WindowSchema,
    bytes: &[u8],
) -> Result<Vec<WindowRecord>, WindowCodecError> {
    let encoded = decode_encoded_window(bytes)?;
    decode_records(schema, &encoded)
}

/// Finds one record in a window by exact key, decoding only that record.
///
/// Records in a window are key-sorted, so this binary-searches the key
/// columns and materializes just the matching row — the cheap point-read
/// path over a packed window. Returns `None` when the key is absent.
pub fn lookup_window(
    schema: &WindowSchema,
    bytes: &[u8],
    key: &OwnedRecord,
) -> Result<Option<WindowRecord>, WindowCodecError> {
    if key.descriptor() != &schema.key {
        return Err(WindowCodecError::DescriptorMismatch);
    }
    let encoded = decode_encoded_window(bytes)?;
    let Some(row_idx) = lookup_window_key_index_encoded(schema, &encoded, key)? else {
        return Ok(None);
    };
    Ok(Some(decode_record_at(schema, &encoded, row_idx)?))
}

/// Like [`lookup_window`], but returns the matching record's *index* within
/// the window rather than the record itself.
pub fn lookup_window_key_index(
    schema: &WindowSchema,
    bytes: &[u8],
    key: &OwnedRecord,
) -> Result<Option<usize>, WindowCodecError> {
    if key.descriptor() != &schema.key {
        return Err(WindowCodecError::DescriptorMismatch);
    }
    let encoded = decode_encoded_window(bytes)?;
    lookup_window_key_index_encoded(schema, &encoded, key)
}

/// Binary-searches an already-decoded window's key columns for `key`,
/// materializing one candidate key per probe.
fn lookup_window_key_index_encoded(
    schema: &WindowSchema,
    encoded: &EncodedWindow,
    key: &OwnedRecord,
) -> Result<Option<usize>, WindowCodecError> {
    let specs = column_specs(schema);
    if encoded.columns.len() != specs.len()
        || usize::from(encoded.key_column_count) != schema.key.fields().len()
        || usize::from(encoded.value_column_count) != schema.value.fields().len()
    {
        return Err(WindowCodecError::Invalid(
            "encoded column counts do not match schema",
        ));
    }

    let mut low = 0usize;
    let mut high = usize::from(encoded.record_count);
    while low < high {
        let mid = low + (high - low) / 2;
        let candidate = decode_key_at(schema, &specs, encoded, mid)?;
        match candidate.as_slice().cmp(key.raw()) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Equal => return Ok(Some(mid)),
            std::cmp::Ordering::Greater => high = mid,
        }
    }
    Ok(None)
}

/// Reads a window's header and per-column encodings into a [`WindowSummary`]
/// without materializing any records — for diagnostics and compression
/// analysis.
pub fn summarize_window(bytes: &[u8]) -> Result<WindowSummary, WindowCodecError> {
    let encoded = decode_encoded_window(bytes)?;
    Ok(WindowSummary {
        record_count: usize::from(encoded.record_count),
        encoded_bytes: bytes.len(),
        column_encodings: encoded.columns.iter().map(|column| column.kind).collect(),
    })
}

/// Deserializes the window header/columns and validates the format version,
/// rejecting trailing bytes.
fn decode_encoded_window(bytes: &[u8]) -> Result<EncodedWindow, WindowCodecError> {
    let (encoded, tail) = postcard::take_from_bytes::<EncodedWindow>(bytes)?;
    if !tail.is_empty() {
        return Err(WindowCodecError::TrailingBytes);
    }
    if encoded.version != FORMAT_VERSION {
        return Err(WindowCodecError::UnsupportedVersion(encoded.version));
    }
    Ok(encoded)
}

/// Decodes every column of a window, then transposes them back into records
/// (each row's key columns and value columns re-assembled into a
/// [`WindowRecord`]).
fn decode_records(
    schema: &WindowSchema,
    encoded: &EncodedWindow,
) -> Result<Vec<WindowRecord>, WindowCodecError> {
    let specs = column_specs(schema);
    if encoded.columns.len() != specs.len()
        || usize::from(encoded.key_column_count) != schema.key.fields().len()
        || usize::from(encoded.value_column_count) != schema.value.fields().len()
    {
        return Err(WindowCodecError::Invalid(
            "encoded column counts do not match schema",
        ));
    }

    let mut columns = vec![Vec::new(); specs.len()];
    for column_idx in 0..encoded.columns.len() {
        columns[column_idx] = decode_column(
            column_idx,
            &specs[column_idx],
            &encoded.columns[column_idx],
            &columns,
            usize::from(encoded.record_count),
        )?;
    }

    let record_count = usize::from(encoded.record_count);
    let mut records = Vec::with_capacity(record_count);
    for row_values in (0..record_count).map(|row_idx| {
        columns
            .iter()
            .map(move |column| column[row_idx].clone())
            .collect::<Vec<_>>()
    }) {
        let mut key_values = Vec::with_capacity(schema.key.fields().len());
        let mut value_values = Vec::with_capacity(schema.value.fields().len());
        for (value, spec) in row_values.into_iter().zip(&specs) {
            match spec.role {
                ColumnRole::Key => key_values.push(value),
                ColumnRole::Value => value_values.push(value),
            }
        }
        records.push(WindowRecord {
            key: OwnedRecord::new(schema.key.create(&key_values)?, schema.key),
            value: OwnedRecord::new(schema.value.create(&value_values)?, schema.value),
        });
    }
    Ok(records)
}

/// Materializes just one row's key (its encoded bytes) — the per-probe work
/// of the binary search in [`lookup_window_key_index_encoded`].
fn decode_key_at(
    schema: &WindowSchema,
    specs: &[ColumnSpec],
    encoded: &EncodedWindow,
    row_idx: usize,
) -> Result<Vec<u8>, WindowCodecError> {
    let key_count = usize::from(encoded.key_column_count);
    let mut memo = BTreeMap::new();
    let mut key_values = Vec::with_capacity(key_count);
    for column_idx in 0..key_count {
        key_values.push(decode_column_value_at(
            column_idx, row_idx, specs, encoded, &mut memo,
        )?);
    }
    Ok(schema.key.create(&key_values)?)
}

/// Materializes one full record (key + value) at `row_idx` without decoding
/// the rest of the window — the payload read after a successful key lookup.
fn decode_record_at(
    schema: &WindowSchema,
    encoded: &EncodedWindow,
    row_idx: usize,
) -> Result<WindowRecord, WindowCodecError> {
    let record_count = usize::from(encoded.record_count);
    if row_idx >= record_count {
        return Err(WindowCodecError::Invalid("window row index out of bounds"));
    }
    let specs = column_specs(schema);
    if encoded.columns.len() != specs.len()
        || usize::from(encoded.key_column_count) != schema.key.fields().len()
        || usize::from(encoded.value_column_count) != schema.value.fields().len()
    {
        return Err(WindowCodecError::Invalid(
            "encoded column counts do not match schema",
        ));
    }

    let mut memo = BTreeMap::new();
    let mut key_values = Vec::with_capacity(schema.key.fields().len());
    let mut value_values = Vec::with_capacity(schema.value.fields().len());
    for column_idx in 0..specs.len() {
        let value = decode_column_value_at(column_idx, row_idx, &specs, encoded, &mut memo)?;
        match specs[column_idx].role {
            ColumnRole::Key => key_values.push(value),
            ColumnRole::Value => value_values.push(value),
        }
    }
    Ok(WindowRecord {
        key: OwnedRecord::new(schema.key.create(&key_values)?, schema.key),
        value: OwnedRecord::new(schema.value.create(&value_values)?, schema.value),
    })
}

/// Decodes one column's value at one row, dispatching on the column's
/// encoding. Results are memoized because `PreviousRowField` columns chase
/// back references and would otherwise re-decode earlier rows repeatedly.
fn decode_column_value_at(
    column_idx: usize,
    row_idx: usize,
    specs: &[ColumnSpec],
    encoded: &EncodedWindow,
    memo: &mut BTreeMap<(usize, usize), Value>,
) -> Result<Value, WindowCodecError> {
    if let Some(value) = memo.get(&(column_idx, row_idx)) {
        return Ok(value.clone());
    }
    let record_count = usize::from(encoded.record_count);
    let spec = &specs[column_idx];
    let column = &encoded.columns[column_idx];
    let value = match column.kind {
        ColumnEncodingKind::Constant => decode_constant_at(&column.data, &spec.value_type)?,
        ColumnEncodingKind::DeltaVarint => {
            decode_delta_varint_at(&column.data, &spec.value_type, row_idx, record_count)?
        }
        ColumnEncodingKind::Dictionary => {
            decode_dictionary_at(&column.data, &spec.value_type, row_idx, record_count)?
        }
        ColumnEncodingKind::PreviousRowField => decode_previous_row_field_at(
            column_idx,
            row_idx,
            &column.data,
            specs,
            encoded,
            memo,
            &spec.value_type,
        )?,
        ColumnEncodingKind::Verbatim => {
            decode_verbatim_at(&column.data, &spec.value_type, row_idx, record_count)?
        }
    };
    memo.insert((column_idx, row_idx), value.clone());
    Ok(value)
}

/// Checks that every record's key and value descriptor matches the window
/// schema before encoding.
fn validate_record_descriptors(
    schema: &WindowSchema,
    records: &[WindowRecord],
) -> Result<(), WindowCodecError> {
    for record in records {
        if record.key.descriptor() != &schema.key || record.value.descriptor() != &schema.value {
            return Err(WindowCodecError::DescriptorMismatch);
        }
    }
    Ok(())
}

/// The ordered column layout of a window: every key field first, then every
/// value field. This order is shared by the encoder and decoder.
fn column_specs(schema: &WindowSchema) -> Vec<ColumnSpec> {
    schema
        .key
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| ColumnSpec {
            role: ColumnRole::Key,
            idx,
            value_type: field.value_type.clone(),
        })
        .chain(
            schema
                .value
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| ColumnSpec {
                    role: ColumnRole::Value,
                    idx,
                    value_type: field.value_type.clone(),
                }),
        )
        .collect()
}

/// Transposes row-oriented records into column-oriented value vectors, one
/// per [`ColumnSpec`] — the step that turns records into columns the encoder
/// can compress.
fn collect_columns(
    specs: &[ColumnSpec],
    records: &[WindowRecord],
) -> Result<Vec<Vec<Value>>, WindowCodecError> {
    let mut columns = specs
        .iter()
        .map(|_| Vec::with_capacity(records.len()))
        .collect::<Vec<_>>();
    for record in records {
        for (column_idx, spec) in specs.iter().enumerate() {
            let value = match spec.role {
                ColumnRole::Key => record.key.get_idx(spec.idx)?,
                ColumnRole::Value => record.value.get_idx(spec.idx)?,
            };
            columns[column_idx].push(value);
        }
    }
    Ok(columns)
}

/// Encodes one column by trying every applicable encoding and keeping the
/// smallest (ties broken deterministically by [`encoding_tiebreaker`]).
/// `Verbatim` is always available as the fallback.
fn encode_column(
    column_idx: usize,
    spec: &ColumnSpec,
    values: &[Value],
    all_columns: &[Vec<Value>],
) -> Result<EncodedColumn, WindowCodecError> {
    let mut candidates = Vec::new();
    if let Some(candidate) = constant_candidate(values)? {
        candidates.push(candidate);
    }
    if let Some(candidate) = delta_varint_candidate(&spec.value_type, values)? {
        candidates.push(candidate);
    }
    if let Some(candidate) = previous_row_field_candidate(column_idx, values, all_columns)? {
        candidates.push(candidate);
    }
    if let Some(candidate) = dictionary_candidate(values)? {
        candidates.push(candidate);
    }
    candidates.push(verbatim_candidate(values)?);

    let candidate = candidates
        .into_iter()
        .min_by_key(|candidate| (candidate.data.len(), encoding_tiebreaker(candidate.kind)))
        .ok_or(WindowCodecError::Invalid("missing column candidate"))?;
    Ok(EncodedColumn {
        kind: candidate.kind,
        data: candidate.data,
    })
}

/// Decodes one whole column back to its per-row values, dispatching on the
/// stored [`ColumnEncodingKind`]. `PreviousRowField` columns read from the
/// already-decoded earlier columns.
fn decode_column(
    column_idx: usize,
    spec: &ColumnSpec,
    column: &EncodedColumn,
    decoded_columns: &[Vec<Value>],
    record_count: usize,
) -> Result<Vec<Value>, WindowCodecError> {
    match column.kind {
        ColumnEncodingKind::Constant => {
            decode_constant(&column.data, &spec.value_type, record_count)
        }
        ColumnEncodingKind::DeltaVarint => {
            decode_delta_varint(&column.data, &spec.value_type, record_count)
        }
        ColumnEncodingKind::Dictionary => {
            decode_dictionary(&column.data, &spec.value_type, record_count)
        }
        ColumnEncodingKind::PreviousRowField => decode_previous_row_field(
            column_idx,
            &column.data,
            decoded_columns,
            &spec.value_type,
            record_count,
        ),
        ColumnEncodingKind::Verbatim => {
            decode_verbatim(&column.data, &spec.value_type, record_count)
        }
    }
}

/// Deterministic priority for breaking size ties between encodings, so the
/// same input always encodes to the same bytes (simplest encoding wins).
fn encoding_tiebreaker(kind: ColumnEncodingKind) -> u8 {
    match kind {
        ColumnEncodingKind::Constant => 0,
        ColumnEncodingKind::DeltaVarint => 1,
        ColumnEncodingKind::PreviousRowField => 2,
        ColumnEncodingKind::Dictionary => 3,
        ColumnEncodingKind::Verbatim => 4,
    }
}

/// Candidate: applicable only when every row is equal, storing the value
/// once. Also the encoding for an empty column.
fn constant_candidate(values: &[Value]) -> Result<Option<Candidate>, WindowCodecError> {
    let Some(first) = values.first() else {
        return Ok(Some(Candidate {
            kind: ColumnEncodingKind::Constant,
            data: Vec::new(),
        }));
    };
    if values.iter().all(|value| value == first) {
        let mut data = Vec::new();
        write_cell(&mut data, first)?;
        Ok(Some(Candidate {
            kind: ColumnEncodingKind::Constant,
            data,
        }))
    } else {
        Ok(None)
    }
}

/// Candidate: applicable to integer columns, storing the first value then
/// zigzag-varint deltas between consecutive rows — compact for slowly-varying
/// or monotonic keys.
fn delta_varint_candidate(
    value_type: &ValueType,
    values: &[Value],
) -> Result<Option<Candidate>, WindowCodecError> {
    if values.is_empty() || !is_integer_type(value_type) {
        return Ok(None);
    }
    let mut ints = Vec::with_capacity(values.len());
    for value in values {
        let Some(int) = integer_value(value, value_type) else {
            return Ok(None);
        };
        ints.push(int);
    }

    let mut data = Vec::new();
    write_varint(&mut data, ints[0]);
    for pair in ints.windows(2) {
        let delta = i128::try_from(pair[1])
            .map_err(|_| WindowCodecError::Invalid("integer delta value out of range"))?
            - i128::try_from(pair[0])
                .map_err(|_| WindowCodecError::Invalid("integer delta value out of range"))?;
        write_varint(&mut data, zigzag(delta)?);
    }
    Ok(Some(Candidate {
        kind: ColumnEncodingKind::DeltaVarint,
        data,
    }))
}

/// Candidate: applicable when values repeat, storing the distinct values once
/// plus a varint index per row. Declined when every value is unique (no
/// saving).
fn dictionary_candidate(values: &[Value]) -> Result<Option<Candidate>, WindowCodecError> {
    if values.len() <= 1 {
        return Ok(None);
    }
    let mut unique = Vec::<Vec<u8>>::new();
    let mut by_cell = BTreeMap::<Vec<u8>, usize>::new();
    let mut indexes = Vec::with_capacity(values.len());

    for value in values {
        let cell = cell_bytes(value)?;
        let idx = if let Some(idx) = by_cell.get(&cell) {
            *idx
        } else {
            let idx = unique.len();
            by_cell.insert(cell.clone(), idx);
            unique.push(cell);
            idx
        };
        indexes.push(idx);
    }
    if unique.len() == values.len() {
        return Ok(None);
    }

    let mut data = Vec::new();
    write_varint(&mut data, unique.len() as u128);
    for cell in unique {
        write_bytes(&mut data, &cell);
    }
    for idx in indexes {
        write_varint(&mut data, idx as u128);
    }
    Ok(Some(Candidate {
        kind: ColumnEncodingKind::Dictionary,
        data,
    }))
}

/// Candidate: applicable when this column equals some earlier column shifted
/// down one row (row `n` equals the source column's row `n-1`). Stores the
/// source column index plus this column's first value. Captures
/// "next-pointer" relationships between columns.
fn previous_row_field_candidate(
    column_idx: usize,
    values: &[Value],
    all_columns: &[Vec<Value>],
) -> Result<Option<Candidate>, WindowCodecError> {
    if values.len() <= 1 {
        return Ok(None);
    }
    for (source_idx, source) in all_columns.iter().enumerate().take(column_idx) {
        if (1..values.len()).all(|row_idx| values[row_idx] == source[row_idx - 1]) {
            let mut data = Vec::new();
            write_varint(&mut data, source_idx as u128);
            write_cell(&mut data, &values[0])?;
            return Ok(Some(Candidate {
                kind: ColumnEncodingKind::PreviousRowField,
                data,
            }));
        }
    }
    Ok(None)
}

/// The always-available fallback: every value stored one after another. This
/// is why [`encode_column`] can never fail to find a candidate.
fn verbatim_candidate(values: &[Value]) -> Result<Candidate, WindowCodecError> {
    let mut data = Vec::new();
    for value in values {
        write_cell(&mut data, value)?;
    }
    Ok(Candidate {
        kind: ColumnEncodingKind::Verbatim,
        data,
    })
}

/// Decodes a `Constant` column: read the single value, repeat it per row.
fn decode_constant(
    data: &[u8],
    value_type: &ValueType,
    record_count: usize,
) -> Result<Vec<Value>, WindowCodecError> {
    if record_count == 0 {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        return Err(WindowCodecError::Invalid("empty constant column has data"));
    }
    let mut cursor = Cursor::new(data);
    let value = cursor.read_cell(value_type)?;
    cursor.finish()?;
    Ok(vec![value; record_count])
}

/// Single-row `Constant` decode: the stored value applies to every row.
fn decode_constant_at(data: &[u8], value_type: &ValueType) -> Result<Value, WindowCodecError> {
    let mut cursor = Cursor::new(data);
    let value = cursor.read_cell(value_type)?;
    cursor.finish()?;
    Ok(value)
}

/// Decodes a `DeltaVarint` column: reconstruct each row by summing deltas
/// onto the first value.
fn decode_delta_varint(
    data: &[u8],
    value_type: &ValueType,
    record_count: usize,
) -> Result<Vec<Value>, WindowCodecError> {
    if record_count == 0 {
        return Ok(Vec::new());
    }
    let mut cursor = Cursor::new(data);
    let mut current = cursor.read_varint()?;
    let mut values = Vec::with_capacity(record_count);
    values.push(integer_to_value(current, value_type)?);
    for _ in 1..record_count {
        let delta = unzigzag(cursor.read_varint()?);
        current = apply_delta(current, delta)?;
        values.push(integer_to_value(current, value_type)?);
    }
    cursor.finish()?;
    Ok(values)
}

/// Single-row `DeltaVarint` decode: sums deltas up to `row_idx` only.
fn decode_delta_varint_at(
    data: &[u8],
    value_type: &ValueType,
    row_idx: usize,
    record_count: usize,
) -> Result<Value, WindowCodecError> {
    if row_idx >= record_count {
        return Err(WindowCodecError::Invalid("delta row index out of bounds"));
    }
    let mut cursor = Cursor::new(data);
    let mut current = cursor.read_varint()?;
    for _ in 0..row_idx {
        let delta = unzigzag(cursor.read_varint()?);
        current = apply_delta(current, delta)?;
    }
    integer_to_value(current, value_type)
}

/// Decodes a `Dictionary` column: read the distinct values, then map each
/// row's stored index back to one of them.
fn decode_dictionary(
    data: &[u8],
    value_type: &ValueType,
    record_count: usize,
) -> Result<Vec<Value>, WindowCodecError> {
    let mut cursor = Cursor::new(data);
    let unique_count = usize::try_from(cursor.read_varint()?)
        .map_err(|_| WindowCodecError::Invalid("dictionary too large"))?;
    let mut unique = Vec::with_capacity(unique_count);
    for _ in 0..unique_count {
        let cell = cursor.read_bytes()?;
        unique.push(value_from_cell(&cell, value_type)?);
    }
    let mut values = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let idx = usize::try_from(cursor.read_varint()?)
            .map_err(|_| WindowCodecError::Invalid("dictionary index too large"))?;
        values.push(
            unique
                .get(idx)
                .ok_or(WindowCodecError::Invalid("dictionary index out of bounds"))?
                .clone(),
        );
    }
    cursor.finish()?;
    Ok(values)
}

/// Single-row `Dictionary` decode: resolves just `row_idx`'s index.
fn decode_dictionary_at(
    data: &[u8],
    value_type: &ValueType,
    row_idx: usize,
    record_count: usize,
) -> Result<Value, WindowCodecError> {
    if row_idx >= record_count {
        return Err(WindowCodecError::Invalid(
            "dictionary row index out of bounds",
        ));
    }
    let mut cursor = Cursor::new(data);
    let unique_count = usize::try_from(cursor.read_varint()?)
        .map_err(|_| WindowCodecError::Invalid("dictionary too large"))?;
    let mut unique = Vec::with_capacity(unique_count);
    for _ in 0..unique_count {
        let cell = cursor.read_bytes()?;
        unique.push(value_from_cell(&cell, value_type)?);
    }
    let mut selected = None;
    for current_row in 0..record_count {
        let idx = usize::try_from(cursor.read_varint()?)
            .map_err(|_| WindowCodecError::Invalid("dictionary index too large"))?;
        if current_row == row_idx {
            selected = Some(
                unique
                    .get(idx)
                    .ok_or(WindowCodecError::Invalid("dictionary index out of bounds"))?
                    .clone(),
            );
        }
    }
    cursor.finish()?;
    selected.ok_or(WindowCodecError::Invalid("dictionary row missing"))
}

/// Decodes a `PreviousRowField` column: row 0 is the stored first value; row
/// `n` is the source column's row `n-1`.
fn decode_previous_row_field(
    column_idx: usize,
    data: &[u8],
    decoded_columns: &[Vec<Value>],
    value_type: &ValueType,
    record_count: usize,
) -> Result<Vec<Value>, WindowCodecError> {
    if record_count == 0 {
        return Ok(Vec::new());
    }
    let mut cursor = Cursor::new(data);
    let source_idx = usize::try_from(cursor.read_varint()?)
        .map_err(|_| WindowCodecError::Invalid("previous-row source index too large"))?;
    if source_idx >= column_idx {
        return Err(WindowCodecError::Invalid(
            "previous-row source must be an earlier column",
        ));
    }
    let source = decoded_columns
        .get(source_idx)
        .ok_or(WindowCodecError::Invalid("previous-row source missing"))?;
    if source.len() != record_count {
        return Err(WindowCodecError::Invalid(
            "previous-row source has wrong record count",
        ));
    }
    let mut values = Vec::with_capacity(record_count);
    values.push(cursor.read_cell(value_type)?);
    for row_idx in 1..record_count {
        values.push(source[row_idx - 1].clone());
    }
    cursor.finish()?;
    Ok(values)
}

/// Single-row `PreviousRowField` decode: row 0 is the stored first value;
/// otherwise recurse into the source column at `row_idx - 1` (the memo in
/// [`decode_column_value_at`] keeps this from re-decoding repeatedly).
fn decode_previous_row_field_at(
    column_idx: usize,
    row_idx: usize,
    data: &[u8],
    specs: &[ColumnSpec],
    encoded: &EncodedWindow,
    memo: &mut BTreeMap<(usize, usize), Value>,
    value_type: &ValueType,
) -> Result<Value, WindowCodecError> {
    let mut cursor = Cursor::new(data);
    let source_idx = usize::try_from(cursor.read_varint()?)
        .map_err(|_| WindowCodecError::Invalid("previous-row source index too large"))?;
    if source_idx >= column_idx {
        return Err(WindowCodecError::Invalid(
            "previous-row source must be an earlier column",
        ));
    }
    let first_value = cursor.read_cell(value_type)?;
    cursor.finish()?;
    if row_idx == 0 {
        return Ok(first_value);
    }
    decode_column_value_at(source_idx, row_idx - 1, specs, encoded, memo)
}

/// Decodes a `Verbatim` column: read one cell per row in order.
fn decode_verbatim(
    data: &[u8],
    value_type: &ValueType,
    record_count: usize,
) -> Result<Vec<Value>, WindowCodecError> {
    let mut cursor = Cursor::new(data);
    let mut values = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        values.push(cursor.read_cell(value_type)?);
    }
    cursor.finish()?;
    Ok(values)
}

/// Single-row `Verbatim` decode: scans cells up to and including `row_idx`.
fn decode_verbatim_at(
    data: &[u8],
    value_type: &ValueType,
    row_idx: usize,
    record_count: usize,
) -> Result<Value, WindowCodecError> {
    if row_idx >= record_count {
        return Err(WindowCodecError::Invalid(
            "verbatim row index out of bounds",
        ));
    }
    let mut cursor = Cursor::new(data);
    let mut selected = None;
    for current_row in 0..record_count {
        let value = cursor.read_cell(value_type)?;
        if current_row == row_idx {
            selected = Some(value);
        }
    }
    cursor.finish()?;
    selected.ok_or(WindowCodecError::Invalid("verbatim row missing"))
}

/// Writes one length-prefixed serialized value into `out`.
fn write_cell(out: &mut Vec<u8>, value: &Value) -> Result<(), WindowCodecError> {
    let cell = cell_bytes(value)?;
    write_bytes(out, &cell);
    Ok(())
}

/// Serializes one [`Value`] to bytes (postcard).
fn cell_bytes(value: &Value) -> Result<Vec<u8>, WindowCodecError> {
    Ok(postcard::to_allocvec(value)?)
}

/// Deserializes one value and re-validates it against `value_type` — so the
/// record layer's type rules (arrays, nullables, enums, NaN rejection) stay
/// the authority even on the columnar path.
fn value_from_cell(cell: &[u8], value_type: &ValueType) -> Result<Value, WindowCodecError> {
    let (value, tail) = postcard::take_from_bytes::<Value>(cell)?;
    if !tail.is_empty() {
        return Err(WindowCodecError::TrailingBytes);
    }
    // Rebuild through a one-field descriptor so existing type validation remains
    // the authority for nested arrays, nullables, enums, and NaN rejection.
    let descriptor = RecordDescriptor::new([("cell", value_type.clone())]);
    descriptor.create(std::slice::from_ref(&value))?;
    Ok(value)
}

/// Writes a varint length prefix followed by the bytes.
fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_varint(out, bytes.len() as u128);
    out.extend_from_slice(bytes);
}

/// Appends an LEB128 varint (7 bits per byte, high bit = continue).
fn write_varint(out: &mut Vec<u8>, mut value: u128) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Maps a signed integer to an unsigned one so small magnitudes (of either
/// sign) stay small varints: `0, -1, 1, -2, 2 → 0, 1, 2, 3, 4`. Rejects
/// `i128::MIN`, which has no positive counterpart.
fn zigzag(value: i128) -> Result<u128, WindowCodecError> {
    if value == i128::MIN {
        return Err(WindowCodecError::Invalid("delta underflow"));
    }
    Ok(if value < 0 {
        ((-value) as u128) * 2 - 1
    } else {
        (value as u128) * 2
    })
}

/// Reverses [`zigzag`].
fn unzigzag(value: u128) -> i128 {
    if value & 1 == 0 {
        (value / 2) as i128
    } else {
        -((value / 2 + 1) as i128)
    }
}

/// Applies a signed delta to a running unsigned value, failing on
/// over/underflow — the reconstruction step of delta-varint decoding.
fn apply_delta(current: u128, delta: i128) -> Result<u128, WindowCodecError> {
    if delta < 0 {
        current
            .checked_sub((-delta) as u128)
            .ok_or(WindowCodecError::Invalid("delta underflow"))
    } else {
        current
            .checked_add(delta as u128)
            .ok_or(WindowCodecError::Invalid("delta overflow"))
    }
}

/// `true` for the unsigned integer types that support delta-varint encoding.
fn is_integer_type(value_type: &ValueType) -> bool {
    matches!(
        value_type,
        ValueType::U8 | ValueType::U16 | ValueType::U32 | ValueType::U64
    )
}

/// Reads an integer value out as a `u128`, or `None` if it is not the
/// expected integer type.
fn integer_value(value: &Value, value_type: &ValueType) -> Option<u128> {
    match (value, value_type) {
        (Value::U8(value), ValueType::U8) => Some(u128::from(*value)),
        (Value::U16(value), ValueType::U16) => Some(u128::from(*value)),
        (Value::U32(value), ValueType::U32) => Some(u128::from(*value)),
        (Value::U64(value), ValueType::U64) => Some(u128::from(*value)),
        _ => None,
    }
}

/// Narrows a decoded `u128` back into a typed integer [`Value`], failing when
/// it does not fit the target width.
fn integer_to_value(value: u128, value_type: &ValueType) -> Result<Value, WindowCodecError> {
    match value_type {
        ValueType::U8 => u8::try_from(value)
            .map(Value::U8)
            .map_err(|_| WindowCodecError::Invalid("u8 delta value out of range")),
        ValueType::U16 => u16::try_from(value)
            .map(Value::U16)
            .map_err(|_| WindowCodecError::Invalid("u16 delta value out of range")),
        ValueType::U32 => u32::try_from(value)
            .map(Value::U32)
            .map_err(|_| WindowCodecError::Invalid("u32 delta value out of range")),
        ValueType::U64 => u64::try_from(value)
            .map(Value::U64)
            .map_err(|_| WindowCodecError::Invalid("u64 delta value out of range")),
        _ => Err(WindowCodecError::Invalid("non-integer delta column")),
    }
}

/// A forward byte reader over one encoded column, tracking a read offset. The
/// decoders read varints, length-prefixed byte strings, and cells through it,
/// and call [`Self::finish`] to assert the whole column was consumed.
struct Cursor<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    /// A cursor positioned at the start of `data`.
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    /// Reads one LEB128 varint, advancing past it.
    fn read_varint(&mut self) -> Result<u128, WindowCodecError> {
        let mut shift = 0u32;
        let mut value = 0u128;
        loop {
            let byte = *self
                .data
                .get(self.offset)
                .ok_or(WindowCodecError::Invalid("unexpected eof in varint"))?;
            self.offset += 1;
            value |= u128::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift >= 128 {
                return Err(WindowCodecError::Invalid("varint too long"));
            }
        }
    }

    /// Reads a varint length prefix then that many bytes.
    fn read_bytes(&mut self) -> Result<Vec<u8>, WindowCodecError> {
        let len = usize::try_from(self.read_varint()?)
            .map_err(|_| WindowCodecError::Invalid("length too large"))?;
        let end = self
            .offset
            .checked_add(len)
            .ok_or(WindowCodecError::Invalid("length overflow"))?;
        let bytes = self
            .data
            .get(self.offset..end)
            .ok_or(WindowCodecError::Invalid("unexpected eof in bytes"))?
            .to_vec();
        self.offset = end;
        Ok(bytes)
    }

    /// Reads one length-prefixed, type-validated value cell.
    fn read_cell(&mut self, value_type: &ValueType) -> Result<Value, WindowCodecError> {
        let cell = self.read_bytes()?;
        value_from_cell(&cell, value_type)
    }

    /// Asserts the column was fully consumed; leftover bytes are corruption.
    fn finish(self) -> Result<(), WindowCodecError> {
        if self.offset == self.data.len() {
            Ok(())
        } else {
            Err(WindowCodecError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{EnumSchema, ValueType};
    use crate::schema::{IntegerKeyType, PrimaryKey, PrimaryKeyColumn};

    #[test]
    fn randomized_supported_values_round_trip() {
        let enum_schema = EnumSchema::new("state", ["new", "open", "done"]).unwrap();
        let schema = WindowSchema::new(
            RecordDescriptor::new([
                ("tenant", ValueType::String),
                ("seq", ValueType::U64),
                ("node", ValueType::Uuid),
            ]),
            RecordDescriptor::new([
                ("u8", ValueType::U8),
                ("u16", ValueType::U16),
                ("u32", ValueType::U32),
                ("u64", ValueType::U64),
                ("f64", ValueType::F64),
                ("bool", ValueType::Bool),
                ("string", ValueType::String),
                ("bytes", ValueType::Bytes),
                ("uuid", ValueType::Uuid),
                ("enum", ValueType::Enum(enum_schema.clone())),
                (
                    "tuple",
                    ValueType::Tuple(vec![ValueType::U16, ValueType::Bool, ValueType::Uuid]),
                ),
                ("array", ValueType::Array(Box::new(ValueType::String))),
                ("nullable", ValueType::Nullable(Box::new(ValueType::Bytes))),
            ]),
        );
        let mut rng = Rng::new(0x51a7_5eed_c0de);
        for len in [0, 1, 2, 3, 17, TARGET_RECORDS_PER_WINDOW] {
            let records = (0..len)
                .map(|idx| random_supported_record(&schema, &enum_schema, idx, &mut rng))
                .collect::<Vec<_>>();
            assert_round_trip(&schema, &records);
        }
    }

    #[test]
    fn boundary_values_round_trip() {
        let schema = WindowSchema::new(
            RecordDescriptor::new([
                ("a", ValueType::U8),
                ("b", ValueType::U16),
                ("c", ValueType::U32),
                ("d", ValueType::U64),
            ]),
            RecordDescriptor::new([
                ("empty_string", ValueType::String),
                ("empty_bytes", ValueType::Bytes),
                ("min_f64", ValueType::F64),
                ("max_f64", ValueType::F64),
                ("none", ValueType::Nullable(Box::new(ValueType::U64))),
                ("some", ValueType::Nullable(Box::new(ValueType::U64))),
                ("empty_array", ValueType::Array(Box::new(ValueType::U16))),
            ]),
        );
        let records = vec![WindowRecord::new(
            owned(
                schema.key_descriptor(),
                &[
                    Value::U8(u8::MAX),
                    Value::U16(u16::MAX),
                    Value::U32(u32::MAX),
                    Value::U64(u64::MAX),
                ],
            ),
            owned(
                schema.value_descriptor(),
                &[
                    Value::String(String::new()),
                    Value::Bytes(Vec::new()),
                    Value::F64(f64::MIN),
                    Value::F64(f64::MAX),
                    Value::Nullable(None),
                    Value::Nullable(Some(Box::new(Value::U64(u64::MAX)))),
                    Value::Array(Vec::new()),
                ],
            ),
        )];
        assert_round_trip(&schema, &records);
    }

    #[test]
    fn encoding_is_deterministic() {
        let (schema, records) = serial_chain_window(128);
        let first = encode_window(&schema, &records).unwrap();
        let second = encode_window(&schema, &records).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn adversarial_unique_window_degrades_to_verbatim() {
        let schema = WindowSchema::new(
            RecordDescriptor::new([("id", ValueType::Bytes)]),
            RecordDescriptor::new([("payload", ValueType::Bytes), ("label", ValueType::String)]),
        );
        let mut rng = Rng::new(0xa11d_1ffe_4ade);
        let records = (0..128)
            .map(|idx| {
                WindowRecord::new(
                    owned(schema.key_descriptor(), &[Value::Bytes(rng.bytes(16))]),
                    owned(
                        schema.value_descriptor(),
                        &[
                            Value::Bytes(rng.bytes(37 + idx % 7)),
                            Value::String(format!("unique-{idx}-{}", rng.next())),
                        ],
                    ),
                )
            })
            .collect::<Vec<_>>();
        let encoded = encode_window(&schema, &records).unwrap();
        let summary = summarize_window(&encoded).unwrap();
        assert_eq!(
            summary.column_encodings,
            vec![
                ColumnEncodingKind::Verbatim,
                ColumnEncodingKind::Verbatim,
                ColumnEncodingKind::Verbatim,
            ]
        );
        assert_round_trip(&schema, &records);
    }

    #[test]
    fn lookup_by_key_matches_linear_scan() {
        let (schema, records) = serial_chain_window(64);
        let encoded = encode_window(&schema, &records).unwrap();
        let target = &records[37].key;
        let expected = records
            .iter()
            .find(|record| record.key.raw() == target.raw())
            .cloned();
        assert_eq!(lookup_window(&schema, &encoded, target).unwrap(), expected);

        let missing = owned(
            schema.key_descriptor(),
            &[
                Value::Bytes(vec![9; 16]),
                Value::U64(999_999),
                Value::U64(7),
            ],
        );
        assert!(
            lookup_window(&schema, &encoded, &missing)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn schema_can_be_derived_from_primary_key() {
        let primary_key = PrimaryKey::composite([
            PrimaryKeyColumn::bytes("row"),
            PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
            PrimaryKeyColumn::uuid("node"),
        ]);
        let value = RecordDescriptor::new([("payload", ValueType::Bytes)]);
        let schema = WindowSchema::from_primary_key(&primary_key, value);
        assert_eq!(
            schema
                .key_descriptor()
                .fields()
                .iter()
                .map(|field| field.value_type.clone())
                .collect::<Vec<_>>(),
            [ValueType::Bytes, ValueType::U64, ValueType::Uuid]
        );
    }

    #[test]
    fn serial_chain_measurement_stays_compact() {
        let (schema, records) = serial_chain_window(TARGET_RECORDS_PER_WINDOW);
        let encoded = encode_window(&schema, &records).unwrap();
        let summary = summarize_window(&encoded).unwrap();
        let bytes_per_record = encoded.len() as f64 / records.len() as f64;
        println!(
            "serial_chain_window encoded={} records={} bytes_per_record={bytes_per_record:.2} encodings={:?}",
            encoded.len(),
            records.len(),
            summary.column_encodings
        );
        assert!(
            bytes_per_record < 24.0,
            "serial-chain window used {bytes_per_record:.2} B/record"
        );
        assert!(
            summary
                .column_encodings
                .contains(&ColumnEncodingKind::PreviousRowField)
        );
        assert_round_trip(&schema, &records);
    }

    #[test]
    fn adversarial_measurement_remains_correct_when_large() {
        let schema = WindowSchema::new(
            RecordDescriptor::new([("id", ValueType::Bytes)]),
            RecordDescriptor::new([("payload", ValueType::Bytes)]),
        );
        let mut rng = Rng::new(0x00ad_c0de);
        let records = (0..TARGET_RECORDS_PER_WINDOW)
            .map(|_| {
                WindowRecord::new(
                    owned(schema.key_descriptor(), &[Value::Bytes(rng.bytes(24))]),
                    owned(schema.value_descriptor(), &[Value::Bytes(rng.bytes(80))]),
                )
            })
            .collect::<Vec<_>>();
        let encoded = encode_window(&schema, &records).unwrap();
        let bytes_per_record = encoded.len() as f64 / records.len() as f64;
        println!(
            "adversarial_window encoded={} records={} bytes_per_record={bytes_per_record:.2}",
            encoded.len(),
            records.len()
        );
        assert!(bytes_per_record > 90.0);
        assert_round_trip(&schema, &records);
    }

    fn assert_round_trip(schema: &WindowSchema, records: &[WindowRecord]) {
        let encoded = encode_window(schema, records).unwrap();
        let decoded = decode_window(schema, &encoded).unwrap();
        assert_eq!(decoded, records);
    }

    fn random_supported_record(
        schema: &WindowSchema,
        enum_schema: &EnumSchema,
        idx: usize,
        rng: &mut Rng,
    ) -> WindowRecord {
        let uuid = uuid::Uuid::from_bytes(rng.bytes_array());
        let key = owned(
            schema.key_descriptor(),
            &[
                Value::String(format!("tenant-{}", idx % 4)),
                Value::U64(idx as u64),
                Value::Uuid(uuid),
            ],
        );
        let nullable = if idx.is_multiple_of(3) {
            Value::Nullable(None)
        } else {
            Value::Nullable(Some(Box::new(Value::Bytes(rng.bytes(5 + idx % 9)))))
        };
        let value = owned(
            schema.value_descriptor(),
            &[
                Value::U8(rng.next() as u8),
                Value::U16(rng.next() as u16),
                Value::U32(rng.next() as u32),
                Value::U64(rng.next()),
                Value::F64((rng.next() % 10_000) as f64 / 10.0),
                Value::Bool(rng.next().is_multiple_of(2)),
                Value::String(format!("s-{}-{}", idx, rng.next() % 97)),
                Value::Bytes(rng.bytes(idx % 13)),
                Value::Uuid(uuid::Uuid::from_bytes(rng.bytes_array())),
                Value::Enum((idx % enum_schema.variants.len()) as u8),
                Value::Tuple(vec![
                    Value::U16(idx as u16),
                    Value::Bool(idx.is_multiple_of(2)),
                    Value::Uuid(uuid),
                ]),
                Value::Array(vec![
                    Value::String(format!("a{idx}")),
                    Value::String(format!("b{}", rng.next() % 19)),
                ]),
                nullable,
            ],
        );
        WindowRecord::new(key, value)
    }

    fn serial_chain_window(len: usize) -> (WindowSchema, Vec<WindowRecord>) {
        let schema = WindowSchema::new(
            RecordDescriptor::new([
                ("row", ValueType::Bytes),
                ("time", ValueType::U64),
                ("node", ValueType::U64),
            ]),
            RecordDescriptor::new([
                ("author", ValueType::Bytes),
                ("tx", ValueType::U64),
                ("parent", ValueType::U64),
                ("op", ValueType::Bytes),
            ]),
        );
        let row = vec![7; 16];
        let author = vec![3; 16];
        let records = (0..len)
            .map(|idx| {
                let tx = 10_000 + idx as u64;
                WindowRecord::new(
                    owned(
                        schema.key_descriptor(),
                        &[Value::Bytes(row.clone()), Value::U64(tx), Value::U64(42)],
                    ),
                    owned(
                        schema.value_descriptor(),
                        &[
                            Value::Bytes(author.clone()),
                            Value::U64(tx),
                            Value::U64(tx.saturating_sub(1)),
                            Value::Bytes(vec![b'i', (idx % 26) as u8]),
                        ],
                    ),
                )
            })
            .collect::<Vec<_>>();
        (schema, records)
    }

    fn owned(descriptor: RecordDescriptor, values: &[Value]) -> OwnedRecord {
        OwnedRecord::new(descriptor.create(values).unwrap(), descriptor)
    }

    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next(&mut self) -> u64 {
            let mut value = self.0;
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            self.0 = value;
            value
        }

        fn bytes(&mut self, len: usize) -> Vec<u8> {
            (0..len).map(|_| self.next() as u8).collect()
        }

        fn bytes_array<const N: usize>(&mut self) -> [u8; N] {
            let mut bytes = [0; N];
            for byte in &mut bytes {
                *byte = self.next() as u8;
            }
            bytes
        }
    }
}

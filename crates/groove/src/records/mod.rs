//! Compact binary record descriptors, layout, and encoded row access.
//!
//! A [`RecordDescriptor`] is a named, declaration-ordered set of value types.
//! Public APIs use logical declaration-order indices. The encoded bytes use an
//! internal physical order with fixed-width fields first and variable-width
//! fields second; that physical space is represented only by the private
//! [`PhysicalFieldIdx`] newtype and folded into the descriptor layout cache.
//! Records only store bytes; names and types live in the descriptor.
//!
//! This module owns descriptor construction, layout validation, encoding,
//! decoding, typed field accessors, projection, patching, and owned/borrowed
//! record wrappers. Logical values and value-type encoding helpers live in
//! [`values`]; generated typed row wrappers live in [`macros`]. Schemas decide
//! which descriptors to build, and storage only sees encoded bytes.
//!
//! Multi-byte scalars and offsets are little-endian. All offsets are `u32`
//! byte positions from the start of the enclosing record or array.
//!
//! Fixed-only records are simple concatenation:
//!
//! ```text
//! descriptor: [id: u64, active: bool]
//!
//! +----------+--------+
//! | id: u64  | active |
//! | 8 bytes  | 1 byte |
//! +----------+--------+
//! ```
//!
//! Mixed fixed and variable records place fixed values first, then enough
//! offsets to find the ends of all but the final variable value, then payloads:
//!
//! ```text
//! descriptor: [id: u64, active: bool, name: string, blob: bytes]
//!
//! +----------+--------+-----------------+-------------+-------------+
//! | id: u64  | active | name_end: u32   | name bytes  | blob bytes  |
//! | fixed    | fixed  | first var end   | variable #1 | variable #2 |
//! +----------+--------+-----------------+-------------+-------------+
//!                                                     ^ name_end points here
//! ```
//!
//! The first variable value starts immediately after the fixed fields and offset
//! table. The final variable value ends at the end of the enclosing record, so
//! its offset is implicit.
//!
//! Nullable values are encoded as a flag byte plus payload when present. Null
//! fixed-width values reserve their normal width with zero bytes; null variable
//! values are just the flag byte.
//!
//! ```text
//! nullable u16 present       nullable u16 null       nullable string null
//!
//! +------+---------+         +------+----------+     +------+
//! | 0x01 | u16     |         | 0x00 | 00 00    |     | 0x00 |
//! +------+---------+         +------+----------+     +------+
//! ```
//!
//! Fixed-width arrays concatenate elements and infer length from payload size:
//!
//! ```text
//! array<u16> [10, 20, 30]
//!
//! +---------+---------+---------+
//! | u16 #1  | u16 #2  | u16 #3  |
//! +---------+---------+---------+
//! ```
//!
//! Variable-width arrays store element count, offsets for all but the final
//! element, then payloads:
//!
//! ```text
//! array<string> ["a", "bop", "c"]
//!
//! +------------+---------------+---------------+-----+-------+-----+
//! | count: u32 | elem2_end:u32 | elem3_end:u32 | "a" | "bop" | "c" |
//! +------------+---------------+---------------+-----+-------+-----+
//!                                                    ^ elem2_end points here
//! ```

pub mod macros;
mod values;

use std::ops::Deref;
use std::str;

use bytes::BytesMut;
use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

pub use macros::{FieldKind, RecordField, assert_record_field_layout};
pub use values::{EnumSchema, Value, ValueType};

use values::{
    checked_add, decode_value, encode_fixed_value, encode_value, ensure_value_type, usize_to_u32,
    validate_schema_value_type, write_u32,
};

/// Encodes one standalone value with [`encode_value`], outside any record.
///
/// * `value` — the logical value to encode.
/// * `value_type` — the type to encode it as; the pair must match.
///
/// This is the crate-visible door into the private value encoders, for
/// callers that need a single field's bytes (for example key building)
/// without laying out a whole record.
pub(crate) fn encode_single_field_value(
    value: &Value,
    value_type: &ValueType,
) -> Result<Vec<u8>, Error> {
    encode_value(value, value_type)
}

/// Interned schema-side description needed to interpret compact record bytes.
///
/// Equality and hashing are intern-handle based; deterministic code must not
/// use descriptors as ordered keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RecordDescriptor(Intern<RecordDescriptorData>);

impl RecordDescriptor {
    /// Builds a descriptor from `(name, type)` pairs in declaration order.
    ///
    /// * `fields` — one `(field name, field type)` pair per field, for
    ///   example `[("id", ValueType::U64), ("title", ValueType::String)]`.
    ///
    /// Descriptors are interned: building the same field list twice returns
    /// handles that compare equal and share one stored copy.
    pub fn new(fields: impl IntoIterator<Item = (impl Into<String>, ValueType)>) -> Self {
        let fields = fields
            .into_iter()
            .map(|(name, value_type)| DescriptorField {
                name: Some(name.into()),
                value_type,
            })
            .collect::<Vec<_>>();

        Self::from_logical_fields(fields)
    }

    /// All fields in logical (declaration) order.
    pub fn fields(&self) -> &[DescriptorField] {
        &self.fields
    }

    /// Finds the logical index of a field by name.
    ///
    /// * `field_name` — the declared name, for example `"title"`.
    ///
    /// Returns `None` when no field has that name. Resolve names once and
    /// keep the index around: the per-field accessors all take indices.
    pub fn field_index(&self, field_name: &str) -> Option<usize> {
        self.fields
            .iter()
            .position(|field| field.name.as_deref() == Some(field_name))
    }

    /// Encodes one record from its logical-order values.
    ///
    /// * `values` — one [`Value`] per field, in declaration order; the count
    ///   and each type must match the descriptor.
    ///
    /// Values are validated first, then written in physical order: fixed
    /// fields, then the offset table, then variable payloads (the module
    /// docs show the exact layout).
    ///
    /// ```text
    /// descriptor: [id: u64, name: string]
    /// create(&[Value::U64(13), "Yellow".into()]) ->
    /// [0d 00 00 00 00 00 00 00][59 65 6c 6c 6f 77]
    ///  id, fixed, 8 bytes       "Yellow" (last variable field: no offset)
    /// ```
    pub fn create(&self, values: &[Value]) -> Result<Vec<u8>, Error> {
        if self.fields.len() != values.len() {
            return Err(Error::ArityMismatch {
                expected: self.fields.len(),
                actual: values.len(),
            });
        }

        for (field, value) in self.fields.iter().zip(values) {
            ensure_value_type(value, &field.value_type)?;
        }

        let fixed_size = self.fixed_size();
        let variable_count = self.variable_count();
        let offset_table_size = variable_count.saturating_sub(1) * 4;
        let mut record = Vec::with_capacity(fixed_size + offset_table_size);
        let mut variable_values = Vec::with_capacity(variable_count);

        for logical_idx in &self.layout.logical_by_physical {
            let field = &self.fields[*logical_idx];
            let value = &values[*logical_idx];
            let layout = &self.layout.fields[*logical_idx];
            match layout {
                FieldLayout::Static { .. } => {
                    encode_fixed_value(&mut record, value, &field.value_type)?;
                }
                FieldLayout::Variable { .. } => {
                    variable_values.push(encode_value(value, &field.value_type)?);
                }
            }
        }

        let variable_start = fixed_size + offset_table_size;
        let mut next_offset = variable_start;
        for encoded in variable_values
            .iter()
            .take(variable_values.len().saturating_sub(1))
        {
            next_offset = checked_add(next_offset, encoded.len())?;
            write_u32(&mut record, usize_to_u32(next_offset)?);
        }

        for encoded in variable_values {
            record.extend(encoded);
        }

        Ok(record)
    }

    /// Builds a new descriptor and record by picking fields out of one or
    /// more source records.
    ///
    /// * `source_descriptors` — the descriptor of each source record,
    ///   position by position.
    /// * `source_records` — the encoded source records, in the same order.
    /// * `mapping` — one `(source record index, source field index)` pair
    ///   per output field, in output order. For example `[(0, 2), (1, 0)]`
    ///   builds a two-field record from field 2 of record 0 and field 0 of
    ///   record 1.
    ///
    /// Returns the derived output descriptor together with the encoded
    /// output record. Prefer [`Self::project_record`] when the output
    /// descriptor already exists.
    pub fn project(
        source_descriptors: &[RecordDescriptor],
        source_records: &[&[u8]],
        mapping: &[(usize, usize)],
    ) -> Result<(RecordDescriptor, Vec<u8>), Error> {
        let (fields, values) =
            project_fields_and_values(source_descriptors, source_records, mapping)?;
        let descriptor = RecordDescriptor::from_logical_fields(fields);
        let record = descriptor.create(&values)?;
        Ok((descriptor, record))
    }

    /// Encodes a record of *this* descriptor by picking fields from source
    /// records.
    ///
    /// The arguments are the same as [`Self::project`], with `self` playing
    /// the output descriptor. Each picked field is decoded to a [`Value`]
    /// and re-encoded; the `raw` variants below skip that round trip.
    pub fn project_record(
        &self,
        source_descriptors: &[RecordDescriptor],
        source_records: &[&[u8]],
        mapping: &[(usize, usize)],
    ) -> Result<Vec<u8>, Error> {
        let (_, values) = project_fields_and_values(source_descriptors, source_records, mapping)?;
        self.create(&values)
    }

    /// Like [`Self::project_record`], but copies each field's encoded byte
    /// span directly — no [`Value`] is ever materialized.
    ///
    /// * `source_descriptors` / `source_records` — sources, position by
    ///   position.
    /// * `mapping` — `mapping[i] = (source record index, source field
    ///   index)` for output field `i`. Mapped types must match exactly,
    ///   since bytes are copied verbatim.
    pub(crate) fn project_record_raw(
        &self,
        source_descriptors: &[RecordDescriptor],
        source_records: &[&[u8]],
        mapping: &[(usize, usize)],
    ) -> Result<Vec<u8>, Error> {
        if self.fields.len() != mapping.len() {
            return Err(Error::ArityMismatch {
                expected: self.fields.len(),
                actual: mapping.len(),
            });
        }
        if source_descriptors.len() != source_records.len() {
            return Err(Error::ArityMismatch {
                expected: source_descriptors.len(),
                actual: source_records.len(),
            });
        }

        let fixed_size = self.fixed_size();
        let variable_count = self.variable_count();
        let offset_table_size = variable_count.saturating_sub(1) * 4;
        let mut record = Vec::with_capacity(fixed_size + offset_table_size);
        let mut variable_values = Vec::with_capacity(variable_count);

        for logical_idx in &self.layout.logical_by_physical {
            let (source_descriptor_idx, source_field_idx) = mapping[*logical_idx];
            let source_descriptor = source_descriptors.get(source_descriptor_idx).ok_or(
                Error::FieldIndexOutOfBounds {
                    index: source_descriptor_idx,
                    len: source_descriptors.len(),
                },
            )?;
            let source_record =
                source_records
                    .get(source_descriptor_idx)
                    .ok_or(Error::FieldIndexOutOfBounds {
                        index: source_descriptor_idx,
                        len: source_records.len(),
                    })?;
            let source_field = source_descriptor.fields.get(source_field_idx).ok_or(
                Error::FieldIndexOutOfBounds {
                    index: source_field_idx,
                    len: source_descriptor.fields.len(),
                },
            )?;
            let output_field = &self.fields[*logical_idx];
            if source_field.value_type != output_field.value_type {
                return Err(Error::TypeMismatch {
                    expected: output_field.value_type.clone(),
                });
            }
            let span = source_descriptor.field_span(source_record, source_field_idx)?;
            let encoded = &source_record[span];
            match self.layout.fields[*logical_idx] {
                FieldLayout::Static { width, .. } => {
                    if encoded.len() != width {
                        return Err(Error::InvalidOffset);
                    }
                    record.extend_from_slice(encoded);
                }
                FieldLayout::Variable { .. } => variable_values.push(encoded),
            }
        }

        let variable_start = fixed_size + offset_table_size;
        let mut next_offset = variable_start;
        for encoded in variable_values
            .iter()
            .take(variable_values.len().saturating_sub(1))
        {
            next_offset = checked_add(next_offset, encoded.len())?;
            write_u32(&mut record, usize_to_u32(next_offset)?);
        }
        for encoded in variable_values {
            record.extend_from_slice(encoded);
        }

        Ok(record)
    }

    /// Like [`Self::project_record_raw`], but appends into a shared buffer
    /// instead of allocating a fresh `Vec` per record.
    ///
    /// * `source_descriptors` / `source_records` / `mapping` — as in
    ///   [`Self::project_record_raw`].
    /// * `output` — the shared buffer; the record is appended at the end and
    ///   its byte range inside `output` is returned.
    /// * `variable_scratch` — reusable work area for variable-width fields
    ///   (cleared here, so callers just pass the same vector every call).
    ///   Each entry remembers which source record owns the field and where
    ///   its bytes are, so payloads can be copied after the offset table is
    ///   written.
    ///
    /// The join runtime uses this to pack every output row of a batch into
    /// one allocation (see `JoinOutputBuffer` in `ivm/runtime/join.rs`).
    pub(crate) fn project_record_raw_into(
        &self,
        source_descriptors: &[RecordDescriptor],
        source_records: &[&[u8]],
        mapping: &[(usize, usize)],
        output: &mut BytesMut,
        variable_scratch: &mut Vec<(usize, std::ops::Range<usize>)>,
    ) -> Result<std::ops::Range<usize>, Error> {
        if self.fields.len() != mapping.len() {
            return Err(Error::ArityMismatch {
                expected: self.fields.len(),
                actual: mapping.len(),
            });
        }
        if source_descriptors.len() != source_records.len() {
            return Err(Error::ArityMismatch {
                expected: source_descriptors.len(),
                actual: source_records.len(),
            });
        }

        variable_scratch.clear();
        let start = output.len();
        let fixed_size = self.fixed_size();
        let variable_count = self.variable_count();
        let offset_table_size = variable_count.saturating_sub(1) * 4;
        output.reserve(fixed_size + offset_table_size);

        for logical_idx in &self.layout.logical_by_physical {
            let (source_descriptor_idx, source_field_idx) = mapping[*logical_idx];
            let source_descriptor = source_descriptors.get(source_descriptor_idx).ok_or(
                Error::FieldIndexOutOfBounds {
                    index: source_descriptor_idx,
                    len: source_descriptors.len(),
                },
            )?;
            let source_record =
                source_records
                    .get(source_descriptor_idx)
                    .ok_or(Error::FieldIndexOutOfBounds {
                        index: source_descriptor_idx,
                        len: source_records.len(),
                    })?;
            let source_field = source_descriptor.fields.get(source_field_idx).ok_or(
                Error::FieldIndexOutOfBounds {
                    index: source_field_idx,
                    len: source_descriptor.fields.len(),
                },
            )?;
            let output_field = &self.fields[*logical_idx];
            if source_field.value_type != output_field.value_type {
                return Err(Error::TypeMismatch {
                    expected: output_field.value_type.clone(),
                });
            }
            let span = source_descriptor.field_span(source_record, source_field_idx)?;
            let encoded = &source_record[span.clone()];
            match self.layout.fields[*logical_idx] {
                FieldLayout::Static { width, .. } => {
                    if encoded.len() != width {
                        return Err(Error::InvalidOffset);
                    }
                    output.extend_from_slice(encoded);
                }
                FieldLayout::Variable { .. } => {
                    variable_scratch.push((source_descriptor_idx, span));
                }
            }
        }

        let variable_start = fixed_size + offset_table_size;
        let mut next_offset = variable_start;
        for (_, span) in variable_scratch
            .iter()
            .take(variable_scratch.len().saturating_sub(1))
        {
            next_offset = checked_add(next_offset, span.end - span.start)?;
            output.extend_from_slice(&usize_to_u32(next_offset)?.to_le_bytes());
        }
        for (source_record_idx, span) in variable_scratch {
            output.extend_from_slice(&source_records[*source_record_idx][span.clone()]);
        }

        Ok(start..output.len())
    }

    /// Wraps encoded bytes in a zero-copy [`BorrowedRecord`] view.
    ///
    /// * `raw` — an encoded record created with this descriptor.
    pub fn bind<'a>(&'a self, raw: &'a [u8]) -> BorrowedRecord<'a> {
        BorrowedRecord::new(raw, self)
    }

    /// Wraps owned encoded bytes in a [`Record`] tied to this descriptor.
    ///
    /// * `raw` — an encoded record created with this descriptor.
    pub fn bind_owned<'a>(&'a self, raw: Vec<u8>) -> Record<'a> {
        Record::new(raw, self)
    }

    /// Decodes one field by name.
    ///
    /// * `record` — the encoded record bytes.
    /// * `field_name` — the declared field name, for example `"title"`.
    ///
    /// Convenience over [`Self::get_idx`]; it re-resolves the name on every
    /// call.
    pub fn get(&self, record: &[u8], field_name: &str) -> Result<Value, Error> {
        let field_idx = self
            .field_index(field_name)
            .ok_or_else(|| Error::FieldNotFound(field_name.to_owned()))?;

        self.get_idx(record, field_idx)
    }

    /// Decodes one field by logical index.
    ///
    /// * `record` — the encoded record bytes.
    /// * `field_idx` — the field's declaration-order index.
    ///
    /// Only the requested field is decoded; the rest of the record is never
    /// touched.
    pub fn get_idx(&self, record: &[u8], field_idx: usize) -> Result<Value, Error> {
        let field = self
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.fields.len(),
            })?;

        let span = self.field_span(record, field_idx)?;
        decode_value(&record[span.start..span.end], &field.value_type)
    }

    /// Returns where one field's encoded bytes live inside `record`.
    ///
    /// * `record` — the encoded record bytes.
    /// * `field_idx` — the field's declaration-order index.
    ///
    /// Fixed fields come straight from the cached layout; variable fields
    /// are located through the offset table.
    pub fn field_span(
        &self,
        record: &[u8],
        field_idx: usize,
    ) -> Result<std::ops::Range<usize>, Error> {
        record_value_span(record, self, field_idx)
    }

    /// Returns a copy of `record` with one field replaced.
    ///
    /// * `record` — the encoded record to start from.
    /// * `field_idx` — which field to replace.
    /// * `value` — the new value; it must match the field's declared type.
    ///
    /// A fixed-width field is patched in place in the copy. A variable-width
    /// field forces a full decode and re-encode, because the offsets of
    /// everything after it move.
    pub fn patch_field(
        &self,
        record: &[u8],
        field_idx: usize,
        value: &Value,
    ) -> Result<Vec<u8>, Error> {
        let field = self
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.fields.len(),
            })?;
        ensure_value_type(value, &field.value_type)?;
        let span = self.field_span(record, field_idx)?;
        if let FieldLayout::Static { width, .. } = self.layout.fields[field_idx]
            && span.end - span.start == width
        {
            let mut patched = record.to_vec();
            let mut encoded = Vec::with_capacity(width);
            encode_fixed_value(&mut encoded, value, &field.value_type)?;
            debug_assert_eq!(encoded.len(), width);
            patched[span].copy_from_slice(&encoded);
            return Ok(patched);
        }

        let mut values = self.bind(record).to_values()?;
        values[field_idx] = value.clone();
        self.create(&values)
    }

    /// Total width in bytes of the record's fixed prefix (all fixed fields).
    fn fixed_size(&self) -> usize {
        self.layout.fixed_size
    }

    /// How many variable-width fields the descriptor has.
    fn variable_count(&self) -> usize {
        self.layout.variable_count
    }

    /// Builds the descriptor and its layout cache from declaration-order
    /// fields.
    ///
    /// This is where physical order is decided: fixed-size fields are packed
    /// first, each at a precomputed offset, and variable-size fields follow
    /// in declaration order. Panics on a field type that
    /// [`validate_schema_value_type`] rejects — descriptors are built from
    /// schemas, and schemas are validated before ever reaching this point.
    fn from_logical_fields(fields: Vec<DescriptorField>) -> Self {
        for field in &fields {
            validate_schema_value_type(&field.value_type)
                .unwrap_or_else(|err| panic!("invalid record field {:?}: {err}", field.name));
        }
        let mut layout_fields = vec![
            FieldLayout::Variable {
                variable_idx: usize::MAX
            };
            fields.len()
        ];
        let mut fixed_size = 0usize;
        let mut variable_count = 0usize;
        let mut logical_by_physical = Vec::with_capacity(fields.len());

        let (fixed_logical, variable_logical): (Vec<_>, Vec<_>) = fields
            .iter()
            .enumerate()
            .partition(|(_, field)| field.value_type.is_fixed_size());
        for (physical_idx, (logical_idx, field)) in fixed_logical
            .into_iter()
            .chain(variable_logical)
            .enumerate()
        {
            let physical_idx = PhysicalFieldIdx(physical_idx);
            logical_by_physical.push(logical_idx);
            if let Some(width) = field.value_type.fixed_size() {
                let offset = fixed_size;
                fixed_size = fixed_size
                    .checked_add(width)
                    .expect("record descriptor fixed layout exceeds usize");
                layout_fields[logical_idx] = FieldLayout::Static { offset, width };
            } else {
                let _ = physical_idx;
                layout_fields[logical_idx] = FieldLayout::Variable {
                    variable_idx: variable_count,
                };
                variable_count += 1;
            }
        }

        Self(Intern::new(RecordDescriptorData {
            fields,
            layout: RecordLayout {
                fields: layout_fields,
                logical_by_physical,
                fixed_size,
                variable_count,
            },
        }))
    }
}

impl Default for RecordDescriptor {
    fn default() -> Self {
        Self::from_logical_fields(Vec::new())
    }
}

impl Deref for RecordDescriptor {
    type Target = RecordDescriptorData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Descriptors serialize as their field list only; the layout cache and the
// intern handle are rebuilt on deserialize.
impl Serialize for RecordDescriptor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.fields.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RecordDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = Vec::<DescriptorField>::deserialize(deserializer)?;
        Ok(Self::from_logical_fields(fields))
    }
}

/// The interned payload behind [`RecordDescriptor`]: the declared fields plus
/// the precomputed layout cache. Public only because `Deref` needs a public
/// target; use [`RecordDescriptor`] everywhere.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RecordDescriptorData {
    fields: Vec<DescriptorField>,
    layout: RecordLayout,
}

/// Precomputed layout answers for one descriptor, built once at intern time
/// so encoding and field lookup never recompute them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct RecordLayout {
    /// Layout of each field, indexed by logical (declaration) index.
    fields: Vec<FieldLayout>,
    /// Logical field indices in physical write order: all fixed fields
    /// first, then variable fields. Encoders walk this list to lay a record
    /// out.
    logical_by_physical: Vec<usize>,
    /// Total width in bytes of the fixed prefix.
    fixed_size: usize,
    /// Number of variable-width fields.
    variable_count: usize,
}

/// Index into physical (write) order, as opposed to the logical
/// (declaration) order the public API uses. The newtype exists so the two
/// index spaces cannot be mixed up by accident.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PhysicalFieldIdx(usize);

/// Where one field lives inside an encoded record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum FieldLayout {
    /// Fixed-width field: always `width` bytes starting at byte `offset`.
    Static { offset: usize, width: usize },
    /// Variable-width field: the `variable_idx`-th variable field, located
    /// through the offset table at read time.
    Variable { variable_idx: usize },
}

/// Borrowed zero-copy view over an encoded record.
///
/// Accessors decode only the requested field and do not allocate. Callers pass
/// field indices; resolve names once with [`RecordDescriptor::field_index`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BorrowedRecord<'a> {
    raw: &'a [u8],
    descriptor: RecordDescriptor,
}

/// Descriptor-validated encoded record projection.
///
/// Projection intentionally copies encoded field spans and rebuilds only the
/// target record framing; it never calls `get_idx` or materializes [`Value`]s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordProjector {
    source: RecordDescriptor,
    target: RecordDescriptor,
    target_to_source: Vec<usize>,
}

/// One output field's recipe in a raw (bytes-only) projection, used by
/// [`RecordDescriptor::project_raw_fields_into`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RawProjectionField {
    /// Copy the encoded bytes of source field `source_idx` unchanged.
    Copy { source_idx: usize },
    /// Take non-nullable source field `source_idx` and emit it as a present
    /// nullable: a `1` flag byte followed by the source bytes.
    WrapNullable { source_idx: usize },
    /// Emit source field `source_idx` as nullable, wrapping only when the
    /// source is not already nullable (an already-nullable source is copied
    /// as-is).
    FlattenNullable { source_idx: usize },
    /// Splice in pre-encoded bytes as-is, for example a NULL constant.
    Encoded { bytes: Vec<u8> },
}

/// Reusable buffers for raw projections, cleared at the start of every call
/// so one scratch value can serve a whole batch of rows.
#[derive(Debug, Default)]
pub(crate) struct RawProjectionScratch {
    /// Variable-width output fields in physical order, parked here until the
    /// offset table has been written and their payloads can follow it.
    variable_fields: Vec<RawProjectedBytes>,
    /// Bytes produced during projection (nullable flags, constants) that
    /// have no home in the source record.
    generated: BytesMut,
}

/// Where one projected field's bytes come from: a span of the source record,
/// or a span of the scratch `generated` buffer.
#[derive(Debug)]
enum RawProjectedBytes {
    Source(std::ops::Range<usize>),
    Generated(std::ops::Range<usize>),
}

impl RecordProjector {
    /// Builds a projector from source field indices to target field indices.
    ///
    /// * `source` — the descriptor every input record must have.
    /// * `target` — the descriptor of the produced records.
    /// * `mapping` — `(source field index, target field index)` pairs. Every
    ///   target field must be mapped exactly once, and each mapped pair must
    ///   have identical types (bytes are copied verbatim, never converted).
    pub fn new(
        source: RecordDescriptor,
        target: RecordDescriptor,
        mapping: impl IntoIterator<Item = (usize, usize)>,
    ) -> Result<Self, Error> {
        let mut target_to_source = vec![None; target.fields.len()];
        for (source_idx, target_idx) in mapping {
            let source_field =
                source
                    .fields
                    .get(source_idx)
                    .ok_or(Error::FieldIndexOutOfBounds {
                        index: source_idx,
                        len: source.fields.len(),
                    })?;
            let target_field =
                target
                    .fields
                    .get(target_idx)
                    .ok_or(Error::FieldIndexOutOfBounds {
                        index: target_idx,
                        len: target.fields.len(),
                    })?;
            if target_to_source[target_idx].is_some() {
                return Err(Error::ProjectDuplicateTarget { target_idx });
            }
            if source_field.value_type != target_field.value_type {
                return Err(Error::ProjectTypeMismatch {
                    source_idx,
                    target_idx,
                    source_type: source_field.value_type.clone(),
                    target_type: target_field.value_type.clone(),
                });
            }
            target_to_source[target_idx] = Some(source_idx);
        }

        let target_to_source = target_to_source
            .into_iter()
            .enumerate()
            .map(|(target_idx, source_idx)| {
                source_idx.ok_or(Error::ProjectMissingTarget { target_idx })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            source,
            target,
            target_to_source,
        })
    }

    /// Projects one source record into the target descriptor.
    ///
    /// * `record` — a borrowed view whose descriptor must be the projector's
    ///   `source` descriptor.
    ///
    /// Copies each mapped field's encoded span into a fresh target-layout
    /// record: fixed fields first, then the offset table, then variable
    /// payloads.
    pub fn project(&self, record: BorrowedRecord<'_>) -> Result<OwnedRecord, Error> {
        if record.descriptor != self.source {
            return Err(Error::ProjectSourceDescriptorMismatch);
        }

        let fixed_size = self.target.fixed_size();
        let variable_count = self.target.variable_count();
        let offset_table_size = variable_count.saturating_sub(1) * 4;
        let mut raw = Vec::with_capacity(checked_add(fixed_size, offset_table_size)?);

        for target_idx in &self.target.layout.logical_by_physical {
            let layout = self.target.layout.fields[*target_idx];
            let source_idx = self.target_to_source[*target_idx];
            let span = self.source.field_span(record.raw, source_idx)?;
            match layout {
                FieldLayout::Static { .. } => raw.extend_from_slice(&record.raw[span]),
                FieldLayout::Variable { .. } => {}
            }
        }

        let variable_start = fixed_size + offset_table_size;
        let mut next_offset = variable_start;
        if variable_count > 1 {
            for target_idx in &self.target.layout.logical_by_physical {
                let layout = self.target.layout.fields[*target_idx];
                let FieldLayout::Variable { variable_idx } = layout else {
                    continue;
                };
                if variable_idx + 1 == variable_count {
                    break;
                }
                let source_idx = self.target_to_source[*target_idx];
                let span = self.source.field_span(record.raw, source_idx)?;
                next_offset = checked_add(next_offset, span.end - span.start)?;
                write_u32(&mut raw, usize_to_u32(next_offset)?);
            }
        }
        for target_idx in &self.target.layout.logical_by_physical {
            let layout = self.target.layout.fields[*target_idx];
            if matches!(layout, FieldLayout::Variable { .. }) {
                let source_idx = self.target_to_source[*target_idx];
                let span = self.source.field_span(record.raw, source_idx)?;
                raw.extend_from_slice(&record.raw[span]);
            }
        }

        Ok(OwnedRecord::new(raw, self.target))
    }
}

impl RecordDescriptor {
    /// Builds one output record from per-field recipes, appending into a
    /// shared buffer.
    ///
    /// * `source` / `source_record` — the single source record the recipes
    ///   read from.
    /// * `fields` — one [`RawProjectionField`] recipe per output field, in
    ///   logical order.
    /// * `output` — shared output buffer; the new record's byte range inside
    ///   it is returned.
    /// * `scratch` — reusable buffers, cleared here.
    ///
    /// This is the bytes-only cousin of [`RecordProjector`] with two extra
    /// powers plain span copying cannot express: wrapping a value into a
    /// present nullable, and splicing in pre-encoded constants.
    pub(crate) fn project_raw_fields_into(
        &self,
        source: &RecordDescriptor,
        source_record: &[u8],
        fields: &[RawProjectionField],
        output: &mut BytesMut,
        scratch: &mut RawProjectionScratch,
    ) -> Result<std::ops::Range<usize>, Error> {
        if self.fields.len() != fields.len() {
            return Err(Error::ArityMismatch {
                expected: self.fields.len(),
                actual: fields.len(),
            });
        }

        scratch.variable_fields.clear();
        scratch.generated.clear();
        let start = output.len();
        let fixed_size = self.fixed_size();
        let variable_count = self.variable_count();
        let offset_table_size = variable_count.saturating_sub(1) * 4;
        output.reserve(fixed_size + offset_table_size);

        for target_idx in &self.layout.logical_by_physical {
            let layout = self.layout.fields[*target_idx];
            if matches!(layout, FieldLayout::Variable { .. }) {
                let bytes = self.raw_projected_field_bytes(
                    source,
                    source_record,
                    *target_idx,
                    fields[*target_idx].clone(),
                    scratch,
                )?;
                scratch.variable_fields.push(bytes);
                continue;
            }

            let bytes = self.raw_projected_field_bytes(
                source,
                source_record,
                *target_idx,
                fields[*target_idx].clone(),
                scratch,
            )?;
            match bytes {
                RawProjectedBytes::Source(span) => output.extend_from_slice(&source_record[span]),
                RawProjectedBytes::Generated(span) => {
                    output.extend_from_slice(&scratch.generated[span])
                }
            }
        }

        let variable_start = fixed_size + offset_table_size;
        let mut next_offset = variable_start;
        for bytes in scratch
            .variable_fields
            .iter()
            .take(scratch.variable_fields.len().saturating_sub(1))
        {
            next_offset = checked_add(next_offset, bytes.len())?;
            output.extend_from_slice(&usize_to_u32(next_offset)?.to_le_bytes());
        }
        for bytes in &scratch.variable_fields {
            match bytes {
                RawProjectedBytes::Source(span) => {
                    output.extend_from_slice(&source_record[span.clone()])
                }
                RawProjectedBytes::Generated(span) => {
                    output.extend_from_slice(&scratch.generated[span.clone()])
                }
            }
        }

        Ok(start..output.len())
    }

    /// Copies a record while unwrapping one nullable field to its inner
    /// type.
    ///
    /// * `source` / `source_record` — the input record. Every field except
    ///   `field_idx` must already have the output type; field counts must
    ///   match.
    /// * `field_idx` — the field to unwrap. When that field is nullable and
    ///   NULL, nothing is written and `Ok(None)` is returned so the caller
    ///   can drop the row. When present, its payload is copied without the
    ///   flag byte. A source field that is already non-nullable is copied
    ///   unchanged.
    /// * `output` / `scratch` — shared output buffer and reusable scratch,
    ///   as in [`Self::project_raw_fields_into`].
    ///
    /// Returns the new record's byte range in `output`, or `None` when the
    /// row was dropped because the field was NULL.
    pub(crate) fn unwrap_nullable_field_into(
        &self,
        source: &RecordDescriptor,
        source_record: &[u8],
        field_idx: usize,
        output: &mut BytesMut,
        scratch: &mut RawProjectionScratch,
    ) -> Result<Option<std::ops::Range<usize>>, Error> {
        if self.fields.len() != source.fields.len() {
            return Err(Error::ArityMismatch {
                expected: self.fields.len(),
                actual: source.fields.len(),
            });
        }
        let source_field = source
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: source.fields.len(),
            })?;
        let target_field = self
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.fields.len(),
            })?;
        let unwrap_inner = match &source_field.value_type {
            ValueType::Nullable(inner) => {
                if inner.as_ref() != &target_field.value_type {
                    return Err(Error::TypeMismatch {
                        expected: target_field.value_type.clone(),
                    });
                }
                Some(inner.as_ref())
            }
            value_type => {
                if value_type != &target_field.value_type {
                    return Err(Error::TypeMismatch {
                        expected: target_field.value_type.clone(),
                    });
                }
                None
            }
        };

        scratch.variable_fields.clear();
        scratch.generated.clear();
        let start = output.len();
        let fixed_size = self.fixed_size();
        let variable_count = self.variable_count();
        let offset_table_size = variable_count.saturating_sub(1) * 4;
        output.reserve(fixed_size + offset_table_size);

        for target_idx in &self.layout.logical_by_physical {
            let bytes = if *target_idx == field_idx {
                match unwrap_inner {
                    Some(inner) => {
                        let span = source.field_span(source_record, field_idx)?;
                        match nullable_present_payload(source_record, span, inner)? {
                            Some(payload) => RawProjectedBytes::Source(payload),
                            None => {
                                output.truncate(start);
                                scratch.variable_fields.clear();
                                scratch.generated.clear();
                                return Ok(None);
                            }
                        }
                    }
                    None => source
                        .field_span(source_record, field_idx)
                        .map(RawProjectedBytes::Source)?,
                }
            } else {
                let source_field =
                    source
                        .fields
                        .get(*target_idx)
                        .ok_or(Error::FieldIndexOutOfBounds {
                            index: *target_idx,
                            len: source.fields.len(),
                        })?;
                let target_field =
                    self.fields
                        .get(*target_idx)
                        .ok_or(Error::FieldIndexOutOfBounds {
                            index: *target_idx,
                            len: self.fields.len(),
                        })?;
                if source_field.value_type != target_field.value_type {
                    return Err(Error::TypeMismatch {
                        expected: target_field.value_type.clone(),
                    });
                }
                source
                    .field_span(source_record, *target_idx)
                    .map(RawProjectedBytes::Source)?
            };

            if matches!(
                self.layout.fields[*target_idx],
                FieldLayout::Variable { .. }
            ) {
                scratch.variable_fields.push(bytes);
            } else {
                match bytes {
                    RawProjectedBytes::Source(span) => {
                        output.extend_from_slice(&source_record[span])
                    }
                    RawProjectedBytes::Generated(span) => {
                        output.extend_from_slice(&scratch.generated[span])
                    }
                }
            }
        }

        let variable_start = fixed_size + offset_table_size;
        let mut next_offset = variable_start;
        for bytes in scratch
            .variable_fields
            .iter()
            .take(scratch.variable_fields.len().saturating_sub(1))
        {
            next_offset = checked_add(next_offset, bytes.len())?;
            output.extend_from_slice(&usize_to_u32(next_offset)?.to_le_bytes());
        }
        for bytes in &scratch.variable_fields {
            match bytes {
                RawProjectedBytes::Source(span) => {
                    output.extend_from_slice(&source_record[span.clone()])
                }
                RawProjectedBytes::Generated(span) => {
                    output.extend_from_slice(&scratch.generated[span.clone()])
                }
            }
        }

        Ok(Some(start..output.len()))
    }

    /// Resolves one recipe to the bytes it contributes — a source span or a
    /// freshly generated span — without writing them to the output yet.
    fn raw_projected_field_bytes(
        &self,
        source: &RecordDescriptor,
        source_record: &[u8],
        target_idx: usize,
        field: RawProjectionField,
        scratch: &mut RawProjectionScratch,
    ) -> Result<RawProjectedBytes, Error> {
        let target_field = self
            .fields
            .get(target_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: target_idx,
                len: self.fields.len(),
            })?;
        match field {
            RawProjectionField::Copy { source_idx } => {
                let source_field =
                    source
                        .fields
                        .get(source_idx)
                        .ok_or(Error::FieldIndexOutOfBounds {
                            index: source_idx,
                            len: source.fields.len(),
                        })?;
                if source_field.value_type != target_field.value_type {
                    return Err(Error::TypeMismatch {
                        expected: target_field.value_type.clone(),
                    });
                }
                source
                    .field_span(source_record, source_idx)
                    .map(RawProjectedBytes::Source)
            }
            RawProjectionField::WrapNullable { source_idx } => self.wrap_nullable_field_bytes(
                source,
                source_record,
                source_idx,
                target_idx,
                scratch,
            ),
            RawProjectionField::FlattenNullable { source_idx } => {
                let source_field =
                    source
                        .fields
                        .get(source_idx)
                        .ok_or(Error::FieldIndexOutOfBounds {
                            index: source_idx,
                            len: source.fields.len(),
                        })?;
                if source_field.value_type == target_field.value_type
                    && matches!(source_field.value_type, ValueType::Nullable(_))
                {
                    return source
                        .field_span(source_record, source_idx)
                        .map(RawProjectedBytes::Source);
                }
                self.wrap_nullable_field_bytes(
                    source,
                    source_record,
                    source_idx,
                    target_idx,
                    scratch,
                )
            }
            RawProjectionField::Encoded { bytes } => {
                let start = scratch.generated.len();
                scratch.generated.extend_from_slice(&bytes);
                Ok(RawProjectedBytes::Generated(start..scratch.generated.len()))
            }
        }
    }

    /// Writes `[1] + source field bytes` into the scratch buffer: the source
    /// field re-encoded as a present nullable. The target field's type must
    /// be `Nullable(source field's type)`.
    fn wrap_nullable_field_bytes(
        &self,
        source: &RecordDescriptor,
        source_record: &[u8],
        source_idx: usize,
        target_idx: usize,
        scratch: &mut RawProjectionScratch,
    ) -> Result<RawProjectedBytes, Error> {
        let source_field = source
            .fields
            .get(source_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: source_idx,
                len: source.fields.len(),
            })?;
        let target_field = self
            .fields
            .get(target_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: target_idx,
                len: self.fields.len(),
            })?;
        let ValueType::Nullable(inner) = &target_field.value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::Nullable(Box::new(source_field.value_type.clone())),
            });
        };
        if inner.as_ref() != &source_field.value_type {
            return Err(Error::TypeMismatch {
                expected: target_field.value_type.clone(),
            });
        }
        let source_span = source.field_span(source_record, source_idx)?;
        let start = scratch.generated.len();
        scratch.generated.extend_from_slice(&[1]);
        scratch
            .generated
            .extend_from_slice(&source_record[source_span]);
        Ok(RawProjectedBytes::Generated(start..scratch.generated.len()))
    }
}

impl RawProjectedBytes {
    /// Byte length of the span, wherever it lives.
    fn len(&self) -> usize {
        match self {
            Self::Source(span) | Self::Generated(span) => span.end - span.start,
        }
    }
}

/// Splits an encoded nullable field into its payload.
///
/// * `record` — the whole encoded record.
/// * `span` — where the nullable field lives inside `record`.
/// * `inner` — the inner (present) type, used to validate NULL padding.
///
/// Returns `Some(payload range)` when the value is present, `None` when it
/// is NULL. Corrupt flags or padding fail with the usual decode errors.
fn nullable_present_payload(
    record: &[u8],
    span: std::ops::Range<usize>,
    inner: &ValueType,
) -> Result<Option<std::ops::Range<usize>>, Error> {
    let bytes = &record[span.clone()];
    let Some((&flag, payload)) = bytes.split_first() else {
        return Err(Error::UnexpectedEof);
    };
    match flag {
        0 => {
            if inner.fixed_size().is_some() {
                if payload.iter().any(|byte| *byte != 0) {
                    return Err(Error::InvalidOffset);
                }
            } else if !payload.is_empty() {
                return Err(Error::InvalidOffset);
            }
            Ok(None)
        }
        1 => Ok(Some(span.start + 1..span.end)),
        value => Err(Error::InvalidNullFlag(value)),
    }
}

impl<'a> BorrowedRecord<'a> {
    /// Wraps encoded bytes with the descriptor that describes them.
    ///
    /// * `raw` — an encoded record created with `descriptor`.
    /// * `descriptor` — the record's descriptor.
    pub fn new(raw: &'a [u8], descriptor: &'a RecordDescriptor) -> Self {
        Self {
            raw,
            descriptor: *descriptor,
        }
    }

    /// The underlying encoded bytes.
    pub fn bytes(&self) -> &'a [u8] {
        self.raw
    }

    /// The underlying encoded bytes (alias of [`Self::bytes`]).
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// The descriptor these bytes belong to.
    pub fn descriptor(&self) -> RecordDescriptor {
        self.descriptor
    }

    /// Decodes one field by name; see [`RecordDescriptor::get`].
    pub fn get(&self, field_name: &str) -> Result<Value, Error> {
        self.descriptor.get(self.raw, field_name)
    }

    /// Decodes one field by logical index; see [`RecordDescriptor::get_idx`].
    pub fn get_idx(&self, field_idx: usize) -> Result<Value, Error> {
        self.descriptor.get_idx(self.raw, field_idx)
    }

    /// Decodes every field into owned [`Value`]s, in declaration order.
    pub fn to_values(&self) -> Result<Vec<Value>, Error> {
        let spans = record_value_spans(self.raw, &self.descriptor)?;
        self.descriptor
            .fields
            .iter()
            .zip(spans)
            .map(|(field, span)| decode_value(&self.raw[span.start..span.end], &field.value_type))
            .collect()
    }

    // The typed getters below read one field straight out of the encoded
    // bytes without building a `Value`. They all take the field's
    // declaration-order index, and the field's declared type must be exactly
    // the type in the getter's name (`get_u64` requires `ValueType::U64`,
    // `get_nullable_u64` requires `ValueType::Nullable(U64)`, and so on).

    /// Reads a `U64` field.
    pub fn get_u64(&self, field_idx: usize) -> Result<u64, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::U64)?;
        read_exact_array::<8>(bytes).map(u64::from_le_bytes)
    }

    /// Reads an `I64` field.
    pub fn get_i64(&self, field_idx: usize) -> Result<i64, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::I64)?;
        read_exact_array::<8>(bytes).map(i64::from_le_bytes)
    }

    /// Reads an `F64` field. A stored NaN is corrupt data and fails with
    /// [`Error::InvalidF64NaN`].
    pub fn get_f64(&self, field_idx: usize) -> Result<f64, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::F64)?;
        let value = read_exact_array::<8>(bytes).map(f64::from_le_bytes)?;
        if value.is_nan() {
            return Err(Error::InvalidF64NaN);
        }
        Ok(value)
    }

    /// Reads a `U32` field.
    pub fn get_u32(&self, field_idx: usize) -> Result<u32, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::U32)?;
        read_exact_array::<4>(bytes).map(u32::from_le_bytes)
    }

    /// Reads a `U8` field.
    pub fn get_u8(&self, field_idx: usize) -> Result<u8, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::U8)?;
        bytes.first().copied().ok_or(Error::UnexpectedEof)
    }

    /// Reads a `Bool` field. Only the bytes `00` and `01` are valid.
    pub fn get_bool(&self, field_idx: usize) -> Result<bool, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::Bool)?;
        match bytes.first().copied().ok_or(Error::UnexpectedEof)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidBool(value)),
        }
    }

    /// Reads an `Enum` field's raw discriminant byte. Use
    /// [`Self::get_enum_name`] when the variant's name is wanted instead.
    pub fn get_enum(&self, field_idx: usize) -> Result<u8, Error> {
        let field = self
            .descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })?;
        if !matches!(field.value_type, ValueType::Enum(_)) {
            return Err(Error::TypeMismatch {
                expected: ValueType::U8,
            });
        }
        let span = self.descriptor.field_span(self.raw, field_idx)?;
        read_exact_array::<1>(&self.raw[span]).map(|bytes| bytes[0])
    }

    /// Reads an `Enum` field and resolves the discriminant to its declared
    /// variant name through the field's [`EnumSchema`].
    pub fn get_enum_name(&self, field_idx: usize) -> Result<&str, Error> {
        let field = self
            .descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })?;
        let ValueType::Enum(schema) = &field.value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::U8,
            });
        };
        schema.variant(self.get_enum(field_idx)?)
    }

    /// Reads a `Bytes` field, borrowing straight from the record.
    pub fn get_bytes(&self, field_idx: usize) -> Result<&'a [u8], Error> {
        self.field_bytes(field_idx, &ValueType::Bytes)
    }

    /// Reads a `Uuid` field.
    pub fn get_uuid(&self, field_idx: usize) -> Result<uuid::Uuid, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::Uuid)?;
        read_exact_array::<16>(bytes).map(uuid::Uuid::from_bytes)
    }

    /// Reads a `String` field, borrowing straight from the record.
    pub fn get_str(&self, field_idx: usize) -> Result<&'a str, Error> {
        str::from_utf8(self.field_bytes(field_idx, &ValueType::String)?)
            .map_err(|_| Error::InvalidUtf8)
    }

    /// Reads a `Nullable(U64)` field; `None` means NULL.
    pub fn get_nullable_u64(&self, field_idx: usize) -> Result<Option<u64>, Error> {
        self.nullable_field(field_idx, &ValueType::U64, |payload| {
            read_exact_array::<8>(payload).map(u64::from_le_bytes)
        })
    }

    /// Reads a `Nullable(I64)` field; `None` means NULL.
    pub fn get_nullable_i64(&self, field_idx: usize) -> Result<Option<i64>, Error> {
        self.nullable_field(field_idx, &ValueType::I64, |payload| {
            read_exact_array::<8>(payload).map(i64::from_le_bytes)
        })
    }

    /// Reads a `Nullable(F64)` field; `None` means NULL. A stored NaN fails
    /// with [`Error::InvalidF64NaN`].
    pub fn get_nullable_f64(&self, field_idx: usize) -> Result<Option<f64>, Error> {
        self.nullable_field(field_idx, &ValueType::F64, |payload| {
            let value = read_exact_array::<8>(payload).map(f64::from_le_bytes)?;
            if value.is_nan() {
                return Err(Error::InvalidF64NaN);
            }
            Ok(value)
        })
    }

    /// Reads a `Nullable(Enum)` field's discriminant; `None` means NULL.
    pub fn get_nullable_enum(&self, field_idx: usize) -> Result<Option<u8>, Error> {
        let field = self
            .descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })?;
        let ValueType::Nullable(inner) = &field.value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::Nullable(Box::new(ValueType::U8)),
            });
        };
        if !matches!(inner.as_ref(), ValueType::Enum(_)) {
            return Err(Error::TypeMismatch {
                expected: ValueType::Nullable(Box::new(ValueType::U8)),
            });
        }
        let span = self.descriptor.field_span(self.raw, field_idx)?;
        let bytes = &self.raw[span];
        let (&flag, payload) = bytes.split_first().ok_or(Error::UnexpectedEof)?;
        match flag {
            0 => {
                if payload.iter().any(|byte| *byte != 0) {
                    return Err(Error::InvalidOffset);
                }
                Ok(None)
            }
            1 => read_exact_array::<1>(payload).map(|bytes| Some(bytes[0])),
            value => Err(Error::InvalidNullFlag(value)),
        }
    }

    /// Reads a `Nullable(String)` field, borrowing; `None` means NULL.
    pub fn get_nullable_string(&self, field_idx: usize) -> Result<Option<&'a str>, Error> {
        self.nullable_field(field_idx, &ValueType::String, |payload| {
            str::from_utf8(payload).map_err(|_| Error::InvalidUtf8)
        })
    }

    /// Reads a `Nullable(Bytes)` field, borrowing; `None` means NULL.
    pub fn get_nullable_bytes(&self, field_idx: usize) -> Result<Option<&'a [u8]>, Error> {
        self.nullable_field(field_idx, &ValueType::Bytes, Ok)
    }

    /// Reads a `Nullable(Uuid)` field; `None` means NULL.
    pub fn get_nullable_uuid(&self, field_idx: usize) -> Result<Option<uuid::Uuid>, Error> {
        self.nullable_field(field_idx, &ValueType::Uuid, |payload| {
            read_exact_array::<16>(payload).map(uuid::Uuid::from_bytes)
        })
    }

    /// Decodes one element of an `Array` field without touching the others.
    ///
    /// * `field_idx` — the array field's declaration-order index.
    /// * `element_idx` — which element to decode, starting at `0`.
    ///
    /// Only arrays of fixed-size elements support random access (the element
    /// position is `element_idx * element width`); variable-size elements
    /// fail with [`Error::InvalidTupleMember`].
    pub fn get_array_element(&self, field_idx: usize, element_idx: usize) -> Result<Value, Error> {
        let field = self
            .descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })?;
        let ValueType::Array(element_type) = &field.value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::Array(Box::new(ValueType::Bytes)),
            });
        };
        let Some(element_size) = element_type.fixed_size() else {
            return Err(Error::InvalidTupleMember {
                member_type: element_type.as_ref().clone(),
            });
        };
        let span = self.descriptor.field_span(self.raw, field_idx)?;
        let array = &self.raw[span];
        if !array.len().is_multiple_of(element_size) {
            return Err(Error::UnexpectedEof);
        }
        let len = array.len() / element_size;
        if element_idx >= len {
            return Err(Error::FieldIndexOutOfBounds {
                index: element_idx,
                len,
            });
        }
        let start = element_idx
            .checked_mul(element_size)
            .ok_or(Error::LengthOverflow)?;
        let end = checked_add(start, element_size)?;
        let element = array.get(start..end).ok_or(Error::UnexpectedEof)?;
        decode_value(element, element_type)
    }

    /// The descriptor field at `field_idx`, bounds-checked.
    pub(crate) fn field(&self, field_idx: usize) -> Result<&DescriptorField, Error> {
        self.descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })
    }

    /// A field's raw encoded bytes with only a bounds check — no type check.
    /// "Unchecked" means the caller already knows what type lives there.
    pub(crate) fn field_bytes_unchecked(&self, field_idx: usize) -> Result<&'a [u8], Error> {
        let _ = self.field(field_idx)?;
        let span = self.descriptor.field_span(self.raw, field_idx)?;
        Ok(&self.raw[span])
    }

    /// A field's raw encoded bytes, after checking that its declared type is
    /// exactly `expected`.
    fn field_bytes(&self, field_idx: usize, expected: &ValueType) -> Result<&'a [u8], Error> {
        let field = self
            .descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })?;
        if &field.value_type != expected {
            return Err(Error::TypeMismatch {
                expected: expected.clone(),
            });
        }
        let span = self.descriptor.field_span(self.raw, field_idx)?;
        Ok(&self.raw[span])
    }

    /// Shared decode path for the nullable getters: checks the flag byte,
    /// validates NULL padding, and hands the payload to `decode` when the
    /// value is present.
    fn nullable_field<T>(
        &self,
        field_idx: usize,
        inner: &ValueType,
        decode: impl FnOnce(&'a [u8]) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let bytes = self.nullable_field_bytes(field_idx, inner)?;
        let (&flag, payload) = bytes.split_first().ok_or(Error::UnexpectedEof)?;
        match flag {
            0 => {
                if matches!(
                    self.descriptor.layout.fields[field_idx],
                    FieldLayout::Static { .. }
                ) {
                    if payload.iter().any(|byte| *byte != 0) {
                        return Err(Error::InvalidOffset);
                    }
                } else if !payload.is_empty() {
                    return Err(Error::InvalidOffset);
                }
                Ok(None)
            }
            1 => decode(payload).map(Some),
            value => Err(Error::InvalidNullFlag(value)),
        }
    }

    /// Like [`Self::field_bytes`], but for fields declared
    /// `Nullable(inner)`.
    fn nullable_field_bytes(&self, field_idx: usize, inner: &ValueType) -> Result<&'a [u8], Error> {
        let field = self
            .descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })?;
        match &field.value_type {
            ValueType::Nullable(actual) if actual.as_ref() == inner => {}
            _ => {
                return Err(Error::TypeMismatch {
                    expected: ValueType::Nullable(Box::new(inner.clone())),
                });
            }
        }
        let span = self.descriptor.field_span(self.raw, field_idx)?;
        Ok(&self.raw[span])
    }
}

/// Decodes the mapped fields out of the source records: the shared front
/// half of the value-based projection APIs.
///
/// Returns the output field list (canonicalized through descriptor
/// interning) and the decoded values, both in output order.
fn project_fields_and_values(
    source_descriptors: &[RecordDescriptor],
    source_records: &[&[u8]],
    mapping: &[(usize, usize)],
) -> Result<(Vec<DescriptorField>, Vec<Value>), Error> {
    if source_descriptors.len() != source_records.len() {
        return Err(Error::ArityMismatch {
            expected: source_descriptors.len(),
            actual: source_records.len(),
        });
    }

    let mut selected = Vec::with_capacity(mapping.len());

    for &(descriptor_idx, field_idx) in mapping {
        let descriptor =
            source_descriptors
                .get(descriptor_idx)
                .ok_or(Error::FieldIndexOutOfBounds {
                    index: descriptor_idx,
                    len: source_descriptors.len(),
                })?;
        let field = descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: descriptor.fields.len(),
            })?;

        selected.push((
            field.clone(),
            descriptor.get_idx(source_records[descriptor_idx], field_idx)?,
        ));
    }

    let mut fields = Vec::with_capacity(selected.len());
    let mut values = Vec::with_capacity(selected.len());
    for (field, value) in &selected {
        fields.push(field.clone());
        values.push(value.clone());
    }

    let descriptor = RecordDescriptor::from_logical_fields(fields);
    Ok((descriptor.fields.clone(), values))
}

/// Byte range of one field inside an encoded record. A plain `start`/`end`
/// pair instead of `Range<usize>` so it stays `Copy`.
#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
}

/// Locates every field of `record`, in logical order.
fn record_value_spans(record: &[u8], descriptor: &RecordDescriptor) -> Result<Vec<Span>, Error> {
    validate_record_header(record, descriptor)?;
    descriptor
        .layout
        .fields
        .iter()
        .map(|layout| record_value_span_for_layout(record, descriptor, *layout))
        .collect()
}

/// Locates one field of `record`; the body behind
/// [`RecordDescriptor::field_span`].
fn record_value_span(
    record: &[u8],
    descriptor: &RecordDescriptor,
    field_idx: usize,
) -> Result<std::ops::Range<usize>, Error> {
    let layout = descriptor
        .layout
        .fields
        .get(field_idx)
        .ok_or(Error::FieldIndexOutOfBounds {
            index: field_idx,
            len: descriptor.fields.len(),
        })?;

    validate_record_header(record, descriptor)?;
    let span = record_value_span_for_layout(record, descriptor, *layout)?;
    Ok(span.start..span.end)
}

/// Cheap structural checks before reading any span: the record must be long
/// enough for its fixed prefix plus offset table, and a record with no
/// variable fields must be exactly its fixed size.
fn validate_record_header(record: &[u8], descriptor: &RecordDescriptor) -> Result<(), Error> {
    let fixed_size = descriptor.fixed_size();
    let variable_count = descriptor.variable_count();
    let offset_table_size = variable_count.saturating_sub(1) * 4;
    let variable_start = checked_add(fixed_size, offset_table_size)?;

    if record.len() < variable_start {
        return Err(Error::UnexpectedEof);
    }
    if variable_count == 0 && record.len() != fixed_size {
        return Err(Error::InvalidOffset);
    }
    Ok(())
}

/// Resolves one field layout to concrete byte positions inside `record`.
///
/// A fixed field comes straight from its cached offset and width. A variable
/// field starts where the previous variable field ends (the first one starts
/// right after the offset table) and ends at its offset-table entry (the
/// last one ends at the end of the record).
fn record_value_span_for_layout(
    record: &[u8],
    descriptor: &RecordDescriptor,
    layout: FieldLayout,
) -> Result<Span, Error> {
    match layout {
        FieldLayout::Static { offset, width } => {
            let end = checked_add(offset, width)?;
            if record.len() < end {
                return Err(Error::UnexpectedEof);
            }
            Ok(Span { start: offset, end })
        }
        FieldLayout::Variable { variable_idx } => {
            let fixed_size = descriptor.fixed_size();
            let variable_count = descriptor.variable_count();
            let offset_table_size = variable_count.saturating_sub(1) * 4;
            let variable_start = checked_add(fixed_size, offset_table_size)?;
            let start = if variable_idx == 0 {
                variable_start
            } else {
                u32_to_usize(read_u32_at(record, fixed_size + (variable_idx - 1) * 4)?)?
            };
            let end = if variable_idx + 1 == variable_count {
                record.len()
            } else {
                u32_to_usize(read_u32_at(record, fixed_size + variable_idx * 4)?)?
            };
            if end < start || end > record.len() {
                return Err(Error::InvalidOffset);
            }
            Ok(Span { start, end })
        }
    }
}

/// Converts a slice into `[u8; N]`, distinguishing "too short"
/// ([`Error::UnexpectedEof`]) from "wrong size" ([`Error::InvalidOffset`]).
fn read_exact_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
    bytes.try_into().map_err(|_| {
        if bytes.len() < N {
            Error::UnexpectedEof
        } else {
            Error::InvalidOffset
        }
    })
}

/// Reads one little-endian `u32` at byte `offset`, bounds-checked.
fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    let end = checked_add(offset, 4)?;
    let slice = bytes.get(offset..end).ok_or(Error::UnexpectedEof)?;
    read_exact_array::<4>(slice).map(u32::from_le_bytes)
}

/// Widens a stored `u32` offset back to `usize`.
fn u32_to_usize(value: u32) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| Error::LengthOverflow)
}

/// One field in canonical record layout order.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct DescriptorField {
    /// The declared name, or `None` for synthesized positional fields.
    pub name: Option<String>,
    /// The field's type, which fixes both the accepted values and the byte
    /// layout.
    pub value_type: ValueType,
}

/// Owned encoded record tied to the descriptor that decodes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record<'a> {
    raw: Vec<u8>,
    descriptor: &'a RecordDescriptor,
}

/// Owned encoded record tied to an owned descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedRecord {
    raw: Vec<u8>,
    descriptor: RecordDescriptor,
}

impl OwnedRecord {
    /// Pairs owned encoded bytes with their descriptor. `raw` must be a
    /// record created with `descriptor`.
    pub fn new(raw: Vec<u8>, descriptor: RecordDescriptor) -> Self {
        Self { raw, descriptor }
    }

    /// The descriptor these bytes belong to.
    pub fn descriptor(&self) -> &RecordDescriptor {
        &self.descriptor
    }

    /// The encoded bytes.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Consumes the record, keeping only the encoded bytes.
    pub fn into_raw(self) -> Vec<u8> {
        self.raw
    }

    /// A zero-copy [`BorrowedRecord`] view over these bytes, for the typed
    /// getters.
    pub fn borrowed(&self) -> BorrowedRecord<'_> {
        BorrowedRecord::new(&self.raw, &self.descriptor)
    }

    /// Decodes one field by name; see [`RecordDescriptor::get`].
    pub fn get(&self, field_name: &str) -> Result<Value, Error> {
        self.borrowed().get(field_name)
    }

    /// Decodes one field by logical index; see [`RecordDescriptor::get_idx`].
    pub fn get_idx(&self, field_idx: usize) -> Result<Value, Error> {
        self.borrowed().get_idx(field_idx)
    }

    /// Decodes every field into owned [`Value`]s, in declaration order.
    pub fn to_values(&self) -> Result<Vec<Value>, Error> {
        self.borrowed().to_values()
    }
}

impl Serialize for OwnedRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        OwnedRecordSerde {
            descriptor: self.descriptor,
            raw: &self.raw,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OwnedRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = OwnedRecordSerdeOwned::deserialize(deserializer)?;
        Ok(Self::new(record.raw, record.descriptor))
    }
}

/// Serde shape of [`OwnedRecord`]: the descriptor (as its field list) plus
/// the raw bytes. Borrowing half, used when serializing.
#[derive(serde::Serialize)]
struct OwnedRecordSerde<'a> {
    descriptor: RecordDescriptor,
    raw: &'a [u8],
}

/// Owning half of the [`OwnedRecord`] serde shape, used when deserializing.
#[derive(serde::Deserialize)]
struct OwnedRecordSerdeOwned {
    descriptor: RecordDescriptor,
    raw: Vec<u8>,
}

impl<'a> Record<'a> {
    /// Pairs owned encoded bytes with a borrowed descriptor. `raw` must be a
    /// record created with `descriptor`.
    pub fn new(raw: Vec<u8>, descriptor: &'a RecordDescriptor) -> Self {
        Self { raw, descriptor }
    }

    /// The descriptor these bytes belong to.
    pub fn descriptor(&self) -> &'a RecordDescriptor {
        self.descriptor
    }

    /// The encoded bytes.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Consumes the record, keeping only the encoded bytes.
    pub fn into_raw(self) -> Vec<u8> {
        self.raw
    }

    /// A zero-copy [`BorrowedRecord`] view over these bytes, for the typed
    /// getters.
    pub fn borrowed(&self) -> BorrowedRecord<'_> {
        BorrowedRecord::new(&self.raw, self.descriptor)
    }

    /// Decodes one field by name; see [`RecordDescriptor::get`].
    pub fn get(&self, field_name: &str) -> Result<Value, Error> {
        self.descriptor.get(&self.raw, field_name)
    }

    /// Decodes one field by logical index; see [`RecordDescriptor::get_idx`].
    pub fn get_idx(&self, field_idx: usize) -> Result<Value, Error> {
        self.descriptor.get_idx(&self.raw, field_idx)
    }

    /// Decodes every field into owned [`Value`]s, in declaration order.
    pub fn to_values(&self) -> Result<Vec<Value>, Error> {
        self.borrowed().to_values()
    }
}

impl AsRef<[u8]> for Record<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.raw
    }
}

impl Deref for Record<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl AsRef<[u8]> for BorrowedRecord<'_> {
    fn as_ref(&self) -> &[u8] {
        self.raw
    }
}

impl Deref for BorrowedRecord<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.raw
    }
}

/// Everything that can go wrong building, encoding, decoding, or projecting
/// records. The `#[error]` string on each variant states exactly which rule
/// was violated.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("expected {expected} values, got {actual}")]
    ArityMismatch { expected: usize, actual: usize },
    #[error("field not found: {0}")]
    FieldNotFound(String),
    #[error("field index {index} out of bounds for descriptor length {len}")]
    FieldIndexOutOfBounds { index: usize, len: usize },
    #[error("invalid boolean byte: {0}")]
    InvalidBool(u8),
    #[error("invalid nullable flag byte: {0}")]
    InvalidNullFlag(u8),
    #[error("enum {name} has {variants} variants; maximum is 256")]
    EnumTooManyVariants { name: String, variants: usize },
    #[error("invalid enum discriminant {discriminant} for enum {enum_name}")]
    InvalidEnumDiscriminant { enum_name: String, discriminant: u8 },
    #[error("unknown enum variant {variant} for enum {enum_name}")]
    UnknownEnumVariant { enum_name: String, variant: String },
    #[error("invalid offset")]
    InvalidOffset,
    #[error("tuple members must be fixed-width, got {member_type:?}")]
    InvalidTupleMember { member_type: ValueType },
    #[error("invalid utf-8 string")]
    InvalidUtf8,
    #[error("NaN is not a valid f64 record value")]
    InvalidF64NaN,
    #[error("encoded length exceeds u32::MAX")]
    LengthOverflow,
    #[error("value does not match type {expected:?}")]
    TypeMismatch { expected: ValueType },
    #[error("projection target field {target_idx} was mapped more than once")]
    ProjectDuplicateTarget { target_idx: usize },
    #[error("projection target field {target_idx} has no source field")]
    ProjectMissingTarget { target_idx: usize },
    #[error(
        "projection source field {source_idx} type {source_type:?} does not match target field {target_idx} type {target_type:?}"
    )]
    ProjectTypeMismatch {
        source_idx: usize,
        target_idx: usize,
        source_type: ValueType,
        target_type: ValueType,
    },
    #[error("projection source record descriptor does not match projector source descriptor")]
    ProjectSourceDescriptorMismatch,
    #[error("unexpected end of record")]
    UnexpectedEof,
}

#[cfg(test)]
mod tests;

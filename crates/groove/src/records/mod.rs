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

/// Interned schema-side description needed to interpret compact record bytes.
///
/// Equality and hashing are intern-handle based; deterministic code must not
/// use descriptors as ordered keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RecordDescriptor(Intern<RecordDescriptorData>);

impl RecordDescriptor {
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

    pub fn fields(&self) -> &[DescriptorField] {
        &self.fields
    }

    pub fn field_index(&self, field_name: &str) -> Option<usize> {
        self.fields
            .iter()
            .position(|field| field.name.as_deref() == Some(field_name))
    }

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

    pub fn project_record(
        &self,
        source_descriptors: &[RecordDescriptor],
        source_records: &[&[u8]],
        mapping: &[(usize, usize)],
    ) -> Result<Vec<u8>, Error> {
        let (_, values) = project_fields_and_values(source_descriptors, source_records, mapping)?;
        self.create(&values)
    }

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

    pub fn bind<'a>(&'a self, raw: &'a [u8]) -> BorrowedRecord<'a> {
        BorrowedRecord::new(raw, self)
    }

    pub fn bind_owned<'a>(&'a self, raw: Vec<u8>) -> Record<'a> {
        Record::new(raw, self)
    }

    pub fn get(&self, record: &[u8], field_name: &str) -> Result<Value, Error> {
        let field_idx = self
            .field_index(field_name)
            .ok_or_else(|| Error::FieldNotFound(field_name.to_owned()))?;

        self.get_idx(record, field_idx)
    }

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

    pub fn field_span(
        &self,
        record: &[u8],
        field_idx: usize,
    ) -> Result<std::ops::Range<usize>, Error> {
        record_value_span(record, self, field_idx)
    }

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

    fn fixed_size(&self) -> usize {
        self.layout.fixed_size
    }

    fn variable_count(&self) -> usize {
        self.layout.variable_count
    }

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

#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RecordDescriptorData {
    fields: Vec<DescriptorField>,
    layout: RecordLayout,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct RecordLayout {
    fields: Vec<FieldLayout>,
    logical_by_physical: Vec<usize>,
    fixed_size: usize,
    variable_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PhysicalFieldIdx(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum FieldLayout {
    Static { offset: usize, width: usize },
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RawProjectionField {
    Copy { source_idx: usize },
    WrapNullable { source_idx: usize },
    FlattenNullable { source_idx: usize },
    Encoded { bytes: Vec<u8> },
}

#[derive(Debug, Default)]
pub(crate) struct RawProjectionScratch {
    variable_fields: Vec<RawProjectedBytes>,
    generated: BytesMut,
}

#[derive(Debug)]
enum RawProjectedBytes {
    Source(std::ops::Range<usize>),
    Generated(std::ops::Range<usize>),
}

impl RecordProjector {
    /// Build a projector from source field indices to target field indices.
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

    /// Project one source record into the target descriptor.
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
    fn len(&self) -> usize {
        match self {
            Self::Source(span) | Self::Generated(span) => span.end - span.start,
        }
    }
}

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
    pub fn new(raw: &'a [u8], descriptor: &'a RecordDescriptor) -> Self {
        Self {
            raw,
            descriptor: *descriptor,
        }
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.raw
    }

    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    pub fn descriptor(&self) -> RecordDescriptor {
        self.descriptor
    }

    pub fn get(&self, field_name: &str) -> Result<Value, Error> {
        self.descriptor.get(self.raw, field_name)
    }

    pub fn get_idx(&self, field_idx: usize) -> Result<Value, Error> {
        self.descriptor.get_idx(self.raw, field_idx)
    }

    pub fn to_values(&self) -> Result<Vec<Value>, Error> {
        let spans = record_value_spans(self.raw, &self.descriptor)?;
        self.descriptor
            .fields
            .iter()
            .zip(spans)
            .map(|(field, span)| decode_value(&self.raw[span.start..span.end], &field.value_type))
            .collect()
    }

    pub fn get_u64(&self, field_idx: usize) -> Result<u64, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::U64)?;
        read_exact_array::<8>(bytes).map(u64::from_le_bytes)
    }

    pub fn get_f64(&self, field_idx: usize) -> Result<f64, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::F64)?;
        let value = read_exact_array::<8>(bytes).map(f64::from_le_bytes)?;
        if value.is_nan() {
            return Err(Error::InvalidF64NaN);
        }
        Ok(value)
    }

    pub fn get_u32(&self, field_idx: usize) -> Result<u32, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::U32)?;
        read_exact_array::<4>(bytes).map(u32::from_le_bytes)
    }

    pub fn get_u8(&self, field_idx: usize) -> Result<u8, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::U8)?;
        bytes.first().copied().ok_or(Error::UnexpectedEof)
    }

    pub fn get_bool(&self, field_idx: usize) -> Result<bool, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::Bool)?;
        match bytes.first().copied().ok_or(Error::UnexpectedEof)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidBool(value)),
        }
    }

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

    pub fn get_bytes(&self, field_idx: usize) -> Result<&'a [u8], Error> {
        self.field_bytes(field_idx, &ValueType::Bytes)
    }

    pub fn get_uuid(&self, field_idx: usize) -> Result<uuid::Uuid, Error> {
        let bytes = self.field_bytes(field_idx, &ValueType::Uuid)?;
        read_exact_array::<16>(bytes).map(uuid::Uuid::from_bytes)
    }

    pub fn get_str(&self, field_idx: usize) -> Result<&'a str, Error> {
        str::from_utf8(self.field_bytes(field_idx, &ValueType::String)?)
            .map_err(|_| Error::InvalidUtf8)
    }

    pub fn get_nullable_u64(&self, field_idx: usize) -> Result<Option<u64>, Error> {
        self.nullable_field(field_idx, &ValueType::U64, |payload| {
            read_exact_array::<8>(payload).map(u64::from_le_bytes)
        })
    }

    pub fn get_nullable_f64(&self, field_idx: usize) -> Result<Option<f64>, Error> {
        self.nullable_field(field_idx, &ValueType::F64, |payload| {
            let value = read_exact_array::<8>(payload).map(f64::from_le_bytes)?;
            if value.is_nan() {
                return Err(Error::InvalidF64NaN);
            }
            Ok(value)
        })
    }

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

    pub fn get_nullable_string(&self, field_idx: usize) -> Result<Option<&'a str>, Error> {
        self.nullable_field(field_idx, &ValueType::String, |payload| {
            str::from_utf8(payload).map_err(|_| Error::InvalidUtf8)
        })
    }

    pub fn get_nullable_bytes(&self, field_idx: usize) -> Result<Option<&'a [u8]>, Error> {
        self.nullable_field(field_idx, &ValueType::Bytes, Ok)
    }

    pub fn get_nullable_uuid(&self, field_idx: usize) -> Result<Option<uuid::Uuid>, Error> {
        self.nullable_field(field_idx, &ValueType::Uuid, |payload| {
            read_exact_array::<16>(payload).map(uuid::Uuid::from_bytes)
        })
    }

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

    pub(crate) fn field(&self, field_idx: usize) -> Result<&DescriptorField, Error> {
        self.descriptor
            .fields
            .get(field_idx)
            .ok_or(Error::FieldIndexOutOfBounds {
                index: field_idx,
                len: self.descriptor.fields.len(),
            })
    }

    pub(crate) fn field_bytes_unchecked(&self, field_idx: usize) -> Result<&'a [u8], Error> {
        let _ = self.field(field_idx)?;
        let span = self.descriptor.field_span(self.raw, field_idx)?;
        Ok(&self.raw[span])
    }

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

#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
}

fn record_value_spans(record: &[u8], descriptor: &RecordDescriptor) -> Result<Vec<Span>, Error> {
    validate_record_header(record, descriptor)?;
    descriptor
        .layout
        .fields
        .iter()
        .map(|layout| record_value_span_for_layout(record, descriptor, *layout))
        .collect()
}

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

fn read_exact_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
    bytes.try_into().map_err(|_| {
        if bytes.len() < N {
            Error::UnexpectedEof
        } else {
            Error::InvalidOffset
        }
    })
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    let end = checked_add(offset, 4)?;
    let slice = bytes.get(offset..end).ok_or(Error::UnexpectedEof)?;
    read_exact_array::<4>(slice).map(u32::from_le_bytes)
}

fn u32_to_usize(value: u32) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| Error::LengthOverflow)
}

/// One field in canonical record layout order.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct DescriptorField {
    pub name: Option<String>,
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
    pub fn new(raw: Vec<u8>, descriptor: RecordDescriptor) -> Self {
        Self { raw, descriptor }
    }

    pub fn descriptor(&self) -> &RecordDescriptor {
        &self.descriptor
    }

    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    pub fn into_raw(self) -> Vec<u8> {
        self.raw
    }

    pub fn borrowed(&self) -> BorrowedRecord<'_> {
        BorrowedRecord::new(&self.raw, &self.descriptor)
    }

    pub fn get(&self, field_name: &str) -> Result<Value, Error> {
        self.borrowed().get(field_name)
    }

    pub fn get_idx(&self, field_idx: usize) -> Result<Value, Error> {
        self.borrowed().get_idx(field_idx)
    }

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

#[derive(serde::Serialize)]
struct OwnedRecordSerde<'a> {
    descriptor: RecordDescriptor,
    raw: &'a [u8],
}

#[derive(serde::Deserialize)]
struct OwnedRecordSerdeOwned {
    descriptor: RecordDescriptor,
    raw: Vec<u8>,
}

impl<'a> Record<'a> {
    pub fn new(raw: Vec<u8>, descriptor: &'a RecordDescriptor) -> Self {
        Self { raw, descriptor }
    }

    pub fn descriptor(&self) -> &'a RecordDescriptor {
        self.descriptor
    }

    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    pub fn into_raw(self) -> Vec<u8> {
        self.raw
    }

    pub fn borrowed(&self) -> BorrowedRecord<'_> {
        BorrowedRecord::new(&self.raw, self.descriptor)
    }

    pub fn get(&self, field_name: &str) -> Result<Value, Error> {
        self.descriptor.get(&self.raw, field_name)
    }

    pub fn get_idx(&self, field_idx: usize) -> Result<Value, Error> {
        self.descriptor.get_idx(&self.raw, field_idx)
    }

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

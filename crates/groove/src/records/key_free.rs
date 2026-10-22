//! Stored rows of a keyed table leave out the primary-key fields.
//!
//! A table row is stored under its encoded primary key, so a copy of the key
//! columns in the stored value is redundant. [`KeyFreeLayout`] maps between a
//! table variant's full record (every field, as reads, IVM operators and
//! writes see it) and its stored record (the same fields without the key
//! fields): writes drop the key fields, reads put them back at their declared
//! positions from the decoded key. Every other field is copied as encoded
//! bytes and never decoded.

use super::{Error, FieldLayout, RecordDescriptor, Value};

/// How one fixed-width primary-key part is encoded in a row's key: a tag
/// byte, then `width` value bytes that become the field's bytes in `order`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedKeyPart {
    pub tag: u8,
    pub width: usize,
    pub order: KeyByteOrder,
}

/// How a key part's value bytes map to its record field's bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyByteOrder {
    /// The same bytes.
    Same,
    /// The bytes reversed (a big-endian key, a little-endian field).
    Reversed,
}

/// Fixed-width key parts at fixed key offsets, by key part.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EncodedKeyPlan {
    /// Each part's tag offset in the key and encoding.
    parts: Vec<(usize, EncodedKeyPart)>,
    /// The length of every key.
    len: usize,
}

/// Where one field of the full record comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldSource {
    /// Field `n` of the stored record.
    Stored(usize),
    /// Part `n` of the row's primary key.
    Key(usize),
}

/// The full and stored descriptors of one keyed table variant, and how their
/// fields correspond.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyFreeLayout {
    full: RecordDescriptor,
    stored: RecordDescriptor,
    /// One entry per full field.
    sources: Vec<FieldSource>,
    /// The full field index of each stored field.
    full_by_stored: Vec<usize>,
    /// The full field index of each primary-key part, in key order.
    key_fields: Vec<usize>,
    /// Byte plan between the two records.
    plan: Option<RebuildPlan>,
    /// Key byte plan, set by [`Self::with_encoded_key`].
    encoded_key: Option<EncodedKeyPlan>,
}

/// How the two records' bytes correspond, compiled once. Both have a fixed
/// region (static fields in field order), an offset table (the end of each
/// variable field but the last) and the variable fields in field order. So
/// either direction copies runs of fixed bytes and runs of variable bytes,
/// adjusts the copied offsets, and writes or skips the key fields, with no
/// per-field span lookups.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RebuildPlan {
    /// The full fixed region in order.
    fixed: Vec<FixedPiece>,
    /// The full variable fields in order.
    variable: Vec<VariablePiece>,
    /// Upper bound of key field bytes beyond the stored record's length.
    key_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FixedPiece {
    /// Stored fixed bytes `start..start + len`, at `full_start` in the full
    /// record.
    Stored {
        start: usize,
        len: usize,
        full_start: usize,
    },
    /// Key part `part`, the full field `index`, `width` bytes wide.
    Key {
        part: usize,
        index: usize,
        width: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VariablePiece {
    /// `count` consecutive variable fields, stored variable field
    /// `stored_first` onward and full variable field `full_first` onward.
    Stored {
        stored_first: usize,
        full_first: usize,
        count: usize,
    },
    /// Key part `part`, the full field `index`, full variable field
    /// `full_variable`.
    Key {
        part: usize,
        index: usize,
        full_variable: usize,
    },
}

impl RebuildPlan {
    fn new(full: RecordDescriptor, stored: RecordDescriptor, sources: &[FieldSource]) -> Self {
        let mut fixed = Vec::<FixedPiece>::new();
        let mut variable = Vec::<VariablePiece>::new();
        let mut key_bytes = 0;
        for &index in &full.layout.logical_by_physical {
            match (full.layout.fields[index], sources[index]) {
                (FieldLayout::Static { width, .. }, FieldSource::Key(part)) => {
                    key_bytes += width;
                    fixed.push(FixedPiece::Key { part, index, width });
                }
                (FieldLayout::Static { offset, width }, FieldSource::Stored(stored_index)) => {
                    let FieldLayout::Static { offset: start, .. } =
                        stored.layout.fields[stored_index]
                    else {
                        unreachable!("a stored field keeps its full field's type");
                    };
                    match fixed.last_mut() {
                        Some(FixedPiece::Stored {
                            start: run_start,
                            len,
                            ..
                        }) if *run_start + *len == start => *len += width,
                        _ => fixed.push(FixedPiece::Stored {
                            start,
                            len: width,
                            full_start: offset,
                        }),
                    }
                }
                (FieldLayout::Variable { variable_idx }, FieldSource::Key(part)) => {
                    // Room for an offset table entry and a short key.
                    key_bytes += 4 + 32;
                    variable.push(VariablePiece::Key {
                        part,
                        index,
                        full_variable: variable_idx,
                    });
                }
                (FieldLayout::Variable { variable_idx }, FieldSource::Stored(stored_index)) => {
                    let FieldLayout::Variable {
                        variable_idx: stored_variable,
                    } = stored.layout.fields[stored_index]
                    else {
                        unreachable!("a stored field keeps its full field's type");
                    };
                    match variable.last_mut() {
                        Some(VariablePiece::Stored {
                            stored_first,
                            full_first,
                            count,
                        }) if *stored_first + *count == stored_variable
                            && *full_first + *count == variable_idx =>
                        {
                            *count += 1
                        }
                        _ => variable.push(VariablePiece::Stored {
                            stored_first: stored_variable,
                            full_first: variable_idx,
                            count: 1,
                        }),
                    }
                }
            }
        }
        Self {
            fixed,
            variable,
            key_bytes,
        }
    }
}

/// The variable fields of one encoded record, checked the way a span read of
/// each would check them: the record holds its fixed region and offset
/// table, and the offsets never decrease and stay within the record.
struct VariableSpans<'a> {
    record: &'a [u8],
    fixed: usize,
    count: usize,
    header: usize,
}

impl<'a> VariableSpans<'a> {
    fn new(record: &'a [u8], descriptor: RecordDescriptor) -> Option<Self> {
        let fixed = descriptor.layout.fixed_size;
        let count = descriptor.layout.variable_count;
        let header = fixed + count.saturating_sub(1) * 4;
        if record.len() < header || (count == 0 && record.len() != fixed) {
            return None;
        }
        let mut previous = header;
        for entry in record[fixed..header].chunks_exact(4) {
            let offset = u32::from_le_bytes(entry.try_into().expect("4 bytes")) as usize;
            if offset < previous || offset > record.len() {
                return None;
            }
            previous = offset;
        }
        Some(Self {
            record,
            fixed,
            count,
            header,
        })
    }

    /// Where variable field `variable` ends.
    fn end(&self, variable: usize) -> usize {
        if variable + 1 == self.count {
            self.record.len()
        } else {
            let at = self.fixed + variable * 4;
            u32::from_le_bytes(self.record[at..at + 4].try_into().expect("4 bytes")) as usize
        }
    }

    /// Where variable field `variable` starts.
    fn start(&self, variable: usize) -> usize {
        if variable == 0 {
            self.header
        } else {
            self.end(variable - 1)
        }
    }
}

/// Record that full variable field `variable` of `count` ends where `output`
/// now ends, in the offset table at `table`. `None` past `u32` offsets.
fn set_variable_end(output: &mut [u8], table: usize, count: usize, variable: usize) -> Option<()> {
    if variable + 1 < count {
        let end = u32::try_from(output.len()).ok()?;
        let at = table + variable * 4;
        output[at..at + 4].copy_from_slice(&end.to_le_bytes());
    }
    Some(())
}

impl KeyFreeLayout {
    /// The layout of records whose full descriptor is `full` and whose stored
    /// form leaves out the fields at `key_fields` (the full field index of
    /// each primary-key part, in key order). `None` when `key_fields` is
    /// empty, so stored bytes already are the full record.
    pub fn new(full: RecordDescriptor, key_fields: &[usize]) -> Result<Option<Self>, Error> {
        if key_fields.is_empty() {
            return Ok(None);
        }
        let len = full.fields().len();
        for (position, &index) in key_fields.iter().enumerate() {
            if index >= len {
                return Err(Error::FieldIndexOutOfBounds { index, len });
            }
            if key_fields[..position].contains(&index) {
                return Err(Error::DuplicateKeyField { index });
            }
        }
        let mut sources = Vec::with_capacity(len);
        let mut full_by_stored = Vec::with_capacity(len);
        let mut stored_fields = Vec::with_capacity(len);
        for (index, field) in full.fields().iter().enumerate() {
            if let Some(part) = key_fields.iter().position(|key| *key == index) {
                sources.push(FieldSource::Key(part));
                continue;
            }
            sources.push(FieldSource::Stored(stored_fields.len()));
            full_by_stored.push(index);
            stored_fields.push(field.clone());
        }
        let stored = RecordDescriptor::new_with_fields(stored_fields);
        Ok(Some(Self {
            plan: Some(RebuildPlan::new(full, stored, &sources)),
            encoded_key: None,
            full,
            stored,
            sources,
            full_by_stored,
            key_fields: key_fields.to_vec(),
        }))
    }

    /// Let reads copy key fields straight from a row's encoded key whose
    /// parts are `parts`, in key order. Ignored unless every part has a
    /// fixed width matching its field's.
    pub fn with_encoded_key(mut self, parts: &[EncodedKeyPart]) -> Self {
        let Some(plan) = &self.plan else {
            return self;
        };
        if parts.len() != self.key_fields.len()
            || plan
                .variable
                .iter()
                .any(|piece| matches!(piece, VariablePiece::Key { .. }))
        {
            return self;
        }
        let widths_match = plan.fixed.iter().all(|piece| match *piece {
            FixedPiece::Key { part, width, .. } => parts[part].width == width,
            FixedPiece::Stored { .. } => true,
        });
        if !widths_match {
            return self;
        }
        let mut offset = 0;
        let parts = parts
            .iter()
            .map(|part| {
                let at = offset;
                offset += 1 + part.width;
                (at, *part)
            })
            .collect();
        self.encoded_key = Some(EncodedKeyPlan { parts, len: offset });
        self
    }

    /// The descriptor of the full record.
    pub fn full(&self) -> RecordDescriptor {
        self.full
    }

    /// The descriptor of the stored bytes.
    pub fn stored(&self) -> RecordDescriptor {
        self.stored
    }

    /// The full field index of each primary-key part, in key order.
    pub fn key_fields(&self) -> &[usize] {
        &self.key_fields
    }

    /// Rebuild the full record from stored bytes and the row's decoded
    /// primary-key parts, in key order.
    pub fn to_full(&self, stored: &[u8], key: &[Value]) -> Result<Vec<u8>, Error> {
        if key.len() != self.key_fields.len() {
            return Err(Error::ArityMismatch {
                expected: self.key_fields.len(),
                actual: key.len(),
            });
        }
        self.to_full_with(stored, |part, index, output| {
            self.full.encode_field_into(index, &key[part], output)
        })
    }

    /// Rebuild the full record from stored bytes, with `append_key(part,
    /// index, output)` appending the encoding of key part `part` as full
    /// field `index`. Reads that hold the row's encoded key use this to
    /// copy key parts across without decoding them to values.
    pub fn to_full_with<E: From<Error>>(
        &self,
        stored: &[u8],
        mut append_key: impl FnMut(usize, usize, &mut Vec<u8>) -> Result<(), E>,
    ) -> Result<Vec<u8>, E> {
        if let Some(full) = self.planned_to_full(stored, &mut append_key) {
            return full;
        }
        self.full.create_with_encoded_fields(
            stored.len() + 16 * self.key_fields.len(),
            |index, output| match self.sources[index] {
                FieldSource::Key(part) => append_key(part, index, output),
                FieldSource::Stored(stored_index) => {
                    let span = self.stored.field_span(stored, stored_index)?;
                    output.extend_from_slice(&stored[span]);
                    Ok(())
                }
            },
        )
    }

    /// [`Self::to_full_with`] by the byte plan. `None` when the stored bytes
    /// do not check out; the field-by-field rebuild then reports the
    /// precise error.
    #[inline]
    fn planned_to_full<E: From<Error>>(
        &self,
        stored: &[u8],
        append_key: &mut impl FnMut(usize, usize, &mut Vec<u8>) -> Result<(), E>,
    ) -> Option<Result<Vec<u8>, E>> {
        let plan = self.plan.as_ref()?;
        let spans = VariableSpans::new(stored, self.stored)?;
        let full_variables = self.full.layout.variable_count;
        let table = self.full.layout.fixed_size;
        let mut output = Vec::with_capacity(stored.len() + plan.key_bytes);
        for piece in &plan.fixed {
            match *piece {
                FixedPiece::Stored { start, len, .. } => {
                    output.extend_from_slice(&stored[start..start + len]);
                }
                FixedPiece::Key { part, index, width } => {
                    let start = output.len();
                    if let Err(error) = append_key(part, index, &mut output) {
                        return Some(Err(error));
                    }
                    if output.len() - start != width {
                        return Some(Err(Error::TypeMismatch {
                            expected: self.full.fields[index].value_type.clone(),
                        }
                        .into()));
                    }
                }
            }
        }
        output.resize(table + full_variables.saturating_sub(1) * 4, 0);
        for piece in &plan.variable {
            match *piece {
                VariablePiece::Stored {
                    stored_first,
                    full_first,
                    count,
                } => {
                    let start = spans.start(stored_first);
                    let base = output.len();
                    output.extend_from_slice(&stored[start..spans.end(stored_first + count - 1)]);
                    for field in 0..count {
                        let end = base + spans.end(stored_first + field) - start;
                        if full_first + field + 1 < full_variables {
                            let end = u32::try_from(end).ok()?;
                            let at = table + (full_first + field) * 4;
                            output[at..at + 4].copy_from_slice(&end.to_le_bytes());
                        }
                    }
                }
                VariablePiece::Key {
                    part,
                    index,
                    full_variable,
                } => {
                    if let Err(error) = append_key(part, index, &mut output) {
                        return Some(Err(error));
                    }
                    set_variable_end(&mut output, table, full_variables, full_variable)?;
                }
            }
        }
        Some(Ok(output))
    }

    /// Rebuild the full record from stored bytes and the row's encoded key
    /// with byte copies alone. `None` when the layout has no key byte plan
    /// or the bytes do not check out; the field-by-field rebuild then
    /// reports the precise error.
    #[inline]
    pub fn to_full_from_encoded_key(&self, stored: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let key_plan = self.encoded_key.as_ref()?;
        if key.len() != key_plan.len {
            return None;
        }
        self.planned_to_full(stored, &mut |part, _, output: &mut Vec<u8>| {
            let (at, part) = key_plan.parts[part];
            if key[at] != part.tag {
                return Err(KeyMismatch);
            }
            let bytes = &key[at + 1..at + 1 + part.width];
            match (part.order, part.width) {
                (KeyByteOrder::Same, _) => output.extend_from_slice(bytes),
                (KeyByteOrder::Reversed, 8) => output.extend_from_slice(
                    &u64::from_be_bytes(bytes.try_into().expect("8 bytes")).to_le_bytes(),
                ),
                (KeyByteOrder::Reversed, 4) => output.extend_from_slice(
                    &u32::from_be_bytes(bytes.try_into().expect("4 bytes")).to_le_bytes(),
                ),
                (KeyByteOrder::Reversed, _) => output.extend(bytes.iter().rev()),
            }
            Ok(())
        })?
        .ok()
    }

    fn planned_to_stored(&self, full: &[u8]) -> Option<Vec<u8>> {
        let plan = self.plan.as_ref()?;
        let spans = VariableSpans::new(full, self.full)?;
        let stored_variables = self.stored.layout.variable_count;
        let table = self.stored.layout.fixed_size;
        let mut output = Vec::with_capacity(full.len());
        for piece in &plan.fixed {
            if let FixedPiece::Stored {
                len, full_start, ..
            } = *piece
            {
                output.extend_from_slice(&full[full_start..full_start + len]);
            }
        }
        output.resize(table + stored_variables.saturating_sub(1) * 4, 0);
        for piece in &plan.variable {
            if let VariablePiece::Stored {
                stored_first,
                full_first,
                count,
            } = *piece
            {
                let start = spans.start(full_first);
                let base = output.len();
                output.extend_from_slice(&full[start..spans.end(full_first + count - 1)]);
                for field in 0..count {
                    let end = base + spans.end(full_first + field) - start;
                    if stored_first + field + 1 < stored_variables {
                        let end = u32::try_from(end).ok()?;
                        let at = table + (stored_first + field) * 4;
                        output[at..at + 4].copy_from_slice(&end.to_le_bytes());
                    }
                }
            }
        }
        Some(output)
    }

    /// Rebuild the stored bytes of a full record, leaving out its key fields.
    pub fn to_stored(&self, full: &[u8]) -> Result<Vec<u8>, Error> {
        if let Some(stored) = self.planned_to_stored(full) {
            return Ok(stored);
        }
        self.stored
            .create_with_encoded_fields(full.len(), |stored_index, output| {
                let span = self
                    .full
                    .field_span(full, self.full_by_stored[stored_index])?;
                output.extend_from_slice(&full[span]);
                Ok(())
            })
    }
}

/// An encoded key part whose tag does not match the plan.
struct KeyMismatch;

impl From<Error> for KeyMismatch {
    fn from(_: Error) -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::ValueType;

    fn descriptor() -> RecordDescriptor {
        RecordDescriptor::new([
            ("title", ValueType::String),
            ("id", ValueType::U64),
            ("rank", ValueType::U32),
            ("org", ValueType::Uuid),
            ("body", ValueType::Bytes),
            ("tag", ValueType::String),
        ])
    }

    fn values() -> Vec<Value> {
        vec![
            Value::String("hello".to_owned()),
            Value::U64(42),
            Value::U32(7),
            Value::Uuid(uuid::Uuid::from_u128(9)),
            Value::Bytes(vec![1, 2, 3]),
            Value::String("t".to_owned()),
        ]
    }

    fn without_plan(layout: &KeyFreeLayout) -> KeyFreeLayout {
        KeyFreeLayout {
            plan: None,
            ..layout.clone()
        }
    }

    #[test]
    fn fixed_width_key_plan_matches_field_by_field_rebuild() {
        let full = descriptor();
        let values = values();
        let record = full.create(&values).unwrap();
        for key_fields in [vec![1], vec![3, 1], vec![1, 2, 3]] {
            let layout = KeyFreeLayout::new(full, &key_fields).unwrap().unwrap();
            assert!(layout.plan.is_some());
            let general = without_plan(&layout);
            let key = key_fields
                .iter()
                .map(|&index| values[index].clone())
                .collect::<Vec<_>>();
            let stored = layout.to_stored(&record).unwrap();
            assert_eq!(stored, general.to_stored(&record).unwrap());
            assert_eq!(layout.to_full(&stored, &key).unwrap(), record);
            assert_eq!(general.to_full(&stored, &key).unwrap(), record);
        }
    }

    #[test]
    fn records_without_variable_fields_use_the_plan() {
        let full = RecordDescriptor::new([("id", ValueType::Uuid), ("n", ValueType::U32)]);
        let values = [Value::Uuid(uuid::Uuid::from_u128(5)), Value::U32(3)];
        let record = full.create(&values).unwrap();
        let layout = KeyFreeLayout::new(full, &[0]).unwrap().unwrap();
        assert!(layout.plan.is_some());
        let stored = layout.to_stored(&record).unwrap();
        assert_eq!(stored, 3_u32.to_le_bytes());
        assert_eq!(layout.to_full(&stored, &values[..1]).unwrap(), record);
        let mut long = stored.clone();
        long.push(0);
        assert_eq!(
            layout.to_full(&long, &values[..1]).unwrap_err(),
            without_plan(&layout)
                .to_full(&long, &values[..1])
                .unwrap_err()
        );
    }

    #[test]
    fn variable_width_key_plan_matches_field_by_field_rebuild() {
        let full = descriptor();
        let values = values();
        let record = full.create(&values).unwrap();
        for key_fields in [vec![0, 1], vec![5], vec![4, 2, 0]] {
            let layout = KeyFreeLayout::new(full, &key_fields).unwrap().unwrap();
            let general = without_plan(&layout);
            let key = key_fields
                .iter()
                .map(|&index| values[index].clone())
                .collect::<Vec<_>>();
            let stored = layout.to_stored(&record).unwrap();
            assert_eq!(stored, general.to_stored(&record).unwrap());
            assert_eq!(layout.to_full(&stored, &key).unwrap(), record);
            assert_eq!(general.to_full(&stored, &key).unwrap(), record);
        }
    }

    #[test]
    fn malformed_offsets_fail_as_the_field_by_field_rebuild_does() {
        let full = descriptor();
        let values = values();
        let record = full.create(&values).unwrap();
        let layout = KeyFreeLayout::new(full, &[1]).unwrap().unwrap();
        let general = without_plan(&layout);
        let key = [values[1].clone()];
        let stored = layout.to_stored(&record).unwrap();
        let fixed = layout.stored.layout.fixed_size;
        let mut past_end = stored.clone();
        past_end[fixed..fixed + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut decreasing = stored.clone();
        decreasing[fixed + 4..fixed + 8].copy_from_slice(&0_u32.to_le_bytes());
        for bad in [past_end, decreasing, stored[..fixed + 2].to_vec()] {
            assert_eq!(
                layout.to_full(&bad, &key).unwrap_err(),
                general.to_full(&bad, &key).unwrap_err()
            );
        }
        let mut full_bad = record.clone();
        let full_fixed = full.layout.fixed_size;
        full_bad[full_fixed..full_fixed + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            layout.to_stored(&full_bad).unwrap_err(),
            general.to_stored(&full_bad).unwrap_err()
        );
    }

    #[test]
    fn encoded_key_plan_matches_field_by_field_rebuild() {
        let full = descriptor();
        let values = values();
        let record = full.create(&values).unwrap();
        // Key (org, id): uuid tag 10 as is, u64 tag 3 big-endian.
        let layout = KeyFreeLayout::new(full, &[3, 1])
            .unwrap()
            .unwrap()
            .with_encoded_key(&[
                EncodedKeyPart {
                    tag: 10,
                    width: 16,
                    order: KeyByteOrder::Same,
                },
                EncodedKeyPart {
                    tag: 3,
                    width: 8,
                    order: KeyByteOrder::Reversed,
                },
            ]);
        let mut key = vec![10];
        key.extend_from_slice(uuid::Uuid::from_u128(9).as_bytes());
        key.push(3);
        key.extend_from_slice(&42_u64.to_be_bytes());
        let stored = layout.to_stored(&record).unwrap();
        assert_eq!(
            layout.to_full_from_encoded_key(&stored, &key).unwrap(),
            record
        );
        let mut wrong_tag = key.clone();
        wrong_tag[17] = 2;
        assert_eq!(layout.to_full_from_encoded_key(&stored, &wrong_tag), None);
        assert_eq!(layout.to_full_from_encoded_key(&stored, &key[..20]), None);
        let mismatched = KeyFreeLayout::new(full, &[1])
            .unwrap()
            .unwrap()
            .with_encoded_key(&[EncodedKeyPart {
                tag: 2,
                width: 4,
                order: KeyByteOrder::Reversed,
            }]);
        assert_eq!(
            mismatched.to_full_from_encoded_key(&stored, &[2, 0, 0, 0, 42]),
            None
        );
    }
}

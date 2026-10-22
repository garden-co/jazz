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
    /// Byte plan when every key field has a static width.
    static_key: Option<StaticKeyPlan>,
    /// Key byte plan, set by [`Self::with_encoded_key`].
    encoded_key: Option<EncodedKeyPlan>,
}

/// When every key field has a static width, the two records differ only in
/// their fixed regions: the variable fields, in the same order, follow an
/// offset table whose absolute offsets differ by the key fields' total width.
/// So both directions copy runs of fixed bytes and shift the offsets, with no
/// per-field span lookups.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StaticKeyPlan {
    /// The full fixed region in order: runs of stored fixed bytes and key
    /// fields.
    pieces: Vec<FixedPiece>,
    /// Fixed region plus offset table of the stored record.
    stored_header: usize,
    /// Fixed region plus offset table of the full record.
    full_header: usize,
    /// Full fixed size minus stored fixed size.
    shift: u32,
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
    /// Key part `part`, the full field `index` at `full_start`, `width`
    /// bytes wide.
    Key {
        part: usize,
        index: usize,
        full_start: usize,
        width: usize,
    },
}

impl StaticKeyPlan {
    fn new(
        full: RecordDescriptor,
        stored: RecordDescriptor,
        sources: &[FieldSource],
    ) -> Option<Self> {
        let mut pieces = Vec::<FixedPiece>::new();
        for &index in &full.layout.logical_by_physical {
            let FieldLayout::Static { offset, width } = full.layout.fields[index] else {
                continue;
            };
            match sources[index] {
                FieldSource::Key(part) => pieces.push(FixedPiece::Key {
                    part,
                    index,
                    full_start: offset,
                    width,
                }),
                FieldSource::Stored(stored_index) => {
                    let FieldLayout::Static {
                        offset: start,
                        width: len,
                    } = stored.layout.fields[stored_index]
                    else {
                        return None;
                    };
                    if let Some(FixedPiece::Stored {
                        start: run_start,
                        len: run_len,
                        ..
                    }) = pieces.last_mut()
                        && *run_start + *run_len == start
                    {
                        *run_len += len;
                    } else {
                        pieces.push(FixedPiece::Stored {
                            start,
                            len,
                            full_start: offset,
                        });
                    }
                }
            }
        }
        for (index, source) in sources.iter().enumerate() {
            match (full.layout.fields[index], source) {
                (FieldLayout::Variable { .. }, FieldSource::Key(_)) => return None,
                (FieldLayout::Variable { variable_idx }, FieldSource::Stored(stored_index)) => {
                    if stored.layout.fields[*stored_index]
                        != (FieldLayout::Variable { variable_idx })
                    {
                        return None;
                    }
                }
                _ => {}
            }
        }
        let variables = full.layout.variable_count;
        if stored.layout.variable_count != variables {
            return None;
        }
        let offsets = variables.saturating_sub(1);
        Some(Self {
            pieces,
            stored_header: stored.layout.fixed_size + offsets * 4,
            full_header: full.layout.fixed_size + offsets * 4,
            shift: u32::try_from(full.layout.fixed_size - stored.layout.fixed_size).ok()?,
        })
    }

    /// Copy a record's offset table to `output`, each offset mapped by
    /// `shift`, checking it the way a span read of each variable field
    /// would: offsets never decrease and stay within the variable section.
    /// `None` sends the caller to the field-by-field path, which reports the
    /// precise error.
    fn shift_offsets(
        &self,
        record: &[u8],
        fixed: usize,
        header: usize,
        output: &mut Vec<u8>,
        shift: impl Fn(u32) -> Option<u32>,
    ) -> Option<()> {
        let mut previous = header;
        for entry in record[fixed..header].chunks_exact(4) {
            let offset = u32::from_le_bytes(entry.try_into().expect("4 bytes"));
            let at = offset as usize;
            if at < previous || at > record.len() {
                return None;
            }
            previous = at;
            output.extend_from_slice(&shift(offset)?.to_le_bytes());
        }
        Some(())
    }
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
            static_key: StaticKeyPlan::new(full, stored, &sources),
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
        let Some(plan) = &self.static_key else {
            return self;
        };
        if parts.len() != self.key_fields.len() {
            return self;
        }
        let widths_match = plan.pieces.iter().all(|piece| match *piece {
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
        if let Some(full) = self.static_key_to_full(stored, &mut append_key) {
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

    fn static_key_to_full<E: From<Error>>(
        &self,
        stored: &[u8],
        append_key: &mut impl FnMut(usize, usize, &mut Vec<u8>) -> Result<(), E>,
    ) -> Option<Result<Vec<u8>, E>> {
        let plan = self.static_key.as_ref()?;
        let stored_fixed = self.stored.layout.fixed_size;
        if stored.len() < plan.stored_header
            || (self.stored.layout.variable_count == 0 && stored.len() != stored_fixed)
        {
            return None;
        }
        let mut output = Vec::with_capacity(stored.len() + plan.shift as usize);
        for piece in &plan.pieces {
            match *piece {
                FixedPiece::Stored { start, len, .. } => {
                    output.extend_from_slice(&stored[start..start + len]);
                }
                FixedPiece::Key {
                    part, index, width, ..
                } => {
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
        plan.shift_offsets(
            stored,
            stored_fixed,
            plan.stored_header,
            &mut output,
            |offset| offset.checked_add(plan.shift),
        )?;
        output.extend_from_slice(&stored[plan.stored_header..]);
        Some(Ok(output))
    }

    /// Rebuild the full record from stored bytes and the row's encoded key
    /// with byte copies alone. `None` when the layout has no key byte plan
    /// or the bytes do not check out; the field-by-field rebuild then
    /// reports the precise error.
    #[inline]
    pub fn to_full_from_encoded_key(&self, stored: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let plan = self.static_key.as_ref()?;
        let key_plan = self.encoded_key.as_ref()?;
        let stored_fixed = self.stored.layout.fixed_size;
        if key.len() != key_plan.len
            || stored.len() < plan.stored_header
            || (self.stored.layout.variable_count == 0 && stored.len() != stored_fixed)
        {
            return None;
        }
        // Pieces come in full field order, so the record is appended front
        // to back into one exact allocation.
        let mut output = Vec::with_capacity(stored.len() + plan.shift as usize);
        for piece in &plan.pieces {
            match *piece {
                FixedPiece::Stored { start, len, .. } => {
                    output.extend_from_slice(&stored[start..start + len]);
                }
                FixedPiece::Key { part, .. } => {
                    let (at, part) = key_plan.parts[part];
                    if key[at] != part.tag {
                        return None;
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
                }
            }
        }
        plan.shift_offsets(
            stored,
            stored_fixed,
            plan.stored_header,
            &mut output,
            |offset| offset.checked_add(plan.shift),
        )?;
        output.extend_from_slice(&stored[plan.stored_header..]);
        Some(output)
    }

    fn static_key_to_stored(&self, full: &[u8]) -> Option<Vec<u8>> {
        let plan = self.static_key.as_ref()?;
        if full.len() < plan.full_header {
            return None;
        }
        if self.full.layout.variable_count == 0 && full.len() != self.full.layout.fixed_size {
            return None;
        }
        let mut output = Vec::with_capacity(full.len() - plan.shift as usize);
        for piece in &plan.pieces {
            if let FixedPiece::Stored {
                len, full_start, ..
            } = *piece
            {
                output.extend_from_slice(&full[full_start..full_start + len]);
            }
        }
        plan.shift_offsets(
            full,
            self.full.layout.fixed_size,
            plan.full_header,
            &mut output,
            |offset| offset.checked_sub(plan.shift),
        )?;
        output.extend_from_slice(&full[plan.full_header..]);
        Some(output)
    }

    /// Rebuild the stored bytes of a full record, leaving out its key fields.
    pub fn to_stored(&self, full: &[u8]) -> Result<Vec<u8>, Error> {
        if let Some(stored) = self.static_key_to_stored(full) {
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
            static_key: None,
            ..layout.clone()
        }
    }

    #[test]
    fn static_key_plan_matches_field_by_field_rebuild() {
        let full = descriptor();
        let values = values();
        let record = full.create(&values).unwrap();
        for key_fields in [vec![1], vec![3, 1], vec![1, 2, 3]] {
            let layout = KeyFreeLayout::new(full, &key_fields).unwrap().unwrap();
            assert!(layout.static_key.is_some());
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
        assert!(layout.static_key.is_some());
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
    fn variable_width_keys_rebuild_field_by_field() {
        let full = descriptor();
        let values = values();
        let record = full.create(&values).unwrap();
        let layout = KeyFreeLayout::new(full, &[0, 1]).unwrap().unwrap();
        assert!(layout.static_key.is_none());
        let stored = layout.to_stored(&record).unwrap();
        assert_eq!(layout.to_full(&stored, &values[..2]).unwrap(), record);
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

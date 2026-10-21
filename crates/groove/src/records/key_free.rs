//! Stored rows of a keyed table leave out the primary-key fields.
//!
//! A table row is stored under its encoded primary key, so a copy of the key
//! columns in the stored value is redundant. [`KeyFreeLayout`] maps between a
//! table variant's full record (every field, as reads, IVM operators and
//! writes see it) and its stored record (the same fields without the key
//! fields): writes drop the key fields, reads put them back at their declared
//! positions from the decoded key. Every other field is copied as encoded
//! bytes and never decoded.

use super::{Error, RecordDescriptor, Value};

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
        Ok(Some(Self {
            full,
            stored: RecordDescriptor::new_with_fields(stored_fields),
            sources,
            full_by_stored,
            key_fields: key_fields.to_vec(),
        }))
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
        self.full
            .create_with_encoded_fields(stored.len() + 16 * key.len(), |index, output| {
                match self.sources[index] {
                    FieldSource::Key(part) => {
                        self.full.encode_field_into(index, &key[part], output)
                    }
                    FieldSource::Stored(stored_index) => {
                        let span = self.stored.field_span(stored, stored_index)?;
                        output.extend_from_slice(&stored[span]);
                        Ok(())
                    }
                }
            })
    }

    /// Rebuild the stored bytes of a full record, leaving out its key fields.
    pub fn to_stored(&self, full: &[u8]) -> Result<Vec<u8>, Error> {
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

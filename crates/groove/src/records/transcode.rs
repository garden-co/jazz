//! Conversion between the stored (physical) and logical forms of records
//! whose descriptor has interned or U64-stored-as-U48 fields.
//!
//! An interned field stores a 4-byte dictionary code (`u32` LE) where the
//! logical record holds the full value. A U64-stored-as-U48 field
//! (`INV-STORAGE-39`) stores 6 bytes little-endian where the logical record
//! holds an 8-byte `U64`. Every other field has the same encoding in both
//! forms, but the record layout differs: a code is fixed-width while the
//! logical value usually is not, so fields move between the fixed region and
//! the variable region. [`RecordTranscoder`] rebuilds a record field by field
//! in the other form; it never decodes a field whose form does not change.

use super::values::{
    U48_MAX, decode_value, encode_value, ensure_value_type, validate_canonical_value,
    visit_encoded_indirect_values,
};
use super::{Error, RecordDescriptor, ValueType};

/// How one stored field whose form differs from its logical form is laid out.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TranscodedField {
    /// The field is `Nullable(..)`: a presence flag precedes the payload.
    nullable: bool,
    kind: TranscodedKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TranscodedKind {
    Interned {
        dictionary: String,
        /// The logical type stored in the dictionary.
        logical: ValueType,
    },
    /// A logical `U64` kept in a 6-byte little-endian slot.
    StoredU48,
}

/// Width of a stored U64-stored-as-U48 payload.
const STORED_U48_WIDTH: usize = 6;

/// Rebuilds records of one stored descriptor between its physical form (codes)
/// and its logical form (values).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordTranscoder {
    physical: RecordDescriptor,
    logical: RecordDescriptor,
    fields: Vec<Option<TranscodedField>>,
}

/// The logical descriptor of a stored descriptor: every interned field
/// replaced by its logical type and every U64-stored-as-U48 field by `U64`.
/// Names and identities are kept, so a field index means the same field in
/// both descriptors.
pub(crate) fn logical_record_descriptor(descriptor: &RecordDescriptor) -> RecordDescriptor {
    if !descriptor.contains_storage_transcoding() {
        return *descriptor;
    }
    RecordDescriptor::new_with_fields(descriptor.fields().iter().map(|field| {
        let mut field = field.clone();
        field.value_type = field.value_type.logical();
        field
    }))
}

impl RecordDescriptor {
    /// Whether any field of this descriptor is interned at any depth.
    pub fn contains_interned(&self) -> bool {
        self.fields()
            .iter()
            .any(|field| field.value_type.contains_interned())
    }

    /// Whether any field's stored form differs from its logical form: it is
    /// interned or a U64-stored-as-U48 field, at any depth.
    pub fn contains_storage_transcoding(&self) -> bool {
        self.fields()
            .iter()
            .any(|field| field.value_type.contains_storage_transcoding())
    }

    /// This descriptor with interned fields replaced by their logical types
    /// and U64-stored-as-U48 fields by `U64`.
    pub fn logical(&self) -> RecordDescriptor {
        logical_record_descriptor(self)
    }
}

impl RecordTranscoder {
    /// A transcoder for records stored under `physical`, or `None` when no
    /// field is interned or U64-stored-as-U48 and the stored bytes already
    /// are the logical record.
    ///
    /// Such a field must sit at top level, optionally under one `Nullable`;
    /// any other placement is refused.
    pub fn new(physical: RecordDescriptor) -> Result<Option<Self>, Error> {
        if !physical.contains_storage_transcoding() {
            return Ok(None);
        }
        let mut fields = Vec::with_capacity(physical.fields().len());
        for field in physical.fields() {
            let (candidate, nullable) = match &field.value_type {
                ValueType::Nullable(inner) => (inner.as_ref(), true),
                other => (other, false),
            };
            if candidate.is_u64_stored_as_u48() {
                fields.push(Some(TranscodedField {
                    nullable,
                    kind: TranscodedKind::StoredU48,
                }));
                continue;
            }
            if field.value_type.contains_stored_u48() {
                return Err(Error::UnsupportedStoredU48Placement {
                    field: field.name.clone().unwrap_or_default(),
                });
            }
            match candidate.as_interned() {
                Some((dictionary, logical)) if !logical.contains_storage_transcoding() => {
                    fields.push(Some(TranscodedField {
                        nullable,
                        kind: TranscodedKind::Interned {
                            dictionary: dictionary.to_owned(),
                            logical: logical.clone(),
                        },
                    }));
                }
                None if !field.value_type.contains_interned() => fields.push(None),
                _ => {
                    return Err(Error::UnsupportedInternedPlacement {
                        field: field.name.clone().unwrap_or_default(),
                    });
                }
            }
        }
        Ok(Some(Self {
            physical,
            logical: logical_record_descriptor(&physical),
            fields,
        }))
    }

    pub fn physical(&self) -> RecordDescriptor {
        self.physical
    }

    pub fn logical(&self) -> RecordDescriptor {
        self.logical
    }

    /// The dictionaries this descriptor's interned fields use, with their
    /// logical value types, in field order (repeats included).
    pub fn interned_dictionaries(&self) -> impl Iterator<Item = (&str, &ValueType)> {
        self.fields
            .iter()
            .flatten()
            .filter_map(|field| match &field.kind {
                TranscodedKind::Interned {
                    dictionary,
                    logical,
                } => Some((dictionary.as_str(), logical)),
                TranscodedKind::StoredU48 => None,
            })
    }

    /// Rebuild a stored record in its logical form. `decode` resolves a code
    /// of a named dictionary to the canonical bytes of its value.
    pub fn to_logical<'d, E>(
        &self,
        physical: &[u8],
        decode: impl Fn(&str, u32) -> Result<&'d [u8], E>,
    ) -> Result<Vec<u8>, E>
    where
        E: From<Error>,
    {
        self.logical
            .create_with_encoded_fields(physical.len() * 2, |index, output| {
                let span = self.physical.field_span(physical, index)?;
                let stored = &physical[span];
                let Some(field) = &self.fields[index] else {
                    output.extend_from_slice(stored);
                    return Ok(());
                };
                let payload = if field.nullable {
                    match stored.split_first() {
                        Some((0, padding)) if padding.iter().all(|byte| *byte == 0) => {
                            output.push(0);
                            if let Some(width) = field.logical_fixed_size() {
                                output.resize(output.len() + width, 0);
                            }
                            return Ok(());
                        }
                        Some((1, payload)) => {
                            output.push(1);
                            payload
                        }
                        Some((0, _)) => return Err(Error::InvalidOffset.into()),
                        Some((flag, _)) => return Err(Error::InvalidNullFlag(*flag).into()),
                        None => return Err(Error::UnexpectedEof.into()),
                    }
                } else {
                    stored
                };
                match &field.kind {
                    TranscodedKind::Interned { dictionary, .. } => {
                        let code: [u8; 4] = payload.try_into().map_err(|_| Error::InvalidOffset)?;
                        output.extend_from_slice(decode(dictionary, u32::from_le_bytes(code))?);
                    }
                    TranscodedKind::StoredU48 => {
                        let narrow: [u8; STORED_U48_WIDTH] =
                            payload.try_into().map_err(|_| Error::InvalidOffset)?;
                        let mut wide = [0u8; 8];
                        wide[..STORED_U48_WIDTH].copy_from_slice(&narrow);
                        output.extend_from_slice(&wide);
                    }
                }
                Ok(())
            })
    }

    /// Rebuild a logical record in its stored form. `encode` returns the code
    /// of a value (canonical bytes) in a named dictionary.
    pub fn to_physical<E>(
        &self,
        logical: &[u8],
        mut encode: impl FnMut(&str, &ValueType, &[u8]) -> Result<u32, E>,
    ) -> Result<Vec<u8>, E>
    where
        E: From<Error>,
    {
        self.physical
            .create_with_encoded_fields(logical.len(), |index, output| {
                let span = self.logical.field_span(logical, index)?;
                let value = &logical[span];
                let Some(field) = &self.fields[index] else {
                    output.extend_from_slice(value);
                    return Ok(());
                };
                let value = if field.nullable {
                    match value.split_first() {
                        Some((0, _)) => {
                            output.push(0);
                            output.resize(output.len() + field.stored_width(), 0);
                            return Ok(());
                        }
                        Some((1, value)) => {
                            output.push(1);
                            value
                        }
                        Some((flag, _)) => return Err(Error::InvalidNullFlag(*flag).into()),
                        None => return Err(Error::UnexpectedEof.into()),
                    }
                } else {
                    value
                };
                match &field.kind {
                    TranscodedKind::Interned {
                        dictionary,
                        logical,
                    } => {
                        let code = encode(dictionary, logical, value)?;
                        output.extend_from_slice(&code.to_le_bytes());
                    }
                    TranscodedKind::StoredU48 => {
                        let wide: [u8; 8] = value.try_into().map_err(|_| Error::InvalidOffset)?;
                        let value = u64::from_le_bytes(wide);
                        if value > U48_MAX {
                            return Err(Error::U48OutOfRange(value).into());
                        }
                        output.extend_from_slice(&wide[..STORED_U48_WIDTH]);
                    }
                }
                Ok(())
            })
    }
}

impl TranscodedField {
    /// Width of the stored payload, without a nullable presence flag.
    fn stored_width(&self) -> usize {
        match self.kind {
            TranscodedKind::Interned { .. } => 4,
            TranscodedKind::StoredU48 => STORED_U48_WIDTH,
        }
    }

    /// Width of the logical payload when it is fixed, without a nullable
    /// presence flag.
    fn logical_fixed_size(&self) -> Option<usize> {
        match &self.kind {
            TranscodedKind::Interned { logical, .. } => logical.fixed_size(),
            TranscodedKind::StoredU48 => Some(8),
        }
    }
}

/// Check that `bytes` is the one canonical encoding of a `value_type` value
/// and holds no indirect (chunked) scalar, as a dictionary value must: equal
/// values then have equal bytes, and an entry never references large-value
/// storage that Groove could reclaim.
pub(crate) fn validate_dictionary_value(bytes: &[u8], value_type: &ValueType) -> Result<(), Error> {
    validate_canonical_value(bytes, value_type)?;
    if visit_encoded_indirect_values(bytes, value_type, &mut |_, _| Ok(true))? {
        return Err(Error::IndirectDictionaryValue);
    }
    let value = decode_value(bytes, value_type)?;
    ensure_value_type(&value, value_type)?;
    if encode_value(&value, value_type)? != bytes {
        return Err(Error::NonCanonicalRecord);
    }
    Ok(())
}

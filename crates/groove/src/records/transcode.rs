//! Conversion between the stored (physical) and logical forms of records
//! whose descriptor has interned fields.
//!
//! An interned field stores a 4-byte dictionary code (`u32` LE) where the
//! logical record holds the full value. Every other field has the same
//! encoding in both forms, but the record layout differs: a code is
//! fixed-width while the logical value usually is not, so fields move between
//! the fixed region and the variable region. [`RecordTranscoder`] rebuilds a
//! record field by field in the other form; it never decodes a field that is
//! not interned.

use super::values::{
    decode_value, encode_value, ensure_value_type, validate_canonical_value,
    visit_encoded_indirect_values,
};
use super::{Error, RecordDescriptor, ValueType};

/// How one interned field of a stored record is laid out.
#[derive(Clone, Debug, PartialEq, Eq)]
struct InternedField {
    dictionary: String,
    /// The field is `Nullable(Interned)`: a presence flag precedes the code.
    nullable: bool,
    /// The logical type stored in the dictionary.
    logical: ValueType,
}

/// Rebuilds records of one stored descriptor between its physical form (codes)
/// and its logical form (values).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordTranscoder {
    physical: RecordDescriptor,
    logical: RecordDescriptor,
    fields: Vec<Option<InternedField>>,
}

/// The logical descriptor of a stored descriptor: every interned field
/// replaced by its logical type. Names and identities are kept, so a field
/// index means the same field in both descriptors.
pub(crate) fn logical_record_descriptor(descriptor: &RecordDescriptor) -> RecordDescriptor {
    if !descriptor
        .fields()
        .iter()
        .any(|field| field.value_type.contains_interned())
    {
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

    /// This descriptor with interned fields replaced by their logical types.
    pub fn logical(&self) -> RecordDescriptor {
        logical_record_descriptor(self)
    }
}

impl RecordTranscoder {
    /// A transcoder for records stored under `physical`, or `None` when no
    /// field is interned and the stored bytes already are the logical record.
    ///
    /// An interned field must sit at top level, optionally under one
    /// `Nullable`; any other placement is refused.
    pub fn new(physical: RecordDescriptor) -> Result<Option<Self>, Error> {
        if !physical.contains_interned() {
            return Ok(None);
        }
        let mut fields = Vec::with_capacity(physical.fields().len());
        for field in physical.fields() {
            let interned = match &field.value_type {
                ValueType::Nullable(inner) => inner.as_interned().map(|parts| (parts, true)),
                other => other.as_interned().map(|parts| (parts, false)),
            };
            match interned {
                Some(((dictionary, logical), nullable)) if !logical.contains_interned() => {
                    fields.push(Some(InternedField {
                        dictionary: dictionary.to_owned(),
                        nullable,
                        logical: logical.clone(),
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
            .map(|field| (field.dictionary.as_str(), &field.logical))
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
                let code = if field.nullable {
                    match stored.split_first() {
                        Some((0, padding)) if padding.iter().all(|byte| *byte == 0) => {
                            output.push(0);
                            if let Some(width) = field.logical.fixed_size() {
                                output.resize(output.len() + width, 0);
                            }
                            return Ok(());
                        }
                        Some((1, code)) => {
                            output.push(1);
                            code
                        }
                        Some((0, _)) => return Err(Error::InvalidOffset.into()),
                        Some((flag, _)) => return Err(Error::InvalidNullFlag(*flag).into()),
                        None => return Err(Error::UnexpectedEof.into()),
                    }
                } else {
                    stored
                };
                let code: [u8; 4] = code.try_into().map_err(|_| Error::InvalidOffset)?;
                output.extend_from_slice(decode(&field.dictionary, u32::from_le_bytes(code))?);
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
                            output.extend_from_slice(&[0; 5]);
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
                let code = encode(&field.dictionary, &field.logical, value)?;
                output.extend_from_slice(&code.to_le_bytes());
                Ok(())
            })
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

//! Groove dictionaries: durable `u32` codes for column values that repeat.
//!
//! A column declared with [`ValueType::interned`] holds, in each row, the
//! 4-byte code (`u32` LE) of its value in a named dictionary instead of the
//! value itself. The code is the column's value everywhere inside the node:
//! stored rows, point reads, scans, IVM sources, operators and query results
//! all carry it, under one descriptor whose field keeps the interned type.
//! Nothing transcodes a whole row. Equality, joins and grouping between
//! columns of one dictionary work on codes directly, because one value has
//! exactly one code. An operator that needs the value itself (a comparison
//! with a literal, a sub-field of a record value, an ordering) resolves the
//! code through the in-memory dictionary on demand.
//!
//! Codes are node-local, so they never leave the node: a caller resolves
//! them where rows leave it (the sync wire, a client API) with
//! [`Dictionaries`], and interns values arriving from outside with
//! [`Dictionaries::intern`] before it writes them.
//!
//! # Storage format (`groove.dictionary.v1`)
//!
//! Each dictionary declared by a [`DictionarySchema`] is its own Groove-owned
//! column family, [`dictionary_column_family`] of the dictionary name. An
//! entry's key is the code as a 4-byte big-endian `u32`, so a scan returns
//! entries in code order; its value is the canonical Groove encoding of the
//! logical value (the declared [`DictionarySchema::value_type`]), which may
//! not hold an indirect large value. Codes are dense from 0: the entries of a
//! dictionary are always exactly the codes `0..n`.
//!
//! # Allocation
//!
//! The whole dictionary is loaded into memory on open, validated (every key
//! is 4 bytes, codes are dense, every value is canonical, no value appears
//! twice) and kept in a [`DictionaryRegistry`] shared by the database, its
//! IVM runtime and every [`Dictionaries`] handle, across schema rebuilds.
//! Interning a value that has no code yet allocates the next code in memory,
//! synchronously. The allocation is never
//! undone: when the batch that caused it is dropped, the code stays reserved
//! for that value and its entry is written by the next publication, so a code
//! is never handed to two values. Every publication carries every entry not
//! yet known to be durable, so a row never names a code whose entry is
//! missing after a crash, and durable codes stay dense.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use thiserror::Error;

use crate::records;
use crate::schema::DictionarySchema;
use crate::storage::{OrderedKvStorage, OwnedWriteOperation, ScanRequest};

/// Column-family name prefix of Groove dictionaries.
pub const DICTIONARY_CF_PREFIX: &str = "__groove_dictionary_";

/// The column family that stores dictionary `name`.
pub fn dictionary_column_family(name: &str) -> String {
    format!("{DICTIONARY_CF_PREFIX}{name}")
}

/// The storage key of `code`: its 4-byte big-endian encoding.
pub fn dictionary_entry_key(code: u32) -> [u8; 4] {
    code.to_be_bytes()
}

/// A dictionary failure. Every variant names the dictionary.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DictionaryError {
    #[error("dictionary {0} is not declared")]
    UnknownDictionary(String),
    #[error("dictionary {dictionary} has no entry for code {code}")]
    UnknownCode { dictionary: String, code: u32 },
    #[error("dictionary {0} has no free code left")]
    Exhausted(String),
    #[error("invalid dictionary {dictionary}: {reason}")]
    InvalidDictionary { dictionary: String, reason: String },
    #[error("value for dictionary {dictionary} is not a canonical inline value: {source}")]
    InvalidValue {
        dictionary: String,
        source: records::Error,
    },
    #[error(
        "dictionary {0} is declared by the new schema but was not loaded when the database opened"
    )]
    NotLoaded(String),
    #[error("dictionary {0} is redeclared with a different value type")]
    ValueTypeMismatch(String),
}

/// Shared registry handle: the database, its IVM runtime and every rebuilt
/// runtime share one registry, so a code handed out once is never handed out
/// again for another value within this process.
pub(crate) type SharedDictionaries = Rc<RefCell<DictionaryRegistry>>;

/// A caller's handle on a database's dictionaries, from
/// [`crate::db::Database::dictionaries`]. Cheap to clone; every clone sees
/// the same registry as the database, so a code interned through it is the
/// code the database stores and resolves.
#[derive(Clone)]
pub struct Dictionaries(SharedDictionaries);

impl std::fmt::Debug for Dictionaries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Dictionaries").finish_non_exhaustive()
    }
}

impl Dictionaries {
    pub(crate) fn new(shared: SharedDictionaries) -> Self {
        Self(shared)
    }

    /// Empty dictionaries for `schemas` that belong to no database: codes
    /// they allocate are never stored. For codecs and tests that need codes
    /// without opening a database.
    pub fn detached<'a>(schemas: impl IntoIterator<Item = &'a DictionarySchema>) -> Self {
        Self(std::rc::Rc::new(std::cell::RefCell::new(
            DictionaryRegistry::declared(schemas),
        )))
    }

    /// The canonical bytes of the value `code` stands for in `dictionary`.
    pub fn resolve(&self, dictionary: &str, code: u32) -> Result<Arc<[u8]>, DictionaryError> {
        self.0.borrow().decode_shared(dictionary, code)
    }

    /// The code of `value` (its canonical bytes) in `dictionary`, allocating
    /// the next code when the value is new. A new entry becomes durable with
    /// the next publication, before or together with any row that stores it.
    pub fn intern(&self, dictionary: &str, value: &[u8]) -> Result<u32, DictionaryError> {
        self.0.borrow_mut().encode(dictionary, value)
    }

    /// The code `value` already has in `dictionary`, without allocating one.
    pub fn code(&self, dictionary: &str, value: &[u8]) -> Option<u32> {
        self.0.borrow().code(dictionary, value)
    }

    /// The record bytes of `record` under `descriptor` with every interned
    /// field resolved to its value, and the descriptor of the result (each
    /// interned field replaced by its logical type). Egress uses this where a
    /// row leaves the node. A descriptor without interned fields is returned
    /// unchanged and its bytes are copied.
    pub fn resolve_record(
        &self,
        descriptor: records::RecordDescriptor,
        record: &[u8],
    ) -> Result<(Vec<u8>, records::RecordDescriptor), DictionaryTranscodeError> {
        let Some(transcoder) = records::RecordTranscoder::new(descriptor)? else {
            return Ok((record.to_vec(), descriptor));
        };
        let bytes = transcode_to_logical(&transcoder, &self.0.borrow(), record)?;
        Ok((bytes, transcoder.logical()))
    }

    /// The record bytes of `record`, laid out by `descriptor`'s logical form
    /// (see [`records::RecordDescriptor::logical`]), with every interned field
    /// replaced by its code under `descriptor`, interning new values. Ingress
    /// uses this where a row with full values arrives from outside the node.
    pub fn intern_record(
        &self,
        descriptor: records::RecordDescriptor,
        logical: &[u8],
    ) -> Result<Vec<u8>, DictionaryTranscodeError> {
        let Some(transcoder) = records::RecordTranscoder::new(descriptor)? else {
            return Ok(logical.to_vec());
        };
        transcode_to_physical(&transcoder, &mut self.0.borrow_mut(), logical)
    }
}

#[derive(Debug)]
struct DictionaryState {
    schema: DictionarySchema,
    /// `values[code]`. Codes are dense from 0.
    values: Vec<Arc<[u8]>>,
    codes: FxHashMap<Arc<[u8]>, u32>,
    /// The next code to allocate. Exceeds `u32::MAX` once exhausted.
    next_code: u64,
    /// Allocated codes whose entries are not yet known to be durable.
    unpersisted: BTreeSet<u32>,
}

impl DictionaryState {
    fn new(schema: DictionarySchema) -> Self {
        Self {
            schema,
            values: Vec::new(),
            codes: FxHashMap::default(),
            next_code: 0,
            unpersisted: BTreeSet::new(),
        }
    }

    fn invalid(&self, reason: impl Into<String>) -> DictionaryError {
        DictionaryError::InvalidDictionary {
            dictionary: self.schema.name.clone(),
            reason: reason.into(),
        }
    }

    fn validate_value(&self, value: &[u8]) -> Result<(), DictionaryError> {
        records::validate_dictionary_value(value, &self.schema.value_type).map_err(|source| {
            DictionaryError::InvalidValue {
                dictionary: self.schema.name.clone(),
                source,
            }
        })
    }

    fn insert(&mut self, code: u32, value: Arc<[u8]>) {
        debug_assert_eq!(code as usize, self.values.len());
        self.codes.insert(Arc::clone(&value), code);
        self.values.push(value);
        self.next_code = u64::from(code) + 1;
    }
}

/// In-memory image of every declared dictionary, plus which entries are not
/// yet durable.
#[derive(Debug, Default)]
pub struct DictionaryRegistry {
    dictionaries: BTreeMap<String, DictionaryState>,
}

/// The dictionary entries one publication must make durable, and what to
/// mark once it has.
#[derive(Debug, Default)]
pub(crate) struct PendingDictionaryEntries {
    operations: Vec<OwnedWriteOperation>,
    entries: Vec<(String, u32)>,
}

impl PendingDictionaryEntries {
    pub(crate) fn operations(&self) -> &[OwnedWriteOperation] {
        &self.operations
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl DictionaryRegistry {
    /// Empty dictionaries for `schemas`, for a runtime without storage.
    pub(crate) fn declared<'a>(schemas: impl IntoIterator<Item = &'a DictionarySchema>) -> Self {
        Self {
            dictionaries: schemas
                .into_iter()
                .map(|schema| (schema.name.clone(), DictionaryState::new(schema.clone())))
                .collect(),
        }
    }

    /// Load and validate every dictionary in `schemas` from storage.
    ///
    /// Refuses, with [`DictionaryError::InvalidDictionary`], a malformed key,
    /// codes that are not dense from 0, a value that is not the canonical
    /// inline encoding of the declared type, and a value stored twice.
    pub(crate) async fn load<S>(
        storage: &S,
        schemas: &[DictionarySchema],
    ) -> Result<Self, crate::db::Error>
    where
        S: OrderedKvStorage + ?Sized,
    {
        let mut registry = Self::default();
        for schema in schemas {
            let mut state = DictionaryState::new(schema.clone());
            if schema.column_family() != dictionary_column_family(&schema.name) {
                return Err(state
                    .invalid("column family does not match its name")
                    .into());
            }
            let entries = crate::storage::collect_scan(
                storage
                    .scan(ScanRequest::prefix(
                        schema.column_family().to_owned(),
                        Vec::new(),
                    ))
                    .await?,
            )
            .await?;
            for (key, value) in entries {
                let key: [u8; 4] = key
                    .as_slice()
                    .try_into()
                    .map_err(|_| state.invalid("an entry key is not a 4-byte code"))?;
                let code = u32::from_be_bytes(key);
                if u64::from(code) != state.next_code {
                    return Err(state
                        .invalid(format!(
                            "codes are not dense: expected code {}, found {code}",
                            state.next_code
                        ))
                        .into());
                }
                state.validate_value(&value).map_err(|error| match error {
                    DictionaryError::InvalidValue { source, .. } => {
                        state.invalid(format!("entry {code} is not canonical: {source}"))
                    }
                    other => other,
                })?;
                let value: Arc<[u8]> = value.into();
                if state.codes.contains_key(&value) {
                    return Err(state
                        .invalid(format!("entry {code} repeats an earlier value"))
                        .into());
                }
                state.insert(code, value);
            }
            if registry
                .dictionaries
                .insert(schema.name.clone(), state)
                .is_some()
            {
                return Err(DictionaryError::InvalidDictionary {
                    dictionary: schema.name.clone(),
                    reason: "declared twice".to_owned(),
                }
                .into());
            }
        }
        Ok(registry)
    }

    /// Refuse a schema whose dictionaries this registry did not load, or
    /// loaded with another value type.
    pub(crate) fn check_declares(
        &self,
        schemas: &[DictionarySchema],
    ) -> Result<(), DictionaryError> {
        for schema in schemas {
            let state = self
                .dictionaries
                .get(&schema.name)
                .ok_or_else(|| DictionaryError::NotLoaded(schema.name.clone()))?;
            if state.schema != *schema {
                return Err(DictionaryError::ValueTypeMismatch(schema.name.clone()));
            }
        }
        Ok(())
    }

    fn state(&self, dictionary: &str) -> Result<&DictionaryState, DictionaryError> {
        self.dictionaries
            .get(dictionary)
            .ok_or_else(|| DictionaryError::UnknownDictionary(dictionary.to_owned()))
    }

    /// The code of `value` (its canonical bytes) in `dictionary`, allocating
    /// the next code when the value is new. A new value is validated first.
    pub fn encode(&mut self, dictionary: &str, value: &[u8]) -> Result<u32, DictionaryError> {
        let state = self
            .dictionaries
            .get_mut(dictionary)
            .ok_or_else(|| DictionaryError::UnknownDictionary(dictionary.to_owned()))?;
        if let Some(code) = state.codes.get(value) {
            return Ok(*code);
        }
        state.validate_value(value)?;
        let code = u32::try_from(state.next_code)
            .map_err(|_| DictionaryError::Exhausted(dictionary.to_owned()))?;
        state.insert(code, value.into());
        state.unpersisted.insert(code);
        Ok(code)
    }

    /// The canonical bytes of the value that `code` stands for.
    pub fn decode(&self, dictionary: &str, code: u32) -> Result<&[u8], DictionaryError> {
        self.state(dictionary)?
            .values
            .get(code as usize)
            .map(|value| &**value)
            .ok_or_else(|| DictionaryError::UnknownCode {
                dictionary: dictionary.to_owned(),
                code,
            })
    }

    /// [`Self::decode`] as a shared handle on the value bytes.
    pub(crate) fn decode_shared(
        &self,
        dictionary: &str,
        code: u32,
    ) -> Result<Arc<[u8]>, DictionaryError> {
        self.state(dictionary)?
            .values
            .get(code as usize)
            .cloned()
            .ok_or_else(|| DictionaryError::UnknownCode {
                dictionary: dictionary.to_owned(),
                code,
            })
    }

    /// Check that `code` names an entry of `dictionary`. A written row may
    /// only store codes the dictionary already holds.
    pub(crate) fn check_code(&self, dictionary: &str, code: u32) -> Result<(), DictionaryError> {
        let state = self.state(dictionary)?;
        if (code as usize) < state.values.len() {
            Ok(())
        } else {
            Err(DictionaryError::UnknownCode {
                dictionary: dictionary.to_owned(),
                code,
            })
        }
    }

    /// The code `value` already has, without allocating one.
    pub fn code(&self, dictionary: &str, value: &[u8]) -> Option<u32> {
        self.dictionaries.get(dictionary)?.codes.get(value).copied()
    }

    /// How many codes `dictionary` has allocated.
    pub fn len(&self, dictionary: &str) -> Option<usize> {
        Some(self.dictionaries.get(dictionary)?.values.len())
    }

    /// Entries allocated in memory and not yet known to be durable.
    pub(crate) fn pending(&self) -> PendingDictionaryEntries {
        let mut pending = PendingDictionaryEntries::default();
        for (name, state) in &self.dictionaries {
            for &code in &state.unpersisted {
                pending.operations.push(OwnedWriteOperation::Set {
                    cf: state.schema.column_family().to_owned(),
                    key: dictionary_entry_key(code).to_vec(),
                    value: state.values[code as usize].to_vec(),
                });
                pending.entries.push((name.clone(), code));
            }
        }
        pending
    }

    /// Record that `pending` is durable (or is part of a write whose failure
    /// poisons the database instance).
    pub(crate) fn mark_persisted(&mut self, pending: &PendingDictionaryEntries) {
        for (name, code) in &pending.entries {
            if let Some(state) = self.dictionaries.get_mut(name) {
                state.unpersisted.remove(code);
            }
        }
    }

    /// Pretend `dictionary` already allocated every code below `next_code`.
    #[cfg(test)]
    pub(crate) fn set_next_code_for_test(&mut self, dictionary: &str, next_code: u64) {
        self.dictionaries
            .get_mut(dictionary)
            .expect("declared dictionary")
            .next_code = next_code;
    }
}

/// Transcode a stored record to its logical form, resolving codes through
/// `dictionaries`.
pub(crate) fn transcode_to_logical(
    transcoder: &records::RecordTranscoder,
    dictionaries: &DictionaryRegistry,
    physical: &[u8],
) -> Result<Vec<u8>, DictionaryTranscodeError> {
    transcoder.to_logical(physical, |dictionary, code| {
        dictionaries
            .decode(dictionary, code)
            .map_err(DictionaryTranscodeError::Dictionary)
    })
}

/// Transcode a logical record to its stored form, allocating codes for new
/// values in `dictionaries`.
pub(crate) fn transcode_to_physical(
    transcoder: &records::RecordTranscoder,
    dictionaries: &mut DictionaryRegistry,
    logical: &[u8],
) -> Result<Vec<u8>, DictionaryTranscodeError> {
    transcoder.to_physical(logical, |dictionary, value_type, value| {
        let state = dictionaries.state(dictionary)?;
        if state.schema.value_type != *value_type {
            return Err(DictionaryError::ValueTypeMismatch(dictionary.to_owned()).into());
        }
        Ok(dictionaries.encode(dictionary, value)?)
    })
}

/// A transcoding failure: malformed record bytes or a dictionary failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DictionaryTranscodeError {
    #[error(transparent)]
    Record(#[from] records::Error),
    #[error(transparent)]
    Dictionary(#[from] DictionaryError),
}

// Registry-level tests: code exhaustion needs four billion allocations
// through the public API, and the pending/persisted bookkeeping is not
// observable apart from the storage writes the database tests cover.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::ValueType;

    fn registry() -> DictionaryRegistry {
        DictionaryRegistry::declared(&[DictionarySchema::new("names", ValueType::String)])
    }

    fn name(value: &str) -> Vec<u8> {
        crate::records::encode_single_field_value(
            &crate::records::Value::String(value.into()),
            &ValueType::String,
        )
        .unwrap()
    }

    /// Codes are dense from 0, a known value keeps its code, and an
    /// allocation is pending until a publication marks it persisted.
    #[test]
    fn codes_are_dense_and_pending_until_persisted() {
        let mut registry = registry();
        assert_eq!(registry.encode("names", &name("alice")).unwrap(), 0);
        assert_eq!(registry.encode("names", &name("bob")).unwrap(), 1);
        assert_eq!(registry.encode("names", &name("alice")).unwrap(), 0);
        assert_eq!(registry.decode("names", 1).unwrap(), name("bob"));
        let pending = registry.pending();
        assert_eq!(pending.operations().len(), 2);
        registry.mark_persisted(&pending);
        assert!(registry.pending().is_empty());
        assert_eq!(registry.encode("names", &name("carol")).unwrap(), 2);
        assert_eq!(registry.pending().operations().len(), 1);
    }

    /// The last code is `u32::MAX`; past it, allocation fails with a typed
    /// error and known values still resolve.
    #[test]
    fn allocation_past_the_last_code_is_refused() {
        let mut registry = registry();
        assert_eq!(registry.encode("names", &name("alice")).unwrap(), 0);
        registry.set_next_code_for_test("names", u64::from(u32::MAX) + 1);
        assert_eq!(
            registry.encode("names", &name("bob")),
            Err(DictionaryError::Exhausted("names".to_owned()))
        );
        assert_eq!(registry.encode("names", &name("alice")).unwrap(), 0);
    }

    /// A value that is not the canonical inline encoding of the dictionary's
    /// type gets no code, and an undeclared dictionary is refused.
    #[test]
    fn invalid_values_and_unknown_dictionaries_are_refused() {
        let mut registry = registry();
        assert!(matches!(
            registry.encode("names", &[0xff]),
            Err(DictionaryError::InvalidValue { .. })
        ));
        assert_eq!(
            registry.encode("tags", &name("x")),
            Err(DictionaryError::UnknownDictionary("tags".to_owned()))
        );
        assert_eq!(
            registry.decode("names", 0),
            Err(DictionaryError::UnknownCode {
                dictionary: "names".to_owned(),
                code: 0
            })
        );
    }
}

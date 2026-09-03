//! Receiver-local terminal records. Child edits mutate decoded state; encoding
//! the complete root is reserved for an explicitly requested snapshot.

use super::{Error, ErrorCode};
use groove::ivm::{TerminalEdit, TerminalOperation, TerminalPathSegment};
use groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct TerminalRecordState {
    descriptor: RecordDescriptor,
    values: Vec<Value>,
    collections: BTreeMap<usize, TerminalCollectionState>,
}

#[derive(Clone, Debug)]
struct TerminalCollectionState {
    descriptor: RecordDescriptor,
    order: Vec<Vec<u8>>,
    rows: BTreeMap<Vec<u8>, TerminalRecordState>,
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorCode::Protocol, message)
}

impl TerminalRecordState {
    pub(crate) fn new(record: OwnedRecord) -> Result<Self, Error> {
        let values = record
            .borrowed()
            .to_values()
            .map_err(|error| invalid(&format!("invalid retained terminal record: {error}")))?;
        Ok(Self {
            descriptor: *record.descriptor(),
            values,
            collections: BTreeMap::new(),
        })
    }

    pub(crate) fn apply(&mut self, operation: &TerminalOperation) -> Result<(), Error> {
        if self.descriptor != operation.root_descriptor {
            return Err(invalid(
                "terminal descendant descriptor disagrees with its retained root",
            ));
        }
        self.apply_path(&operation.path, &operation.edit)
    }

    /// A compiler terminal update replaces this occurrence's scalar payload.
    /// Collections addressed by child operations have independent lifetimes;
    /// the scalar record's empty placeholders do not retract their contents.
    pub(crate) fn update_record(&mut self, record: OwnedRecord) -> Result<(), Error> {
        if record.descriptor() != &self.descriptor {
            return Err(invalid("terminal update changed its retained descriptor"));
        }
        let mut replacement = Self::new(record)?;
        for index in self.collections.keys() {
            replacement.values[*index] = Value::Array(Vec::new());
        }
        self.values = replacement.values;
        Ok(())
    }

    fn apply_path(
        &mut self,
        path: &[TerminalPathSegment],
        edit: &TerminalEdit,
    ) -> Result<(), Error> {
        let Some((TerminalPathSegment::Collection(name), rest)) = path.split_first() else {
            return Err(invalid(
                "terminal descendant path must begin with a collection",
            ));
        };
        let index = self
            .descriptor
            .field_index(name)
            .ok_or_else(|| invalid("terminal descendant path names an unknown collection"))?;
        if !self.collections.contains_key(&index) {
            let descriptor = match &self.descriptor.fields()[index].value_type {
                ValueType::Array(element) => match element.as_ref() {
                    ValueType::Record(descriptor) => **descriptor,
                    _ => {
                        return Err(invalid(
                            "terminal descendant collection does not contain records",
                        ));
                    }
                },
                _ => return Err(invalid("terminal descendant path names a non-array field")),
            };
            let Some(Value::Array(children)) = self.values.get_mut(index) else {
                return Err(invalid(
                    "terminal descendant collection payload is not an array",
                ));
            };
            let mut collection = TerminalCollectionState {
                descriptor,
                order: Vec::with_capacity(children.len()),
                rows: BTreeMap::new(),
            };
            // Decode this collection once, rather than scanning and allocating
            // keys for every already-retained child on each later edit.
            for child in children.iter() {
                let Value::Record(record) = child else {
                    return Err(invalid(
                        "terminal descendant collection contains a non-record child",
                    ));
                };
                let key = super::terminal_child_key(child)?;
                let state = Self::new(record.clone())?;
                if collection.rows.insert(key.clone(), state).is_some() {
                    return Err(invalid(
                        "terminal descendant collection has duplicate child keys",
                    ));
                }
                collection.order.push(key);
            }
            children.clear();
            self.collections.insert(index, collection);
        }
        let collection = self.collections.get_mut(&index).expect("initialized above");
        match rest {
            [] => collection.apply(edit),
            [TerminalPathSegment::Key(key), tail @ ..] if !tail.is_empty() => collection
                .rows
                .get_mut(key)
                .ok_or_else(|| invalid("terminal child path addressed a missing key"))?
                .apply_path(tail, edit),
            _ => Err(invalid(
                "terminal descendant path does not alternate collection and key",
            )),
        }
    }

    pub(crate) fn record(&self) -> Result<OwnedRecord, Error> {
        let mut values = self.values.clone();
        for (index, collection) in &self.collections {
            values[*index] = Value::Array(
                collection
                    .order
                    .iter()
                    .map(|key| {
                        collection
                            .rows
                            .get(key)
                            .expect("ordered child exists")
                            .record()
                            .map(Value::Record)
                    })
                    .collect::<Result<_, _>>()?,
            );
        }
        let raw = self.descriptor.create(&values).map_err(|error| {
            invalid(&format!("cannot encode retained terminal record: {error}"))
        })?;
        Ok(OwnedRecord::new(raw, self.descriptor))
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        // An allocation-free accounting estimate, not a storage/wire codec.
        postcard::experimental::serialized_size(&self.values).unwrap_or_default()
            + self.values.capacity() * std::mem::size_of::<Value>()
            + self
                .collections
                .values()
                .map(|collection| {
                    collection.order.capacity() * std::mem::size_of::<Vec<u8>>()
                        + collection.order.iter().map(Vec::capacity).sum::<usize>()
                        + collection
                            .rows
                            .iter()
                            .map(|(key, row)| key.capacity() + row.retained_bytes())
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
}

impl TerminalCollectionState {
    fn apply(&mut self, edit: &TerminalEdit) -> Result<(), Error> {
        let key = match edit {
            TerminalEdit::Insert { key, .. }
            | TerminalEdit::Update { key, .. }
            | TerminalEdit::Remove { key }
            | TerminalEdit::Move { key, .. } => key,
        };
        match edit {
            TerminalEdit::Insert { value, .. } | TerminalEdit::Update { value, .. } => {
                let record = OwnedRecord::new(value.clone(), self.descriptor);
                if super::terminal_child_key(&Value::Record(record.clone()))? != *key {
                    return Err(invalid(
                        "terminal child payload key disagrees with edit key",
                    ));
                }
                match edit {
                    TerminalEdit::Insert { index, .. } => {
                        let state = TerminalRecordState::new(record)?;
                        if self.rows.contains_key(key) {
                            self.order.retain(|existing| existing != key);
                        }
                        self.order
                            .insert((*index).min(self.order.len()), key.clone());
                        self.rows.insert(key.clone(), state);
                    }
                    TerminalEdit::Update { .. } => {
                        self.rows
                            .get_mut(key)
                            .ok_or_else(|| {
                                invalid("terminal child update addressed a missing key")
                            })?
                            .update_record(record)?;
                    }
                    _ => unreachable!(),
                }
            }
            TerminalEdit::Remove { .. } => {
                if self.rows.remove(key).is_none() {
                    return Err(invalid("terminal child removal addressed a missing key"));
                }
                self.order.retain(|existing| existing != key);
            }
            TerminalEdit::Move { index, .. } => {
                let previous = self
                    .order
                    .iter()
                    .position(|existing| existing == key)
                    .ok_or_else(|| invalid("terminal child move addressed a missing key"))?;
                let key = self.order.remove(previous);
                self.order.insert((*index).min(self.order.len()), key);
            }
        }
        Ok(())
    }
}

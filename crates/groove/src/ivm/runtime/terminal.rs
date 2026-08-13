//! Structured terminal snapshot ordering and incremental-tree differencing.
//!
//! This is the subscription boundary between relational record deltas and
//! terminal-tree edits. It is intentionally separate from graph evaluation so
//! the encoding and path rules have one local owner.

use std::collections::{BTreeMap, BTreeSet};

use crate::records::{OwnedRecord, RecordDescriptor, Value, ValueType};

use super::{IvmRuntimeError, RecordDeltas, encoded_record_key_part};

/// Incremental edits to a materialized terminal tree. Paths alternate public
/// collection fields and stable descendant keys, starting below `root_key`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TerminalDeltas {
    pub operations: Vec<TerminalOperation>,
}

impl TerminalDeltas {
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TerminalOperation {
    /// Exact descriptor of the root terminal record addressed by this edit.
    ///
    /// Terminal payload bytes are deliberately self-describing at the wire
    /// boundary: a query may legitimately produce either the public logical
    /// record layout or a nullable `CurrentRow` carrier. Consumers must use
    /// this descriptor rather than guessing from a query shape or byte prefix.
    pub root_descriptor: RecordDescriptor,
    pub root_key: Vec<u8>,
    pub path: Vec<TerminalPathSegment>,
    pub edit: TerminalEdit,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TerminalPathSegment {
    Collection(String),
    Key(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TerminalEdit {
    Insert {
        index: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Update {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Remove {
        key: Vec<u8>,
    },
    Move {
        key: Vec<u8>,
        index: usize,
    },
}

pub(super) fn terminal_deltas_from_record_deltas(
    deltas: &RecordDeltas,
) -> Result<TerminalDeltas, IvmRuntimeError> {
    let mut before = BTreeMap::<Vec<u8>, OwnedRecord>::new();
    let mut after = BTreeMap::<Vec<u8>, OwnedRecord>::new();
    for delta in &deltas.deltas {
        let key = encoded_record_key_part(deltas.descriptor, delta.raw(), &[0])?;
        let record = OwnedRecord::new(delta.raw().to_vec(), deltas.descriptor);
        if delta.weight < 0 {
            before.insert(key, record);
        } else if delta.weight > 0 {
            after.insert(key, record);
        }
    }

    let mut keys = BTreeSet::new();
    keys.extend(before.keys().cloned());
    keys.extend(after.keys().cloned());
    let mut operations = Vec::new();
    for key in keys {
        match (before.get(&key), after.get(&key)) {
            (None, Some(record)) => operations.push(TerminalOperation {
                root_descriptor: deltas.descriptor,
                root_key: key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Insert {
                    index: 0,
                    key: key.clone(),
                    value: record.raw().to_vec(),
                },
            }),
            (Some(_), None) => operations.push(TerminalOperation {
                root_descriptor: deltas.descriptor,
                root_key: key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Remove { key: key.clone() },
            }),
            (Some(before), Some(after)) => diff_terminal_record(
                &key,
                deltas.descriptor,
                Vec::new(),
                before,
                after,
                &mut operations,
            )?,
            (None, None) => unreachable!("terminal key came from before or after"),
        }
    }
    Ok(TerminalDeltas { operations })
}

pub(super) fn order_terminal_snapshot(
    terminal: &mut RecordDeltas,
    ordering: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    let mut positions = BTreeMap::new();
    for (index, delta) in ordering
        .deltas
        .iter()
        .filter(|delta| delta.weight > 0)
        .enumerate()
    {
        let key = encoded_record_key_part(ordering.descriptor, delta.raw(), &[0])?;
        positions.entry(key).or_insert(index);
    }
    let mut keyed = terminal
        .deltas
        .drain(..)
        .map(|delta| {
            let key = encoded_record_key_part(terminal.descriptor, delta.raw(), &[0])?;
            let position = positions.get(&key).copied().unwrap_or(usize::MAX);
            Ok((position, key, delta))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    terminal.deltas = keyed.into_iter().map(|(_, _, delta)| delta).collect();
    Ok(())
}

fn diff_terminal_record(
    root_key: &[u8],
    root_descriptor: RecordDescriptor,
    path: Vec<TerminalPathSegment>,
    before: &OwnedRecord,
    after: &OwnedRecord,
    operations: &mut Vec<TerminalOperation>,
) -> Result<(), IvmRuntimeError> {
    let before_values = before
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    let after_values = after.to_values().map_err(IvmRuntimeError::RecordEncoding)?;
    let mut scalar_changed = false;
    for (index, (before_value, after_value)) in before_values.iter().zip(&after_values).enumerate()
    {
        let descriptor_field = after
            .descriptor()
            .fields()
            .get(index)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?;
        match (before_value, after_value) {
            (Value::Array(before_children), Value::Array(after_children))
                if matches!(
                    &descriptor_field.value_type,
                    ValueType::Array(inner) if matches!(inner.as_ref(), ValueType::Record(_))
                ) =>
            {
                let field = descriptor_field
                    .name
                    .clone()
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
                let mut child_path = path.clone();
                child_path.push(TerminalPathSegment::Collection(field));
                diff_terminal_collection(
                    root_key,
                    root_descriptor,
                    child_path,
                    before_children,
                    after_children,
                    operations,
                )?;
            }
            _ if before_value != after_value => scalar_changed = true,
            _ => {}
        }
    }
    if scalar_changed {
        let key = encoded_record_key_part(*after.descriptor(), after.raw(), &[0])?;
        operations.push(TerminalOperation {
            root_descriptor,
            root_key: root_key.to_vec(),
            path,
            edit: TerminalEdit::Update {
                key,
                value: after.raw().to_vec(),
            },
        });
    }
    Ok(())
}

fn diff_terminal_collection(
    root_key: &[u8],
    root_descriptor: RecordDescriptor,
    path: Vec<TerminalPathSegment>,
    before_values: &[Value],
    after_values: &[Value],
    operations: &mut Vec<TerminalOperation>,
) -> Result<(), IvmRuntimeError> {
    let children =
        |values: &[Value]| -> Result<BTreeMap<Vec<u8>, (usize, OwnedRecord)>, IvmRuntimeError> {
            let mut children = BTreeMap::new();
            for (index, value) in values.iter().enumerate() {
                let Value::Record(record) = value else {
                    return Err(IvmRuntimeError::InvalidCollectBy(
                        "structured terminal arrays must contain records".to_owned(),
                    ));
                };
                let key = encoded_record_key_part(*record.descriptor(), record.raw(), &[0])?;
                if children.insert(key, (index, record.clone())).is_some() {
                    return Err(IvmRuntimeError::DuplicateCollectByOccurrenceId);
                }
            }
            Ok(children)
        };
    let before = children(before_values)?;
    let after = children(after_values)?;
    let mut keys = BTreeSet::new();
    keys.extend(before.keys().cloned());
    keys.extend(after.keys().cloned());
    for key in keys {
        match (before.get(&key), after.get(&key)) {
            (None, Some((index, record))) => operations.push(TerminalOperation {
                root_descriptor,
                root_key: root_key.to_vec(),
                path: path.clone(),
                edit: TerminalEdit::Insert {
                    index: *index,
                    key: key.clone(),
                    value: record.raw().to_vec(),
                },
            }),
            (Some(_), None) => operations.push(TerminalOperation {
                root_descriptor,
                root_key: root_key.to_vec(),
                path: path.clone(),
                edit: TerminalEdit::Remove { key: key.clone() },
            }),
            (Some((before_index, before_record)), Some((after_index, after_record))) => {
                if before_index != after_index {
                    operations.push(TerminalOperation {
                        root_descriptor,
                        root_key: root_key.to_vec(),
                        path: path.clone(),
                        edit: TerminalEdit::Move {
                            key: key.clone(),
                            index: *after_index,
                        },
                    });
                }
                let mut descendant_path = path.clone();
                descendant_path.push(TerminalPathSegment::Key(key));
                diff_terminal_record(
                    root_key,
                    root_descriptor,
                    descendant_path,
                    before_record,
                    after_record,
                    operations,
                )?;
            }
            (None, None) => unreachable!("child key came from before or after"),
        }
    }
    Ok(())
}

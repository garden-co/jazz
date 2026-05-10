use ahash::{AHashMap, AHashSet};
use std::collections::HashSet;

use crate::object::ObjectId;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::graph_nodes::policy_eval::{
    PolicyContextEvaluator, collect_policy_dependency_tables,
};
use crate::query_manager::magic_columns::{MagicColumnKind, magic_column_descriptor};
use crate::query_manager::policy::Operation;
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    LoadedRow, Row, RowDescriptor, RowPolicyMode, Schema, TableName, Tuple, TupleDelta,
    TupleDescriptor, TupleElement, Value,
};
use crate::storage::Storage;

use super::RowNode;

fn tuple_content_changed(old: &Tuple, new: &Tuple) -> bool {
    old.iter().zip(new.iter()).any(
        |(old_element, new_element)| match (old_element, new_element) {
            (
                TupleElement::Row {
                    content: old_content,
                    batch_id: old_commit,
                    ..
                },
                TupleElement::Row {
                    content: new_content,
                    batch_id: new_commit,
                    ..
                },
            ) => old_content != new_content || old_commit != new_commit,
            (TupleElement::Id(_), TupleElement::Id(_)) => false,
            _ => true,
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MagicColumnRequest {
    pub element_index: usize,
    pub table_name: TableName,
    pub kind: MagicColumnKind,
}

#[derive(Debug, Clone)]
struct ElementMagicColumns {
    element_index: usize,
    table_name: TableName,
    kinds: Vec<MagicColumnKind>,
}

#[derive(Debug)]
pub struct MagicColumnsNode {
    input_tuple_descriptor: TupleDescriptor,
    output_tuple_descriptor: TupleDescriptor,
    output_descriptor: RowDescriptor,
    element_requests: Vec<ElementMagicColumns>,
    session: Option<Session>,
    schema: Schema,
    branch: String,
    row_policy_mode: RowPolicyMode,
    dependency_tables: HashSet<String>,
    dependency_dirty: bool,
    current_tuples: AHashSet<Tuple>,
    input_tuples: AHashSet<Tuple>,
    projected_by_input: AHashMap<Tuple, Tuple>,
    dirty: bool,
}

impl MagicColumnsNode {
    pub(crate) fn new_with_policy_mode(
        input_tuple_descriptor: TupleDescriptor,
        requests: &[MagicColumnRequest],
        session: Option<Session>,
        schema: Schema,
        branch: impl Into<String>,
        row_policy_mode: RowPolicyMode,
    ) -> Option<Self> {
        if requests.is_empty() {
            return None;
        }

        let mut grouped = AHashMap::<usize, ElementMagicColumns>::new();
        for request in requests {
            let element =
                grouped
                    .entry(request.element_index)
                    .or_insert_with(|| ElementMagicColumns {
                        element_index: request.element_index,
                        table_name: request.table_name,
                        kinds: Vec::new(),
                    });
            if !element.kinds.contains(&request.kind) {
                element.kinds.push(request.kind);
            }
        }

        let mut tables = Vec::with_capacity(input_tuple_descriptor.element_count());
        let mut dependency_tables = HashSet::new();

        for (element_index, element) in input_tuple_descriptor.iter().enumerate() {
            let mut descriptor = element.descriptor.clone();

            if let Some(requests) = grouped.get(&element_index) {
                for kind in &requests.kinds {
                    descriptor.columns.push(magic_column_descriptor(*kind));
                }

                if session.is_some()
                    && let Some(table_schema) = schema.get(&requests.table_name)
                {
                    for kind in &requests.kinds {
                        if matches!(
                            kind,
                            MagicColumnKind::CanRead
                                | MagicColumnKind::CanEdit
                                | MagicColumnKind::CanDelete
                        ) {
                            let policy = match kind {
                                MagicColumnKind::CanRead => {
                                    table_schema.policies.select.using.as_ref()
                                }
                                MagicColumnKind::CanEdit => {
                                    table_schema.policies.update.using.as_ref()
                                }
                                MagicColumnKind::CanDelete => {
                                    table_schema.policies.effective_delete_using()
                                }
                                MagicColumnKind::CreatedBy
                                | MagicColumnKind::CreatedAt
                                | MagicColumnKind::UpdatedBy
                                | MagicColumnKind::UpdatedAt => None,
                            };
                            if let Some(policy) = policy {
                                dependency_tables.extend(collect_policy_dependency_tables(
                                    policy,
                                    &table_schema.columns,
                                ));
                            }
                        }
                    }
                }
            }

            tables.push((element.table, descriptor));
        }

        let output_tuple_descriptor = TupleDescriptor::from_tables(&tables).with_all_materialized();
        let output_descriptor = output_tuple_descriptor.combined_descriptor();

        Some(Self {
            input_tuple_descriptor,
            output_tuple_descriptor,
            output_descriptor,
            element_requests: grouped.into_values().collect(),
            session,
            schema,
            branch: branch.into(),
            row_policy_mode,
            dependency_tables,
            dependency_dirty: false,
            current_tuples: AHashSet::new(),
            input_tuples: AHashSet::new(),
            projected_by_input: AHashMap::new(),
            dirty: true,
        })
    }

    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    pub fn dependency_tables(&self) -> &HashSet<String> {
        &self.dependency_tables
    }

    pub fn mark_dependency_dirty(&mut self) {
        self.dependency_dirty = true;
    }

    pub fn process_with_context(
        &mut self,
        input: TupleDelta,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> TupleDelta {
        let mut result = TupleDelta::default();

        if self.dependency_dirty {
            self.dependency_dirty = false;
            result = self.reevaluate_all_with_context(io, row_loader);
        }

        if !self.dirty
            && input.added.is_empty()
            && input.removed.is_empty()
            && input.updated.is_empty()
        {
            return result;
        }

        for tuple in input.added {
            self.input_tuples.insert(tuple.clone());
            let Some(projected) = self.augment_tuple_with_context(&tuple, io, row_loader) else {
                continue;
            };
            self.projected_by_input
                .insert(tuple.clone(), projected.clone());
            self.current_tuples.insert(projected.clone());
            result.added.push(projected);
        }

        for tuple in input.removed {
            self.input_tuples.remove(&tuple);
            if let Some(projected) = self.projected_by_input.remove(&tuple)
                && self.current_tuples.remove(&projected)
            {
                result.removed.push(projected);
            }
        }

        for (old_tuple, new_tuple) in input.updated {
            self.input_tuples.remove(&old_tuple);
            self.input_tuples.insert(new_tuple.clone());

            let old_projected = self.projected_by_input.remove(&old_tuple);
            let new_projected = self.augment_tuple_with_context(&new_tuple, io, row_loader);

            match (old_projected, new_projected) {
                (Some(old_projected), Some(new_projected)) => {
                    self.projected_by_input
                        .insert(new_tuple.clone(), new_projected.clone());
                    if tuple_content_changed(&old_projected, &new_projected) {
                        self.current_tuples.remove(&old_projected);
                        self.current_tuples.insert(new_projected.clone());
                        result.updated.push((old_projected, new_projected));
                    }
                }
                (Some(old_projected), None) => {
                    self.current_tuples.remove(&old_projected);
                    result.removed.push(old_projected);
                }
                (None, Some(new_projected)) => {
                    self.projected_by_input
                        .insert(new_tuple.clone(), new_projected.clone());
                    self.current_tuples.insert(new_projected.clone());
                    result.added.push(new_projected);
                }
                (None, None) => {}
            }
        }

        self.dirty = false;
        result
    }

    fn reevaluate_all_with_context(
        &mut self,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> TupleDelta {
        let mut result = TupleDelta::default();
        let input_tuples: Vec<_> = self.input_tuples.iter().cloned().collect();

        for tuple in input_tuples {
            let current = self.projected_by_input.get(&tuple).cloned();
            let updated = self.augment_tuple_with_context(&tuple, io, row_loader);

            match (current, updated) {
                (Some(current), Some(updated)) => {
                    if tuple_content_changed(&current, &updated) {
                        self.projected_by_input
                            .insert(tuple.clone(), updated.clone());
                        self.current_tuples.remove(&current);
                        self.current_tuples.insert(updated.clone());
                        result.updated.push((current, updated));
                    }
                }
                (Some(current), None) => {
                    self.projected_by_input.remove(&tuple);
                    self.current_tuples.remove(&current);
                    result.removed.push(current);
                }
                (None, Some(updated)) => {
                    self.projected_by_input
                        .insert(tuple.clone(), updated.clone());
                    self.current_tuples.insert(updated.clone());
                    result.added.push(updated);
                }
                (None, None) => {}
            }
        }

        self.dirty = false;
        result
    }

    fn augment_tuple_with_context(
        &self,
        tuple: &Tuple,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> Option<Tuple> {
        let mut projected = tuple.clone();

        for request in &self.element_requests {
            let input_element = tuple.get(request.element_index)?;
            let input_descriptor = &self
                .input_tuple_descriptor
                .element(request.element_index)?
                .descriptor;
            let output_descriptor = &self
                .output_tuple_descriptor
                .element(request.element_index)?
                .descriptor;
            let row = input_element.to_row()?;
            let mut values = decode_row(input_descriptor, input_element.content()?).ok()?;

            for kind in &request.kinds {
                values.push(self.evaluate_magic_column(
                    *kind,
                    request.table_name,
                    &row,
                    input_descriptor,
                    io,
                    row_loader,
                ));
            }

            let new_content = encode_row(output_descriptor, &values).ok()?;
            let element = projected.get_mut(request.element_index)?;
            *element = TupleElement::Row {
                id: row.id,
                content: new_content.into(),
                batch_id: row.batch_id,
                row_provenance: row.provenance,
            };
        }

        Some(projected)
    }

    fn evaluate_magic_column(
        &self,
        kind: MagicColumnKind,
        table_name: TableName,
        row: &Row,
        descriptor: &RowDescriptor,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> Value {
        match kind {
            MagicColumnKind::CreatedBy => Value::Text(row.provenance.created_by.clone()),
            MagicColumnKind::CreatedAt => Value::Timestamp(row.provenance.created_at),
            MagicColumnKind::UpdatedBy => Value::Text(row.provenance.updated_by.clone()),
            MagicColumnKind::UpdatedAt => Value::Timestamp(row.provenance.updated_at),
            MagicColumnKind::CanRead | MagicColumnKind::CanEdit | MagicColumnKind::CanDelete => {
                let Some(session) = self.session.as_ref() else {
                    return Value::Null;
                };

                let evaluator = PolicyContextEvaluator::new(
                    &self.schema,
                    session,
                    &self.branch,
                    self.row_policy_mode,
                );
                let operation = match kind {
                    MagicColumnKind::CanRead => Operation::Select,
                    MagicColumnKind::CanEdit => Operation::Update,
                    MagicColumnKind::CanDelete => Operation::Delete,
                    MagicColumnKind::CreatedBy
                    | MagicColumnKind::CreatedAt
                    | MagicColumnKind::UpdatedBy
                    | MagicColumnKind::UpdatedAt => unreachable!(),
                };
                let mut visited = HashSet::new();
                let allowed = evaluator.evaluate_row_access(
                    operation,
                    row,
                    descriptor,
                    table_name.as_str(),
                    None,
                    io,
                    row_loader,
                    0,
                    &mut visited,
                );
                Value::Boolean(allowed)
            }
        }
    }
}

impl RowNode for MagicColumnsNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // The graph settlement loop should always use `process_with_context` so
        // relation-backed clauses evaluate against real storage state.
        self.input_tuples.extend(input.added.iter().cloned());
        self.dirty = false;
        TupleDelta::default()
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty || self.dependency_dirty
    }
}

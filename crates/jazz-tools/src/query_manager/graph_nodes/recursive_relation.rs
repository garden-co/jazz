//! RecursiveRelationNode for bounded, unrolled recursive relation evaluation.
//!
//! This node is intentionally naive:
//! - full recompute on seed/inner-table changes,
//! - per-level subgraph instantiation,
//! - deterministic dedupe by normalized row content.

use ahash::{AHashMap, AHashSet};
use uuid::Uuid;

use crate::commit::CommitId;
use crate::metadata::{RowProvenance, SYSTEM_PRINCIPAL_ID};
use crate::object::ObjectId;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::types::{
    LoadedRow, Row, RowDescriptor, Schema, TableName, Tuple, TupleDelta, TupleDescriptor,
    TupleElement, TupleProvenance, Value,
};
use crate::storage::Storage;

use super::RowNode;
use super::subgraph::SubgraphTemplate;

/// Source of the value used to correlate recursive steps.
#[derive(Debug, Clone, Copy)]
pub enum CorrelationSource {
    /// Correlate using a column from the recursive output row.
    Column(usize),
    /// Correlate using the row object id.
    ObjectId,
}

/// Optional hop applied to each recursive step row.
#[derive(Debug, Clone)]
pub struct RecursiveHop {
    /// Target table reached by hop.
    pub table: TableName,
    /// Column index on the step row containing the target row id.
    pub step_column_index: usize,
}

/// Node that evaluates recursive relations using bounded unrolling.
#[derive(Debug)]
pub struct RecursiveRelationNode {
    /// Descriptor for incoming seed tuples.
    input_descriptor: TupleDescriptor,
    /// Descriptor for normalized recursive rows.
    output_descriptor: RowDescriptor,
    /// Template for recursive step evaluation.
    step_template: SubgraphTemplate,
    /// Schema used to compile step subgraphs.
    schema: Schema,
    /// Value source used for step correlation.
    correlation_source: CorrelationSource,
    /// Optional hop from step rows to target rows.
    hop: Option<RecursiveHop>,
    /// Maximum recursion depth (levels beyond seed level).
    max_depth: usize,
    /// Current seed tuples keyed by input row id.
    seed_tuples: AHashMap<ObjectId, Tuple>,
    /// Current output tuples.
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
    /// True when inner step dependencies changed.
    inner_dirty: bool,
}

impl RecursiveRelationNode {
    /// Create a new recursive relation node.
    pub fn new(
        input_descriptor: TupleDescriptor,
        output_descriptor: RowDescriptor,
        step_template: SubgraphTemplate,
        correlation_source: CorrelationSource,
        hop: Option<RecursiveHop>,
        max_depth: usize,
        schema: Schema,
    ) -> Self {
        Self {
            input_descriptor,
            output_descriptor,
            step_template,
            schema,
            correlation_source,
            hop,
            max_depth,
            seed_tuples: AHashMap::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
            inner_dirty: false,
        }
    }

    /// Mark the recursive step dependency as dirty.
    pub fn mark_inner_dirty(&mut self) {
        self.inner_dirty = true;
    }

    /// Check if recursive step dependency is dirty.
    pub fn is_inner_dirty(&self) -> bool {
        self.inner_dirty
    }

    /// Process seed tuple deltas with query context.
    pub fn process_with_context<F>(
        &mut self,
        input: TupleDelta,
        io: &dyn Storage,
        mut row_loader: F,
    ) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        self.apply_seed_delta(input);

        if !self.dirty && !self.inner_dirty {
            return TupleDelta::default();
        }

        let next = self.recompute(io, &mut row_loader);
        let delta = diff_sets(&self.current_tuples, &next);

        self.current_tuples = next;
        self.dirty = false;
        self.inner_dirty = false;
        delta
    }

    fn apply_seed_delta(&mut self, input: TupleDelta) {
        if input.is_empty() {
            return;
        }

        for tuple in input.removed {
            if let Some(id) = tuple.first_id() {
                self.seed_tuples.remove(&id);
            }
        }

        for tuple in input.added {
            if let Some(id) = tuple.first_id() {
                self.seed_tuples.insert(id, tuple);
            }
        }

        for (old_tuple, new_tuple) in input.updated {
            if let Some(old_id) = old_tuple.first_id() {
                self.seed_tuples.remove(&old_id);
            }
            if let Some(new_id) = new_tuple.first_id() {
                self.seed_tuples.insert(new_id, new_tuple);
            }
        }

        self.dirty = true;
    }

    fn recompute(
        &self,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> AHashSet<Tuple> {
        if self.hop.is_some() {
            return self.recompute_with_hop(io, row_loader);
        }
        if matches!(self.correlation_source, CorrelationSource::ObjectId) {
            return self.recompute_with_object_id(io, row_loader);
        }

        let mut seen_contents = AHashMap::<Vec<u8>, TupleProvenance>::new();
        let mut frontier_contents = Vec::<(Vec<u8>, TupleProvenance)>::new();

        for tuple in self.seed_tuples.values() {
            if let Some(content) = self.normalize_seed_tuple(tuple) {
                let provenance = tuple.provenance().clone();
                let entry = seen_contents.entry(content.clone()).or_default();
                let previous_len = entry.len();
                for scoped_object in provenance.iter().copied() {
                    entry.insert(scoped_object);
                }
                if previous_len == 0 || entry.len() > previous_len {
                    frontier_contents.push((content, entry.clone()));
                }
            }
        }

        for _level in 0..self.max_depth {
            if frontier_contents.is_empty() {
                break;
            }

            let mut next_frontier = Vec::<(Vec<u8>, TupleProvenance)>::new();

            for (content, frontier_provenance) in frontier_contents {
                let corr = match self.extract_correlation_from_content(None, &content) {
                    Some(v) => v,
                    None => continue,
                };

                for (step_content, step_provenance) in self.evaluate_step(&corr, io, row_loader) {
                    let mut combined_provenance = frontier_provenance.clone();
                    for scoped_object in step_provenance.iter().copied() {
                        combined_provenance.insert(scoped_object);
                    }

                    let entry = seen_contents.entry(step_content.clone()).or_default();
                    let previous_len = entry.len();
                    for scoped_object in combined_provenance.iter().copied() {
                        entry.insert(scoped_object);
                    }
                    if previous_len == 0 || entry.len() > previous_len {
                        next_frontier.push((step_content, entry.clone()));
                    }
                }
            }

            frontier_contents = next_frontier;
        }

        seen_contents
            .into_iter()
            .map(|(content, provenance)| tuple_from_normalized_content(content, provenance))
            .collect()
    }

    fn recompute_with_object_id(
        &self,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> AHashSet<Tuple> {
        let mut seen_rows =
            AHashMap::<ObjectId, (Vec<u8>, CommitId, RowProvenance, TupleProvenance)>::new();
        let mut frontier = Vec::<(ObjectId, Vec<u8>, TupleProvenance)>::new();

        for tuple in self.seed_tuples.values() {
            if let Some((id, content, version_id, row_provenance)) =
                self.normalize_seed_tuple_with_id(tuple)
            {
                let provenance = tuple.provenance().clone();
                let enqueue = match seen_rows.get_mut(&id) {
                    Some((
                        existing_content,
                        existing_commit_id,
                        existing_row_provenance,
                        existing_provenance,
                    )) => {
                        let previous_len = existing_provenance.len();
                        for scoped_object in provenance.iter().copied() {
                            existing_provenance.insert(scoped_object);
                        }
                        let changed =
                            *existing_content != content || *existing_commit_id != version_id;
                        if changed {
                            *existing_content = content.clone();
                            *existing_commit_id = version_id;
                            *existing_row_provenance = row_provenance.clone();
                        }
                        changed || existing_provenance.len() > previous_len
                    }
                    None => {
                        seen_rows.insert(
                            id,
                            (
                                content.clone(),
                                version_id,
                                row_provenance,
                                provenance.clone(),
                            ),
                        );
                        true
                    }
                };
                if enqueue {
                    let frontier_provenance = seen_rows
                        .get(&id)
                        .map(|(_, _, _, provenance)| provenance.clone())
                        .unwrap_or(provenance);
                    frontier.push((id, content, frontier_provenance));
                }
            }
        }

        let step_desc = self.step_template.output_descriptor().clone();

        for _level in 0..self.max_depth {
            if frontier.is_empty() {
                break;
            }

            let mut next_frontier = Vec::<(ObjectId, Vec<u8>, TupleProvenance)>::new();

            for (row_id, _content, frontier_provenance) in frontier {
                let corr = Value::Uuid(row_id);

                for (step_row, step_provenance) in self.evaluate_step_rows(&corr, io, row_loader) {
                    let Some((next_id, next_content, next_commit_id, next_row_provenance)) =
                        self.normalize_step_row(&step_desc, &step_row)
                    else {
                        continue;
                    };

                    let mut combined_provenance = frontier_provenance.clone();
                    for scoped_object in step_provenance.iter().copied() {
                        combined_provenance.insert(scoped_object);
                    }

                    let enqueue = match seen_rows.get_mut(&next_id) {
                        Some((
                            existing_content,
                            existing_commit_id,
                            existing_row_provenance,
                            existing_provenance,
                        )) => {
                            let previous_len = existing_provenance.len();
                            for scoped_object in combined_provenance.iter().copied() {
                                existing_provenance.insert(scoped_object);
                            }
                            let changed = *existing_content != next_content
                                || *existing_commit_id != next_commit_id;
                            if changed {
                                *existing_content = next_content.clone();
                                *existing_commit_id = next_commit_id;
                                *existing_row_provenance = next_row_provenance.clone();
                            }
                            changed || existing_provenance.len() > previous_len
                        }
                        None => {
                            seen_rows.insert(
                                next_id,
                                (
                                    next_content.clone(),
                                    next_commit_id,
                                    next_row_provenance,
                                    combined_provenance.clone(),
                                ),
                            );
                            true
                        }
                    };
                    if enqueue {
                        let frontier_provenance = seen_rows
                            .get(&next_id)
                            .map(|(_, _, _, provenance)| provenance.clone())
                            .unwrap_or(combined_provenance);
                        next_frontier.push((next_id, next_content, frontier_provenance));
                    }
                }
            }

            frontier = next_frontier;
        }

        seen_rows
            .into_iter()
            .map(|(id, (content, version_id, row_provenance, provenance))| {
                Tuple::new_with_provenance(
                    vec![TupleElement::Row {
                        id,
                        content: content.into(),
                        version_id,
                        row_provenance,
                    }],
                    provenance,
                )
            })
            .collect()
    }

    fn recompute_with_hop(
        &self,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> AHashSet<Tuple> {
        let Some(hop) = &self.hop else {
            return AHashSet::new();
        };

        let mut seen_rows =
            AHashMap::<ObjectId, (Vec<u8>, CommitId, RowProvenance, TupleProvenance)>::new();
        let mut frontier = Vec::<(ObjectId, Vec<u8>, TupleProvenance)>::new();

        for tuple in self.seed_tuples.values() {
            if let Some((id, content, version_id, row_provenance)) =
                self.normalize_seed_tuple_with_id(tuple)
            {
                let provenance = tuple.provenance().clone();
                let enqueue = match seen_rows.get_mut(&id) {
                    Some((
                        existing_content,
                        existing_commit_id,
                        existing_row_provenance,
                        existing_provenance,
                    )) => {
                        let previous_len = existing_provenance.len();
                        for scoped_object in provenance.iter().copied() {
                            existing_provenance.insert(scoped_object);
                        }
                        let changed =
                            *existing_content != content || *existing_commit_id != version_id;
                        if changed {
                            *existing_content = content.clone();
                            *existing_commit_id = version_id;
                            *existing_row_provenance = row_provenance.clone();
                        }
                        changed || existing_provenance.len() > previous_len
                    }
                    None => {
                        seen_rows.insert(
                            id,
                            (
                                content.clone(),
                                version_id,
                                row_provenance,
                                provenance.clone(),
                            ),
                        );
                        true
                    }
                };
                if enqueue {
                    let frontier_provenance = seen_rows
                        .get(&id)
                        .map(|(_, _, _, provenance)| provenance.clone())
                        .unwrap_or(provenance);
                    frontier.push((id, content, frontier_provenance));
                }
            }
        }

        for _level in 0..self.max_depth {
            if frontier.is_empty() {
                break;
            }

            let mut next_frontier = Vec::<(ObjectId, Vec<u8>, TupleProvenance)>::new();

            for (row_id, content, frontier_provenance) in frontier {
                let corr = match self.extract_correlation_from_content(Some(row_id), &content) {
                    Some(v) => v,
                    None => continue,
                };

                for (step_row, step_provenance) in self.evaluate_step_rows(&corr, io, row_loader) {
                    let step_values =
                        match decode_row(self.step_template.output_descriptor(), &step_row.data) {
                            Ok(values) => values,
                            Err(_) => continue,
                        };
                    let Some(Value::Uuid(target_id)) = step_values.get(hop.step_column_index)
                    else {
                        continue;
                    };
                    let Some(target_row) = row_loader(*target_id, Some(hop.table)) else {
                        continue;
                    };
                    if decode_row(&self.output_descriptor, &target_row.data).is_err() {
                        continue;
                    }
                    let mut combined_provenance = frontier_provenance.clone();
                    for scoped_object in step_provenance.iter().copied() {
                        combined_provenance.insert(scoped_object);
                    }
                    for scoped_object in target_row.provenance.iter().copied() {
                        combined_provenance.insert(scoped_object);
                    }

                    let enqueue = match seen_rows.get_mut(target_id) {
                        Some((
                            existing_content,
                            existing_commit_id,
                            existing_row_provenance,
                            existing_provenance,
                        )) => {
                            let previous_len = existing_provenance.len();
                            for scoped_object in combined_provenance.iter().copied() {
                                existing_provenance.insert(scoped_object);
                            }
                            let changed = *existing_content != target_row.data
                                || *existing_commit_id != target_row.version_id;
                            if changed {
                                *existing_content = target_row.data.to_vec();
                                *existing_commit_id = target_row.version_id;
                                *existing_row_provenance = target_row.row_provenance.clone();
                            }
                            changed || existing_provenance.len() > previous_len
                        }
                        None => {
                            seen_rows.insert(
                                *target_id,
                                (
                                    target_row.data.to_vec(),
                                    target_row.version_id,
                                    target_row.row_provenance.clone(),
                                    combined_provenance.clone(),
                                ),
                            );
                            true
                        }
                    };
                    if enqueue {
                        let frontier_provenance = seen_rows
                            .get(target_id)
                            .map(|(_, _, _, provenance)| provenance.clone())
                            .unwrap_or(combined_provenance);
                        next_frontier.push((
                            *target_id,
                            target_row.data.to_vec(),
                            frontier_provenance,
                        ));
                    }
                }
            }

            frontier = next_frontier;
        }

        seen_rows
            .into_iter()
            .map(|(id, (content, version_id, row_provenance, provenance))| {
                Tuple::new_with_provenance(
                    vec![TupleElement::Row {
                        id,
                        content: content.into(),
                        version_id,
                        row_provenance,
                    }],
                    provenance,
                )
            })
            .collect()
    }

    fn normalize_seed_tuple(&self, tuple: &Tuple) -> Option<Vec<u8>> {
        let element = tuple.get(0)?;
        let content = element.content()?;
        let in_desc = self.input_descriptor.combined_descriptor();
        let values = decode_row(&in_desc, content).ok()?;
        if values.len() != self.output_descriptor.columns.len() {
            return None;
        }
        encode_row(&self.output_descriptor, &values).ok()
    }

    fn normalize_seed_tuple_with_id(
        &self,
        tuple: &Tuple,
    ) -> Option<(ObjectId, Vec<u8>, CommitId, RowProvenance)> {
        let element = tuple.get(0)?;
        let id = element.id();
        let version_id = element.version_id().unwrap_or(CommitId([0; 32]));
        let row_provenance = element.row_provenance()?.clone();
        let content = self.normalize_seed_tuple(tuple)?;
        Some((id, content, version_id, row_provenance))
    }

    fn extract_correlation_from_content(
        &self,
        row_id: Option<ObjectId>,
        normalized_content: &[u8],
    ) -> Option<Value> {
        match self.correlation_source {
            CorrelationSource::ObjectId => row_id.map(Value::Uuid),
            CorrelationSource::Column(correlation_col) => {
                let values = decode_row(&self.output_descriptor, normalized_content).ok()?;
                values.get(correlation_col).cloned()
            }
        }
    }

    fn evaluate_step(
        &self,
        correlation_value: &Value,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> Vec<(Vec<u8>, TupleProvenance)> {
        let step_desc = self.step_template.output_descriptor().clone();
        self.evaluate_step_rows(correlation_value, io, row_loader)
            .into_iter()
            .filter_map(|(row, provenance)| {
                let values = decode_row(&step_desc, &row.data).ok()?;
                if values.len() != self.output_descriptor.columns.len() {
                    return None;
                }
                Some((
                    encode_row(&self.output_descriptor, &values).ok()?,
                    provenance,
                ))
            })
            .collect()
    }

    fn evaluate_step_rows(
        &self,
        correlation_value: &Value,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> Vec<(crate::query_manager::types::Row, TupleProvenance)> {
        let mut instance = match self
            .step_template
            .instantiate(correlation_value.clone(), &self.schema)
        {
            Some(instance) => instance,
            None => return Vec::new(),
        };
        let _delta = instance
            .graph
            .settle(io, &mut |id, hint| row_loader(id, hint));
        instance.graph.current_output_rows_with_provenance()
    }

    fn normalize_step_row(
        &self,
        step_descriptor: &RowDescriptor,
        step_row: &Row,
    ) -> Option<(ObjectId, Vec<u8>, crate::commit::CommitId, RowProvenance)> {
        let values = decode_row(step_descriptor, &step_row.data).ok()?;
        if values.len() != self.output_descriptor.columns.len() {
            return None;
        }
        let normalized_content = encode_row(&self.output_descriptor, &values).ok()?;
        Some((
            step_row.id,
            normalized_content,
            step_row.version_id,
            step_row.provenance.clone(),
        ))
    }
}

impl RowNode for RecursiveRelationNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // Without context we can't settle recursive step subgraphs.
        // Keep seed bookkeeping and defer real evaluation to process_with_context.
        self.apply_seed_delta(input);
        TupleDelta::default()
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

fn tuple_from_normalized_content(content: Vec<u8>, provenance: TupleProvenance) -> Tuple {
    // Stable synthetic id by row content for deterministic dedupe.
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, &content);
    let id = ObjectId::from_uuid(uuid);
    let version_id = CommitId([0; 32]);
    Tuple::new_with_provenance(
        vec![TupleElement::Row {
            id,
            content: content.into(),
            version_id,
            row_provenance: RowProvenance::for_insert(SYSTEM_PRINCIPAL_ID, 0),
        }],
        provenance,
    )
}

fn diff_sets(old_set: &AHashSet<Tuple>, new_set: &AHashSet<Tuple>) -> TupleDelta {
    let mut delta = TupleDelta::new();
    let mut old_by_ids = AHashMap::<Vec<ObjectId>, &Tuple>::new();
    let mut new_by_ids = AHashMap::<Vec<ObjectId>, &Tuple>::new();

    for tuple in old_set {
        old_by_ids.insert(tuple.ids(), tuple);
    }
    for tuple in new_set {
        new_by_ids.insert(tuple.ids(), tuple);
    }

    for (ids, old_tuple) in &old_by_ids {
        let Some(new_tuple) = new_by_ids.get(ids) else {
            delta.removed.push((*old_tuple).clone());
            continue;
        };

        let old_content = old_tuple.get(0).and_then(|e| e.content());
        let new_content = new_tuple.get(0).and_then(|e| e.content());
        if old_content != new_content || old_tuple.provenance() != new_tuple.provenance() {
            delta
                .updated
                .push(((*old_tuple).clone(), (*new_tuple).clone()));
        }
    }

    for (ids, new_tuple) in &new_by_ids {
        if !old_by_ids.contains_key(ids) {
            delta.added.push((*new_tuple).clone());
        }
    }

    delta
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::graph_nodes::subgraph::SubgraphBuilder;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TableName};

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("teams"),
            RowDescriptor::new(vec![ColumnDescriptor::new("team_id", ColumnType::Integer)]).into(),
        );
        schema.insert(
            TableName::new("team_edges"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("child_team", ColumnType::Integer),
                ColumnDescriptor::new("parent_team", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn recursive_node_uses_expected_output_descriptor() {
        let schema = test_schema();
        let output_desc = schema
            .get(&TableName::new("teams"))
            .unwrap()
            .columns
            .clone();
        let input_desc =
            TupleDescriptor::single_with_materialization("", output_desc.clone(), true);
        let step = SubgraphBuilder::new("team_edges")
            .correlate("child_team")
            .select(&["parent_team"])
            .build(&schema)
            .unwrap();

        let node = RecursiveRelationNode::new(
            input_desc,
            output_desc.clone(),
            step,
            CorrelationSource::Column(0),
            None,
            10,
            schema,
        );
        assert_eq!(node.output_descriptor(), &output_desc);
        assert_eq!(node.max_depth, 10);
    }

    #[test]
    fn recursive_node_without_context_is_deferred() {
        let schema = test_schema();
        let output_desc = schema
            .get(&TableName::new("teams"))
            .unwrap()
            .columns
            .clone();
        let input_desc =
            TupleDescriptor::single_with_materialization("", output_desc.clone(), true);
        let step = SubgraphBuilder::new("team_edges")
            .correlate("child_team")
            .select(&["parent_team"])
            .build(&schema)
            .unwrap();

        let mut node = RecursiveRelationNode::new(
            input_desc,
            output_desc,
            step,
            CorrelationSource::Column(0),
            None,
            10,
            schema.clone(),
        );

        let seed_desc = &schema.get(&TableName::new("teams")).unwrap().columns;
        let seed = encode_row(seed_desc, &[Value::Integer(1)]).unwrap();
        let seed_tuple = Tuple::new(vec![TupleElement::Row {
            id: ObjectId::new(),
            content: seed.into(),
            version_id: CommitId([0; 32]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }]);
        let mut input = TupleDelta::new();
        input.added.push(seed_tuple);

        let out = node.process(input);
        assert!(out.is_empty());
        assert!(node.is_dirty());
    }
}

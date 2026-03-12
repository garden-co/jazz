use ahash::AHashSet;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Bound;

use crate::object::{BranchName, ObjectId};
use crate::query_manager::encoding::{compare_column, decode_column, encode_value};
use crate::query_manager::graph_nodes::filter::FilterNode;
use crate::query_manager::graph_nodes::policy_filter::PolicyFilterNode;
use crate::query_manager::graph_nodes::sort::SortDirection;
use crate::query_manager::graph_nodes::tuple_delta::compute_tuple_delta;
use crate::query_manager::query::{Condition, Conjunction};
use crate::query_manager::types::{
    ColumnType, LoadedRow, Row, RowDescriptor, SchemaHash, TableName, Tuple, TupleDelta,
    TupleDescriptor, TupleElement, TupleProvenance, Value,
};
use crate::schema_manager::{SchemaContext, translate_column_for_index};
use crate::storage::{IndexScanDirection, OrderedIndexCursor, OrderedIndexScan, Storage};

use super::{SourceContext, SourceNode};

const DRIVER_BATCH_SIZE: usize = 64;
const DIRECT_REQUIRED_IDS_MIN: usize = 256;
const DIRECT_REQUIRED_IDS_PREFIX_MULTIPLIER: usize = 4;

#[derive(Debug)]
pub(crate) struct IndexedQueryNodeConfig {
    pub branches: Vec<String>,
    pub branch_schema_map: std::collections::HashMap<String, SchemaHash>,
    pub schema_context: SchemaContext,
    pub tuple_descriptor: TupleDescriptor,
    pub table_descriptors: Vec<RowDescriptor>,
    pub disjuncts: Vec<Conjunction>,
    pub driver: DriverPlan,
    pub join_edges: Vec<JoinEdgePlan>,
    pub residual_filter: Option<FilterNode>,
    pub policies: Vec<TablePolicySpec>,
    pub limit: Option<usize>,
    pub offset: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct DriverPlan {
    pub scope_index: usize,
    pub table: TableName,
    pub key: ResolvedRowKey,
    pub direction: IndexScanDirection,
    pub sort_keys: Vec<ResolvedSortKey>,
}

#[derive(Debug, Clone)]
pub(crate) struct JoinEdgePlan {
    pub left_scope_index: usize,
    pub right_scope_index: usize,
    pub left_table: TableName,
    pub right_table: TableName,
    pub left_key: ResolvedRowKey,
    pub right_key: ResolvedRowKey,
}

#[derive(Debug)]
pub(crate) struct TablePolicySpec {
    pub scope_index: usize,
    pub evaluators_by_branch: std::collections::HashMap<String, PolicyFilterNode>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedRowKey {
    pub logical_column: String,
    pub local_col_index: Option<usize>,
    pub use_row_id: bool,
    pub expand_array: bool,
}

impl ResolvedRowKey {
    pub(crate) fn from_descriptor(descriptor: &RowDescriptor, raw: &str) -> Option<Self> {
        let column = raw.split('.').next_back().unwrap_or(raw);
        if let Some(local_col_index) = descriptor.column_index(column) {
            let expand_array = matches!(
                descriptor.columns[local_col_index].column_type,
                ColumnType::Array { .. }
            );
            return Some(Self {
                logical_column: column.to_string(),
                local_col_index: Some(local_col_index),
                use_row_id: false,
                expand_array,
            });
        }

        (column == "id" || column == "_id").then_some(Self {
            logical_column: "_id".to_string(),
            local_col_index: None,
            use_row_id: true,
            expand_array: false,
        })
    }

    pub(crate) fn index_column(&self) -> &str {
        if self.use_row_id {
            "_id"
        } else {
            self.logical_column.as_str()
        }
    }

    pub(crate) fn matches_selector(&self, descriptor: &RowDescriptor, selector: &str) -> bool {
        let column = selector.split('.').next_back().unwrap_or(selector);
        if self.use_row_id {
            (column == "id" || column == "_id") && descriptor.column_index(column).is_none()
        } else {
            column == self.logical_column
        }
    }

    pub(crate) fn extract_value(&self, row: &Row, descriptor: &RowDescriptor) -> Option<Value> {
        if self.use_row_id {
            return Some(Value::Uuid(row.id));
        }

        decode_column(descriptor, &row.data, self.local_col_index?).ok()
    }

    fn extract_lookup_values(&self, row: &Row, descriptor: &RowDescriptor) -> Vec<Value> {
        let Some(value) = self.extract_value(row, descriptor) else {
            return Vec::new();
        };
        if value == Value::Null {
            return Vec::new();
        }

        if self.expand_array
            && let Value::Array(values) = value
        {
            return values
                .into_iter()
                .filter(|value| *value != Value::Null)
                .collect();
        }

        vec![value]
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedSortKey {
    pub target: ResolvedSortTarget,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub(crate) enum ResolvedSortTarget {
    Column {
        element_index: usize,
        descriptor: RowDescriptor,
        local_col_index: usize,
    },
    RowId {
        element_index: usize,
    },
}

#[derive(Debug)]
pub struct IndexedQueryNode {
    config: IndexedQueryNodeConfig,
    ordered_tuples: Vec<Tuple>,
    sync_input_tuples: Vec<Tuple>,
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

#[derive(Debug, Clone)]
struct StreamState {
    branch: String,
    translated_driver_column: String,
    start: Bound<Value>,
    end: Bound<Value>,
    required_ids: Option<AHashSet<ObjectId>>,
    cursor: Option<OrderedIndexCursor>,
    buffer: VecDeque<DriverCandidate>,
    exhausted: bool,
}

#[derive(Debug, Clone)]
struct DriverCandidate {
    row: Row,
    provenance: TupleProvenance,
    branch: String,
    lead_key_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ScopedRow {
    row: Row,
    provenance: TupleProvenance,
    branch: String,
}

#[derive(Debug, Clone)]
struct PartialTuple {
    rows: Vec<Option<ScopedRow>>,
}

impl IndexedQueryNode {
    pub(crate) fn new(config: IndexedQueryNodeConfig) -> Self {
        Self {
            config,
            ordered_tuples: Vec::new(),
            sync_input_tuples: Vec::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    pub fn windowed_tuples(&self) -> &[Tuple] {
        &self.ordered_tuples
    }

    pub fn sync_input_tuples(&self) -> &[Tuple] {
        &self.sync_input_tuples
    }

    fn desired_prefix_len(&self) -> Option<usize> {
        self.config
            .limit
            .map(|limit| self.config.offset.saturating_add(limit))
    }

    fn max_direct_required_ids(&self) -> usize {
        self.desired_prefix_len()
            .map(|prefix_len| {
                prefix_len
                    .saturating_mul(DIRECT_REQUIRED_IDS_PREFIX_MULTIPLIER)
                    .max(DIRECT_REQUIRED_IDS_MIN)
            })
            .unwrap_or(DIRECT_REQUIRED_IDS_MIN)
    }

    fn build_streams(&self, storage: &dyn Storage) -> Vec<StreamState> {
        let mut streams = Vec::new();
        for branch in &self.config.branches {
            let branch_schema_hash = self.config.branch_schema_map.get(branch).copied();
            let translated_driver_column = if let Some(target_hash) = branch_schema_hash {
                if target_hash != self.config.schema_context.current_hash {
                    translate_column_for_index(
                        &self.config.schema_context,
                        self.config.driver.table.as_str(),
                        self.config.driver.key.index_column(),
                        &target_hash,
                    )
                    .unwrap_or_else(|| self.config.driver.key.index_column().to_string())
                } else {
                    self.config.driver.key.index_column().to_string()
                }
            } else {
                self.config.driver.key.index_column().to_string()
            };

            for disjunct in &self.config.disjuncts {
                let required_ids =
                    self.required_driver_ids(storage, branch, &translated_driver_column, disjunct);
                let (start, end) = self.driver_bounds(disjunct);
                streams.push(StreamState {
                    branch: branch.clone(),
                    translated_driver_column: translated_driver_column.clone(),
                    start,
                    end,
                    required_ids,
                    cursor: None,
                    buffer: VecDeque::new(),
                    exhausted: false,
                });
            }
        }

        streams
    }

    fn required_driver_ids(
        &self,
        storage: &dyn Storage,
        branch: &str,
        _translated_driver_column: &str,
        disjunct: &Conjunction,
    ) -> Option<AHashSet<ObjectId>> {
        let driver_element = self
            .config
            .tuple_descriptor
            .element(self.config.driver.scope_index)?;
        let mut intersection: Option<AHashSet<ObjectId>> = None;

        for condition in &disjunct.conditions {
            let Some(scope_index) = self.resolve_condition_scope(condition) else {
                continue;
            };
            if scope_index != self.config.driver.scope_index {
                continue;
            }

            let Condition::Eq { column, value } = condition else {
                continue;
            };
            if self
                .config
                .driver
                .key
                .matches_selector(&driver_element.descriptor, column)
            {
                continue;
            }

            let translated_column =
                self.translate_index_column(branch, self.config.driver.table, column);
            let ids = storage
                .index_lookup(
                    self.config.driver.table.as_str(),
                    &translated_column,
                    branch,
                    value,
                )
                .into_iter()
                .collect::<AHashSet<_>>();

            if let Some(existing) = &mut intersection {
                existing.retain(|id| ids.contains(id));
            } else {
                intersection = Some(ids);
            }
        }

        intersection
    }

    fn driver_bounds(&self, disjunct: &Conjunction) -> (Bound<Value>, Bound<Value>) {
        let Some(driver_descriptor) = self
            .config
            .tuple_descriptor
            .element(self.config.driver.scope_index)
        else {
            return (Bound::Unbounded, Bound::Unbounded);
        };

        let mut conditions = Vec::new();
        for condition in &disjunct.conditions {
            let Some(scope_index) = self.resolve_condition_scope(condition) else {
                continue;
            };
            if scope_index != self.config.driver.scope_index {
                continue;
            }
            if !self
                .config
                .driver
                .key
                .matches_selector(&driver_descriptor.descriptor, condition.column())
            {
                continue;
            }
            if !condition.is_index_scannable() {
                continue;
            }
            conditions.push(condition.clone());
        }

        if conditions.is_empty() {
            return (Bound::Unbounded, Bound::Unbounded);
        }

        bounds_for_driver_conditions(&Conjunction { conditions })
            .unwrap_or((Bound::Unbounded, Bound::Unbounded))
    }

    fn resolve_condition_scope(&self, condition: &Condition) -> Option<usize> {
        let raw_column = condition.raw_column();
        let column = raw_column.split('.').next_back().unwrap_or(raw_column);

        if let Some((scope, _)) = raw_column.rsplit_once('.') {
            let scope: &str = scope.trim();
            return (0..self.config.tuple_descriptor.element_count()).find(|index| {
                self.config
                    .tuple_descriptor
                    .element(*index)
                    .is_some_and(|element| element.table == scope)
            });
        }

        let matches: Vec<_> = (0..self.config.tuple_descriptor.element_count())
            .filter(|index| {
                self.config
                    .tuple_descriptor
                    .element(*index)
                    .is_some_and(|element| element.descriptor.column_index(column).is_some())
            })
            .collect();
        if matches.len() == 1 {
            return matches.into_iter().next();
        }

        if (column == "id" || column == "_id") && self.config.tuple_descriptor.element_count() == 1
        {
            let element = self.config.tuple_descriptor.element(0)?;
            if element.descriptor.column_index(column).is_none() {
                return Some(0);
            }
        }

        None
    }

    fn translate_index_column(&self, branch: &str, table: TableName, column: &str) -> String {
        let logical_column = column.split('.').next_back().unwrap_or(column);
        let branch_schema_hash = self.config.branch_schema_map.get(branch).copied();
        if let Some(target_hash) = branch_schema_hash
            && target_hash != self.config.schema_context.current_hash
        {
            return translate_column_for_index(
                &self.config.schema_context,
                table.as_str(),
                logical_column,
                &target_hash,
            )
            .unwrap_or_else(|| logical_column.to_string());
        }
        logical_column.to_string()
    }

    fn ensure_stream_head(
        &self,
        stream: &mut StreamState,
        ctx: &SourceContext,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    ) {
        if self.seed_stream_from_required_ids(stream, ctx.storage, row_loader) {
            return;
        }

        while stream.buffer.is_empty() && !stream.exhausted {
            let cursors = ctx.storage.index_scan_ordered(OrderedIndexScan {
                table: self.config.driver.table.as_str(),
                column: &stream.translated_driver_column,
                branch: &stream.branch,
                start: borrow_bound(&stream.start),
                end: borrow_bound(&stream.end),
                direction: self.config.driver.direction,
                take: Some(DRIVER_BATCH_SIZE),
                resume_after: stream.cursor.as_ref(),
            });

            if cursors.is_empty() {
                stream.exhausted = true;
                break;
            }

            let branch_name = BranchName::new(&stream.branch);
            for cursor in &cursors {
                stream.cursor = Some(cursor.clone());

                if let Some(required_ids) = &stream.required_ids
                    && !required_ids.contains(&cursor.row_id)
                {
                    continue;
                }

                let Some(loaded) = row_loader(cursor.row_id) else {
                    continue;
                };
                let row = Row::new(cursor.row_id, loaded.data, loaded.commit_id);
                let provenance = if loaded.provenance.is_empty() {
                    [(cursor.row_id, branch_name)].into_iter().collect()
                } else {
                    loaded.provenance
                };
                let scoped_row = ScopedRow {
                    row,
                    provenance,
                    branch: stream.branch.clone(),
                };
                if !self.row_passes_policy(
                    self.config.driver.scope_index,
                    &scoped_row,
                    ctx.storage,
                    row_loader,
                ) {
                    continue;
                }

                stream.buffer.push_back(DriverCandidate {
                    lead_key_bytes: encode_value(&cursor.value),
                    row: scoped_row.row,
                    provenance: scoped_row.provenance,
                    branch: scoped_row.branch,
                });
            }

            if stream.buffer.is_empty() && cursors.len() < DRIVER_BATCH_SIZE {
                stream.exhausted = true;
            }
        }
    }

    fn seed_stream_from_required_ids(
        &self,
        stream: &mut StreamState,
        storage: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    ) -> bool {
        // Small exact-match candidate sets are cheaper to load and sort directly
        // than to rediscover by walking the full ordered index on every refresh.
        if stream.cursor.is_some() || !stream.buffer.is_empty() || stream.exhausted {
            return false;
        }

        let Some(required_ids) = stream.required_ids.as_ref() else {
            return false;
        };
        if required_ids.len() > self.max_direct_required_ids() {
            return false;
        }

        let Some(driver_descriptor) = self
            .config
            .tuple_descriptor
            .element(self.config.driver.scope_index)
            .map(|element| &element.descriptor)
        else {
            return false;
        };

        let branch_name = BranchName::new(&stream.branch);
        let mut candidates = Vec::with_capacity(required_ids.len());
        for row_id in required_ids {
            let Some(loaded) = row_loader(*row_id) else {
                continue;
            };
            let row = Row::new(*row_id, loaded.data, loaded.commit_id);
            let provenance = if loaded.provenance.is_empty() {
                [(*row_id, branch_name)].into_iter().collect()
            } else {
                loaded.provenance
            };
            let scoped_row = ScopedRow {
                row,
                provenance,
                branch: stream.branch.clone(),
            };
            if !self.row_passes_policy(
                self.config.driver.scope_index,
                &scoped_row,
                storage,
                row_loader,
            ) {
                continue;
            }
            let Some(lead_value) = self
                .config
                .driver
                .key
                .extract_value(&scoped_row.row, driver_descriptor)
            else {
                continue;
            };

            candidates.push(DriverCandidate {
                lead_key_bytes: encode_value(&lead_value),
                row: scoped_row.row,
                provenance: scoped_row.provenance,
                branch: scoped_row.branch,
            });
        }

        candidates.sort_by(|left, right| self.compare_candidates(left, right));
        stream.buffer.extend(candidates);
        stream.exhausted = true;
        true
    }

    fn row_passes_policy(
        &self,
        scope_index: usize,
        scoped_row: &ScopedRow,
        storage: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    ) -> bool {
        let Some(policy) = self
            .config
            .policies
            .iter()
            .find(|policy| policy.scope_index == scope_index)
        else {
            return true;
        };
        let Some(evaluator) = policy.evaluators_by_branch.get(&scoped_row.branch) else {
            return true;
        };
        evaluator.evaluate_with_context(&scoped_row.row, storage, row_loader)
    }

    fn compare_candidates(&self, left: &DriverCandidate, right: &DriverCandidate) -> Ordering {
        match self.config.driver.direction {
            IndexScanDirection::Ascending => left
                .lead_key_bytes
                .cmp(&right.lead_key_bytes)
                .then_with(|| left.row.id.cmp(&right.row.id)),
            IndexScanDirection::Descending => right
                .lead_key_bytes
                .cmp(&left.lead_key_bytes)
                .then_with(|| left.row.id.cmp(&right.row.id)),
        }
    }

    fn build_joined_tuples(
        &self,
        candidate: &DriverCandidate,
        ctx: &SourceContext,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    ) -> Vec<Tuple> {
        let mut seed = PartialTuple {
            rows: vec![None; self.config.table_descriptors.len()],
        };
        seed.rows[self.config.driver.scope_index] = Some(ScopedRow {
            row: candidate.row.clone(),
            provenance: candidate.provenance.clone(),
            branch: candidate.branch.clone(),
        });

        self.expand_from_segment(
            self.config.driver.scope_index,
            self.config.driver.scope_index,
            seed,
            ctx.storage,
            row_loader,
        )
        .into_iter()
        .filter_map(|partial| self.partial_tuple_to_tuple(partial))
        .filter(|tuple| {
            self.config
                .residual_filter
                .as_ref()
                .is_none_or(|filter| filter.evaluate_tuple(tuple))
        })
        .collect()
    }

    fn expand_from_segment(
        &self,
        left_index: usize,
        right_index: usize,
        partial: PartialTuple,
        storage: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    ) -> Vec<PartialTuple> {
        if left_index > 0 {
            let edge = &self.config.join_edges[left_index - 1];
            let Some(known_row) = partial.rows[left_index].as_ref() else {
                return Vec::new();
            };
            let matches = self.lookup_join_matches(storage, row_loader, known_row, edge, false);
            let mut expanded = Vec::new();
            for matched in matches {
                let mut next = partial.clone();
                next.rows[left_index - 1] = Some(matched);
                expanded.extend(self.expand_from_segment(
                    left_index - 1,
                    right_index,
                    next,
                    storage,
                    row_loader,
                ));
            }
            return expanded;
        }

        if right_index + 1 < self.config.table_descriptors.len() {
            let edge = &self.config.join_edges[right_index];
            let Some(known_row) = partial.rows[right_index].as_ref() else {
                return Vec::new();
            };
            let matches = self.lookup_join_matches(storage, row_loader, known_row, edge, true);
            let mut expanded = Vec::new();
            for matched in matches {
                let mut next = partial.clone();
                next.rows[right_index + 1] = Some(matched);
                expanded.extend(self.expand_from_segment(
                    left_index,
                    right_index + 1,
                    next,
                    storage,
                    row_loader,
                ));
            }
            return expanded;
        }

        vec![partial]
    }

    fn lookup_join_matches(
        &self,
        storage: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
        known_row: &ScopedRow,
        edge: &JoinEdgePlan,
        probe_right: bool,
    ) -> Vec<ScopedRow> {
        let (source_key, target_key, target_table, target_scope_index) = if probe_right {
            (
                &edge.left_key,
                &edge.right_key,
                edge.right_table,
                edge.right_scope_index,
            )
        } else {
            (
                &edge.right_key,
                &edge.left_key,
                edge.left_table,
                edge.left_scope_index,
            )
        };

        let source_descriptor = &self.config.table_descriptors[if probe_right {
            edge.left_scope_index
        } else {
            edge.right_scope_index
        }];
        let expected_values = source_key.extract_lookup_values(&known_row.row, source_descriptor);
        if expected_values.is_empty() {
            return Vec::new();
        }

        let translated_column =
            self.translate_index_column(&known_row.branch, target_table, target_key.index_column());
        let mut candidate_ids = AHashSet::new();
        for value in &expected_values {
            candidate_ids.extend(storage.index_lookup(
                target_table.as_str(),
                &translated_column,
                &known_row.branch,
                value,
            ));
        }

        let mut ids: Vec<_> = candidate_ids.into_iter().collect();
        ids.sort_unstable();

        let branch_name = BranchName::new(&known_row.branch);
        let target_descriptor = &self.config.table_descriptors[target_scope_index];
        let mut matched_rows = Vec::new();
        for row_id in ids {
            let Some(loaded) = row_loader(row_id) else {
                continue;
            };
            let row = Row::new(row_id, loaded.data, loaded.commit_id);
            let candidate = ScopedRow {
                row,
                provenance: if loaded.provenance.is_empty() {
                    [(row_id, branch_name)].into_iter().collect()
                } else {
                    loaded.provenance
                },
                branch: known_row.branch.clone(),
            };
            if !self.row_passes_policy(target_scope_index, &candidate, storage, row_loader) {
                continue;
            }

            let actual_values = target_key.extract_lookup_values(&candidate.row, target_descriptor);
            if !actual_values
                .iter()
                .any(|value| expected_values.contains(value))
            {
                continue;
            }

            matched_rows.push(candidate);
        }

        matched_rows
    }

    fn partial_tuple_to_tuple(&self, partial: PartialTuple) -> Option<Tuple> {
        let mut elements = Vec::with_capacity(partial.rows.len());
        let mut provenance = TupleProvenance::default();
        for scoped_row in partial.rows {
            let scoped_row = scoped_row?;
            provenance.extend(scoped_row.provenance.iter().copied());
            elements.push(TupleElement::Row {
                id: scoped_row.row.id,
                content: scoped_row.row.data,
                commit_id: scoped_row.row.commit_id,
            });
        }

        Some(Tuple::new_with_provenance(elements, provenance))
    }

    fn compare_tuples(&self, left: &Tuple, right: &Tuple) -> Ordering {
        for key in &self.config.driver.sort_keys {
            let ord = key.compare(left, right);
            if ord != Ordering::Equal {
                return ord;
            }
        }

        left.ids().cmp(&right.ids())
    }

    fn scan_internal(
        &mut self,
        ctx: &SourceContext,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    ) -> TupleDelta {
        let mut streams = self.build_streams(ctx.storage);
        let mut ordered_prefix = Vec::new();
        let mut seen_output = AHashSet::new();
        let mut seen_driver_ids = AHashSet::new();
        let desired_prefix_len = self.desired_prefix_len();

        loop {
            for stream in &mut streams {
                self.ensure_stream_head(stream, ctx, row_loader);
            }

            let next_stream_index = streams
                .iter()
                .enumerate()
                .filter_map(|(index, stream)| {
                    stream.buffer.front().map(|candidate| (index, candidate))
                })
                .min_by(|(_, left), (_, right)| self.compare_candidates(left, right))
                .map(|(index, _)| index);
            let Some(next_stream_index) = next_stream_index else {
                break;
            };

            let Some(first_candidate) = streams[next_stream_index].buffer.front().cloned() else {
                break;
            };
            let lead_key = first_candidate.lead_key_bytes.clone();

            let mut group_candidates = Vec::new();
            loop {
                let mut progressed = false;
                for stream in &mut streams {
                    self.ensure_stream_head(stream, ctx, row_loader);
                    while let Some(candidate) = stream.buffer.front() {
                        if candidate.lead_key_bytes != lead_key {
                            break;
                        }
                        let candidate = stream.buffer.pop_front().expect("candidate front");
                        progressed = true;
                        if seen_driver_ids.insert(candidate.row.id) {
                            group_candidates.push(candidate);
                        }
                    }
                }
                if !progressed {
                    break;
                }
            }

            if group_candidates.is_empty() {
                continue;
            }

            let mut group_tuples = Vec::new();
            for candidate in &group_candidates {
                group_tuples.extend(self.build_joined_tuples(candidate, ctx, row_loader));
            }
            group_tuples.sort_by(|left, right| self.compare_tuples(left, right));

            for tuple in group_tuples {
                if seen_output.insert(tuple.clone()) {
                    ordered_prefix.push(tuple);
                }
            }

            if desired_prefix_len.is_some_and(|needed| ordered_prefix.len() >= needed) {
                break;
            }
        }

        let old_window = std::mem::take(&mut self.ordered_tuples);
        self.sync_input_tuples = ordered_prefix.clone();
        let sync_scope = self
            .sync_input_tuples
            .iter()
            .flat_map(|tuple| tuple.provenance().iter().copied())
            .collect();

        let start = self.config.offset.min(ordered_prefix.len());
        let end = match self.config.limit {
            Some(limit) => start.saturating_add(limit).min(ordered_prefix.len()),
            None => ordered_prefix.len(),
        };
        self.ordered_tuples = ordered_prefix[start..end]
            .iter()
            .cloned()
            .map(|mut tuple| {
                tuple.merge_provenance(&sync_scope);
                tuple
            })
            .collect();
        self.current_tuples = self.ordered_tuples.iter().cloned().collect();
        self.dirty = false;

        compute_tuple_delta(&old_window, &self.ordered_tuples)
    }

    pub fn scan_with_context<F>(&mut self, ctx: &SourceContext, row_loader: &mut F) -> TupleDelta
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
    {
        self.scan_internal(ctx, row_loader)
    }
}

impl SourceNode for IndexedQueryNode {
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta {
        self.scan_internal(ctx, &mut |_| None)
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

impl ResolvedSortKey {
    fn compare(&self, left: &Tuple, right: &Tuple) -> Ordering {
        let ord = match &self.target {
            ResolvedSortTarget::Column {
                element_index,
                descriptor,
                local_col_index,
            } => {
                let left_content = left
                    .get(*element_index)
                    .and_then(|element| element.content());
                let right_content = right
                    .get(*element_index)
                    .and_then(|element| element.content());
                match (left_content, right_content) {
                    (Some(left_data), Some(right_data)) => compare_column(
                        descriptor,
                        left_data,
                        *local_col_index,
                        right_data,
                        *local_col_index,
                    )
                    .unwrap_or(Ordering::Equal),
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                }
            }
            ResolvedSortTarget::RowId { element_index } => left
                .get(*element_index)
                .map(|element| element.id())
                .cmp(&right.get(*element_index).map(|element| element.id())),
        };

        match self.direction {
            SortDirection::Ascending => ord,
            SortDirection::Descending => ord.reverse(),
        }
    }
}

fn borrow_bound(bound: &Bound<Value>) -> Bound<&Value> {
    match bound {
        Bound::Included(value) => Bound::Included(value),
        Bound::Excluded(value) => Bound::Excluded(value),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn bounds_for_driver_conditions(conjunction: &Conjunction) -> Option<(Bound<Value>, Bound<Value>)> {
    let mut lower: Option<Bound<Value>> = None;
    let mut upper: Option<Bound<Value>> = None;
    let mut exact: Option<Value> = None;

    for condition in &conjunction.conditions {
        match condition {
            Condition::Eq { value, .. } => {
                if lower.is_some() || upper.is_some() || exact.is_some() {
                    return None;
                }
                exact = Some(value.clone());
            }
            Condition::Gt { value, .. } => {
                if lower.is_some() || exact.is_some() {
                    return None;
                }
                lower = Some(Bound::Excluded(value.clone()));
            }
            Condition::Ge { value, .. } => {
                if lower.is_some() || exact.is_some() {
                    return None;
                }
                lower = Some(Bound::Included(value.clone()));
            }
            Condition::Lt { value, .. } => {
                if upper.is_some() || exact.is_some() {
                    return None;
                }
                upper = Some(Bound::Excluded(value.clone()));
            }
            Condition::Le { value, .. } => {
                if upper.is_some() || exact.is_some() {
                    return None;
                }
                upper = Some(Bound::Included(value.clone()));
            }
            Condition::Between { min, max, .. } => {
                if lower.is_some() || upper.is_some() || exact.is_some() {
                    return None;
                }
                lower = Some(Bound::Included(min.clone()));
                upper = Some(Bound::Included(max.clone()));
            }
            _ => return None,
        }
    }

    if let Some(value) = exact {
        return Some((Bound::Included(value.clone()), Bound::Included(value)));
    }

    Some((
        lower.unwrap_or(Bound::Unbounded),
        upper.unwrap_or(Bound::Unbounded),
    ))
}

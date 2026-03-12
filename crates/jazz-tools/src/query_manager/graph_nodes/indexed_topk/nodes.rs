use ahash::AHashSet;
use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use crate::object::{BranchName, ObjectId};
use crate::query_manager::encoding::{compare_column, encode_value};
use crate::query_manager::graph_nodes::tuple_delta::compute_tuple_delta;
use crate::query_manager::types::{LoadedRow, Row, Tuple, TupleElement, TupleProvenance, Value};
use crate::storage::{IndexScanDirection, OrderedIndexCursor, OrderedIndexScan, Storage};

use super::plan::{
    JoinLookupSpec, MergeOrderedSpec, OrderedDriverSourceSpec, ProbeJoinSpec, ResolvedRowKey,
    ResolvedSortKey, ResolvedSortTarget, ScopedPolicySpec, TieSortSpec,
};

const DRIVER_BATCH_SIZE: usize = 64;

#[derive(Debug, Default)]
struct OrderedNodeState {
    ordered_tuples: Vec<Tuple>,
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl OrderedNodeState {
    fn new_dirty() -> Self {
        Self {
            dirty: true,
            ..Self::default()
        }
    }

    fn ordered_tuples(&self) -> &[Tuple] {
        &self.ordered_tuples
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

    fn replace_ordered(&mut self, next: Vec<Tuple>) -> crate::query_manager::types::TupleDelta {
        let old_tuples = std::mem::take(&mut self.ordered_tuples);
        self.current_tuples = next.iter().cloned().collect();
        self.ordered_tuples = next;
        self.dirty = false;
        compute_tuple_delta(&old_tuples, &self.ordered_tuples)
    }
}

#[derive(Debug, Clone)]
struct ScopedRow {
    row: Row,
    provenance: TupleProvenance,
    branch: BranchName,
}

#[derive(Debug, Clone)]
struct PartialTuple {
    rows: Vec<Option<ScopedRow>>,
}

#[derive(Debug, Clone)]
struct DriverCandidate {
    row: Row,
    provenance: TupleProvenance,
    lead_key_bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct OrderedDriverSourceNode {
    spec: Arc<OrderedDriverSourceSpec>,
    state: OrderedNodeState,
}

impl OrderedDriverSourceNode {
    pub(crate) fn new(spec: Arc<OrderedDriverSourceSpec>) -> Self {
        Self {
            spec,
            state: OrderedNodeState::new_dirty(),
        }
    }

    pub fn ordered_tuples(&self) -> &[Tuple] {
        self.state.ordered_tuples()
    }

    pub fn current_tuples(&self) -> &AHashSet<Tuple> {
        self.state.current_tuples()
    }

    pub fn mark_dirty(&mut self) {
        self.state.mark_dirty();
    }

    pub fn is_dirty(&self) -> bool {
        self.state.is_dirty()
    }

    pub fn process_with_context<F>(
        &mut self,
        storage: &dyn Storage,
        row_loader: &mut F,
    ) -> crate::query_manager::types::TupleDelta
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
    {
        let required_ids = required_driver_ids(&self.spec, storage);
        let ordered = if required_ids
            .as_ref()
            .is_some_and(|ids| ids.len() <= self.spec.max_direct_required_ids)
        {
            load_direct_candidates(&self.spec, required_ids.as_ref(), storage, row_loader)
        } else {
            scan_ordered_candidates(&self.spec, required_ids.as_ref(), storage, row_loader)
        };
        self.state.replace_ordered(ordered)
    }
}

#[derive(Debug)]
pub struct MergeOrderedNode {
    spec: Arc<MergeOrderedSpec>,
    state: OrderedNodeState,
}

impl MergeOrderedNode {
    pub(crate) fn new(spec: Arc<MergeOrderedSpec>) -> Self {
        Self {
            spec,
            state: OrderedNodeState::new_dirty(),
        }
    }

    pub fn ordered_tuples(&self) -> &[Tuple] {
        self.state.ordered_tuples()
    }

    pub fn current_tuples(&self) -> &AHashSet<Tuple> {
        self.state.current_tuples()
    }

    pub fn mark_dirty(&mut self) {
        self.state.mark_dirty();
    }

    pub fn is_dirty(&self) -> bool {
        self.state.is_dirty()
    }

    pub fn process_ordered_inputs(
        &mut self,
        inputs: &[&[Tuple]],
    ) -> crate::query_manager::types::TupleDelta {
        let mut positions = vec![0usize; inputs.len()];
        let mut ordered = Vec::new();
        let mut seen_driver_ids = AHashSet::new();

        loop {
            let next_index = inputs
                .iter()
                .enumerate()
                .filter_map(|(index, tuples)| {
                    tuples.get(positions[index]).map(|tuple| (index, tuple))
                })
                .min_by(|(_, left), (_, right)| compare_driver_tuples(&self.spec, left, right))
                .map(|(index, _)| index);
            let Some(next_index) = next_index else {
                break;
            };

            let tuple = inputs[next_index][positions[next_index]].clone();
            positions[next_index] += 1;
            if tuple
                .first_id()
                .is_some_and(|driver_id| seen_driver_ids.insert(driver_id))
            {
                ordered.push(tuple);
            }
        }

        self.state.replace_ordered(ordered)
    }
}

#[derive(Debug)]
pub struct ProbeJoinNode {
    spec: Arc<ProbeJoinSpec>,
    state: OrderedNodeState,
}

impl ProbeJoinNode {
    pub(crate) fn new(spec: Arc<ProbeJoinSpec>) -> Self {
        Self {
            spec,
            state: OrderedNodeState::new_dirty(),
        }
    }

    pub fn ordered_tuples(&self) -> &[Tuple] {
        self.state.ordered_tuples()
    }

    pub fn current_tuples(&self) -> &AHashSet<Tuple> {
        self.state.current_tuples()
    }

    pub fn mark_dirty(&mut self) {
        self.state.mark_dirty();
    }

    pub fn is_dirty(&self) -> bool {
        self.state.is_dirty()
    }

    pub fn process_with_context<F>(
        &mut self,
        ordered_driver_tuples: &[Tuple],
        storage: &dyn Storage,
        row_loader: &mut F,
    ) -> crate::query_manager::types::TupleDelta
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
    {
        let mut ordered = Vec::new();
        let mut seen_output = AHashSet::new();

        for tuple in ordered_driver_tuples {
            for joined in build_joined_tuples(&self.spec, tuple, storage, row_loader) {
                if seen_output.insert(joined.clone()) {
                    ordered.push(joined);
                }
            }
        }

        self.state.replace_ordered(ordered)
    }
}

#[derive(Debug)]
pub struct TieSortNode {
    spec: Arc<TieSortSpec>,
    state: OrderedNodeState,
}

impl TieSortNode {
    pub(crate) fn new(spec: Arc<TieSortSpec>) -> Self {
        Self {
            spec,
            state: OrderedNodeState::new_dirty(),
        }
    }

    pub fn ordered_tuples(&self) -> &[Tuple] {
        self.state.ordered_tuples()
    }

    pub fn current_tuples(&self) -> &AHashSet<Tuple> {
        self.state.current_tuples()
    }

    pub fn mark_dirty(&mut self) {
        self.state.mark_dirty();
    }

    pub fn is_dirty(&self) -> bool {
        self.state.is_dirty()
    }

    pub fn process_ordered_input(
        &mut self,
        ordered_tuples: &[Tuple],
    ) -> crate::query_manager::types::TupleDelta {
        let mut ordered = Vec::new();
        let mut seen_output = AHashSet::new();
        let mut index = 0usize;

        while index < ordered_tuples.len() {
            let Some(group_key) = lead_key_bytes_for_full_tuple(&self.spec, &ordered_tuples[index])
            else {
                index += 1;
                continue;
            };

            let mut group = Vec::new();
            while index < ordered_tuples.len()
                && lead_key_bytes_for_full_tuple(&self.spec, &ordered_tuples[index])
                    .is_some_and(|lead_key| lead_key == group_key)
            {
                group.push(ordered_tuples[index].clone());
                index += 1;
            }
            group.sort_by(|left, right| compare_tuples(&self.spec, left, right));

            for tuple in group {
                if seen_output.insert(tuple.clone()) {
                    ordered.push(tuple);
                }
            }

            if self
                .spec
                .desired_prefix_len
                .is_some_and(|needed| ordered.len() >= needed)
            {
                ordered.truncate(self.spec.desired_prefix_len.unwrap_or(ordered.len()));
                break;
            }
        }

        self.state.replace_ordered(ordered)
    }
}

fn required_driver_ids(
    spec: &OrderedDriverSourceSpec,
    storage: &dyn Storage,
) -> Option<AHashSet<ObjectId>> {
    let mut probes = spec.required_probes.iter();
    let first = probes.next()?;
    let mut intersection: AHashSet<_> = storage
        .index_lookup(
            spec.table.as_str(),
            &first.translated_column,
            spec.branch.as_str(),
            &first.value,
        )
        .into_iter()
        .collect();
    for probe in probes {
        let ids: AHashSet<_> = storage
            .index_lookup(
                spec.table.as_str(),
                &probe.translated_column,
                spec.branch.as_str(),
                &probe.value,
            )
            .into_iter()
            .collect();
        intersection.retain(|id| ids.contains(id));
    }
    Some(intersection)
}

fn load_direct_candidates<F>(
    spec: &OrderedDriverSourceSpec,
    required_ids: Option<&AHashSet<ObjectId>>,
    storage: &dyn Storage,
    row_loader: &mut F,
) -> Vec<Tuple>
where
    F: FnMut(ObjectId) -> Option<LoadedRow>,
{
    let Some(required_ids) = required_ids else {
        return Vec::new();
    };

    let mut candidates = Vec::with_capacity(required_ids.len());
    for row_id in required_ids {
        let Some(loaded) = row_loader(*row_id) else {
            continue;
        };
        let row = Row::new(*row_id, loaded.data, loaded.commit_id);
        let provenance = if loaded.provenance.is_empty() {
            [(*row_id, spec.branch)].into_iter().collect()
        } else {
            loaded.provenance
        };
        let scoped_row = ScopedRow {
            row,
            provenance,
            branch: spec.branch,
        };
        if !passes_source_policy(spec, &scoped_row, storage, row_loader) {
            continue;
        }
        let Some(lead_value) = spec
            .driver_key
            .extract_value(&scoped_row.row, &spec.driver_descriptor)
        else {
            continue;
        };
        if !value_within_bounds(&lead_value, &spec.start, &spec.end) {
            continue;
        }

        candidates.push(DriverCandidate {
            lead_key_bytes: encode_value(&lead_value),
            row: scoped_row.row,
            provenance: scoped_row.provenance,
        });
    }

    candidates.sort_by(|left, right| compare_driver_candidates(spec, left, right));
    truncate_visible_driver_candidates(spec, &mut candidates);
    candidates
        .into_iter()
        .map(|candidate| {
            Tuple::new_with_provenance(
                vec![TupleElement::Row {
                    id: candidate.row.id,
                    content: candidate.row.data,
                    commit_id: candidate.row.commit_id,
                }],
                candidate.provenance,
            )
        })
        .collect()
}

fn scan_ordered_candidates<F>(
    spec: &OrderedDriverSourceSpec,
    required_ids: Option<&AHashSet<ObjectId>>,
    storage: &dyn Storage,
    row_loader: &mut F,
) -> Vec<Tuple>
where
    F: FnMut(ObjectId) -> Option<LoadedRow>,
{
    let mut ordered = Vec::new();
    let mut resume_after: Option<OrderedIndexCursor> = None;
    let prefix_len = short_circuit_prefix_len(spec);
    if prefix_len == Some(0) {
        return ordered;
    }
    let mut boundary_value: Option<Value> = None;

    'scan: loop {
        let cursors = storage.index_scan_ordered(OrderedIndexScan {
            table: spec.table.as_str(),
            column: &spec.translated_driver_column,
            branch: spec.branch.as_str(),
            start: borrow_bound(&spec.start),
            end: borrow_bound(&spec.end),
            direction: spec.direction,
            take: Some(DRIVER_BATCH_SIZE),
            resume_after: resume_after.as_ref(),
        });
        if cursors.is_empty() {
            break;
        }

        for cursor in &cursors {
            if let (Some(prefix_len), Some(boundary_value)) = (prefix_len, boundary_value.as_ref())
                && ordered.len() >= prefix_len
                && cursor.value != *boundary_value
            {
                break 'scan;
            }

            resume_after = Some(cursor.clone());
            if required_ids.is_some_and(|ids| !ids.contains(&cursor.row_id)) {
                continue;
            }

            let Some(loaded) = row_loader(cursor.row_id) else {
                continue;
            };
            let row = Row::new(cursor.row_id, loaded.data, loaded.commit_id);
            let provenance = if loaded.provenance.is_empty() {
                [(cursor.row_id, spec.branch)].into_iter().collect()
            } else {
                loaded.provenance
            };
            let scoped_row = ScopedRow {
                row,
                provenance,
                branch: spec.branch,
            };
            if !passes_source_policy(spec, &scoped_row, storage, row_loader) {
                continue;
            }

            ordered.push(Tuple::new_with_provenance(
                vec![TupleElement::Row {
                    id: scoped_row.row.id,
                    content: scoped_row.row.data,
                    commit_id: scoped_row.row.commit_id,
                }],
                scoped_row.provenance,
            ));

            if boundary_value.is_none() && prefix_len.is_some_and(|limit| ordered.len() == limit) {
                boundary_value = Some(cursor.value.clone());
            }
        }

        if cursors.len() < DRIVER_BATCH_SIZE {
            break;
        }
    }

    ordered
}

fn passes_source_policy(
    spec: &OrderedDriverSourceSpec,
    scoped_row: &ScopedRow,
    storage: &dyn Storage,
    row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
) -> bool {
    spec.policy_evaluator
        .as_ref()
        .is_none_or(|policy| policy.evaluate_with_context(&scoped_row.row, storage, row_loader))
}

fn short_circuit_prefix_len(spec: &OrderedDriverSourceSpec) -> Option<usize> {
    spec.enable_prefix_short_circuit
        .then_some(spec.desired_prefix_len)
        .flatten()
}

fn truncate_visible_driver_candidates(
    spec: &OrderedDriverSourceSpec,
    candidates: &mut Vec<DriverCandidate>,
) {
    let Some(prefix_len) = short_circuit_prefix_len(spec) else {
        return;
    };
    if prefix_len == 0 {
        candidates.clear();
        return;
    }
    if candidates.len() <= prefix_len {
        return;
    }

    let Some(boundary_key) = candidates
        .get(prefix_len - 1)
        .map(|candidate| candidate.lead_key_bytes.as_slice())
    else {
        return;
    };
    let truncate_at = candidates[prefix_len..]
        .iter()
        .position(|candidate| candidate.lead_key_bytes.as_slice() != boundary_key)
        .map(|index| prefix_len + index)
        .unwrap_or(candidates.len());
    candidates.truncate(truncate_at);
}

fn compare_driver_candidates(
    spec: &OrderedDriverSourceSpec,
    left: &DriverCandidate,
    right: &DriverCandidate,
) -> Ordering {
    compare_ordered_lead_key_bytes(
        spec.direction,
        &left.lead_key_bytes,
        left.row.id,
        &right.lead_key_bytes,
        right.row.id,
    )
}

fn compare_driver_tuples(spec: &MergeOrderedSpec, left: &Tuple, right: &Tuple) -> Ordering {
    compare_ordered_lead_keys(
        spec.direction,
        lead_key_bytes_for_single_tuple(&spec.driver_key, &spec.driver_descriptor, left),
        left.first_id(),
        lead_key_bytes_for_single_tuple(&spec.driver_key, &spec.driver_descriptor, right),
        right.first_id(),
    )
}

fn compare_ordered_lead_key_bytes(
    direction: IndexScanDirection,
    left: &[u8],
    left_id: ObjectId,
    right: &[u8],
    right_id: ObjectId,
) -> Ordering {
    match direction {
        IndexScanDirection::Ascending => left.cmp(right).then_with(|| left_id.cmp(&right_id)),
        IndexScanDirection::Descending => right.cmp(left).then_with(|| left_id.cmp(&right_id)),
    }
}

fn compare_ordered_lead_keys(
    direction: IndexScanDirection,
    left: Option<Vec<u8>>,
    left_id: Option<ObjectId>,
    right: Option<Vec<u8>>,
    right_id: Option<ObjectId>,
) -> Ordering {
    match direction {
        IndexScanDirection::Ascending => left.cmp(&right).then_with(|| left_id.cmp(&right_id)),
        IndexScanDirection::Descending => right.cmp(&left).then_with(|| left_id.cmp(&right_id)),
    }
}

fn lead_key_bytes_for_single_tuple(
    key: &ResolvedRowKey,
    descriptor: &crate::query_manager::types::RowDescriptor,
    tuple: &Tuple,
) -> Option<Vec<u8>> {
    let row = tuple.to_single_row()?;
    let value = key.extract_value(&row, descriptor)?;
    Some(encode_value(&value))
}

fn build_joined_tuples(
    spec: &ProbeJoinSpec,
    driver_tuple: &Tuple,
    storage: &dyn Storage,
    row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
) -> Vec<Tuple> {
    let Some(driver_row) = scoped_row_from_driver_tuple(driver_tuple) else {
        return Vec::new();
    };

    let mut seed = PartialTuple {
        rows: vec![None; spec.table_descriptors.len()],
    };
    seed.rows[spec.driver_scope_index] = Some(driver_row);

    expand_from_segment(
        spec,
        spec.driver_scope_index,
        spec.driver_scope_index,
        seed,
        storage,
        row_loader,
    )
    .into_iter()
    .filter_map(partial_tuple_to_tuple)
    .filter(|tuple| {
        spec.residual_filter
            .as_ref()
            .is_none_or(|filter| filter.evaluate_tuple(tuple))
    })
    .collect()
}

fn scoped_row_from_driver_tuple(tuple: &Tuple) -> Option<ScopedRow> {
    Some(ScopedRow {
        row: tuple.to_single_row()?,
        provenance: tuple.provenance().clone(),
        branch: tuple
            .provenance()
            .iter()
            .next()
            .map(|(_, branch)| *branch)?,
    })
}

fn expand_from_segment(
    spec: &ProbeJoinSpec,
    left_index: usize,
    right_index: usize,
    partial: PartialTuple,
    storage: &dyn Storage,
    row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
) -> Vec<PartialTuple> {
    if left_index > 0 {
        let edge = &spec.join_edges[left_index - 1];
        let Some(known_row) = partial.rows[left_index].as_ref() else {
            return Vec::new();
        };
        let matches = lookup_join_matches(spec, storage, row_loader, known_row, edge, false);
        let mut expanded = Vec::new();
        for matched in matches {
            let mut next = partial.clone();
            next.rows[left_index - 1] = Some(matched);
            expanded.extend(expand_from_segment(
                spec,
                left_index - 1,
                right_index,
                next,
                storage,
                row_loader,
            ));
        }
        return expanded;
    }

    if right_index + 1 < spec.table_descriptors.len() {
        let edge = &spec.join_edges[right_index];
        let Some(known_row) = partial.rows[right_index].as_ref() else {
            return Vec::new();
        };
        let matches = lookup_join_matches(spec, storage, row_loader, known_row, edge, true);
        let mut expanded = Vec::new();
        for matched in matches {
            let mut next = partial.clone();
            next.rows[right_index + 1] = Some(matched);
            expanded.extend(expand_from_segment(
                spec,
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
    spec: &ProbeJoinSpec,
    storage: &dyn Storage,
    row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    known_row: &ScopedRow,
    edge: &JoinLookupSpec,
    probe_right: bool,
) -> Vec<ScopedRow> {
    let (source_key, target_key, target_table, target_scope_index, translated_columns_by_branch) =
        if probe_right {
            (
                &edge.left_key,
                &edge.right_key,
                edge.right_table,
                edge.right_scope_index,
                &edge.right_translated_columns_by_branch,
            )
        } else {
            (
                &edge.right_key,
                &edge.left_key,
                edge.left_table,
                edge.left_scope_index,
                &edge.left_translated_columns_by_branch,
            )
        };

    let source_descriptor = &spec.table_descriptors[if probe_right {
        edge.left_scope_index
    } else {
        edge.right_scope_index
    }];
    let expected_values = source_key.extract_lookup_values(&known_row.row, source_descriptor);
    if expected_values.is_empty() {
        return Vec::new();
    }

    let translated_column = translated_columns_by_branch
        .get(&known_row.branch)
        .map(String::as_str)
        .unwrap_or(target_key.index_column());

    let mut candidate_ids = AHashSet::new();
    for value in &expected_values {
        candidate_ids.extend(storage.index_lookup(
            target_table.as_str(),
            translated_column,
            known_row.branch.as_str(),
            value,
        ));
    }

    let mut ids: Vec<_> = candidate_ids.into_iter().collect();
    ids.sort_unstable();

    let target_descriptor = &spec.table_descriptors[target_scope_index];
    let mut matched_rows = Vec::new();
    for row_id in ids {
        let Some(loaded) = row_loader(row_id) else {
            continue;
        };
        let row = Row::new(row_id, loaded.data, loaded.commit_id);
        let candidate = ScopedRow {
            row,
            provenance: if loaded.provenance.is_empty() {
                [(row_id, known_row.branch)].into_iter().collect()
            } else {
                loaded.provenance
            },
            branch: known_row.branch,
        };
        if !passes_join_policy(
            spec.policies.as_slice(),
            target_scope_index,
            &candidate,
            storage,
            row_loader,
        ) {
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

fn passes_join_policy(
    policies: &[ScopedPolicySpec],
    scope_index: usize,
    scoped_row: &ScopedRow,
    storage: &dyn Storage,
    row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
) -> bool {
    let Some(policy) = policies
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

fn partial_tuple_to_tuple(partial: PartialTuple) -> Option<Tuple> {
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

fn compare_tuples(spec: &TieSortSpec, left: &Tuple, right: &Tuple) -> Ordering {
    for key in &spec.sort_keys {
        let ord = compare_sort_key(key, left, right);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    left.ids().cmp(&right.ids())
}

fn compare_sort_key(key: &ResolvedSortKey, left: &Tuple, right: &Tuple) -> Ordering {
    let ord = match &key.target {
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

    match key.direction {
        crate::query_manager::graph_nodes::sort::SortDirection::Ascending => ord,
        crate::query_manager::graph_nodes::sort::SortDirection::Descending => ord.reverse(),
    }
}

fn lead_key_bytes_for_full_tuple(spec: &TieSortSpec, tuple: &Tuple) -> Option<Vec<u8>> {
    let element = tuple.get(spec.driver_scope_index)?;
    let row = element.to_row()?;
    let value = spec
        .driver_key
        .extract_value(&row, &spec.driver_descriptor)?;
    Some(encode_value(&value))
}

fn borrow_bound(bound: &Bound<Value>) -> Bound<&Value> {
    match bound {
        Bound::Included(value) => Bound::Included(value),
        Bound::Excluded(value) => Bound::Excluded(value),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn value_within_bounds(value: &Value, start: &Bound<Value>, end: &Bound<Value>) -> bool {
    let encoded = encode_value(value);
    let lower_ok = match start {
        Bound::Included(start) => encoded >= encode_value(start),
        Bound::Excluded(start) => encoded > encode_value(start),
        Bound::Unbounded => true,
    };
    let upper_ok = match end {
        Bound::Included(end) => encoded <= encode_value(end),
        Bound::Excluded(end) => encoded < encode_value(end),
        Bound::Unbounded => true,
    };
    lower_ok && upper_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitId;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::graph_nodes::sort::SortDirection;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, LoadedRow, RowDescriptor, TupleElement, Value,
    };
    use crate::storage::{MemoryStorage, Storage};
    use std::collections::HashMap;
    use uuid::Uuid;

    fn branch(name: &str) -> BranchName {
        BranchName::new(name)
    }

    fn object_id(n: u128) -> ObjectId {
        ObjectId::from_uuid(Uuid::from_u128(n))
    }

    fn commit_id(n: u8) -> CommitId {
        CommitId([n; 32])
    }

    fn users_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
    }

    fn project_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new(
            "member_ids",
            ColumnType::Array {
                element: Box::new(ColumnType::Uuid),
            },
        )])
    }

    fn member_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("user_key", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn tie_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("bucket", ColumnType::Integer),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
    }

    fn loaded_row(
        row_id: ObjectId,
        commit_id: CommitId,
        branch: BranchName,
        data: Vec<u8>,
    ) -> LoadedRow {
        LoadedRow::new(data, commit_id, [(row_id, branch)].into_iter().collect())
    }

    fn tuple_from_loaded(row_id: ObjectId, loaded: &LoadedRow) -> Tuple {
        Tuple::new_with_provenance(
            vec![TupleElement::Row {
                id: row_id,
                content: loaded.data.clone(),
                commit_id: loaded.commit_id,
            }],
            loaded.provenance.clone(),
        )
    }

    #[test]
    fn driver_source_direct_id_path_matches_ordered_scan_path() {
        let descriptor = users_descriptor();
        let main = branch("main");
        let mut storage = MemoryStorage::new();
        let mut rows = HashMap::new();

        for (idx, (row_id, name, score)) in [
            (object_id(1), "Alice", 50),
            (object_id(2), "Alice", 40),
            (object_id(3), "Bob", 100),
        ]
        .into_iter()
        .enumerate()
        {
            let data = encode_row(
                &descriptor,
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
            let loaded = loaded_row(row_id, commit_id(idx as u8 + 1), main, data);
            storage
                .index_insert("users", "score", "main", &Value::Integer(score), row_id)
                .unwrap();
            storage
                .index_insert("users", "name", "main", &Value::Text(name.into()), row_id)
                .unwrap();
            storage
                .index_insert("users", "_id", "main", &Value::Uuid(row_id), row_id)
                .unwrap();
            rows.insert(row_id, loaded);
        }

        let build_node = |max_direct_required_ids| {
            OrderedDriverSourceNode::new(Arc::new(OrderedDriverSourceSpec {
                branch: main,
                table: crate::query_manager::types::TableName::new("users"),
                driver_descriptor: descriptor.clone(),
                driver_key: ResolvedRowKey::from_descriptor(&descriptor, "score").unwrap(),
                direction: IndexScanDirection::Descending,
                translated_driver_column: "score".into(),
                start: Bound::Unbounded,
                end: Bound::Unbounded,
                required_probes: vec![super::super::plan::ExactMatchProbe {
                    translated_column: "name".into(),
                    value: Value::Text("Alice".into()),
                }],
                policy_evaluator: None,
                desired_prefix_len: None,
                enable_prefix_short_circuit: true,
                max_direct_required_ids,
            }))
        };

        let mut direct_node = build_node(8);
        let mut scan_node = build_node(0);

        let mut direct_loader = |id| rows.get(&id).cloned();
        let mut scan_loader = |id| rows.get(&id).cloned();

        direct_node.process_with_context(&storage, &mut direct_loader);
        scan_node.process_with_context(&storage, &mut scan_loader);

        assert_eq!(direct_node.ordered_tuples(), scan_node.ordered_tuples());
    }

    #[test]
    fn driver_source_preserves_tie_boundary_truncation() {
        let descriptor = users_descriptor();
        let main = branch("main");
        let mut storage = MemoryStorage::new();
        let mut rows = HashMap::new();

        for (idx, (row_id, score)) in [
            (object_id(11), 100),
            (object_id(12), 100),
            (object_id(13), 90),
        ]
        .into_iter()
        .enumerate()
        {
            let data = encode_row(
                &descriptor,
                &[Value::Text(format!("User {idx}")), Value::Integer(score)],
            )
            .unwrap();
            let loaded = loaded_row(row_id, commit_id(idx as u8 + 10), main, data);
            storage
                .index_insert("users", "score", "main", &Value::Integer(score), row_id)
                .unwrap();
            rows.insert(row_id, loaded);
        }

        let mut node = OrderedDriverSourceNode::new(Arc::new(OrderedDriverSourceSpec {
            branch: main,
            table: crate::query_manager::types::TableName::new("users"),
            driver_descriptor: descriptor.clone(),
            driver_key: ResolvedRowKey::from_descriptor(&descriptor, "score").unwrap(),
            direction: IndexScanDirection::Descending,
            translated_driver_column: "score".into(),
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            required_probes: Vec::new(),
            policy_evaluator: None,
            desired_prefix_len: Some(1),
            enable_prefix_short_circuit: true,
            max_direct_required_ids: 8,
        }));

        let mut loader = |id| rows.get(&id).cloned();
        node.process_with_context(&storage, &mut loader);

        let ordered_ids = node
            .ordered_tuples()
            .iter()
            .filter_map(Tuple::first_id)
            .collect::<Vec<_>>();
        assert_eq!(ordered_ids, vec![object_id(11), object_id(12)]);
    }

    #[test]
    fn probe_join_handles_array_expansion_and_branch_translated_columns() {
        let projects = project_descriptor();
        let members = member_descriptor();
        let feature = branch("feature");
        let project_id = object_id(21);
        let member_a_id = object_id(22);
        let member_b_id = object_id(23);

        let project_data = encode_row(
            &projects,
            &[Value::Array(vec![
                Value::Uuid(member_a_id),
                Value::Uuid(member_b_id),
            ])],
        )
        .unwrap();
        let project_loaded = loaded_row(project_id, commit_id(21), feature, project_data);

        let member_a_data = encode_row(
            &members,
            &[Value::Uuid(member_a_id), Value::Text("A".into())],
        )
        .unwrap();
        let member_b_data = encode_row(
            &members,
            &[Value::Uuid(member_b_id), Value::Text("B".into())],
        )
        .unwrap();

        let member_a_loaded = loaded_row(member_a_id, commit_id(22), feature, member_a_data);
        let member_b_loaded = loaded_row(member_b_id, commit_id(23), feature, member_b_data);

        let mut storage = MemoryStorage::new();
        storage
            .index_insert(
                "members",
                "user_key_v2",
                "feature",
                &Value::Uuid(member_a_id),
                member_a_id,
            )
            .unwrap();
        storage
            .index_insert(
                "members",
                "user_key_v2",
                "feature",
                &Value::Uuid(member_b_id),
                member_b_id,
            )
            .unwrap();

        let mut rows = HashMap::from([
            (project_id, project_loaded.clone()),
            (member_a_id, member_a_loaded.clone()),
            (member_b_id, member_b_loaded.clone()),
        ]);

        let mut node = ProbeJoinNode::new(Arc::new(ProbeJoinSpec {
            driver_scope_index: 0,
            table_descriptors: vec![projects.clone(), members.clone()],
            join_edges: vec![JoinLookupSpec {
                left_scope_index: 0,
                right_scope_index: 1,
                left_table: crate::query_manager::types::TableName::new("projects"),
                right_table: crate::query_manager::types::TableName::new("members"),
                left_key: ResolvedRowKey::from_descriptor(&projects, "member_ids").unwrap(),
                right_key: ResolvedRowKey::from_descriptor(&members, "user_key").unwrap(),
                left_translated_columns_by_branch: HashMap::from([(feature, "member_ids".into())]),
                right_translated_columns_by_branch: HashMap::from([(
                    feature,
                    "user_key_v2".into(),
                )]),
            }],
            residual_filter: None,
            policies: Vec::new(),
        }));

        let driver_tuple = tuple_from_loaded(project_id, &project_loaded);
        let mut loader = |id| {
            rows.remove(&id).or_else(|| {
                if id == project_id {
                    Some(project_loaded.clone())
                } else if id == member_a_id {
                    Some(member_a_loaded.clone())
                } else if id == member_b_id {
                    Some(member_b_loaded.clone())
                } else {
                    None
                }
            })
        };
        node.process_with_context(&[driver_tuple], &storage, &mut loader);

        let joined_ids = node
            .ordered_tuples()
            .iter()
            .map(|tuple| tuple.get(1).unwrap().id())
            .collect::<Vec<_>>();
        assert_eq!(joined_ids, vec![member_a_id, member_b_id]);
    }

    #[test]
    fn tie_sort_only_reorders_within_group_and_is_row_id_stable() {
        let descriptor = tie_descriptor();
        let row_a = object_id(31);
        let row_b = object_id(32);
        let row_c = object_id(33);
        let row_d = object_id(34);

        let make_tuple = |row_id, bucket, score| {
            Tuple::new_with_provenance(
                vec![TupleElement::Row {
                    id: row_id,
                    content: encode_row(
                        &descriptor,
                        &[Value::Integer(bucket), Value::Integer(score)],
                    )
                    .unwrap(),
                    commit_id: commit_id(row_id.uuid().as_bytes()[15]),
                }],
                [(row_id, branch("main"))].into_iter().collect(),
            )
        };

        let mut node = TieSortNode::new(Arc::new(TieSortSpec {
            driver_scope_index: 0,
            driver_descriptor: descriptor.clone(),
            driver_key: ResolvedRowKey::from_descriptor(&descriptor, "bucket").unwrap(),
            sort_keys: vec![
                ResolvedSortKey {
                    target: ResolvedSortTarget::Column {
                        element_index: 0,
                        descriptor: descriptor.clone(),
                        local_col_index: 0,
                    },
                    direction: SortDirection::Ascending,
                },
                ResolvedSortKey {
                    target: ResolvedSortTarget::Column {
                        element_index: 0,
                        descriptor: descriptor.clone(),
                        local_col_index: 1,
                    },
                    direction: SortDirection::Ascending,
                },
            ],
            desired_prefix_len: None,
        }));

        node.process_ordered_input(&[
            make_tuple(row_b, 1, 2),
            make_tuple(row_d, 1, 1),
            make_tuple(row_a, 1, 1),
            make_tuple(row_c, 2, 1),
        ]);

        let ordered_ids = node
            .ordered_tuples()
            .iter()
            .filter_map(Tuple::first_id)
            .collect::<Vec<_>>();
        assert_eq!(ordered_ids, vec![row_a, row_d, row_b, row_c]);
    }
}

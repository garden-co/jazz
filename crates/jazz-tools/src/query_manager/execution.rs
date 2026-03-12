use ahash::AHashSet;

use crate::object::ObjectId;
use crate::storage::Storage;

use super::graph_nodes::NodeId;
use super::types::{LoadedRow, Tuple, TupleDelta};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamOrdering {
    #[default]
    Unordered,
    Ordered,
    PreservesInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamMaterialization {
    #[default]
    Unknown,
    Unmaterialized,
    Partial,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamDistinctness {
    #[default]
    Unknown,
    Distinct,
    PreservesInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamResumeKey {
    #[default]
    None,
    InputDerived,
    RowId,
    OrderedValueThenRowId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSpec {
    pub ordering: StreamOrdering,
    pub resume_key: StreamResumeKey,
    pub materialization: StreamMaterialization,
    pub distinctness: StreamDistinctness,
}

impl Default for StreamSpec {
    fn default() -> Self {
        Self {
            ordering: StreamOrdering::Unordered,
            resume_key: StreamResumeKey::None,
            materialization: StreamMaterialization::Unknown,
            distinctness: StreamDistinctness::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StreamInvalidation {
    #[default]
    FullRestart,
    Table {
        table: String,
    },
    Column {
        table: String,
        column: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionStats {
    pub polls: usize,
    pub resume_count: usize,
    pub invalidations: usize,
    pub rows_scanned: usize,
    pub full_restarts: usize,
}

#[derive(Debug)]
pub struct ExecutionInput<'a> {
    pub delta: &'a TupleDelta,
    pub current_tuples: AHashSet<Tuple>,
    pub ordered: Option<&'a [Tuple]>,
    pub sync_input: Option<&'a [Tuple]>,
    pub spec: StreamSpec,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionOutput {
    pub delta: TupleDelta,
    pub ordered: Option<Vec<Tuple>>,
    pub sync_input: Option<Vec<Tuple>>,
    pub spec: StreamSpec,
}

impl ExecutionOutput {
    pub fn rows_scanned_hint(&self, current_len: usize) -> usize {
        if let Some(sync_input) = &self.sync_input {
            return sync_input.len();
        }
        if let Some(ordered) = &self.ordered {
            return ordered.len();
        }

        let delta_rows = self.delta.added.len()
            + self.delta.removed.len()
            + self.delta.moved.len()
            + self.delta.updated.len();
        delta_rows.max(current_len)
    }
}

pub struct ExecutionContext<'a> {
    pub storage: &'a dyn Storage,
    pub node_id: NodeId,
}

pub trait ExecutionOperator {
    fn operator_name(&self) -> &'static str;

    fn stream_spec(&self, inputs: &[ExecutionInput<'_>]) -> StreamSpec;

    fn execute(
        &mut self,
        inputs: &[ExecutionInput<'_>],
        ctx: &ExecutionContext<'_>,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<LoadedRow>,
    ) -> ExecutionOutput;

    fn current_tuples(&self) -> &AHashSet<Tuple>;

    fn ordered_tuples(&self) -> Option<&[Tuple]> {
        None
    }

    fn sync_input_tuples(&self) -> Option<&[Tuple]> {
        None
    }

    fn mark_dirty(&mut self);

    fn is_dirty(&self) -> bool;
}

pub fn passthrough_ordered(
    input: Option<&ExecutionInput<'_>>,
    current_tuples: &AHashSet<Tuple>,
) -> Option<Vec<Tuple>> {
    let ordered = input?.ordered?;
    Some(
        ordered
            .iter()
            .filter_map(|tuple| current_tuples.get(tuple).cloned())
            .collect(),
    )
}

pub fn mapped_ordered_distinct<F>(
    input: Option<&ExecutionInput<'_>>,
    current_tuples: &AHashSet<Tuple>,
    mut map_tuple: F,
) -> Option<Vec<Tuple>>
where
    F: FnMut(&Tuple) -> Option<Tuple>,
{
    let ordered = input?.ordered?;
    let mut seen = AHashSet::new();
    let mut result = Vec::new();
    for tuple in ordered {
        let mapped = map_tuple(tuple)?;
        let current = current_tuples.get(&mapped)?.clone();
        if seen.insert(current.clone()) {
            result.push(current);
        }
    }
    Some(result)
}

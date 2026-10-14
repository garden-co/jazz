//! Stored arrangements: an arrangement over a stored table, keyed on the
//! table's primary key, read from storage by key instead of held in memory.
//!
//! Such an arrangement duplicates an index storage already has. Its bucket
//! for a key is the table's rows under that key, passed through the
//! operators between the table and the arrangement. When every one of those
//! operators keeps or drops whole keys (row-wise operators, unions, and a
//! per-group winner whose group contains the key), the bucket can be rebuilt
//! from just those rows. The storage engine's cache then decides what stays
//! in memory, the arrangement node stays shared by every consumer, and
//! nothing is copied or written back.
//!
//! A stored arrangement's buckets are a cache over storage. Consumers ask
//! for the buckets they will read before reading them
//! (`prefetch_stored_buckets`): a join reads the other side's changed keys,
//! a threshold join and a grouped consumer every changed key. Buckets that
//! are not resident are read from storage, which holds the evaluation's
//! state, and stay resident afterwards; resident buckets follow each
//! transition like a held arrangement's. A Replace evaluation rebuilds the
//! arrangement in full, leaving every bucket resident.
//!
//! The runtime's arrangement budget bounds the resident bytes of all stored
//! arrangements together, evicting the least recently read buckets after
//! each install. No budget keeps every bucket read, which costs what a held
//! arrangement does; a budget of zero keeps none, reading every bucket from
//! storage; budgets between trade memory for storage reads smoothly.

use super::evaluation_session::StorageRequestKey;
use super::join::{ArrangementTransition, JoinKey, probed_buckets};
use super::*;
use crate::storage::KeyValue;
use std::sync::atomic::{AtomicBool, Ordering};

/// How to read the buckets of one stored arrangement.
#[derive(Debug)]
pub(super) struct StoredArrangementPlan {
    descriptor: RecordDescriptor,
    fields: Vec<String>,
    /// How many leading key fields storage is read by. Fewer than `fields`
    /// when the arrangement key extends the primary key (a row and its
    /// version): the read covers the row, and the full key picks the bucket.
    probe_parts: usize,
    root: ProbeNode,
    /// Set while the runtime has a finite arrangement budget: reads then
    /// record which resident buckets they used, for eviction.
    tracks_use: Arc<AtomicBool>,
}

impl StoredArrangementPlan {
    pub(super) fn tracks_use(&self) -> bool {
        self.tracks_use.load(Ordering::Relaxed)
    }

    /// The storage keys that read the buckets of `keys`.
    fn probe_keys(&self, keys: &[JoinKey]) -> Result<Vec<JoinKey>, IvmRuntimeError> {
        if self.probe_parts == self.fields.len() {
            return Ok(keys.to_vec());
        }
        let mut probes = keys
            .iter()
            .map(|key| {
                let mut len = 0;
                for _ in 0..self.probe_parts {
                    len += key_part_len(&key[len..]).ok_or(IvmRuntimeError::UnsupportedJoinKey)?;
                }
                Ok(JoinKey::from_slice(&key[..len]))
            })
            .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
        probes.sort_unstable();
        probes.dedup();
        Ok(probes)
    }
}

/// The length of the first encoded key part of `key` (`encode_key_part`).
fn key_part_len(key: &[u8]) -> Option<usize> {
    let fixed = |len: usize| (key.len() >= len).then_some(len);
    match *key.first()? {
        0 | 5 => fixed(2),
        1 => fixed(3),
        2 | 14 => fixed(5),
        3 | 4 | 13 => fixed(9),
        16 => fixed(7),
        10 => fixed(17),
        8 => Some(1),
        9 => Some(1 + key_part_len(&key[1..])?),
        6 | 7 => {
            // Terminated by 00 00; an embedded NUL is escaped as 00 ff.
            let mut at = 1;
            loop {
                match key.get(at..at + 2)? {
                    [0, 0] => return Some(at + 2),
                    [0, 0xff] => at += 2,
                    [0, _] => return None,
                    _ => at += 1,
                }
            }
        }
        _ => None,
    }
}

/// Whether key parts of this type can be skipped without the type.
fn skippable_key_type(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::U8
        | ValueType::U16
        | ValueType::U32
        | ValueType::U64
        | ValueType::U48
        | ValueType::I32
        | ValueType::I64
        | ValueType::F64
        | ValueType::Bool
        | ValueType::String
        | ValueType::Bytes
        | ValueType::Uuid
        | ValueType::EnumTag(_) => true,
        ValueType::Nullable(inner) => skippable_key_type(inner),
        _ => false,
    }
}

/// The operators between a stored table and its arrangement, applied to the
/// table rows of the probed keys.
#[derive(Debug)]
enum ProbeNode {
    Table {
        source: TableSourceOp,
        output: RecordDescriptor,
        /// Storage-key prefix of the static scan; the probed key follows it.
        prefix: Vec<u8>,
    },
    Filter {
        node: NodeId,
        filter: FilterOp,
        output: RecordDescriptor,
        input: Box<ProbeNode>,
    },
    Project {
        node: NodeId,
        project: MapProjectOp,
        output: RecordDescriptor,
        input: Box<ProbeNode>,
    },
    UnwrapNullable {
        unwrap: UnwrapNullableOp,
        output: RecordDescriptor,
        input: Box<ProbeNode>,
    },
    ArgBy {
        group_field_indices: Vec<usize>,
        comparison_field_indices: Vec<usize>,
        max: bool,
        output: RecordDescriptor,
        input: Box<ProbeNode>,
    },
    Union {
        output: RecordDescriptor,
        inputs: Vec<ProbeNode>,
    },
    /// A semi- or anti-join on exactly the probed key: a key's rows pass or
    /// fail together, decided by the other side's rows under the same key.
    Threshold {
        semi: bool,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        output: RecordDescriptor,
        left: Box<ProbeNode>,
        right: Box<ProbeNode>,
    },
}

/// The storage plan of `arrange`, when its buckets can be read from a
/// stored table by key.
pub(super) fn plan_stored_arrangement(
    graph: &IvmGraph,
    schema: &DatabaseSchema,
    arrange: NodeId,
    tracks_use: Arc<AtomicBool>,
) -> Option<StoredArrangementPlan> {
    let descriptor = &graph.node(arrange)?.descriptor;
    let OpType::Arrange(op) = &descriptor.operator else {
        return None;
    };
    // Policy comparison normalises key values, so its keys are not the
    // storage encoding.
    if op.comparison != ValueComparison::Exact || op.fields.is_empty() {
        return None;
    }
    let [input] = descriptor.inputs.as_slice() else {
        return None;
    };
    let input_desc = records_of(graph, *input)?;
    let fields = op
        .fields
        .iter()
        .map(|field| resolve_field_name(&input_desc, field))
        .collect::<Option<Vec<_>>>()?;
    // Read by the whole key when storage is keyed by it, else by its longest
    // leading part storage is keyed by.
    let (probe_parts, root) = (1..=fields.len()).rev().find_map(|parts| {
        let leading = &fields[..parts];
        let skippable = parts == fields.len()
            || leading
                .iter()
                .all(|field| skippable_key_type(&input_desc.fields()[*field].value_type));
        if !skippable {
            return None;
        }
        Some((parts, probe_node(graph, schema, *input, leading)?))
    })?;
    Some(StoredArrangementPlan {
        descriptor: input_desc,
        fields: op.fields.clone(),
        probe_parts,
        root,
        tracks_use,
    })
}

fn records_of(graph: &IvmGraph, node: NodeId) -> Option<RecordDescriptor> {
    match graph.node(node)?.descriptor.output {
        NodeOutput::Records(descriptor) => Some(descriptor),
        NodeOutput::Arrangement(_) => None,
    }
}

/// The input an `Arrange` node passes through, or `node` itself.
fn arranged_input(graph: &IvmGraph, node: NodeId) -> Option<NodeId> {
    let descriptor = &graph.node(node)?.descriptor;
    match (&descriptor.operator, descriptor.inputs.as_slice()) {
        (OpType::Arrange(_), [input]) => Some(*input),
        (OpType::Arrange(_), _) => None,
        _ => Some(node),
    }
}

/// The field names of join keys made only of plain fields.
fn plain_fields(keys: &[PlanExpr]) -> Option<Vec<String>> {
    keys.iter()
        .map(|key| match key {
            PlanExpr::Field(name) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// `node` read for the keys in its output `fields`, or `None` when some
/// operator below it does not keep or drop whole keys.
fn probe_node(
    graph: &IvmGraph,
    schema: &DatabaseSchema,
    node: NodeId,
    fields: &[usize],
) -> Option<ProbeNode> {
    let descriptor = &graph.node(node)?.descriptor;
    let output = records_of(graph, node)?;
    let unary_input = || match descriptor.inputs.as_slice() {
        [input] => Some(*input),
        _ => None,
    };
    Some(match &descriptor.operator {
        OpType::TableSource(source) => {
            let prefix_len = match &source.scan {
                None => 0,
                Some(StaticScanSpec::Prefix(values)) => values.len(),
                Some(_) => return None,
            };
            let table = schema.table(&source.table)?;
            let primary_key = primary_key_field_indices(table, &output).ok()?;
            if primary_key.get(prefix_len..prefix_len + fields.len())? != fields {
                return None;
            }
            let prefix = match source.scan.as_ref().map(scan_bounds).transpose().ok()? {
                None => Vec::new(),
                Some(StaticScanBounds::Prefix(prefix)) => prefix,
                Some(StaticScanBounds::Range { .. }) => return None,
            };
            ProbeNode::Table {
                source: TableSourceOp {
                    scan: None,
                    ..source.clone()
                },
                output,
                prefix,
            }
        }
        OpType::Filter(filter) => ProbeNode::Filter {
            node,
            filter: filter.clone(),
            output,
            input: Box::new(probe_node(graph, schema, unary_input()?, fields)?),
        },
        OpType::UnwrapNullable(unwrap) => ProbeNode::UnwrapNullable {
            unwrap: unwrap.clone(),
            output,
            input: Box::new(probe_node(graph, schema, unary_input()?, fields)?),
        },
        OpType::MapProject(project) => {
            let input = unary_input()?;
            let input_desc = records_of(graph, input)?;
            // Each key field must be a plain copy of an input field, so the
            // input's key is the output's key.
            let input_fields = fields
                .iter()
                .map(|field| match &project.expressions.get(*field)?.expression {
                    ProjectExpr::Field(source) => resolve_field_ref(&input_desc, source).ok(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            ProbeNode::Project {
                node,
                project: project.clone(),
                output,
                input: Box::new(probe_node(graph, schema, input, &input_fields)?),
            }
        }
        // A winner per group stays the winner when whole groups are read,
        // which holds when the key lies in the group.
        OpType::ArgMaxBy(ArgMaxByOp {
            group_field_indices,
            comparison_field_indices,
            ..
        })
        | OpType::ArgMinBy(ArgMinByOp {
            group_field_indices,
            comparison_field_indices,
            ..
        }) => {
            if !fields
                .iter()
                .all(|field| group_field_indices.contains(field))
            {
                return None;
            }
            // The winner is chosen over an arrangement of the input, which
            // passes its rows through.
            let arranged = arranged_input(graph, unary_input()?)?;
            ProbeNode::ArgBy {
                group_field_indices: group_field_indices.clone(),
                comparison_field_indices: comparison_field_indices.clone(),
                max: matches!(descriptor.operator, OpType::ArgMaxBy(_)),
                output,
                input: Box::new(probe_node(graph, schema, arranged, fields)?),
            }
        }
        OpType::SemiJoin(join) | OpType::AntiJoin(join) => {
            if join.comparison != ValueComparison::Exact
                || join.residual_predicate.is_some()
                || join.kind != JoinOpKind::Inner
            {
                return None;
            }
            let [left, right] = descriptor.inputs.as_slice() else {
                return None;
            };
            let left_keys = plain_fields(&join.left_key)?;
            let right_keys = plain_fields(&join.right_key)?;
            let left_desc = join.left_descriptor;
            let right_desc = join.right_descriptor;
            let left_fields = left_keys
                .iter()
                .map(|name| left_desc.field_index(name))
                .collect::<Option<Vec<_>>>()?;
            let right_fields = right_keys
                .iter()
                .map(|name| right_desc.field_index(name))
                .collect::<Option<Vec<_>>>()?;
            // The join key must be the probed key, and both sides must
            // encode it alike, so one probe reads both sides.
            let same_types = left_fields.iter().zip(&right_fields).all(|(l, r)| {
                left_desc.fields()[*l].value_type == right_desc.fields()[*r].value_type
            });
            if left_fields != fields || !same_types {
                return None;
            }
            ProbeNode::Threshold {
                semi: matches!(descriptor.operator, OpType::SemiJoin(_)),
                left_keys,
                right_keys,
                output,
                left: Box::new(probe_node(
                    graph,
                    schema,
                    arranged_input(graph, *left)?,
                    fields,
                )?),
                right: Box::new(probe_node(
                    graph,
                    schema,
                    arranged_input(graph, *right)?,
                    &right_fields,
                )?),
            }
        }
        // Union inputs share the output descriptor, so the key fields are the
        // same in every branch, and every branch must be readable by key.
        OpType::Union => ProbeNode::Union {
            output,
            inputs: descriptor
                .inputs
                .iter()
                .map(|input| probe_node(graph, schema, *input, fields))
                .collect::<Option<Vec<_>>>()?,
        },
        _ => return None,
    })
}

impl ProbeNode {
    /// The storage reads that load `keys`.
    fn reads(&self, keys: &[JoinKey], out: &mut Vec<StorageRequestKey>) {
        match self {
            Self::Table { source, prefix, .. } => {
                out.extend(keys.iter().map(|key| {
                    let mut probe = prefix.clone();
                    probe.extend_from_slice(key);
                    StorageRequestKey::ScanPrefix {
                        family: source.table.clone(),
                        prefix: probe,
                    }
                }));
            }
            Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::UnwrapNullable { input, .. }
            | Self::ArgBy { input, .. } => input.reads(keys, out),
            Self::Union { inputs, .. } => {
                for input in inputs {
                    input.reads(keys, out);
                }
            }
            Self::Threshold { left, right, .. } => {
                left.reads(keys, out);
                right.reads(keys, out);
            }
        }
    }
}

/// Stored rows by the read that loaded them.
type LoadedReads = HashMap<StorageRequestKey, Vec<KeyValue>>;

impl TickEvaluator<'_> {
    /// Read the buckets of `keys` of a stored arrangement in this
    /// evaluation, so its consumers can read them. Through the evaluation
    /// session, returns `EvaluationBlocked` (with every missing read
    /// registered) until all of them are loaded; without a session, awaits
    /// storage directly.
    pub(super) async fn prefetch_stored_buckets(
        &mut self,
        arrangement: &ArrangementKey,
        keys: Vec<JoinKey>,
    ) -> Result<(), IvmRuntimeError> {
        let Some((plan, missing)) = self.unprobed(arrangement, keys) else {
            return Ok(());
        };
        let probes = plan.probe_keys(&missing)?;
        let mut requests = Vec::new();
        plan.root.reads(&probes, &mut requests);
        let mut loaded = LoadedReads::default();
        if let Some(inputs) = self.evaluation_inputs.as_deref_mut() {
            let mut blocked = false;
            for request in requests {
                match inputs.rows(request.clone()) {
                    Ok(rows) => {
                        loaded.insert(request, rows.to_vec());
                    }
                    Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
                    Err(error) => return Err(error),
                }
            }
            if blocked {
                return Err(IvmRuntimeError::EvaluationBlocked);
            }
        } else {
            let storage = self.storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
            for request in requests {
                let StorageRequestKey::ScanPrefix { family, prefix } = &request else {
                    unreachable!("stored arrangements read prefixes");
                };
                let scan = storage
                    .scan(ScanRequest::prefix(family.clone(), prefix.clone()))
                    .await?;
                let rows = crate::storage::collect_scan(scan).await?;
                loaded.insert(request, rows);
            }
        }
        self.load_probed(arrangement, &plan, &probes, &missing, &loaded)
    }

    /// Synchronous form for kernels: the buckets must already be loaded or
    /// loadable through the evaluation session.
    pub(super) fn prefetch_stored_buckets_now(
        &mut self,
        arrangement: &ArrangementKey,
        keys: Vec<JoinKey>,
    ) -> Result<(), IvmRuntimeError> {
        let Some((plan, missing)) = self.unprobed(arrangement, keys) else {
            return Ok(());
        };
        let probes = plan.probe_keys(&missing)?;
        let mut requests = Vec::new();
        plan.root.reads(&probes, &mut requests);
        let inputs = self
            .evaluation_inputs
            .as_deref_mut()
            .ok_or(IvmRuntimeError::StoredArrangementUnprobed)?;
        let mut loaded = LoadedReads::default();
        let mut blocked = false;
        for request in requests {
            match inputs.rows(request.clone()) {
                Ok(rows) => {
                    loaded.insert(request, rows.to_vec());
                }
                Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
                Err(error) => return Err(error),
            }
        }
        if blocked {
            return Err(IvmRuntimeError::EvaluationBlocked);
        }
        self.load_probed(arrangement, &plan, &probes, &missing, &loaded)
    }

    /// The plan and the not yet probed keys of a stored arrangement, or
    /// `None` when there is nothing to read.
    fn unprobed(
        &self,
        arrangement: &ArrangementKey,
        keys: Vec<JoinKey>,
    ) -> Option<(Arc<StoredArrangementPlan>, Vec<JoinKey>)> {
        let state = self.arrangement_states.get(arrangement)?.value();
        let plan = Arc::clone(state.probe_plan()?);
        let mut missing = keys
            .into_iter()
            .filter(|key| !state.is_known(key))
            .collect::<Vec<_>>();
        missing.sort_unstable();
        missing.dedup();
        (!missing.is_empty()).then_some((plan, missing))
    }

    fn load_probed(
        &mut self,
        arrangement: &ArrangementKey,
        plan: &StoredArrangementPlan,
        probes: &[JoinKey],
        keys: &[JoinKey],
        loaded: &LoadedReads,
    ) -> Result<(), IvmRuntimeError> {
        let rows = self.probe_rows(&plan.root, probes, loaded)?;
        let fields = plan.fields.iter().cloned().collect::<BTreeSet<_>>();
        let rows = self.materialize_indirect_fields(&rows, &fields)?;
        let buckets = probed_buckets(&plan.descriptor, &plan.fields, &rows.deltas, keys)?;
        if let Some(state) = self.arrangement_states.get_mut(arrangement) {
            state.value_mut().insert_probed(buckets);
        }
        Ok(())
    }

    /// The rows of `node` under `keys`, from the loaded table rows.
    fn probe_rows(
        &mut self,
        node: &ProbeNode,
        keys: &[JoinKey],
        loaded: &LoadedReads,
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        Ok(Arc::new(match node {
            ProbeNode::Table { source, output, .. } => {
                let mut requests = Vec::new();
                node.reads(keys, &mut requests);
                let table = self
                    .schema
                    .table(&source.table)
                    .ok_or_else(|| IvmRuntimeError::TableNotFound(source.table.clone()))?;
                let rows = requests
                    .iter()
                    .filter_map(|request| loaded.get(request))
                    .flatten()
                    .map(|(_, row)| row.as_slice());
                let grouped = NodeState::group_source_rows(table, rows)?;
                NodeState::update_table_source(
                    source,
                    self.schema,
                    self.variant_projections,
                    output,
                    &grouped,
                )?
            }
            ProbeNode::Filter {
                node,
                filter,
                output,
                input,
            } => {
                let input = self.probe_rows(input, keys, loaded)?;
                self.compute_filter(*node, filter, *output, &input)?
            }
            ProbeNode::Project {
                node,
                project,
                output,
                input,
            } => {
                let input = self.probe_rows(input, keys, loaded)?;
                self.compute_projection(*node, project, *output, &input)?
            }
            ProbeNode::UnwrapNullable {
                unwrap,
                output,
                input,
            } => {
                let input = self.probe_rows(input, keys, loaded)?;
                NodeState::update_unwrap_nullable(unwrap, *output, &input)?
            }
            ProbeNode::ArgBy {
                group_field_indices,
                comparison_field_indices,
                max,
                output,
                input,
            } => {
                let input = self.probe_rows(input, keys, loaded)?;
                let input =
                    self.materialize_indirect_field_indices(&input, comparison_field_indices)?;
                super::recursion::hydrated_arg_by_winners(
                    &input,
                    *output,
                    group_field_indices,
                    comparison_field_indices,
                    if *max {
                        ArgByDirection::Max
                    } else {
                        ArgByDirection::Min
                    },
                )?
            }
            ProbeNode::Union { output, inputs } => {
                let inputs = inputs
                    .iter()
                    .map(|input| self.probe_rows(input, keys, loaded))
                    .collect::<Result<Vec<_>, _>>()?;
                NodeState::update_union(*output, inputs)?
            }
            ProbeNode::Threshold {
                semi,
                left_keys,
                right_keys,
                output,
                left,
                right,
            } => {
                let left = self.probe_rows(left, keys, loaded)?;
                let right = self.probe_rows(right, keys, loaded)?;
                let left_names = left_keys.iter().cloned().collect::<BTreeSet<_>>();
                let right_names = right_keys.iter().cloned().collect::<BTreeSet<_>>();
                let left = self.materialize_indirect_fields(&left, &left_names)?;
                let right = self.materialize_indirect_fields(&right, &right_names)?;
                RecordDeltas {
                    descriptor: *output,
                    deltas: super::join::threshold_snapshot(
                        &left,
                        &right,
                        left_keys,
                        right_keys,
                        ValueComparison::Exact,
                        *semi,
                    )?,
                }
            }
        }))
    }

    /// The stored arrangements `node` reads and the keys it will read from
    /// each: an inner join reads the other side's changed keys; a threshold
    /// join every changed key; a grouped consumer its own changed groups.
    fn stored_probe_requests(
        &mut self,
        node: NodeId,
    ) -> Result<Vec<(ArrangementKey, Vec<JoinKey>)>, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        // Only an input with a storage plan can hold a stored arrangement.
        let stored_input = graph_node.descriptor.inputs.iter().any(|input| {
            self.node_meta
                .get(input)
                .is_some_and(|meta| meta.stored_arrangement.is_some())
        });
        if !stored_input {
            return Ok(Vec::new());
        }
        let inputs = graph_node.descriptor.inputs.clone();
        // (arrangement, the arrangements whose keys it is probed by, and
        // whether by all their keys rather than their changed ones)
        let mut reads = Vec::<(ArrangementKey, Vec<ArrangementKey>, bool)>::new();
        let replace = self.context.arrangement_update_mode == ArrangementUpdateMode::Replace;
        match &graph_node.descriptor.operator {
            OpType::Join(join) | OpType::SemiJoin(join) | OpType::AntiJoin(join) => {
                let threshold = !matches!(graph_node.descriptor.operator, OpType::Join(_));
                let [left_input, right_input] = inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let (left_on, right_on) = self.join_field_names(node, join);
                let left = self.arrangement_key(
                    *left_input,
                    join.left_descriptor,
                    &left_on,
                    join.comparison,
                )?;
                let right = self.arrangement_key(
                    *right_input,
                    join.right_descriptor,
                    &right_on,
                    join.comparison,
                )?;
                if replace {
                    // A rebuild scans the driving side and probes the other
                    // with every key it holds.
                    let (driver, probed) =
                        if self.replace_driver(&graph_node.descriptor) == Some(*right_input) {
                            (right, left)
                        } else {
                            (left, right)
                        };
                    reads.push((probed, vec![driver], true));
                } else {
                    for (stored, other) in [(&left, &right), (&right, &left)] {
                        let mut by = vec![other.clone()];
                        if threshold {
                            by.push(stored.clone());
                        }
                        reads.push((stored.clone(), by, false));
                    }
                }
            }
            OpType::Aggregate(aggregate) => {
                let [input] = inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let group_fields = self.aggregate_group_fields(node, aggregate);
                let key = self.arrangement_key(
                    *input,
                    self.records_output(*input)?,
                    &group_fields,
                    ValueComparison::Exact,
                )?;
                reads.push((key.clone(), vec![key], false));
            }
            OpType::CollectBy(collect_by) if collect_by.mode != CollectByMode::Root => {
                let [input] = inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let key = self.arrangement_key(
                    *input,
                    self.records_output(*input)?,
                    &collect_by.group_fields,
                    ValueComparison::Exact,
                )?;
                reads.push((key.clone(), vec![key], false));
            }
            _ => {}
        }
        let mut requests = Vec::new();
        for (stored, by, all) in reads {
            if self
                .arrangement_states
                .get(&stored)
                .is_none_or(|state| state.value().probe_plan().is_none())
            {
                continue;
            }
            let mut keys = Vec::new();
            for changed in by {
                let stamp = self.arrangement_sub_tick(&changed);
                if let Some(state) = self.arrangement_states.get(&changed) {
                    if all {
                        keys.extend(state.value().keys().cloned());
                        continue;
                    }
                    keys.extend(
                        ArrangementTransition::at(state, stamp)
                            .changed_keys()
                            .cloned(),
                    );
                }
            }
            requests.push((stored, keys));
        }
        Ok(requests)
    }

    /// Whether `node` arranges a stored table, so its arrangement may be
    /// read from storage.
    fn has_storage_plan(&self, node: NodeId) -> bool {
        self.node_meta
            .get(&node)
            .is_some_and(|meta| meta.stored_arrangement.is_some())
    }

    /// The input a Replace evaluation of a join scans in full; the other is
    /// probed by its keys. Semi- and anti-joins scan their left input; an
    /// inner join scans its left input unless only the left is stored.
    fn replace_driver(&self, join: &NodeDescriptor) -> Option<NodeId> {
        match (&join.operator, join.inputs.as_slice()) {
            (OpType::Join(_), [left, right])
                if self.has_storage_plan(*left) && !self.has_storage_plan(*right) =>
            {
                Some(*right)
            }
            (OpType::Join(_) | OpType::SemiJoin(_) | OpType::AntiJoin(_), [left, _]) => Some(*left),
            _ => None,
        }
    }

    /// Whether a Replace evaluation must hold the whole of the stored
    /// arrangement of `arrange`: some consumer scans it rather than probing
    /// it by another input's keys.
    pub(super) fn stored_arrangement_is_scanned(&self, arrange: NodeId) -> bool {
        self.graph.node(arrange).is_some_and(|node| {
            node.children.iter().any(|child| {
                self.graph
                    .node(*child)
                    .is_some_and(|child| match &child.descriptor.operator {
                        OpType::Join(_) | OpType::SemiJoin(_) | OpType::AntiJoin(_) => {
                            self.replace_driver(&child.descriptor) == Some(arrange)
                        }
                        OpType::Aggregate(_) => true,
                        OpType::CollectBy(collect) => collect.mode != CollectByMode::Root,
                        _ => false,
                    })
            })
        })
    }

    fn records_output(&self, node: NodeId) -> Result<RecordDescriptor, IvmRuntimeError> {
        Ok(self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .descriptor
            .output
            .records())
    }

    /// Load every stored bucket `node` will read, awaiting storage when the
    /// evaluation has no session.
    pub(super) async fn prefetch_stored_inputs(
        &mut self,
        node: NodeId,
    ) -> Result<(), IvmRuntimeError> {
        for (arrangement, keys) in self.stored_probe_requests(node)? {
            self.prefetch_stored_buckets(&arrangement, keys).await?;
        }
        Ok(())
    }

    /// Load every stored bucket `node` will read through the evaluation
    /// session. Returns the stored arrangements it reads.
    pub(super) fn prefetch_stored_inputs_now(
        &mut self,
        node: NodeId,
    ) -> Result<Vec<ArrangementKey>, IvmRuntimeError> {
        let mut stored = Vec::new();
        for (arrangement, keys) in self.stored_probe_requests(node)? {
            self.prefetch_stored_buckets_now(&arrangement, keys)?;
            stored.push(arrangement);
        }
        Ok(stored)
    }

    /// Fail an evaluation whose operator read a stored bucket it never
    /// probed, rather than let it read as empty.
    pub(super) fn check_stored_reads(
        &self,
        stored: &[ArrangementKey],
    ) -> Result<(), IvmRuntimeError> {
        for key in stored {
            if self
                .arrangement_states
                .get(key)
                .is_some_and(|state| state.value().take_probe_miss())
            {
                return Err(IvmRuntimeError::StoredArrangementUnprobed);
            }
        }
        Ok(())
    }

    /// The arrangement state for a new `Arrange` node: read from storage
    /// when its node has a storage plan and it lives in the root scope.
    pub(super) fn new_arrangement_state(
        &self,
        key: &ArrangementKey,
    ) -> AsOf<ArrangementState, SubTick> {
        let plan = (key.scope == ScopeId::root())
            .then(|| self.node_meta.get(&key.input))
            .flatten()
            .and_then(|meta| meta.stored_arrangement.clone());
        match plan {
            Some(plan) => AsOf::new(ArrangementState::stored(plan)),
            None => AsOf::default(),
        }
    }
}

//! Physical unary tasks. Graph identity, retained state and recursive execution
//! remain unchanged. Only private stateless edges lose their batch boundary.

use super::evaluator::NodeMemoLookup;
use super::*;
use std::task::{Context, Poll};
mod fields;
use fields::FieldRoutes;

pub(crate) fn supports_node(graph: &IvmGraph, id: NodeId) -> bool {
    let Some(node) = graph.node(id) else {
        return false;
    };
    let [input] = node.descriptor.inputs.as_slice() else {
        return false;
    };
    match &node.descriptor.operator {
        OpType::MapProject(_) => true,
        OpType::Filter(filter) => {
            let Some(input) = graph.node(*input) else {
                return false;
            };
            let descriptor = input.descriptor.output.records();
            let mut fields = BTreeSet::new();
            filter.predicate.referenced_fields(&mut fields);
            fields.iter().all(|field| {
                resolve_field_name(&descriptor, field).is_some_and(|index| {
                    !descriptor.fields()[index]
                        .value_type
                        .may_contain_stored_scalar()
                })
            })
        }
        _ => false,
    }
}

#[derive(Debug)]
enum Stage {
    /// A total projection was composed into subsequent field routes. Keep its
    /// budget slot so arbitrarily deep chains still yield within a row.
    VirtualProject,
    RoutedFilter(FilterOp, FieldRoutes),
    Materialize(FieldRoutes),
    Filter(FilterOp, RecordDescriptor),
    Project {
        project: MapProjectOp,
        input: RecordDescriptor,
        output: RecordDescriptor,
        prepared: Option<Arc<PreparedProjection>>,
        omit_unrepresentable: bool,
    },
}

#[derive(Debug)]
pub(super) struct PreparedPipeline {
    nodes: Arc<[NodeId]>,
    input: NodeId,
    output: RecordDescriptor,
    stages: Vec<Stage>,
}

/// Owns all state across a CPU yield; no borrowed evaluator, published prefix,
/// per-stage RecordDeltas or per-row scratch allocation.
pub(super) struct PendingPipeline {
    plan: Arc<PreparedPipeline>,
    lookup: NodeMemoLookup,
    input: Arc<RecordDeltas>,
    next: usize,
    stage: usize,
    location: usize,
    scratch: [BytesMut; 2],
    output: BytesMut,
    spans: Vec<(std::ops::Range<usize>, i64)>,
    borrowed: Vec<RecordDelta>,
    error: Option<(usize, IvmRuntimeError)>,
    #[cfg(feature = "cold-settle-attribution")]
    stage_visits: u64,
}

impl PendingPipeline {
    fn row(&mut self, index: usize, budget: &mut usize) -> bool {
        let delta = &self.input.deltas[index];
        // 0 = source, 1/2 = reusable scratch. Byte-preserving projections keep
        // the current location; only the descriptor changes in the next stage.
        let mut location = self.location;
        for (stage_index, stage) in self.plan.stages.iter().enumerate().skip(self.stage) {
            if self
                .error
                .as_ref()
                .is_some_and(|(first, _)| stage_index >= *first)
            {
                return true;
            }
            if *budget == 0 {
                self.location = location;
                return false;
            }
            *budget -= 1;
            #[cfg(feature = "cold-settle-attribution")]
            {
                self.stage_visits += 1;
            }
            self.stage = stage_index + 1;
            let raw = match location {
                0 => delta.raw(),
                1 => self.scratch[0].as_ref(),
                _ => self.scratch[1].as_ref(),
            };
            let result = match stage {
                Stage::VirtualProject => continue,
                Stage::RoutedFilter(filter, routes) => filter
                    .predicate
                    .matches(routes.record(raw), filter.comparison),
                Stage::Materialize(routes) => {
                    if routes.reuses_input {
                        continue;
                    }
                    if stage_index + 1 == self.plan.stages.len() {
                        match routes.append(raw, &mut self.output) {
                            Ok(span) => {
                                self.spans.push((span, delta.weight));
                                return true;
                            }
                            Err(error) => Err(error),
                        }
                    } else {
                        let [left, right] = &mut self.scratch;
                        let (raw, destination) = match location {
                            1 => (left.as_ref(), right),
                            2 => (right.as_ref(), left),
                            _ => (delta.raw(), left),
                        };
                        destination.clear();
                        let result = routes.append(raw, destination);
                        location = if location == 1 { 2 } else { 1 };
                        result.map(|_| true)
                    }
                }
                Stage::Filter(filter, descriptor) => filter
                    .predicate
                    .matches(BorrowedRecord::new(raw, descriptor), filter.comparison),
                Stage::Project {
                    project,
                    input,
                    output,
                    prepared,
                    omit_unrepresentable,
                } => {
                    if prepared.as_ref().is_some_and(|plan| plan.reuses_input) {
                        continue;
                    }
                    if stage_index + 1 == self.plan.stages.len() {
                        let start = self.output.len();
                        match NodeState::project_row_into(
                            project,
                            *output,
                            *input,
                            raw,
                            prepared.as_deref(),
                            *omit_unrepresentable,
                            &mut self.output,
                        ) {
                            Ok(Some(span)) => {
                                self.spans.push((span, delta.weight));
                                return true;
                            }
                            Ok(None) => {
                                self.output.truncate(start);
                                return true;
                            }
                            Err(error) => Err(error),
                        }
                    } else {
                        let [left, right] = &mut self.scratch;
                        let (raw, destination) = match location {
                            1 => (left.as_ref(), right),
                            2 => (right.as_ref(), left),
                            _ => (delta.raw(), left),
                        };
                        destination.clear();
                        let result = NodeState::project_row_into(
                            project,
                            *output,
                            *input,
                            raw,
                            prepared.as_deref(),
                            *omit_unrepresentable,
                            destination,
                        );
                        location = if location == 1 { 2 } else { 1 };
                        result.map(|span| span.is_some())
                    }
                }
            };
            match result {
                Ok(true) => {}
                Ok(false) => return true,
                Err(error) => {
                    // Earlier stages outrank later ones, regardless of row.
                    // Keep evaluating only the preceding stages on subsequent
                    // rows; this preserves stage-major first-error semantics.
                    self.error = Some((stage_index, error));
                    self.output.clear();
                    self.spans.clear();
                    self.borrowed.clear();
                    return true;
                }
            }
        }
        if location == 0 {
            self.borrowed.push(delta.clone());
            return true;
        }
        let raw = match location {
            1 => self.scratch[0].as_ref(),
            _ => self.scratch[1].as_ref(),
        };
        let start = self.output.len();
        self.output.extend_from_slice(raw);
        self.spans.push((start..self.output.len(), delta.weight));
        true
    }

    fn finish(self) -> Result<(NodeMemoLookup, RecordDeltas), IvmRuntimeError> {
        if let Some((_, error)) = self.error {
            return Err(error);
        }
        let bytes = self.output.freeze();
        let deltas = if self.spans.is_empty() {
            self.borrowed
        } else {
            self.spans
                .into_iter()
                .map(|(span, weight)| RecordDelta {
                    record: bytes.slice(span),
                    weight,
                })
                .collect()
        };
        Ok((
            self.lookup,
            RecordDeltas {
                descriptor: self.plan.output,
                deltas,
            },
        ))
    }
}

impl TickEvaluator<'_> {
    fn prepared_pipeline(&mut self, nodes: &Arc<[NodeId]>) -> Option<Arc<PreparedPipeline>> {
        let tail = *nodes.last()?;
        if let Some(plan) = self
            .node_meta
            .get(&tail)
            .and_then(|meta| meta.pipeline.as_ref())
            && plan.nodes.as_ref() == nodes.as_ref()
        {
            return Some(Arc::clone(plan));
        }
        let input = self.graph.node(nodes[0])?.descriptor.inputs[0];
        let mut stages = Vec::with_capacity(nodes.len());
        let mut descriptor = self.graph.node(input)?.descriptor.output.records();
        let mut routes = FieldRoutes::identity(descriptor);
        let mut virtual_rows = false;
        for node in nodes.iter().copied() {
            let graph_node = self.graph.node(node)?;
            let output = graph_node.descriptor.output.records();
            let stage = match &graph_node.descriptor.operator {
                OpType::Filter(filter) if virtual_rows => {
                    Stage::RoutedFilter(filter.clone(), routes.clone())
                }
                OpType::Filter(filter) => Stage::Filter(filter.clone(), descriptor),
                OpType::MapProject(project) => {
                    let prepared = self
                        .raw_projection_fields(node, project, &descriptor, output)
                        .ok()?;
                    if let Some(composed) = prepared
                        .as_ref()
                        .and_then(|plan| routes.compose(output, plan))
                    {
                        routes = composed;
                        virtual_rows = true;
                        descriptor = output;
                        stages.push(Stage::VirtualProject);
                        continue;
                    }
                    if virtual_rows {
                        stages.push(Stage::Materialize(routes));
                    }
                    routes = FieldRoutes::identity(output);
                    virtual_rows = false;
                    Stage::Project {
                        project: project.clone(),
                        input: descriptor,
                        output,
                        // If preparation itself fails, run the ordinary evaluator
                        // to preserve empty-input and upstream error precedence.
                        prepared,
                        omit_unrepresentable: project.expressions.iter().any(|expression| {
                            matches!(
                                expression.expression,
                                ProjectExpr::RecursiveEnumRemap {
                                    omit_unrepresentable: true,
                                    ..
                                }
                            )
                        }),
                    }
                }
                _ => return None,
            };
            stages.push(stage);
            descriptor = output;
        }
        if virtual_rows {
            stages.push(Stage::Materialize(routes));
        }
        let plan = Arc::new(PreparedPipeline {
            nodes: Arc::clone(nodes),
            input,
            output: descriptor,
            stages,
        });
        self.node_meta.entry(tail).or_default().pipeline = Some(Arc::clone(&plan));
        Some(plan)
    }

    pub(super) fn poll_pipeline(
        &mut self,
        nodes: &Arc<[NodeId]>,
        pending: &mut HashMap<NodeId, PendingPipeline>,
        frame_inputs: super::evaluator::FrameInputs<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        let tail = *nodes.last().expect("nonempty pipeline");
        if let std::collections::hash_map::Entry::Vacant(entry) = pending.entry(tail) {
            if self
                .node_meta
                .get(&tail)
                .is_none_or(|meta| meta.input_signature.is_none())
            {
                // Contracting the queue must not turn metadata preparation
                // back into recursive traversal of an arbitrarily deep chain.
                for node in nodes.iter().copied() {
                    if let Err(error) = self.input_signature(node) {
                        return Poll::Ready(Err(error));
                    }
                }
            }
            let lookup = match self.prepare_memo_lookup(tail) {
                Ok(lookup) => lookup,
                Err(error) => return Poll::Ready(Err(error)),
            };
            match self.cached_node_records(&lookup) {
                Ok(Some(records)) => return Poll::Ready(Ok(records)),
                Err(error) => return Poll::Ready(Err(error)),
                Ok(None) => {}
            }
            if self.context.sub_tick > 1 && !lookup.depends_on_context {
                return self.compute_node(tail, lookup).as_mut().poll(cx);
            }
            let Some(plan) = self.prepared_pipeline(nodes) else {
                return self.compute_node(tail, lookup).as_mut().poll(cx);
            };
            let resident = match self.resolve_register_inputs(frame_inputs) {
                Ok(Some(mut inputs)) => inputs.pop(),
                Ok(None) => None,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let input = match resident.map_or_else(
                || {
                    self.prepare_memo_lookup(plan.input)
                        .and_then(|key| self.cached_node_records(&key))
                },
                |input| Ok(Some(input)),
            ) {
                Ok(Some(input)) => input,
                Err(error) => return Poll::Ready(Err(error)),
                Ok(None) => return self.compute_node(tail, lookup).as_mut().poll(cx),
            };
            if self.context.eval_mode == EvalMode::Hydrate {
                self.metrics.hydration_memo_computes += 1;
                self.metrics.hydration_memo_computed_nodes.insert(tail);
            }
            entry.insert(PendingPipeline {
                plan,
                lookup,
                input,
                next: 0,
                stage: 0,
                location: 0,
                scratch: Default::default(),
                output: BytesMut::new(),
                spans: Vec::new(),
                borrowed: Vec::new(),
                error: None,
                #[cfg(feature = "cold-settle-attribution")]
                stage_visits: 0,
            });
        }
        let batch = pending.get_mut(&tail).expect("prepared pipeline");
        let mut budget = 256;
        while batch.next < batch.input.deltas.len() && budget > 0 {
            if !batch.row(batch.next, &mut budget) {
                break;
            }
            batch.next += 1;
            batch.stage = 0;
            batch.location = 0;
            if batch.error.as_ref().is_some_and(|(stage, _)| *stage == 0) {
                break;
            }
        }
        if batch.next < batch.input.deltas.len()
            && batch.error.as_ref().is_none_or(|(stage, _)| *stage != 0)
        {
            return Poll::Pending;
        }
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_pipeline(
            self.context.eval_mode == EvalMode::Hydrate,
            batch.input.deltas.len(),
            batch.stage_visits,
        );
        Poll::Ready(
            pending
                .remove(&tail)
                .expect("completed pipeline")
                .finish()
                .map(|(lookup, output)| self.memoize_result(&lookup, output)),
        )
    }
}

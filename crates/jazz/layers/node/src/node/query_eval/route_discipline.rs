//! Route discipline for prepared graphs shared across bindings (#4017).
//!
//! A prepared graph reads every binding of its binding source at once, so
//! one compiled graph can serve every subscriber of a query. It keeps those
//! subscribers apart only through binding routes: a row whose membership
//! depends on a binding carries that binding's route values, every operator
//! that combines or groups such rows keys on them, and each terminal
//! delivers a row only to the bindings whose values it carries.
//!
//! The lowering applies this rule operator by operator. This module checks
//! the result structurally instead of trusting each operator. A program that
//! fails the check still runs, but on a binding source of its own: it shares
//! nothing with another binding, exactly as before graphs were shared.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use groove::ivm::{FieldRef, GraphBuilder, TopByLimit};

use super::lowering::{
    app_row_terminal_route_eligible_fields, prepared_route_param_names,
    terminal_route_eligible_fields, terminal_route_fields,
};
use super::query_engine::ParameterDomain;
use super::*;

/// Why a lowered program may not share a binding source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RouteViolation(pub(super) String);

/// Binding slots of a parameter domain, by the route field each carries in
/// rows: a user parameter as `__jazz_route_<name>`, a claim under its own
/// slot name.
pub(super) fn binding_slot_routes(domain: &ParameterDomain) -> BTreeMap<String, String> {
    domain
        .user_params
        .keys()
        .map(|param| (param.clone(), route_param_field(param)))
        .chain(
            domain
                .claim_params
                .keys()
                .map(|claim| (claim.clone(), claim.clone())),
        )
        .collect()
}

/// Checks that every terminal keeps each binding's rows apart.
///
/// `terminals` pairs each terminal graph with the route fields its sink
/// delivers by. `infer` describes a subgraph's output records.
pub(super) fn check_route_discipline<'g, E: std::fmt::Debug>(
    terminals: impl IntoIterator<Item = (&'g str, &'g GraphBuilder, BTreeSet<String>)>,
    slot_routes: &BTreeMap<String, String>,
    infer: impl Fn(&GraphBuilder) -> Result<RecordDescriptor, E>,
) -> Result<(), RouteViolation> {
    let mut checker = RouteChecker {
        slot_routes,
        routes: slot_routes.values().cloned().collect(),
        infer,
        deps: HashMap::new(),
        carried: HashMap::new(),
        checked: BTreeSet::new(),
    };
    for (sink, graph, delivered) in terminals {
        let depends = checker.depends(graph);
        if !depends.is_subset(&delivered) {
            return Err(RouteViolation(format!(
                "terminal {sink} depends on binding routes {depends:?} but delivers by {delivered:?}"
            )));
        }
        checker.check(graph)?;
    }
    Ok(())
}

struct RouteChecker<'a, F> {
    slot_routes: &'a BTreeMap<String, String>,
    routes: BTreeSet<String>,
    infer: F,
    deps: HashMap<*const GraphBuilder, BTreeSet<String>>,
    carried: HashMap<*const GraphBuilder, BTreeSet<String>>,
    checked: BTreeSet<*const GraphBuilder>,
}

impl<F, E> RouteChecker<'_, F>
where
    F: Fn(&GraphBuilder) -> Result<RecordDescriptor, E>,
    E: std::fmt::Debug,
{
    /// Binding routes whose values can decide which rows `node` outputs:
    /// every slot of every binding source it reads.
    fn depends(&mut self, node: &GraphBuilder) -> BTreeSet<String> {
        let key = std::ptr::from_ref(node);
        if let Some(depends) = self.deps.get(&key) {
            return depends.clone();
        }
        let depends: BTreeSet<String> = match node {
            GraphBuilder::BindingSource { output, .. } => output
                .fields()
                .iter()
                .filter_map(|field| field.name.as_deref())
                .map(|slot| {
                    self.slot_routes
                        .get(slot)
                        .cloned()
                        .unwrap_or_else(|| slot.to_owned())
                })
                .collect(),
            _ => children(node)
                .into_iter()
                .flat_map(|child| self.depends(child))
                .collect(),
        };
        self.deps.insert(key, depends.clone());
        depends
    }

    /// Binding route fields present in `node`'s output records.
    fn carried(&mut self, node: &GraphBuilder) -> Result<BTreeSet<String>, RouteViolation> {
        let key = std::ptr::from_ref(node);
        if let Some(carried) = self.carried.get(&key) {
            return Ok(carried.clone());
        }
        let carried = self
            .output_names(node)?
            .into_iter()
            .filter(|field| self.routes.contains(field))
            .collect::<BTreeSet<_>>();
        self.carried.insert(key, carried.clone());
        Ok(carried)
    }

    fn output_names(&self, node: &GraphBuilder) -> Result<Vec<String>, RouteViolation> {
        let output = (self.infer)(node).map_err(|error| {
            RouteViolation(format!(
                "cannot describe a binding-dependent input: {error:?}"
            ))
        })?;
        Ok(output
            .fields()
            .iter()
            .map(|field| field.name.clone().unwrap_or_default())
            .collect())
    }

    fn field_name(&self, input: &GraphBuilder, field: &FieldRef) -> Result<String, RouteViolation> {
        Ok(match field {
            FieldRef::Name(name) | FieldRef::StoredName(name) => name.clone(),
            FieldRef::Resolved(index) => self
                .output_names(input)?
                .get(*index)
                .cloned()
                .unwrap_or_default(),
        })
    }

    fn check(&mut self, root: &GraphBuilder) -> Result<(), RouteViolation> {
        let mut pending = vec![root];
        while let Some(node) = pending.pop() {
            if !self.checked.insert(std::ptr::from_ref(node)) {
                continue;
            }
            pending.extend(children(node));
            self.check_node(node)?;
        }
        Ok(())
    }

    fn check_node(&mut self, node: &GraphBuilder) -> Result<(), RouteViolation> {
        match node {
            GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
                ..
            }
            | GraphBuilder::SemiJoin {
                left,
                right,
                left_on,
                right_on,
                ..
            }
            | GraphBuilder::AntiJoin {
                left,
                right,
                left_on,
                right_on,
                ..
            } => {
                let left_depends = self.depends(left);
                let right_depends = self.depends(right);
                if left_depends.is_empty() && right_depends.is_empty() {
                    return Ok(());
                }
                // A binding-dependent input must carry the routes it depends
                // on, or the join cannot tell whose rows it is matching.
                let left_carried = self.carried(left)?;
                let right_carried = self.carried(right)?;
                for (side, depends, carried) in [
                    ("left", &left_depends, &left_carried),
                    ("right", &right_depends, &right_carried),
                ] {
                    if !depends.is_subset(carried) {
                        return Err(RouteViolation(format!(
                            "{} {side} input depends on binding routes {depends:?} but carries {carried:?} (left names {:?}, right names {:?})",
                            operator_name(node),
                            self.output_names(left)?,
                            self.output_names(right)?,
                        )));
                    }
                }
                let mut keyed = BTreeSet::new();
                for (left_key, right_key) in left_on.iter().zip(right_on) {
                    let left_key = self.field_name(left, left_key)?;
                    if left_key == self.field_name(right, right_key)? {
                        keyed.insert(left_key);
                    }
                }
                // An inner join must pair each route both sides carry. A
                // semi or anti join emits left rows only, so every route its
                // right input depends on must also key the left row.
                let required = if matches!(node, GraphBuilder::Join { .. }) {
                    left_carried
                        .intersection(&right_carried)
                        .cloned()
                        .collect::<BTreeSet<_>>()
                } else {
                    right_depends
                };
                if let Some(route) = required.difference(&keyed).next() {
                    let keys = left_on
                        .iter()
                        .zip(right_on)
                        .map(|(l, r)| Ok((self.field_name(left, l)?, self.field_name(right, r)?)))
                        .collect::<Result<Vec<_>, RouteViolation>>()?;
                    return Err(RouteViolation(format!(
                        "join does not key on binding route {route} (keys {keys:?}, left carries {left_carried:?}, right carries {right_carried:?}, left names {:?}, right names {:?})",
                        self.output_names(left)?,
                        self.output_names(right)?,
                    )));
                }
                Ok(())
            }
            GraphBuilder::Aggregate {
                input, group_cols, ..
            }
            | GraphBuilder::ArgMaxBy {
                input, group_cols, ..
            }
            | GraphBuilder::ArgMinBy {
                input, group_cols, ..
            }
            | GraphBuilder::TopBy {
                input, group_cols, ..
            } => {
                if let GraphBuilder::TopBy {
                    offset: 0,
                    limit: TopByLimit::Unbounded,
                    ..
                } = node
                {
                    // Orders without excluding any row: nothing to separate.
                    return Ok(());
                }
                // Grouping across bindings would aggregate, rank or page one
                // subscriber's rows together with another's.
                let depends = self.depends(input);
                if depends.is_empty() {
                    return Ok(());
                }
                let groups = group_cols
                    .iter()
                    .map(|field| self.field_name(input, field))
                    .collect::<Result<BTreeSet<_>, _>>()?;
                match depends.difference(&groups).next() {
                    Some(route) => Err(RouteViolation(format!(
                        "grouping does not group by binding route {route}"
                    ))),
                    None => Ok(()),
                }
            }
            GraphBuilder::CollectBy { input, collect } => {
                // Each collection must belong to one binding's parent record.
                let depends = self.depends(input);
                if depends.is_empty() {
                    return Ok(());
                }
                let mut groupings = vec![&collect.group_cols];
                let mut slots = collect.slots.iter().collect::<Vec<_>>();
                while let Some(slot) = slots.pop() {
                    groupings.push(&slot.group_cols);
                    slots.extend(&slot.slots);
                }
                for group_cols in groupings {
                    let groups = group_cols
                        .iter()
                        .map(|field| self.field_name(input, field))
                        .collect::<Result<BTreeSet<_>, _>>()?;
                    if let Some(route) = depends.difference(&groups).next() {
                        return Err(RouteViolation(format!(
                            "collection does not group by binding route {route}"
                        )));
                    }
                }
                Ok(())
            }
            GraphBuilder::Recursive { .. }
            | GraphBuilder::RecursiveStepWitness { .. }
            | GraphBuilder::TypedTemplate { .. }
            | GraphBuilder::TemplateInput { .. } => {
                // Not route-aware yet: keep them private to one binding.
                let depends = self.depends(node);
                if depends.is_empty() {
                    Ok(())
                } else {
                    Err(RouteViolation(format!(
                        "{} is not route-aware but depends on binding routes {depends:?}",
                        operator_name(node)
                    )))
                }
            }
            _ => Ok(()),
        }
    }
}

fn operator_name(node: &GraphBuilder) -> &'static str {
    match node {
        GraphBuilder::Recursive { .. } => "Recursive",
        GraphBuilder::RecursiveStepWitness { .. } => "RecursiveStepWitness",
        GraphBuilder::TypedTemplate { .. } => "TypedTemplate",
        GraphBuilder::TemplateInput { .. } => "TemplateInput",
        GraphBuilder::Join { .. } => "join",
        GraphBuilder::SemiJoin { .. } => "semi-join",
        GraphBuilder::AntiJoin { .. } => "anti-join",
        _ => "operator",
    }
}

fn children(node: &GraphBuilder) -> Vec<&GraphBuilder> {
    match node {
        GraphBuilder::TypedTemplate { inputs, .. } | GraphBuilder::Union { inputs } => {
            inputs.iter().map(AsRef::as_ref).collect()
        }
        GraphBuilder::TemplateInput { input, .. } => input.iter().map(AsRef::as_ref).collect(),
        GraphBuilder::Recursive {
            seed,
            step,
            step_witness,
            ..
        } => [Some(seed), Some(step), step_witness.as_ref()]
            .into_iter()
            .flatten()
            .map(AsRef::as_ref)
            .collect(),
        GraphBuilder::RecursiveStepWitness { recursive } => vec![recursive.as_ref()],
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::StreamingChecksum { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => vec![input.as_ref()],
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::InputSource { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => Vec::new(),
    }
}

/// Renames every binding source `graph` reads by appending `suffix`.
pub(super) fn isolate_binding_sources(graph: &GraphBuilder, suffix: &str) -> GraphBuilder {
    let mut renamed = HashMap::new();
    isolate_node(graph, suffix, &mut renamed)
}

fn isolate_node(
    node: &GraphBuilder,
    suffix: &str,
    renamed: &mut HashMap<*const GraphBuilder, std::sync::Arc<GraphBuilder>>,
) -> GraphBuilder {
    let mut arc = |child: &std::sync::Arc<GraphBuilder>| {
        let key = std::sync::Arc::as_ptr(child);
        if let Some(done) = renamed.get(&key) {
            return std::sync::Arc::clone(done);
        }
        let done = std::sync::Arc::new(isolate_node(child, suffix, renamed));
        renamed.insert(key, std::sync::Arc::clone(&done));
        done
    };
    let mut node = node.clone();
    match &mut node {
        GraphBuilder::BindingSource { shape, .. } => shape.push_str(suffix),
        GraphBuilder::TypedTemplate { inputs, .. } | GraphBuilder::Union { inputs } => {
            for input in inputs {
                *input = arc(input);
            }
        }
        GraphBuilder::TemplateInput { input, .. } => {
            if let Some(input) = input {
                *input = arc(input);
            }
        }
        GraphBuilder::Recursive {
            seed,
            step,
            step_witness,
            ..
        } => {
            *seed = arc(seed);
            *step = arc(step);
            if let Some(witness) = step_witness {
                *witness = arc(witness);
            }
        }
        GraphBuilder::RecursiveStepWitness { recursive } => *recursive = arc(recursive),
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::StreamingChecksum { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => *input = arc(input),
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            *left = arc(left);
            *right = arc(right);
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::InputSource { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => {}
    }
    node
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Gives a program that fails [`check_route_discipline`] a binding
    /// source of its own, so its graph never serves another binding.
    ///
    /// The name is scoped to this session's claims and this binding's
    /// values: equal bindings of one session still share a graph, since they
    /// read exactly the same rows.
    pub(super) fn isolate_route_unsafe_program(&self, program: &mut QueryProgram) {
        let Some(source_shape) = program.request.input.binding.source_shape.clone() else {
            return;
        };
        if matches!(
            program.request.policy,
            PolicyContext::AuthorizationSubplan { .. }
        ) {
            // A subplan is embedded in, and checked with, its outer program.
            return;
        }
        let Err(violation) = self.check_program_route_discipline(program) else {
            return;
        };
        let suffix = self.isolated_binding_suffix(&program.request);
        if crate::debug_env::covered_input_trace() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=isolate_binding_source source_shape={source_shape} reason={violation:?}"
            );
        }
        for terminal in &mut program.lowered.terminals {
            terminal.graph = isolate_binding_sources(&terminal.graph, &suffix);
        }
        if let Some(graph) = program.lowered.internal_app_rows_graph.as_mut() {
            *graph = isolate_binding_sources(graph, &suffix);
        }
        program.request.input.binding.source_shape = Some(format!("{source_shape}{suffix}"));
    }

    pub(super) fn check_program_route_discipline(
        &self,
        program: &QueryProgram,
    ) -> Result<(), RouteViolation> {
        let parameters = &program.lowered.parameters;
        let route_params = prepared_route_param_names(parameters);
        let mut terminals = Vec::new();
        for terminal in &program.lowered.terminals {
            let eligible = terminal_route_eligible_fields(&terminal.output)
                .map_err(|error| RouteViolation(format!("{error:?}")))?;
            terminals.push((
                terminal.sink.as_str(),
                &terminal.graph,
                terminal_route_fields(&route_params, &eligible)
                    .into_iter()
                    .collect(),
            ));
        }
        if let Some(graph) = &program.lowered.internal_app_rows_graph {
            let eligible = app_row_terminal_route_eligible_fields(&program.lowered.output)
                .map_err(|error| RouteViolation(format!("{error:?}")))?;
            terminals.push((
                "internal-app-rows",
                graph,
                route_params
                    .iter()
                    .filter(|field| eligible.contains(*field))
                    .cloned()
                    .collect(),
            ));
        }
        check_route_discipline(terminals, &binding_slot_routes(parameters), |graph| {
            self.database.graph_output_descriptor(graph)
        })
    }

    fn isolated_binding_suffix(&self, request: &QueryProgramRequest) -> String {
        let session = match &request.policy {
            PolicyContext::Identity {
                permission_subject, ..
            } => self
                .active_session_claim_scope_key(*permission_subject)
                .unwrap_or_else(|| format!("{permission_subject:?}")),
            _ => "system".to_owned(),
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"jazz/isolated-binding-source/v1\0");
        hasher.update(session.as_bytes());
        hasher.update(format!("{:?}", request.input.binding.id).as_bytes());
        format!(":isolated:{}", hasher.finalize().to_hex())
    }
}

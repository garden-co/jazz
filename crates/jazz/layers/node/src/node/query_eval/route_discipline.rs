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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use groove::ivm::{FieldRef, GraphBuilder, ProjectExpr, TopByLimit};

use super::lowering::{
    app_row_terminal_route_eligible_fields, prepared_route_param_names,
    terminal_route_eligible_fields, terminal_route_fields,
};
use super::query_engine::ParameterDomain;
use super::*;

mod borrowed;

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
    infer: impl Fn(&'g GraphBuilder) -> Result<RecordDescriptor, E>,
) -> Result<(), RouteViolation> {
    check_routes(terminals, slot_routes, &DirectGraph { infer })
}

trait RouteGraph<'g> {
    type Node: Copy;
    fn operator(&self, node: Self::Node) -> &'g GraphBuilder;
    fn child(&self, node: Self::Node, child: &'g GraphBuilder) -> Self::Node;
    fn key(&self, node: Self::Node) -> usize;
    fn output(&self, node: Self::Node) -> Result<RecordDescriptor, RouteViolation>;
    fn children(&self, node: Self::Node) -> Vec<Self::Node> {
        children(self.operator(node))
            .map(|child| self.child(node, child))
            .collect()
    }
}

struct DirectGraph<F> {
    infer: F,
}
impl<'g, F, E> RouteGraph<'g> for DirectGraph<F>
where
    F: Fn(&'g GraphBuilder) -> Result<RecordDescriptor, E>,
    E: std::fmt::Debug,
{
    type Node = &'g GraphBuilder;
    fn operator(&self, node: Self::Node) -> &'g GraphBuilder {
        node
    }
    fn child(&self, _: Self::Node, child: &'g GraphBuilder) -> Self::Node {
        child
    }
    fn key(&self, node: Self::Node) -> usize {
        std::ptr::from_ref(node) as usize
    }
    fn output(&self, node: Self::Node) -> Result<RecordDescriptor, RouteViolation> {
        (self.infer)(node).map_err(|error| {
            RouteViolation(format!(
                "cannot describe a binding-dependent input: {error:?}"
            ))
        })
    }
}

fn check_routes<'g, G: RouteGraph<'g>>(
    terminals: impl IntoIterator<Item = (&'g str, G::Node, BTreeSet<String>)>,
    slot_routes: &BTreeMap<String, String>,
    graph: &G,
) -> Result<(), RouteViolation> {
    let terminals = terminals.into_iter().collect::<Vec<_>>();
    let aliases = slot_aliases(
        terminals.iter().map(|(_, graph, _)| *graph),
        slot_routes,
        graph,
    );
    let mut checker = RouteChecker {
        slot_routes,
        aliases,
        routes: slot_routes.values().cloned().collect(),
        graph,
        deps: HashMap::new(),
        carried: HashMap::new(),
        masks: HashMap::new(),
        equivalents: HashMap::new(),
        checked: BTreeSet::new(),
        graphs: std::marker::PhantomData,
    };
    for (sink, graph, delivered) in terminals {
        let depends = checker.depends(graph);
        if !depends.is_subset(&delivered) {
            return Err(RouteViolation(format!(
                "terminal {sink} depends on binding routes {depends:?} but delivers by {delivered:?}"
            )));
        }
        checker.check(graph)?;
        let masked = checker.masked(graph)?;
        if !masked.is_empty() {
            return Err(RouteViolation(format!(
                "terminal {sink} reads binding-keyed lookup rows for routes {masked:?}, which shared programs do not support yet"
            )));
        }
    }
    Ok(())
}

/// Slot names a graph may use for its route: a binding source names its
/// fields by slot, and a join against it keeps that name. A slot whose name
/// a stored relation also uses is not aliased, so a stored column is never
/// taken for a route.
fn slot_aliases<'g, G: RouteGraph<'g>>(
    graphs: impl IntoIterator<Item = G::Node>,
    slot_routes: &BTreeMap<String, String>,
    graph: &G,
) -> BTreeMap<String, String> {
    let mut stored = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut pending = graphs.into_iter().collect::<Vec<_>>();
    while let Some(node) = pending.pop() {
        if !seen.insert(graph.key(node)) {
            continue;
        }
        let kids = graph.children(node);
        if !matches!(graph.operator(node), GraphBuilder::BindingSource { .. })
            && (kids.is_empty() || matches!(graph.operator(node), GraphBuilder::Lookup { .. }))
        {
            match graph.output(node) {
                Ok(output) => stored.extend(
                    output
                        .fields()
                        .iter()
                        .filter_map(|field| field.name.clone()),
                ),
                // Unknown columns could be named like any slot.
                Err(_) => return BTreeMap::new(),
            }
        }
        pending.extend(kids);
    }
    slot_routes
        .iter()
        .filter(|(slot, route)| slot != route && !stored.contains(*slot))
        .map(|(slot, route)| (slot.clone(), route.clone()))
        .collect()
}

struct RouteChecker<'a, 'g, G: RouteGraph<'g>> {
    slot_routes: &'a BTreeMap<String, String>,
    aliases: BTreeMap<String, String>,
    routes: BTreeSet<String>,
    graph: &'a G,
    deps: HashMap<usize, BTreeSet<String>>,
    carried: HashMap<usize, BTreeSet<String>>,
    masks: HashMap<usize, BTreeSet<String>>,
    equivalents: HashMap<usize, BTreeMap<String, String>>,
    checked: BTreeSet<usize>,
    graphs: std::marker::PhantomData<&'g GraphBuilder>,
}

impl<'g, G: RouteGraph<'g>> RouteChecker<'_, 'g, G> {
    /// Binding routes whose values can decide which rows `node` outputs:
    /// every slot of every binding source it reads.
    fn depends(&mut self, node: G::Node) -> BTreeSet<String> {
        let key = self.graph.key(node);
        if let Some(depends) = self.deps.get(&key) {
            return depends.clone();
        }
        let depends: BTreeSet<String> = match self.graph.operator(node) {
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
            _ => self
                .graph
                .children(node)
                .into_iter()
                .flat_map(|child| self.depends(child))
                .collect(),
        };
        self.deps.insert(key, depends.clone());
        depends
    }

    /// Binding route fields present in `node`'s output records.
    fn carried(&mut self, node: G::Node) -> Result<BTreeSet<String>, RouteViolation> {
        let key = self.graph.key(node);
        if let Some(carried) = self.carried.get(&key) {
            return Ok(carried.clone());
        }
        let carried = self
            .output_names(node)?
            .into_iter()
            .map(|field| self.route_name(field))
            .filter(|field| self.routes.contains(field))
            .collect::<BTreeSet<_>>();
        self.carried.insert(key, carried.clone());
        Ok(carried)
    }

    /// Binding routes `node` depends on through a binding lookup
    /// (`GraphBuilder::Lookup` keyed by binding values). Such rows are a
    /// superset for every binding at once and carry no route. A join could
    /// pair them with their route only on the lookup's own key, which this
    /// check cannot yet tell from the table's other columns, so a program
    /// that reads one is not shared.
    fn masked(&mut self, node: G::Node) -> Result<BTreeSet<String>, RouteViolation> {
        let key = self.graph.key(node);
        if let Some(masked) = self.masks.get(&key) {
            return Ok(masked.clone());
        }
        let masked = match self.graph.operator(node) {
            GraphBuilder::Lookup { keys, .. } => self.depends(self.graph.child(node, keys)),
            _ => {
                let mut masked = BTreeSet::new();
                for child in self.graph.children(node) {
                    masked.extend(self.masked(child)?);
                }
                masked
            }
        };
        self.masks.insert(key, masked.clone());
        Ok(masked)
    }

    fn output_names(&self, node: G::Node) -> Result<Vec<String>, RouteViolation> {
        let output = self.graph.output(node)?;
        Ok(output
            .fields()
            .iter()
            .map(|field| field.name.clone().unwrap_or_default())
            .collect())
    }

    /// The route a join key stands for: the key's own route name, or the
    /// route an earlier join pinned that field to.
    fn key_route(&mut self, input: G::Node, field: &FieldRef) -> Result<String, RouteViolation> {
        let name = self.raw_field_name(input, field)?;
        let equivalents = self.equivalents(input)?;
        Ok(equivalents
            .get(&name)
            .cloned()
            .unwrap_or_else(|| self.route_name(name)))
    }

    /// Fields of `node`'s output that equal a binding route in every row,
    /// because a join keyed them on that route. A later join keyed on such
    /// a field keys on the route itself.
    fn equivalents(&mut self, node: G::Node) -> Result<BTreeMap<String, String>, RouteViolation> {
        let key = self.graph.key(node);
        if let Some(equivalents) = self.equivalents.get(&key) {
            return Ok(equivalents.clone());
        }
        let equivalents = match self.graph.operator(node) {
            GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
                ..
            } => {
                let left = self.graph.child(node, left);
                let right = self.graph.child(node, right);
                let mut equivalents = BTreeMap::new();
                for (prefix, side) in [("left.", left), ("right.", right)] {
                    for (field, route) in self.equivalents(side)? {
                        equivalents.insert(format!("{prefix}{field}"), route);
                    }
                }
                let left_carried = self.carried(left)?;
                let right_carried = self.carried(right)?;
                for (left_key, right_key) in left_on.iter().zip(right_on) {
                    let left_name = self.raw_field_name(left, left_key)?;
                    let right_name = self.raw_field_name(right, right_key)?;
                    let left_route = self.key_route(left, left_key)?;
                    let right_route = self.key_route(right, right_key)?;
                    if right_carried.contains(&right_route)
                        || self.routes.contains(&right_route) && right_route != right_name
                    {
                        equivalents.insert(format!("left.{left_name}"), right_route.clone());
                    }
                    if left_carried.contains(&left_route)
                        || self.routes.contains(&left_route) && left_route != left_name
                    {
                        equivalents.insert(format!("right.{right_name}"), left_route.clone());
                    }
                }
                equivalents
            }
            GraphBuilder::Project { input, fields } => {
                let input = self.graph.child(node, input);
                let input_equivalents = self.equivalents(input)?;
                let mut equivalents = BTreeMap::new();
                for field in fields {
                    let source = match &field.expression {
                        ProjectExpr::Field(source)
                        | ProjectExpr::Nullable(source)
                        | ProjectExpr::NullableFlat(source) => source,
                        _ => continue,
                    };
                    let source = self.raw_field_name(input, source)?;
                    if let Some(route) = input_equivalents.get(&source) {
                        equivalents.insert(field.output_name.clone(), route.clone());
                    }
                }
                equivalents
            }
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::SemiJoin { left: input, .. }
            | GraphBuilder::AntiJoin { left: input, .. } => {
                self.equivalents(self.graph.child(node, input))?
            }
            _ => BTreeMap::new(),
        };
        self.equivalents.insert(key, equivalents.clone());
        Ok(equivalents)
    }

    /// `name`, or the route it aliases.
    fn route_name(&self, name: String) -> String {
        self.aliases.get(&name).cloned().unwrap_or(name)
    }

    /// The name of `field` in `input`, as the route it aliases if any.
    fn field_name(&self, input: G::Node, field: &FieldRef) -> Result<String, RouteViolation> {
        let name = self.raw_field_name(input, field)?;
        Ok(self.route_name(name))
    }

    fn raw_field_name(&self, input: G::Node, field: &FieldRef) -> Result<String, RouteViolation> {
        Ok(match field {
            FieldRef::Name(name) | FieldRef::StoredName(name) => name.clone(),
            FieldRef::Resolved(index) => self
                .output_names(input)?
                .get(*index)
                .cloned()
                .unwrap_or_default(),
        })
    }

    fn check(&mut self, root: G::Node) -> Result<(), RouteViolation> {
        let mut pending = vec![root];
        while let Some(node) = pending.pop() {
            if !self.checked.insert(self.graph.key(node)) {
                continue;
            }
            pending.extend(self.graph.children(node));
            self.check_node(node)?;
        }
        Ok(())
    }

    fn check_node(&mut self, node: G::Node) -> Result<(), RouteViolation> {
        match self.graph.operator(node) {
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
                let left = self.graph.child(node, left);
                let right = self.graph.child(node, right);
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
                        let mut reason = format!(
                            "{} {side} input depends on binding routes {depends:?} but carries {carried:?}",
                            operator_name(self.graph.operator(node)),
                        );
                        if crate::debug_env::covered_input_trace() {
                            reason.push_str(&format!(
                                " (sketch {}; left names {:?}, right names {:?})",
                                sketch(self.graph.operator(node), 12),
                                self.output_names(left)?,
                                self.output_names(right)?,
                            ));
                        }
                        return Err(RouteViolation(reason));
                    }
                }
                let mut keyed = BTreeSet::new();
                for (left_key, right_key) in left_on.iter().zip(right_on) {
                    let left_key = self.key_route(left, left_key)?;
                    if left_key == self.key_route(right, right_key)? {
                        keyed.insert(left_key);
                    }
                }
                // An inner join must pair each route both sides carry. A
                // semi or anti join emits left rows only, so every route its
                // right input depends on must also key the left row.
                let required = if matches!(self.graph.operator(node), GraphBuilder::Join { .. }) {
                    left_carried
                        .intersection(&right_carried)
                        .cloned()
                        .collect::<BTreeSet<_>>()
                } else {
                    right_depends
                };
                if let Some(route) = required.difference(&keyed).next() {
                    let mut reason = format!("join does not key on binding route {route}");
                    if crate::debug_env::covered_input_trace() {
                        let keys = left_on
                            .iter()
                            .zip(right_on)
                            .map(|(l, r)| {
                                Ok((self.field_name(left, l)?, self.field_name(right, r)?))
                            })
                            .collect::<Result<Vec<_>, RouteViolation>>()?;
                        let left_equivalents = self.equivalents(left)?;
                        reason.push_str(&format!(
                            " (keys {keys:?}, left carries {left_carried:?}, right carries {right_carried:?}, left equivalents {left_equivalents:?}, sketch {}; left names {:?}, right names {:?})",
                            sketch(self.graph.operator(node), 12),
                            self.output_names(left)?,
                            self.output_names(right)?,
                        ));
                    }
                    return Err(RouteViolation(reason));
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
                let input = self.graph.child(node, input);
                if let GraphBuilder::TopBy {
                    offset: 0,
                    limit: TopByLimit::Unbounded,
                    ..
                } = self.graph.operator(node)
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
                let input = self.graph.child(node, input);
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
            | GraphBuilder::StreamingChecksum { .. }
            | GraphBuilder::TypedTemplate { .. }
            | GraphBuilder::TemplateInput { .. } => {
                // Not route-aware yet: keep them private to one binding. A
                // checksum folds every input row into one value.
                // Templates are expanded before the check, so one reaches
                // here only when it could not be.
                let depends = self.depends(node);
                if depends.is_empty() {
                    Ok(())
                } else {
                    Err(RouteViolation(format!(
                        "{} is not route-aware but depends on binding routes {depends:?}",
                        operator_name(self.graph.operator(node))
                    )))
                }
            }
            _ => Ok(()),
        }
    }
}

fn sketch(node: &GraphBuilder, depth: usize) -> String {
    let name = format!("{node:?}");
    let name = name
        .split([' ', '{', '('])
        .next()
        .unwrap_or_default()
        .to_owned();
    if depth == 0 {
        return name;
    }
    let kids = children(node)
        .map(|child| sketch(child, depth - 1))
        .collect::<Vec<_>>();
    if kids.is_empty() {
        name
    } else {
        format!("{name}[{}]", kids.join(", "))
    }
}

fn operator_name(node: &GraphBuilder) -> &'static str {
    match node {
        GraphBuilder::Recursive { .. } => "Recursive",
        GraphBuilder::RecursiveStepWitness { .. } => "RecursiveStepWitness",
        GraphBuilder::TypedTemplate { .. } => "TypedTemplate",
        GraphBuilder::TemplateInput { .. } => "TemplateInput",
        GraphBuilder::StreamingChecksum { .. } => "StreamingChecksum",
        GraphBuilder::Join { .. } => "join",
        GraphBuilder::SemiJoin { .. } => "semi-join",
        GraphBuilder::AntiJoin { .. } => "anti-join",
        _ => "operator",
    }
}

fn children(node: &GraphBuilder) -> impl ExactSizeIterator<Item = &GraphBuilder> {
    child_arcs(node).into_iter().map(AsRef::as_ref)
}

fn child_arcs(node: &GraphBuilder) -> Vec<&Arc<GraphBuilder>> {
    match node {
        GraphBuilder::TypedTemplate { inputs, .. } | GraphBuilder::Union { inputs } => {
            inputs.iter().collect()
        }
        GraphBuilder::TemplateInput { input, .. } => input.iter().collect(),
        GraphBuilder::Recursive {
            seed,
            step,
            step_witness,
            ..
        } => [Some(seed), Some(step), step_witness.as_ref()]
            .into_iter()
            .flatten()
            .collect(),
        GraphBuilder::RecursiveStepWitness { recursive } => vec![recursive],
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
        | GraphBuilder::Aggregate { input, .. }
        | GraphBuilder::Lookup { keys: input, .. } => vec![input],
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => vec![left, right],
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::InputSource { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => Vec::new(),
    }
}

/// A binding-source suffix naming `program` by its lowered graphs. Equal
/// programs get equal suffixes. A collision only makes two programs share a
/// source, which is what every program of a signature did before; delivery
/// stays correct, since the route check holds for any set of bindings.
fn program_binding_suffix(program: &QueryProgram) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    for terminal in &program.lowered.terminals {
        terminal.sink.hash(&mut hasher);
        terminal.graph.hash(&mut hasher);
    }
    program.lowered.internal_app_rows_graph.hash(&mut hasher);
    format!(":program:{:016x}", hasher.finish())
}

#[cfg(test)]
thread_local! {
    static SCOPED_BINDING_SOURCES: std::cell::RefCell<Vec<String>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The binding source names programs were scoped to since the last call.
#[cfg(test)]
pub(crate) fn take_scoped_binding_sources_for_test() -> Vec<String> {
    SCOPED_BINDING_SOURCES.with(|names| std::mem::take(&mut *names.borrow_mut()))
}

/// The names of the binding sources `program`'s graphs read.
fn program_binding_source_names(program: &QueryProgram) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut pending = program
        .lowered
        .terminals
        .iter()
        .map(|terminal| &terminal.graph)
        .chain(program.lowered.internal_app_rows_graph.as_ref())
        .collect::<Vec<_>>();
    while let Some(node) = pending.pop() {
        if !seen.insert(std::ptr::from_ref(node)) {
            continue;
        }
        if let GraphBuilder::BindingSource { shape, .. } = node {
            names.insert(shape.clone());
        }
        pending.extend(children(node));
    }
    names
}

type RewrittenNodes = HashMap<*const GraphBuilder, (Arc<GraphBuilder>, Arc<GraphBuilder>)>;

/// Renames every binding source `graph` reads by appending `suffix`.
/// The memo belongs to one program and one suffix.
pub(super) fn isolate_binding_sources(
    graph: &GraphBuilder,
    suffix: &str,
    rewritten: &mut RewrittenNodes,
) -> GraphBuilder {
    rewrite_graph(
        graph,
        &mut |node| {
            let GraphBuilder::BindingSource { shape, output } = node else {
                return None;
            };
            Some(GraphBuilder::BindingSource {
                shape: format!("{shape}{suffix}"),
                output: *output,
            })
        },
        rewritten,
    )
}

/// `graph` with every typed template replaced by the declarative operators
/// it was compiled from, and every bound template input by its graph, so
/// the route check sees each operator a template runs. A template that
/// cannot be expanded stays, and the check treats it as opaque.
fn expand_templates(graph: &GraphBuilder) -> GraphBuilder {
    expand_templates_with_memo(graph, &mut HashMap::new())
}

fn expand_templates_with_memo(
    graph: &GraphBuilder,
    rewritten: &mut RewrittenNodes,
) -> GraphBuilder {
    rewrite_graph(
        graph,
        &mut |node| match node {
            GraphBuilder::TypedTemplate {
                program,
                inputs,
                predicates,
                scalars,
            } => program
                .bind_declarative_with_arguments(inputs, predicates, scalars)
                .ok()
                .map(|expanded| expand_templates(&expanded)),
            GraphBuilder::TemplateInput {
                input: Some(input), ..
            } => Some(input.as_ref().clone()),
            _ => None,
        },
        rewritten,
    )
}

/// Applies `map` bottom-up, copying only changed nodes and their ancestors.
/// Shared inputs stay shared; unchanged inputs keep their original Arc.
fn rewrite_graph(
    graph: &GraphBuilder,
    map: &mut impl FnMut(&GraphBuilder) -> Option<GraphBuilder>,
    rewritten: &mut RewrittenNodes,
) -> GraphBuilder {
    rewrite_node(graph, map, rewritten).into_owned()
}

fn rewrite_node<'g>(
    node: &'g GraphBuilder,
    map: &mut impl FnMut(&GraphBuilder) -> Option<GraphBuilder>,
    rewritten: &mut RewrittenNodes,
) -> Cow<'g, GraphBuilder> {
    let mut mapped = None;
    let mut changed_inputs = false;
    for child in child_arcs(node) {
        let key = Arc::as_ptr(child);
        if !rewritten.contains_key(&key) {
            let done = match rewrite_node(child, map, rewritten) {
                Cow::Borrowed(_) => Arc::clone(child),
                Cow::Owned(done) => Arc::new(done),
            };
            // Other terminal roots can be replaced while this memo lives.
            // Keep the original allocation alive so its address stays valid.
            rewritten.insert(key, (Arc::clone(child), done));
        }
        changed_inputs |= !Arc::ptr_eq(child, &rewritten[&key].1);
    }
    if changed_inputs {
        let updated = mapped.get_or_insert_with(|| node.clone());
        rewrite_inputs(updated, &mut |child| {
            Arc::clone(&rewritten[&Arc::as_ptr(child)].1)
        });
    }
    if let Some(replacement) = map(mapped.as_ref().unwrap_or(node)) {
        mapped = Some(replacement);
    }
    match mapped {
        Some(mapped) => Cow::Owned(mapped),
        None => Cow::Borrowed(node),
    }
}

fn rewrite_inputs(
    node: &mut GraphBuilder,
    arc: &mut impl FnMut(&Arc<GraphBuilder>) -> Arc<GraphBuilder>,
) {
    match node {
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
        | GraphBuilder::Aggregate { input, .. }
        | GraphBuilder::Lookup { keys: input, .. } => *input = arc(input),
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
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => {}
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Names the binding source `program` reads after the program itself.
    ///
    /// Bindings of one parameter signature share a source name, but only
    /// programs with equal graphs share a prepared shape. A program whose
    /// graph inlines a session's claims would otherwise read every other
    /// session's bindings too, and a route-free policy proof pairs each
    /// admitted row with each of them. So the source is named after the
    /// program's graphs: equal programs still share one source, and a
    /// program of its own reads only its own bindings.
    ///
    /// Client-local sources are already scoped to their one session, so
    /// they keep their name.
    ///
    /// A program that fails [`check_route_discipline`] gets a source scoped
    /// to this session's claims and this binding's values instead, so its
    /// graph never serves another binding. Equal bindings of one session
    /// still share it, since they read exactly the same rows.
    pub(super) fn scope_program_binding_source(&self, program: &mut QueryProgram) {
        // A program with no source name may still read a binding source
        // (recursive and frontier policies seed from claim slots), and its
        // subscription then binds under the signature's fallback name, so it
        // is checked and scoped like any other.
        let read_sources = program_binding_source_names(program);
        let Some(source_shape) = program
            .request
            .input
            .binding
            .source_shape
            .clone()
            .or_else(|| read_sources.first().cloned())
        else {
            return;
        };
        if matches!(
            program.request.policy,
            PolicyContext::AuthorizationSubplan { .. }
        ) {
            // A subplan is embedded in, and checked with, its outer program.
            return;
        }
        // The route check proves delivery for bindings of one source. A
        // program reading several has no single binding to check against,
        // so it stays with its session.
        let discipline = if read_sources.len() > 1 {
            Err(RouteViolation(format!(
                "program reads several binding sources {read_sources:?}"
            )))
        } else {
            self.check_program_route_discipline(program)
        };
        let suffix = match discipline {
            // A client-local source already belongs to one session, and
            // its bindings attach to it by the name computed before any
            // program compiles.
            Ok(()) if program.request.authorization_mode == QueryAuthorizationMode::ClientLocal => {
                return;
            }
            Ok(()) => program_binding_suffix(program),
            Err(violation) => {
                if crate::debug_env::covered_input_trace() {
                    eprintln!(
                        "JAZZ_COVERED_INPUT_TRACE stage=isolate_binding_source source_shape={source_shape} reason={violation:?}"
                    );
                }
                self.isolated_binding_suffix(&program.request)
            }
        };
        // One suffix applies to the whole program. Preserve shared inputs
        // across its terminal roots while retaining the originals in the memo.
        let mut rewritten = HashMap::new();
        for terminal in &mut program.lowered.terminals {
            terminal.graph = isolate_binding_sources(&terminal.graph, &suffix, &mut rewritten);
        }
        if let Some(graph) = program.lowered.internal_app_rows_graph.as_mut() {
            *graph = isolate_binding_sources(graph, &suffix, &mut rewritten);
        }
        let scoped = format!("{source_shape}{suffix}");
        #[cfg(test)]
        SCOPED_BINDING_SOURCES.with(|names| names.borrow_mut().push(scoped.clone()));
        program.request.input.binding.source_shape = Some(scoped);
    }

    pub(super) fn check_program_route_discipline(
        &self,
        program: &QueryProgram,
    ) -> Result<(), RouteViolation> {
        let borrowed = self.check_borrowed_program_routes(program);
        #[cfg(any(test, feature = "testing"))]
        let audit = std::env::var_os("JAZZ_BORROWED_ROUTE_AUDIT").is_some();
        #[cfg(not(any(test, feature = "testing")))]
        let audit = false;
        if !audit && let Some(proof) = borrowed {
            return proof;
        }
        let expanded = self.check_expanded_program_routes(program);
        if audit {
            if let Some(proof) = borrowed {
                assert_eq!(
                    proof.is_ok(),
                    expanded.is_ok(),
                    "borrowed route proof disagrees with expanded proof"
                );
                eprintln!(
                    "JAZZ_BORROWED_ROUTE decided=true accepted={}",
                    proof.is_ok()
                );
            } else {
                eprintln!("JAZZ_BORROWED_ROUTE decided=false");
            }
        }
        expanded
    }

    fn check_borrowed_program_routes(
        &self,
        program: &QueryProgram,
    ) -> Option<Result<(), RouteViolation>> {
        let parameters = &program.lowered.parameters;
        let route_params = prepared_route_param_names(parameters);
        let mut view = borrowed::View::new(&self.database);
        let mut terminals = Vec::new();
        for terminal in &program.lowered.terminals {
            let eligible = terminal_route_eligible_fields(&terminal.output).ok()?;
            terminals.push((
                terminal.sink.as_str(),
                view.build(&terminal.graph).ok()?,
                terminal_route_fields(&route_params, &eligible)
                    .into_iter()
                    .collect(),
            ));
        }
        if let Some(graph) = &program.lowered.internal_app_rows_graph {
            let eligible = app_row_terminal_route_eligible_fields(&program.lowered.output).ok()?;
            terminals.push((
                "internal-app-rows",
                view.build(graph).ok()?,
                route_params
                    .iter()
                    .filter(|field| eligible.contains(*field))
                    .cloned()
                    .collect(),
            ));
        }
        let proof = check_routes(terminals, &binding_slot_routes(parameters), &view);
        // A failed proof without lookups is definitive too. Lookup-dependent
        // failures keep the existing expanded analysis path.
        (proof.is_ok() || !view.has_lookups).then_some(proof)
    }

    fn check_expanded_program_routes(&self, program: &QueryProgram) -> Result<(), RouteViolation> {
        let parameters = &program.lowered.parameters;
        let route_params = prepared_route_param_names(parameters);
        // Expansion depends only on each immutable input, not its terminal.
        let mut expanded_inputs = HashMap::new();
        let expanded = program
            .lowered
            .terminals
            .iter()
            .map(|terminal| expand_templates_with_memo(&terminal.graph, &mut expanded_inputs))
            .collect::<Vec<_>>();
        let expanded_app_rows = program
            .lowered
            .internal_app_rows_graph
            .as_ref()
            .map(|graph| expand_templates_with_memo(graph, &mut expanded_inputs));
        let mut terminals = Vec::new();
        for (terminal, graph) in program.lowered.terminals.iter().zip(&expanded) {
            let eligible = terminal_route_eligible_fields(&terminal.output)
                .map_err(|error| RouteViolation(format!("{error:?}")))?;
            terminals.push((
                terminal.sink.as_str(),
                graph,
                terminal_route_fields(&route_params, &eligible)
                    .into_iter()
                    .collect(),
            ));
        }
        if let Some(graph) = &expanded_app_rows {
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
        // The expanded graphs remain alive and immutable until the proof ends.
        // Descriptor inference can therefore share one memo across its nodes
        // and terminal roots, without retaining results across source/schema changes.
        let descriptors = std::cell::RefCell::new(self.database.graph_output_descriptors());
        check_route_discipline(terminals, &binding_slot_routes(parameters), |graph| {
            descriptors.borrow_mut().describe(graph)
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

#[cfg(test)]
mod tests {
    use super::*;
    use groove::ivm::ProjectField;
    use groove::records::ValueType;

    const ROUTE: &str = "__jazz_route_chat";

    /// Output names only: the check reads field names, never values.
    fn infer(node: &GraphBuilder) -> Result<RecordDescriptor, ()> {
        let names = match node {
            GraphBuilder::BindingSource { output, .. } => return Ok(*output),
            GraphBuilder::Lookup { .. } => {
                vec!["id".to_owned(), "chat".to_owned(), "author".to_owned()]
            }
            GraphBuilder::Join { left, right, .. } => [("left", left), ("right", right)]
                .into_iter()
                .map(|(side, input)| {
                    Ok(infer(input)?
                        .fields()
                        .iter()
                        .filter_map(|field| field.name.clone())
                        .map(|name| format!("{side}.{name}"))
                        .collect::<Vec<_>>())
                })
                .collect::<Result<Vec<_>, ()>>()?
                .concat(),
            GraphBuilder::Project { fields, .. } => fields
                .iter()
                .map(|field| field.output_name.clone())
                .collect(),
            _ => return Err(()),
        };
        Ok(RecordDescriptor::new(
            names.into_iter().map(|name| (name, ValueType::String)),
        ))
    }

    fn binding() -> GraphBuilder {
        GraphBuilder::binding_source(
            "chats",
            RecordDescriptor::new([("chat", ValueType::String)]),
        )
    }

    /// Messages of every bound chat at once: a binding-keyed lookup.
    fn lookup() -> GraphBuilder {
        let table = GraphBuilder::Table {
            table: "messages".to_owned(),
            scan: None,
            variant_projection: None,
        };
        GraphBuilder::lookup(table, binding(), ["chat"]).expect("table lookup")
    }

    fn check(graph: &GraphBuilder) -> Result<(), RouteViolation> {
        check_route_discipline(
            [("rows", graph, BTreeSet::from([ROUTE.to_owned()]))],
            &BTreeMap::from([("chat".to_owned(), ROUTE.to_owned())]),
            infer,
        )
    }

    #[test]
    fn binding_keyed_lookup_is_not_delivered_unpaired() {
        // Its rows are the union over every binding's chat and carry no
        // route, so a terminal cannot tell whose rows they are.
        let violation = check(&lookup()).expect_err("unpaired lookup must not share");
        assert!(
            violation.0.contains("binding-keyed lookup"),
            "{violation:?}"
        );
    }

    #[test]
    fn binding_keyed_lookup_is_not_shared_even_when_paired() {
        // Pairing on the lookup's own key would be sound, but a pairing on
        // any other column lets one binding's rows depend on another's
        // (a row reaches X when its author is X's chat while its chat is
        // Y's), and the check cannot yet tell the two apart.
        let routes = binding().project_fields([ProjectField::renamed("chat", ROUTE)]);
        for key in ["chat", "author"] {
            let paired = GraphBuilder::join(lookup(), routes.clone(), [key], [ROUTE])
                .project_fields([
                    ProjectField::renamed("left.id", "id"),
                    ProjectField::renamed(format!("right.{ROUTE}"), ROUTE),
                ]);
            check(&paired).expect_err("a binding-keyed lookup is not shared yet");
        }
    }
}

//! Immutable operator topology with explicit, typed, instance-owned inputs.
//!
//! This is a compiler artifact, never a storage or wire encoding. Binding does
//! not authorize a source, capture its rows, or install/retain an evaluator.

use super::GraphBuilder;
use crate::records::RecordDescriptor;
use std::{collections::HashMap, sync::Arc};
mod sources;
pub use sources::{match_template_sources, split_template_sources};
pub(crate) mod arguments;
pub use arguments::{TemplateScalarArgument, bind_template_arguments};

/// A typed, source-independent installation program. Its identity and contents
/// are process-local compiler state, never a storage or wire representation.
#[derive(Clone, Debug)]
pub struct TypedGraphTemplate {
    pub(crate) identity: u64,
    pub(crate) nodes: Vec<TemplateNode>,
    pub(crate) inputs: Vec<RecordDescriptor>,
    pub(crate) output: RecordDescriptor,
    pub(crate) root: TemplateNodeRef,
    pub(crate) ordering: Option<TemplateNodeRef>,
    pub(crate) terminal: bool,
    pub(crate) fallback: GraphBuilder,
    pub(crate) predicate_markers: Vec<super::PredicateExpr>,
}

/// One terminal's program plus the forest argument slots it consumes, in local
/// slot order. Slot numbering is structural, never value-dependent.
#[derive(Clone, Debug)]
pub(crate) struct TemplateSlice {
    pub(crate) inputs: Vec<usize>,
    pub(crate) predicates: Vec<usize>,
}

pub(crate) type CompiledForest = Arc<[(Arc<TypedGraphTemplate>, TemplateSlice)]>;

/// Bounded whole-graph compiler cache. Keys contain source contracts and
/// operator structure, not actual sources, rows or predicate argument values.
#[derive(Clone, Debug, Default)]
pub struct TypedGraphTemplateCache {
    pub(crate) entries: std::collections::VecDeque<(u64, Vec<GraphBuilder>, CompiledForest)>,
    pub(crate) compilations: u64,
    pub(crate) reuses: u64,
}

impl TypedGraphTemplateCache {
    /// Cumulative compiler work, retained across entry invalidation.
    pub fn counters(&self) -> (u64, u64) {
        (self.compilations, self.reuses)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl PartialEq for TypedGraphTemplate {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}
impl Eq for TypedGraphTemplate {}
impl TypedGraphTemplate {
    pub fn output_descriptor(&self) -> RecordDescriptor {
        self.output
    }

    /// Reconstruct the declarative equivalent for diagnostics and compiler
    /// fallbacks. Ordinary typed installation does not pay for this walk.
    pub fn bind_declarative(
        &self,
        inputs: &[Arc<GraphBuilder>],
        predicates: &[super::PredicateExpr],
    ) -> Result<GraphBuilder, TemplateBindingError> {
        self.bind_declarative_with_arguments(inputs, predicates, &[])
    }

    pub fn bind_declarative_with_arguments(
        &self,
        inputs: &[Arc<GraphBuilder>],
        predicates: &[super::PredicateExpr],
        scalars: &[TemplateScalarArgument],
    ) -> Result<GraphBuilder, TemplateBindingError> {
        if predicates.len() != self.predicate_markers.len() {
            return Err(TemplateBindingError::PredicateArity);
        }
        for (slot, (marker, argument)) in self.predicate_markers.iter().zip(predicates).enumerate()
        {
            if !marker.accepts_template_argument(argument) {
                return Err(TemplateBindingError::PredicateArgument(slot as u32));
            }
        }
        let mut rewritten = HashMap::<usize, Arc<GraphBuilder>>::new();
        for node in self.fallback.postorder() {
            let mut bound =
                node.map_inputs(|child| rewritten[&(Arc::as_ptr(child) as usize)].clone());
            if let GraphBuilder::Filter { predicate, .. } = &mut bound
                && let Some(slot) = self
                    .predicate_markers
                    .iter()
                    .position(|marker| marker == predicate)
            {
                *predicate = predicates[slot].clone();
            }
            if let GraphBuilder::Project { fields, .. } = &mut bound {
                for field in fields {
                    arguments::bind_project_expression(&mut field.expression, scalars)?;
                }
            }
            rewritten.insert(node as *const GraphBuilder as usize, Arc::new(bound));
        }
        let fallback = rewritten[&(&self.fallback as *const GraphBuilder as usize)].clone();
        let inputs = inputs
            .iter()
            .zip(&self.inputs)
            .map(|(graph, output)| {
                TemplateGraphInput::with_output_contract((**graph).clone(), *output)
            })
            .collect::<Vec<_>>();
        Ok(bind_template_graphs_inner(
            std::slice::from_ref(fallback.as_ref()),
            &inputs,
            true,
            None,
        )?
        .remove(0))
    }
}
impl std::hash::Hash for TypedGraphTemplate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TemplateNodeRef {
    Input(usize),
    Node(usize),
}

#[derive(Clone, Debug)]
pub(crate) struct TemplateNode {
    pub(crate) descriptor: super::NodeDescriptor,
    pub(crate) inputs: Vec<TemplateNodeRef>,
    pub(crate) predicate: Option<usize>,
}

/// Compile source-slot graphs once into typed operator definitions. The
/// compiler uses an empty scratch runtime: it cannot capture a caller's rows,
/// subscriptions, schema catalogue, or source capabilities.
pub fn compile_template_graphs(
    graphs: &[GraphBuilder],
) -> Result<Vec<GraphBuilder>, super::IvmRuntimeError> {
    TypedGraphTemplateCache::default().compile(graphs)
}

/// A graph with an explicit output contract. This does not prove that
/// the source is valid or remains live: installation checks source lifetime,
/// ownership and current schema. Callers cannot substitute a different graph
/// while retaining this descriptor.
#[derive(Clone, Debug)]
pub struct TemplateGraphInput {
    graph: Arc<GraphBuilder>,
    output: RecordDescriptor,
}

impl TemplateGraphInput {
    pub(crate) fn new(graph: GraphBuilder, output: RecordDescriptor) -> Self {
        Self {
            graph: Arc::new(graph),
            output,
        }
    }
    pub fn descriptor(&self) -> RecordDescriptor {
        self.output
    }

    /// Carry a source preparer's already-known contract without inferring its
    /// entire graph again. This is a declaration, not a trusted capability:
    /// installation validates the actual output against this exact descriptor.
    pub fn with_output_contract(graph: GraphBuilder, output: RecordDescriptor) -> Self {
        Self::new(graph, output)
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TemplateBindingError {
    #[error("template scalar argument {0} is missing or has a different type")]
    ScalarArgument(u32),
    #[error("template predicate argument {0} is missing or remains unbound")]
    PredicateArgument(u32),
    #[error("template predicate argument count does not match")]
    PredicateArity,
    #[error("template input {0} is missing")]
    MissingInput(u32),
    #[error("template input {0} has a different record descriptor")]
    DescriptorMismatch(u32),
}

/// Bind an entire multisink forest in one iterative pass, preserving shared
/// fragments across roots. Input contracts are checked both here and at
/// installation, so schema changes cannot invalidate an earlier description.
/// The compiler erases the check without adding an execution operator.
/// Extra supplied inputs are not consumed; already-bound slots stay private.
pub fn bind_template_graphs(
    graphs: &[GraphBuilder],
    inputs: &[TemplateGraphInput],
) -> Result<Vec<GraphBuilder>, TemplateBindingError> {
    bind_template_graphs_inner(graphs, inputs, false, None)
}

/// Bind all source and scalar arguments in one forest pass. Fresh inputs are
/// attached only after argument substitution; their private namespaces remain
/// opaque. No execution operators or persistent encodings are introduced.
pub fn bind_template_program(
    graphs: &[GraphBuilder],
    inputs: &[TemplateGraphInput],
    predicates: &[super::PredicateExpr],
    scalars: Arc<[TemplateScalarArgument]>,
) -> Result<Vec<GraphBuilder>, TemplateBindingError> {
    bind_template_graphs_inner(
        graphs,
        inputs,
        false,
        Some(arguments::TemplateArguments {
            predicates,
            scalars: &scalars,
        }),
    )
}

// Compiler-owned normalized families may contain bound contract wrappers
// above local slots. Never recurse into a supplied replacement, whose slots
// belong to its original owner.
pub(crate) fn bind_template_graphs_inner(
    graphs: &[GraphBuilder],
    inputs: &[TemplateGraphInput],
    descend_bound: bool,
    arguments: Option<arguments::TemplateArguments<'_>>,
) -> Result<Vec<GraphBuilder>, TemplateBindingError> {
    let mut rewritten = HashMap::<*const GraphBuilder, Arc<GraphBuilder>>::new();
    for root in graphs {
        let mut pending = vec![(root, false)];
        while let Some((node, expanded)) = pending.pop() {
            let key = std::ptr::from_ref(node);
            if rewritten.contains_key(&key) {
                continue;
            }
            if let GraphBuilder::TemplateInput {
                slot,
                output,
                input: bound,
            } = node
            {
                if bound.is_some() && !descend_bound {
                    rewritten.insert(key, Arc::new(node.clone()));
                    continue;
                }
                if bound.is_none() {
                    let input = inputs
                        .get(*slot as usize)
                        .ok_or(TemplateBindingError::MissingInput(*slot))?;
                    if input.output != *output {
                        return Err(TemplateBindingError::DescriptorMismatch(*slot));
                    }
                    rewritten.insert(
                        key,
                        Arc::new(GraphBuilder::TemplateInput {
                            slot: *slot,
                            output: *output,
                            input: Some(input.graph.clone()),
                        }),
                    );
                    continue;
                }
            }
            if !expanded {
                pending.push((node, true));
                node.visit_inputs(|child| pending.push((child, false)));
                continue;
            }
            let mut bound = node.map_inputs(|child| rewritten[&Arc::as_ptr(child)].clone());
            if let Some(arguments) = &arguments {
                arguments.bind_node(&mut bound)?;
            }
            rewritten.insert(key, Arc::new(bound));
        }
    }
    Ok(graphs
        .iter()
        .map(|root| (*rewritten[&std::ptr::from_ref(root)]).clone())
        .collect())
}

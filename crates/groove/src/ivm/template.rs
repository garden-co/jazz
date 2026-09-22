//! Immutable operator topology with explicit, typed, instance-owned inputs.
//!
//! This is a compiler artifact, never a storage or wire encoding. Binding does
//! not authorize a source, capture its rows, or install/retain an evaluator.

use super::GraphBuilder;
use crate::records::RecordDescriptor;
use std::{collections::HashMap, sync::Arc};

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
    ) -> Result<GraphBuilder, TemplateBindingError> {
        let inputs = inputs
            .iter()
            .zip(&self.inputs)
            .map(|(graph, output)| {
                TemplateGraphInput::with_output_contract((**graph).clone(), *output)
            })
            .collect::<Vec<_>>();
        Ok(bind_template_graphs(std::slice::from_ref(&self.fallback), &inputs)?.remove(0))
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
}

/// Compile source-slot graphs once into typed operator definitions. The
/// compiler uses an empty scratch runtime: it cannot capture a caller's rows,
/// subscriptions, schema catalogue, or source capabilities.
pub fn compile_template_graphs(
    graphs: &[GraphBuilder],
) -> Result<Vec<GraphBuilder>, super::IvmRuntimeError> {
    super::runtime::IvmRuntime::compile_template_graphs(graphs)
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
                if bound.is_some() {
                    rewritten.insert(key, Arc::new(node.clone()));
                    continue;
                }
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
            if !expanded {
                pending.push((node, true));
                node.visit_inputs(|child| pending.push((child, false)));
                continue;
            }
            let bound = node.map_inputs(|child| rewritten[&Arc::as_ptr(child)].clone());
            rewritten.insert(key, Arc::new(bound));
        }
    }
    Ok(graphs
        .iter()
        .map(|root| (*rewritten[&std::ptr::from_ref(root)]).clone())
        .collect())
}

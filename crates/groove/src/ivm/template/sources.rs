//! Separate instance-owned leaves from immutable source operator context.
//!
//! Unlike replacing a whole source with one opaque slot, this preserves the
//! projections visible to the compiler and BindingSource metadata visible to
//! Jazz's policy/collector lowering. No source data or runtime identity enters
//! the reusable blueprint.
use super::*;

struct SourceBindings {
    inputs: Vec<TemplateGraphInput>,
    rewritten: std::collections::HashMap<usize, Arc<GraphBuilder>>,
}

impl SourceBindings {
    fn new() -> Self {
        Self {
            inputs: Vec::new(),
            rewritten: Default::default(),
        }
    }

    fn blueprint<E>(
        &mut self,
        root: &GraphBuilder,
        describe: &impl Fn(GraphBuilder) -> Result<TemplateGraphInput, E>,
    ) -> Result<GraphBuilder, E> {
        let mut pending = vec![(root, false)];
        while let Some((graph, expanded)) = pending.pop() {
            let key = graph as *const GraphBuilder as usize;
            if self.rewritten.contains_key(&key) {
                continue;
            }
            let external = match graph {
                GraphBuilder::Table { .. }
                | GraphBuilder::Index { .. }
                | GraphBuilder::InputSource { .. }
                | GraphBuilder::InlineRecords { .. }
                | GraphBuilder::TypedTemplate { .. }
                | GraphBuilder::TemplateInput { input: Some(_), .. } => true,
                // The ordinary compiler proves a direct-table winner's primary
                // key and chooses its comparison semantics. Keep that proof
                // attached to the fresh leaf, not to an arbitrary input slot.
                GraphBuilder::ArgMaxBy { input, .. } | GraphBuilder::ArgMinBy { input, .. }
                    if matches!(input.as_ref(), GraphBuilder::Table { .. }) =>
                {
                    true
                }
                _ => false,
            };
            if external {
                let input = describe(graph.clone())?;
                let slot = GraphBuilder::TemplateInput {
                    slot: self.inputs.len() as u32,
                    output: input.descriptor(),
                    input: None,
                };
                self.inputs.push(input);
                self.rewritten.insert(key, Arc::new(slot));
            } else if !expanded {
                pending.push((graph, true));
                graph.visit_inputs(|child| pending.push((child, false)));
            } else {
                self.rewritten.insert(
                    key,
                    Arc::new(graph.map_inputs(|child| {
                        self.rewritten[&(Arc::as_ptr(child) as usize)].clone()
                    })),
                );
            }
        }
        Ok((*self.rewritten[&(root as *const GraphBuilder as usize)]).clone())
    }
}

/// Split an immutable source forest into static operator blueprints and fresh
/// typed leaf arguments. The pointer memo lives only during this call while
/// every borrowed source remains alive. BindingSource metadata stays visible
/// to the owning language's lowering. Input descriptors are rechecked at
/// installation; they do not authorize or keep runtime inputs alive.
pub fn split_template_sources<E>(
    graphs: &[&GraphBuilder],
    describe: impl Fn(GraphBuilder) -> Result<TemplateGraphInput, E>,
) -> Result<(Vec<GraphBuilder>, Vec<TemplateGraphInput>), E> {
    let mut bindings = SourceBindings::new();
    let graphs = graphs
        .iter()
        .map(|graph| bindings.blueprint(graph, &describe))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((graphs, bindings.inputs))
}

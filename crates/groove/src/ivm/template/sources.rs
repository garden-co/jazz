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
            if is_external_leaf(graph) {
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

/// Instance-owned leaves: everything whose identity or rows belong to one
/// admitted request rather than to the reusable operator context.
fn is_external_leaf(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::Table { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::InputSource { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::TypedTemplate { .. }
        | GraphBuilder::TemplateInput { input: Some(_), .. } => true,
        // The ordinary compiler proves a direct-table winner's primary
        // key and chooses its comparison semantics. Keep that proof
        // attached to the fresh leaf, not to an arbitrary input slot.
        GraphBuilder::ArgMaxBy { input, .. } | GraphBuilder::ArgMinBy { input, .. } => {
            matches!(input.as_ref(), GraphBuilder::Table { .. })
        }
        _ => false,
    }
}

/// Match fresh source graphs against blueprints previously produced by
/// [`split_template_sources`], without rebuilding them. Returns the fresh
/// typed inputs in blueprint slot order exactly when splitting the fresh
/// graphs would yield structurally equal blueprints: every non-leaf node
/// compares equal, each blueprint slot pairs with exactly one fresh leaf
/// (by identity, as the splitter's pointer memo does), and each fresh leaf's
/// described contract equals its slot's contract. `None` means a different
/// family; the caller then splits as usual.
pub fn match_template_sources<E>(
    graphs: &[&GraphBuilder],
    blueprints: &[&GraphBuilder],
    describe: impl Fn(GraphBuilder) -> Result<TemplateGraphInput, E>,
) -> Result<Option<Vec<TemplateGraphInput>>, E> {
    if graphs.len() != blueprints.len() {
        return Ok(None);
    }
    let mut inputs: Vec<Option<TemplateGraphInput>> = Vec::new();
    let mut leaf_slots = std::collections::HashMap::<*const GraphBuilder, u32>::new();
    let mut error = None;
    for (graph, blueprint) in graphs.iter().zip(blueprints) {
        let matched =
            crate::ivm::runtime::graph_builders_equal_with(graph, blueprint, |fresh, slot| {
                if !is_external_leaf(fresh) {
                    // A blueprint slot can only stand for an external leaf.
                    return matches!(slot, GraphBuilder::TemplateInput { input: None, .. })
                        .then_some(false);
                }
                let GraphBuilder::TemplateInput {
                    slot,
                    output,
                    input: None,
                } = slot
                else {
                    return Some(false);
                };
                let key = std::ptr::from_ref(fresh);
                if let Some(existing) = leaf_slots.get(&key) {
                    return Some(existing == slot);
                }
                let index = *slot as usize;
                if inputs.len() <= index {
                    inputs.resize_with(index + 1, || None);
                }
                if inputs[index].is_some() {
                    // Two distinct fresh leaves cannot share one blueprint slot.
                    return Some(false);
                }
                match describe(fresh.clone()) {
                    Ok(input) if input.descriptor() == *output => {
                        leaf_slots.insert(key, *slot);
                        inputs[index] = Some(input);
                        Some(true)
                    }
                    Ok(_) => Some(false),
                    Err(err) => {
                        error = Some(err);
                        Some(false)
                    }
                }
            });
        if let Some(err) = error.take() {
            return Err(err);
        }
        if !matched {
            return Ok(None);
        }
    }
    Ok(inputs.into_iter().collect())
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

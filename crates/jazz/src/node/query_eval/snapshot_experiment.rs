//! Experimental concrete binding substitution; leaves the lowered operators intact.
use super::*;
use std::sync::Arc;

pub(super) fn freeze_bindings(
    graph: &GraphBuilder,
    values: &BTreeMap<String, Value>,
) -> Result<GraphBuilder, Error> {
    fn visit(
        graph: &GraphBuilder,
        values: &BTreeMap<String, Value>,
        memo: &mut std::collections::HashMap<*const GraphBuilder, Arc<GraphBuilder>>,
    ) -> Result<Arc<GraphBuilder>, Error> {
        let key = std::ptr::from_ref(graph);
        if let Some(value) = memo.get(&key) {
            return Ok(value.clone());
        }
        let mut result = graph.clone();
        match &mut result {
            GraphBuilder::BindingSource { output, .. } => {
                let row = output
                    .fields()
                    .iter()
                    .map(|field| {
                        values
                            .get(field.name.as_deref().expect("named binding field"))
                            .cloned()
                            .ok_or(Error::InvalidStoredValue(
                                "experimental snapshot binding value missing",
                            ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                result = GraphBuilder::values(*output, [row]).map_err(|_| {
                    Error::InvalidStoredValue("experimental binding encoding failed")
                })?;
            }
            GraphBuilder::Recursive {
                seed,
                step,
                step_witness,
                ..
            } => {
                *seed = visit(seed, values, memo)?;
                *step = visit(step, values, memo)?;
                if let Some(witness) = step_witness {
                    *witness = visit(witness, values, memo)?;
                }
            }
            GraphBuilder::RecursiveStepWitness { recursive } => {
                *recursive = visit(recursive, values, memo)?;
            }
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
            | GraphBuilder::Aggregate { input, .. } => {
                *input = visit(input, values, memo)?;
            }
            GraphBuilder::Union { inputs } => {
                for input in inputs {
                    *input = visit(input, values, memo)?;
                }
            }
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => {
                *left = visit(left, values, memo)?;
                *right = visit(right, values, memo)?;
            }
            GraphBuilder::Table { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::InputSource { .. }
            | GraphBuilder::FrontierSource { .. } => {}
        }
        let result = Arc::new(result);
        memo.insert(key, result.clone());
        Ok(result)
    }
    Ok((*visit(graph, values, &mut Default::default())?).clone())
}

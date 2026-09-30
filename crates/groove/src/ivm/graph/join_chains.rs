//! Demand-driven narrowing of flattened inner-join chains.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::{FieldRef, GraphBuilder, ProjectExpr, ProjectField};
use crate::records::FieldIdentity;

const LEFT: &str = "left.";
const RIGHT: &str = "right.";

impl GraphBuilder {
    /// Drop fields nothing consumes from the projections between the inner
    /// joins below this projection.
    ///
    /// A flattened chain `Project(Join(Project(Join(a, b)), c))` renames and
    /// carries every field of every joined input so that later joins and the
    /// final projection can name any of them. When the final projection keeps
    /// only a few, each intermediate record still grows with the number of
    /// joins, and so does every compile step over it. Narrowing keeps the
    /// fields the consumer, the join keys and any filter in the chain name.
    /// Projections, filters and joins act row by row on weighted records, so
    /// dropping an unread field never changes the result.
    ///
    /// Only projections directly over a join are narrowed; other inputs, such
    /// as source graphs, stay as they are and stay shared. A positional field
    /// reference, or a name the pass cannot trace, leaves that part unchanged.
    pub fn narrow_projected_join_chain(self) -> Self {
        let GraphBuilder::Project { input, fields } = self else {
            return self;
        };
        let narrowed = demanded_names(&fields).and_then(|demand| narrow_chain(&input, &demand));
        GraphBuilder::Project {
            input: narrowed.unwrap_or(input),
            fields,
        }
    }
}

/// Rebuilds a chain node so that it still provides every demanded name.
/// Returns `None` when nothing below it changed.
fn narrow_chain(node: &GraphBuilder, demand: &BTreeSet<String>) -> Option<Arc<GraphBuilder>> {
    match node {
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            let left_demand = join_side_demand(demand, LEFT, left_on)?;
            let right_demand = join_side_demand(demand, RIGHT, right_on)?;
            let narrowed_left = narrow_join_input(left, &left_demand);
            let narrowed_right = narrow_join_input(right, &right_demand);
            if narrowed_left.is_none() && narrowed_right.is_none() {
                return None;
            }
            Some(Arc::new(GraphBuilder::Join {
                left: narrowed_left.unwrap_or_else(|| left.clone()),
                right: narrowed_right.unwrap_or_else(|| right.clone()),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            }))
        }
        // A semi-join outputs its left record unchanged and reads only the
        // left keys; its right input is an existence gate.
        GraphBuilder::SemiJoin {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            let mut left_demand = demand.clone();
            for key in left_on {
                left_demand.insert(field_ref_name(key)?);
            }
            let narrowed_left = narrow_join_input(left, &left_demand)?;
            Some(Arc::new(GraphBuilder::SemiJoin {
                left: narrowed_left,
                right: right.clone(),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            }))
        }
        GraphBuilder::UnwrapNullable { input, field } => {
            let mut input_demand = demand.clone();
            input_demand.insert(field_ref_name(field)?);
            let input = narrow_join_input(input, &input_demand)?;
            Some(Arc::new(GraphBuilder::UnwrapNullable {
                input,
                field: field.clone(),
            }))
        }
        GraphBuilder::Filter {
            input,
            predicate,
            comparison,
        } => {
            let mut input_demand = demand.clone();
            predicate.referenced_fields(&mut input_demand);
            let input = narrow_join_input(input, &input_demand)?;
            Some(Arc::new(GraphBuilder::Filter {
                input,
                predicate: predicate.clone(),
                comparison: *comparison,
            }))
        }
        _ => None,
    }
}

/// Narrows one input of a chain node. A projection over a join keeps only
/// the demanded fields; any other chain node passes the demand down.
fn narrow_join_input(
    node: &Arc<GraphBuilder>,
    demand: &BTreeSet<String>,
) -> Option<Arc<GraphBuilder>> {
    let GraphBuilder::Project { input, fields } = node.as_ref() else {
        return narrow_chain(node, demand);
    };
    if !matches!(
        input.as_ref(),
        GraphBuilder::Join { .. } | GraphBuilder::SemiJoin { .. }
    ) {
        return None;
    }
    let kept = fields
        .iter()
        .filter(|field| demand.iter().any(|name| provides(field, name)))
        .cloned()
        .collect::<Vec<_>>();
    // The consumer resolved every demanded name against this projection. If
    // one no longer resolves, it named a field in a way this pass does not
    // model; keep the projection whole rather than guess.
    if !demand
        .iter()
        .all(|name| kept.iter().any(|field| provides(field, name)))
    {
        return None;
    }
    let narrowed_input = demanded_names(&kept).and_then(|demand| narrow_chain(input, &demand));
    if kept.len() == fields.len() && narrowed_input.is_none() {
        return None;
    }
    Some(Arc::new(GraphBuilder::Project {
        input: narrowed_input.unwrap_or_else(|| input.clone()),
        fields: kept,
    }))
}

/// The input fields a join side must provide: the demanded outputs of that
/// side, without their side prefix, plus its keys.
fn join_side_demand(
    demand: &BTreeSet<String>,
    prefix: &str,
    keys: &[FieldRef],
) -> Option<BTreeSet<String>> {
    let mut side = demand
        .iter()
        .filter_map(|name| name.strip_prefix(prefix).map(str::to_owned))
        .collect::<BTreeSet<_>>();
    for key in keys {
        side.insert(field_ref_name(key)?);
    }
    Some(side)
}

/// The input names a projection reads, or `None` if one is positional.
fn demanded_names(fields: &[ProjectField]) -> Option<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for field in fields {
        if let Some(source) = project_expr_source(&field.expression) {
            names.insert(field_ref_name(source)?);
        }
    }
    Some(names)
}

fn field_ref_name(field: &FieldRef) -> Option<String> {
    match field {
        FieldRef::Name(name) | FieldRef::StoredName(name) => Some(name.clone()),
        FieldRef::Resolved(_) => None,
    }
}

/// Whether a consumer naming `name` can resolve to this output field. Names
/// resolve by logical identity first and by stored name second, so a field
/// matching either way is kept.
fn provides(field: &ProjectField, name: &str) -> bool {
    let logical = match &field.output_identity {
        FieldIdentity::Name(logical) | FieldIdentity::NamedSlot { name: logical, .. } => {
            logical.as_str()
        }
        FieldIdentity::Slot(_) => field.output_name.as_str(),
    };
    field.output_name == name || logical == name
}

fn project_expr_source(expression: &ProjectExpr) -> Option<&FieldRef> {
    match expression {
        ProjectExpr::Field(field)
        | ProjectExpr::Nullable(field)
        | ProjectExpr::NullableFlat(field)
        | ProjectExpr::RecordField { source: field, .. }
        | ProjectExpr::EnumTagRemap { source: field, .. }
        | ProjectExpr::EnumRemap { source: field, .. }
        | ProjectExpr::RecursiveEnumRemap { source: field, .. } => Some(field),
        ProjectExpr::Literal(_)
        | ProjectExpr::TypedLiteral { .. }
        | ProjectExpr::Null(_)
        | ProjectExpr::TemplateArgument { .. } => None,
    }
}

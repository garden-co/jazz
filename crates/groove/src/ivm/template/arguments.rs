//! Typed bind-time arguments. Values never enter the reusable program.
use super::*;
use crate::ivm::{LiteralValue, PredicateExpr, ProjectExpr};
use crate::records::ValueType;

/// A checked scalar value. Private fields prevent a caller changing a value
/// after validation while retaining its type proof. This is not authority.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TemplateScalarArgument {
    value: LiteralValue,
    value_type: ValueType,
}

impl TemplateScalarArgument {
    pub fn new(
        value: crate::records::Value,
        value_type: ValueType,
    ) -> Result<Self, crate::records::Error> {
        crate::records::encode_single_field_value(&value, &value_type)?;
        Ok(Self {
            value: value.into(),
            value_type,
        })
    }
}

pub(crate) fn bind_project_expression(
    expression: &mut ProjectExpr,
    arguments: &[TemplateScalarArgument],
) -> Result<(), TemplateBindingError> {
    if let ProjectExpr::TemplateArgument { slot, value_type } = expression {
        let argument = arguments
            .get(*slot as usize)
            .filter(|argument| argument.value_type == *value_type)
            .ok_or(TemplateBindingError::ScalarArgument(*slot))?;
        *expression = ProjectExpr::TypedLiteral {
            value: argument.value.clone(),
            value_type: value_type.clone(),
        };
    }
    Ok(())
}

fn bind_predicate(
    predicate: &mut PredicateExpr,
    arguments: &[PredicateExpr],
) -> Result<(), TemplateBindingError> {
    if let PredicateExpr::TemplateArgument { slot, .. } = predicate {
        let slot = *slot;
        let argument = arguments
            .get(slot as usize)
            .filter(|argument| predicate.accepts_template_argument(argument))
            .ok_or(TemplateBindingError::PredicateArgument(slot))?;
        *predicate = argument.clone();
        return Ok(());
    }
    match predicate {
        PredicateExpr::And(children) | PredicateExpr::Or(children) => {
            for child in children {
                bind_predicate(child, arguments)?;
            }
        }
        PredicateExpr::EnumMatch { payload, .. } => bind_predicate(payload, arguments)?,
        _ => {}
    }
    Ok(())
}

pub(crate) struct TemplateArguments<'a> {
    pub predicates: &'a [PredicateExpr],
    pub scalars: &'a Arc<[TemplateScalarArgument]>,
}

impl TemplateArguments<'_> {
    pub(crate) fn bind_node(&self, node: &mut GraphBuilder) -> Result<(), TemplateBindingError> {
        match node {
            GraphBuilder::Filter { predicate, .. } => bind_predicate(predicate, self.predicates)?,
            GraphBuilder::Project { fields, .. } => {
                for field in fields {
                    bind_project_expression(&mut field.expression, self.scalars)?;
                }
            }
            GraphBuilder::TypedTemplate {
                predicates,
                scalars,
                ..
            } => {
                for predicate in predicates {
                    bind_predicate(predicate, self.predicates)?;
                }
                *scalars = self.scalars.clone();
            }
            _ => {}
        }
        Ok(())
    }
}

/// Bind predicates and typed projected constants across one program forest.
/// Bind before attaching fresh source inputs: already-bound inputs belong to
/// another program and are deliberately opaque to this argument namespace.
pub fn bind_template_arguments(
    graphs: &[GraphBuilder],
    predicates: &[PredicateExpr],
    scalars: Arc<[TemplateScalarArgument]>,
) -> Result<Vec<GraphBuilder>, TemplateBindingError> {
    let mut rewritten = HashMap::<usize, Arc<GraphBuilder>>::new();
    for root in graphs {
        let mut pending = vec![(root, false)];
        while let Some((node, expanded)) = pending.pop() {
            let key = node as *const GraphBuilder as usize;
            if rewritten.contains_key(&key) {
                continue;
            }
            if matches!(node, GraphBuilder::TemplateInput { input: Some(_), .. }) {
                rewritten.insert(key, Arc::new(node.clone()));
                continue;
            }
            if !expanded {
                pending.push((node, true));
                node.visit_inputs(|child| pending.push((child, false)));
                continue;
            }
            let mut bound =
                node.map_inputs(|child| rewritten[&(Arc::as_ptr(child) as usize)].clone());
            TemplateArguments {
                predicates,
                scalars: &scalars,
            }
            .bind_node(&mut bound)?;
            rewritten.insert(key, Arc::new(bound));
        }
    }
    Ok(graphs
        .iter()
        .map(|root| (*rewritten[&(root as *const GraphBuilder as usize)]).clone())
        .collect())
}

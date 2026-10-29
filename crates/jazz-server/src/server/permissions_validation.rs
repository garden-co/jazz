//! Checks on relation policies that compiling a schema does not make.

use std::collections::{BTreeSet, HashMap};

use jazz::tools::public_schema::{
    PolicyExpr, RelationExpr, RelationPredicate, RelationValueRef, TableName, TablePolicies, Value,
};

const MAX_RELATION_UNION_ARM_LABEL_BYTES: usize = 4096;

/// Reject relation policies the runtime cannot evaluate: unsupported
/// operators, query parameters, untyped literals and malformed union labels.
pub(crate) fn validate_permissions_relations(
    permissions: &HashMap<TableName, TablePolicies>,
) -> Result<(), String> {
    for policies in permissions.values() {
        for policy in [
            policies.select.using.as_ref(),
            policies.insert.with_check.as_ref(),
            policies.update.using.as_ref(),
            policies.update.with_check.as_ref(),
            policies.delete.using.as_ref(),
            policies.delete.with_check.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_policy_relations(policy)?;
        }
    }
    Ok(())
}

fn validate_policy_relations(policy: &PolicyExpr) -> Result<(), String> {
    match policy {
        PolicyExpr::ExistsRel { rel } => validate_relation_policy(rel),
        PolicyExpr::Exists { condition, .. } | PolicyExpr::Not(condition) => {
            validate_policy_relations(condition)
        }
        PolicyExpr::And(expressions) | PolicyExpr::Or(expressions) => {
            for expression in expressions {
                validate_policy_relations(expression)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_relation_policy(rel: &RelationExpr) -> Result<(), String> {
    match rel {
        RelationExpr::TableScan { .. } => Ok(()),
        RelationExpr::Filter { input, predicate } => {
            validate_relation_policy_predicate(predicate)?;
            validate_relation_policy(input)
        }
        RelationExpr::Project { input, .. } => validate_relation_policy(input),
        RelationExpr::Distinct { .. }
        | RelationExpr::OrderBy { .. }
        | RelationExpr::Offset { .. }
        | RelationExpr::Limit { .. } => {
            Err("relation policies do not support Distinct, OrderBy, Offset or Limit".to_owned())
        }
        RelationExpr::Union { inputs } => {
            let mut labels = BTreeSet::new();
            for arm in inputs {
                if arm.label.is_empty()
                    || arm.label.len() > MAX_RELATION_UNION_ARM_LABEL_BYTES
                    || arm.label.contains('\0')
                    || !labels.insert(&arm.label)
                {
                    return Err(
                        "relation union labels must be unique, NUL-free, and 1..=4096 bytes"
                            .to_owned(),
                    );
                }
                validate_relation_policy(&arm.input)?;
            }
            Ok(())
        }
        RelationExpr::Join { left, right, .. } => {
            validate_relation_policy(left)?;
            validate_relation_policy(right)
        }
        RelationExpr::Gather { seed, step, .. } => {
            validate_relation_policy(seed)?;
            validate_relation_policy(step)
        }
    }
}

fn validate_relation_policy_predicate(predicate: &RelationPredicate) -> Result<(), String> {
    match predicate {
        RelationPredicate::Cmp { right, .. } | RelationPredicate::Contains { right, .. } => {
            validate_relation_policy_value(right)
        }
        RelationPredicate::In { values, .. } => {
            values.iter().try_for_each(validate_relation_policy_value)
        }
        RelationPredicate::EnumMatch { payload, .. } | RelationPredicate::Not(payload) => {
            validate_relation_policy_predicate(payload)
        }
        RelationPredicate::And(predicates) | RelationPredicate::Or(predicates) => predicates
            .iter()
            .try_for_each(validate_relation_policy_predicate),
        RelationPredicate::IsNull { .. }
        | RelationPredicate::IsNotNull { .. }
        | RelationPredicate::True
        | RelationPredicate::False => Ok(()),
    }
}

fn validate_relation_policy_value(value: &RelationValueRef) -> Result<(), String> {
    let message = match value {
        RelationValueRef::Literal(literal) => match relation_policy_literal(literal) {
            Ok(_) => return Ok(()),
            Err(error) => format!("relation policy literals must be typed values: {error}"),
        },
        RelationValueRef::Param(_) => {
            "relation policies do not support query parameters".to_owned()
        }
        RelationValueRef::FrontierColumn(_) => {
            "relation policies do not support frontier column operands".to_owned()
        }
        RelationValueRef::SessionRef(_)
        | RelationValueRef::OuterColumn(_)
        | RelationValueRef::RowId(_) => return Ok(()),
    };
    Err(message)
}

fn relation_policy_literal(literal: &serde_json::Value) -> Result<Value, serde_json::Error> {
    serde_json::from_value(literal.clone())
}

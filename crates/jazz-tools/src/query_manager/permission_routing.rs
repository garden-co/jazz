use crate::object::{BranchName, ObjectId};
use crate::query_manager::graph_nodes::policy_eval::PolicyContextEvaluator;
use crate::query_manager::manager::QueryManager;
use crate::query_manager::policy::{Operation, PolicyExpr, PolicyValue};
use crate::query_manager::relation_ir::{PredicateExpr, RelExpr, ValueRef};
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    BranchPolicies, ComposedBranchName, Row, RowDescriptor, RowPolicyMode, Schema, TableName,
    TablePolicies, Value,
};
use crate::row_format::decode_row;
use crate::storage::Storage;
use std::collections::HashSet;

#[derive(Debug)]
pub struct ResolvedBranchRow<'a> {
    pub table_name: &'a TableName,
    pub row_id: ObjectId,
    pub descriptor: &'a RowDescriptor,
    pub content: Vec<u8>,
}

impl ResolvedBranchRow<'_> {
    pub fn column_value(&self, column: &str) -> Option<Value> {
        let index = self
            .descriptor
            .columns
            .iter()
            .position(|descriptor| descriptor.name.as_str() == column)?;
        decode_row(self.descriptor, &self.content)
            .ok()?
            .get(index)
            .cloned()
    }
}

pub type BranchPolicyContext<'a> = ResolvedBranchRow<'a>;

pub enum PermissionRoute<'a> {
    Normal,
    Branch {
        policy: &'a TablePolicies,
        context: ResolvedBranchRow<'a>,
    },
    NoBranchPolicy,
    Deny,
}

pub(crate) struct RoutedWritePolicies {
    pub using: Option<PolicyExpr>,
    pub with_check: Option<PolicyExpr>,
}

pub(crate) fn branch_write_policies_from_route(
    route: PermissionRoute<'_>,
    operation: Operation,
) -> Result<Option<RoutedWritePolicies>, ()> {
    match route {
        PermissionRoute::Normal => Ok(None),
        PermissionRoute::NoBranchPolicy | PermissionRoute::Deny => Err(()),
        PermissionRoute::Branch { policy, context } => {
            let policies = match operation {
                Operation::Insert => RoutedWritePolicies {
                    using: None,
                    with_check: policy
                        .insert_policy()
                        .map(|expr| bind_branch_refs(expr, &context)),
                },
                Operation::Update => RoutedWritePolicies {
                    using: policy
                        .update_using_policy()
                        .map(|expr| bind_branch_refs(expr, &context)),
                    with_check: policy
                        .update_check_policy()
                        .map(|expr| bind_branch_refs(expr, &context)),
                },
                Operation::Delete => RoutedWritePolicies {
                    using: policy
                        .effective_delete_using()
                        .map(|expr| bind_branch_refs(expr, &context)),
                    with_check: None,
                },
                Operation::Select => return Ok(None),
            };

            if policies.using.is_none() && policies.with_check.is_none() {
                Err(())
            } else {
                Ok(Some(policies))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PolicyEvalRefs<'a> {
    pub row_id: Option<ObjectId>,
    pub branch_context: Option<&'a BranchPolicyContext<'a>>,
}

impl<'a> PolicyEvalRefs<'a> {
    pub fn with_branch_context(mut self, ctx: &'a BranchPolicyContext<'a>) -> Self {
        self.branch_context = Some(ctx);
        self
    }
}

pub fn bind_branch_refs(expr: &PolicyExpr, ctx: &ResolvedBranchRow<'_>) -> PolicyExpr {
    fn bind_policy_value(value: &PolicyValue, ctx: &ResolvedBranchRow<'_>) -> PolicyValue {
        match value {
            PolicyValue::BranchRef(column) => {
                PolicyValue::Literal(ctx.column_value(column).unwrap_or(Value::Null))
            }
            PolicyValue::Literal(value) => PolicyValue::Literal(value.clone()),
            PolicyValue::SessionRef(path) => PolicyValue::SessionRef(path.clone()),
        }
    }

    match expr {
        PolicyExpr::Cmp { column, op, value } => PolicyExpr::Cmp {
            column: column.clone(),
            op: op.clone(),
            value: bind_policy_value(value, ctx),
        },
        PolicyExpr::SessionCmp { path, op, value } => PolicyExpr::SessionCmp {
            path: path.clone(),
            op: op.clone(),
            value: value.clone(),
        },
        PolicyExpr::IsNull { column } => PolicyExpr::IsNull {
            column: column.clone(),
        },
        PolicyExpr::SessionIsNull { path } => PolicyExpr::SessionIsNull { path: path.clone() },
        PolicyExpr::IsNotNull { column } => PolicyExpr::IsNotNull {
            column: column.clone(),
        },
        PolicyExpr::SessionIsNotNull { path } => {
            PolicyExpr::SessionIsNotNull { path: path.clone() }
        }
        PolicyExpr::Contains { column, value } => PolicyExpr::Contains {
            column: column.clone(),
            value: bind_policy_value(value, ctx),
        },
        PolicyExpr::SessionContains { path, value } => PolicyExpr::SessionContains {
            path: path.clone(),
            value: value.clone(),
        },
        PolicyExpr::In {
            column,
            session_path,
        } => PolicyExpr::In {
            column: column.clone(),
            session_path: session_path.clone(),
        },
        PolicyExpr::InList { column, values } => PolicyExpr::InList {
            column: column.clone(),
            values: values
                .iter()
                .map(|value| bind_policy_value(value, ctx))
                .collect(),
        },
        PolicyExpr::SessionInList { path, values } => PolicyExpr::SessionInList {
            path: path.clone(),
            values: values.clone(),
        },
        PolicyExpr::Exists { table, condition } => PolicyExpr::Exists {
            table: table.clone(),
            condition: Box::new(bind_branch_refs(condition, ctx)),
        },
        PolicyExpr::ExistsRel { rel } => PolicyExpr::ExistsRel {
            rel: bind_relation_branch_refs(rel, ctx),
        },
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => PolicyExpr::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => PolicyExpr::InheritsReferencing {
            operation: *operation,
            source_table: source_table.clone(),
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        PolicyExpr::And(exprs) => PolicyExpr::And(
            exprs
                .iter()
                .map(|expr| bind_branch_refs(expr, ctx))
                .collect(),
        ),
        PolicyExpr::Or(exprs) => PolicyExpr::Or(
            exprs
                .iter()
                .map(|expr| bind_branch_refs(expr, ctx))
                .collect(),
        ),
        PolicyExpr::Not(expr) => PolicyExpr::Not(Box::new(bind_branch_refs(expr, ctx))),
        PolicyExpr::True => PolicyExpr::True,
        PolicyExpr::False => PolicyExpr::False,
    }
}

pub fn bind_relation_branch_refs(rel: &RelExpr, ctx: &ResolvedBranchRow<'_>) -> RelExpr {
    fn bind_value_ref(value_ref: &ValueRef, ctx: &ResolvedBranchRow<'_>) -> ValueRef {
        match value_ref {
            ValueRef::BranchRef(column) => {
                ValueRef::Literal(ctx.column_value(column).unwrap_or(Value::Null))
            }
            ValueRef::Literal(value) => ValueRef::Literal(value.clone()),
            ValueRef::SessionRef(path) => ValueRef::SessionRef(path.clone()),
            ValueRef::OuterColumn(column) => ValueRef::OuterColumn(column.clone()),
            ValueRef::FrontierColumn(column) => ValueRef::FrontierColumn(column.clone()),
            ValueRef::RowId(row_id) => ValueRef::RowId(*row_id),
        }
    }

    fn bind_predicate(predicate: &PredicateExpr, ctx: &ResolvedBranchRow<'_>) -> PredicateExpr {
        match predicate {
            PredicateExpr::Cmp { left, op, right } => PredicateExpr::Cmp {
                left: left.clone(),
                op: *op,
                right: bind_value_ref(right, ctx),
            },
            PredicateExpr::Contains { left, right } => PredicateExpr::Contains {
                left: left.clone(),
                right: bind_value_ref(right, ctx),
            },
            PredicateExpr::IsNull { column } => PredicateExpr::IsNull {
                column: column.clone(),
            },
            PredicateExpr::IsNotNull { column } => PredicateExpr::IsNotNull {
                column: column.clone(),
            },
            PredicateExpr::In { left, values } => PredicateExpr::In {
                left: left.clone(),
                values: values
                    .iter()
                    .map(|value| bind_value_ref(value, ctx))
                    .collect(),
            },
            PredicateExpr::And(predicates) => PredicateExpr::And(
                predicates
                    .iter()
                    .map(|predicate| bind_predicate(predicate, ctx))
                    .collect(),
            ),
            PredicateExpr::Or(predicates) => PredicateExpr::Or(
                predicates
                    .iter()
                    .map(|predicate| bind_predicate(predicate, ctx))
                    .collect(),
            ),
            PredicateExpr::Not(predicate) => {
                PredicateExpr::Not(Box::new(bind_predicate(predicate, ctx)))
            }
            PredicateExpr::True => PredicateExpr::True,
            PredicateExpr::False => PredicateExpr::False,
        }
    }

    match rel {
        RelExpr::TableScan { table } => RelExpr::TableScan { table: *table },
        RelExpr::Union { inputs } => RelExpr::Union {
            inputs: inputs
                .iter()
                .map(|input| bind_relation_branch_refs(input, ctx))
                .collect(),
        },
        RelExpr::Filter { input, predicate } => RelExpr::Filter {
            input: Box::new(bind_relation_branch_refs(input, ctx)),
            predicate: bind_predicate(predicate, ctx),
        },
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => RelExpr::Join {
            left: Box::new(bind_relation_branch_refs(left, ctx)),
            right: Box::new(bind_relation_branch_refs(right, ctx)),
            on: on.clone(),
            join_kind: *join_kind,
        },
        RelExpr::Project { input, columns } => RelExpr::Project {
            input: Box::new(bind_relation_branch_refs(input, ctx)),
            columns: columns.clone(),
        },
        RelExpr::Gather {
            seed,
            step,
            frontier_key,
            max_depth,
            dedupe_key,
        } => RelExpr::Gather {
            seed: Box::new(bind_relation_branch_refs(seed, ctx)),
            step: Box::new(bind_relation_branch_refs(step, ctx)),
            frontier_key: frontier_key.clone(),
            max_depth: *max_depth,
            dedupe_key: dedupe_key.clone(),
        },
        RelExpr::Distinct { input, key } => RelExpr::Distinct {
            input: Box::new(bind_relation_branch_refs(input, ctx)),
            key: key.clone(),
        },
        RelExpr::OrderBy { input, terms } => RelExpr::OrderBy {
            input: Box::new(bind_relation_branch_refs(input, ctx)),
            terms: terms.clone(),
        },
        RelExpr::Offset { input, offset } => RelExpr::Offset {
            input: Box::new(bind_relation_branch_refs(input, ctx)),
            offset: *offset,
        },
        RelExpr::Limit { input, limit } => RelExpr::Limit {
            input: Box::new(bind_relation_branch_refs(input, ctx)),
            limit: *limit,
        },
    }
}

impl QueryManager {
    pub fn resolve_branch_route<'a>(
        &'a self,
        target_table: &TableName,
        branch: &str,
        op: Operation,
        storage: &'a dyn Storage,
        session: Option<&'a Session>,
    ) -> PermissionRoute<'a> {
        resolve_branch_route_with_policies(
            self.authorization_schema
                .as_deref()
                .unwrap_or(self.schema.as_ref()),
            &self.authorization_branch_policies,
            target_table,
            branch,
            op,
            storage,
            session,
        )
    }
}

pub(crate) fn resolve_branch_route_with_policies<'a>(
    schema: &'a Schema,
    branch_policies: &'a BranchPolicies,
    target_table: &TableName,
    branch: &str,
    op: Operation,
    storage: &dyn Storage,
    session: Option<&'a Session>,
) -> PermissionRoute<'a> {
    if branch == "main" {
        return PermissionRoute::Normal;
    }

    let Some(composed) = ComposedBranchName::parse(&BranchName::new(branch)) else {
        return PermissionRoute::Deny;
    };

    if composed.user_branch == "main" {
        return PermissionRoute::Normal;
    }

    let Ok(row_uuid) = uuid::Uuid::parse_str(&composed.user_branch) else {
        return PermissionRoute::Deny;
    };
    let row_id = ObjectId::from_uuid(row_uuid);
    let Some(session) = session else {
        return PermissionRoute::Deny;
    };
    let main_branch = ComposedBranchName::new(&composed.env, composed.schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    for (backing_table, policies_by_target) in branch_policies {
        let Some(target_policy) = policies_by_target.get(target_table) else {
            continue;
        };
        let Some(backing_schema) = schema.get(backing_table) else {
            continue;
        };
        let Ok(Some(backing_row)) =
            storage.load_visible_region_row(backing_table.as_str(), &main_branch, row_id)
        else {
            continue;
        };
        if backing_row.is_hard_deleted() {
            continue;
        }

        let Some(backing_read_policy) = backing_schema.policies.select_policy() else {
            return PermissionRoute::Deny;
        };
        let row = Row::new(
            row_id,
            backing_row.data.to_vec(),
            backing_row.batch_id(),
            backing_row.row_provenance(),
        );
        let mut evaluator =
            PolicyContextEvaluator::new(schema, session, &main_branch, RowPolicyMode::Enforcing);
        let mut visited_referencing = HashSet::new();
        let mut row_loader = |related_id: ObjectId, table_hint: Option<TableName>| {
            let table = table_hint?;
            let row = storage
                .load_visible_region_row(table.as_str(), &main_branch, related_id)
                .ok()??;
            Some(crate::query_manager::types::LoadedRow::new(
                row.data.clone(),
                row.row_provenance(),
                [(related_id, BranchName::new(&main_branch))]
                    .into_iter()
                    .collect(),
                row.batch_id(),
            ))
        };

        if !evaluator.evaluate_row_access(
            Operation::Select,
            &row,
            &backing_schema.columns,
            backing_table.as_str(),
            Some(backing_read_policy),
            storage,
            &mut row_loader,
            0,
            &mut visited_referencing,
        ) {
            return PermissionRoute::Deny;
        }

        if policy_for_operation(target_policy, op).is_none() {
            return PermissionRoute::NoBranchPolicy;
        }

        return PermissionRoute::Branch {
            policy: target_policy,
            context: ResolvedBranchRow {
                table_name: backing_table,
                row_id,
                descriptor: &backing_schema.columns,
                content: row.data.to_vec(),
            },
        };
    }

    PermissionRoute::Deny
}

pub(crate) fn policy_for_operation(
    policies: &TablePolicies,
    operation: Operation,
) -> Option<&PolicyExpr> {
    match operation {
        Operation::Select => policies.select_policy(),
        Operation::Insert => policies.insert_policy(),
        Operation::Update => policies.update_using_policy(),
        Operation::Delete => policies.effective_delete_using(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::policy::CmpOp;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};
    use crate::row_format::encode_row;

    #[test]
    fn bind_branch_refs_rewrites_branch_ref_operands() {
        // This helper is tested directly because branch-reference binding is not
        // user-observable until route enforcement is wired in the next tasks.
        let table_name = TableName::new("projects");
        let row_id = ObjectId::new();
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("projectId", ColumnType::Text),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);
        let content = encode_row(
            &descriptor,
            &[
                Value::Text("p1".to_string()),
                Value::Text("Project".to_string()),
            ],
        )
        .expect("encode backing row");
        let backing_row = ResolvedBranchRow {
            table_name: &table_name,
            row_id,
            descriptor: &descriptor,
            content,
        };
        let expr = PolicyExpr::And(vec![
            PolicyExpr::Cmp {
                column: "projectId".to_string(),
                op: CmpOp::Eq,
                value: PolicyValue::BranchRef("projectId".to_string()),
            },
            PolicyExpr::Cmp {
                column: "status".to_string(),
                op: CmpOp::Eq,
                value: PolicyValue::Literal(Value::Text("active".to_string())),
            },
        ]);

        let bound = bind_branch_refs(&expr, &backing_row);

        assert_eq!(
            bound,
            PolicyExpr::And(vec![
                PolicyExpr::Cmp {
                    column: "projectId".to_string(),
                    op: CmpOp::Eq,
                    value: PolicyValue::Literal(Value::Text("p1".to_string())),
                },
                PolicyExpr::Cmp {
                    column: "status".to_string(),
                    op: CmpOp::Eq,
                    value: PolicyValue::Literal(Value::Text("active".to_string())),
                },
            ])
        );
    }
}

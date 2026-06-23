use crate::object::{BranchName, ObjectId};
use crate::query_manager::manager::QueryManager;
use crate::query_manager::policy::{Operation, PolicyExpr, PolicyValue};
use crate::query_manager::relation_ir::{PredicateExpr, RelExpr, ValueRef};
use crate::query_manager::types::{
    ComposedBranchName, RowDescriptor, TableName, TablePolicies, Value,
};
use crate::row_format::decode_row;
use crate::storage::Storage;

#[derive(Debug)]
pub struct ResolvedBranchRow<'a> {
    pub table_name: &'a TableName,
    pub row_id: ObjectId,
    pub descriptor: &'a RowDescriptor,
    pub content: &'a [u8],
}

impl ResolvedBranchRow<'_> {
    pub fn column_value(&self, column: &str) -> Option<Value> {
        let index = self
            .descriptor
            .columns
            .iter()
            .position(|descriptor| descriptor.name.as_str() == column)?;
        decode_row(self.descriptor, self.content)
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
        storage: &'a impl Storage,
    ) -> PermissionRoute<'a> {
        if branch == "main" || branch == self.current_branch() {
            return PermissionRoute::Normal;
        }

        let Some(composed) = ComposedBranchName::parse(&BranchName::new(branch)) else {
            return PermissionRoute::Deny;
        };

        if composed.user_branch == "main" {
            return PermissionRoute::Normal;
        }

        let Ok(_row_uuid) = uuid::Uuid::parse_str(&composed.user_branch) else {
            return PermissionRoute::Deny;
        };

        let _ = (target_table, op, storage);
        // TODO(task5): QueryManager currently does not retain the decoded
        // CurrentPermissionsSummary.branch_policies map. Once the call sites pass
        // or expose that summary, resolve the backing row id from user_branch,
        // load the backing row, gate it on normal readability, and return
        // PermissionRoute::Branch or NoBranchPolicy.
        PermissionRoute::Deny
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
            content: &content,
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

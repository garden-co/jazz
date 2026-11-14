//! The structured policy forms of `jazz.permissions.json` (SPEC §10.10):
//! the engine's policy expressions and relation IR, field for field, as
//! internally tagged JSON. They express every policy the engine has, so
//! the file can say whatever the TS DSL can; the printer uses them wherever
//! an expression string can't reproduce the policy exactly.

use serde::{Deserialize, Serialize};

use crate::query::{
    OrderDirection, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationOrderBy, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationUnionArm,
    RelationValueRef,
};
use crate::tools::public_schema::{CmpOp, Operation, PolicyExpr, PolicyValue, Value};

/// [`PolicyExpr`], with relation expressions in this file's form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactPolicyExpr {
    Cmp {
        column: String,
        op: CmpOp,
        value: ArtifactPolicyValue,
    },
    SessionCmp {
        path: Vec<String>,
        op: CmpOp,
        value: Value,
    },
    IsNull {
        column: String,
    },
    SessionIsNull {
        path: Vec<String>,
    },
    IsNotNull {
        column: String,
    },
    SessionIsNotNull {
        path: Vec<String>,
    },
    Contains {
        column: String,
        value: ArtifactPolicyValue,
    },
    SessionContains {
        path: Vec<String>,
        value: Value,
    },
    In {
        column: String,
        session_path: Vec<String>,
    },
    InList {
        column: String,
        values: Vec<ArtifactPolicyValue>,
    },
    SessionInList {
        path: Vec<String>,
        values: Vec<Value>,
    },
    Exists {
        table: String,
        condition: Box<ArtifactPolicyExpr>,
    },
    ExistsRel {
        rel: ArtifactRelationExpr,
    },
    Inherits {
        operation: Operation,
        via_column: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
    },
    InheritsReferencing {
        operation: Operation,
        source_table: String,
        via_column: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
    },
    And {
        exprs: Vec<ArtifactPolicyExpr>,
    },
    Or {
        exprs: Vec<ArtifactPolicyExpr>,
    },
    Not {
        expr: Box<ArtifactPolicyExpr>,
    },
    True {},
    False {},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactPolicyValue {
    Literal { value: Value },
    SessionRef { path: Vec<String> },
}

/// [`RelationExpr`], internally tagged. (The engine's own form is
/// externally tagged because it is also postcard-encoded on the query wire,
/// which internal tags don't support.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactRelationExpr {
    TableScan {
        table: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
    },
    Filter {
        input: Box<ArtifactRelationExpr>,
        predicate: ArtifactPredicate,
    },
    Union {
        inputs: Vec<ArtifactUnionArm>,
    },
    Join {
        left: Box<ArtifactRelationExpr>,
        right: Box<ArtifactRelationExpr>,
        on: Vec<ArtifactJoinCondition>,
        join_kind: ArtifactJoinKind,
    },
    Project {
        input: Box<ArtifactRelationExpr>,
        columns: Vec<ArtifactProjectColumn>,
    },
    Gather {
        seed: Box<ArtifactRelationExpr>,
        step: Box<ArtifactRelationExpr>,
        frontier_key: ArtifactKeyRef,
        /// Maximum recursion depth; absent means until a fixpoint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
        dedupe_key: Vec<ArtifactKeyRef>,
    },
    Distinct {
        input: Box<ArtifactRelationExpr>,
        key: Vec<ArtifactKeyRef>,
    },
    OrderBy {
        input: Box<ArtifactRelationExpr>,
        terms: Vec<ArtifactOrderBy>,
    },
    Offset {
        input: Box<ArtifactRelationExpr>,
        offset: usize,
    },
    Limit {
        input: Box<ArtifactRelationExpr>,
        limit: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUnionArm {
    pub label: String,
    pub input: ArtifactRelationExpr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactJoinCondition {
    pub left: ArtifactColumnRef,
    pub right: ArtifactColumnRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactJoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProjectColumn {
    pub alias: String,
    pub expr: ArtifactKeyRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactOrderBy {
    pub column: ArtifactColumnRef,
    pub direction: ArtifactDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactColumnRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub column: String,
}

/// A column or a row id, as join keys, dedupe keys and projections name
/// them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactKeyRef {
    Column { column: ArtifactColumnRef },
    RowId { row: ArtifactRowId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactRowId {
    Current,
    Outer,
    Frontier,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactPredicate {
    Cmp {
        left: ArtifactColumnRef,
        op: RelationCmpOp,
        right: ArtifactValueRef,
    },
    IsNull {
        column: ArtifactColumnRef,
    },
    IsNotNull {
        column: ArtifactColumnRef,
    },
    In {
        left: ArtifactColumnRef,
        values: Vec<ArtifactValueRef>,
    },
    Contains {
        left: ArtifactColumnRef,
        right: ArtifactValueRef,
    },
    EnumMatch {
        column: ArtifactColumnRef,
        case: String,
        payload: Box<ArtifactPredicate>,
    },
    And {
        exprs: Vec<ArtifactPredicate>,
    },
    Or {
        exprs: Vec<ArtifactPredicate>,
    },
    Not {
        expr: Box<ArtifactPredicate>,
    },
    True {},
    False {},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactValueRef {
    /// A literal in the relation IR's JSON form, kept as the engine reads it.
    Literal {
        value: serde_json::Value,
    },
    Param {
        name: String,
    },
    SessionRef {
        path: Vec<String>,
    },
    OuterColumn {
        column: ArtifactColumnRef,
    },
    FrontierColumn {
        column: ArtifactColumnRef,
    },
    RowId {
        row: ArtifactRowId,
    },
}

fn policy_value_to_artifact(value: &PolicyValue) -> ArtifactPolicyValue {
    match value {
        PolicyValue::Literal(value) => ArtifactPolicyValue::Literal {
            value: value.clone(),
        },
        PolicyValue::SessionRef(path) => ArtifactPolicyValue::SessionRef { path: path.clone() },
    }
}

fn policy_value_from_artifact(value: &ArtifactPolicyValue) -> PolicyValue {
    match value {
        ArtifactPolicyValue::Literal { value } => PolicyValue::Literal(value.clone()),
        ArtifactPolicyValue::SessionRef { path } => PolicyValue::SessionRef(path.clone()),
    }
}

fn policy_to_artifact(expr: &PolicyExpr) -> Result<ArtifactPolicyExpr, String> {
    let all = |exprs: &[PolicyExpr]| -> Result<Vec<_>, String> {
        exprs.iter().map(policy_to_artifact).collect()
    };
    Ok(match expr {
        PolicyExpr::Cmp { column, op, value } => ArtifactPolicyExpr::Cmp {
            column: column.clone(),
            op: op.clone(),
            value: policy_value_to_artifact(value),
        },
        PolicyExpr::SessionCmp { path, op, value } => ArtifactPolicyExpr::SessionCmp {
            path: path.clone(),
            op: op.clone(),
            value: value.clone(),
        },
        PolicyExpr::IsNull { column } => ArtifactPolicyExpr::IsNull {
            column: column.clone(),
        },
        PolicyExpr::SessionIsNull { path } => {
            ArtifactPolicyExpr::SessionIsNull { path: path.clone() }
        }
        PolicyExpr::IsNotNull { column } => ArtifactPolicyExpr::IsNotNull {
            column: column.clone(),
        },
        PolicyExpr::SessionIsNotNull { path } => {
            ArtifactPolicyExpr::SessionIsNotNull { path: path.clone() }
        }
        PolicyExpr::Contains { column, value } => ArtifactPolicyExpr::Contains {
            column: column.clone(),
            value: policy_value_to_artifact(value),
        },
        PolicyExpr::SessionContains { path, value } => ArtifactPolicyExpr::SessionContains {
            path: path.clone(),
            value: value.clone(),
        },
        PolicyExpr::In {
            column,
            session_path,
        } => ArtifactPolicyExpr::In {
            column: column.clone(),
            session_path: session_path.clone(),
        },
        PolicyExpr::InList { column, values } => ArtifactPolicyExpr::InList {
            column: column.clone(),
            values: values.iter().map(policy_value_to_artifact).collect(),
        },
        PolicyExpr::SessionInList { path, values } => ArtifactPolicyExpr::SessionInList {
            path: path.clone(),
            values: values.clone(),
        },
        PolicyExpr::Exists { table, condition } => ArtifactPolicyExpr::Exists {
            table: table.clone(),
            condition: Box::new(policy_to_artifact(condition)?),
        },
        PolicyExpr::ExistsRel { rel } => ArtifactPolicyExpr::ExistsRel {
            rel: relation_to_artifact(rel),
        },
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => ArtifactPolicyExpr::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => ArtifactPolicyExpr::InheritsReferencing {
            operation: *operation,
            source_table: source_table.clone(),
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        PolicyExpr::And(exprs) => ArtifactPolicyExpr::And { exprs: all(exprs)? },
        PolicyExpr::Or(exprs) => ArtifactPolicyExpr::Or { exprs: all(exprs)? },
        PolicyExpr::Not(expr) => ArtifactPolicyExpr::Not {
            expr: Box::new(policy_to_artifact(expr)?),
        },
        PolicyExpr::True => ArtifactPolicyExpr::True {},
        PolicyExpr::False => ArtifactPolicyExpr::False {},
    })
}

fn policy_from_artifact(expr: &ArtifactPolicyExpr) -> PolicyExpr {
    let all = |exprs: &[ArtifactPolicyExpr]| exprs.iter().map(policy_from_artifact).collect();
    match expr {
        ArtifactPolicyExpr::Cmp { column, op, value } => PolicyExpr::Cmp {
            column: column.clone(),
            op: op.clone(),
            value: policy_value_from_artifact(value),
        },
        ArtifactPolicyExpr::SessionCmp { path, op, value } => PolicyExpr::SessionCmp {
            path: path.clone(),
            op: op.clone(),
            value: value.clone(),
        },
        ArtifactPolicyExpr::IsNull { column } => PolicyExpr::IsNull {
            column: column.clone(),
        },
        ArtifactPolicyExpr::SessionIsNull { path } => {
            PolicyExpr::SessionIsNull { path: path.clone() }
        }
        ArtifactPolicyExpr::IsNotNull { column } => PolicyExpr::IsNotNull {
            column: column.clone(),
        },
        ArtifactPolicyExpr::SessionIsNotNull { path } => {
            PolicyExpr::SessionIsNotNull { path: path.clone() }
        }
        ArtifactPolicyExpr::Contains { column, value } => PolicyExpr::Contains {
            column: column.clone(),
            value: policy_value_from_artifact(value),
        },
        ArtifactPolicyExpr::SessionContains { path, value } => PolicyExpr::SessionContains {
            path: path.clone(),
            value: value.clone(),
        },
        ArtifactPolicyExpr::In {
            column,
            session_path,
        } => PolicyExpr::In {
            column: column.clone(),
            session_path: session_path.clone(),
        },
        ArtifactPolicyExpr::InList { column, values } => PolicyExpr::InList {
            column: column.clone(),
            values: values.iter().map(policy_value_from_artifact).collect(),
        },
        ArtifactPolicyExpr::SessionInList { path, values } => PolicyExpr::SessionInList {
            path: path.clone(),
            values: values.clone(),
        },
        ArtifactPolicyExpr::Exists { table, condition } => PolicyExpr::Exists {
            table: table.clone(),
            condition: Box::new(policy_from_artifact(condition)),
        },
        ArtifactPolicyExpr::ExistsRel { rel } => PolicyExpr::ExistsRel {
            rel: relation_from_artifact(rel),
        },
        ArtifactPolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => PolicyExpr::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        ArtifactPolicyExpr::InheritsReferencing {
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
        ArtifactPolicyExpr::And { exprs } => PolicyExpr::And(all(exprs)),
        ArtifactPolicyExpr::Or { exprs } => PolicyExpr::Or(all(exprs)),
        ArtifactPolicyExpr::Not { expr } => PolicyExpr::Not(Box::new(policy_from_artifact(expr))),
        ArtifactPolicyExpr::True {} => PolicyExpr::True,
        ArtifactPolicyExpr::False {} => PolicyExpr::False,
    }
}

fn column_ref_to_artifact(column: &RelationColumnRef) -> ArtifactColumnRef {
    ArtifactColumnRef {
        scope: column.scope.clone(),
        column: column.column.clone(),
    }
}

fn column_ref_from_artifact(column: &ArtifactColumnRef) -> RelationColumnRef {
    RelationColumnRef {
        scope: column.scope.clone(),
        column: column.column.clone(),
    }
}

fn row_id_to_artifact(row: RelationRowIdRef) -> ArtifactRowId {
    match row {
        RelationRowIdRef::Current => ArtifactRowId::Current,
        RelationRowIdRef::Outer => ArtifactRowId::Outer,
        RelationRowIdRef::Frontier => ArtifactRowId::Frontier,
    }
}

fn row_id_from_artifact(row: ArtifactRowId) -> RelationRowIdRef {
    match row {
        ArtifactRowId::Current => RelationRowIdRef::Current,
        ArtifactRowId::Outer => RelationRowIdRef::Outer,
        ArtifactRowId::Frontier => RelationRowIdRef::Frontier,
    }
}

fn key_to_artifact(key: &RelationKeyRef) -> ArtifactKeyRef {
    match key {
        RelationKeyRef::Column(column) => ArtifactKeyRef::Column {
            column: column_ref_to_artifact(column),
        },
        RelationKeyRef::RowId(row) => ArtifactKeyRef::RowId {
            row: row_id_to_artifact(*row),
        },
    }
}

fn key_from_artifact(key: &ArtifactKeyRef) -> RelationKeyRef {
    match key {
        ArtifactKeyRef::Column { column } => {
            RelationKeyRef::Column(column_ref_from_artifact(column))
        }
        ArtifactKeyRef::RowId { row } => RelationKeyRef::RowId(row_id_from_artifact(*row)),
    }
}

fn relation_to_artifact(expr: &RelationExpr) -> ArtifactRelationExpr {
    let boxed = |expr: &RelationExpr| Box::new(relation_to_artifact(expr));
    match expr {
        RelationExpr::TableScan { table, alias } => ArtifactRelationExpr::TableScan {
            table: table.clone(),
            alias: alias.clone(),
        },
        RelationExpr::Filter { input, predicate } => ArtifactRelationExpr::Filter {
            input: boxed(input),
            predicate: predicate_to_artifact(predicate),
        },
        RelationExpr::Union { inputs } => ArtifactRelationExpr::Union {
            inputs: inputs
                .iter()
                .map(|arm| ArtifactUnionArm {
                    label: arm.label.clone(),
                    input: relation_to_artifact(&arm.input),
                })
                .collect(),
        },
        RelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => ArtifactRelationExpr::Join {
            left: boxed(left),
            right: boxed(right),
            on: on
                .iter()
                .map(|condition| ArtifactJoinCondition {
                    left: column_ref_to_artifact(&condition.left),
                    right: column_ref_to_artifact(&condition.right),
                })
                .collect(),
            join_kind: match join_kind {
                RelationJoinKind::Inner => ArtifactJoinKind::Inner,
                RelationJoinKind::Left => ArtifactJoinKind::Left,
            },
        },
        RelationExpr::Project { input, columns } => ArtifactRelationExpr::Project {
            input: boxed(input),
            columns: columns
                .iter()
                .map(|column| ArtifactProjectColumn {
                    alias: column.alias.clone(),
                    expr: match &column.expr {
                        RelationProjectExpr::Column(column) => ArtifactKeyRef::Column {
                            column: column_ref_to_artifact(column),
                        },
                        RelationProjectExpr::RowId(row) => ArtifactKeyRef::RowId {
                            row: row_id_to_artifact(*row),
                        },
                    },
                })
                .collect(),
        },
        RelationExpr::Gather {
            seed,
            step,
            frontier_key,
            bound,
            dedupe_key,
        } => ArtifactRelationExpr::Gather {
            seed: boxed(seed),
            step: boxed(step),
            frontier_key: key_to_artifact(frontier_key),
            max_depth: match bound {
                RecursionBound::Fixpoint => None,
                RecursionBound::MaxDepth(depth) => Some(*depth),
            },
            dedupe_key: dedupe_key.iter().map(key_to_artifact).collect(),
        },
        RelationExpr::Distinct { input, key } => ArtifactRelationExpr::Distinct {
            input: boxed(input),
            key: key.iter().map(key_to_artifact).collect(),
        },
        RelationExpr::OrderBy { input, terms } => ArtifactRelationExpr::OrderBy {
            input: boxed(input),
            terms: terms
                .iter()
                .map(|term| ArtifactOrderBy {
                    column: column_ref_to_artifact(&term.column),
                    direction: match term.direction {
                        OrderDirection::Asc => ArtifactDirection::Asc,
                        OrderDirection::Desc => ArtifactDirection::Desc,
                    },
                })
                .collect(),
        },
        RelationExpr::Offset { input, offset } => ArtifactRelationExpr::Offset {
            input: boxed(input),
            offset: *offset,
        },
        RelationExpr::Limit { input, limit } => ArtifactRelationExpr::Limit {
            input: boxed(input),
            limit: *limit,
        },
    }
}

fn relation_from_artifact(expr: &ArtifactRelationExpr) -> RelationExpr {
    let boxed = |expr: &ArtifactRelationExpr| Box::new(relation_from_artifact(expr));
    match expr {
        ArtifactRelationExpr::TableScan { table, alias } => RelationExpr::TableScan {
            table: table.clone(),
            alias: alias.clone(),
        },
        ArtifactRelationExpr::Filter { input, predicate } => RelationExpr::Filter {
            input: boxed(input),
            predicate: predicate_from_artifact(predicate),
        },
        ArtifactRelationExpr::Union { inputs } => RelationExpr::Union {
            inputs: inputs
                .iter()
                .map(|arm| RelationUnionArm {
                    label: arm.label.clone(),
                    input: relation_from_artifact(&arm.input),
                })
                .collect(),
        },
        ArtifactRelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => RelationExpr::Join {
            left: boxed(left),
            right: boxed(right),
            on: on
                .iter()
                .map(|condition| RelationJoinCondition {
                    left: column_ref_from_artifact(&condition.left),
                    right: column_ref_from_artifact(&condition.right),
                })
                .collect(),
            join_kind: match join_kind {
                ArtifactJoinKind::Inner => RelationJoinKind::Inner,
                ArtifactJoinKind::Left => RelationJoinKind::Left,
            },
        },
        ArtifactRelationExpr::Project { input, columns } => RelationExpr::Project {
            input: boxed(input),
            columns: columns
                .iter()
                .map(|column| RelationProjectColumn {
                    alias: column.alias.clone(),
                    expr: match &column.expr {
                        ArtifactKeyRef::Column { column } => {
                            RelationProjectExpr::Column(column_ref_from_artifact(column))
                        }
                        ArtifactKeyRef::RowId { row } => {
                            RelationProjectExpr::RowId(row_id_from_artifact(*row))
                        }
                    },
                })
                .collect(),
        },
        ArtifactRelationExpr::Gather {
            seed,
            step,
            frontier_key,
            max_depth,
            dedupe_key,
        } => RelationExpr::Gather {
            seed: boxed(seed),
            step: boxed(step),
            frontier_key: key_from_artifact(frontier_key),
            bound: match max_depth {
                None => RecursionBound::Fixpoint,
                Some(depth) => RecursionBound::MaxDepth(*depth),
            },
            dedupe_key: dedupe_key.iter().map(key_from_artifact).collect(),
        },
        ArtifactRelationExpr::Distinct { input, key } => RelationExpr::Distinct {
            input: boxed(input),
            key: key.iter().map(key_from_artifact).collect(),
        },
        ArtifactRelationExpr::OrderBy { input, terms } => RelationExpr::OrderBy {
            input: boxed(input),
            terms: terms
                .iter()
                .map(|term| RelationOrderBy {
                    column: column_ref_from_artifact(&term.column),
                    direction: match term.direction {
                        ArtifactDirection::Asc => OrderDirection::Asc,
                        ArtifactDirection::Desc => OrderDirection::Desc,
                    },
                })
                .collect(),
        },
        ArtifactRelationExpr::Offset { input, offset } => RelationExpr::Offset {
            input: boxed(input),
            offset: *offset,
        },
        ArtifactRelationExpr::Limit { input, limit } => RelationExpr::Limit {
            input: boxed(input),
            limit: *limit,
        },
    }
}

fn value_ref_to_artifact(value: &RelationValueRef) -> ArtifactValueRef {
    match value {
        RelationValueRef::Literal(value) => ArtifactValueRef::Literal {
            value: value.clone(),
        },
        RelationValueRef::Param(name) => ArtifactValueRef::Param { name: name.clone() },
        RelationValueRef::SessionRef(path) => ArtifactValueRef::SessionRef { path: path.clone() },
        RelationValueRef::OuterColumn(column) => ArtifactValueRef::OuterColumn {
            column: column_ref_to_artifact(column),
        },
        RelationValueRef::FrontierColumn(column) => ArtifactValueRef::FrontierColumn {
            column: column_ref_to_artifact(column),
        },
        RelationValueRef::RowId(row) => ArtifactValueRef::RowId {
            row: row_id_to_artifact(*row),
        },
    }
}

fn value_ref_from_artifact(value: &ArtifactValueRef) -> RelationValueRef {
    match value {
        ArtifactValueRef::Literal { value } => RelationValueRef::Literal(value.clone()),
        ArtifactValueRef::Param { name } => RelationValueRef::Param(name.clone()),
        ArtifactValueRef::SessionRef { path } => RelationValueRef::SessionRef(path.clone()),
        ArtifactValueRef::OuterColumn { column } => {
            RelationValueRef::OuterColumn(column_ref_from_artifact(column))
        }
        ArtifactValueRef::FrontierColumn { column } => {
            RelationValueRef::FrontierColumn(column_ref_from_artifact(column))
        }
        ArtifactValueRef::RowId { row } => RelationValueRef::RowId(row_id_from_artifact(*row)),
    }
}

fn predicate_to_artifact(predicate: &RelationPredicate) -> ArtifactPredicate {
    let all = |exprs: &[RelationPredicate]| exprs.iter().map(predicate_to_artifact).collect();
    match predicate {
        RelationPredicate::Cmp { left, op, right } => ArtifactPredicate::Cmp {
            left: column_ref_to_artifact(left),
            op: *op,
            right: value_ref_to_artifact(right),
        },
        RelationPredicate::IsNull { column } => ArtifactPredicate::IsNull {
            column: column_ref_to_artifact(column),
        },
        RelationPredicate::IsNotNull { column } => ArtifactPredicate::IsNotNull {
            column: column_ref_to_artifact(column),
        },
        RelationPredicate::In { left, values } => ArtifactPredicate::In {
            left: column_ref_to_artifact(left),
            values: values.iter().map(value_ref_to_artifact).collect(),
        },
        RelationPredicate::Contains { left, right } => ArtifactPredicate::Contains {
            left: column_ref_to_artifact(left),
            right: value_ref_to_artifact(right),
        },
        RelationPredicate::EnumMatch {
            column,
            case,
            payload,
        } => ArtifactPredicate::EnumMatch {
            column: column_ref_to_artifact(column),
            case: case.clone(),
            payload: Box::new(predicate_to_artifact(payload)),
        },
        RelationPredicate::And(exprs) => ArtifactPredicate::And { exprs: all(exprs) },
        RelationPredicate::Or(exprs) => ArtifactPredicate::Or { exprs: all(exprs) },
        RelationPredicate::Not(expr) => ArtifactPredicate::Not {
            expr: Box::new(predicate_to_artifact(expr)),
        },
        RelationPredicate::True => ArtifactPredicate::True {},
        RelationPredicate::False => ArtifactPredicate::False {},
    }
}

fn predicate_from_artifact(predicate: &ArtifactPredicate) -> RelationPredicate {
    let all = |exprs: &[ArtifactPredicate]| exprs.iter().map(predicate_from_artifact).collect();
    match predicate {
        ArtifactPredicate::Cmp { left, op, right } => RelationPredicate::Cmp {
            left: column_ref_from_artifact(left),
            op: *op,
            right: value_ref_from_artifact(right),
        },
        ArtifactPredicate::IsNull { column } => RelationPredicate::IsNull {
            column: column_ref_from_artifact(column),
        },
        ArtifactPredicate::IsNotNull { column } => RelationPredicate::IsNotNull {
            column: column_ref_from_artifact(column),
        },
        ArtifactPredicate::In { left, values } => RelationPredicate::In {
            left: column_ref_from_artifact(left),
            values: values.iter().map(value_ref_from_artifact).collect(),
        },
        ArtifactPredicate::Contains { left, right } => RelationPredicate::Contains {
            left: column_ref_from_artifact(left),
            right: value_ref_from_artifact(right),
        },
        ArtifactPredicate::EnumMatch {
            column,
            case,
            payload,
        } => RelationPredicate::EnumMatch {
            column: column_ref_from_artifact(column),
            case: case.clone(),
            payload: Box::new(predicate_from_artifact(payload)),
        },
        ArtifactPredicate::And { exprs } => RelationPredicate::And(all(exprs)),
        ArtifactPredicate::Or { exprs } => RelationPredicate::Or(all(exprs)),
        ArtifactPredicate::Not { expr } => {
            RelationPredicate::Not(Box::new(predicate_from_artifact(expr)))
        }
        ArtifactPredicate::True {} => RelationPredicate::True,
        ArtifactPredicate::False {} => RelationPredicate::False,
    }
}

// ---------------------------------------------------------------------------
// The file's JSON <-> these forms
// ---------------------------------------------------------------------------

use super::json::Json;

fn to_serde(value: &Json) -> serde_json::Value {
    match value {
        Json::Null => serde_json::Value::Null,
        Json::Bool(b) => serde_json::Value::Bool(*b),
        Json::Number(text) => {
            serde_json::from_str(text).unwrap_or_else(|_| serde_json::Value::String(text.clone()))
        }
        Json::String(s) => serde_json::Value::String(s.clone()),
        Json::Array(items) => serde_json::Value::Array(items.iter().map(to_serde).collect()),
        Json::Object(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), to_serde(v)))
                .collect(),
        ),
    }
}

fn from_serde(value: serde_json::Value) -> Json {
    match value {
        serde_json::Value::Null => Json::Null,
        serde_json::Value::Bool(b) => Json::Bool(b),
        serde_json::Value::Number(n) => Json::Number(n.to_string()),
        serde_json::Value::String(s) => Json::String(s),
        serde_json::Value::Array(items) => Json::Array(items.into_iter().map(from_serde).collect()),
        serde_json::Value::Object(map) => {
            // Keys in a stable order: the tag first, then the rest by name.
            let mut entries: Vec<(String, Json)> =
                map.into_iter().map(|(k, v)| (k, from_serde(v))).collect();
            entries
                .sort_by(|(a, _), (b, _)| (a != "type").cmp(&(b != "type")).then_with(|| a.cmp(b)));
            Json::Object(entries)
        }
    }
}

/// Whether `value` is one of the structured policy forms: `{"exists": …}`
/// or an internally tagged `{"type": …}` policy.
pub fn is_structured(value: &Json) -> bool {
    match value.as_object() {
        Some([(key, _)]) if key == "exists" => true,
        Some(entries) => entries.iter().any(|(key, _)| key == "type"),
        None => false,
    }
}

/// `expr` in its structured form: `{"exists": <relation>}` for a relation
/// existence check, the tagged policy otherwise.
pub fn write(expr: &PolicyExpr) -> Json {
    if let PolicyExpr::ExistsRel { rel } = expr {
        let rel = serde_json::to_value(relation_to_artifact(rel)).expect("relation serializes");
        return Json::Object(vec![("exists".into(), from_serde(rel))]);
    }
    let file = policy_to_artifact(expr).expect("every policy has a structured form");
    from_serde(serde_json::to_value(file).expect("policy serializes"))
}

/// A structured policy back to the engine's.
pub fn read(value: &Json) -> Result<PolicyExpr, String> {
    if let Some([(key, rel)]) = value.as_object()
        && key == "exists"
    {
        let rel: ArtifactRelationExpr = serde_json::from_value(to_serde(rel))
            .map_err(|e| format!("`exists`: not a relation expression: {e}"))?;
        return Ok(PolicyExpr::ExistsRel {
            rel: relation_from_artifact(&rel),
        });
    }
    let file: ArtifactPolicyExpr = serde_json::from_value(to_serde(value))
        .map_err(|e| format!("not a structured policy: {e}"))?;
    Ok(policy_from_artifact(&file))
}

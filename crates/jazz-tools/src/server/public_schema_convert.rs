use std::collections::BTreeMap;
use std::fmt;

use jazz::groove::records::{EnumSchema, Value as GrooveValue};
use jazz::groove::schema::ColumnType as GrooveColumnType;
use jazz::query::{
    JoinCorrelation, JoinSourceLookup, JoinTarget, JoinVia, Operand, PolicyBranch, Predicate, Query,
};
use jazz::schema::{
    ColumnSchema as CoreColumnSchema, JazzSchema, LargeValueKind as CoreLargeValueKind,
    MergeStrategy, TableSchema as CoreTableSchema, WritePolicies,
};

use crate::public_api::policy::{CmpOp, PolicyValue};
use crate::public_schema::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, LargeValueKind, Operation, PolicyExpr,
    Schema, TableName, TableSchema, Value,
};

const DIRECT_USER_ID_CLAIM: &str = "user_id";
const PUBLIC_USER_ID_SESSION_PATHS: &[&str] = &["user_id", "userId"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaConversionError {
    path: String,
    message: String,
}

impl SchemaConversionError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for SchemaConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for SchemaConversionError {}

pub(crate) fn convert_public_schema(schema: &Schema) -> Result<JazzSchema, SchemaConversionError> {
    let mut tables = schema.iter().collect::<Vec<_>>();
    tables.sort_by_key(|(name, _)| name.as_str());
    tables
        .into_iter()
        .map(|(name, table)| convert_table(schema, name, table))
        .collect::<Result<Vec<_>, _>>()
        .map(JazzSchema::new)
}

fn convert_table(
    schema: &Schema,
    name: &TableName,
    table: &TableSchema,
) -> Result<CoreTableSchema, SchemaConversionError> {
    let mut references = BTreeMap::new();
    let mut columns = Vec::with_capacity(table.columns.columns.len());
    let mut merge_strategies = BTreeMap::new();
    for column in &table.columns.columns {
        let converted = convert_column(name, column)?;
        if let Some(reference) = &column.references {
            references.insert(
                column.name.as_str().to_owned(),
                reference.as_str().to_owned(),
            );
        }
        if let Some(strategy) = column.merge_strategy {
            merge_strategies.insert(
                column.name.as_str().to_owned(),
                convert_merge_strategy(name, column, strategy)?,
            );
        }
        columns.push(converted);
    }

    let mut converted = CoreTableSchema::new(name.as_str(), columns);
    converted.references = references;
    converted.indexed_columns = table
        .indexed_columns
        .as_ref()
        .map(|columns| {
            columns
                .iter()
                .map(|column| column.as_str().to_owned())
                .collect()
        })
        .unwrap_or_default();
    converted.merge_strategies = merge_strategies;
    converted.read_policy = convert_optional_policy(
        schema,
        table,
        name,
        "policies.select.using",
        table.policies.select.using.as_ref(),
    )?;
    converted.write_policies = WritePolicies {
        insert_check: convert_optional_policy(
            schema,
            table,
            name,
            "policies.insert.with_check",
            table.policies.insert.with_check.as_ref(),
        )?,
        update_using: convert_optional_policy(
            schema,
            table,
            name,
            "policies.update.using",
            table.policies.update.using.as_ref(),
        )?,
        update_check: convert_optional_policy(
            schema,
            table,
            name,
            "policies.update.with_check",
            table.policies.update.with_check.as_ref(),
        )?,
        delete_using: convert_optional_policy(
            schema,
            table,
            name,
            "policies.delete.using",
            table.policies.delete.using.as_ref(),
        )?,
    };
    Ok(converted)
}

fn convert_column(
    table: &TableName,
    column: &ColumnDescriptor,
) -> Result<CoreColumnSchema, SchemaConversionError> {
    let mut column_type = convert_column_type(table, column.name.as_str(), &column.column_type)?;
    if column.nullable {
        column_type = column_type.nullable();
    }
    let mut converted = CoreColumnSchema::new(column.name.as_str(), column_type);
    if let Some(kind) = column.large_value {
        if column.column_type != ColumnType::Bytea {
            return Err(err(
                format!("$.{}.{}", table.as_str(), column.name.as_str()),
                "large_value is only supported on Bytea columns",
            ));
        }
        converted.large_value = Some(match kind {
            LargeValueKind::Text => CoreLargeValueKind::Text,
            LargeValueKind::Blob => CoreLargeValueKind::Blob,
        });
    }
    Ok(converted)
}

fn convert_column_type(
    table: &TableName,
    column: &str,
    column_type: &ColumnType,
) -> Result<GrooveColumnType, SchemaConversionError> {
    match column_type {
        ColumnType::Boolean => Ok(GrooveColumnType::Bool),
        ColumnType::Text => Ok(GrooveColumnType::String),
        ColumnType::Timestamp => Ok(GrooveColumnType::U64),
        ColumnType::Double => Ok(GrooveColumnType::F64),
        ColumnType::Uuid => Ok(GrooveColumnType::Uuid),
        ColumnType::Bytea => Ok(GrooveColumnType::Bytes),
        ColumnType::Enum { variants } => Ok(GrooveColumnType::Enum(
            EnumSchema::new(
                format!("{}_{}", table.as_str(), column),
                variants.iter().cloned(),
            )
            .map_err(|error| {
                err(
                    format!("$.{}.{}", table.as_str(), column),
                    format!("invalid enum: {error}"),
                )
            })?,
        )),
        ColumnType::Array { element } => {
            Ok(convert_column_type(table, column, element.as_ref())?.array_of())
        }
        // Core does not currently have signed integer cells. Public
        // INTEGER columns are therefore represented as U32 and the
        // core write path rejects negative values.
        ColumnType::Integer => Ok(GrooveColumnType::U32),
        ColumnType::BigInt => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "BIGINT is signed, but server shell fixed schemas only support unsigned integer columns",
        )),
        ColumnType::BatchId => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "BatchId columns are not supported by core schema conversion yet",
        )),
        ColumnType::Json { .. } => Ok(GrooveColumnType::String),
        ColumnType::Row { .. } => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "nested Row columns are not supported by core schema conversion yet",
        )),
    }
}

fn convert_merge_strategy(
    table: &TableName,
    column: &ColumnDescriptor,
    strategy: ColumnMergeStrategy,
) -> Result<MergeStrategy, SchemaConversionError> {
    match strategy {
        ColumnMergeStrategy::Counter => Ok(MergeStrategy::Counter),
        ColumnMergeStrategy::GSet => Err(err(
            format!("$.{}.{}", table.as_str(), column.name.as_str()),
            "GSet merge strategy is not supported by core schema conversion yet",
        )),
    }
}

fn convert_optional_policy(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    expr: Option<&PolicyExpr>,
) -> Result<Option<Query>, SchemaConversionError> {
    expr.map(|expr| convert_policy(schema, table_schema, table, path, expr))
        .transpose()
}

fn convert_policy(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    expr: &PolicyExpr,
) -> Result<Query, SchemaConversionError> {
    match expr {
        PolicyExpr::And(exprs) => {
            if !exprs.iter().any(is_core_policy_clause) {
                return Ok(Query::from(table.as_str())
                    .filter(convert_policy_predicate(table, path, expr)?));
            }
            let mut query = Query::from(table.as_str());
            for (index, expr) in exprs.iter().enumerate() {
                query = append_policy_clause(
                    schema,
                    table_schema,
                    table,
                    &format!("{path}.And[{index}]"),
                    query,
                    expr,
                )?;
            }
            Ok(query)
        }
        PolicyExpr::Or(exprs) if exprs.iter().any(policy_requires_branch) => {
            let mut query = Query::from(table.as_str()).filter(Predicate::Any(Vec::new()));
            for (index, expr) in exprs.iter().enumerate() {
                let branch = convert_policy(
                    schema,
                    table_schema,
                    table,
                    &format!("{path}.Or[{index}]"),
                    expr,
                )?;
                for branch in PolicyBranch::alternatives_from_query(branch) {
                    query = query.policy_branch(branch);
                }
            }
            Ok(query)
        }
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth: _,
        } => append_inherited_policy(
            schema,
            table_schema,
            table,
            path,
            Query::from(table.as_str()),
            *operation,
            via_column,
        ),
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth: _,
        } => append_inherited_referencing_policy(
            schema,
            table,
            path,
            Query::from(table.as_str()),
            *operation,
            source_table,
            via_column,
        ),
        PolicyExpr::Exists {
            table: exists_table,
            condition,
        } => append_exists_policy_clause(
            schema,
            table,
            path,
            Query::from(table.as_str()),
            exists_table,
            condition,
        ),
        _ => Ok(Query::from(table.as_str()).filter(convert_policy_predicate(table, path, expr)?)),
    }
}

fn is_core_policy_clause(expr: &PolicyExpr) -> bool {
    matches!(
        expr,
        PolicyExpr::Inherits { max_depth: _, .. }
            | PolicyExpr::InheritsReferencing { .. }
            | PolicyExpr::Exists { .. }
    )
}

fn policy_requires_branch(expr: &PolicyExpr) -> bool {
    match expr {
        PolicyExpr::Inherits { max_depth: _, .. }
        | PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::Exists { .. } => true,
        PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => exprs.iter().any(policy_requires_branch),
        PolicyExpr::Not(expr) => policy_requires_branch(expr),
        _ => false,
    }
}

fn append_policy_clause(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    query: Query,
    expr: &PolicyExpr,
) -> Result<Query, SchemaConversionError> {
    match expr {
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth: _,
        } => append_inherited_policy(
            schema,
            table_schema,
            table,
            path,
            query,
            *operation,
            via_column,
        ),
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth: _,
        } => append_inherited_referencing_policy(
            schema,
            table,
            path,
            query,
            *operation,
            source_table,
            via_column,
        ),
        PolicyExpr::Exists {
            table: exists_table,
            condition,
        } => append_exists_policy_clause(schema, table, path, query, exists_table, condition),
        _ => Ok(query.filter(convert_policy_predicate(table, path, expr)?)),
    }
}

fn append_inherited_referencing_policy(
    schema: &Schema,
    table: &TableName,
    path: &str,
    query: Query,
    operation: Operation,
    source_table: &str,
    via_column: &str,
) -> Result<Query, SchemaConversionError> {
    let source_table_name = TableName::new(source_table.to_owned());
    let source_schema = schema.get(&source_table_name).ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!("INHERITS_REFERENCING source_table '{source_table}' was not found"),
        )
    })?;
    let column = source_schema
        .columns
        .columns
        .iter()
        .find(|column| column.name.as_str() == via_column)
        .ok_or_else(|| {
            err(
                format!("$.{}.{}", table.as_str(), path),
                format!(
                    "INHERITS_REFERENCING via_column '{via_column}' was not found on source_table '{source_table}'"
                ),
            )
        })?;
    match column.references.as_ref() {
        Some(target) if target == table => {}
        Some(target) => {
            return Err(err(
                format!("$.{}.{}", table.as_str(), path),
                format!(
                    "INHERITS_REFERENCING via_column '{via_column}' references '{target}', expected '{}'",
                    table.as_str()
                ),
            ));
        }
        None => {
            return Err(err(
                format!("$.{}.{}", table.as_str(), path),
                format!("INHERITS_REFERENCING via_column '{via_column}' has no FK reference"),
            ));
        }
    }

    let source_policy = source_operation_policy(source_schema, operation).ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!(
                "INHERITS_REFERENCING source_table '{source_table}' has no {operation:?} policy"
            ),
        )
    })?;
    let source_filter = convert_policy_predicate(
        &source_table_name,
        &format!("{path}.InheritsReferencing[{source_table}]"),
        source_policy,
    );
    match source_filter {
        Ok(source_filter) => Ok(query.join_via(source_table, via_column, [source_filter])),
        Err(_) if policy_requires_branch(source_policy) => {
            let source_query = convert_policy(
                schema,
                source_schema,
                &source_table_name,
                &format!("{path}.InheritsReferencing[{source_table}]"),
                source_policy,
            )?;
            append_inherited_referencing_policy_branches(
                query,
                source_table,
                via_column,
                source_query,
            )
        }
        Err(err) => Err(err),
    }
}

fn source_operation_policy(table: &TableSchema, operation: Operation) -> Option<&PolicyExpr> {
    match operation {
        Operation::Select => table.policies.select.using.as_ref(),
        Operation::Insert => table.policies.insert.with_check.as_ref(),
        Operation::Update => table.policies.update.using.as_ref().or(table
            .policies
            .update
            .with_check
            .as_ref()),
        Operation::Delete => table.policies.effective_delete_using(),
    }
}

fn append_inherited_referencing_policy_branches(
    mut query: Query,
    source_table: &str,
    via_column: &str,
    source_query: Query,
) -> Result<Query, SchemaConversionError> {
    query = query.filter(Predicate::Any(Vec::new()));
    let branches = PolicyBranch::alternatives_from_query(source_query);

    for branch in branches {
        if !branch.reachable.is_empty() {
            return Err(err(
                format!("$.{source_table}.InheritsReferencing"),
                "core schema policies do not support INHERITS_REFERENCING through reachability yet",
            ));
        }
        let branch_query = Query::from(query.table.as_str()).join_via_with_nested_joins(
            source_table,
            via_column,
            branch.filters,
            branch.joins,
        );
        for branch in PolicyBranch::alternatives_from_query(branch_query) {
            query = query.policy_branch(branch);
        }
    }
    Ok(query)
}

fn append_exists_policy_clause(
    schema: &Schema,
    table: &TableName,
    path: &str,
    query: Query,
    exists_table: &str,
    condition: &PolicyExpr,
) -> Result<Query, SchemaConversionError> {
    let exists_table_name = TableName::new(exists_table.to_owned());
    if !schema.contains_key(&exists_table_name) {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            format!("EXISTS references unknown table '{exists_table}'"),
        ));
    }

    let mut join_column = None;
    let mut source_column = None;
    let mut correlated_filters = Vec::new();
    let mut filters = Vec::new();

    let conditions = match condition {
        PolicyExpr::And(exprs) => exprs.as_slice(),
        expr => std::slice::from_ref(expr),
    };

    for (index, expr) in conditions.iter().enumerate() {
        match expr {
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(path_segments),
            } if path_segments.len() == 2 && path_segments[0] == "__jazz_outer_row" => {
                if join_column.is_none() {
                    join_column = Some(column.clone());
                    source_column = Some(path_segments[1].clone());
                } else {
                    correlated_filters.push(JoinCorrelation {
                        join_column: column.clone(),
                        source_column: path_segments[1].clone(),
                    });
                }
            }
            other => filters.push(convert_policy_predicate(
                &exists_table_name,
                &format!("{path}.Exists[{index}]"),
                other,
            )?),
        }
    }

    let join_column = join_column.ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            "core schema policies require EXISTS to include an equality against __jazz_outer_row",
        )
    })?;
    let source_column = source_column.expect("join_column and source_column are set together");

    if join_column == "id" {
        Ok(query.join_via_row_id(exists_table, source_column, filters))
    } else if correlated_filters.is_empty() {
        Ok(query.join_via_column(exists_table, join_column, source_column, filters))
    } else {
        Ok(query.join_via_column_with_correlations(
            exists_table,
            join_column,
            source_column,
            correlated_filters,
            filters,
        ))
    }
}

fn append_inherited_policy(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    query: Query,
    operation: Operation,
    via_column: &str,
) -> Result<Query, SchemaConversionError> {
    let column = table_schema
        .columns
        .columns
        .iter()
        .find(|column| column.name.as_str() == via_column)
        .ok_or_else(|| {
            err(
                format!("$.{}.{}", table.as_str(), path),
                format!("INHERITS via_column '{via_column}' was not found"),
            )
        })?;
    let parent_table = column.references.as_ref().ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!("INHERITS via_column '{via_column}' has no FK reference"),
        )
    })?;
    let parent_schema = schema.get(parent_table).ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!("INHERITS via_column '{via_column}' references unknown table '{parent_table}'"),
        )
    })?;
    let parent_policy = source_operation_policy(parent_schema, operation).ok_or_else(|| {
        err(
            format!("$.{}.{}", table.as_str(), path),
            format!("INHERITS via_column '{via_column}' references table '{parent_table}' without a {operation:?} policy"),
        )
    })?;
    let parent_filter = convert_policy_predicate(
        parent_table,
        &format!("{path}.Inherits[{parent_table}]"),
        parent_policy,
    );
    match parent_filter {
        Ok(parent_filter) => {
            Ok(query.join_via_row_id(parent_table.as_str(), via_column, [parent_filter]))
        }
        Err(_) if policy_requires_branch(parent_policy) => {
            let parent_query = convert_policy(
                schema,
                parent_schema,
                parent_table,
                &format!("{path}.Inherits[{parent_table}]"),
                parent_policy,
            )?;
            append_inherited_policy_branches(
                table,
                path,
                query,
                parent_table,
                via_column,
                parent_query,
            )
        }
        Err(err) => Err(err),
    }
}

fn append_inherited_policy_branches(
    table: &TableName,
    path: &str,
    mut query: Query,
    parent_table: &TableName,
    via_column: &str,
    parent_query: Query,
) -> Result<Query, SchemaConversionError> {
    query = query.filter(Predicate::Any(Vec::new()));
    let branches = PolicyBranch::alternatives_from_query(parent_query);

    for (index, branch) in branches.into_iter().enumerate() {
        let branch_query = inherited_parent_branch_to_child_query(
            table,
            path,
            parent_table,
            via_column,
            index,
            branch,
        )?;
        for branch in PolicyBranch::alternatives_from_query(branch_query) {
            query = query.policy_branch(branch);
        }
    }
    Ok(query)
}

fn inherited_parent_branch_to_child_query(
    table: &TableName,
    path: &str,
    parent_table: &TableName,
    via_column: &str,
    index: usize,
    branch: PolicyBranch,
) -> Result<Query, SchemaConversionError> {
    if !branch.reachable.is_empty() {
        return Err(err(
            format!("$.{}.{}.InheritsBranch[{index}]", table.as_str(), path),
            "core schema policies do not support inherited SELECT branches with reachability yet",
        ));
    }
    let mut query = Query::from(table.as_str());
    if !branch.filters.is_empty() {
        query = query.join_via_row_id(parent_table.as_str(), via_column, branch.filters);
    }
    for join in branch.joins {
        let JoinVia {
            table: join_table,
            on_column,
            target,
            source_column,
            source_lookup: _,
            correlated_filters,
            filters,
            nested_joins,
        } = join;
        query = match target {
            JoinTarget::Column => {
                if let Some(source_column) = source_column {
                    let source_lookup = JoinSourceLookup {
                        table: parent_table.as_str().to_owned(),
                        row_id_source_column: via_column.to_owned(),
                        value_column: source_column,
                    };
                    let mut query = query;
                    query.joins.push(JoinVia {
                        table: join_table,
                        on_column,
                        target: JoinTarget::Column,
                        source_column: Some(source_lookup.value_column.clone()),
                        source_lookup: Some(source_lookup),
                        correlated_filters,
                        filters,
                        nested_joins,
                    });
                    query
                } else {
                    let mut query = query.join_via_column(
                        join_table,
                        on_column,
                        via_column.to_owned(),
                        filters,
                    );
                    if let Some(last) = query.joins.last_mut() {
                        last.correlated_filters = correlated_filters;
                        last.nested_joins = nested_joins;
                    }
                    query
                }
            }
            JoinTarget::RowId => {
                if let Some(source_column) = source_column {
                    let source_lookup = JoinSourceLookup {
                        table: parent_table.as_str().to_owned(),
                        row_id_source_column: via_column.to_owned(),
                        value_column: source_column,
                    };
                    let mut query = query;
                    query.joins.push(JoinVia {
                        table: join_table,
                        on_column,
                        target: JoinTarget::RowId,
                        source_column: Some(source_lookup.value_column.clone()),
                        source_lookup: Some(source_lookup),
                        correlated_filters,
                        filters,
                        nested_joins,
                    });
                    query
                } else {
                    if join_table != parent_table.as_str() {
                        return Err(err(
                            format!("$.{}.{}.InheritsBranch[{index}]", table.as_str(), path),
                            "core schema policies do not support inherited SELECT row-id joins to non-parent tables yet",
                        ));
                    }
                    let mut query =
                        query.join_via_row_id(join_table, via_column.to_owned(), filters);
                    if let Some(last) = query.joins.last_mut() {
                        last.correlated_filters = correlated_filters;
                        last.nested_joins = nested_joins;
                    }
                    query
                }
            }
        };
    }
    Ok(query)
}

fn convert_policy_predicate(
    table: &TableName,
    path: &str,
    expr: &PolicyExpr,
) -> Result<Predicate, SchemaConversionError> {
    match expr {
        PolicyExpr::True => Ok(Predicate::All(Vec::new())),
        PolicyExpr::False => Ok(Predicate::Any(Vec::new())),
        PolicyExpr::And(exprs) => exprs
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                convert_policy_predicate(table, &format!("{path}.And[{index}]"), expr)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Predicate::All),
        PolicyExpr::Or(exprs) => exprs
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                convert_policy_predicate(table, &format!("{path}.Or[{index}]"), expr)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Predicate::Any),
        PolicyExpr::Not(expr) => Ok(Predicate::Not(Box::new(convert_policy_predicate(
            table,
            &format!("{path}.Not"),
            expr,
        )?))),
        PolicyExpr::Cmp { column, op, value } => {
            let left = Operand::Column(column.clone());
            let right = convert_policy_operand(table, path, value)?;
            Ok(match op {
                CmpOp::Eq => Predicate::Eq(left, right),
                CmpOp::Ne => Predicate::Ne(left, right),
                CmpOp::Lt => Predicate::Lt(left, right),
                CmpOp::Le => Predicate::Lte(left, right),
                CmpOp::Gt => Predicate::Gt(left, right),
                CmpOp::Ge => Predicate::Gte(left, right),
            })
        }
        PolicyExpr::IsNull { column } => Ok(Predicate::IsNull(Operand::Column(column.clone()))),
        PolicyExpr::IsNotNull { column } => Ok(Predicate::Not(Box::new(Predicate::IsNull(
            Operand::Column(column.clone()),
        )))),
        PolicyExpr::Contains { column, value } => Ok(Predicate::Contains(
            Operand::Column(column.clone()),
            convert_policy_operand(table, path, value)?,
        )),
        PolicyExpr::In {
            column,
            session_path,
        } => Ok(Predicate::Contains(
            convert_session_path_operand(table, path, session_path)?,
            Operand::Column(column.clone()),
        )),
        PolicyExpr::InList { column, values } => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                convert_policy_operand(table, &format!("{path}.InList[{index}]"), value)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|values| {
                Predicate::Any(
                    values
                        .into_iter()
                        .map(|value| Predicate::Eq(Operand::Column(column.clone()), value))
                        .collect(),
                )
            }),
        other => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            format!("core schema policies do not support {other:?} yet"),
        )),
    }
}

fn convert_policy_operand(
    table: &TableName,
    path: &str,
    value: &PolicyValue,
) -> Result<Operand, SchemaConversionError> {
    match value {
        PolicyValue::SessionRef(path_segments) => {
            convert_session_path_operand(table, path, path_segments)
        }
        PolicyValue::Literal(value) => Ok(Operand::Literal(convert_policy_literal(
            table, path, value,
        )?)),
    }
}

fn convert_session_path_operand(
    table: &TableName,
    path: &str,
    path_segments: &[String],
) -> Result<Operand, SchemaConversionError> {
    if path_segments.len() == 1 && PUBLIC_USER_ID_SESSION_PATHS.contains(&path_segments[0].as_str())
    {
        return Ok(Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()));
    }
    if path_segments.len() == 2 && path_segments[0] == "claims" {
        return Ok(Operand::Claim(path_segments[1].clone()));
    }
    Err(err(
        format!("$.{}.{}", table.as_str(), path),
        format!(
            "core schema policies only support session.user_id and session.claims.* references, got session.{}",
            path_segments.join(".")
        ),
    ))
}

fn convert_policy_literal(
    table: &TableName,
    path: &str,
    value: &Value,
) -> Result<GrooveValue, SchemaConversionError> {
    match value {
        Value::Null => Ok(GrooveValue::Nullable(None)),
        Value::Boolean(value) => Ok(GrooveValue::Bool(*value)),
        Value::Text(value) => Ok(GrooveValue::String(value.clone())),
        Value::Uuid(value) => Ok(GrooveValue::Uuid(*value.uuid())),
        other => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            format!("core schema policies do not support {other:?} literals yet"),
        )),
    }
}

fn err(path: impl Into<String>, message: impl Into<String>) -> SchemaConversionError {
    SchemaConversionError::new(path, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::public_api::policy::{CmpOp, PolicyValue};
    use crate::public_api::types::TableSchemaBuilder;
    use crate::public_schema::{
        ColumnDescriptor, ColumnType, LargeValueKind, PolicyExpr, RowDescriptor, SchemaBuilder,
        TablePolicies, TableSchema,
    };
    use jazz::query::{JoinTarget, Operand, Predicate};
    use uuid::Uuid;

    #[test]
    fn converts_supported_columns_references_and_indexes() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("projects").column("name", ColumnType::Text))
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("created", ColumnType::Timestamp)
                    .column("score", ColumnType::Double)
                    .column("data", ColumnType::Bytea)
                    .fk_column("project_id", "projects")
                    .index_only(["project_id"]),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let todos = converted
            .tables
            .iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            todos.references.get("project_id").map(String::as_str),
            Some("projects")
        );
        assert!(todos.indexed_columns.contains("project_id"));
        assert_eq!(
            todos
                .columns
                .iter()
                .find(|column| column.name == "done")
                .unwrap()
                .column_type,
            GrooveColumnType::Bool
        );
    }

    #[test]
    fn converts_large_value_columns() {
        let schema = [(
            TableName::new("files"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("data", ColumnType::Bytea).large_value(LargeValueKind::Blob),
            ])),
        )]
        .into_iter()
        .collect();

        let converted = convert_public_schema(&schema).unwrap();
        let column = converted.tables[0]
            .columns
            .iter()
            .find(|column| column.name == "data")
            .unwrap();

        assert_eq!(column.column_type, GrooveColumnType::Bytes);
        assert_eq!(column.large_value, Some(jazz::schema::LargeValueKind::Blob));
    }

    #[test]
    fn converts_public_integer_as_core_u32_and_ignores_defaults() {
        let integer_schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column("count", ColumnType::Integer))
            .build();
        let integer_table = convert_public_schema(&integer_schema)
            .unwrap()
            .tables
            .into_iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            integer_table
                .columns
                .iter()
                .find(|column| column.name == "count")
                .unwrap()
                .column_type,
            GrooveColumnType::U32
        );

        let integer_array_schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column(
                "partSizes",
                ColumnType::Array {
                    element: Box::new(ColumnType::Integer),
                },
            ))
            .build();
        let integer_array_table = convert_public_schema(&integer_array_schema)
            .unwrap()
            .tables
            .into_iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            integer_array_table
                .columns
                .iter()
                .find(|column| column.name == "partSizes")
                .unwrap()
                .column_type,
            GrooveColumnType::U32.array_of()
        );

        let default_schema = [(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text)
                    .default(Value::Text("x".to_owned())),
            ])),
        )]
        .into_iter()
        .collect();
        let default_table = convert_public_schema(&default_schema)
            .unwrap()
            .tables
            .into_iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            default_table
                .columns
                .iter()
                .find(|column| column.name == "title")
                .unwrap()
                .column_type,
            GrooveColumnType::String
        );
    }

    #[test]
    fn converts_public_json_as_core_string_storage() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("events")
                    .column(
                        "payload",
                        ColumnType::Json {
                            schema: Some(serde_json::json!({
                                "type": "object",
                                "properties": {
                                    "kind": { "type": "string" }
                                }
                            })),
                        },
                    )
                    .nullable_column("metadata", ColumnType::Json { schema: None }),
            )
            .build();

        let table = convert_public_schema(&schema)
            .unwrap()
            .tables
            .into_iter()
            .find(|table| table.name == "events")
            .unwrap();

        assert_eq!(
            table
                .columns
                .iter()
                .find(|column| column.name == "payload")
                .unwrap()
                .column_type,
            GrooveColumnType::String
        );
        assert_eq!(
            table
                .columns
                .iter()
                .find(|column| column.name == "metadata")
                .unwrap()
                .column_type,
            GrooveColumnType::String.nullable()
        );
    }

    #[test]
    fn rejects_unsupported_public_column_types() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column("count", ColumnType::BigInt))
            .build();

        let error = convert_public_schema(&schema).unwrap_err();
        assert_eq!(error.path, "$.todos.count");
        assert!(error.message.contains("BIGINT is signed"));

        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column(
                "payload",
                ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                        "title",
                        ColumnType::Text,
                    )])),
                },
            ))
            .build();

        let error = convert_public_schema(&schema).unwrap_err();
        assert_eq!(error.path, "$.todos.payload");
        assert!(error.message.contains("nested Row columns"));
    }

    #[test]
    fn converts_supported_table_policies_to_core_read_and_write_queries() {
        let owner_id = ObjectId::from_uuid(Uuid::nil());
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("owner_id", ColumnType::Text)
                    .column("token_id", ColumnType::Uuid)
                    .column("archived", ColumnType::Boolean)
                    .nullable_column("deleted_at", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::And(vec![
                                PolicyExpr::Cmp {
                                    column: "owner_id".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                                },
                                PolicyExpr::Not(Box::new(PolicyExpr::Cmp {
                                    column: "archived".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::Literal(false.into()),
                                })),
                                PolicyExpr::Or(vec![
                                    PolicyExpr::IsNull {
                                        column: "deleted_at".to_owned(),
                                    },
                                    PolicyExpr::IsNotNull {
                                        column: "deleted_at".to_owned(),
                                    },
                                    PolicyExpr::Cmp {
                                        column: "owner_id".to_owned(),
                                        op: CmpOp::Eq,
                                        value: PolicyValue::SessionRef(vec![
                                            "claims".to_owned(),
                                            "team_id".to_owned(),
                                        ]),
                                    },
                                ]),
                            ]))
                            .with_insert(PolicyExpr::Cmp {
                                column: "token_id".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::Literal(Value::Uuid(owner_id)),
                            }),
                    ),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let todos = converted
            .tables
            .iter()
            .find(|table| table.name == "todos")
            .unwrap();

        assert_eq!(todos.read_policy.as_ref().unwrap().table, "todos");
        assert_eq!(
            todos.read_policy.as_ref().unwrap().filters,
            vec![Predicate::All(vec![
                Predicate::Eq(
                    Operand::Column("owner_id".to_owned()),
                    Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()),
                ),
                Predicate::Not(Box::new(Predicate::Eq(
                    Operand::Column("archived".to_owned()),
                    Operand::Literal(GrooveValue::Bool(false)),
                ))),
                Predicate::Any(vec![
                    Predicate::IsNull(Operand::Column("deleted_at".to_owned())),
                    Predicate::Not(Box::new(Predicate::IsNull(Operand::Column(
                        "deleted_at".to_owned(),
                    )))),
                    Predicate::Eq(
                        Operand::Column("owner_id".to_owned()),
                        Operand::Claim("team_id".to_owned()),
                    ),
                ]),
            ])]
        );
        assert_eq!(
            todos.write_policies.insert_check.as_ref().unwrap().table,
            "todos"
        );
        assert_eq!(
            todos.write_policies.insert_check.as_ref().unwrap().filters,
            vec![Predicate::Eq(
                Operand::Column("token_id".to_owned()),
                Operand::Literal(GrooveValue::Uuid(Uuid::nil())),
            )]
        );
    }

    #[test]
    fn converts_correlated_exists_policy_to_join() {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("chats").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_insert(PolicyExpr::Exists {
                        table: "chatMembers".to_owned(),
                        condition: Box::new(PolicyExpr::And(vec![
                            PolicyExpr::Cmp {
                                column: "chatId".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "__jazz_outer_row".to_owned(),
                                    "chatId".to_owned(),
                                ]),
                            },
                            PolicyExpr::Cmp {
                                column: "userId".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                            },
                        ])),
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let messages = converted
            .tables
            .iter()
            .find(|table| table.name == "messages")
            .unwrap();
        let policy = messages.write_policies.insert_check.as_ref().unwrap();
        assert!(policy.filters.is_empty());
        assert_eq!(policy.joins.len(), 1);
        let join = &policy.joins[0];
        assert_eq!(join.table, "chatMembers");
        assert_eq!(join.on_column, "chatId");
        assert_eq!(join.target, JoinTarget::Column);
        assert_eq!(join.source_column.as_deref(), Some("chatId"));
        assert_eq!(
            join.filters,
            vec![Predicate::Eq(
                Operand::Column("userId".to_owned()),
                Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()),
            )]
        );
    }

    #[test]
    fn converts_correlated_exists_against_id_to_row_id_join() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("chats")
                    .column("name", ColumnType::Text)
                    .column("isPublic", ColumnType::Boolean),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Exists {
                        table: "chats".to_owned(),
                        condition: Box::new(PolicyExpr::And(vec![
                            PolicyExpr::Cmp {
                                column: "id".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "__jazz_outer_row".to_owned(),
                                    "chatId".to_owned(),
                                ]),
                            },
                            PolicyExpr::Cmp {
                                column: "isPublic".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::Literal(Value::Boolean(true)),
                            },
                        ])),
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let messages = converted
            .tables
            .iter()
            .find(|table| table.name == "messages")
            .unwrap();
        let policy = messages.read_policy.as_ref().unwrap();
        assert!(policy.filters.is_empty());
        assert_eq!(policy.joins.len(), 1);
        let join = &policy.joins[0];
        assert_eq!(join.table, "chats");
        assert_eq!(join.on_column, "id");
        assert_eq!(join.target, JoinTarget::RowId);
        assert_eq!(join.source_column.as_deref(), Some("chatId"));
        assert_eq!(
            join.filters,
            vec![Predicate::Eq(
                Operand::Column("isPublic".to_owned()),
                Operand::Literal(GrooveValue::Bool(true)),
            )]
        );
    }

    #[test]
    fn converts_or_with_correlated_exists_to_policy_branches() {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("chats").column("isPublic", ColumnType::Boolean))
            .table(
                TableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("isPinned", ColumnType::Boolean)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Or(vec![
                        PolicyExpr::Cmp {
                            column: "isPinned".to_owned(),
                            op: CmpOp::Eq,
                            value: PolicyValue::Literal(Value::Boolean(true)),
                        },
                        PolicyExpr::Exists {
                            table: "chatMembers".to_owned(),
                            condition: Box::new(PolicyExpr::And(vec![
                                PolicyExpr::Cmp {
                                    column: "chatId".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::SessionRef(vec![
                                        "__jazz_outer_row".to_owned(),
                                        "chatId".to_owned(),
                                    ]),
                                },
                                PolicyExpr::Cmp {
                                    column: "userId".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                                },
                            ])),
                        },
                    ]))),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let messages = converted
            .tables
            .iter()
            .find(|table| table.name == "messages")
            .unwrap();
        let policy = messages.read_policy.as_ref().unwrap();
        assert_eq!(policy.filters, vec![Predicate::Any(Vec::new())]);
        assert_eq!(policy.policy_branches.len(), 2);
        assert_eq!(policy.policy_branches[0].joins.len(), 0);
        assert_eq!(policy.policy_branches[1].joins.len(), 1);
        let join = &policy.policy_branches[1].joins[0];
        assert_eq!(join.table, "chatMembers");
        assert_eq!(join.on_column, "chatId");
        assert_eq!(join.source_column.as_deref(), Some("chatId"));
    }

    #[test]
    fn converts_chat_public_or_membership_select_policy_to_branches() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .column("visibility", ColumnType::Text)
                    .column("owner_id", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Or(vec![
                        PolicyExpr::Cmp {
                            column: "visibility".to_owned(),
                            op: CmpOp::Eq,
                            value: PolicyValue::Literal(Value::Text("public".to_owned())),
                        },
                        PolicyExpr::Exists {
                            table: "chat_members".to_owned(),
                            condition: Box::new(PolicyExpr::And(vec![
                                PolicyExpr::Cmp {
                                    column: "chat_id".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::SessionRef(vec![
                                        "__jazz_outer_row".to_owned(),
                                        "id".to_owned(),
                                    ]),
                                },
                                PolicyExpr::Cmp {
                                    column: "user_id".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                                },
                            ])),
                        },
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("chat_members")
                    .fk_column("chat_id", "chats")
                    .column("user_id", ColumnType::Text),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let chats = converted
            .tables
            .iter()
            .find(|table| table.name == "chats")
            .unwrap();
        let policy = chats.read_policy.as_ref().unwrap();
        assert_eq!(policy.filters, vec![Predicate::Any(Vec::new())]);
        assert_eq!(policy.policy_branches.len(), 2);
        assert_eq!(
            policy.policy_branches[0].filters,
            vec![Predicate::Eq(
                Operand::Column("visibility".to_owned()),
                Operand::Literal(GrooveValue::String("public".to_owned())),
            )]
        );
        assert_eq!(policy.policy_branches[0].joins.len(), 0);
        assert_eq!(policy.policy_branches[1].filters.len(), 0);
        assert_eq!(policy.policy_branches[1].joins.len(), 1);
        let join = &policy.policy_branches[1].joins[0];
        assert_eq!(join.table, "chat_members");
        assert_eq!(join.on_column, "chat_id");
        assert_eq!(join.source_column.as_deref(), Some("id"));
        assert_eq!(
            join.filters,
            vec![Predicate::Eq(
                Operand::Column("user_id".to_owned()),
                Operand::Claim("user_id".to_owned()),
            )]
        );
    }

    #[test]
    fn rejects_unsupported_policy_subset() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new().with_select(PolicyExpr::SessionContains {
                            path: vec!["roles".to_owned()],
                            value: "admin".into(),
                        }),
                    ),
            )
            .build();

        let error = convert_public_schema(&schema).unwrap_err();
        assert!(error.to_string().starts_with(
            "$.todos.policies.select.using: core schema policies do not support SessionContains"
        ));
    }

    #[test]
    fn converts_unbounded_inherited_select_to_row_id_join() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column(
                        "owners",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Text),
                        },
                    )
                    .policies(TablePolicies::new().with_select(PolicyExpr::Contains {
                        column: "owners".to_owned(),
                        value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                    })),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .nullable_fk_column("folder_id", "folders")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "folder_id".to_owned(),
                        max_depth: None,
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let documents = converted
            .tables
            .iter()
            .find(|table| table.name == "documents")
            .unwrap();
        let policy = documents.read_policy.as_ref().unwrap();
        assert!(policy.filters.is_empty());
        assert_eq!(policy.joins.len(), 1);
        let join = &policy.joins[0];
        assert_eq!(join.table, "folders");
        assert_eq!(join.on_column, "id");
        assert_eq!(join.target, JoinTarget::RowId);
        assert_eq!(join.source_column.as_deref(), Some("folder_id"));
        assert_eq!(
            join.filters,
            vec![Predicate::Contains(
                Operand::Column("owners".to_owned()),
                Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()),
            )]
        );
    }

    #[test]
    fn converts_depth_limited_inherited_select_to_row_id_join() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("name", ColumnType::Text)
                    .column("kind", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::And(vec![
                        PolicyExpr::In {
                            column: "id".to_owned(),
                            session_path: vec!["claims".to_owned(), "team_ids".to_owned()],
                        },
                        PolicyExpr::InList {
                            column: "kind".to_owned(),
                            values: vec![
                                PolicyValue::Literal(Value::Text("individual".to_owned())),
                                PolicyValue::Literal(Value::Text("manual".to_owned())),
                            ],
                        },
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("team_access_edges")
                    .fk_column("target_team", "teams")
                    .column("grant_role", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "target_team".to_owned(),
                        max_depth: Some(32),
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let access_edges = converted
            .tables
            .iter()
            .find(|table| table.name == "team_access_edges")
            .unwrap();
        let policy = access_edges.read_policy.as_ref().unwrap();
        assert!(policy.filters.is_empty());
        assert_eq!(policy.joins.len(), 1);
        let join = &policy.joins[0];
        assert_eq!(join.table, "teams");
        assert_eq!(join.on_column, "id");
        assert_eq!(join.target, JoinTarget::RowId);
        assert_eq!(join.source_column.as_deref(), Some("target_team"));
        assert_eq!(
            join.filters,
            vec![Predicate::All(vec![
                Predicate::Contains(
                    Operand::Claim("team_ids".to_owned()),
                    Operand::Column("id".to_owned()),
                ),
                Predicate::Any(vec![
                    Predicate::Eq(
                        Operand::Column("kind".to_owned()),
                        Operand::Literal(GrooveValue::String("individual".to_owned())),
                    ),
                    Predicate::Eq(
                        Operand::Column("kind".to_owned()),
                        Operand::Literal(GrooveValue::String("manual".to_owned())),
                    ),
                ])
            ])]
        );
    }

    #[test]
    fn converts_empty_in_list_to_false_predicate() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("kind", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::InList {
                        column: "kind".to_owned(),
                        values: Vec::new(),
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let teams = converted
            .tables
            .iter()
            .find(|table| table.name == "teams")
            .unwrap();
        let policy = teams.read_policy.as_ref().unwrap();
        assert_eq!(policy.filters, vec![Predicate::Any(Vec::new())]);
    }

    #[test]
    fn converts_inherited_select_branch_with_parent_column_join() {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("chats").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Exists {
                        table: "chatMembers".to_owned(),
                        condition: Box::new(PolicyExpr::And(vec![
                            PolicyExpr::Cmp {
                                column: "chatId".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "__jazz_outer_row".to_owned(),
                                    "chatId".to_owned(),
                                ]),
                            },
                            PolicyExpr::Cmp {
                                column: "userId".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                            },
                        ])),
                    })),
            )
            .table(
                TableSchemaBuilder::new("reactions")
                    .fk_column("messageId", "messages")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "messageId".to_owned(),
                        max_depth: None,
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let reactions = converted
            .tables
            .iter()
            .find(|table| table.name == "reactions")
            .unwrap();
        let policy = reactions.read_policy.as_ref().unwrap();
        assert_eq!(policy.policy_branches.len(), 1);
        let join = &policy.policy_branches[0].joins[0];
        assert_eq!(join.table, "chatMembers");
        assert_eq!(join.on_column, "chatId");
        assert_eq!(join.target, JoinTarget::Column);
        assert_eq!(join.source_column.as_deref(), Some("chatId"));
        let lookup = join.source_lookup.as_ref().unwrap();
        assert_eq!(lookup.table, "messages");
        assert_eq!(lookup.row_id_source_column, "messageId");
        assert_eq!(lookup.value_column, "chatId");
    }

    #[test]
    fn converts_reverse_inherited_select_to_source_table_join() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("attachments")
                    .fk_column("fileId", "files")
                    .column("ownerId", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Cmp {
                        column: "ownerId".to_owned(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                    })),
            )
            .table(
                TableSchemaBuilder::new("files")
                    .column("name", ColumnType::Text)
                    .policies(
                        TablePolicies::new().with_select(PolicyExpr::InheritsReferencing {
                            operation: Operation::Select,
                            source_table: "attachments".to_owned(),
                            via_column: "fileId".to_owned(),
                            max_depth: None,
                        }),
                    ),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let files = converted
            .tables
            .iter()
            .find(|table| table.name == "files")
            .unwrap();
        let policy = files.read_policy.as_ref().unwrap();
        assert!(policy.filters.is_empty());
        assert_eq!(policy.joins.len(), 1);
        let join = &policy.joins[0];
        assert_eq!(join.table, "attachments");
        assert_eq!(join.on_column, "fileId");
        assert_eq!(join.target, JoinTarget::Column);
        assert_eq!(join.source_column, None);
        assert_eq!(
            join.filters,
            vec![Predicate::Eq(
                Operand::Column("ownerId".to_owned()),
                Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()),
            )]
        );
    }

    #[test]
    fn converts_reverse_inherited_select_with_nested_source_policy() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("ownerId", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Cmp {
                        column: "ownerId".to_owned(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
                    })),
            )
            .table(
                TableSchemaBuilder::new("attachments")
                    .fk_column("fileId", "files")
                    .fk_column("teamId", "teams")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "teamId".to_owned(),
                        max_depth: None,
                    })),
            )
            .table(
                TableSchemaBuilder::new("files")
                    .column("name", ColumnType::Text)
                    .policies(
                        TablePolicies::new().with_select(PolicyExpr::InheritsReferencing {
                            operation: Operation::Select,
                            source_table: "attachments".to_owned(),
                            via_column: "fileId".to_owned(),
                            max_depth: None,
                        }),
                    ),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let files = converted
            .tables
            .iter()
            .find(|table| table.name == "files")
            .unwrap();
        let policy = files.read_policy.as_ref().unwrap();
        assert_eq!(policy.policy_branches.len(), 1);
        let branch = &policy.policy_branches[0];
        assert_eq!(branch.joins.len(), 1);
        let attachment_join = &branch.joins[0];
        assert_eq!(attachment_join.table, "attachments");
        assert_eq!(attachment_join.on_column, "fileId");
        assert_eq!(attachment_join.nested_joins.len(), 1);
        let team_join = &attachment_join.nested_joins[0];
        assert_eq!(team_join.table, "teams");
        assert_eq!(team_join.target, JoinTarget::RowId);
        assert_eq!(team_join.source_column.as_deref(), Some("teamId"));
        assert_eq!(
            team_join.filters,
            vec![Predicate::Eq(
                Operand::Column("ownerId".to_owned()),
                Operand::Claim(DIRECT_USER_ID_CLAIM.to_owned()),
            )]
        );
    }
}

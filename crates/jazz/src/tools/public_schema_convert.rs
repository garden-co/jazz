use std::collections::BTreeMap;
use std::fmt;

use crate::groove::records::{
    EnumCase, EnumSchema, EnumValue, RecordDescriptor, ScalarEnumSchema, Value as GrooveValue,
};
use crate::groove::schema::ColumnType as GrooveColumnType;
use crate::query::{
    InheritsOperation, JoinCorrelation, JoinSourceLookup, JoinTarget, JoinVia, Operand,
    PolicyBranch, Predicate, Query, provider_claim_operand_key,
};
use crate::schema::{
    ColumnSchema as CoreColumnSchema, JazzSchema, MergeStrategy, RuntimeSchema,
    TableSchema as CoreTableSchema, WritePolicies,
};

use crate::tools::public_api::policy::{CmpOp, PolicyValue};
use crate::tools::public_api::relation_ir::{
    ColumnRef, JoinKind as RelJoinKind, PredicateCmpOp as RelPredicateCmpOp,
    PredicateExpr as RelPredicateExpr, ProjectExpr as RelProjectExpr,
    RecursionBound as RelRecursionBound, RelExpr, RowIdRef, ValueRef as RelValueRef,
};
use crate::tools::public_schema::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, Operation, PolicyExpr, Schema, TableName,
    TableSchema, Value,
};

const DIRECT_USER_CLAIM: &str = "user";
#[cfg(test)]
const DIRECT_USER_ID_CLAIM: &str = "\0claims:sub";
const PUBLIC_USER_SESSION_PATHS: &[&str] = &["user"];
const DIRECT_AUTH_MODE_CLAIM: &str = "authMode";
const PUBLIC_AUTH_MODE_SESSION_PATHS: &[&str] = &["authMode", "auth_mode"];
const RESERVED_AGGREGATE_OUTPUT_PREFIX: &str = "__jazz_aggregate_";

#[derive(Debug, Clone, PartialEq, Eq)]
/// Error produced while compiling a developer-authored schema for the runtime.
pub struct SchemaConversionError {
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
    let mut converted = schema
        .iter()
        .map(|(name, table)| convert_table(schema, name, table))
        .collect::<Result<Vec<_>, _>>()?;
    coerce_typed_literals(schema, &mut converted);
    validate_converted_schema(&converted)?;
    Ok(JazzSchema::from_runtime(
        schema.clone(),
        RuntimeSchema { tables: converted },
    ))
}

/// Decode and compile the public JSON schema used by native bindings.
pub fn decode_public_schema_json(bytes: &[u8]) -> Result<JazzSchema, String> {
    let schema: Schema =
        serde_json::from_slice(bytes).map_err(|error| format!("decode public schema: {error}"))?;
    convert_public_schema(&schema).map_err(|error| format!("compile public schema: {error}"))
}

fn validate_converted_schema(tables: &[CoreTableSchema]) -> Result<(), SchemaConversionError> {
    let schema = RuntimeSchema {
        tables: tables.to_vec(),
    };
    let mut branch_column_types = BTreeMap::<String, GrooveColumnType>::new();
    for table in tables {
        let mut bound = std::collections::BTreeSet::new();
        for branch_column in &table.branch_by {
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *branch_column)
                .ok_or_else(|| {
                    err(
                        format!("$.{}.branchBy", table.name),
                        format!("unknown branch column {branch_column:?}"),
                    )
                })?;
            if !bound.insert(branch_column) {
                return Err(err(
                    format!("$.{}.branchBy", table.name),
                    format!("branch column {branch_column:?} is listed more than once"),
                ));
            }
            if matches!(column.column_type, GrooveColumnType::Nullable(_)) {
                return Err(err(
                    format!("$.{}.branchBy", table.name),
                    format!("branch column {branch_column:?} must be non-null"),
                ));
            }
            if !matches!(
                column.column_type,
                GrooveColumnType::String
                    | GrooveColumnType::Uuid
                    | GrooveColumnType::U8
                    | GrooveColumnType::U16
                    | GrooveColumnType::U32
                    | GrooveColumnType::U64
                    | GrooveColumnType::I32
                    | GrooveColumnType::I64
                    | GrooveColumnType::EnumTag(_)
            ) {
                return Err(err(
                    format!("$.{}.branchBy", table.name),
                    "branch columns require string, UUID, stable enum, or fixed-width integer values",
                ));
            }
            if let Some(existing) =
                branch_column_types.insert(branch_column.clone(), column.column_type.clone())
                && existing != column.column_type
            {
                return Err(err(
                    format!("$.{}.branchBy", table.name),
                    format!(
                        "branch column {branch_column:?} must have the same type in every table"
                    ),
                ));
            }
        }
    }
    for table in tables {
        if let Some(policy) = &table.read_policy {
            policy.validate_runtime(&schema).map_err(|error| {
                err(
                    format!("$.{}.policies.select.using", table.name),
                    format!("converted read policy is invalid: {error:?}"),
                )
            })?;
        }
        for (label, policy) in table.write_policies.iter() {
            policy.validate_runtime(&schema).map_err(|error| {
                err(
                    format!("$.{}.policies.{label}", table.name),
                    format!("converted write policy is invalid: {error:?}"),
                )
            })?;
        }
    }
    Ok(())
}

#[derive(Clone)]
enum TypedLiteralTarget {
    Core(GrooveColumnType),
    PublicEnum,
}

fn coerce_typed_literals(schema: &Schema, tables: &mut [CoreTableSchema]) {
    let column_types = tables
        .iter()
        .map(|table| {
            let columns = table
                .columns
                .iter()
                .map(|column| {
                    let target = schema
                        .get(&TableName::from(table.name.clone()))
                        .and_then(|table_schema| table_schema.columns.column(&column.name))
                        .map(|public_column| match public_column.column_type {
                            ColumnType::Enum { .. } => TypedLiteralTarget::PublicEnum,
                            _ => TypedLiteralTarget::Core(column.column_type.clone()),
                        })
                        .unwrap_or_else(|| TypedLiteralTarget::Core(column.column_type.clone()));
                    (column.name.clone(), target)
                })
                .collect::<BTreeMap<_, _>>();
            (table.name.clone(), columns)
        })
        .collect::<BTreeMap<_, _>>();

    for table in tables {
        if let Some(policy) = &mut table.read_policy {
            coerce_query_typed_literals(policy, &column_types);
        }
        coerce_optional_query_typed_literals(&mut table.write_policies.insert_check, &column_types);
        coerce_optional_query_typed_literals(&mut table.write_policies.update_using, &column_types);
        coerce_optional_query_typed_literals(&mut table.write_policies.update_check, &column_types);
        coerce_optional_query_typed_literals(&mut table.write_policies.delete_using, &column_types);
    }
}

fn coerce_optional_query_typed_literals(
    query: &mut Option<Query>,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) {
    if let Some(query) = query {
        coerce_query_typed_literals(query, column_types);
    }
}

fn coerce_query_typed_literals(
    query: &mut Query,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) {
    coerce_predicates_typed_literals(&query.table, &mut query.filters, column_types);
    for join in &mut query.joins {
        coerce_join_typed_literals(join, column_types);
    }
    for reachable in &mut query.reachable {
        coerce_predicates_typed_literals(
            &reachable.access_table,
            &mut reachable.access_filters,
            column_types,
        );
        coerce_predicates_typed_literals(
            &reachable.edge_table,
            &mut reachable.edge_filters,
            column_types,
        );
        if let Some(seed) = &mut reachable.seed {
            coerce_predicates_typed_literals(&seed.table, &mut seed.filters, column_types);
        }
    }
    for branch in &mut query.policy_branches {
        coerce_predicates_typed_literals(&query.table, &mut branch.filters, column_types);
        for join in &mut branch.joins {
            coerce_join_typed_literals(join, column_types);
        }
        for reachable in &mut branch.reachable {
            coerce_predicates_typed_literals(
                &reachable.access_table,
                &mut reachable.access_filters,
                column_types,
            );
            coerce_predicates_typed_literals(
                &reachable.edge_table,
                &mut reachable.edge_filters,
                column_types,
            );
            if let Some(seed) = &mut reachable.seed {
                coerce_predicates_typed_literals(&seed.table, &mut seed.filters, column_types);
            }
        }
    }
}

fn coerce_join_typed_literals(
    join: &mut JoinVia,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) {
    coerce_predicates_typed_literals(&join.table, &mut join.filters, column_types);
    for nested in &mut join.nested_joins {
        coerce_join_typed_literals(nested, column_types);
    }
}

fn coerce_predicates_typed_literals(
    table: &str,
    predicates: &mut [Predicate],
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) {
    for predicate in predicates {
        coerce_predicate_typed_literals(table, predicate, column_types);
    }
}

fn coerce_predicate_typed_literals(
    table: &str,
    predicate: &mut Predicate,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) {
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => {
            coerce_predicates_typed_literals(table, predicates, column_types)
        }
        Predicate::Not(predicate) => {
            coerce_predicate_typed_literals(table, predicate, column_types)
        }
        Predicate::Eq(left, right)
        | Predicate::Ne(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right)
        | Predicate::Gt(left, right)
        | Predicate::Gte(left, right) => {
            if coerce_operand_pair_typed_literal(table, left, right, column_types)
                || coerce_operand_pair_typed_literal(table, right, left, column_types)
            {
                *predicate = Predicate::Any(Vec::new());
            }
        }
        Predicate::In(left, values) => {
            values.retain_mut(|value| {
                !coerce_operand_pair_typed_literal(table, left, value, column_types)
            });
        }
        Predicate::Contains(left, right) => {
            if coerce_operand_pair_typed_literal(table, left, right, column_types)
                || coerce_operand_pair_typed_literal(table, right, left, column_types)
            {
                *predicate = Predicate::Any(Vec::new());
            }
        }
        Predicate::IsNull(_) => {}
        Predicate::EnumMatch { payload, .. } => {
            coerce_predicate_typed_literals(table, payload, column_types);
        }
    }
}

fn coerce_operand_pair_typed_literal(
    table: &str,
    column_operand: &Operand,
    literal_operand: &mut Operand,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) -> bool {
    let Operand::Column(column) = column_operand else {
        return false;
    };
    if let Some(target) = column_type_for_operand(table, column, column_types)
        && let Operand::Literal(value) = literal_operand
    {
        *literal_operand = Operand::Literal(coerce_literal_for_target(value.clone(), &target));
    }
    if let Some(discriminant) =
        column_enum_literal_discriminant(table, column, literal_operand, column_types)
    {
        *literal_operand = Operand::Literal(GrooveValue::EnumTag(discriminant));
        return false;
    }
    if column_is_enum(table, column, column_types) {
        return true;
    }
    if !column_is_string(table, column, column_types) {
        return false;
    }
    if let Operand::Literal(GrooveValue::Uuid(uuid)) = literal_operand {
        *literal_operand = Operand::Literal(GrooveValue::String(uuid.to_string()));
    }
    false
}

fn column_type_for_operand(
    table: &str,
    column: &str,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) -> Option<TypedLiteralTarget> {
    match column {
        "id" => Some(TypedLiteralTarget::Core(GrooveColumnType::Uuid)),
        "$createdBy" | "$updatedBy" => Some(TypedLiteralTarget::Core(GrooveColumnType::String)),
        "$createdAt" | "$updatedAt" => Some(TypedLiteralTarget::Core(GrooveColumnType::U64)),
        _ => column_types
            .get(table)
            .and_then(|columns| columns.get(column))
            .cloned(),
    }
}

fn coerce_literal_for_target(value: GrooveValue, target: &TypedLiteralTarget) -> GrooveValue {
    match target {
        TypedLiteralTarget::Core(column_type) => coerce_literal_for_column_type(value, column_type),
        TypedLiteralTarget::PublicEnum => match value {
            GrooveValue::String(value) => uuid::Uuid::parse_str(&value)
                .map(GrooveValue::Uuid)
                .unwrap_or(GrooveValue::String(value)),
            value => value,
        },
    }
}

fn coerce_literal_for_column_type(
    value: GrooveValue,
    column_type: &GrooveColumnType,
) -> GrooveValue {
    match (value, column_type) {
        (GrooveValue::Uuid(value), GrooveColumnType::String) => {
            GrooveValue::String(value.to_string())
        }
        (GrooveValue::String(value), GrooveColumnType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(GrooveValue::Uuid)
            .unwrap_or(GrooveValue::String(value)),
        (GrooveValue::U64(value), GrooveColumnType::I64) => i64::try_from(value)
            .map(GrooveValue::I64)
            .unwrap_or(GrooveValue::U64(value)),
        (GrooveValue::U64(value), GrooveColumnType::I32) => i32::try_from(value)
            .map(GrooveValue::I32)
            .unwrap_or(GrooveValue::U64(value)),
        (GrooveValue::Nullable(Some(value)), GrooveColumnType::Nullable(inner)) => {
            GrooveValue::Nullable(Some(Box::new(coerce_literal_for_column_type(
                *value, inner,
            ))))
        }
        (GrooveValue::Array(values), GrooveColumnType::Array(inner)) => GrooveValue::Array(
            values
                .into_iter()
                .map(|value| coerce_literal_for_column_type(value, inner))
                .collect(),
        ),
        (GrooveValue::Tuple(values), GrooveColumnType::Tuple(types))
            if values.len() == types.len() =>
        {
            GrooveValue::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, column_type)| coerce_literal_for_column_type(value, column_type))
                    .collect(),
            )
        }
        (GrooveValue::Nullable(Some(value)), column_type) => GrooveValue::Nullable(Some(Box::new(
            coerce_literal_for_column_type(*value, column_type),
        ))),
        (value, GrooveColumnType::Nullable(inner)) => coerce_literal_for_column_type(value, inner),
        (value, _) => value,
    }
}

fn column_enum_literal_discriminant(
    table: &str,
    column: &str,
    literal_operand: &Operand,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) -> Option<u8> {
    let Operand::Literal(GrooveValue::String(value)) = literal_operand else {
        return None;
    };
    column_enum_schema(table, column, column_types)
        .and_then(|schema| schema.discriminant(value).ok())
}

fn column_is_enum(
    table: &str,
    column: &str,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) -> bool {
    column_enum_schema(table, column, column_types).is_some()
}

fn column_enum_schema<'a>(
    table: &str,
    column: &str,
    column_types: &'a BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) -> Option<&'a ScalarEnumSchema> {
    let column_type = match column_types
        .get(table)
        .and_then(|columns| columns.get(column))?
    {
        TypedLiteralTarget::Core(column_type) => column_type,
        TypedLiteralTarget::PublicEnum => return None,
    };
    groove_column_type_enum_schema(column_type)
}

fn groove_column_type_enum_schema(column_type: &GrooveColumnType) -> Option<&ScalarEnumSchema> {
    match column_type {
        GrooveColumnType::EnumTag(schema) => Some(schema),
        GrooveColumnType::Nullable(inner) => groove_column_type_enum_schema(inner),
        _ => None,
    }
}

fn column_is_string(
    table: &str,
    column: &str,
    column_types: &BTreeMap<String, BTreeMap<String, TypedLiteralTarget>>,
) -> bool {
    let Some(TypedLiteralTarget::Core(column_type)) = column_types
        .get(table)
        .and_then(|columns| columns.get(column))
    else {
        return false;
    };
    groove_column_type_is_string(column_type)
}

fn groove_column_type_is_string(column_type: &GrooveColumnType) -> bool {
    match column_type {
        GrooveColumnType::String => true,
        GrooveColumnType::Nullable(inner) => groove_column_type_is_string(inner),
        _ => false,
    }
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
        if column
            .name
            .as_str()
            .starts_with(RESERVED_AGGREGATE_OUTPUT_PREFIX)
        {
            return Err(err(
                format!("$.{}.columns.{}", name.as_str(), column.name.as_str()),
                "column name uses the reserved aggregate-output namespace",
            ));
        }
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
    converted.branch_by = table
        .branch_by
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect();
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
    let update_using = convert_optional_policy(
        schema,
        table,
        name,
        "policies.update.using",
        table.policies.update.using.as_ref(),
    )?;
    let delete_using = if table.policies.delete.using.is_some() {
        convert_optional_policy(
            schema,
            table,
            name,
            "policies.delete.using",
            table.policies.delete.using.as_ref(),
        )?
    } else {
        update_using.clone()
    };
    converted.write_policies = WritePolicies {
        insert_check: convert_optional_policy(
            schema,
            table,
            name,
            "policies.insert.with_check",
            table.policies.insert.with_check.as_ref(),
        )?,
        update_using,
        update_check: convert_optional_policy(
            schema,
            table,
            name,
            "policies.update.with_check",
            table.policies.update.with_check.as_ref(),
        )?,
        delete_using,
    };
    Ok(converted)
}

fn convert_column(
    table: &TableName,
    column: &ColumnDescriptor,
) -> Result<CoreColumnSchema, SchemaConversionError> {
    let path = format!("$.{}.{}", table.as_str(), column.name.as_str());
    crate::tools::public_schema::validate_json_schemas(&column.column_type, column.name.as_str())
        .map_err(|message| err(path, message))?;
    let mut column_type = convert_column_type(table, column.name.as_str(), &column.column_type)?;
    if column.nullable {
        column_type = column_type.nullable();
    }
    let mut converted = CoreColumnSchema::new(column.name.as_str(), column_type);
    if matches!(column.column_type, ColumnType::Json { .. }) {
        // JSON is logically represented as Groove's canonical string value,
        // but its physical large-scalar interpretation is schema-derived and
        // deliberately distinct from text.
        converted.large_value_kind = crate::schema::LargeValueSemanticKind::Json;
    }
    if let Some(default) = &column.default {
        converted.default = Some(convert_column_default(table, column, default)?);
    }
    Ok(converted)
}

fn convert_column_default(
    table: &TableName,
    column: &ColumnDescriptor,
    value: &Value,
) -> Result<GrooveValue, SchemaConversionError> {
    crate::tools::public_schema::validate_json_value(
        value,
        &column.column_type,
        column.name.as_str(),
    )
    .map_err(|message| {
        err(
            format!("$.{}.{}", table.as_str(), column.name.as_str()),
            message,
        )
    })?;
    if matches!(value, Value::Null) {
        return Ok(GrooveValue::Nullable(None));
    }
    // Record cells carry logical signed values. Groove applies its separate
    // order-preserving transform only when those values become ordered keys.
    let default =
        convert_default_for_column_type(table, column.name.as_str(), &column.column_type, value)?;
    if column.nullable {
        Ok(GrooveValue::Nullable(Some(Box::new(default))))
    } else {
        Ok(default)
    }
}

fn convert_default_for_column_type(
    table: &TableName,
    column: &str,
    column_type: &ColumnType,
    value: &Value,
) -> Result<GrooveValue, SchemaConversionError> {
    match (column_type, value) {
        (ColumnType::Boolean, Value::Boolean(value)) => Ok(GrooveValue::Bool(*value)),
        (
            ColumnType::Text | ColumnType::Json { .. } | ColumnType::Enum { .. },
            Value::Text(value),
        ) => Ok(GrooveValue::String(value.clone())),
        (
            ColumnType::EnumPayload { cases } | ColumnType::CatalogueEnumPayload { cases, .. },
            Value::Enum { case, values },
        ) => {
            let (case_index, entry) = cases
                .iter()
                .enumerate()
                .find(|(_, entry)| entry.name == *case)
                .ok_or_else(|| {
                    err(
                        format!("$.{}.{}", table.as_str(), column),
                        format!("payload enum default case {case:?} is not declared"),
                    )
                })?;
            if entry.fields.len() != values.len() {
                return Err(err(
                    format!("$.{}.{}", table.as_str(), column),
                    "payload enum default field count does not match its case",
                ));
            }
            let mut fields = Vec::with_capacity(entry.fields.len());
            let mut converted = Vec::with_capacity(values.len());
            for (field, value) in entry.fields.iter().zip(values) {
                let mut value_type =
                    convert_column_type(table, field.name.as_str(), &field.column_type)?;
                if field.nullable {
                    value_type = value_type.nullable();
                }
                let value = if matches!(value, Value::Null) {
                    if !field.nullable {
                        return Err(err(
                            format!("$.{}.{}.{}", table.as_str(), column, field.name.as_str()),
                            "payload enum default cannot use null for a required field",
                        ));
                    }
                    GrooveValue::Nullable(None)
                } else {
                    let inner = convert_default_for_column_type(
                        table,
                        field.name.as_str(),
                        &field.column_type,
                        value,
                    )?;
                    if field.nullable {
                        GrooveValue::Nullable(Some(Box::new(inner)))
                    } else {
                        inner
                    }
                };
                fields.push((field.name.as_str().to_owned(), value_type));
                converted.push(value);
            }
            let case_tag = u32::try_from(case_index).map_err(|_| {
                err(
                    format!("$.{}.{}", table.as_str(), column),
                    "payload enum case index exceeds u32",
                )
            })?;
            EnumValue::create(case_tag, RecordDescriptor::new(fields), &converted)
                .map(GrooveValue::Enum)
                .map_err(|error| {
                    err(
                        format!("$.{}.{}", table.as_str(), column),
                        error.to_string(),
                    )
                })
        }
        (ColumnType::Timestamp, Value::Timestamp(value)) => Ok(GrooveValue::U64(*value)),
        (ColumnType::Double, Value::Double(value)) => Ok(GrooveValue::F64(*value)),
        (ColumnType::Uuid, Value::Uuid(value)) => Ok(GrooveValue::Uuid(*value.uuid())),
        (ColumnType::Bytea, Value::Bytea(value)) => Ok(GrooveValue::Bytes(value.clone())),
        (ColumnType::Integer, Value::Integer(value)) => Ok(GrooveValue::I32(*value)),
        (ColumnType::Integer, Value::BigInt(value)) => {
            i32::try_from(*value).map(GrooveValue::I32).map_err(|_| {
                err(
                    format!("$.{}.{}", table.as_str(), column),
                    format!("BIGINT default {value} is outside INTEGER range"),
                )
            })
        }
        (ColumnType::BigInt, Value::Integer(value)) => Ok(GrooveValue::I64(i64::from(*value))),
        (ColumnType::BigInt, Value::BigInt(value)) => Ok(GrooveValue::I64(*value)),
        (ColumnType::Array { element }, Value::Array(values)) => values
            .iter()
            .map(|value| convert_default_for_column_type(table, column, element.as_ref(), value))
            .collect::<Result<Vec<_>, _>>()
            .map(GrooveValue::Array),
        _ => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            format!("default {value:?} does not match column type {column_type:?}"),
        )),
    }
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
        ColumnType::Enum { .. } => Ok(GrooveColumnType::String),
        ColumnType::ScalarEnum { name, variants } => {
            groove::records::ScalarEnumSchema::new(name.clone(), variants.iter().cloned())
                .map(GrooveColumnType::EnumTag)
                .map_err(|error| {
                    err(
                        format!("$.{}.{}", table.as_str(), column),
                        error.to_string(),
                    )
                })
        }
        ColumnType::EnumPayload { cases } | ColumnType::CatalogueEnumPayload { cases, .. } => {
            let mut converted_cases = Vec::with_capacity(cases.len());
            for case in cases {
                if case.name.is_empty() {
                    return Err(err(
                        format!("$.{}.{}", table.as_str(), column),
                        "enum case names cannot be empty",
                    ));
                }
                let mut fields = Vec::with_capacity(case.fields.len());
                for field in &case.fields {
                    validate_payload_enum_field(table, column, &case.name, field)?;
                    let mut value_type =
                        convert_column_type(table, field.name.as_str(), &field.column_type)?;
                    if field.nullable {
                        value_type = value_type.nullable();
                    }
                    fields.push((field.name.as_str().to_owned(), value_type));
                }
                converted_cases.push(EnumCase::new(
                    case.name.clone(),
                    RecordDescriptor::new(fields),
                ));
            }
            let enum_name = match column_type {
                ColumnType::CatalogueEnumPayload { name, .. } => name.clone(),
                _ => format!("{}_{}", table.as_str(), column),
            };
            EnumSchema::new(enum_name, converted_cases)
                .map(|schema| GrooveColumnType::Enum(Box::new(schema)))
                .map_err(|error| {
                    err(
                        format!("$.{}.{}", table.as_str(), column),
                        error.to_string(),
                    )
                })
        }
        ColumnType::Array { element } => {
            Ok(convert_column_type(table, column, element.as_ref())?.array_of())
        }
        // Public INTEGER follows PostgreSQL semantics: signed 32-bit integer.
        ColumnType::Integer => Ok(GrooveColumnType::I32),
        // Public BIGINT follows PostgreSQL semantics: signed 64-bit integer.
        ColumnType::BigInt => Ok(GrooveColumnType::I64),
        ColumnType::TransactionId => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "TransactionId columns are not supported by core schema conversion yet",
        )),
        ColumnType::Json { .. } => Ok(GrooveColumnType::String),
        ColumnType::Row { .. } => Err(err(
            format!("$.{}.{}", table.as_str(), column),
            "nested Row columns are not supported by core schema conversion yet",
        )),
    }
}

/// Enforce the same v1 payload-enum boundary for raw public-schema ingress as
/// the TypeScript DSL. Schema JSON can bypass the DSL, so this must reject
/// unsupported layouts before recursive column conversion admits them.
fn validate_payload_enum_field(
    table: &TableName,
    column: &str,
    case: &str,
    field: &ColumnDescriptor,
) -> Result<(), SchemaConversionError> {
    let path = format!(
        "$.{}.{}.{}.{}",
        table.as_str(),
        column,
        case,
        field.name.as_str()
    );
    if field.name.as_str() == "type" {
        return Err(err(
            path,
            "enum payload field name \"type\" is reserved for the discriminant",
        ));
    }
    if field.name.as_str().starts_with('$') {
        return Err(err(
            path,
            "enum payload field names starting with \"$\" are reserved for magic columns",
        ));
    }
    if field.references.is_some() {
        return Err(err(
            path,
            "enum payload fields cannot use references; use UUID values instead",
        ));
    }
    if !matches!(
        field.column_type,
        ColumnType::Boolean
            | ColumnType::Text
            | ColumnType::Timestamp
            | ColumnType::Double
            | ColumnType::Uuid
            | ColumnType::Bytea
            | ColumnType::Json { .. }
            | ColumnType::Integer
            | ColumnType::BigInt
    ) {
        return Err(err(
            path,
            "payload enum v1 fields must be scalar columns; arrays, rows, and nested enums are not supported",
        ));
    }
    Ok(())
}

fn convert_merge_strategy(
    table: &TableName,
    column: &ColumnDescriptor,
    strategy: ColumnMergeStrategy,
) -> Result<MergeStrategy, SchemaConversionError> {
    column.validate_merge_strategy().map_err(|message| {
        err(
            format!("$.{}.{}", table.as_str(), column.name.as_str()),
            message,
        )
    })?;

    match strategy {
        ColumnMergeStrategy::Counter => Ok(MergeStrategy::Counter),
        ColumnMergeStrategy::GSet => Ok(MergeStrategy::GSet),
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
    convert_policy_with_native_select_inherits(schema, table_schema, table, path, expr, true)
}

/// Convert a policy for a location where native SELECT inherits may or may not
/// be representable. A top-level policy keeps the native atom so lowering can
/// use derivation collapse. An `INHERITS_REFERENCING` source policy is embedded
/// in a join, where that atom applies to the wrong row and must be expanded.
fn convert_policy_with_native_select_inherits(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    expr: &PolicyExpr,
    native_select_inherits: bool,
) -> Result<Query, SchemaConversionError> {
    // Do this before choosing a lowering path. In particular, an ExistsRel
    // nested below a boolean operator must not escape this validation merely
    // because that operator is rejected by a later lowering stage.
    validate_exists_rel_policy_join_conditions(table, path, expr)?;
    match expr {
        PolicyExpr::And(exprs) => {
            if !exprs.iter().any(is_core_policy_clause) {
                return query_with_predicate_filters(
                    table.as_str(),
                    predicate_filters_for_expr(table, path, expr)?,
                );
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
                    native_select_inherits,
                )?;
            }
            Ok(query)
        }
        PolicyExpr::Or(exprs) if exprs.iter().any(policy_requires_branch) => {
            let mut query = Query::from(table.as_str()).filter(Predicate::Any(Vec::new()));
            for (index, expr) in exprs.iter().enumerate() {
                let branch = convert_policy_with_native_select_inherits(
                    schema,
                    table_schema,
                    table,
                    &format!("{path}.Or[{index}]"),
                    expr,
                    native_select_inherits,
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
            max_depth,
        } => append_inherited_policy(
            schema,
            table_schema,
            table,
            path,
            Query::from(table.as_str()),
            *operation,
            via_column,
            *max_depth,
            native_select_inherits,
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
        PolicyExpr::ExistsRel { rel } => {
            append_exists_rel_policy_clause(table, path, Query::from(table.as_str()), rel)
        }
        _ => query_with_predicate_filters(
            table.as_str(),
            predicate_filters_for_expr(table, path, expr)?,
        ),
    }
}

fn is_core_policy_clause(expr: &PolicyExpr) -> bool {
    match expr {
        PolicyExpr::Inherits { max_depth: _, .. }
        | PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::Exists { .. }
        | PolicyExpr::ExistsRel { .. } => true,
        PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => exprs.iter().any(is_core_policy_clause),
        PolicyExpr::Not(expr) => is_core_policy_clause(expr),
        _ => false,
    }
}

fn policy_requires_branch(expr: &PolicyExpr) -> bool {
    match expr {
        PolicyExpr::Inherits { max_depth: _, .. }
        | PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::Exists { .. }
        | PolicyExpr::ExistsRel { .. } => true,
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
    native_select_inherits: bool,
) -> Result<Query, SchemaConversionError> {
    match expr {
        PolicyExpr::And(exprs) => {
            let mut query = query;
            for (index, expr) in exprs.iter().enumerate() {
                query = append_policy_clause(
                    schema,
                    table_schema,
                    table,
                    &format!("{path}.And[{index}]"),
                    query,
                    expr,
                    native_select_inherits,
                )?;
            }
            Ok(query)
        }
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => append_inherited_policy(
            schema,
            table_schema,
            table,
            path,
            query,
            *operation,
            via_column,
            *max_depth,
            native_select_inherits,
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
        PolicyExpr::ExistsRel { rel } => append_exists_rel_policy_clause(table, path, query, rel),
        _ => Ok(append_predicate_filters(
            query,
            predicate_filters_for_expr(table, path, expr)?,
        )),
    }
}

fn query_with_predicate_filters(
    table: &str,
    filters: Vec<Predicate>,
) -> Result<Query, SchemaConversionError> {
    Ok(append_predicate_filters(Query::from(table), filters))
}

fn append_predicate_filters(mut query: Query, filters: Vec<Predicate>) -> Query {
    for filter in filters {
        query = query.filter(filter);
    }
    query
}

fn predicate_filters_for_expr(
    table: &TableName,
    path: &str,
    expr: &PolicyExpr,
) -> Result<Vec<Predicate>, SchemaConversionError> {
    match expr {
        PolicyExpr::True => Ok(Vec::new()),
        PolicyExpr::And(exprs) => {
            let mut filters = Vec::new();
            for (index, expr) in exprs.iter().enumerate() {
                filters.extend(predicate_filters_for_expr(
                    table,
                    &format!("{path}.And[{index}]"),
                    expr,
                )?);
            }
            Ok(filters)
        }
        _ => Ok(vec![convert_policy_predicate(table, path, expr)?]),
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
            let source_query = convert_policy_with_native_select_inherits(
                schema,
                source_schema,
                &source_table_name,
                &format!("{path}.InheritsReferencing[{source_table}]"),
                source_policy,
                false,
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

fn convert_inherits_operation(operation: Operation) -> InheritsOperation {
    match operation {
        Operation::Select => InheritsOperation::Select,
        Operation::Insert => InheritsOperation::Insert,
        Operation::Update => InheritsOperation::Update,
        Operation::Delete => InheritsOperation::Delete,
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

    let mut outer_correlations = Vec::new();
    let mut filters = Vec::new();
    let mut nested_exists = Vec::new();
    let mut conditions = Vec::new();
    collect_policy_conjuncts(condition, &mut conditions);

    for (index, expr) in conditions.into_iter().enumerate() {
        match expr {
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(path_segments),
            } if path_segments.len() == 2 && path_segments[0] == "__jazz_outer_row" => {
                outer_correlations.push(JoinCorrelation {
                    join_column: column.clone(),
                    source_column: path_segments[1].clone(),
                });
            }
            PolicyExpr::Exists {
                table: nested_table,
                condition: nested_condition,
            } => nested_exists.push((index, nested_table.as_str(), nested_condition.as_ref())),
            other => filters.push(convert_policy_predicate(
                &exists_table_name,
                &format!("{path}.Exists[{index}]"),
                other,
            )?),
        }
    }

    if outer_correlations.is_empty() {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "core schema policies require EXISTS to include an equality against __jazz_outer_row",
        ));
    }
    let primary_index = outer_correlations
        .iter()
        .position(|correlation| correlation.join_column == "id")
        .unwrap_or(0);
    let primary = outer_correlations.remove(primary_index);
    let join_column = primary.join_column;
    let source_column = primary.source_column;
    let correlated_filters = outer_correlations;

    let query = if join_column == "id" {
        query.join_via_row_id_with_correlations(
            exists_table,
            source_column,
            correlated_filters,
            filters,
        )
    } else if correlated_filters.is_empty() {
        query.join_via_column(exists_table, join_column, source_column, filters)
    } else {
        query.join_via_column_with_correlations(
            exists_table,
            join_column,
            source_column,
            correlated_filters,
            filters,
        )
    };

    // Legacy `Exists` has one outer-row scope, even when an all-of nests
    // another EXISTS. Each nested existential is therefore an additional
    // proof about the protected row, not a join relative to the first proof
    // row. Lower it through this same correlated-join path so every declared
    // FK edge remains independently validated by the core query contract.
    nested_exists
        .into_iter()
        .try_fold(query, |query, (index, nested_table, nested_condition)| {
            append_exists_policy_clause(
                schema,
                table,
                &format!("{path}.Exists[{index}]"),
                query,
                nested_table,
                nested_condition,
            )
        })
}

fn collect_policy_conjuncts<'a>(expr: &'a PolicyExpr, output: &mut Vec<&'a PolicyExpr>) {
    match expr {
        PolicyExpr::And(children) => {
            for child in children {
                collect_policy_conjuncts(child, output);
            }
        }
        other => output.push(other),
    }
}

#[derive(Clone)]
struct LoweredRelPredicate {
    predicate: Predicate,
    column: Option<String>,
    value: Option<LoweredRelValue>,
}

#[derive(Clone)]
enum LoweredRelValue {
    Operand(Operand),
    OuterRow(String),
    FrontierRow,
}

#[derive(Clone)]
struct PendingReachable {
    from: Operand,
    seed: Option<crate::query::ReachableSeed>,
    edge_table: String,
    edge_member_column: String,
    edge_parent_column: String,
    edge_filters: Vec<Predicate>,
    max_depth: usize,
}

struct LoweredRel {
    table: String,
    filters: Vec<LoweredRelPredicate>,
    joins: Vec<JoinVia>,
    reachable: Vec<crate::query::ReachableVia>,
    pending_reachable: Option<PendingReachable>,
}

fn validate_exists_rel_join_conditions(
    table: &TableName,
    path: &str,
    rel: &RelExpr,
) -> Result<(), SchemaConversionError> {
    match rel {
        RelExpr::TableScan { .. } => Ok(()),
        RelExpr::Filter { input, .. } | RelExpr::Project { input, .. } => {
            validate_exists_rel_join_conditions(table, path, input)
        }
        RelExpr::Union { inputs } => {
            for input in inputs {
                validate_exists_rel_join_conditions(table, path, input)?;
            }
            Ok(())
        }
        RelExpr::Join {
            left, right, on, ..
        } => {
            if on.len() > 1 {
                return Err(err(
                    format!("$.{}.{}", table.as_str(), path),
                    "core schema ExistsRel joins support exactly one column equality",
                ));
            }
            validate_exists_rel_join_conditions(table, path, left)?;
            validate_exists_rel_join_conditions(table, path, right)
        }
        RelExpr::Gather { seed, step, .. } => {
            validate_exists_rel_join_conditions(table, path, seed)?;
            validate_exists_rel_join_conditions(table, path, step)
        }
    }
}

fn validate_exists_rel_policy_join_conditions(
    table: &TableName,
    path: &str,
    expr: &PolicyExpr,
) -> Result<(), SchemaConversionError> {
    match expr {
        PolicyExpr::ExistsRel { rel } => validate_exists_rel_join_conditions(table, path, rel),
        PolicyExpr::And(exprs) => {
            for (index, expr) in exprs.iter().enumerate() {
                validate_exists_rel_policy_join_conditions(
                    table,
                    &format!("{path}.And[{index}]"),
                    expr,
                )?;
            }
            Ok(())
        }
        PolicyExpr::Or(exprs) => {
            for (index, expr) in exprs.iter().enumerate() {
                validate_exists_rel_policy_join_conditions(
                    table,
                    &format!("{path}.Or[{index}]"),
                    expr,
                )?;
            }
            Ok(())
        }
        PolicyExpr::Not(expr) => {
            validate_exists_rel_policy_join_conditions(table, &format!("{path}.Not"), expr)
        }
        _ => Ok(()),
    }
}

fn append_exists_rel_policy_clause(
    table: &TableName,
    path: &str,
    mut query: Query,
    rel: &RelExpr,
) -> Result<Query, SchemaConversionError> {
    let mut lowered = lower_exists_rel(table, path, rel)?;
    let correlation_index = lowered
        .filters
        .iter()
        .position(|filter| {
            matches!(filter.value, Some(LoweredRelValue::OuterRow(_)))
                && filter.column.as_deref() == Some("id")
        })
        .or_else(|| {
            lowered
                .filters
                .iter()
                .position(|filter| matches!(filter.value, Some(LoweredRelValue::OuterRow(_))))
        })
        .ok_or_else(|| {
            err(
                format!("$.{}.{}", table.as_str(), path),
                "core schema ExistsRel policies must include an outer row equality",
            )
        })?;
    let correlation = lowered.filters.remove(correlation_index);
    let Some(correlation_column) = correlation.column.clone() else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "core schema ExistsRel policies must correlate a concrete column",
        ));
    };
    let Some(LoweredRelValue::OuterRow(source_column)) = correlation.value.clone() else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "core schema ExistsRel policies must correlate to an outer row reference",
        ));
    };

    // An ExistsRel can correlate more than one key from the protected row.
    // Keep every extra outer-row equality as part of the join predicate;
    // lowering it as the placeholder `Predicate::All([])` would otherwise
    // prove the referenced row but not that it belongs to the same workspace.
    let mut correlated_filters = Vec::new();
    let mut remaining_filters = Vec::new();
    for filter in &lowered.filters {
        match (&filter.column, &filter.value) {
            (Some(join_column), Some(LoweredRelValue::OuterRow(source_column))) => {
                correlated_filters.push(JoinCorrelation {
                    join_column: join_column.clone(),
                    source_column: source_column.clone(),
                });
            }
            _ => remaining_filters.push(filter.predicate.clone()),
        }
    }

    for reachable in &mut lowered.reachable {
        if reachable.access_row_column == "__pending_outer_row" {
            reachable.access_row_column = correlation_column.clone();
            reachable.access_filters.extend(remaining_filters.clone());
        }
    }
    if let Some(pending) = lowered.pending_reachable.take() {
        lowered.reachable.push(crate::query::ReachableVia {
            access_table: table.as_str().to_owned(),
            access_row_column: "id".to_owned(),
            access_team_column: source_column.clone(),
            access_team_target: if source_column == "id" {
                JoinTarget::RowId
            } else {
                JoinTarget::Column
            },
            from: pending.from,
            access_filters: remaining_filters.clone(),
            edge_table: pending.edge_table,
            edge_member_column: pending.edge_member_column,
            edge_parent_column: pending.edge_parent_column,
            edge_filters: pending.edge_filters,
            bound: crate::query::RecursionBound::MaxDepth(pending.max_depth),
            seed: pending.seed,
        });
    }
    if !lowered.reachable.is_empty() {
        for reachable in lowered.reachable {
            query.reachable.push(reachable);
        }
        return Ok(query);
    }

    if !lowered.joins.is_empty() {
        // The relation's left-most scan is the row correlated to the
        // protected row. Its joins remain nested beneath that root; replacing
        // it with the first nested join loses the FK relation and produces a
        // `JoinNotRefCompatible` plan (for example task.block -> member.id).
        // Lift outer correlations on a nested join through its equality edge,
        // so `member.workspace = outer.workspace` constrains the root block's
        // workspace without publishing any proof carrier.
        for nested in &mut lowered.joins {
            if nested
                .nested_joins
                .iter()
                .any(|child| !child.correlated_filters.is_empty())
            {
                return Err(err(
                    format!("$.{}.{}", table.as_str(), path),
                    "core schema ExistsRel does not yet support outer correlations beyond one nested join",
                ));
            }
            let source_column = nested.source_column.clone().ok_or_else(|| {
                err(
                    format!("$.{}.{}", table.as_str(), path),
                    "core schema ExistsRel nested join is missing its source column",
                )
            })?;
            for correlation in std::mem::take(&mut nested.correlated_filters) {
                if correlation.join_column != nested.on_column {
                    return Err(err(
                        format!("$.{}.{}", table.as_str(), path),
                        "core schema ExistsRel nested outer correlation must use its join key",
                    ));
                }
                correlated_filters.push(JoinCorrelation {
                    join_column: source_column.clone(),
                    source_column: correlation.source_column,
                });
            }
        }
        query.joins.push(JoinVia {
            table: lowered.table,
            on_column: correlation_column.clone(),
            target: if correlation_column == "id" {
                JoinTarget::RowId
            } else {
                JoinTarget::Column
            },
            source_column: Some(source_column),
            source_lookup: None,
            correlated_filters,
            filters: remaining_filters,
            nested_joins: lowered.joins,
        });
        return Ok(query);
    }

    let filters = lowered
        .filters
        .into_iter()
        .map(|filter| filter.predicate)
        .collect::<Vec<_>>();
    if correlation_column == "id" {
        Ok(query.join_via_row_id_with_correlations(
            lowered.table,
            source_column,
            correlated_filters,
            filters,
        ))
    } else {
        Ok(query.join_via_column_with_correlations(
            lowered.table,
            correlation_column,
            source_column,
            correlated_filters,
            filters,
        ))
    }
}

fn lower_exists_rel(
    table: &TableName,
    path: &str,
    rel: &RelExpr,
) -> Result<LoweredRel, SchemaConversionError> {
    match rel {
        RelExpr::TableScan { table, .. } => Ok(LoweredRel {
            table: table.as_str().to_owned(),
            filters: Vec::new(),
            joins: Vec::new(),
            reachable: Vec::new(),
            pending_reachable: None,
        }),
        RelExpr::Filter { input, predicate } => {
            let mut lowered = lower_exists_rel(table, path, input)?;
            lowered
                .filters
                .extend(rel_predicate_to_policy(table, path, predicate)?);
            Ok(lowered)
        }
        RelExpr::Project { input, .. } => lower_exists_rel(table, path, input),
        RelExpr::Gather {
            seed, step, bound, ..
        } => lower_gather_rel(table, path, seed, step, bound),
        RelExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => {
            if *join_kind != RelJoinKind::Inner {
                return Err(err(
                    format!("$.{}.{}", table.as_str(), path),
                    "core schema ExistsRel policies only support inner joins",
                ));
            }
            let mut left = lower_exists_rel(table, path, left)?;
            let right = lower_exists_rel(table, path, right)?;
            let Some(on) = on.first() else {
                return Err(err(
                    format!("$.{}.{}", table.as_str(), path),
                    "core schema ExistsRel joins require a column equality",
                ));
            };
            if let Some(pending) = left.pending_reachable.take() {
                if on.left.column != "id" {
                    return Err(err(
                        format!("$.{}.{}", table.as_str(), path),
                        "core schema ExistsRel reachable joins must join from reachable id",
                    ));
                }
                let mut reachable = crate::query::ReachableVia {
                    access_table: right.table,
                    access_row_column: "__pending_outer_row".to_owned(),
                    access_team_column: on.right.column.clone(),
                    access_team_target: if on.right.column == "id" {
                        JoinTarget::RowId
                    } else {
                        JoinTarget::Column
                    },
                    from: pending.from,
                    access_filters: right
                        .filters
                        .iter()
                        .map(|filter| filter.predicate.clone())
                        .collect(),
                    edge_table: pending.edge_table,
                    edge_member_column: pending.edge_member_column,
                    edge_parent_column: pending.edge_parent_column,
                    edge_filters: pending.edge_filters,
                    bound: crate::query::RecursionBound::MaxDepth(pending.max_depth),
                    seed: pending.seed,
                };
                reachable
                    .access_filters
                    .extend(left.filters.iter().map(|filter| filter.predicate.clone()));
                return Ok(LoweredRel {
                    table: left.table,
                    filters: Vec::new(),
                    joins: left.joins,
                    reachable: {
                        let mut reachables = left.reachable;
                        reachables.extend(right.reachable);
                        reachables.push(reachable);
                        reachables
                    },
                    pending_reachable: None,
                });
            }

            let mut correlated_filters = Vec::new();
            let filters = right
                .filters
                .into_iter()
                .filter_map(|filter| match (filter.column, filter.value) {
                    (Some(join_column), Some(LoweredRelValue::OuterRow(source_column))) => {
                        correlated_filters.push(JoinCorrelation {
                            join_column,
                            source_column,
                        });
                        None
                    }
                    _ => Some(filter.predicate),
                })
                .collect();
            let join = JoinVia {
                table: right.table,
                on_column: on.right.column.clone(),
                target: if on.right.column == "id" {
                    JoinTarget::RowId
                } else {
                    JoinTarget::Column
                },
                source_column: Some(on.left.column.clone()),
                source_lookup: None,
                correlated_filters,
                filters,
                nested_joins: right.joins,
            };
            left.joins.push(join);
            left.reachable.extend(right.reachable);
            Ok(left)
        }
        RelExpr::Union { .. } => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "core schema ExistsRel policies do not support Union yet",
        )),
    }
}

fn lower_gather_rel(
    table: &TableName,
    path: &str,
    seed: &RelExpr,
    step: &RelExpr,
    bound: &RelRecursionBound,
) -> Result<LoweredRel, SchemaConversionError> {
    let (from, seed) = lower_gather_seed(table, path, seed)?;
    let (edge_table, output_table, edge_member_column, edge_parent_column, edge_filters) =
        lower_gather_step(table, path, step)?;
    let seed = if seed.table == output_table
        && seed.user_column.as_deref() == Some("id")
        && seed.team_column == "id"
        && seed.filters.is_empty()
    {
        None
    } else {
        Some(seed)
    };
    let max_depth = match bound {
        RelRecursionBound::MaxDepth(depth) if *depth > 0 => *depth,
        RelRecursionBound::MaxDepth(_) => {
            return Err(err(
                format!("$.{}.{}", table.as_str(), path),
                "Gather relation policies require a positive MaxDepth",
            ));
        }
        RelRecursionBound::Fixpoint => {
            return Err(err(
                format!("$.{}.{}", table.as_str(), path),
                "server shell public schema conversion does not support Gather Fixpoint yet",
            ));
        }
    };
    Ok(LoweredRel {
        table: output_table,
        filters: Vec::new(),
        joins: Vec::new(),
        reachable: Vec::new(),
        pending_reachable: Some(PendingReachable {
            from,
            seed,
            edge_table,
            edge_member_column,
            edge_parent_column,
            edge_filters,
            max_depth,
        }),
    })
}

fn lower_gather_seed(
    table: &TableName,
    path: &str,
    seed: &RelExpr,
) -> Result<(Operand, crate::query::ReachableSeed), SchemaConversionError> {
    let (input, projected_team_column, projected_filters) =
        unwrap_seed_projection(table, path, seed)?;
    let (input, filters) = unwrap_rel_filter(input);
    let filters = filters
        .into_iter()
        .chain(projected_filters)
        .collect::<Vec<_>>();
    let RelExpr::TableScan {
        table: seed_table, ..
    } = input
    else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather seeded reachability requires a filtered seed table scan",
        ));
    };
    let lowered_filters = rel_predicates_to_policy(table, path, &filters)?;
    let seed_index = lowered_filters
        .iter()
        .position(|filter| {
            matches!(
                filter.value,
                Some(LoweredRelValue::Operand(Operand::Claim(_)))
            )
        })
        .ok_or_else(|| {
            err(
                format!("$.{}.{}", table.as_str(), path),
                "Gather same-table seeds require one claim-keyed equality filter",
            )
        })?;
    let seed_filter = &lowered_filters[seed_index];
    let Some(user_column) = seed_filter.column.clone() else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather seed claim filter must name a seed-table column",
        ));
    };
    let Some(LoweredRelValue::Operand(Operand::Claim(user_claim))) = seed_filter.value.clone()
    else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather seed claim filter must compare against a claim",
        ));
    };
    let filters = lowered_filters
        .into_iter()
        .enumerate()
        .filter_map(|(index, filter)| (index != seed_index).then_some(filter.predicate))
        .collect();
    Ok((
        Operand::Claim(user_claim.clone()),
        crate::query::ReachableSeed {
            table: seed_table.as_str().to_owned(),
            user_column: Some(user_column),
            user_claim: Some(user_claim),
            team_column: projected_team_column.unwrap_or_else(|| "id".to_owned()),
            filters,
        },
    ))
}

fn unwrap_seed_projection<'a>(
    table: &TableName,
    path: &str,
    seed: &'a RelExpr,
) -> Result<(&'a RelExpr, Option<String>, Vec<&'a RelPredicateExpr>), SchemaConversionError> {
    let RelExpr::Project { input, columns } = seed else {
        return Ok((seed, None, Vec::new()));
    };
    let Some(column) = columns.iter().find(|column| column.alias == "id") else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather projected seed must expose an id column",
        ));
    };
    let RelProjectExpr::Column(projected) = &column.expr else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather projected seed id must be a column projection",
        ));
    };
    let (project_input, filters) = unwrap_rel_filter(input);
    if let RelExpr::Join {
        left, right, on, ..
    } = project_input
    {
        let (left, team_column) =
            unwrap_joined_seed_projection(table, path, left, right, on, projected)?;
        return Ok((left, team_column, filters));
    }
    Ok((input.as_ref(), Some(projected.column.clone()), Vec::new()))
}

fn unwrap_joined_seed_projection<'a>(
    table: &TableName,
    path: &str,
    left: &'a RelExpr,
    right: &RelExpr,
    on: &[crate::tools::public_api::relation_ir::JoinCondition],
    projected: &ColumnRef,
) -> Result<(&'a RelExpr, Option<String>), SchemaConversionError> {
    let RelExpr::TableScan {
        alias: right_alias, ..
    } = right
    else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather projected seed hop must join to a table scan",
        ));
    };
    let Some(join) = on.first() else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather projected seed hop requires a join condition",
        ));
    };
    if projected.scope.as_ref() != right_alias.as_ref() || projected.column != "id" {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather projected seed id must come from the hop target row id",
        ));
    }
    Ok((left, Some(join.left.column.clone())))
}

fn lower_gather_step(
    table: &TableName,
    path: &str,
    step: &RelExpr,
) -> Result<(String, String, String, String, Vec<Predicate>), SchemaConversionError> {
    let RelExpr::Project { input, .. } = step else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather policies require projected recursive hops",
        ));
    };
    let RelExpr::Join {
        left, right, on, ..
    } = input.as_ref()
    else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather policies require recursive hop joins",
        ));
    };
    let (edge_input, filters) = unwrap_rel_filter(left);
    let RelExpr::TableScan {
        table: edge_table, ..
    } = edge_input
    else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather recursive hop must start from an edge table scan",
        ));
    };
    let RelExpr::TableScan {
        table: output_table,
        ..
    } = right.as_ref()
    else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather recursive hop must join to the output table",
        ));
    };
    let lowered_filters = rel_predicates_to_policy(table, path, &filters)?;
    let frontier_index = lowered_filters
        .iter()
        .position(|filter| matches!(filter.value, Some(LoweredRelValue::FrontierRow)))
        .ok_or_else(|| {
            err(
                format!("$.{}.{}", table.as_str(), path),
                "Gather recursive hop requires a frontier equality",
            )
        })?;
    let frontier = &lowered_filters[frontier_index];
    let Some(edge_member_column) = frontier.column.clone() else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather frontier equality must name an edge column",
        ));
    };
    let Some(on) = on.first() else {
        return Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "Gather recursive hop join requires a column equality",
        ));
    };
    let edge_filters = lowered_filters
        .into_iter()
        .enumerate()
        .filter_map(|(index, filter)| (index != frontier_index).then_some(filter.predicate))
        .collect();
    Ok((
        edge_table.as_str().to_owned(),
        output_table.as_str().to_owned(),
        edge_member_column,
        on.left.column.clone(),
        edge_filters,
    ))
}

fn unwrap_rel_filter(rel: &RelExpr) -> (&RelExpr, Vec<&RelPredicateExpr>) {
    match rel {
        RelExpr::Filter { input, predicate } => (input.as_ref(), vec![predicate]),
        other => (other, Vec::new()),
    }
}

fn rel_predicate_to_policy(
    table: &TableName,
    path: &str,
    predicate: &RelPredicateExpr,
) -> Result<Vec<LoweredRelPredicate>, SchemaConversionError> {
    match predicate {
        RelPredicateExpr::True => Ok(Vec::new()),
        RelPredicateExpr::False => Ok(vec![LoweredRelPredicate {
            predicate: Predicate::Any(Vec::new()),
            column: None,
            value: None,
        }]),
        RelPredicateExpr::And(children) => {
            let mut lowered = Vec::new();
            for child in children {
                lowered.extend(rel_predicate_to_policy(table, path, child)?);
            }
            Ok(lowered)
        }
        RelPredicateExpr::Or(children) => {
            let predicates = children
                .iter()
                .map(|child| {
                    rel_predicate_to_policy(table, path, child).map(|parts| {
                        Predicate::All(parts.into_iter().map(|part| part.predicate).collect())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(vec![LoweredRelPredicate {
                predicate: Predicate::Any(predicates),
                column: None,
                value: None,
            }])
        }
        RelPredicateExpr::Not(child) => {
            let predicates = rel_predicate_to_policy(table, path, child)?
                .into_iter()
                .map(|part| part.predicate)
                .collect::<Vec<_>>();
            Ok(vec![LoweredRelPredicate {
                predicate: Predicate::Not(Box::new(Predicate::All(predicates))),
                column: None,
                value: None,
            }])
        }
        RelPredicateExpr::Cmp { left, op, right } => {
            let value = rel_value_to_policy_operand(table, path, right)?;
            let predicate = match (&value, op) {
                (LoweredRelValue::Operand(operand), RelPredicateCmpOp::Eq) => {
                    Predicate::Eq(Operand::Column(left.column.clone()), operand.clone())
                }
                (LoweredRelValue::Operand(operand), RelPredicateCmpOp::Ne) => {
                    Predicate::Ne(Operand::Column(left.column.clone()), operand.clone())
                }
                (LoweredRelValue::Operand(operand), RelPredicateCmpOp::Lt) => {
                    Predicate::Lt(Operand::Column(left.column.clone()), operand.clone())
                }
                (LoweredRelValue::Operand(operand), RelPredicateCmpOp::Le) => {
                    Predicate::Lte(Operand::Column(left.column.clone()), operand.clone())
                }
                (LoweredRelValue::Operand(operand), RelPredicateCmpOp::Gt) => {
                    Predicate::Gt(Operand::Column(left.column.clone()), operand.clone())
                }
                (LoweredRelValue::Operand(operand), RelPredicateCmpOp::Ge) => {
                    Predicate::Gte(Operand::Column(left.column.clone()), operand.clone())
                }
                (
                    LoweredRelValue::OuterRow(_) | LoweredRelValue::FrontierRow,
                    RelPredicateCmpOp::Eq,
                ) => Predicate::All(Vec::new()),
                _ => {
                    return Err(err(
                        format!("$.{}.{}", table.as_str(), path),
                        "core schema ExistsRel special row-id comparisons only support equality",
                    ));
                }
            };
            Ok(vec![LoweredRelPredicate {
                predicate,
                column: Some(left.column.clone()),
                value: Some(value),
            }])
        }
        RelPredicateExpr::IsNull { column } => Ok(vec![LoweredRelPredicate {
            predicate: Predicate::IsNull(Operand::Column(column.column.clone())),
            column: Some(column.column.clone()),
            value: None,
        }]),
        RelPredicateExpr::IsNotNull { column } => Ok(vec![LoweredRelPredicate {
            predicate: Predicate::Not(Box::new(Predicate::IsNull(Operand::Column(
                column.column.clone(),
            )))),
            column: Some(column.column.clone()),
            value: None,
        }]),
        RelPredicateExpr::Contains { left, right } => {
            let LoweredRelValue::Operand(operand) =
                rel_value_to_policy_operand(table, path, right)?
            else {
                return Err(err(
                    format!("$.{}.{}", table.as_str(), path),
                    "core schema ExistsRel Contains does not support row-id operands",
                ));
            };
            Ok(vec![LoweredRelPredicate {
                predicate: Predicate::Contains(Operand::Column(left.column.clone()), operand),
                column: Some(left.column.clone()),
                value: None,
            }])
        }
        RelPredicateExpr::EnumMatch {
            column,
            case,
            payload,
        } => {
            let payload = rel_predicate_to_policy(table, path, payload)?;
            Ok(vec![LoweredRelPredicate {
                predicate: Predicate::EnumMatch {
                    column: column.column.clone(),
                    case: case.clone(),
                    payload: Box::new(Predicate::All(
                        payload.into_iter().map(|part| part.predicate).collect(),
                    )),
                },
                column: Some(column.column.clone()),
                value: None,
            }])
        }
        RelPredicateExpr::In { left, values } => {
            let values = values
                .iter()
                .map(
                    |value| match rel_value_to_policy_operand(table, path, value)? {
                        LoweredRelValue::Operand(operand) => Ok(operand),
                        _ => Err(err(
                            format!("$.{}.{}", table.as_str(), path),
                            "core schema ExistsRel In does not support row-id operands",
                        )),
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;
            Ok(vec![LoweredRelPredicate {
                predicate: Predicate::In(Operand::Column(left.column.clone()), values),
                column: Some(left.column.clone()),
                value: None,
            }])
        }
    }
}

fn rel_predicates_to_policy(
    table: &TableName,
    path: &str,
    predicates: &[&RelPredicateExpr],
) -> Result<Vec<LoweredRelPredicate>, SchemaConversionError> {
    let mut lowered = Vec::new();
    for predicate in predicates {
        lowered.extend(rel_predicate_to_policy(table, path, predicate)?);
    }
    Ok(lowered)
}

fn rel_value_to_policy_operand(
    table: &TableName,
    path: &str,
    value: &RelValueRef,
) -> Result<LoweredRelValue, SchemaConversionError> {
    match value {
        RelValueRef::Literal(value) => Ok(LoweredRelValue::Operand(Operand::Literal(
            convert_policy_literal(table, path, value)?,
        ))),
        RelValueRef::SessionRef(path_segments) => Ok(LoweredRelValue::Operand(
            convert_session_path_operand(table, path, path_segments)?,
        )),
        RelValueRef::OuterColumn(ColumnRef { column, .. }) => {
            Ok(LoweredRelValue::OuterRow(column.clone()))
        }
        RelValueRef::RowId(RowIdRef::Outer) => Ok(LoweredRelValue::OuterRow("id".to_owned())),
        RelValueRef::RowId(RowIdRef::Frontier) => Ok(LoweredRelValue::FrontierRow),
        RelValueRef::RowId(RowIdRef::Current) => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "core schema ExistsRel policies do not support Current row-id operands here",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn append_inherited_policy(
    schema: &Schema,
    table_schema: &TableSchema,
    table: &TableName,
    path: &str,
    query: Query,
    operation: Operation,
    via_column: &str,
    max_depth: Option<usize>,
    native_select_inherits: bool,
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
    // A top-level SELECT inherit lowers through derivation collapse. When its
    // policy is embedded under INHERITS_REFERENCING, though, an inherit atom
    // would be evaluated against the outer row; expand it into the source join
    // chain instead.
    if operation == Operation::Select && !native_select_inherits {
        if max_depth.is_some() {
            return Err(err(
                format!("$.{}.{}", table.as_str(), path),
                "bounded SELECT INHERITS under INHERITS_REFERENCING is unsupported because its expanded join chain cannot preserve max_depth",
            ));
        }
        return append_inherited_policy_expanded_fallback(
            schema,
            parent_schema,
            table,
            path,
            query,
            parent_table,
            via_column,
            parent_policy,
        );
    }

    Ok(match max_depth {
        Some(max_depth) => query.inherits_operation_with_depth(
            via_column,
            convert_inherits_operation(operation),
            max_depth,
        ),
        None => query.inherits_operation(via_column, convert_inherits_operation(operation)),
    })
}

#[allow(clippy::too_many_arguments)]
fn append_inherited_policy_expanded_fallback(
    schema: &Schema,
    parent_schema: &TableSchema,
    table: &TableName,
    path: &str,
    query: Query,
    parent_table: &TableName,
    via_column: &str,
    parent_policy: &PolicyExpr,
) -> Result<Query, SchemaConversionError> {
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
            let parent_query = convert_policy_with_native_select_inherits(
                schema,
                parent_schema,
                parent_table,
                &format!("{path}.Inherits[{parent_table}]"),
                parent_policy,
                false,
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
            source_lookup,
            correlated_filters,
            filters,
            nested_joins,
        } = join;
        let source_column = source_lookup
            .as_ref()
            .map(|lookup| lookup.row_id_source_column.clone())
            .or(source_column);
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
            if matches!(value, PolicyValue::Literal(Value::Null)) {
                // Match `session_where`: equality against the public NULL
                // literal is the ergonomic spelling of a null test.
                match op {
                    CmpOp::Eq => return Ok(Predicate::IsNull(left)),
                    CmpOp::Ne => {
                        return Ok(Predicate::Not(Box::new(Predicate::IsNull(left))));
                    }
                    CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {}
                }
            }
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
        PolicyExpr::SessionCmp {
            path: path_segments,
            op,
            value,
        } => {
            let left = convert_session_path_operand(table, path, path_segments)?;
            let right = Operand::Literal(convert_policy_literal(table, path, value)?);
            Ok(match op {
                CmpOp::Eq => Predicate::Eq(left, right),
                CmpOp::Ne => Predicate::Ne(left, right),
                CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                    return Err(err(
                        format!("$.{}.{}", table.as_str(), path),
                        "core schema policies only support Eq/Ne SessionCmp operators yet",
                    ));
                }
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
            .map(|values| Predicate::In(Operand::Column(column.clone()), values)),
        PolicyExpr::SessionInList {
            path: path_segments,
            values,
        } => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                convert_policy_literal(table, &format!("{path}.SessionInList[{index}]"), value)
                    .map(Operand::Literal)
            })
            .collect::<Result<Vec<_>, _>>()
            .and_then(|values| {
                convert_session_path_operand(table, path, path_segments)
                    .map(|operand| Predicate::In(operand, values))
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
    if path_segments.len() == 1 && PUBLIC_USER_SESSION_PATHS.contains(&path_segments[0].as_str()) {
        return Ok(Operand::Claim(DIRECT_USER_CLAIM.to_owned()));
    }
    if path_segments.len() == 1
        && PUBLIC_AUTH_MODE_SESSION_PATHS.contains(&path_segments[0].as_str())
    {
        return Ok(Operand::Claim(DIRECT_AUTH_MODE_CLAIM.to_owned()));
    }
    if path_segments.len() == 2 && path_segments[0] == "claims" {
        return Ok(Operand::Claim(provider_claim_operand_key(
            &path_segments[1],
        )));
    }
    Err(err(
        format!("$.{}.{}", table.as_str(), path),
        format!(
            "core schema policies only support session.user, session.authMode, and raw provider claims through session.claims[\"name\"]; got session.{}",
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
        Value::Integer(value) => Ok(GrooveValue::I32(*value)),
        Value::BigInt(value) => Ok(GrooveValue::I64(*value)),
        Value::Double(value) if value.is_finite() => Ok(GrooveValue::F64(*value)),
        Value::Double(_) => Err(err(
            format!("$.{}.{}", table.as_str(), path),
            "core schema policy floating-point literals must be finite numbers",
        )),
        Value::Timestamp(value) => Ok(GrooveValue::U64(*value)),
        Value::Uuid(value) => Ok(GrooveValue::Uuid(*value.uuid())),
        Value::Bytea(value) => Ok(GrooveValue::Bytes(value.clone())),
        Value::Array(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                convert_policy_literal(table, &format!("{path}.Array[{index}]"), value)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(GrooveValue::Array),
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
    use crate::query::{InheritsOperation, JoinTarget, Operand, Predicate};
    use crate::tools::object::ObjectId;
    use crate::tools::public_api::policy::{CmpOp, PolicyValue};
    use crate::tools::public_api::relation_ir::{
        ColumnRef as RelColumnRef, JoinCondition as RelJoinCondition, JoinKind as RelJoinKind,
        KeyRef as RelKeyRef, PredicateCmpOp as RelPredicateCmpOp,
        PredicateExpr as RelPredicateExpr, RecursionBound as RelRecursionBound,
        RelExpr as PublicRelExpr, RowIdRef as RelRowIdRef, ValueRef as RelValueRef,
    };
    #[test]
    fn rejects_user_columns_in_the_compiler_aggregate_namespace() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("metrics")
                    .column("__jazz_aggregate_sum_score", ColumnType::Integer),
            )
            .build();
        let error =
            convert_public_schema(&schema).expect_err("reserved column must fail conversion");
        assert!(
            error
                .to_string()
                .contains("reserved aggregate-output namespace")
        );
    }

    use crate::tools::public_api::types::EnumCaseDescriptor;
    use crate::tools::public_api::types::TableSchemaBuilder;
    use crate::tools::public_schema::{
        ColumnDescriptor, ColumnType, PolicyExpr, RowDescriptor, Schema, SchemaBuilder,
        TablePolicies, TableSchema,
    };
    use std::path::PathBuf;
    use uuid::Uuid;

    fn schema_with_counter_column(column: ColumnDescriptor) -> Schema {
        Schema::from([(
            TableName::new("counters"),
            TableSchema::new(RowDescriptor::new(vec![
                column.merge_strategy(ColumnMergeStrategy::Counter),
            ])),
        )])
    }

    #[test]
    fn converts_counter_merge_strategy_on_integer_columns() {
        let schema =
            schema_with_counter_column(ColumnDescriptor::new("integer_value", ColumnType::Integer));

        let converted =
            convert_public_schema(&schema).expect("integer counter column must convert");

        assert_eq!(
            converted.tables[0].merge_strategy("integer_value"),
            MergeStrategy::Counter
        );
    }

    #[test]
    fn converts_counter_merge_strategy_on_bigint_columns() {
        let schema =
            schema_with_counter_column(ColumnDescriptor::new("bigint_value", ColumnType::BigInt));

        let converted = convert_public_schema(&schema).expect("bigint counter column must convert");

        assert_eq!(
            converted.tables[0].merge_strategy("bigint_value"),
            MergeStrategy::Counter
        );
    }

    #[test]
    fn rejects_counter_merge_strategy_on_nullable_integer_columns_with_exact_path() {
        let cases = [
            (
                ColumnDescriptor::new("nullable_integer", ColumnType::Integer).nullable(),
                "$.counters.nullable_integer: Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns",
            ),
            (
                ColumnDescriptor::new("nullable_bigint", ColumnType::BigInt).nullable(),
                "$.counters.nullable_bigint: Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns",
            ),
        ];

        for (column, expected_error) in cases {
            let schema = schema_with_counter_column(column);
            let error = convert_public_schema(&schema)
                .expect_err("nullable counter column must fail conversion");

            assert_eq!(error.to_string(), expected_error);
        }
    }

    #[test]
    fn rejects_counter_merge_strategy_on_non_integer_columns_with_exact_path() {
        let cases = [
            (
                ColumnDescriptor::new("timestamp_value", ColumnType::Timestamp),
                "$.counters.timestamp_value: Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns",
            ),
            (
                ColumnDescriptor::new("text_value", ColumnType::Text),
                "$.counters.text_value: Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns",
            ),
            (
                ColumnDescriptor::new("double_value", ColumnType::Double),
                "$.counters.double_value: Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns",
            ),
            (
                ColumnDescriptor::new(
                    "array_value",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                ),
                "$.counters.array_value: Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns",
            ),
        ];

        for (column, expected_error) in cases {
            let schema = schema_with_counter_column(column);
            let error = convert_public_schema(&schema)
                .expect_err("non-integer counter column must fail conversion");

            assert_eq!(error.to_string(), expected_error);
        }
    }

    #[test]
    fn compiles_branch_columns_with_the_same_name_and_type_across_tables() {
        let todos = TableSchemaBuilder::new("todos")
            .column("workspace_id", ColumnType::Uuid)
            .branch_by("workspace_id")
            .build();
        let notes = TableSchemaBuilder::new("notes")
            .column("workspace_id", ColumnType::Uuid)
            .branch_by("workspace_id")
            .build();
        let schema = Schema::from([
            (TableName::new("todos"), todos),
            (TableName::new("notes"), notes),
        ]);

        let converted = convert_public_schema(&schema).expect("branch columns compile");
        assert_eq!(
            converted
                .tables
                .iter()
                .map(|table| table.branch_by.len())
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn rejects_same_named_branch_columns_with_different_types() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("workspace_id", ColumnType::Uuid)
                    .branch_by("workspace_id"),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("workspace_id", ColumnType::Integer)
                    .branch_by("workspace_id"),
            )
            .build();

        let error = convert_public_schema(&schema).expect_err("conflicting types must fail");
        assert!(error.to_string().contains("same type in every table"));
    }

    #[test]
    fn rejects_missing_and_non_key_encodable_branch_columns() {
        let missing_column = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("todos").branch_by("workspace_id"))
            .build();
        assert!(
            convert_public_schema(&missing_column)
                .expect_err("missing column must fail")
                .to_string()
                .contains("unknown branch column")
        );

        let boolean_column = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("workspace_id", ColumnType::Boolean)
                    .branch_by("workspace_id"),
            )
            .build();
        assert!(
            convert_public_schema(&boolean_column)
                .expect_err("boolean branch column must fail")
                .to_string()
                .contains("fixed-width integer values")
        );
    }

    #[test]
    fn accepts_string_branch_columns() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch", ColumnType::Text)
                    .branch_by("branch"),
            )
            .build();

        convert_public_schema(&schema).expect("string branch column must compile");
    }

    fn policy_graph_perf_fixture_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../packages/jazz-tools/src/testing/fixtures/policy-graph-perf")
    }

    fn policy_graph_perf_fixture_schema() -> Schema {
        let source =
            std::fs::read_to_string(policy_graph_perf_fixture_dir().join("schema-source.json"))
                .expect("read policy graph perf fixture source");
        let source: serde_json::Value =
            serde_json::from_str(&source).expect("parse policy graph perf fixture source");
        let tables: BTreeMap<TableName, TableSchema> =
            serde_json::from_value(source["mergedSchema"].clone())
                .expect("decode policy graph perf public schema fixture");
        tables.into_iter().collect()
    }

    #[test]
    fn converts_policy_graph_perf_source_deterministically() {
        let source = policy_graph_perf_fixture_schema();
        let converted = convert_public_schema(&source).expect("convert policy graph schema");
        let recompiled = convert_public_schema(converted.public_schema())
            .expect("retained public schema recompiles");

        assert_eq!(converted, recompiled);
        assert_eq!(converted.version_id(), recompiled.version_id());
        assert_eq!(&source, converted.public_schema());
    }

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
    fn converts_public_integer_as_core_i32_and_column_defaults() {
        let integer_schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column("count", ColumnType::Integer))
            .build();
        let integer_table = convert_public_schema(&integer_schema)
            .unwrap()
            .into_runtime()
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
            GrooveColumnType::I32
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
            .into_runtime()
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
            GrooveColumnType::I32.array_of()
        );

        let numeric_default_schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("numeric_defaults")
                    .column_with_default("integer", ColumnType::Integer, Value::Integer(-7))
                    .column_with_default("bigint", ColumnType::BigInt, Value::Integer(-7))
                    .column_with_default("bigint_literal", ColumnType::BigInt, Value::BigInt(-7))
                    .column_with_default(
                        "integers",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Integer),
                        },
                        Value::Array(vec![Value::Integer(-7), Value::BigInt(8)]),
                    ),
            )
            .build();
        let numeric_default_table = convert_public_schema(&numeric_default_schema)
            .unwrap()
            .into_runtime()
            .tables
            .into_iter()
            .find(|table| table.name == "numeric_defaults")
            .unwrap();
        assert_eq!(
            numeric_default_table
                .columns
                .iter()
                .map(|column| (column.name.as_str(), &column.default))
                .collect::<Vec<_>>(),
            vec![
                ("integer", &Some(GrooveValue::I32(-7))),
                ("bigint", &Some(GrooveValue::I64(-7))),
                ("bigint_literal", &Some(GrooveValue::I64(-7))),
                (
                    "integers",
                    &Some(GrooveValue::Array(vec![
                        GrooveValue::I32(-7),
                        GrooveValue::I32(8),
                    ])),
                ),
            ]
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
            .into_runtime()
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
        assert_eq!(
            default_table
                .columns
                .iter()
                .find(|column| column.name == "title")
                .unwrap()
                .default,
            Some(GrooveValue::String("x".to_owned()))
        );
    }

    #[test]
    fn converts_payload_enum_default_to_tagged_record() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("events").column_with_default(
                "event",
                ColumnType::EnumPayload {
                    cases: vec![
                        EnumCaseDescriptor {
                            name: "message".to_owned(),
                            fields: vec![
                                ColumnDescriptor::new("text", ColumnType::Text),
                                ColumnDescriptor::new("level", ColumnType::Integer).nullable(),
                            ],
                        },
                        EnumCaseDescriptor {
                            name: "closed".to_owned(),
                            fields: vec![ColumnDescriptor::new("code", ColumnType::Integer)],
                        },
                    ],
                },
                Value::Enum {
                    case: "message".to_owned(),
                    values: vec![Value::Text("hello".to_owned()), Value::Null],
                },
            ))
            .build();
        let table = convert_public_schema(&schema)
            .unwrap()
            .into_runtime()
            .tables
            .into_iter()
            .find(|table| table.name == "events")
            .unwrap();
        let default = table
            .columns
            .iter()
            .find(|column| column.name == "event")
            .and_then(|column| column.default.as_ref())
            .unwrap();
        let GrooveValue::Enum(value) = default else {
            panic!("expected a payload enum default");
        };
        assert_eq!(value.tag(), 0);
        assert_eq!(
            value.record().to_values().unwrap(),
            vec![
                GrooveValue::String("hello".to_owned()),
                GrooveValue::Nullable(None),
            ]
        );
    }

    #[test]
    fn converts_payload_enums_with_more_than_u8_cases_and_a_supported_scalar_default() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("events").column_with_default(
                    "event",
                    ColumnType::EnumPayload {
                        cases: (0..257)
                            .map(|ordinal| EnumCaseDescriptor {
                                name: format!("case-{ordinal}"),
                                fields: (ordinal == 256)
                                    .then(|| vec![ColumnDescriptor::new("text", ColumnType::Text)])
                                    .unwrap_or_default(),
                            })
                            .collect(),
                    },
                    Value::Enum {
                        case: "case-256".to_owned(),
                        values: vec![Value::Text("wide default".to_owned())],
                    },
                ),
            )
            .build();
        let table = convert_public_schema(&schema)
            .unwrap()
            .into_runtime()
            .tables
            .into_iter()
            .find(|table| table.name == "events")
            .unwrap();
        let event = table
            .columns
            .iter()
            .find(|column| column.name == "event")
            .unwrap();
        let GrooveColumnType::Enum(payload_enum) = &event.column_type else {
            panic!("payload enum column expected");
        };

        assert_eq!(payload_enum.cases.len(), 257);
        assert_eq!(payload_enum.tag("case-256").unwrap(), 256);
        assert_eq!(
            &payload_enum.cases[256].payload.fields()[0].value_type,
            &GrooveColumnType::String
        );
        let Some(GrooveValue::Enum(default)) = &event.default else {
            panic!("payload enum default expected");
        };
        assert_eq!(default.tag(), 256);
        assert_eq!(
            default.record().to_values().unwrap(),
            vec![GrooveValue::String("wide default".to_owned())]
        );
    }

    // This is an internal converter test because public EnumPayload construction
    // is intentionally not exposed until the client facade can round-trip enum
    // values. It proves schema-default admission walks the selected case fields.
    #[test]
    fn rejects_schema_invalid_json_inside_payload_enum_default() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("events").column_with_default(
                "event",
                ColumnType::EnumPayload {
                    cases: vec![EnumCaseDescriptor {
                        name: "message".to_owned(),
                        fields: vec![ColumnDescriptor::new(
                            "payload",
                            ColumnType::Json {
                                schema: Some(serde_json::json!({
                                    "type": "object",
                                    "properties": { "name": { "type": "string" } },
                                    "required": ["name"],
                                })),
                            },
                        )],
                    }],
                },
                Value::Enum {
                    case: "message".to_owned(),
                    values: vec![Value::Text("{\"name\":123}".to_owned())],
                },
            ))
            .build();

        let error = convert_public_schema(&schema)
            .expect_err("schema-invalid payload-enum JSON default must reject conversion");
        assert!(
            error
                .to_string()
                .contains("JSON schema validation failed for column `event.message.payload`"),
            "unexpected enum JSON default error: {error}"
        );
    }

    #[test]
    fn raw_payload_enum_ingress_rejects_non_scalar_and_reserved_fields() {
        let nested_row = ColumnType::Row {
            columns: Box::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                "nested",
                ColumnType::Text,
            )])),
        };
        let nested_payload_enum = ColumnType::EnumPayload {
            cases: vec![EnumCaseDescriptor {
                name: "nested".to_owned(),
                fields: vec![ColumnDescriptor::new("value", ColumnType::Text)],
            }],
        };
        let cases = [
            (
                ColumnDescriptor::new("type", ColumnType::Text),
                "reserved for the discriminant",
            ),
            (
                ColumnDescriptor::new("$createdAt", ColumnType::Timestamp),
                "reserved for magic columns",
            ),
            (
                ColumnDescriptor::new("owner", ColumnType::Uuid).references("users"),
                "cannot use references",
            ),
            (
                ColumnDescriptor::new(
                    "owners",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Uuid),
                    },
                ),
                "must be scalar columns",
            ),
            (
                ColumnDescriptor::new(
                    "owner_refs",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Uuid),
                    },
                )
                .references("users"),
                "cannot use references",
            ),
            (
                ColumnDescriptor::new("row", nested_row),
                "must be scalar columns",
            ),
            (
                ColumnDescriptor::new(
                    "tag",
                    ColumnType::Enum {
                        variants: vec!["a".to_owned()],
                    },
                ),
                "must be scalar columns",
            ),
            (
                ColumnDescriptor::new("nested", nested_payload_enum),
                "nested enums are not supported",
            ),
        ];

        for (field, message) in cases {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("events").column(
                    "event",
                    ColumnType::EnumPayload {
                        cases: vec![EnumCaseDescriptor {
                            name: "message".to_owned(),
                            fields: vec![field],
                        }],
                    },
                ))
                .build();
            let error = convert_public_schema(&schema).expect_err("raw payload field is rejected");
            assert!(
                error.to_string().contains(message),
                "expected {message:?}, got {error}"
            );
        }
    }

    #[test]
    fn converted_public_integer_records_use_four_core_bytes() {
        // Public clients cannot observe core row width directly; this focused
        // conversion/layout check guards the storage-width contract.
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("metrics").column("score", ColumnType::Integer))
            .build();
        let table = convert_public_schema(&schema)
            .unwrap()
            .into_runtime()
            .tables
            .into_iter()
            .find(|table| table.name == "metrics")
            .unwrap();
        let descriptor = table.history_storage_table().record_schema();
        let values = descriptor
            .fields()
            .iter()
            .map(|field| sample_groove_value(&field.value_type))
            .collect::<Vec<_>>();
        let record = descriptor.create(&values).unwrap();
        let score_idx = descriptor.field_index("user_score").unwrap();
        let score_span = descriptor.field_span(&record, score_idx).unwrap();

        assert_eq!(
            descriptor.bind(&record).get_idx(score_idx).unwrap(),
            GrooveValue::Nullable(Some(Box::new(GrooveValue::I32(-5))))
        );
        assert_eq!(score_span.end - score_span.start, 5);
        assert_eq!(score_span.end - score_span.start - 1, 4);
    }

    fn sample_groove_value(value_type: &crate::groove::records::ValueType) -> GrooveValue {
        match value_type {
            crate::groove::records::ValueType::U8 => GrooveValue::U8(1),
            crate::groove::records::ValueType::U16 => GrooveValue::U16(2),
            crate::groove::records::ValueType::U32 => GrooveValue::U32(3),
            crate::groove::records::ValueType::U64 => GrooveValue::U64(4),
            crate::groove::records::ValueType::I32 => GrooveValue::I32(-5),
            crate::groove::records::ValueType::I64 => GrooveValue::I64(-6),
            crate::groove::records::ValueType::F64 => GrooveValue::F64(7.0),
            crate::groove::records::ValueType::Bool => GrooveValue::Bool(true),
            crate::groove::records::ValueType::String => GrooveValue::String("value".to_owned()),
            crate::groove::records::ValueType::Bytes => GrooveValue::Bytes(vec![8]),
            crate::groove::records::ValueType::Internal(_) => {
                panic!("public schemas must not contain Groove internal storage types")
            }
            crate::groove::records::ValueType::Uuid => GrooveValue::Uuid(Uuid::from_bytes([9; 16])),
            crate::groove::records::ValueType::EnumTag(_) => GrooveValue::EnumTag(0),
            crate::groove::records::ValueType::Tuple(members) => {
                GrooveValue::Tuple(members.iter().map(sample_groove_value).collect::<Vec<_>>())
            }
            crate::groove::records::ValueType::Array(_) => GrooveValue::Array(Vec::new()),
            crate::groove::records::ValueType::Nullable(inner) => {
                GrooveValue::Nullable(Some(Box::new(sample_groove_value(inner))))
            }
            crate::groove::records::ValueType::Record(descriptor) => {
                GrooveValue::Record(crate::groove::records::OwnedRecord::new(
                    descriptor
                        .create(
                            &descriptor
                                .fields()
                                .iter()
                                .map(|field| sample_groove_value(&field.value_type))
                                .collect::<Vec<_>>(),
                        )
                        .unwrap(),
                    **descriptor,
                ))
            }
            crate::groove::records::ValueType::Enum(_) => {
                panic!("Jazz public schema conversion must not receive whole-row Groove enums")
            }
        }
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
            .into_runtime()
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
                .find(|column| column.name == "payload")
                .unwrap()
                .large_value_kind,
            crate::schema::LargeValueSemanticKind::Json,
            "JSON keeps its logical String type but freezes a distinct physical kind"
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
        assert_eq!(
            table
                .columns
                .iter()
                .find(|column| column.name == "metadata")
                .unwrap()
                .large_value_kind,
            crate::schema::LargeValueSemanticKind::Json
        );
    }

    #[test]
    fn converts_public_bigint_as_core_i64() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column("count", ColumnType::BigInt))
            .build();

        let table = convert_public_schema(&schema)
            .unwrap()
            .into_runtime()
            .tables
            .into_iter()
            .find(|table| table.name == "todos")
            .unwrap();
        assert_eq!(
            table
                .columns
                .iter()
                .find(|column| column.name == "count")
                .unwrap()
                .column_type,
            GrooveColumnType::I64
        );
    }

    #[test]
    fn rejects_unsupported_public_column_types() {
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
                                    value: PolicyValue::SessionRef(vec![
                                        "claims".to_owned(),
                                        "sub".to_owned(),
                                    ]),
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
            vec![
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
                        Operand::Claim(provider_claim_operand_key("team_id")),
                    ),
                ]),
            ]
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
    fn converts_runtime_comparable_policy_literal_categories() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("events")
                    .column("occurred_at", ColumnType::Timestamp)
                    .column("ratio", ColumnType::Double)
                    .column("digest", ColumnType::Bytea)
                    .column(
                        "roles",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Text),
                        },
                    )
                    .policies(TablePolicies::new().with_select(PolicyExpr::And(vec![
                        PolicyExpr::eq_literal("occurred_at", Value::Timestamp(1_767_323_045_000)),
                        PolicyExpr::eq_literal("ratio", Value::Double(1.5)),
                        PolicyExpr::eq_literal("digest", Value::Bytea(vec![1, 2, 3])),
                        PolicyExpr::eq_literal(
                            "roles",
                            Value::Array(vec![
                                Value::Text("owner".to_owned()),
                                Value::Text("editor".to_owned()),
                            ]),
                        ),
                    ]))),
            )
            .build();

        let converted =
            convert_public_schema(&schema).expect("runtime-comparable policy literals compile");
        let events = converted
            .tables
            .iter()
            .find(|table| table.name == "events")
            .unwrap();

        assert_eq!(
            events.read_policy.as_ref().unwrap().filters,
            vec![
                Predicate::Eq(
                    Operand::Column("occurred_at".to_owned()),
                    Operand::Literal(GrooveValue::U64(1_767_323_045_000)),
                ),
                Predicate::Eq(
                    Operand::Column("ratio".to_owned()),
                    Operand::Literal(GrooveValue::F64(1.5)),
                ),
                Predicate::Eq(
                    Operand::Column("digest".to_owned()),
                    Operand::Literal(GrooveValue::Bytes(vec![1, 2, 3])),
                ),
                Predicate::Eq(
                    Operand::Column("roles".to_owned()),
                    Operand::Literal(GrooveValue::Array(vec![
                        GrooveValue::String("owner".to_owned()),
                        GrooveValue::String("editor".to_owned()),
                    ])),
                ),
            ]
        );
    }

    #[test]
    fn rejects_non_finite_policy_doubles_at_the_literal_path() {
        for (literal, path_suffix) in [
            (Value::Double(f64::NAN), ""),
            (
                Value::Array(vec![Value::Double(f64::INFINITY)]),
                ".Array[0]",
            ),
        ] {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("events")
                        .column("ratio", ColumnType::Double)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::eq_literal("ratio", literal)),
                        ),
                )
                .build();

            let error =
                convert_public_schema(&schema).expect_err("non-finite double must not compile");
            assert_eq!(
                error.path,
                format!("$.events.policies.select.using{path_suffix}")
            );
            assert_eq!(
                error.message,
                "core schema policy floating-point literals must be finite numbers"
            );
        }
    }

    #[test]
    fn converts_auth_mode_session_comparison_to_reserved_claim() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("messages")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::SessionCmp {
                        path: vec!["authMode".to_owned()],
                        op: CmpOp::Eq,
                        value: Value::Text("external".to_owned()),
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).expect("authMode policy should convert");
        let messages = converted
            .tables
            .iter()
            .find(|table| table.name == "messages")
            .unwrap();

        assert_eq!(
            messages.read_policy.as_ref().unwrap().filters,
            vec![Predicate::Eq(
                Operand::Claim("authMode".to_owned()),
                Operand::Literal(GrooveValue::String("external".to_owned())),
            )]
        );
    }

    #[test]
    fn magic_author_literals_are_text_while_row_id_literals_remain_uuid() {
        let column_types = BTreeMap::new();
        let mut filters = vec![
            Predicate::Eq(
                Operand::Column("id".to_owned()),
                Operand::Literal(GrooveValue::String(
                    "00000000-0000-4000-8000-000000000001".to_owned(),
                )),
            ),
            Predicate::Eq(
                Operand::Column("$createdBy".to_owned()),
                Operand::Literal(GrooveValue::String(
                    r#"["https://issuer.example","alice"]"#.to_owned(),
                )),
            ),
            Predicate::Eq(
                Operand::Column("$updatedBy".to_owned()),
                Operand::Literal(GrooveValue::String(
                    r#"["https://issuer.example","alice"]"#.to_owned(),
                )),
            ),
        ];

        coerce_predicates_typed_literals("todos", &mut filters, &column_types);

        assert_eq!(
            filters,
            vec![
                Predicate::Eq(
                    Operand::Column("id".to_owned()),
                    Operand::Literal(GrooveValue::Uuid(
                        Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap()
                    )),
                ),
                Predicate::Eq(
                    Operand::Column("$createdBy".to_owned()),
                    Operand::Literal(GrooveValue::String(
                        r#"["https://issuer.example","alice"]"#.to_owned(),
                    )),
                ),
                Predicate::Eq(
                    Operand::Column("$updatedBy".to_owned()),
                    Operand::Literal(GrooveValue::String(
                        r#"["https://issuer.example","alice"]"#.to_owned(),
                    )),
                ),
            ]
        );
    }

    #[test]
    fn rejects_bare_sub_session_policy_reference() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("owner", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Cmp {
                        column: "owner".to_owned(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec!["sub".to_owned()]),
                    })),
            )
            .build();

        let error = convert_public_schema(&schema)
            .expect_err("JWT sub is not a public policy-session field");
        assert_eq!(error.path, "$.todos.policies.select.using");
        assert!(error.message.contains("got session.sub"));
    }

    #[test]
    fn relation_session_references_obey_the_public_policy_contract() {
        let table = TableName::new("todos");
        let path = "policies.select.using";

        let Err(bare_sub) = rel_value_to_policy_operand(
            &table,
            path,
            &RelValueRef::SessionRef(vec!["sub".to_owned()]),
        ) else {
            panic!("relation policies must not expose JWT sub");
        };
        assert_eq!(bare_sub.path, "$.todos.policies.select.using");
        assert!(bare_sub.message.contains("got session.sub"));

        let user_id = rel_value_to_policy_operand(
            &table,
            path,
            &RelValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
        )
        .expect("raw provider sub is supported through session.claims");
        assert!(matches!(
            user_id,
            LoweredRelValue::Operand(Operand::Claim(claim)) if claim == DIRECT_USER_ID_CLAIM
        ));

        let user = rel_value_to_policy_operand(
            &table,
            path,
            &RelValueRef::SessionRef(vec!["user".to_owned()]),
        )
        .expect("user is a supported canonical provenance session field");
        assert!(matches!(
            user,
            LoweredRelValue::Operand(Operand::Claim(claim)) if claim == DIRECT_USER_CLAIM
        ));

        for path_segments in [["user"], ["authMode"], ["auth_mode"]] {
            rel_value_to_policy_operand(
                &table,
                path,
                &RelValueRef::SessionRef(path_segments.into_iter().map(str::to_owned).collect()),
            )
            .expect("documented public session aliases are supported");
        }

        let nested_user = rel_value_to_policy_operand(
            &table,
            path,
            &RelValueRef::SessionRef(vec!["claims".to_owned(), "user".to_owned()]),
        )
        .expect("provider claim `user` coexists under the claims namespace");
        assert!(matches!(nested_user,
            LoweredRelValue::Operand(Operand::Claim(claim)) if claim == provider_claim_operand_key("user")
        ));

        let claim = rel_value_to_policy_operand(
            &table,
            path,
            &RelValueRef::SessionRef(vec!["claims".to_owned(), "role".to_owned()]),
        )
        .expect("one-level session claims are supported");
        assert!(matches!(
            claim,
            LoweredRelValue::Operand(Operand::Claim(name)) if name == provider_claim_operand_key("role")
        ));
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
                                value: PolicyValue::SessionRef(vec![
                                    "claims".to_owned(),
                                    "sub".to_owned(),
                                ]),
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

    // This conversion-boundary test stays internal because its purpose is to
    // prove that legacy public-policy syntax is normalized into the same core
    // join representation as relation-backed policies before any runtime
    // schema can be admitted.
    #[test]
    fn converts_nested_correlated_exists_conjunction_to_independent_joins() {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("canvases"))
            .table(TableSchemaBuilder::new("layers").fk_column("canvas_id", "canvases"))
            .table(
                TableSchemaBuilder::new("canvas_members")
                    .fk_column("canvas_id", "canvases")
                    .column("user_id", ColumnType::Text)
                    .column("role", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("shapes")
                    .fk_column("layer_id", "layers")
                    .fk_column("canvas_id", "canvases")
                    .policies(TablePolicies::new().with_insert(PolicyExpr::Exists {
                        table: "layers".to_owned(),
                        condition: Box::new(PolicyExpr::And(vec![
                            PolicyExpr::Cmp {
                                column: "id".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "__jazz_outer_row".to_owned(),
                                    "layer_id".to_owned(),
                                ]),
                            },
                            PolicyExpr::Cmp {
                                column: "canvas_id".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "__jazz_outer_row".to_owned(),
                                    "canvas_id".to_owned(),
                                ]),
                            },
                            PolicyExpr::Exists {
                                table: "canvas_members".to_owned(),
                                condition: Box::new(PolicyExpr::And(vec![
                                    PolicyExpr::Cmp {
                                        column: "canvas_id".to_owned(),
                                        op: CmpOp::Eq,
                                        value: PolicyValue::SessionRef(vec![
                                            "__jazz_outer_row".to_owned(),
                                            "canvas_id".to_owned(),
                                        ]),
                                    },
                                    PolicyExpr::Cmp {
                                        column: "user_id".to_owned(),
                                        op: CmpOp::Eq,
                                        value: PolicyValue::SessionRef(vec!["user".to_owned()]),
                                    },
                                    PolicyExpr::Cmp {
                                        column: "role".to_owned(),
                                        op: CmpOp::Eq,
                                        value: PolicyValue::Literal(Value::Text(
                                            "editor".to_owned(),
                                        )),
                                    },
                                ])),
                            },
                        ])),
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema)
            .expect("nested correlated EXISTS conjunction must compile");
        let shapes = converted
            .tables
            .iter()
            .find(|table| table.name == "shapes")
            .unwrap();
        let policy = shapes.write_policies.insert_check.as_ref().unwrap();
        assert_eq!(policy.joins.len(), 2);
        assert_eq!(policy.joins[0].table, "layers");
        assert_eq!(policy.joins[0].source_column.as_deref(), Some("layer_id"));
        assert_eq!(
            policy.joins[0].correlated_filters,
            vec![JoinCorrelation {
                join_column: "canvas_id".to_owned(),
                source_column: "canvas_id".to_owned(),
            }]
        );
        assert_eq!(policy.joins[1].table, "canvas_members");
        assert_eq!(policy.joins[1].on_column, "canvas_id");
        assert_eq!(policy.joins[1].source_column.as_deref(), Some("canvas_id"));
    }

    #[test]
    fn converts_correlated_exists_against_id_to_row_id_join() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("chats")
                    .column("name", ColumnType::Text)
                    .column("createdBy", ColumnType::Text)
                    .column("isPublic", ColumnType::Boolean),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("createdBy", ColumnType::Text)
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Exists {
                        table: "chats".to_owned(),
                        condition: Box::new(PolicyExpr::And(vec![
                            PolicyExpr::Cmp {
                                column: "createdBy".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "__jazz_outer_row".to_owned(),
                                    "createdBy".to_owned(),
                                ]),
                            },
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
            join.correlated_filters,
            vec![JoinCorrelation {
                join_column: "createdBy".to_owned(),
                source_column: "createdBy".to_owned(),
            }]
        );
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
                                    value: PolicyValue::SessionRef(vec![
                                        "claims".to_owned(),
                                        "sub".to_owned(),
                                    ]),
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
                                    value: PolicyValue::SessionRef(vec![
                                        "claims".to_owned(),
                                        "sub".to_owned(),
                                    ]),
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
                Operand::Claim(provider_claim_operand_key("sub")),
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
    fn preserves_unbounded_inherited_select_as_native_atom() {
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
                        value: PolicyValue::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
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
        assert!(policy.joins.is_empty());
        assert_eq!(policy.inherits.len(), 1);
        assert_eq!(policy.inherits[0].parent_column, "folder_id");
        assert_eq!(policy.inherits[0].operation, InheritsOperation::Select);
    }

    #[test]
    fn preserves_inherits_nested_under_a_conjunction() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column("name", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .column("published", ColumnType::Boolean)
                    .nullable_fk_column("folder_id", "folders")
                    .policies(TablePolicies::new().with_select(PolicyExpr::And(vec![
                        PolicyExpr::Cmp {
                            column: "published".to_owned(),
                            op: CmpOp::Eq,
                            value: PolicyValue::Literal(Value::Boolean(true)),
                        },
                        PolicyExpr::And(vec![PolicyExpr::Inherits {
                            operation: Operation::Select,
                            via_column: "folder_id".to_owned(),
                            max_depth: None,
                        }]),
                    ]))),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let documents = converted
            .tables
            .iter()
            .find(|table| table.name == "documents")
            .unwrap();
        let policy = documents.read_policy.as_ref().unwrap();
        assert_eq!(
            policy.filters,
            vec![Predicate::Eq(
                Operand::Column("published".to_owned()),
                Operand::Literal(GrooveValue::Bool(true)),
            )]
        );
        assert_eq!(policy.inherits.len(), 1);
        assert_eq!(policy.inherits[0].parent_column, "folder_id");
        assert_eq!(policy.inherits[0].operation, InheritsOperation::Select);
    }

    #[test]
    fn preserves_depth_limited_inherited_select_as_native_atom() {
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
        assert!(policy.joins.is_empty());
        assert_eq!(policy.inherits.len(), 1);
        assert_eq!(policy.inherits[0].parent_column, "target_team");
        assert_eq!(policy.inherits[0].operation, InheritsOperation::Select);
        assert_eq!(policy.inherits[0].max_depth, Some(32));
    }

    #[test]
    fn preserves_empty_in_list_as_in_predicate() {
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
        assert_eq!(
            policy.filters,
            vec![Predicate::In(
                Operand::Column("kind".to_owned()),
                Vec::new()
            )]
        );
    }

    #[test]
    fn converts_session_in_list_to_claim_membership_predicate() {
        let legacy_role_id = ObjectId::from_uuid(
            uuid::Uuid::parse_str("89d1af9b-6877-44d5-b214-7f9f95800a3d").unwrap(),
        );
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("messages")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::SessionInList {
                        path: vec!["claims".to_owned(), "role".to_owned()],
                        values: vec![
                            Value::Text(legacy_role_id.uuid().to_string()),
                            Value::Text("member".to_owned()),
                        ],
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
        assert_eq!(
            policy.filters,
            vec![Predicate::In(
                Operand::Claim(provider_claim_operand_key("role")),
                vec![
                    Operand::Literal(GrooveValue::String(legacy_role_id.uuid().to_string())),
                    Operand::Literal(GrooveValue::String("member".to_owned())),
                ],
            )]
        );
    }

    #[test]
    fn converts_session_eq_cmp_to_claim_equality_predicate() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("messages")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::SessionCmp {
                        path: vec!["claims".to_owned(), "role".to_owned()],
                        op: CmpOp::Eq,
                        value: Value::Text("admin".to_owned()),
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
        assert_eq!(
            policy.filters,
            vec![Predicate::Eq(
                Operand::Claim(provider_claim_operand_key("role")),
                Operand::Literal(GrooveValue::String("admin".to_owned())),
            )]
        );
    }

    #[test]
    fn converts_enum_policy_literals_as_text_for_native_schema_parity() {
        let legacy_role_id = ObjectId::from_uuid(
            uuid::Uuid::parse_str("0cae56e7-0f54-421c-ba8b-54fcbfec8dd2").unwrap(),
        );
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("access_edges")
                    .column(
                        "grant_role",
                        ColumnType::Enum {
                            variants: vec![
                                "EDITOR".to_owned(),
                                "MANAGER".to_owned(),
                                "VIEWER".to_owned(),
                            ],
                        },
                    )
                    .policies(TablePolicies::new().with_select(PolicyExpr::InList {
                        column: "grant_role".to_owned(),
                        values: vec![
                            PolicyValue::Literal(Value::Text("VIEWER".to_owned())),
                            PolicyValue::Literal(Value::Uuid(legacy_role_id)),
                        ],
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let access_edges = converted
            .tables
            .iter()
            .find(|table| table.name == "access_edges")
            .unwrap();
        let policy = access_edges.read_policy.as_ref().unwrap();
        assert_eq!(
            policy.filters,
            vec![Predicate::In(
                Operand::Column("grant_role".to_owned()),
                vec![
                    Operand::Literal(GrooveValue::String("VIEWER".to_owned())),
                    Operand::Literal(GrooveValue::Uuid(*legacy_role_id.uuid())),
                ],
            )]
        );
    }

    #[test]
    fn lowers_projected_gather_seed_from_membership_hop() {
        let seed = PublicRelExpr::Project {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::Filter {
                    input: Box::new(PublicRelExpr::TableScan {
                        table: "team_entry".into(),
                        alias: None,
                    }),
                    predicate: RelPredicateExpr::Cmp {
                        left: RelColumnRef {
                            scope: None,
                            column: "team_id".to_owned(),
                        },
                        op: RelPredicateCmpOp::Eq,
                        right: RelValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
                    },
                }),
                right: Box::new(PublicRelExpr::TableScan {
                    table: "teams".into(),
                    alias: Some("target".to_owned()),
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "target".to_owned(),
                    },
                    right: RelColumnRef {
                        scope: Some("target".to_owned()),
                        column: "id".to_owned(),
                    },
                }],
                join_kind: RelJoinKind::Inner,
            }),
            columns: vec![crate::tools::public_api::relation_ir::ProjectColumn {
                alias: "id".to_owned(),
                expr: RelProjectExpr::Column(RelColumnRef {
                    scope: Some("target".to_owned()),
                    column: "id".to_owned(),
                }),
            }],
        };

        let (_from, lowered) =
            lower_gather_seed(&TableName::new("resources"), "policy", &seed).unwrap();
        assert_eq!(lowered.table, "team_entry");
        assert_eq!(lowered.user_column.as_deref(), Some("team_id"));
        assert_eq!(lowered.user_claim.as_deref(), Some(DIRECT_USER_ID_CLAIM));
        assert_eq!(lowered.team_column, "target");
    }

    #[test]
    fn compiles_update_using_as_the_default_delete_policy() {
        let owner_policy = PolicyExpr::Cmp {
            column: "owner_id".to_owned(),
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
        };
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("owner_id", ColumnType::Text)
                    .policies(
                        TablePolicies::new().with_update(Some(owner_policy), PolicyExpr::True),
                    ),
            )
            .table(
                TableSchemaBuilder::new("attachments")
                    .fk_column("document_id", "documents")
                    .policies(TablePolicies::new().with_insert(PolicyExpr::Inherits {
                        operation: Operation::Delete,
                        via_column: "document_id".to_owned(),
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
        assert_eq!(
            documents.write_policies.delete_using, documents.write_policies.update_using,
            "DELETE must inherit UPDATE USING when no explicit DELETE USING is declared"
        );
        let attachments = converted
            .tables
            .iter()
            .find(|table| table.name == "attachments")
            .unwrap();
        assert_eq!(
            attachments
                .write_policies
                .insert_check
                .as_ref()
                .unwrap()
                .inherits[0]
                .operation,
            InheritsOperation::Delete
        );
    }

    #[test]
    fn preserves_operation_specific_inherits_for_complex_child_write_policy() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("owner_id", ColumnType::Text)
                    .policies(TablePolicies::new().with_update(
                        Some(PolicyExpr::Or(vec![
                            PolicyExpr::Cmp {
                                column: "owner_id".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "claims".to_owned(),
                                    "sub".to_owned(),
                                ]),
                            },
                            PolicyExpr::Exists {
                                table: "parent_admins".to_owned(),
                                condition: Box::new(PolicyExpr::And(vec![
                                    PolicyExpr::Cmp {
                                        column: "parent_id".to_owned(),
                                        op: CmpOp::Eq,
                                        value: PolicyValue::SessionRef(vec![
                                            "__jazz_outer_row".to_owned(),
                                            "id".to_owned(),
                                        ]),
                                    },
                                    PolicyExpr::Cmp {
                                        column: "user_id".to_owned(),
                                        op: CmpOp::Eq,
                                        value: PolicyValue::SessionRef(vec![
                                            "claims".to_owned(),
                                            "sub".to_owned(),
                                        ]),
                                    },
                                ])),
                            },
                        ])),
                        PolicyExpr::True,
                    )),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent_id", "parents")
                    .policies(TablePolicies::new().with_insert(PolicyExpr::Inherits {
                        operation: Operation::Update,
                        via_column: "parent_id".to_owned(),
                        max_depth: None,
                    })),
            )
            .table(
                TableSchemaBuilder::new("parent_admins")
                    .fk_column("parent_id", "parents")
                    .column("user_id", ColumnType::Text),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let children = converted
            .tables
            .iter()
            .find(|table| table.name == "children")
            .unwrap();
        let inherits = &children
            .write_policies
            .insert_check
            .as_ref()
            .unwrap()
            .inherits[0];
        assert_eq!(inherits.parent_column, "parent_id");
        assert_eq!(inherits.operation, InheritsOperation::Update);
    }

    #[test]
    fn preserves_insert_inherits_when_parent_select_uses_seeded_reachability() {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("teams").column("identity_key", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("team_team_edges")
                    .fk_column("child_team", "teams")
                    .fk_column("parent_team", "teams"),
            )
            .table(TableSchemaBuilder::new("resources"))
            .table(
                TableSchemaBuilder::new("resource_access_edges")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams"),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("resource_id", "resources")
                    .policies(TablePolicies::new().with_insert(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "resource_id".to_owned(),
                        max_depth: None,
                    })),
            )
            .build();

        let seed = PublicRelExpr::Project {
            input: Box::new(PublicRelExpr::Filter {
                input: Box::new(PublicRelExpr::TableScan {
                    table: "teams".into(),
                    alias: None,
                }),
                predicate: RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: None,
                        column: "identity_key".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
                },
            }),
            columns: vec![crate::tools::public_api::relation_ir::ProjectColumn {
                alias: "id".to_owned(),
                expr: RelProjectExpr::Column(RelColumnRef {
                    scope: None,
                    column: "id".to_owned(),
                }),
            }],
        };
        let step = PublicRelExpr::Project {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::Filter {
                    input: Box::new(PublicRelExpr::TableScan {
                        table: "team_team_edges".into(),
                        alias: None,
                    }),
                    predicate: RelPredicateExpr::Cmp {
                        left: RelColumnRef {
                            scope: None,
                            column: "child_team".to_owned(),
                        },
                        op: RelPredicateCmpOp::Eq,
                        right: RelValueRef::RowId(RelRowIdRef::Frontier),
                    },
                }),
                right: Box::new(PublicRelExpr::TableScan {
                    table: "teams".into(),
                    alias: None,
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "parent_team".to_owned(),
                    },
                    right: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                }],
                join_kind: RelJoinKind::Inner,
            }),
            columns: vec![crate::tools::public_api::relation_ir::ProjectColumn {
                alias: "id".to_owned(),
                expr: RelProjectExpr::Column(RelColumnRef {
                    scope: None,
                    column: "id".to_owned(),
                }),
            }],
        };
        let access_rel = PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::Gather {
                    seed: Box::new(seed),
                    step: Box::new(step),
                    frontier_key: RelKeyRef::RowId(RelRowIdRef::Current),
                    bound: RelRecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelKeyRef::RowId(RelRowIdRef::Current)],
                }),
                right: Box::new(PublicRelExpr::TableScan {
                    table: "resource_access_edges".into(),
                    alias: Some("access".to_owned()),
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "team".to_owned(),
                    },
                }],
                join_kind: RelJoinKind::Inner,
            }),
            predicate: RelPredicateExpr::Cmp {
                left: RelColumnRef {
                    scope: Some("access".to_owned()),
                    column: "resource".to_owned(),
                },
                op: RelPredicateCmpOp::Eq,
                right: RelValueRef::RowId(RelRowIdRef::Outer),
            },
        };

        let mut schema = schema;
        schema
            .get_mut(&TableName::new("resources"))
            .unwrap()
            .policies
            .select
            .using = Some(PolicyExpr::ExistsRel { rel: access_rel });

        let converted = convert_public_schema(&schema).unwrap();
        let children = converted
            .tables
            .iter()
            .find(|table| table.name == "children")
            .unwrap();
        let inherits = &children
            .write_policies
            .insert_check
            .as_ref()
            .unwrap()
            .inherits[0];
        assert_eq!(inherits.parent_column, "resource_id");
        assert_eq!(inherits.operation, InheritsOperation::Select);
    }

    #[test]
    fn preserves_inherited_select_branch_parent_policy_as_native_atom() {
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
                                value: PolicyValue::SessionRef(vec![
                                    "claims".to_owned(),
                                    "sub".to_owned(),
                                ]),
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
        assert!(policy.policy_branches.is_empty());
        assert!(policy.joins.is_empty());
        assert_eq!(policy.inherits.len(), 1);
        assert_eq!(policy.inherits[0].parent_column, "messageId");
        assert_eq!(policy.inherits[0].operation, InheritsOperation::Select);
    }

    #[test]
    fn preserves_nested_inherited_select_branch_parent_policy_as_native_atom() {
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("chats").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("canvases")
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
                                value: PolicyValue::SessionRef(vec![
                                    "claims".to_owned(),
                                    "sub".to_owned(),
                                ]),
                            },
                        ])),
                    })),
            )
            .table(
                TableSchemaBuilder::new("strokes")
                    .fk_column("canvasId", "canvases")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "canvasId".to_owned(),
                        max_depth: None,
                    })),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let strokes = converted
            .tables
            .iter()
            .find(|table| table.name == "strokes")
            .unwrap();
        let policy = strokes.read_policy.as_ref().unwrap();
        assert!(policy.policy_branches.is_empty());
        assert!(policy.joins.is_empty());
        assert_eq!(policy.inherits.len(), 1);
        assert_eq!(policy.inherits[0].parent_column, "canvasId");
        assert_eq!(policy.inherits[0].operation, InheritsOperation::Select);
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
                        value: PolicyValue::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
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
                        value: PolicyValue::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
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

    // This conversion-boundary test is intentionally internal: the important
    // contract is that an unrepresentable public policy is rejected before it
    // can be published as a native schema with different traversal semantics.
    #[test]
    fn rejects_depth_limited_inherited_select_in_reverse_source_policy() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("attachments")
                    .fk_column("fileId", "files")
                    .fk_column("teamId", "teams")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "teamId".to_owned(),
                        max_depth: Some(1),
                    })),
            )
            .table(
                TableSchemaBuilder::new("files").policies(TablePolicies::new().with_select(
                    PolicyExpr::InheritsReferencing {
                        operation: Operation::Select,
                        source_table: "attachments".to_owned(),
                        via_column: "fileId".to_owned(),
                        max_depth: None,
                    },
                )),
            )
            .build();

        let error = convert_public_schema(&schema).unwrap_err();
        assert!(error.to_string().contains(
            "bounded SELECT INHERITS under INHERITS_REFERENCING is unsupported because its expanded join chain cannot preserve max_depth"
        ));
    }

    #[test]
    fn converts_exists_rel_gather_seeded_reachability_to_core_query() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("resources")
                    .column("label", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                        rel: PublicRelExpr::Filter {
                            input: Box::new(PublicRelExpr::Join {
                                left: Box::new(PublicRelExpr::Gather {
                                    seed: Box::new(PublicRelExpr::Filter {
                                        input: Box::new(PublicRelExpr::TableScan {
                                            table: "teams".into(),
                                            alias: None,
                                        }),
                                        predicate: RelPredicateExpr::Cmp {
                                            left: RelColumnRef {
                                                scope: Some("teams".to_owned()),
                                                column: "identity_key".to_owned(),
                                            },
                                            op: RelPredicateCmpOp::Eq,
                                            right: RelValueRef::SessionRef(vec![
                                                "claims".to_owned(),
                                                "sub".to_owned(),
                                            ]),
                                        },
                                    }),
                                    step: Box::new(PublicRelExpr::Project {
                                        input: Box::new(PublicRelExpr::Join {
                                            left: Box::new(PublicRelExpr::Filter {
                                                input: Box::new(PublicRelExpr::TableScan {
                                                    table: "team_team_edges".into(),
                                                    alias: None,
                                                }),
                                                predicate: RelPredicateExpr::Cmp {
                                                    left: RelColumnRef {
                                                        scope: Some("team_team_edges".to_owned()),
                                                        column: "child_team".to_owned(),
                                                    },
                                                    op: RelPredicateCmpOp::Eq,
                                                    right: RelValueRef::RowId(
                                                        RelRowIdRef::Frontier,
                                                    ),
                                                },
                                            }),
                                            right: Box::new(PublicRelExpr::TableScan {
                                                table: "teams".into(),
                                                alias: Some("__recursive_hop_0".to_owned()),
                                            }),
                                            on: vec![RelJoinCondition {
                                                left: RelColumnRef {
                                                    scope: Some("team_team_edges".to_owned()),
                                                    column: "parent_team".to_owned(),
                                                },
                                                right: RelColumnRef {
                                                    scope: Some("__recursive_hop_0".to_owned()),
                                                    column: "id".to_owned(),
                                                },
                                            }],
                                            join_kind: RelJoinKind::Inner,
                                        }),
                                        columns: Vec::new(),
                                    }),
                                    frontier_key: RelKeyRef::RowId(RelRowIdRef::Current),
                                    bound: RelRecursionBound::MaxDepth(8),
                                    dedupe_key: vec![RelKeyRef::RowId(RelRowIdRef::Current)],
                                }),
                                right: Box::new(PublicRelExpr::TableScan {
                                    table: "resource_access_edges".into(),
                                    alias: Some("access".to_owned()),
                                }),
                                on: vec![RelJoinCondition {
                                    left: RelColumnRef {
                                        scope: None,
                                        column: "id".to_owned(),
                                    },
                                    right: RelColumnRef {
                                        scope: Some("access".to_owned()),
                                        column: "team".to_owned(),
                                    },
                                }],
                                join_kind: RelJoinKind::Inner,
                            }),
                            predicate: RelPredicateExpr::And(vec![
                                RelPredicateExpr::Cmp {
                                    left: RelColumnRef {
                                        scope: Some("access".to_owned()),
                                        column: "resource".to_owned(),
                                    },
                                    op: RelPredicateCmpOp::Eq,
                                    right: RelValueRef::RowId(RelRowIdRef::Outer),
                                },
                                RelPredicateExpr::Cmp {
                                    left: RelColumnRef {
                                        scope: Some("access".to_owned()),
                                        column: "grant_role".to_owned(),
                                    },
                                    op: RelPredicateCmpOp::Eq,
                                    right: RelValueRef::Literal(Value::Text("viewer".to_owned())),
                                },
                            ]),
                        },
                    })),
            )
            .table(TableSchemaBuilder::new("teams").column("identity_key", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("team_team_edges")
                    .fk_column("child_team", "teams")
                    .fk_column("parent_team", "teams"),
            )
            .table(
                TableSchemaBuilder::new("resource_access_edges")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams")
                    .column("grant_role", ColumnType::Text),
            )
            .build();

        let converted = convert_public_schema(&schema).unwrap();
        let resources = converted
            .tables
            .iter()
            .find(|table| table.name == "resources")
            .unwrap();
        let reachable = &resources.read_policy.as_ref().unwrap().reachable[0];
        assert_eq!(reachable.access_table, "resource_access_edges");
        assert_eq!(reachable.access_row_column, "resource");
        assert_eq!(reachable.access_team_column, "team");
        assert_eq!(reachable.edge_table, "team_team_edges");
        assert_eq!(reachable.edge_member_column, "child_team");
        assert_eq!(reachable.edge_parent_column, "parent_team");
        assert_eq!(reachable.bound, crate::query::RecursionBound::MaxDepth(8));
        assert_eq!(
            reachable.access_filters,
            vec![Predicate::Eq(
                Operand::Column("grant_role".to_owned()),
                Operand::Literal(crate::groove::records::Value::String("viewer".to_owned())),
            )]
        );
        let seed = reachable.seed.as_ref().unwrap();
        assert_eq!(seed.table, "teams");
        assert_eq!(seed.user_column.as_deref(), Some("identity_key"));
        assert_eq!(seed.user_claim.as_deref(), Some(DIRECT_USER_ID_CLAIM));
        assert_eq!(seed.team_column, "id");
    }
}

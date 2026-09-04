//! Aggregate result shaping and ordering for query evaluation.

use std::cmp::Ordering;

use groove::records::BorrowedRecord;
use groove::schema::ColumnType;

use super::{
    Aggregate, AggregateFunction, ColumnSchema, CurrentRow, Error, ResultMemberEntry, RowUuid,
    SyntheticReplacementToken, TableSchema, Value, aggregate_output_column,
    aggregate_result_member_row_uuid, nullable_value,
};

pub(super) fn compare_optional_values(left: Option<Value>, right: Option<Value>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_order_values(&left, &right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub(super) fn aggregate_row_cell(row: &CurrentRow, column: &str) -> Option<Value> {
    let idx = row.record.descriptor().field_index(column)?;
    nullable_value(row.record.borrowed().get_idx(idx).ok()?).ok()?
}

pub(super) fn aggregate_result_table(
    query: &crate::query::Query,
    source_table: &TableSchema,
) -> Result<TableSchema, Error> {
    let aggregate = query.aggregate.as_ref().ok_or(Error::InvalidStoredValue(
        "aggregate query missing aggregate",
    ))?;
    let mut columns = Vec::new();
    if let Some(group_by) = &aggregate.group_by {
        let column = source_table
            .columns
            .iter()
            .find(|column| &column.name == group_by)
            .ok_or(Error::InvalidStoredValue("aggregate group column missing"))?;
        columns.push(ColumnSchema::new(&column.name, column.column_type.clone()));
    }
    for aggregate in &aggregate.aggregates {
        columns.push(ColumnSchema::new(
            aggregate_output_column(&aggregate.alias),
            aggregate_result_column_type(aggregate, source_table)?,
        ));
    }
    Ok(TableSchema::new(&query.table, columns))
}

fn aggregate_result_column_type(
    aggregate: &Aggregate,
    source_table: &TableSchema,
) -> Result<ColumnType, Error> {
    match aggregate.function {
        AggregateFunction::Count => Ok(ColumnType::U64),
        AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => {
            let column = aggregate
                .column
                .as_ref()
                .ok_or(Error::InvalidStoredValue("aggregate input column missing"))?;
            let column_type = source_table
                .columns
                .iter()
                .find(|candidate| &candidate.name == column)
                .map(|column| column.column_type.clone())
                .ok_or(Error::InvalidStoredValue("aggregate input column missing"))?;
            // `CurrentRow` supplies the public nullable envelope. The aggregate
            // payload carries the SQL nullable layer, flattened before it reaches
            // this synthetic table schema.
            Ok(match column_type {
                ColumnType::Nullable(inner) => *inner,
                column_type => column_type,
            })
        }
        AggregateFunction::Avg => Ok(ColumnType::F64),
    }
}

/// Use the same stable identity for direct aggregate reads and maintained
/// aggregate delivery. A global aggregate is keyed by `"global"`; grouped
/// aggregates are keyed by their lowered group value.
pub(super) fn aggregate_query_row_uuid(
    query: &crate::query::Query,
    record: &BorrowedRecord<'_>,
) -> Result<RowUuid, Error> {
    let aggregate = query.aggregate.as_ref().ok_or(Error::InvalidStoredValue(
        "aggregate query missing aggregate",
    ))?;
    let (row_value, row_type) = match &aggregate.group_by {
        Some(group_by) => {
            let index =
                record
                    .descriptor()
                    .field_index(group_by)
                    .ok_or(Error::InvalidStoredValue(
                        "aggregate record is missing group identity",
                    ))?;
            (
                record.get_idx(index)?,
                record
                    .descriptor()
                    .fields()
                    .get(index)
                    .ok_or(Error::InvalidStoredValue(
                        "aggregate group identity field is missing from descriptor",
                    ))?
                    .value_type
                    .clone(),
            )
        }
        None => (
            Value::String("global".to_owned()),
            groove::records::ValueType::String,
        ),
    };
    let row = super::super::codec::settled_result_value_storage_bytes(&row_value, &row_type)?;
    aggregate_result_member_row_uuid(&ResultMemberEntry::Synthetic {
        table: "aggregate_result".to_owned(),
        row,
        replacement: SyntheticReplacementToken::from_encoded_record(
            super::super::codec::settled_result_value_storage_bytes(
                &Value::String("identity-only".to_owned()),
                &groove::records::ValueType::String,
            )?,
        ),
    })
}

fn compare_order_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::U8(left), Value::U8(right)) => left.cmp(right),
        (Value::U16(left), Value::U16(right)) => left.cmp(right),
        (Value::U32(left), Value::U32(right)) => left.cmp(right),
        (Value::U64(left), Value::U64(right)) => left.cmp(right),
        (Value::I32(left), Value::I32(right)) => left.cmp(right),
        (Value::I64(left), Value::I64(right)) => left.cmp(right),
        (Value::F64(left), Value::F64(right)) => left.total_cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.as_bytes().cmp(right.as_bytes()),
        (Value::EnumTag(left), Value::EnumTag(right)) => left.cmp(right),
        (Value::Tuple(left), Value::Tuple(right)) | (Value::Array(left), Value::Array(right)) => {
            compare_order_value_slices(left, right)
        }
        (Value::Nullable(left), Value::Nullable(right)) => match (left, right) {
            (Some(left), Some(right)) => compare_order_values(left, right),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
        _ => Ordering::Equal,
    }
}

fn compare_order_value_slices(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = compare_order_values(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

//! Aggregate result shaping and ordering for query evaluation.

use crate::node::query_engine::{AggregateResultSchema, AppRowTerminal};
use std::cmp::Ordering;

use groove::records::BorrowedRecord;
use groove::schema::ColumnType;

use super::{
    Aggregate, AggregateFunction, ColumnSchema, CurrentRow, Error, ResultMemberEntry, RowUuid,
    SyntheticReplacementToken, TableSchema, Value, aggregate_output_column,
    aggregate_result_member_row_uuid,
};

pub(super) fn compare_optional_values(left: Option<Value>, right: Option<Value>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_order_values(&left, &right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn aggregate_row_cell(row: &CurrentRow, column: &str) -> Result<Option<Value>, Error> {
    let idx = row
        .application_column_index_by_name(column)
        .ok_or(Error::InvalidStoredValue(
            "aggregate ordering field has no publication binding",
        ))?;
    Ok(match row.record.borrowed().get_idx(idx)? {
        Value::Nullable(value) => value.map(|value| *value),
        value => Some(value),
    })
}

/// Decode ordering keys before sorting so malformed rows fail instead of
/// becoming null keys. Aggregate terminals can emit raw scalars while projected
/// current rows wrap the same values in Nullable; both have identical order.
pub(super) fn sort_aggregate_rows<T>(
    query: &crate::query::Query,
    rows: &mut [T],
    row: impl Fn(&T) -> &CurrentRow,
    tie_break: impl Fn(&T, &T) -> Ordering,
) -> Result<(), Error> {
    let mut keys = rows
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let values = query
                .order_by
                .iter()
                .map(|order| aggregate_row_cell(row(item), &order.column))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((index, values))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    keys.sort_by(|(left_index, left), (right_index, right)| {
        for ((left, right), order) in left.iter().zip(right).zip(&query.order_by) {
            let ordering = compare_optional_values(left.clone(), right.clone());
            let ordering = match order.direction {
                crate::query::OrderDirection::Asc => ordering,
                crate::query::OrderDirection::Desc => ordering.reverse(),
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        tie_break(&rows[*left_index], &rows[*right_index])
    });
    // Apply the sorted permutation without cloning rows or separating a
    // maintained row from its occurrence sidecar.
    let mut destinations = vec![0; rows.len()];
    for (destination, (source, _)) in keys.into_iter().enumerate() {
        destinations[source] = destination;
    }
    for source in 0..rows.len() {
        while destinations[source] != source {
            let destination = destinations[source];
            rows.swap(source, destination);
            destinations.swap(source, destination);
        }
    }
    Ok(())
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
pub(super) fn aggregate_output_schema(
    output: &super::AppRowSchema,
) -> Result<&AggregateResultSchema, Error> {
    match &output.terminal {
        AppRowTerminal::Aggregate(schema) => Ok(schema),
        _ => Err(Error::InvalidStoredValue(
            "aggregate materialization has no lowered aggregate schema",
        )),
    }
}

pub(super) fn aggregate_record_field_index(
    record: &BorrowedRecord<'_>,
    field: &groove::records::DescriptorField,
) -> Result<usize, Error> {
    let identity = field.identity.as_ref().ok_or(Error::InvalidStoredValue(
        "lowered aggregate field has no identity",
    ))?;
    let descriptor = record.descriptor();
    let index = descriptor
        .field_index_by_identity(identity)
        .ok_or(Error::InvalidStoredValue(
            "aggregate output is missing its lowered field identity",
        ))?;
    Ok(index)
}

pub(super) fn aggregate_query_row_uuid(
    output: &super::AppRowSchema,
    record: &BorrowedRecord<'_>,
) -> Result<RowUuid, Error> {
    let aggregate = aggregate_output_schema(output)?;
    if aggregate.group_key_fields.len() > 1 {
        return Err(Error::InvalidStoredValue(
            "aggregate row identity requires one lowered group field",
        ));
    }
    let (row_value, row_type) = match aggregate.group_key_fields.first() {
        Some(group) => {
            let index = aggregate_record_field_index(record, group)?;
            (
                record.get_idx(index)?,
                record.descriptor().fields()[index].value_type.clone(),
            )
        }
        None => (
            Value::String("global".to_owned()),
            groove::records::ValueType::String,
        ),
    };
    let row = super::super::codec::runtime_result_identity_bytes(&row_value, &row_type)?;
    aggregate_result_member_row_uuid(&ResultMemberEntry::Synthetic {
        table: "aggregate_result".to_owned(),
        row,
        replacement: SyntheticReplacementToken::from_encoded_record(
            super::super::codec::runtime_result_identity_bytes(
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

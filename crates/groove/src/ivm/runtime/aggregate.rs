//! Aggregate planning and evaluation for the synchronous IVM runtime.
//!
//! This stays separate from the tick evaluator because it operates solely on
//! reconstructed input multisets and record descriptors.

use super::*;

pub(super) fn resolve_aggregate_expr(
    input: &RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<AggregateExpr, IvmRuntimeError> {
    let expression = match &aggregate.expression {
        Some(PlanExpr::Field(field)) => Some(PlanExpr::Field(resolve_field_name(input, field)?)),
        Some(PlanExpr::Nullable(field)) => {
            Some(PlanExpr::Nullable(resolve_field_name(input, field)?))
        }
        Some(PlanExpr::NullableFlat(field)) => {
            Some(PlanExpr::NullableFlat(resolve_field_name(input, field)?))
        }
        Some(PlanExpr::EnumTagRemap { field, tags }) => Some(PlanExpr::EnumTagRemap {
            field: resolve_field_name(input, field)?,
            tags: tags.clone(),
        }),
        Some(PlanExpr::EnumRemap { field, tags }) => Some(PlanExpr::EnumRemap {
            field: resolve_field_name(input, field)?,
            tags: tags.clone(),
        }),
        Some(PlanExpr::RecursiveEnumRemap {
            field,
            remaps,
            omit_unrepresentable,
        }) => Some(PlanExpr::RecursiveEnumRemap {
            field: resolve_field_name(input, field)?,
            remaps: remaps.clone(),
            omit_unrepresentable: *omit_unrepresentable,
        }),
        Some(PlanExpr::Literal(_)) | Some(PlanExpr::Null(_)) | None => aggregate.expression.clone(),
    };
    Ok(AggregateExpr {
        function: aggregate.function.clone(),
        expression,
        distinct: aggregate.distinct,
        output_name: aggregate.output_name.clone(),
        output_identity: aggregate.output_identity.clone(),
    })
}

fn resolve_field_name(input: &RecordDescriptor, field: &str) -> Result<String, IvmRuntimeError> {
    let field_idx = input
        .field_index(field)
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
    field_name_at(input, field_idx)
}

/// Reconstructs the positive pre-tick input multiset from its post-tick state.
pub(super) fn records_before_from_deltas(
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
) -> Vec<(Bytes, i64)> {
    let mut records = BTreeMap::<Bytes, (Bytes, i64)>::new();
    for (record, weight) in after_records {
        records.insert(record.clone(), (record, weight));
    }
    for delta in deltas {
        let entry = records
            .entry(delta.record.clone())
            .or_insert_with(|| (delta.record.clone(), 0));
        entry.1 -= delta.weight;
    }
    records
        .into_iter()
        .filter_map(|(_, (record, weight))| (weight > 0).then_some((record, weight)))
        .collect()
}

pub(super) fn aggregate_row_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    aggregate: &AggregateOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let mut positive = Vec::new();
    let mut total_weight = 0_i64;
    for (record, weight) in records {
        if *weight < 0 {
            return Err(IvmRuntimeError::UnsupportedOperator);
        }
        if *weight > 0 {
            total_weight += *weight;
            positive.push((record.as_ref(), *weight));
        }
    }
    if total_weight == 0 && !aggregate.group_key.is_empty() {
        return Ok(None);
    }

    let mut values = Vec::new();
    if let Some((first, _)) = positive.first() {
        let first = BorrowedRecord::new(first, &input_desc);
        for group_expr in &aggregate.group_key {
            values.push(evaluate_aggregate_expr(&first, group_expr)?);
        }
    }
    for aggregate_expr in &aggregate.aggregates {
        values.push(evaluate_aggregate(records, input_desc, aggregate_expr)?);
    }
    output_desc
        .create(&values)
        .map(Bytes::from)
        .map(Some)
        .map_err(IvmRuntimeError::RecordEncoding)
}

fn evaluate_aggregate(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Value, IvmRuntimeError> {
    if aggregate.distinct {
        return Err(IvmRuntimeError::UnsupportedOperator);
    }
    match aggregate.function {
        AggregateFunction::Count => {
            let mut count = 0_u64;
            for (record, weight) in records {
                if *weight <= 0 {
                    continue;
                }
                if let Some(expr) = &aggregate.expression {
                    let value =
                        evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
                    if is_null_value(&value) {
                        continue;
                    }
                }
                count = count
                    .checked_add(
                        u64::try_from(*weight).map_err(|_| IvmRuntimeError::UnsupportedOperator)?,
                    )
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?;
            }
            Ok(Value::U64(count))
        }
        AggregateFunction::Sum => {
            aggregate_sum(records, input_desc, aggregate).map(nullable_aggregate_value)
        }
        AggregateFunction::Avg => {
            aggregate_avg(records, input_desc, aggregate).map(nullable_aggregate_value)
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            aggregate_extremum(records, input_desc, aggregate).map(nullable_aggregate_value)
        }
    }
}

fn aggregate_sum(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Option<Value>, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut kind = None;
    let mut u64_sum = 0_u64;
    let mut i64_sum = 0_i64;
    let mut f64_sum = 0_f64;
    for (record, weight) in records {
        if *weight <= 0 {
            continue;
        }
        let value = evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
        let Some(value) = unwrap_nullable_value(value) else {
            continue;
        };
        match value {
            Value::U8(value) => {
                kind.get_or_insert(ValueType::U8);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), *weight)?;
            }
            Value::U16(value) => {
                kind.get_or_insert(ValueType::U16);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), *weight)?;
            }
            Value::U32(value) => {
                kind.get_or_insert(ValueType::U32);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), *weight)?;
            }
            Value::U64(value) => {
                kind.get_or_insert(ValueType::U64);
                u64_sum = add_weighted_u64(u64_sum, value, *weight)?;
            }
            Value::I32(value) => {
                kind.get_or_insert(ValueType::I32);
                i64_sum = add_weighted_i64(i64_sum, i64::from(value), *weight)?;
            }
            Value::I64(value) => {
                kind.get_or_insert(ValueType::I64);
                i64_sum = add_weighted_i64(i64_sum, value, *weight)?;
            }
            Value::F64(value) => {
                kind.get_or_insert(ValueType::F64);
                f64_sum += value * (*weight as f64);
            }
            _ => return Err(IvmRuntimeError::UnsupportedOperator),
        }
    }
    match kind {
        None => Ok(None),
        Some(ValueType::U8) => u8::try_from(u64_sum)
            .map(Value::U8)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::U16) => u16::try_from(u64_sum)
            .map(Value::U16)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::U32) => u32::try_from(u64_sum)
            .map(Value::U32)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::U64) => Ok(Some(Value::U64(u64_sum))),
        Some(ValueType::I32) => i32::try_from(i64_sum)
            .map(Value::I32)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::F64) => Ok(Some(Value::F64(f64_sum))),
        Some(ValueType::I64) => Ok(Some(Value::I64(i64_sum))),
        Some(_) => Err(IvmRuntimeError::UnsupportedOperator),
    }
}

fn aggregate_avg(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Option<Value>, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut sum = 0_f64;
    let mut count = 0_i64;
    for (record, weight) in records {
        if *weight <= 0 {
            continue;
        }
        let value = evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
        let Some(value) = unwrap_nullable_value(value) else {
            continue;
        };
        let numeric = numeric_value_as_f64(&value)?;
        sum += numeric * (*weight as f64);
        count += *weight;
    }
    if count <= 0 {
        return Ok(None);
    }
    Ok(Some(Value::F64(sum / (count as f64))))
}

fn aggregate_extremum(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Option<Value>, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut best: Option<(Vec<u8>, Bytes, Value)> = None;
    for (record, weight) in records {
        if *weight <= 0 {
            continue;
        }
        let value = evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
        let Some(value) = unwrap_nullable_value(value) else {
            continue;
        };
        let mut value_key = Vec::new();
        encode_key_part(&mut value_key, &value)?;
        let replaces =
            best.as_ref()
                .is_none_or(|(best_key, best_record, _)| match aggregate.function {
                    AggregateFunction::Min => {
                        value_key < *best_key || (value_key == *best_key && record < best_record)
                    }
                    AggregateFunction::Max => {
                        value_key > *best_key || (value_key == *best_key && record < best_record)
                    }
                    _ => false,
                });
        if replaces {
            best = Some((value_key, record.clone(), value));
        }
    }
    Ok(best.map(|(_, _, value)| value))
}

fn add_weighted_u64(current: u64, value: u64, weight: i64) -> Result<u64, IvmRuntimeError> {
    let weight = u64::try_from(weight).map_err(|_| IvmRuntimeError::AggregateOverflow)?;
    current
        .checked_add(
            value
                .checked_mul(weight)
                .ok_or(IvmRuntimeError::AggregateOverflow)?,
        )
        .ok_or(IvmRuntimeError::AggregateOverflow)
}

fn add_weighted_i64(current: i64, value: i64, weight: i64) -> Result<i64, IvmRuntimeError> {
    current
        .checked_add(
            value
                .checked_mul(weight)
                .ok_or(IvmRuntimeError::AggregateOverflow)?,
        )
        .ok_or(IvmRuntimeError::AggregateOverflow)
}

fn numeric_value_as_f64(value: &Value) -> Result<f64, IvmRuntimeError> {
    match value {
        Value::U8(value) => Ok(f64::from(*value)),
        Value::U16(value) => Ok(f64::from(*value)),
        Value::U32(value) => Ok(f64::from(*value)),
        Value::U64(value) => Ok(*value as f64),
        Value::I32(value) => Ok(f64::from(*value)),
        Value::I64(value) => Ok(*value as f64),
        Value::F64(value) => Ok(*value),
        _ => Err(IvmRuntimeError::UnsupportedOperator),
    }
}

fn nullable_aggregate_value(value: Option<Value>) -> Value {
    Value::Nullable(value.map(Box::new))
}

fn unwrap_nullable_value(value: Value) -> Option<Value> {
    match value {
        Value::Nullable(None) => None,
        Value::Nullable(Some(value)) => Some(*value),
        value => Some(value),
    }
}

fn is_null_value(value: &Value) -> bool {
    matches!(value, Value::Nullable(None))
}

fn evaluate_aggregate_expr(
    record: &BorrowedRecord<'_>,
    expr: &PlanExpr,
) -> Result<Value, IvmRuntimeError> {
    match expr {
        PlanExpr::Field(field) | PlanExpr::Nullable(field) | PlanExpr::NullableFlat(field) => {
            record.get(field).map_err(IvmRuntimeError::RecordEncoding)
        }
        PlanExpr::EnumTagRemap { field, tags } => remap_enum_tag(record.get(field)?, tags),
        PlanExpr::EnumRemap { field, tags } => remap_enum(record.get(field)?, tags),
        // Aggregates do not carry an output descriptor, so there is no
        // well-defined target boundary for a recursive re-encoding.
        PlanExpr::RecursiveEnumRemap { .. } => Err(IvmRuntimeError::UnsupportedOperator),
        PlanExpr::Literal(literal) => Ok(literal.to_value()),
        PlanExpr::Null(value_type) => Ok(Value::Nullable(match value_type {
            ValueType::Nullable(_) => None,
            _ => None,
        })),
    }
}

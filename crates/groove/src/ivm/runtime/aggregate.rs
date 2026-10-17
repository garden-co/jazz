//! Aggregate planning and evaluation for the synchronous IVM runtime.
//!
//! This stays separate from the tick evaluator because it operates solely on
//! reconstructed input multisets and record descriptors.

use super::*;

pub(super) fn resolve_aggregate_expr(
    input: &RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<AggregateExpr, IvmRuntimeError> {
    // Only validate: evaluation resolves this same PlanExpr name again, so
    // rewriting it to a raw carrier could bind a different field's identity.
    if let Some(field) = aggregate.expression.as_ref().and_then(plan_expr_field) {
        super::record_projection::resolve_field_name(input, field)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
    }
    Ok(aggregate.clone())
}

fn plan_expr_field(expr: &PlanExpr) -> Option<&str> {
    match expr {
        PlanExpr::Field(field)
        | PlanExpr::Nullable(field)
        | PlanExpr::NullableFlat(field)
        | PlanExpr::EnumTagRemap { field, .. }
        | PlanExpr::EnumRemap { field, .. }
        | PlanExpr::RecursiveEnumRemap { field, .. } => Some(field),
        PlanExpr::Literal(_) | PlanExpr::Null(_) => None,
    }
}

/// Reconstructs the positive pre-tick input multiset from its post-tick state.
pub(super) fn records_before_from_deltas(
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
) -> Vec<(Bytes, i64)> {
    let mut records = super::windows::records_before_deltas(after_records, &deltas);
    records.retain(|(_, weight)| *weight > 0);
    records
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

/// The non-null expression values of the live (positive-weight) records.
fn live_values<'a>(
    records: &'a [(Bytes, i64)],
    input_desc: RecordDescriptor,
    expr: &'a PlanExpr,
) -> impl Iterator<Item = Result<(&'a Bytes, i64, Value), IvmRuntimeError>> + 'a {
    records
        .iter()
        .filter(|(_, weight)| *weight > 0)
        .filter_map(move |(record, weight)| {
            match evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr) {
                Ok(value) => unwrap_nullable_value(value).map(|value| Ok((record, *weight, value))),
                Err(error) => Some(Err(error)),
            }
        })
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
    for live in live_values(records, input_desc, expr) {
        let (_, weight, value) = live?;
        match value {
            Value::U8(value) => {
                kind.get_or_insert(ValueType::U8);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), weight)?;
            }
            Value::U16(value) => {
                kind.get_or_insert(ValueType::U16);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), weight)?;
            }
            Value::U32(value) => {
                kind.get_or_insert(ValueType::U32);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), weight)?;
            }
            Value::U64(value) => {
                kind.get_or_insert(ValueType::U64);
                u64_sum = add_weighted_u64(u64_sum, value, weight)?;
            }
            Value::I32(value) => {
                kind.get_or_insert(ValueType::I32);
                i64_sum = add_weighted_i64(i64_sum, i64::from(value), weight)?;
            }
            Value::I64(value) => {
                kind.get_or_insert(ValueType::I64);
                i64_sum = add_weighted_i64(i64_sum, value, weight)?;
            }
            Value::F64(value) => {
                kind.get_or_insert(ValueType::F64);
                f64_sum += value * (weight as f64);
            }
            _ => return Err(IvmRuntimeError::UnsupportedOperator),
        }
    }
    match kind {
        None => Ok(None),
        Some(ValueType::U8) => narrow_sum(u64_sum, Value::U8),
        Some(ValueType::U16) => narrow_sum(u64_sum, Value::U16),
        Some(ValueType::U32) => narrow_sum(u64_sum, Value::U32),
        Some(ValueType::U64) => Ok(Some(Value::U64(u64_sum))),
        Some(ValueType::I32) => narrow_sum(i64_sum, Value::I32),
        Some(ValueType::F64) => Ok(Some(Value::F64(f64_sum))),
        Some(ValueType::I64) => Ok(Some(Value::I64(i64_sum))),
        Some(_) => Err(IvmRuntimeError::UnsupportedOperator),
    }
}

/// A sum accumulated in a wide integer, narrowed back to its input type.
fn narrow_sum<W, T: TryFrom<W>>(
    sum: W,
    value: fn(T) -> Value,
) -> Result<Option<Value>, IvmRuntimeError> {
    T::try_from(sum)
        .map(|sum| Some(value(sum)))
        .map_err(|_| IvmRuntimeError::AggregateOverflow)
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
    let mut scale = 0_f64;
    let mut count = 0_i64;
    let mut has_f64 = false;
    let mut has_non_finite = false;
    for live in live_values(records, input_desc, expr) {
        let (_, weight, value) = live?;
        let numeric = numeric_value_as_f64(&value)?;
        sum += numeric * (weight as f64);
        count += weight;
        has_f64 |= matches!(value, Value::F64(_));
        if numeric.is_finite() {
            scale = scale.max(numeric.abs());
        } else {
            has_non_finite = true;
        }
    }
    if count <= 0 {
        return Ok(None);
    }

    // Keep the established accumulation order for integer and non-finite
    // inputs. Scaling is only needed for finite F64 values, and falling back
    // here preserves the existing NaN/infinity behavior exactly.
    if !has_f64 || has_non_finite || sum.is_finite() {
        return Ok(Some(Value::F64(sum / (count as f64))));
    }
    if scale == 0.0 {
        return Ok(Some(Value::F64(0.0)));
    }

    let mut scaled_sum = 0_f64;
    for live in live_values(records, input_desc, expr) {
        let (_, weight, value) = live?;
        let numeric = numeric_value_as_f64(&value)?;
        scaled_sum += (numeric / scale) * (weight as f64);
    }
    let normalized = (scaled_sum / (count as f64)).clamp(-1.0, 1.0);
    Ok(Some(Value::F64(scale * normalized)))
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
    for live in live_values(records, input_desc, expr) {
        let (record, _, value) = live?;
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
        Value::U64(value) | Value::U48(value) => Ok(*value as f64),
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
    let field_value = |field: &str| {
        let field_idx = super::record_projection::resolve_field_name(&record.descriptor(), field)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
        record
            .get_idx(field_idx)
            .map_err(IvmRuntimeError::RecordEncoding)
    };
    match expr {
        PlanExpr::Field(field) | PlanExpr::Nullable(field) | PlanExpr::NullableFlat(field) => {
            field_value(field)
        }
        PlanExpr::EnumTagRemap { field, tags } => remap_enum_tag(field_value(field)?, tags),
        PlanExpr::EnumRemap { field, tags } => remap_enum(field_value(field)?, tags),
        // Aggregates do not carry an output descriptor, so there is no
        // well-defined target boundary for a recursive re-encoding.
        PlanExpr::RecursiveEnumRemap { .. } => Err(IvmRuntimeError::UnsupportedOperator),
        PlanExpr::Literal(literal) => Ok(literal.to_value()),
        PlanExpr::Null(_) => Ok(Value::Nullable(None)),
    }
}

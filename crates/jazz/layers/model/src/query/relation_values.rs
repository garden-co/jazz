/// Decode a relation literal. Queries and relation-backed policies share this
/// so a literal means the same value in both.
///
/// A literal is either a bare JSON value from a query builder or the typed
/// `{"type", "value"}` envelope of a public
/// [`crate::model::public_schema::Value`].
pub(crate) fn relation_literal_to_record_value(value: &serde_json::Value) -> Result<Value, String> {
    match value {
        serde_json::Value::Null => Ok(Value::Nullable(None)),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Ok(Value::U64(value))
            } else if let Some(value) = value.as_i64() {
                Ok(Value::I64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::F64(value))
            } else {
                Err("relation literal is not finite".to_owned())
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(relation_literal_to_record_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        serde_json::Value::Object(_) => {
            let value = serde_json::from_value(value.clone())
                .map_err(|error| format!("object relation literals must be typed values: {error}"))?;
            public_value_to_record_value(&value)
        }
    }
}

/// Lower a typed public value to the record value comparisons evaluate.
pub(crate) fn public_value_to_record_value(
    value: &crate::model::public_schema::Value,
) -> Result<Value, String> {
    use crate::model::public_schema::Value as PublicValue;
    match value {
        PublicValue::Null => Ok(Value::Nullable(None)),
        PublicValue::Boolean(value) => Ok(Value::Bool(*value)),
        PublicValue::Text(value) => Ok(Value::String(value.clone())),
        PublicValue::Integer(value) => Ok(Value::I32(*value)),
        PublicValue::BigInt(value) => Ok(Value::I64(*value)),
        PublicValue::Double(value) if value.is_finite() => Ok(Value::F64(*value)),
        PublicValue::Double(_) => Err("floating-point literals must be finite numbers".to_owned()),
        PublicValue::Timestamp(value) => Ok(Value::U64(*value)),
        PublicValue::Uuid(value) => Ok(Value::Uuid(*value.uuid())),
        PublicValue::Bytea(value) => Ok(Value::Bytes(value.clone())),
        PublicValue::Array(values) => values
            .iter()
            .map(public_value_to_record_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        other => Err(format!("{other:?} literals are not supported yet")),
    }
}

/// Lower one column comparison. A null right-hand side tests nullness, as the
/// TypeScript builders emit it; ordering against null has no meaning.
pub(crate) fn relation_comparison(
    column: &str,
    op: RelationCmpOp,
    right: Operand,
) -> Result<Predicate, String> {
    let left = Operand::Column(column.to_owned());
    if matches!(right, Operand::Literal(Value::Nullable(None))) {
        return match op {
            RelationCmpOp::Eq => Ok(Predicate::IsNull(left)),
            RelationCmpOp::Ne => Ok(Predicate::Not(Box::new(Predicate::IsNull(left)))),
            RelationCmpOp::Lt | RelationCmpOp::Le | RelationCmpOp::Gt | RelationCmpOp::Ge => {
                Err("Ordering comparisons against null are not supported yet".to_owned())
            }
        };
    }
    Ok(match op {
        RelationCmpOp::Eq => Predicate::Eq(left, right),
        RelationCmpOp::Ne => Predicate::Ne(left, right),
        RelationCmpOp::Lt => Predicate::Lt(left, right),
        RelationCmpOp::Le => Predicate::Lte(left, right),
        RelationCmpOp::Gt => Predicate::Gt(left, right),
        RelationCmpOp::Ge => Predicate::Gte(left, right),
    })
}

/// A relation value lowered for a predicate: an operand, or a row the
/// predicate correlates with, which the caller turns into a join or a walk.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RelationOperand {
    Operand(Operand),
    OuterRow(String),
}

/// One top-level conjunct of a lowered relation predicate.
pub(crate) struct RelationConjunct<'a> {
    pub(crate) predicate: Predicate,
    /// The column a leaf test reads; None for compound conjuncts.
    pub(crate) column: Option<&'a RelationColumnRef>,
    /// What a comparison matched against, when it keys a match. Null tests
    /// and compound conjuncts carry none.
    pub(crate) value: Option<RelationOperand>,
    /// Every relation column the conjunct reads, enum payload fields aside.
    pub(crate) columns: Vec<&'a RelationColumnRef>,
}

/// Lower a relation predicate into its top-level conjuncts. Queries and
/// relation-backed policies share this walk; each supplies how value refs
/// lower and decides what the reported columns and correlations mean.
pub(crate) fn lower_relation_conjuncts<'a>(
    predicate: &'a RelationPredicate,
    lower_value: &mut dyn FnMut(&RelationValueRef) -> Result<RelationOperand, String>,
) -> Result<Vec<RelationConjunct<'a>>, String> {
    let leaf = |predicate: Predicate,
                column: &'a RelationColumnRef,
                value: Option<RelationOperand>| RelationConjunct {
        predicate,
        column: Some(column),
        value,
        columns: vec![column],
    };
    let operand = |value: RelationOperand, what: &str| match value {
        RelationOperand::Operand(operand) => Ok(operand),
        RelationOperand::OuterRow(_) => {
            Err(format!("{what} against a row id are not supported yet"))
        }
    };
    match predicate {
        RelationPredicate::True => Ok(Vec::new()),
        RelationPredicate::False => Ok(vec![RelationConjunct {
            predicate: Predicate::Any(Vec::new()),
            column: None,
            value: None,
            columns: Vec::new(),
        }]),
        RelationPredicate::And(children) => {
            let mut conjuncts = Vec::new();
            for child in children {
                conjuncts.extend(lower_relation_conjuncts(child, lower_value)?);
            }
            Ok(conjuncts)
        }
        RelationPredicate::Or(children) => {
            let mut columns = Vec::new();
            let mut alternatives = Vec::new();
            for child in children {
                let (predicate, child_columns) = lower_uncorrelated(child, lower_value, "Or")?;
                if is_always_true(&predicate) {
                    return Ok(Vec::new());
                }
                if !is_always_false(&predicate) {
                    alternatives.push(predicate);
                    columns.extend(child_columns);
                }
            }
            Ok(vec![RelationConjunct {
                predicate: Predicate::Any(alternatives),
                column: None,
                value: None,
                columns,
            }])
        }
        RelationPredicate::Not(child) => {
            let (predicate, columns) = lower_uncorrelated(child, lower_value, "Not")?;
            if is_always_false(&predicate) {
                return Ok(Vec::new());
            }
            let predicate = if is_always_true(&predicate) {
                Predicate::Any(Vec::new())
            } else {
                Predicate::Not(Box::new(predicate))
            };
            Ok(vec![RelationConjunct {
                predicate,
                column: None,
                value: None,
                columns,
            }])
        }
        RelationPredicate::Cmp { left, op, right } => {
            let value = lower_value(right)?;
            let predicate = match &value {
                RelationOperand::Operand(operand) => {
                    relation_comparison(&left.column, *op, operand.clone())?
                }
                // A row-id equality is a correlation the caller consumes.
                RelationOperand::OuterRow(_) if *op == RelationCmpOp::Eq => {
                    Predicate::All(Vec::new())
                }
                RelationOperand::OuterRow(_) => {
                    return Err(
                        "Row-id comparisons other than equality are not supported yet".to_owned(),
                    );
                }
            };
            // A null test keys nothing, so it carries no value to match on.
            let keys = !matches!(
                value,
                RelationOperand::Operand(Operand::Literal(Value::Nullable(None)))
            );
            Ok(vec![leaf(predicate, left, keys.then_some(value))])
        }
        RelationPredicate::IsNull { column } => Ok(vec![leaf(
            Predicate::IsNull(Operand::Column(column.column.clone())),
            column,
            None,
        )]),
        RelationPredicate::IsNotNull { column } => Ok(vec![leaf(
            Predicate::Not(Box::new(Predicate::IsNull(Operand::Column(
                column.column.clone(),
            )))),
            column,
            None,
        )]),
        RelationPredicate::Contains { left, right } => {
            let right = operand(lower_value(right)?, "Contains tests")?;
            Ok(vec![leaf(
                Predicate::Contains(Operand::Column(left.column.clone()), right),
                left,
                None,
            )])
        }
        RelationPredicate::In { left, values } => {
            let values = values
                .iter()
                .map(|value| operand(lower_value(value)?, "In tests"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(vec![leaf(
                Predicate::In(Operand::Column(left.column.clone()), values),
                left,
                None,
            )])
        }
        RelationPredicate::EnumMatch {
            column,
            case,
            payload,
        } => {
            // Payload fields name the matched case's own fields, not relation
            // sources, so they stay unscoped and are not reported as columns.
            let (payload, payload_columns) = lower_uncorrelated(payload, lower_value, "EnumMatch")?;
            if payload_columns.iter().any(|column| column.scope.is_some()) {
                return Err("Scoped enum payload fields are not supported yet".to_owned());
            }
            if relation_predicate_has_enum_match(&payload) {
                return Err("Nested enum payload matches are not supported yet".to_owned());
            }
            Ok(vec![leaf(
                Predicate::EnumMatch {
                    column: column.column.clone(),
                    case: case.clone(),
                    payload: Box::new(payload),
                },
                column,
                None,
            )])
        }
    }
}

/// Lower a predicate under `Or`, `Not` or an enum payload, where a row-id
/// correlation cannot become a join: treating it as `true` would widen the
/// predicate, so it is rejected.
fn lower_uncorrelated<'a>(
    predicate: &'a RelationPredicate,
    lower_value: &mut dyn FnMut(&RelationValueRef) -> Result<RelationOperand, String>,
    under: &str,
) -> Result<(Predicate, Vec<&'a RelationColumnRef>), String> {
    let conjuncts = lower_relation_conjuncts(predicate, lower_value)?;
    let mut columns = Vec::new();
    let mut predicates = Vec::new();
    for conjunct in conjuncts {
        if matches!(conjunct.value, Some(RelationOperand::OuterRow(_))) {
            return Err(format!(
                "Row-id correlations under {under} are not supported yet"
            ));
        }
        columns.extend(conjunct.columns);
        predicates.push(conjunct.predicate);
    }
    let predicate = match <[Predicate; 1]>::try_from(predicates) {
        Ok([predicate]) => predicate,
        Err(predicates) => Predicate::All(predicates),
    };
    Ok((predicate, columns))
}

fn is_always_true(predicate: &Predicate) -> bool {
    matches!(predicate, Predicate::All(predicates) if predicates.is_empty())
}

fn is_always_false(predicate: &Predicate) -> bool {
    matches!(predicate, Predicate::Any(predicates) if predicates.is_empty())
}

fn relation_predicate_has_enum_match(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::EnumMatch { .. } => true,
        Predicate::All(children) | Predicate::Any(children) => {
            children.iter().any(relation_predicate_has_enum_match)
        }
        Predicate::Not(child) => relation_predicate_has_enum_match(child),
        _ => false,
    }
}

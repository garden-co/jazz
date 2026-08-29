//! Normalize validated Jazz queries and policy clauses into the query engine's
//! logical row-set representation.
//!
//! This stage assigns stable source identities, coerces predicates and
//! parameters, expands joins, arrays, reachability, and inherited policies, and
//! describes requested outputs. It does not choose physical storage sources or
//! build executable Groove graphs.

use super::*;

pub(super) fn root_source_id(table: &str) -> SourceId {
    SourceId {
        table: table.to_owned(),
        path: SourcePath {
            components: vec![SourceRole::Root],
        },
    }
}

fn nested_join_source_id(join: &JoinVia, path: &str) -> SourceId {
    SourceId {
        table: join.table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(path.to_owned())],
        },
    }
}

fn join_lookup_source_id(lookup: &crate::query::JoinSourceLookup, path: &str) -> SourceId {
    SourceId {
        table: lookup.table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(format!("{path}:source_lookup"))],
        },
    }
}

pub(super) fn current_query_output_request(
    output: CurrentQueryProgramOutput,
    query: &JazzQuery,
    schema: &RuntimeSchema,
) -> RowSetOutputRequest {
    let facts = match output {
        CurrentQueryProgramOutput::AppRows | CurrentQueryProgramOutput::PolicyPredicate => {
            BTreeSet::new()
        }
        CurrentQueryProgramOutput::AuthorizedRows => {
            BTreeSet::from([ProgramFactKey::AuthorizedRows])
        }
        CurrentQueryProgramOutput::RelationSnapshot if !query.array_subqueries.is_empty() => {
            BTreeSet::from([
                ProgramFactKey::RelationEdges,
                ProgramFactKey::PathCorrelationCoverage,
            ])
        }
        CurrentQueryProgramOutput::RelationSnapshot => BTreeSet::new(),
        CurrentQueryProgramOutput::MaintainedView if !query.array_subqueries.is_empty() => {
            BTreeSet::from([
                ProgramFactKey::ResultMembership,
                ProgramFactKey::VersionWitnesses,
                ProgramFactKey::ReplacementWitnesses,
                ProgramFactKey::RelationEdges,
            ])
        }
        CurrentQueryProgramOutput::MaintainedView => BTreeSet::from([
            ProgramFactKey::ResultMembership,
            ProgramFactKey::VersionWitnesses,
            ProgramFactKey::ReplacementWitnesses,
        ]),
    };
    RowSetOutputRequest {
        app_rows: (matches!(
            output,
            CurrentQueryProgramOutput::AppRows
                | CurrentQueryProgramOutput::PolicyPredicate
                | CurrentQueryProgramOutput::RelationSnapshot
                | CurrentQueryProgramOutput::MaintainedView
        ))
        .then(|| AppRowOutputRequest {
            public_terminal: !matches!(output, CurrentQueryProgramOutput::PolicyPredicate),
            projection: app_row_payload_projection(
                query,
                schema,
                matches!(output, CurrentQueryProgramOutput::MaintainedView)
                    || !query.array_subqueries.is_empty(),
            ),
        }),
        facts,
    }
}

/// Whether a maintained current-read can retain only its delivered result
/// members and re-load their immutable content bodies from storage.
///
/// This is intentionally a proof for the small common case, not a heuristic:
/// one default-view root table, no relation/recursive/output expansion, and
/// no aggregate.  Direct predicate-only policy alternatives are safe because
/// their membership decision remains entirely in the root result terminal.
/// Any shape that needs source provenance beyond that terminal keeps the
/// self-contained version/replacement witness path.
pub(super) fn storage_backed_maintained_view_eligible(
    query: &JazzQuery,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
    normalized: &NormalizedRowSetShape,
) -> bool {
    // The fallback reads the settled physical register to recover an omitted
    // deletion/restore winner.  Edge and Local have separate ahead-current
    // visibility rules, so they retain their self-contained witnesses until
    // that fallback has an equally exact physical resolver.
    tier == DurabilityTier::Global
        && read_view.is_default()
        && query.joins.is_empty()
        && query.flat_join.is_none()
        && query.reachable.is_empty()
        && query.inherits.is_empty()
        && query.includes.is_empty()
        && query.array_subqueries.is_empty()
        && query.aggregate.is_none()
        && query.policy_branches.iter().all(|branch| {
            branch.joins.is_empty() && branch.reachable.is_empty() && branch.inherits.is_empty()
        })
        // `JazzQuery::includes` captures only caller-requested includes.
        // Normalization also injects the default root-reference closure for
        // reference-bearing tables, and that auxiliary source needs its
        // version/replacement witnesses on a separate receiving node.  The
        // storage-backed subset is consequently a proof over the normalized
        // program, not a surface-query shortcut.
        && normalized.closure_paths.is_empty()
        && normalized.auxiliary_sources.is_empty()
        && normalized.join_contributions.is_empty()
        && normalized.reachable_contributions.is_empty()
}

fn app_row_payload_projection(
    query: &JazzQuery,
    schema: &RuntimeSchema,
    collect_relations: bool,
) -> PayloadProjection {
    let paths = if collect_relations {
        app_row_path_projections(
            schema,
            &root_source_id(&query.table),
            &query.array_subqueries,
            &[],
        )
    } else {
        Vec::new()
    };
    if query.select.is_none() && paths.is_empty() {
        return PayloadProjection::ShapeDefault;
    }
    let fields = query
        .select
        .as_ref()
        .map(|select| {
            let mut fields = select
                .iter()
                .filter(|field| !is_implicit_row_id_alias(schema, &query.table, field))
                .cloned()
                .collect::<BTreeSet<_>>();
            for include in &query.includes {
                if let Some(root_field) = include.path.split('.').next() {
                    fields.insert(root_field.to_owned());
                }
            }
            FieldProjection::Fields(fields)
        })
        .unwrap_or(FieldProjection::All);
    PayloadProjection::Tree(AppProjectionTree { fields, paths })
}

fn app_row_path_projections(
    schema: &RuntimeSchema,
    owner: &SourceId,
    subqueries: &[ArraySubquery],
    path: &[usize],
) -> Vec<super::query_engine::AppPathProjection> {
    subqueries
        .iter()
        .enumerate()
        .map(|(index, subquery)| {
            let mut child_path = path.to_vec();
            child_path.push(index);
            let child = correlated_child_source_id(owner, subquery, &child_path);
            let fields = subquery
                .select
                .as_ref()
                .map(|select| {
                    FieldProjection::Fields(
                        select
                            .iter()
                            .filter(|field| !is_implicit_row_id_alias(schema, &child.table, field))
                            .cloned()
                            .collect(),
                    )
                })
                .unwrap_or(FieldProjection::All);
            super::query_engine::AppPathProjection {
                path: ProgramPathId {
                    owner: owner.clone(),
                    child: child.clone(),
                },
                field: subquery.column_name.clone(),
                cardinality: PathCardinality::Many,
                fields,
                children: app_row_path_projections(
                    schema,
                    &child,
                    &subquery.nested_arrays,
                    &child_path,
                ),
                hole_policy: PathHolePolicy::KeepParentWithHoles,
            }
        })
        .collect()
}

/// The legacy `id` spelling resolves to the physical row UUID only for a table
/// that does not declare an application column by that name. Projection must
/// use the same effective-column rule as predicate and ordering normalization:
/// a declared application `id` remains in the payload, while the physical UUID
/// is already carried separately by the row envelope.
fn is_implicit_row_id_alias(schema: &RuntimeSchema, table: &str, field: &str) -> bool {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .is_some_and(|table| crate::query::is_implicit_row_id_alias(table, field))
}

pub(super) fn required_field_idx(
    descriptor: &RecordDescriptor,
    field: &str,
) -> Result<usize, Error> {
    descriptor.field_index(field).ok_or_else(|| {
        Error::QueryLowering(format!(
            "query-engine relation snapshot sink did not emit field '{field}'"
        ))
    })
}

fn normalize_predicates(
    schema: &RuntimeSchema,
    source: &SourceId,
    predicates: &[Predicate],
    flat_join_physical_alias: bool,
) -> Result<NormalizedPredicateExpr, Error> {
    match predicates {
        [] => Ok(NormalizedPredicateExpr::True),
        [predicate] => normalize_predicate(schema, source, predicate, flat_join_physical_alias),
        _ => predicates
            .iter()
            .map(|predicate| {
                normalize_predicate(schema, source, predicate, flat_join_physical_alias)
            })
            .collect::<Result<Vec<_>, Error>>()
            .map(NormalizedPredicateExpr::And),
    }
}

fn route_flat_join_filters(
    predicate: &Predicate,
    root_scope: &str,
    routed: &mut BTreeMap<String, Vec<Predicate>>,
) -> Result<(), Error> {
    if let Predicate::All(predicates) = predicate {
        for predicate in predicates {
            route_flat_join_filters(predicate, root_scope, routed)?;
        }
        return Ok(());
    }

    let sources = crate::query::flat_join_predicate_sources(predicate)?;
    let scope = sources
        .iter()
        .next()
        .map(String::as_str)
        .unwrap_or(root_scope);
    routed
        .entry(scope.to_owned())
        .or_default()
        .push(crate::query::unqualify_flat_join_predicate(
            predicate, scope,
        )?);
    Ok(())
}

fn flat_join_filters_by_source(
    filters: &[Predicate],
    root_scope: &str,
) -> Result<BTreeMap<String, Vec<Predicate>>, Error> {
    let mut routed = BTreeMap::new();
    for predicate in filters {
        route_flat_join_filters(predicate, root_scope, &mut routed)?;
    }
    Ok(routed)
}

pub(super) fn root_literal_equalities(
    query: &JazzQuery,
    binding: &Binding,
) -> Result<BTreeMap<String, Value>, Error> {
    literal_equalities_for_filters(&query.filters, binding)
}

pub(super) fn literal_equalities_for_filters(
    filters: &[Predicate],
    binding: &Binding,
) -> Result<BTreeMap<String, Value>, Error> {
    let mut equalities = BTreeMap::new();
    for predicate in filters {
        collect_root_literal_equalities(predicate, binding, &mut equalities)?;
    }
    Ok(equalities)
}

fn collect_root_literal_equalities(
    predicate: &Predicate,
    binding: &Binding,
    equalities: &mut BTreeMap<String, Value>,
) -> Result<(), Error> {
    match predicate {
        Predicate::All(predicates) => {
            for predicate in predicates {
                collect_root_literal_equalities(predicate, binding, equalities)?;
            }
        }
        Predicate::Eq(left, right) => {
            if let Some((field, value)) = root_equality_literal(left, right, binding)? {
                equalities.entry(field).or_insert(value);
            } else if let Some((field, value)) = root_equality_literal(right, left, binding)? {
                equalities.entry(field).or_insert(value);
            }
        }
        Predicate::Any(_)
        | Predicate::Not(_)
        | Predicate::Ne(_, _)
        | Predicate::In(_, _)
        | Predicate::Gt(_, _)
        | Predicate::Gte(_, _)
        | Predicate::Lt(_, _)
        | Predicate::Lte(_, _)
        | Predicate::Contains(_, _)
        | Predicate::EnumMatch { .. }
        | Predicate::IsNull(_) => {}
    }
    Ok(())
}

fn root_equality_literal(
    field: &Operand,
    value: &Operand,
    binding: &Binding,
) -> Result<Option<(String, Value)>, Error> {
    let Operand::Column(column) = field else {
        return Ok(None);
    };
    let value = match value {
        Operand::Literal(value) => value.clone(),
        Operand::Param(name) => binding
            .values()
            .get(name)
            .cloned()
            .ok_or_else(|| QueryError::MissingParam(name.clone()))?,
        Operand::Column(_) | Operand::Claim(_) => return Ok(None),
    };
    Ok(Some((column.clone(), value)))
}

pub(super) fn select_current_access_path(
    table: &TableSchema,
    equalities: &BTreeMap<String, Value>,
) -> Option<CurrentAccessPath> {
    let has_declared_id = table.columns.iter().any(|column| column.name == "id");
    if !has_declared_id && let Some(value) = equalities.get("id").cloned() {
        return Some(CurrentAccessPath::PrimaryKey(vec![value]));
    }
    let mut probes = Vec::new();
    for column in table.global_current_indexed_columns() {
        if let Some(value) = equalities.get(&column).cloned() {
            probes.push((column, vec![Value::Nullable(Some(Box::new(value)))]));
        }
    }
    let (column, prefix) = probes.first()?.clone();
    Some(CurrentAccessPath::Index {
        column,
        prefix,
        intersections: probes.into_iter().skip(1).collect(),
        maintained: false,
        source_limit: None,
    })
}

pub(super) fn static_scan_for_prefix(prefix: Vec<Value>, full_key_len: usize) -> StaticScanSpec {
    let values = prefix
        .into_iter()
        .map(LiteralValue::from)
        .collect::<Vec<_>>();
    if values.len() == full_key_len {
        StaticScanSpec::Point(values)
    } else {
        StaticScanSpec::Prefix(values)
    }
}

fn normalize_predicate(
    schema: &RuntimeSchema,
    source: &SourceId,
    predicate: &Predicate,
    flat_join_physical_alias: bool,
) -> Result<NormalizedPredicateExpr, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => NormalizedPredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| {
                    normalize_predicate(schema, source, predicate, flat_join_physical_alias)
                })
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Any(predicates) => NormalizedPredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| {
                    normalize_predicate(schema, source, predicate, flat_join_physical_alias)
                })
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Not(predicate) => NormalizedPredicateExpr::Not(Box::new(normalize_predicate(
            schema,
            source,
            predicate,
            flat_join_physical_alias,
        )?)),
        Predicate::Eq(left, right) => normalize_compare(
            schema,
            source,
            left,
            NormalizedComparisonOp::Eq,
            right,
            flat_join_physical_alias,
        )?,
        Predicate::Ne(left, right) => normalize_compare(
            schema,
            source,
            left,
            NormalizedComparisonOp::Ne,
            right,
            flat_join_physical_alias,
        )?,
        Predicate::Gt(left, right) => normalize_compare(
            schema,
            source,
            left,
            NormalizedComparisonOp::Gt,
            right,
            flat_join_physical_alias,
        )?,
        Predicate::Gte(left, right) => normalize_compare(
            schema,
            source,
            left,
            NormalizedComparisonOp::Gte,
            right,
            flat_join_physical_alias,
        )?,
        Predicate::Lt(left, right) => normalize_compare(
            schema,
            source,
            left,
            NormalizedComparisonOp::Lt,
            right,
            flat_join_physical_alias,
        )?,
        Predicate::Lte(left, right) => normalize_compare(
            schema,
            source,
            left,
            NormalizedComparisonOp::Lte,
            right,
            flat_join_physical_alias,
        )?,
        Predicate::In(value, options) => NormalizedPredicateExpr::In {
            value: normalize_predicate_operand_for_schema(
                schema,
                source,
                value,
                None,
                flat_join_physical_alias,
            )?,
            options: options
                .iter()
                .map(|operand| {
                    normalize_predicate_operand_for_schema(
                        schema,
                        source,
                        operand,
                        predicate_operand_column_type(
                            schema,
                            source,
                            value,
                            flat_join_physical_alias,
                        )?
                        .as_ref(),
                        flat_join_physical_alias,
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?,
        },
        Predicate::Contains(value, needle) => NormalizedPredicateExpr::ArrayContains {
            value: normalize_predicate_operand_for_schema(
                schema,
                source,
                value,
                None,
                flat_join_physical_alias,
            )?,
            needle: normalize_predicate_operand_for_schema(
                schema,
                source,
                needle,
                contains_needle_type(schema, source, value)?.as_ref(),
                flat_join_physical_alias,
            )?,
        },
        Predicate::IsNull(value) => {
            NormalizedPredicateExpr::IsNull(normalize_predicate_operand_for_schema(
                schema,
                source,
                value,
                None,
                flat_join_physical_alias,
            )?)
        }
        Predicate::EnumMatch {
            column,
            case,
            payload,
        } => {
            let column_type =
                operand_column_type(schema, source, &Operand::Column(column.clone()))?.ok_or_else(
                    || Error::QueryLowering("enum match column has no type".to_owned()),
                )?;
            let column_type = match column_type {
                ColumnType::Nullable(inner) => *inner,
                other => other,
            };
            let ColumnType::Enum(enum_schema) = column_type else {
                return Err(Error::QueryLowering(
                    "enum match requires a payload enum column".to_owned(),
                ));
            };
            let case_tag = enum_schema
                .tag(case)
                .map_err(|_| Error::QueryLowering(format!("unknown payload enum case {case}")))?;
            let enum_case = enum_schema
                .case(case_tag)
                .map_err(|_| Error::QueryLowering(format!("unknown payload enum case {case}")))?;
            NormalizedPredicateExpr::EnumMatch {
                value: normalize_operand(source, &Operand::Column(column.clone()))?,
                case_tag,
                payload: Box::new(normalize_enum_payload_predicate(
                    &enum_case.payload,
                    source,
                    payload,
                )?),
            }
        }
    })
}

/// Normalize a predicate evaluated inside one selected payload-enum case.
///
/// Payload fields are case-local. They must never be resolved against the
/// outer table, even when that table happens to have the same field name.
pub(super) fn normalize_enum_payload_predicate(
    descriptor: &crate::groove::records::RecordDescriptor,
    source: &SourceId,
    predicate: &Predicate,
) -> Result<NormalizedPredicateExpr, Error> {
    Ok(match predicate {
        Predicate::All(predicates) => NormalizedPredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| normalize_enum_payload_predicate(descriptor, source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Any(predicates) => NormalizedPredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| normalize_enum_payload_predicate(descriptor, source, predicate))
                .collect::<Result<Vec<_>, Error>>()?,
        ),
        Predicate::Not(predicate) => NormalizedPredicateExpr::Not(Box::new(
            normalize_enum_payload_predicate(descriptor, source, predicate)?,
        )),
        Predicate::Eq(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Eq,
            right,
        )?,
        Predicate::Ne(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Ne,
            right,
        )?,
        Predicate::Gt(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Gt,
            right,
        )?,
        Predicate::Gte(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Gte,
            right,
        )?,
        Predicate::Lt(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Lt,
            right,
        )?,
        Predicate::Lte(left, right) => normalize_enum_payload_compare(
            descriptor,
            source,
            left,
            NormalizedComparisonOp::Lte,
            right,
        )?,
        Predicate::In(value, options) => {
            let target_type = enum_payload_operand_type(descriptor, value)?;
            NormalizedPredicateExpr::In {
                value: normalize_enum_payload_operand(descriptor, source, value, None)?,
                options: options
                    .iter()
                    .map(|operand| {
                        normalize_enum_payload_operand(
                            descriptor,
                            source,
                            operand,
                            target_type.as_ref(),
                        )
                    })
                    .collect::<Result<Vec<_>, Error>>()?,
            }
        }
        Predicate::Contains(value, needle) => {
            let needle_type = enum_payload_contains_needle_type(descriptor, value)?;
            NormalizedPredicateExpr::ArrayContains {
                value: normalize_enum_payload_operand(descriptor, source, value, None)?,
                needle: normalize_enum_payload_operand(
                    descriptor,
                    source,
                    needle,
                    needle_type.as_ref(),
                )?,
            }
        }
        Predicate::IsNull(value) => NormalizedPredicateExpr::IsNull(
            normalize_enum_payload_operand(descriptor, source, value, None)?,
        ),
        Predicate::EnumMatch { .. } => {
            return Err(Error::QueryLowering(
                "nested payload enum matches are not supported".to_owned(),
            ));
        }
    })
}

fn normalize_enum_payload_compare(
    descriptor: &crate::groove::records::RecordDescriptor,
    source: &SourceId,
    left: &Operand,
    op: NormalizedComparisonOp,
    right: &Operand,
) -> Result<NormalizedPredicateExpr, Error> {
    let left_type = enum_payload_operand_type(descriptor, left)?;
    let right_type = enum_payload_operand_type(descriptor, right)?;
    Ok(NormalizedPredicateExpr::Compare {
        left: normalize_enum_payload_operand(descriptor, source, left, right_type.as_ref())?,
        op,
        right: normalize_enum_payload_operand(descriptor, source, right, left_type.as_ref())?,
    })
}

fn normalize_enum_payload_operand(
    descriptor: &crate::groove::records::RecordDescriptor,
    source: &SourceId,
    operand: &Operand,
    target_type: Option<&ColumnType>,
) -> Result<NormalizedValueRef, Error> {
    match operand {
        Operand::Column(column) => {
            if enum_payload_field_type(descriptor, column).is_none() {
                return Err(Error::QueryLowering(format!(
                    "unknown payload enum field {column}"
                )));
            }
            Ok(NormalizedValueRef::SourceField {
                source: source.clone(),
                field: column.clone(),
            })
        }
        Operand::Param(param) => Ok(NormalizedValueRef::Param(param.clone())),
        Operand::Claim(claim) => Ok(NormalizedValueRef::Claim(ClaimPath(
            crate::query::operand_claim_path(claim),
        ))),
        Operand::Literal(value) => {
            let value = target_type
                .map(|target_type| coerce_literal_for_column_type(value.clone(), target_type))
                .unwrap_or_else(|| value.clone());
            Ok(NormalizedValueRef::Literal(
                postcard::to_allocvec(&value).map_err(|err| {
                    Error::QueryLowering(format!("literal encoding failed: {err}"))
                })?,
            ))
        }
    }
}

fn enum_payload_operand_type(
    descriptor: &crate::groove::records::RecordDescriptor,
    operand: &Operand,
) -> Result<Option<ColumnType>, Error> {
    match operand {
        Operand::Column(column) => enum_payload_field_type(descriptor, column)
            .map(Some)
            .ok_or_else(|| Error::QueryLowering(format!("unknown payload enum field {column}"))),
        Operand::Literal(_) | Operand::Param(_) | Operand::Claim(_) => Ok(None),
    }
}

fn enum_payload_field_type(
    descriptor: &crate::groove::records::RecordDescriptor,
    field: &str,
) -> Option<ColumnType> {
    descriptor
        .fields()
        .iter()
        .find(|candidate| candidate.name.as_deref() == Some(field))
        .map(|candidate| candidate.value_type.clone())
}

fn enum_payload_contains_needle_type(
    descriptor: &crate::groove::records::RecordDescriptor,
    value: &Operand,
) -> Result<Option<ColumnType>, Error> {
    Ok(match enum_payload_operand_type(descriptor, value)? {
        Some(ColumnType::Array(member)) => Some(*member),
        Some(ColumnType::Nullable(inner)) => match *inner {
            ColumnType::Array(member) => Some(*member),
            ColumnType::String => Some(ColumnType::String),
            _ => None,
        },
        Some(ColumnType::String) => Some(ColumnType::String),
        _ => None,
    })
}

fn normalize_compare(
    schema: &RuntimeSchema,
    source: &SourceId,
    left: &Operand,
    op: NormalizedComparisonOp,
    right: &Operand,
    flat_join_physical_alias: bool,
) -> Result<NormalizedPredicateExpr, Error> {
    let left_type = predicate_operand_column_type(schema, source, left, flat_join_physical_alias)?;
    let right_type =
        predicate_operand_column_type(schema, source, right, flat_join_physical_alias)?;
    Ok(NormalizedPredicateExpr::Compare {
        left: normalize_predicate_operand_for_schema(
            schema,
            source,
            left,
            right_type.as_ref(),
            flat_join_physical_alias,
        )?,
        op,
        right: normalize_predicate_operand_for_schema(
            schema,
            source,
            right,
            left_type.as_ref(),
            flat_join_physical_alias,
        )?,
    })
}

fn normalize_operand(source: &SourceId, operand: &Operand) -> Result<NormalizedValueRef, Error> {
    normalize_operand_with_target_type(source, operand, None)
}

fn source_has_declared_id(schema: &RuntimeSchema, source: &SourceId) -> bool {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == source.table)
        .is_some_and(|table| !crate::query::is_implicit_row_id_alias(table, "id"))
}

fn normalize_operand_for_schema(
    schema: &RuntimeSchema,
    source: &SourceId,
    operand: &Operand,
) -> Result<NormalizedValueRef, Error> {
    normalize_operand_with_target_type_and_declared_id(
        source,
        operand,
        None,
        source_has_declared_id(schema, source),
    )
}

fn normalize_predicate_operand_for_schema(
    schema: &RuntimeSchema,
    source: &SourceId,
    operand: &Operand,
    target_type: Option<&ColumnType>,
    flat_join_physical_alias: bool,
) -> Result<NormalizedValueRef, Error> {
    normalize_operand_with_target_type_and_declared_id_and_flat_join_alias(
        source,
        operand,
        target_type,
        source_has_declared_id(schema, source),
        flat_join_physical_alias,
    )
}

fn normalize_operand_with_target_type(
    source: &SourceId,
    operand: &Operand,
    target_type: Option<&ColumnType>,
) -> Result<NormalizedValueRef, Error> {
    normalize_operand_with_target_type_and_declared_id(source, operand, target_type, false)
}

fn normalize_operand_with_target_type_and_declared_id(
    source: &SourceId,
    operand: &Operand,
    target_type: Option<&ColumnType>,
    has_declared_id: bool,
) -> Result<NormalizedValueRef, Error> {
    normalize_operand_with_target_type_and_declared_id_and_flat_join_alias(
        source,
        operand,
        target_type,
        has_declared_id,
        false,
    )
}

fn normalize_operand_with_target_type_and_declared_id_and_flat_join_alias(
    source: &SourceId,
    operand: &Operand,
    target_type: Option<&ColumnType>,
    has_declared_id: bool,
    flat_join_physical_alias: bool,
) -> Result<NormalizedValueRef, Error> {
    Ok(match operand {
        Operand::Column(column) if flat_join_physical_alias && column == "_id" => {
            NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
        }
        Operand::Column(column) if column == "id" && !has_declared_id => {
            NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
        }
        Operand::Column(column) => match provenance_field(column) {
            Some(field) => NormalizedValueRef::Provenance {
                source: source.clone(),
                field,
            },
            None => NormalizedValueRef::SourceField {
                source: source.clone(),
                field: column.clone(),
            },
        },
        Operand::Param(param) => NormalizedValueRef::Param(param.clone()),
        Operand::Claim(claim) => {
            NormalizedValueRef::Claim(ClaimPath(crate::query::operand_claim_path(claim)))
        }
        Operand::Literal(value) => {
            let value = target_type
                .map(|target_type| coerce_literal_for_column_type(value.clone(), target_type))
                .unwrap_or_else(|| value.clone());
            NormalizedValueRef::Literal(
                postcard::to_allocvec(&value).map_err(|err| {
                    Error::QueryLowering(format!("literal encoding failed: {err}"))
                })?,
            )
        }
    })
}

fn predicate_operand_column_type(
    schema: &RuntimeSchema,
    source: &SourceId,
    operand: &Operand,
    flat_join_physical_alias: bool,
) -> Result<Option<ColumnType>, Error> {
    if flat_join_physical_alias && matches!(operand, Operand::Column(column) if column == "_id") {
        return Ok(Some(ColumnType::Uuid));
    }
    operand_column_type(schema, source, operand)
}

pub(super) fn operand_column_type(
    schema: &RuntimeSchema,
    source: &SourceId,
    operand: &Operand,
) -> Result<Option<ColumnType>, Error> {
    let Operand::Column(column) = operand else {
        return Ok(None);
    };
    if let Some(field) = provenance_field(column) {
        return Ok(Some(match field {
            ProvenanceField::CreatedAt | ProvenanceField::UpdatedAt => ColumnType::U64,
            ProvenanceField::CreatedBy | ProvenanceField::UpdatedBy => ColumnType::Uuid,
        }));
    }
    let table = match table_schema(schema, &source.table) {
        Ok(table) => table,
        Err(_) if column == "id" => return Ok(Some(ColumnType::Uuid)),
        Err(error) => return Err(error),
    };
    let declared = table
        .columns
        .iter()
        .find(|candidate| candidate.name == *column)
        .map(|column| column.column_type.clone());
    if declared.is_some() {
        return Ok(declared);
    }
    if column == "id" {
        return Ok(Some(ColumnType::Uuid));
    }
    Ok(None)
}

pub(super) fn contains_needle_type(
    schema: &RuntimeSchema,
    source: &SourceId,
    value: &Operand,
) -> Result<Option<ColumnType>, Error> {
    Ok(match operand_column_type(schema, source, value)? {
        Some(ColumnType::Array(member)) => Some(*member),
        Some(ColumnType::Nullable(inner)) => match *inner {
            ColumnType::Array(member) => Some(*member),
            ColumnType::String => Some(ColumnType::String),
            _ => None,
        },
        Some(ColumnType::String) => Some(ColumnType::String),
        _ => None,
    })
}

pub(super) fn coerce_literal_for_column_type(value: Value, column_type: &ColumnType) -> Value {
    match (value, column_type) {
        (Value::Uuid(value), ColumnType::String) => Value::String(value.to_string()),
        (Value::String(value), ColumnType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .unwrap_or(Value::String(value)),
        (Value::Nullable(Some(value)), ColumnType::Nullable(inner)) => Value::Nullable(Some(
            Box::new(coerce_literal_for_column_type(*value, inner)),
        )),
        (Value::Array(values), ColumnType::Array(inner)) => Value::Array(
            values
                .into_iter()
                .map(|value| coerce_literal_for_column_type(value, inner))
                .collect(),
        ),
        (Value::Tuple(values), ColumnType::Tuple(types)) if values.len() == types.len() => {
            Value::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, column_type)| coerce_literal_for_column_type(value, column_type))
                    .collect(),
            )
        }
        (Value::Nullable(Some(value)), column_type) => Value::Nullable(Some(Box::new(
            coerce_literal_for_column_type(*value, column_type),
        ))),
        (value, ColumnType::Nullable(inner)) => coerce_literal_for_column_type(value, inner),
        (value, _) => value,
    }
}

fn provenance_field(column: &str) -> Option<ProvenanceField> {
    match column {
        "$createdAt" => Some(ProvenanceField::CreatedAt),
        "$createdBy" => Some(ProvenanceField::CreatedBy),
        "$updatedAt" => Some(ProvenanceField::UpdatedAt),
        "$updatedBy" => Some(ProvenanceField::UpdatedBy),
        _ => None,
    }
}

fn normalize_order_key(
    schema: &RuntimeSchema,
    source: &SourceId,
    order: &crate::query::OrderBy,
) -> Result<NormalizedOrderKey, Error> {
    Ok(NormalizedOrderKey {
        value: normalize_operand_for_schema(
            schema,
            source,
            &Operand::Column(order.column.clone()),
        )?,
        direction: match order.direction {
            OrderDirection::Asc => NormalizedSortDirection::Asc,
            OrderDirection::Desc => NormalizedSortDirection::Desc,
        },
    })
}

fn normalized_aggregate_group_by(
    schema: &RuntimeSchema,
    source: &SourceId,
    aggregate: &AggregateQuery,
) -> Result<Vec<NormalizedValueRef>, Error> {
    aggregate
        .group_by
        .iter()
        .map(|column| {
            normalize_operand_for_schema(schema, source, &Operand::Column(column.clone()))
        })
        .collect()
}

fn normalized_aggregate_outputs(
    schema: &RuntimeSchema,
    source: &SourceId,
    aggregate: &AggregateQuery,
) -> Result<Vec<NormalizedAggregateExpr>, Error> {
    aggregate
        .aggregates
        .iter()
        .map(|aggregate| {
            Ok(NormalizedAggregateExpr {
                output: typed_output_field(
                    aggregate_output_field(&aggregate.alias),
                    normalized_aggregate_output_type(aggregate),
                ),
                function: normalized_aggregate_function(aggregate.function),
                input: aggregate
                    .column
                    .as_ref()
                    .map(|column| {
                        normalize_operand_for_schema(
                            schema,
                            source,
                            &Operand::Column(column.clone()),
                        )
                    })
                    .transpose()?,
            })
        })
        .collect()
}

fn normalized_aggregate_function(function: AggregateFunction) -> NormalizedAggregateFunction {
    match function {
        AggregateFunction::Count => NormalizedAggregateFunction::Count,
        AggregateFunction::Sum => NormalizedAggregateFunction::Sum,
        AggregateFunction::Avg => NormalizedAggregateFunction::Avg,
        AggregateFunction::Min => NormalizedAggregateFunction::Min,
        AggregateFunction::Max => NormalizedAggregateFunction::Max,
    }
}

fn normalized_aggregate_output_type(aggregate: &Aggregate) -> ColumnType {
    match aggregate.function {
        AggregateFunction::Count => ColumnType::U64,
        AggregateFunction::Avg => ColumnType::Nullable(Box::new(ColumnType::F64)),
        // Aggregate lowering is currently reported as an unsupported
        // query-engine capability before Groove needs the exact result type.
        AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => {
            ColumnType::Nullable(Box::new(ColumnType::Bytes))
        }
    }
}

fn normalization_gap(message: impl Into<String>) -> Error {
    Error::QueryLowering(message.into())
}

fn array_requirement(requirement: ArraySubqueryRequirement) -> CorrelationRequirement {
    match requirement {
        ArraySubqueryRequirement::Optional => CorrelationRequirement::Optional,
        ArraySubqueryRequirement::AtLeastOne => CorrelationRequirement::AtLeastOne,
        ArraySubqueryRequirement::MatchCorrelationCardinality => {
            CorrelationRequirement::MatchCorrelationCardinality
        }
    }
}

fn correlated_child_source_id(
    owner: &SourceId,
    subquery: &ArraySubquery,
    path: &[usize],
) -> SourceId {
    let mut components = owner.path.components.clone();
    let path_id = path
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(".");
    components.push(SourceRole::CorrelatedChild(format!(
        "{path_id}:{}",
        subquery.column_name
    )));
    SourceId {
        table: subquery.table.clone(),
        path: SourcePath { components },
    }
}

fn include_auxiliary_source_id(
    table: impl Into<String>,
    include_index: usize,
    segment_index: usize,
) -> SourceId {
    SourceId {
        table: table.into(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::Alias(format!("include:{include_index}:{segment_index}")),
            ],
        },
    }
}

fn collect_closure_paths<S>(
    node: &NodeState<S>,
    root_table: &str,
    schema_version: SchemaVersionId,
    includes: &[Include],
) -> Result<(BTreeSet<SourceId>, Vec<ClosurePath>), Error>
where
    S: OrderedKvStorage,
{
    let mut sources = BTreeSet::new();
    let mut paths = Vec::new();
    let root_source = root_source_id(root_table);
    let root_schema = node.table_in_schema(root_table, schema_version)?;
    let explicit_root_segments = includes
        .iter()
        .filter_map(|include| include.path.split('.').next())
        .collect::<BTreeSet<_>>();
    for (reference_index, (column, target_table)) in root_schema.references.iter().enumerate() {
        if explicit_root_segments.contains(column.as_str()) {
            continue;
        }
        let target = include_auxiliary_source_id(target_table.clone(), usize::MAX, reference_index);
        sources.insert(target.clone());
        paths.push(ClosurePath::ImplicitRootReference {
            id: format!("reference:{column}"),
            segment: ClosurePathSegment {
                parent: root_source.clone(),
                target,
                source_field: column.clone(),
            },
        });
    }
    for (include_index, include) in includes.iter().enumerate() {
        let mut current_table_name = root_table.to_owned();
        let mut parent = root_source.clone();
        let mut segments = Vec::new();
        for (segment_index, segment) in include.path.split('.').enumerate() {
            let current_table = node.table_in_schema(&current_table_name, schema_version)?;
            let target_table = current_table
                .references
                .get(segment)
                .cloned()
                .ok_or(Error::InvalidStoredValue("include path was not validated"))?;
            let target =
                include_auxiliary_source_id(target_table.clone(), include_index, segment_index);
            sources.insert(target.clone());
            segments.push(ClosurePathSegment {
                parent: parent.clone(),
                target: target.clone(),
                source_field: segment.to_owned(),
            });
            parent = target;
            current_table_name = target_table;
        }
        paths.push(ClosurePath::ExplicitInclude {
            id: format!("include:{include_index}:{}", include.path),
            segments,
            root_gate: if include.require {
                Some(ClosureRootGate::Required)
            } else if include.join_mode == crate::query::JoinMode::Inner {
                Some(ClosureRootGate::Inner)
            } else {
                None
            },
        });
    }
    Ok((sources, paths))
}

fn normalize_array_subquery(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    current: RowSetNodeId,
    schema: &RuntimeSchema,
    owner_source: &SourceId,
    subquery: &ArraySubquery,
    path: &[usize],
) -> Result<RowSetNodeId, Error> {
    let path_id = path
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(".");
    let child_source = correlated_child_source_id(owner_source, subquery, path);
    let child_node = RowSetNodeId(format!("array_subquery:{path_id}:source"));
    nodes.insert(
        child_node.clone(),
        RowSetExpr::Source {
            source: child_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut child_current = child_node;

    if !subquery.filters.is_empty() {
        let filter_node = RowSetNodeId(format!("array_subquery:{path_id}:filter"));
        nodes.insert(
            filter_node.clone(),
            RowSetExpr::Filter {
                input: child_current,
                predicate: normalize_predicates(schema, &child_source, &subquery.filters, false)
                    .map_err(|err| normalization_gap(err.to_string()))?,
            },
        );
        child_current = filter_node;
    }

    if !subquery.order_by.is_empty() {
        let order_node = RowSetNodeId(format!("array_subquery:{path_id}:order"));
        nodes.insert(
            order_node.clone(),
            RowSetExpr::OrderBy {
                input: child_current,
                keys: subquery
                    .order_by
                    .iter()
                    .map(|order| normalize_order_key(schema, &child_source, order))
                    .collect::<Result<Vec<_>, Error>>()
                    .map_err(|err| normalization_gap(err.to_string()))?,
            },
        );
        child_current = order_node;
    }

    if subquery.limit.is_some() || subquery.offset != 0 {
        let slice_node = RowSetNodeId(format!("array_subquery:{path_id}:slice"));
        nodes.insert(
            slice_node.clone(),
            RowSetExpr::Slice {
                input: child_current,
                partition_by: vec![source_column_value(
                    schema,
                    &child_source,
                    &subquery.inner_column,
                    JoinTarget::Column,
                )],
                limit: subquery
                    .limit
                    .map(|limit| limit.min(u32::MAX as usize) as u32),
                offset: subquery.offset.min(u32::MAX as usize) as u32,
                tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(
                    child_source.clone(),
                ))],
                rank_output: None,
            },
        );
        child_current = slice_node;
    }

    let nested_parent_input = child_current.clone();
    let path_node = RowSetNodeId(format!("array_subquery:{path_id}:path"));
    nodes.insert(
        path_node.clone(),
        RowSetExpr::CorrelatedPathProjection {
            input: current,
            child_input: child_current,
            path: ProgramPathId {
                owner: owner_source.clone(),
                child: child_source.clone(),
            },
            correlation: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::SourceField {
                    source: child_source.clone(),
                    field: subquery.inner_column.clone(),
                },
                op: NormalizedComparisonOp::Eq,
                right: normalize_operand_for_schema(
                    schema,
                    owner_source,
                    &Operand::Column(subquery.outer_column.clone()),
                )
                .map_err(|err| normalization_gap(err.to_string()))?,
            },
            requirement: array_requirement(subquery.requirement),
        },
    );
    for (nested_index, nested) in subquery.nested_arrays.iter().enumerate() {
        let mut nested_path = path.to_vec();
        nested_path.push(nested_index);
        normalize_array_subquery(
            nodes,
            nested_parent_input.clone(),
            schema,
            &child_source,
            nested,
            &nested_path,
        )?;
    }
    Ok(path_node)
}

fn normalize_reachable(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    current: RowSetNodeId,
    schema: &RuntimeSchema,
    root_source: &SourceId,
    reachable: &crate::query::ReachableVia,
    index: usize,
    prefix: &str,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<(RowSetNodeId, ReachableContribution), Error> {
    let reachable_id = if prefix.is_empty() {
        format!("reachable:{index}")
    } else {
        format!("{prefix}:reachable:{index}")
    };
    let frontier = FrontierId(format!("{reachable_id}:frontier"));
    let (seed_node, columns) = normalize_reachable_seed(
        nodes,
        schema,
        reachable,
        &reachable_id,
        binding_source_shape,
        param_types,
    )?;
    let frontier_node = RowSetNodeId(format!("{reachable_id}:frontier"));
    nodes.insert(
        frontier_node.clone(),
        RowSetExpr::FrontierSource {
            frontier: frontier.clone(),
            columns: columns.clone(),
        },
    );

    let edge_source = reachable_edge_source_id(reachable, &reachable_id);
    let edge_source_node = RowSetNodeId(format!("{reachable_id}:edge_source"));
    nodes.insert(
        edge_source_node.clone(),
        RowSetExpr::Source {
            source: edge_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut edge_current = edge_source_node;
    if !reachable.edge_filters.is_empty() {
        let edge_filter_node = RowSetNodeId(format!("{reachable_id}:edge_filter"));
        nodes.insert(
            edge_filter_node.clone(),
            RowSetExpr::Filter {
                input: edge_current,
                predicate: normalize_predicates(
                    schema,
                    &edge_source,
                    &reachable.edge_filters,
                    false,
                )?,
            },
        );
        edge_current = edge_filter_node;
    }

    let step_join_node = RowSetNodeId(format!("{reachable_id}:step_join"));
    nodes.insert(
        step_join_node.clone(),
        RowSetExpr::Join {
            left: frontier_node,
            right: edge_current,
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::FrontierColumn {
                    frontier: frontier.clone(),
                    field: "reachable_team".to_owned(),
                },
                op: NormalizedComparisonOp::Eq,
                right: source_column_value(
                    schema,
                    &edge_source,
                    &reachable.edge_member_column,
                    JoinTarget::Column,
                ),
            },
        },
    );
    let step_project_node = RowSetNodeId(format!("{reachable_id}:step_project"));
    let mut step_columns = vec![
        RowProjection {
            output: typed_output_field("team", ColumnType::Uuid),
            value: NormalizedValueRef::FrontierColumn {
                frontier: frontier.clone(),
                field: "team".to_owned(),
            },
        },
        RowProjection {
            output: typed_output_field("reachable_team", ColumnType::Uuid),
            value: NormalizedValueRef::SourceField {
                source: edge_source.clone(),
                field: reachable.edge_parent_column.clone(),
            },
        },
    ];
    step_columns.extend(
        columns
            .iter()
            .filter(|column| column.name != "team" && column.name != "reachable_team")
            .map(|column| RowProjection {
                output: typed_output_field(&column.name, column.ty.clone()),
                value: NormalizedValueRef::FrontierColumn {
                    frontier: frontier.clone(),
                    field: column.name.clone(),
                },
            }),
    );
    nodes.insert(
        step_project_node.clone(),
        RowSetExpr::Project {
            input: step_join_node,
            columns: step_columns,
        },
    );

    let closure_node = RowSetNodeId(format!("{reachable_id}:closure"));
    nodes.insert(
        closure_node.clone(),
        RowSetExpr::RecursiveRelation {
            seed: seed_node,
            step: step_project_node,
            frontier: frontier.clone(),
            frontier_key: NormalizedValueRef::FrontierColumn {
                frontier: frontier.clone(),
                field: "reachable_team".to_owned(),
            },
            dedupe_keys: reachable_dedupe_keys(&frontier, &columns),
            bound: reachable.bound,
        },
    );

    let access_source = reachable_access_source_id(reachable, &reachable_id);
    let access_source_node = RowSetNodeId(format!("{reachable_id}:access_source"));
    nodes.insert(
        access_source_node.clone(),
        RowSetExpr::Source {
            source: access_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut access_current = access_source_node;
    if !reachable.access_filters.is_empty() {
        let access_filter_node = RowSetNodeId(format!("{reachable_id}:access_filter"));
        nodes.insert(
            access_filter_node.clone(),
            RowSetExpr::Filter {
                input: access_current,
                predicate: normalize_predicates(
                    schema,
                    &access_source,
                    &reachable.access_filters,
                    false,
                )?,
            },
        );
        access_current = access_filter_node;
    }

    let access_join_node = RowSetNodeId(format!("{reachable_id}:access_join"));
    nodes.insert(
        access_join_node.clone(),
        RowSetExpr::Join {
            left: access_current,
            right: closure_node,
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: reachable_access_key(
                    schema,
                    &access_source,
                    &reachable.access_team_column,
                    reachable.access_team_target,
                ),
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::FrontierColumn {
                    frontier: frontier.clone(),
                    field: "reachable_team".to_owned(),
                },
            },
        },
    );

    let root_join_node = RowSetNodeId(format!("{reachable_id}:root_join"));
    nodes.insert(
        root_join_node.clone(),
        RowSetExpr::Join {
            left: current,
            right: access_join_node.clone(),
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: source_column_value(schema, root_source, "id", JoinTarget::Column),
                op: NormalizedComparisonOp::Eq,
                right: reachable_access_key(
                    schema,
                    &access_source,
                    &reachable.access_row_column,
                    JoinTarget::Column,
                ),
            },
        },
    );
    Ok((
        root_join_node,
        ReachableContribution {
            id: reachable_id,
            access_source,
            access_input: access_join_node,
            root_ref_field: reachable.access_row_column.clone(),
        },
    ))
}

pub(super) fn source_column_value(
    schema: &RuntimeSchema,
    source: &SourceId,
    column: &str,
    target: JoinTarget,
) -> NormalizedValueRef {
    if target == JoinTarget::RowId || (column == "id" && !source_has_declared_id(schema, source)) {
        NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
    } else {
        NormalizedValueRef::SourceField {
            source: source.clone(),
            field: column.to_owned(),
        }
    }
}

fn reachable_access_key(
    schema: &RuntimeSchema,
    access_source: &SourceId,
    column: &str,
    target: JoinTarget,
) -> NormalizedValueRef {
    source_column_value(schema, access_source, column, target)
}

fn normalize_join_via_right(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    schema: &RuntimeSchema,
    join: &JoinVia,
    path: &str,
) -> Result<(RowSetNodeId, SourceId), Error> {
    let join_source = nested_join_source_id(join, path);
    auxiliary_sources.insert(join_source.clone());
    let table = table_schema(schema, &join.table)?;
    let source_node = RowSetNodeId(format!("{path}:source"));
    nodes.insert(
        source_node.clone(),
        RowSetExpr::Source {
            source: join_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut current = source_node;
    if !join.filters.is_empty() {
        let filter_node = RowSetNodeId(format!("{path}:filter"));
        nodes.insert(
            filter_node.clone(),
            RowSetExpr::Filter {
                input: current,
                predicate: normalize_predicates(schema, &join_source, &join.filters, false)?,
            },
        );
        current = filter_node;
    }

    if let Some(lookup) = &join.source_lookup {
        let lookup_source = join_lookup_source_id(lookup, path);
        auxiliary_sources.insert(lookup_source.clone());
        let lookup_source_node = RowSetNodeId(format!("{path}:lookup_source"));
        nodes.insert(
            lookup_source_node.clone(),
            RowSetExpr::Source {
                source: lookup_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let lookup_join_node = RowSetNodeId(format!("{path}:lookup_join"));
        nodes.insert(
            lookup_join_node.clone(),
            RowSetExpr::Join {
                left: current,
                right: lookup_source_node,
                mode: NormalizedJoinMode::Inner,
                on: NormalizedPredicateExpr::Compare {
                    left: join_via_target_key(&join_source, join),
                    op: NormalizedComparisonOp::Eq,
                    right: source_column_value(
                        schema,
                        &lookup_source,
                        &lookup.value_column,
                        JoinTarget::Column,
                    ),
                },
            },
        );
        let lookup_project_node = RowSetNodeId(format!("{path}:lookup_project"));
        let mut columns = source_public_field_projections(table, &join_source);
        columns.push(RowProjection {
            output: typed_output_field(lookup.row_id_source_column.clone(), ColumnType::Uuid),
            value: NormalizedValueRef::RowId(RowIdRef::Source(lookup_source)),
        });
        nodes.insert(
            lookup_project_node.clone(),
            RowSetExpr::Project {
                input: lookup_join_node,
                columns,
            },
        );
        current = lookup_project_node;
    }

    for (nested_index, nested) in join.nested_joins.iter().enumerate() {
        let nested_path = format!("{path}:nested:{nested_index}");
        let (nested_right, nested_source) =
            normalize_join_via_right(nodes, auxiliary_sources, schema, nested, &nested_path)?;
        let nested_join_node = RowSetNodeId(format!("{nested_path}:join"));
        nodes.insert(
            nested_join_node.clone(),
            RowSetExpr::Join {
                left: current,
                right: nested_right,
                mode: NormalizedJoinMode::Inner,
                on: join_via_predicate(schema, &join_source, &nested_source, nested),
            },
        );
        let project_node = RowSetNodeId(format!("{nested_path}:parent_project"));
        nodes.insert(
            project_node.clone(),
            RowSetExpr::Project {
                input: nested_join_node,
                columns: source_public_field_projections(table, &join_source),
            },
        );
        current = project_node;
    }

    Ok((current, join_source))
}

fn reachable_dedupe_keys(
    frontier: &FrontierId,
    columns: &[ValueSourceColumn],
) -> Vec<NormalizedValueRef> {
    std::iter::once("reachable_team")
        .chain(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .filter(|name| *name != "team" && *name != "reachable_team"),
        )
        .map(|field| NormalizedValueRef::FrontierColumn {
            frontier: frontier.clone(),
            field: field.to_owned(),
        })
        .collect()
}

fn normalize_reachable_seed(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    schema: &RuntimeSchema,
    reachable: &crate::query::ReachableVia,
    reachable_id: &str,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<(RowSetNodeId, Vec<ValueSourceColumn>), Error> {
    if let Some(seed) = &reachable.seed {
        let seed_source = reachable_seed_source_id(seed, reachable_id);
        let mut columns = reachable_seed_frontier_columns(schema, &seed_source, seed)?;
        let edge_route_columns = reachable_edge_route_columns(reachable, param_types)?;
        for column in &edge_route_columns {
            if !columns.iter().any(|existing| existing.name == column.name) {
                columns.push(column.clone());
            }
        }
        let user_column_ty = seed
            .user_column
            .as_ref()
            .map(|column| schema_column_type(schema, &seed.table, column))
            .transpose()?;
        let team_column_ty = schema_column_type(schema, &seed.table, &seed.team_column)?;
        if team_column_ty != ColumnType::Uuid {
            return Err(Error::QueryLowering(format!(
                "reachable_via seed {}.{} must be uuid, found {:?}",
                seed.table, seed.team_column, team_column_ty
            )));
        }
        let seed_source_node = RowSetNodeId(format!("{reachable_id}:seed_source"));
        nodes.insert(
            seed_source_node.clone(),
            RowSetExpr::Source {
                source: seed_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let mut seed_current = seed_source_node;
        let claim_route_field = seed.user_claim.as_ref().map(|user_claim| {
            let claim_path = ClaimPath(crate::query::operand_claim_path(user_claim));
            (claim_path.clone(), claim_param_field(&claim_path))
        });
        if let (Some(user_column), Some((_, claim_field))) = (&seed.user_column, &claim_route_field)
        {
            let seed_claim_filter_node = RowSetNodeId(format!("{reachable_id}:seed_claim_filter"));
            nodes.insert(
                seed_claim_filter_node.clone(),
                RowSetExpr::Filter {
                    input: seed_current,
                    predicate: NormalizedPredicateExpr::Compare {
                        left: NormalizedValueRef::SourceField {
                            source: seed_source.clone(),
                            field: user_column.clone(),
                        },
                        op: NormalizedComparisonOp::Eq,
                        right: NormalizedValueRef::Param(claim_field.clone()),
                    },
                },
            );
            seed_current = seed_claim_filter_node;
        }
        if !seed.filters.is_empty() {
            let seed_filter_node = RowSetNodeId(format!("{reachable_id}:seed_filter"));
            nodes.insert(
                seed_filter_node.clone(),
                RowSetExpr::Filter {
                    input: seed_current,
                    predicate: normalize_predicates(schema, &seed_source, &seed.filters, false)?,
                },
            );
            seed_current = seed_filter_node;
        }
        let seed_project_node = RowSetNodeId(format!("{reachable_id}:seed_project"));
        let seed_team_value =
            source_column_value(schema, &seed_source, &seed.team_column, JoinTarget::Column);
        let mut seed_columns = vec![
            RowProjection {
                output: typed_output_field("team", ColumnType::Uuid),
                value: seed_team_value.clone(),
            },
            RowProjection {
                output: typed_output_field("reachable_team", ColumnType::Uuid),
                value: seed_team_value,
            },
        ];
        if let Some((_, claim_field)) = &claim_route_field {
            seed_columns.push(RowProjection {
                output: typed_output_field(
                    claim_field,
                    user_column_ty.clone().unwrap_or(ColumnType::Uuid),
                ),
                value: NormalizedValueRef::Param(claim_field.clone()),
            });
        }
        seed_columns.extend(edge_route_columns.into_iter().map(|column| RowProjection {
            output: typed_output_field(&column.name, column.ty),
            value: column.value,
        }));
        nodes.insert(
            seed_project_node.clone(),
            RowSetExpr::Project {
                input: seed_current,
                columns: seed_columns,
            },
        );
        return Ok((seed_project_node, columns));
    }

    let mut columns = reachable_frontier_columns(&reachable.from, param_types)?;
    for column in reachable_edge_route_columns(reachable, param_types)? {
        if !columns.iter().any(|existing| existing.name == column.name) {
            columns.push(column);
        }
    }
    let seed_node = RowSetNodeId(format!("{reachable_id}:seed"));
    nodes.insert(
        seed_node.clone(),
        RowSetExpr::ValueSource {
            shape: binding_source_shape.to_owned(),
            columns: columns.clone(),
            mode: reachable_seed_value_source_mode(&reachable.from)?,
        },
    );
    Ok((seed_node, columns))
}

fn reachable_edge_route_columns(
    reachable: &crate::query::ReachableVia,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<Vec<ValueSourceColumn>, Error> {
    predicate_params(&reachable.edge_filters)
        .into_iter()
        .map(|param| {
            let ty = param_types.get(&param).cloned().ok_or_else(|| {
                Error::QueryLowering(format!("unknown reachable edge parameter {param}"))
            })?;
            Ok(ValueSourceColumn {
                name: route_param_field(&param),
                value: NormalizedValueRef::Param(param),
                ty,
            })
        })
        .collect()
}

fn reachable_seed_frontier_columns(
    schema: &RuntimeSchema,
    source: &SourceId,
    seed: &crate::query::ReachableSeed,
) -> Result<Vec<ValueSourceColumn>, Error> {
    let team_column_ty = schema_column_type(schema, &seed.table, &seed.team_column)?;
    if team_column_ty != ColumnType::Uuid {
        return Err(Error::QueryLowering(format!(
            "reachable_via seed {}.{} must be uuid, found {:?}",
            seed.table, seed.team_column, team_column_ty
        )));
    }
    let value = source_column_value(schema, source, &seed.team_column, JoinTarget::Column);
    let mut columns = vec![
        ValueSourceColumn {
            name: "team".to_owned(),
            value: value.clone(),
            ty: ColumnType::Uuid,
        },
        ValueSourceColumn {
            name: "reachable_team".to_owned(),
            value,
            ty: ColumnType::Uuid,
        },
    ];
    if let Some(user_claim) = &seed.user_claim {
        let Some(user_column) = &seed.user_column else {
            return Err(Error::QueryLowering(
                "reachable_via relation seed user_claim requires user_column".to_owned(),
            ));
        };
        let user_column_ty = schema_column_type(schema, &seed.table, user_column)?;
        let path = ClaimPath(crate::query::operand_claim_path(user_claim));
        columns.push(ValueSourceColumn {
            name: claim_param_field(&path),
            value: NormalizedValueRef::Claim(path),
            ty: user_column_ty,
        });
    }
    Ok(columns)
}

fn reachable_frontier_columns(
    seed: &Operand,
    param_types: &BTreeMap<String, ColumnType>,
) -> Result<Vec<ValueSourceColumn>, Error> {
    let value = reachable_seed_value_ref(seed)?;
    let ty = match seed {
        Operand::Param(param) => param_types.get(param).cloned().unwrap_or(ColumnType::Uuid),
        Operand::Literal(Value::Uuid(_)) => ColumnType::Uuid,
        Operand::Claim(_) => ColumnType::Uuid,
        Operand::Column(_) | Operand::Literal(_) => {
            return Err(normalization_gap(
                "reachable_via currently supports uuid parameter/claim/literal seeds only",
            ));
        }
    };
    let mut columns = vec![
        ValueSourceColumn {
            name: "team".to_owned(),
            value: value.clone(),
            ty: ty.clone(),
        },
        ValueSourceColumn {
            name: "reachable_team".to_owned(),
            value,
            ty,
        },
    ];
    if let Operand::Param(param) = seed {
        columns.push(ValueSourceColumn {
            name: route_param_field(param),
            value: NormalizedValueRef::Param(param.clone()),
            ty: param_types.get(param).cloned().unwrap_or(ColumnType::Uuid),
        });
    }
    if let Operand::Claim(claim) = seed {
        let path = ClaimPath(crate::query::operand_claim_path(claim));
        columns.push(ValueSourceColumn {
            name: claim_param_field(&path),
            value: NormalizedValueRef::Claim(path),
            ty: ColumnType::Uuid,
        });
    }
    if let Operand::Param(param) = seed
        && param != "team"
        && param != "reachable_team"
    {
        columns.push(ValueSourceColumn {
            name: param.clone(),
            value: NormalizedValueRef::Param(param.clone()),
            ty: param_types.get(param).cloned().unwrap_or(ColumnType::Uuid),
        });
    }
    Ok(columns)
}

fn reachable_seed_value_ref(seed: &Operand) -> Result<NormalizedValueRef, Error> {
    match seed {
        Operand::Param(param) => Ok(NormalizedValueRef::Param(param.clone())),
        Operand::Literal(Value::Uuid(uuid)) => literal_value_ref(&Value::Uuid(*uuid)),
        Operand::Claim(claim) => Ok(NormalizedValueRef::Claim(ClaimPath(
            crate::query::operand_claim_path(claim),
        ))),
        Operand::Column(_) | Operand::Literal(_) => Err(normalization_gap(
            "reachable_via currently supports uuid parameter/claim/literal seeds only",
        )),
    }
}

fn reachable_seed_value_source_mode(seed: &Operand) -> Result<ValueSourceMode, Error> {
    match seed {
        Operand::Param(_) | Operand::Claim(_) => Ok(ValueSourceMode::Binding),
        Operand::Literal(Value::Uuid(_)) => Ok(ValueSourceMode::Inline),
        Operand::Column(_) | Operand::Literal(_) => Err(normalization_gap(
            "reachable_via currently supports uuid parameter/claim/literal seeds only",
        )),
    }
}

fn literal_value_ref(value: &Value) -> Result<NormalizedValueRef, Error> {
    Ok(NormalizedValueRef::Literal(
        postcard::to_allocvec(value)
            .map_err(|err| Error::QueryLowering(format!("literal encoding failed: {err}")))?,
    ))
}

fn typed_output_field(name: impl Into<String>, ty: ColumnType) -> TypedOutputField {
    TypedOutputField {
        name: name.into(),
        ty,
    }
}

pub(super) fn table_schema<'a>(
    schema: &'a RuntimeSchema,
    table: &str,
) -> Result<&'a TableSchema, Error> {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .ok_or_else(|| Error::QueryLowering(format!("unknown query table {table}")))
}

fn schema_column_type(
    schema: &RuntimeSchema,
    table: &str,
    column: &str,
) -> Result<ColumnType, Error> {
    let schema_table = table_schema(schema, table)?;
    if let Some(column) = schema_table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
    {
        return Ok(column.column_type.clone());
    }
    if column == "id" {
        return Ok(ColumnType::Uuid);
    }
    Err(Error::QueryLowering(format!(
        "unknown query column {table}.{column}"
    )))
}

fn row_id_output_field() -> TypedOutputField {
    typed_output_field("id", ColumnType::Uuid)
}

fn source_public_field_projections(table: &TableSchema, source: &SourceId) -> Vec<RowProjection> {
    std::iter::once(RowProjection {
        output: row_id_output_field(),
        value: NormalizedValueRef::RowId(RowIdRef::Source(source.clone())),
    })
    .chain(table.columns.iter().map(|column| RowProjection {
        output: typed_output_field(column.name.clone(), column.column_type.clone()),
        value: NormalizedValueRef::SourceField {
            source: source.clone(),
            field: column.name.clone(),
        },
    }))
    .collect()
}

fn join_via_root_key(
    schema: &RuntimeSchema,
    root_source: &SourceId,
    join: &JoinVia,
) -> NormalizedValueRef {
    join.source_column
        .as_ref()
        .map(|field| {
            if field == "id" && !source_has_declared_id(schema, root_source) {
                NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone()))
            } else {
                NormalizedValueRef::SourceField {
                    source: root_source.clone(),
                    field: field.clone(),
                }
            }
        })
        .unwrap_or_else(|| NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())))
}

fn join_via_target_key(join_source: &SourceId, join: &JoinVia) -> NormalizedValueRef {
    match join.target {
        JoinTarget::Column => NormalizedValueRef::SourceField {
            source: join_source.clone(),
            field: join.on_column.clone(),
        },
        JoinTarget::RowId => NormalizedValueRef::RowId(RowIdRef::Source(join_source.clone())),
    }
}

fn join_via_predicate(
    schema: &RuntimeSchema,
    left_source: &SourceId,
    right_source: &SourceId,
    join: &JoinVia,
) -> NormalizedPredicateExpr {
    let mut key_pairs = vec![if let Some(lookup) = &join.source_lookup {
        (
            NormalizedValueRef::SourceField {
                source: left_source.clone(),
                field: lookup.row_id_source_column.clone(),
            },
            NormalizedValueRef::SourceField {
                source: right_source.clone(),
                field: lookup.row_id_source_column.clone(),
            },
        )
    } else {
        (
            join_via_root_key(schema, left_source, join),
            join_via_target_key(right_source, join),
        )
    }];
    key_pairs.extend(join.correlated_filters.iter().map(|correlation| {
        (
            NormalizedValueRef::SourceField {
                source: left_source.clone(),
                field: correlation.source_column.clone(),
            },
            NormalizedValueRef::SourceField {
                source: right_source.clone(),
                field: correlation.join_column.clone(),
            },
        )
    }));
    if key_pairs.len() == 1 {
        let (left, right) = key_pairs.remove(0);
        NormalizedPredicateExpr::Compare {
            left,
            op: NormalizedComparisonOp::Eq,
            right,
        }
    } else {
        NormalizedPredicateExpr::And(
            key_pairs
                .into_iter()
                .map(|(left, right)| NormalizedPredicateExpr::Compare {
                    left,
                    op: NormalizedComparisonOp::Eq,
                    right,
                })
                .collect(),
        )
    }
}

pub(super) fn reachable_edge_source_id(
    reachable: &crate::query::ReachableVia,
    reachable_id: &str,
) -> SourceId {
    SourceId {
        table: reachable.edge_table.clone(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::RecursiveStep(format!("{reachable_id}:{}", reachable.edge_table)),
            ],
        },
    }
}

pub(super) fn reachable_access_source_id(
    reachable: &crate::query::ReachableVia,
    reachable_id: &str,
) -> SourceId {
    SourceId {
        table: reachable.access_table.clone(),
        path: SourcePath {
            components: vec![SourceRole::Alias(format!(
                "{reachable_id}:{}",
                reachable.access_table
            ))],
        },
    }
}

pub(super) fn reachable_seed_source_id(
    seed: &crate::query::ReachableSeed,
    reachable_id: &str,
) -> SourceId {
    SourceId {
        table: seed.table.clone(),
        path: SourcePath {
            components: vec![
                SourceRole::Root,
                SourceRole::RecursiveSeed(format!("{reachable_id}:{}", seed.table)),
            ],
        },
    }
}

fn inherited_parent_source_id(table: &str, prefix: &str) -> SourceId {
    SourceId {
        table: table.to_owned(),
        path: SourcePath {
            components: vec![SourceRole::Alias(prefix.to_owned())],
        },
    }
}

struct FilterJoinChain<'a> {
    pub(super) filters: &'a [Predicate],
    pub(super) joins: &'a [JoinVia],
}

struct PolicyAtomChain<'a> {
    pub(super) filters: &'a [Predicate],
    pub(super) joins: &'a [JoinVia],
    pub(super) inherits: &'a [crate::query::InheritsVia],
    pub(super) reachable: &'a [crate::query::ReachableVia],
}

/// The inheritance atoms expanded on the current policy-composition path.
///
/// A policy can refer back to its own table through an `InheritsVia`. The
/// normalized graph is finite only when that expansion is bounded; keep this
/// state per path so independent policy alternatives do not consume each
/// other's depth budget.
#[derive(Clone, Default)]
struct InheritanceExpansionPath {
    uses: BTreeMap<InheritanceExpansionKey, usize>,
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct InheritanceExpansionKey {
    child_table: String,
    parent_column: String,
    operation: crate::query::InheritsOperation,
}

impl InheritanceExpansionPath {
    pub(crate) fn descend(
        &self,
        child_table: &str,
        inherits: &crate::query::InheritsVia,
    ) -> Option<Self> {
        let key = InheritanceExpansionKey {
            child_table: child_table.to_owned(),
            parent_column: inherits.parent_column.clone(),
            operation: inherits.operation,
        };
        let used = self.uses.get(&key).copied().unwrap_or(0);
        let limit = inherits
            .max_depth
            .unwrap_or_else(|| crate::query::RecursionBound::default_max_depth().depth_steps());
        if used >= limit {
            return None;
        }
        let mut next = self.clone();
        next.uses.insert(key, used + 1);
        Some(next)
    }
}

fn normalize_false_policy_branch(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    input: RowSetNodeId,
    prefix: &str,
) -> RowSetNodeId {
    let node = RowSetNodeId(format!("{prefix}:max_depth"));
    nodes.insert(
        node.clone(),
        RowSetExpr::Filter {
            input,
            predicate: NormalizedPredicateExpr::Or(Vec::new()),
        },
    );
    node
}

fn normalize_filter_join_chain(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    schema: &RuntimeSchema,
    root_source: &SourceId,
    start: RowSetNodeId,
    prefix: &str,
    chain: FilterJoinChain<'_>,
    record_join_contributions: bool,
) -> Result<RowSetNodeId, Error> {
    let mut current = start;
    if !chain.filters.is_empty() {
        let filter_node = RowSetNodeId(format!("{prefix}:filter"));
        nodes.insert(
            filter_node.clone(),
            RowSetExpr::Filter {
                input: current,
                predicate: normalize_predicates(schema, root_source, chain.filters, false)?,
            },
        );
        current = filter_node;
    }

    for (index, join) in chain.joins.iter().enumerate() {
        let path = if prefix == "query" {
            format!("join_via:{index}")
        } else {
            format!("{prefix}:join_via:{index}")
        };
        let (right, join_source) =
            normalize_join_via_right(nodes, auxiliary_sources, schema, join, &path)?;
        let join_predicate = join_via_predicate(schema, root_source, &join_source, join);
        if record_join_contributions {
            join_contributions.push(JoinContribution {
                id: path.clone(),
                source: join_source.clone(),
                input: right.clone(),
                membership: join_predicate.clone(),
            });
        }
        let join_node = RowSetNodeId(format!("{path}:join"));
        nodes.insert(
            join_node.clone(),
            RowSetExpr::Join {
                left: current,
                right,
                mode: NormalizedJoinMode::Inner,
                on: join_predicate,
            },
        );
        current = join_node;
    }
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn normalize_policy_atom_chain(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    reachable_contributions: &mut Vec<ReachableContribution>,
    schema: &RuntimeSchema,
    root_source: &SourceId,
    start: RowSetNodeId,
    prefix: &str,
    chain: PolicyAtomChain<'_>,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
    record_join_contributions: bool,
    inheritance_path: &InheritanceExpansionPath,
) -> Result<RowSetNodeId, Error> {
    let mut current = normalize_filter_join_chain(
        nodes,
        auxiliary_sources,
        join_contributions,
        schema,
        root_source,
        start,
        prefix,
        FilterJoinChain {
            filters: chain.filters,
            joins: chain.joins,
        },
        record_join_contributions,
    )?;
    for (index, inherits) in chain.inherits.iter().enumerate() {
        current = normalize_inherited_parent_policy(
            nodes,
            auxiliary_sources,
            join_contributions,
            reachable_contributions,
            schema,
            root_source,
            current,
            inherits,
            &format!("{prefix}:inherits:{index}"),
            binding_source_shape,
            param_types,
            inheritance_path,
        )?;
    }
    for (index, reachable) in chain.reachable.iter().enumerate() {
        let reachable_prefix = if prefix == "query" { "" } else { prefix };
        let (next, contribution) = normalize_reachable(
            nodes,
            current,
            schema,
            root_source,
            reachable,
            index,
            reachable_prefix,
            binding_source_shape,
            param_types,
        )?;
        current = next;
        reachable_contributions.push(contribution);
    }
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn normalize_inherited_parent_policy(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    reachable_contributions: &mut Vec<ReachableContribution>,
    schema: &RuntimeSchema,
    child_source: &SourceId,
    child_current: RowSetNodeId,
    inherits: &crate::query::InheritsVia,
    prefix: &str,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
    inheritance_path: &InheritanceExpansionPath,
) -> Result<RowSetNodeId, Error> {
    let child_table = table_schema(schema, &child_source.table)?;
    let parent_table_name = child_table
        .references
        .get(&inherits.parent_column)
        .cloned()
        .ok_or_else(|| {
            Error::QueryLowering(format!(
                "{}.{} is not a parent reference",
                child_source.table, inherits.parent_column
            ))
        })?;
    let parent_table = table_schema(schema, &parent_table_name)?;
    let Some(parent_inheritance_path) = inheritance_path.descend(&child_source.table, inherits)
    else {
        return Ok(normalize_false_policy_branch(nodes, child_current, prefix));
    };
    let parent_source = inherited_parent_source_id(&parent_table_name, prefix);
    auxiliary_sources.insert(parent_source.clone());
    let parent_source_node = RowSetNodeId(format!("{prefix}:source"));
    nodes.insert(
        parent_source_node.clone(),
        RowSetExpr::Source {
            source: parent_source.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    let mut parent_current = parent_source_node;
    let parent_policy = match inherits.operation {
        crate::query::InheritsOperation::Select => parent_table.read_policy.as_ref(),
        crate::query::InheritsOperation::Insert => {
            parent_table.write_policies.insert_check.as_ref()
        }
        crate::query::InheritsOperation::Update => {
            parent_table.write_policies.update_using.as_ref()
        }
        crate::query::InheritsOperation::Delete => {
            parent_table.write_policies.delete_using.as_ref()
        }
    };
    if let Some(policy) = parent_policy {
        parent_current = if !policy.policy_branches.is_empty() {
            normalize_policy_branch_authorization(
                nodes,
                auxiliary_sources,
                join_contributions,
                reachable_contributions,
                schema,
                &parent_source,
                parent_current,
                &format!("{prefix}:parent_policy"),
                policy,
                binding_source_shape,
                param_types,
                &parent_inheritance_path,
            )?
        } else {
            normalize_policy_atom_chain(
                nodes,
                auxiliary_sources,
                join_contributions,
                reachable_contributions,
                schema,
                &parent_source,
                parent_current,
                &format!("{prefix}:parent_policy"),
                PolicyAtomChain {
                    filters: &policy.filters,
                    joins: &policy.joins,
                    inherits: &policy.inherits,
                    reachable: &policy.reachable,
                },
                binding_source_shape,
                param_types,
                false,
                &parent_inheritance_path,
            )?
        };
    }
    let join_node = RowSetNodeId(format!("{prefix}:join"));
    nodes.insert(
        join_node.clone(),
        RowSetExpr::Join {
            left: child_current,
            right: parent_current,
            mode: NormalizedJoinMode::Semi,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::SourceField {
                    source: child_source.clone(),
                    field: inherits.parent_column.clone(),
                },
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::RowId(RowIdRef::Source(parent_source)),
            },
        },
    );
    Ok(join_node)
}

#[allow(clippy::too_many_arguments)]
fn normalize_policy_branch_authorization(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    auxiliary_sources: &mut BTreeSet<SourceId>,
    join_contributions: &mut Vec<JoinContribution>,
    reachable_contributions: &mut Vec<ReachableContribution>,
    schema: &RuntimeSchema,
    root_source: &SourceId,
    current: RowSetNodeId,
    prefix: &str,
    policy: &JazzQuery,
    binding_source_shape: &str,
    param_types: &BTreeMap<String, ColumnType>,
    inheritance_path: &InheritanceExpansionPath,
) -> Result<RowSetNodeId, Error> {
    let mut union_inputs = Vec::new();
    if !policy_branch_base_is_converter_false(policy) {
        let base_source_node = RowSetNodeId(format!("{prefix}:base:root"));
        nodes.insert(
            base_source_node.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let base = normalize_policy_atom_chain(
            nodes,
            auxiliary_sources,
            join_contributions,
            reachable_contributions,
            schema,
            root_source,
            base_source_node,
            &format!("{prefix}:base"),
            PolicyAtomChain {
                filters: &policy.filters,
                joins: &policy.joins,
                inherits: &policy.inherits,
                reachable: &policy.reachable,
            },
            binding_source_shape,
            param_types,
            false,
            inheritance_path,
        )?;
        union_inputs.push(UnionInput {
            node: normalize_row_id_projection(
                nodes,
                base,
                root_source,
                RowSetNodeId(format!("{prefix}:base:row_id")),
            ),
            label: policy_branch_semantic_label(
                &policy.filters,
                &policy.joins,
                &policy.reachable,
                &policy.inherits,
            )?,
        });
    }

    for (index, branch) in policy.policy_branches.iter().enumerate() {
        let branch_source_node = RowSetNodeId(format!("{prefix}:{index}:root"));
        nodes.insert(
            branch_source_node.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        let branch_current = normalize_policy_atom_chain(
            nodes,
            auxiliary_sources,
            join_contributions,
            reachable_contributions,
            schema,
            root_source,
            branch_source_node,
            &format!("{prefix}:{index}"),
            PolicyAtomChain {
                filters: &branch.filters,
                joins: &branch.joins,
                inherits: &branch.inherits,
                reachable: &branch.reachable,
            },
            binding_source_shape,
            param_types,
            false,
            inheritance_path,
        )?;
        union_inputs.push(UnionInput {
            node: normalize_row_id_projection(
                nodes,
                branch_current,
                root_source,
                RowSetNodeId(format!("{prefix}:{index}:row_id")),
            ),
            label: policy_branch_semantic_label(
                &branch.filters,
                &branch.joins,
                &branch.reachable,
                &branch.inherits,
            )?,
        });
    }

    let union_node = RowSetNodeId(format!("{prefix}:authorized_rows"));
    nodes.insert(
        union_node.clone(),
        RowSetExpr::Union {
            inputs: union_inputs,
        },
    );
    let join_node = RowSetNodeId(format!("{prefix}:authorize"));
    nodes.insert(
        join_node.clone(),
        RowSetExpr::Join {
            left: current,
            right: union_node,
            mode: NormalizedJoinMode::Inner,
            on: NormalizedPredicateExpr::Compare {
                left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                op: NormalizedComparisonOp::Eq,
                right: NormalizedValueRef::SourceField {
                    source: root_source.clone(),
                    field: "row_uuid".to_owned(),
                },
            },
        },
    );
    Ok(join_node)
}

fn policy_branch_semantic_label(
    filters: &[crate::query::Predicate],
    joins: &[crate::query::JoinVia],
    reachable: &[crate::query::ReachableVia],
    inherits: &[crate::query::InheritsVia],
) -> Result<String, Error> {
    let bytes = postcard::to_allocvec(&(filters, joins, reachable, inherits)).map_err(|error| {
        Error::QueryLowering(format!(
            "policy branch fingerprint encoding failed: {error}"
        ))
    })?;
    Ok(format!("policy:{}", blake3::hash(&bytes).to_hex()))
}

fn normalize_row_id_projection(
    nodes: &mut BTreeMap<RowSetNodeId, RowSetExpr>,
    input: RowSetNodeId,
    root_source: &SourceId,
    node_id: RowSetNodeId,
) -> RowSetNodeId {
    nodes.insert(
        node_id.clone(),
        RowSetExpr::Project {
            input,
            columns: vec![RowProjection {
                output: TypedOutputField {
                    name: "row_uuid".to_owned(),
                    ty: ColumnType::Uuid,
                },
                value: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
            }],
        },
    );
    node_id
}

fn unsupported_policy_branch_reason(query: &JazzQuery) -> Option<String> {
    let _ = query;
    None
}

fn policy_branch_base_is_converter_false(query: &JazzQuery) -> bool {
    matches!(query.filters.as_slice(), [Predicate::Any(predicates)] if predicates.is_empty())
        && query.joins.is_empty()
        && query.reachable.is_empty()
        && query.inherits.is_empty()
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn collect_policy_dependency_claim_params(
        &self,
        schema: &RuntimeSchema,
        policy: &PolicyContext,
        input: &NormalizedRowSetShape,
        params: &mut BTreeMap<String, ProgramClaimParam>,
    ) -> Result<(), Error> {
        let claims = match policy {
            PolicyContext::Identity { claims, .. }
            | PolicyContext::AuthorizationSubplan { claims, .. } => claims,
            PolicyContext::System => return Ok(()),
        };
        for table_name in normalized_source_tables(input) {
            let table = schema
                .tables
                .iter()
                .find(|table| table.name == table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
            let mut query = authorization_query_from_read_policy(table);
            let mut values = BTreeMap::new();
            bind_scope_claim_operands(&mut query, claims, &mut values);
            for (name, claim) in disambiguate_policy_claim_params(&mut query, schema, &mut values)?
            {
                // The root policy may rediscover the same claim slot while
                // walking its source tables. Keep the already-lowered slot in
                // that case; a typed alias is only needed when the same claim
                // path is required at a genuinely different schema type.
                if params
                    .values()
                    .any(|existing| existing.path == claim.path && existing.ty == claim.ty)
                {
                    continue;
                }
                params.insert(name, claim);
            }
        }
        Ok(())
    }

    pub(super) fn normalized_row_set_shape(
        &self,
        shape: &ValidatedQuery,
        _binding: &Binding,
    ) -> Result<NormalizedRowSetShape, Error> {
        let schema = if shape.schema_version() == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            &self
                .catalogue
                .catalogue_schemas
                .get(&shape.schema_version())
                .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?
                .schema
        };
        let query = shape.query();
        let root_source = root_source_id(&query.table);
        let (mut auxiliary_sources, closure_paths) =
            collect_closure_paths(self, &query.table, shape.schema_version(), &query.includes)?;
        let source_node = RowSetNodeId("root".to_owned());
        let mut nodes = BTreeMap::from([(
            source_node.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        )]);
        let mut current = source_node;
        let mut join_contributions = Vec::new();
        let mut reachable_contributions = Vec::new();
        let inheritance_path = InheritanceExpansionPath::default();

        let binding_source_shape = PENDING_BINDING_SOURCE_SHAPE.to_owned();
        let query_chain_filters: &[Predicate] = if query.flat_join.is_some() {
            &[]
        } else {
            &query.filters
        };
        let unsupported_policy_branch = unsupported_policy_branch_reason(query);
        if unsupported_policy_branch.is_none() && !query.policy_branches.is_empty() {
            let mut union_inputs = Vec::new();
            if !policy_branch_base_is_converter_false(query) {
                let base_source_node = RowSetNodeId("policy_branch:base:root".to_owned());
                nodes.insert(
                    base_source_node.clone(),
                    RowSetExpr::Source {
                        source: root_source.clone(),
                        visibility: RowVisibility::Visible,
                    },
                );
                let base = normalize_policy_atom_chain(
                    &mut nodes,
                    &mut auxiliary_sources,
                    &mut join_contributions,
                    &mut reachable_contributions,
                    schema,
                    &root_source,
                    base_source_node,
                    "policy_branch:base",
                    PolicyAtomChain {
                        filters: query_chain_filters,
                        joins: &query.joins,
                        inherits: &query.inherits,
                        reachable: &query.reachable,
                    },
                    &binding_source_shape,
                    shape.params(),
                    false,
                    &inheritance_path,
                )?;
                union_inputs.push(UnionInput {
                    node: normalize_row_id_projection(
                        &mut nodes,
                        base,
                        &root_source,
                        RowSetNodeId("policy_branch:base:row_id".to_owned()),
                    ),
                    label: policy_branch_semantic_label(
                        &query.filters,
                        &query.joins,
                        &query.reachable,
                        &query.inherits,
                    )?,
                });
            }

            for (index, branch) in query.policy_branches.iter().enumerate() {
                let branch_source_node = RowSetNodeId(format!("policy_branch:{index}:root"));
                nodes.insert(
                    branch_source_node.clone(),
                    RowSetExpr::Source {
                        source: root_source.clone(),
                        visibility: RowVisibility::Visible,
                    },
                );
                let branch_current = normalize_policy_atom_chain(
                    &mut nodes,
                    &mut auxiliary_sources,
                    &mut join_contributions,
                    &mut reachable_contributions,
                    schema,
                    &root_source,
                    branch_source_node,
                    &format!("policy_branch:{index}"),
                    PolicyAtomChain {
                        filters: &branch.filters,
                        joins: &branch.joins,
                        inherits: &branch.inherits,
                        reachable: &branch.reachable,
                    },
                    &binding_source_shape,
                    shape.params(),
                    false,
                    &inheritance_path,
                )?;
                union_inputs.push(UnionInput {
                    node: normalize_row_id_projection(
                        &mut nodes,
                        branch_current,
                        &root_source,
                        RowSetNodeId(format!("policy_branch:{index}:row_id")),
                    ),
                    label: policy_branch_semantic_label(
                        &branch.filters,
                        &branch.joins,
                        &branch.reachable,
                        &branch.inherits,
                    )?,
                });
            }

            let union_node = RowSetNodeId("policy_branch:authorized_rows".to_owned());
            nodes.insert(
                union_node.clone(),
                RowSetExpr::Union {
                    inputs: union_inputs,
                },
            );
            let join_node = RowSetNodeId("policy_branch:authorize".to_owned());
            nodes.insert(
                join_node.clone(),
                RowSetExpr::Join {
                    left: current,
                    right: union_node,
                    mode: NormalizedJoinMode::Inner,
                    on: NormalizedPredicateExpr::Compare {
                        left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                        op: NormalizedComparisonOp::Eq,
                        right: NormalizedValueRef::SourceField {
                            source: root_source.clone(),
                            field: "row_uuid".to_owned(),
                        },
                    },
                },
            );
            current = join_node;
        } else {
            current = normalize_policy_atom_chain(
                &mut nodes,
                &mut auxiliary_sources,
                &mut join_contributions,
                &mut reachable_contributions,
                schema,
                &root_source,
                current,
                "query",
                PolicyAtomChain {
                    filters: query_chain_filters,
                    joins: &query.joins,
                    inherits: &query.inherits,
                    reachable: &query.reachable,
                },
                &binding_source_shape,
                shape.params(),
                true,
                &inheritance_path,
            )?;
        }

        // Flat joins are an output form separate from `JoinVia`. Every input
        // stays a normal source, so read-policy filtering and read-view/lens
        // projection happen before Groove's inner JoinOp combines records.
        if let Some(flat_join) = &query.flat_join {
            let root_name = flat_join
                .root_alias
                .as_deref()
                .unwrap_or(query.table.as_str())
                .to_owned();
            let mut routed_filters = flat_join_filters_by_source(&query.filters, &root_name)?;
            if let Some(filters) = routed_filters.remove(&root_name) {
                let filter_node = RowSetNodeId("flat_join:root_filter".to_owned());
                nodes.insert(
                    filter_node.clone(),
                    RowSetExpr::Filter {
                        input: current,
                        predicate: normalize_predicates(schema, &root_source, &filters, true)?,
                    },
                );
                current = filter_node;
            }
            let mut sources = BTreeMap::from([(root_name.clone(), root_source.clone())]);
            let mut tuple_sources = vec![root_source.clone()];
            let mut output_sources = vec![(
                flat_join
                    .root_alias
                    .as_deref()
                    .unwrap_or(query.table.as_str())
                    .to_owned(),
                root_source.clone(),
            )];

            for (index, join) in flat_join.sources.iter().enumerate() {
                let name = join
                    .alias
                    .as_deref()
                    .unwrap_or(join.table.as_str())
                    .to_owned();
                let source = SourceId {
                    table: join.table.clone(),
                    path: SourcePath {
                        components: vec![SourceRole::Alias(format!("flat_join:{index}:{name}"))],
                    },
                };
                let source_node = RowSetNodeId(format!("flat_join:{index}:source"));
                nodes.insert(
                    source_node.clone(),
                    RowSetExpr::Source {
                        source: source.clone(),
                        visibility: RowVisibility::Visible,
                    },
                );
                let right_input = if let Some(filters) = routed_filters.remove(&name) {
                    let filter_node = RowSetNodeId(format!("flat_join:{index}:filter"));
                    nodes.insert(
                        filter_node.clone(),
                        RowSetExpr::Filter {
                            input: source_node,
                            predicate: normalize_predicates(schema, &source, &filters, true)?,
                        },
                    );
                    filter_node
                } else {
                    source_node
                };
                auxiliary_sources.insert(source.clone());
                let value_ref = |field: &str| -> Result<NormalizedValueRef, Error> {
                    let (scope, column) = field.rsplit_once('.').ok_or_else(|| {
                        Error::QueryCapability(format!(
                            "flat join field must be qualified: {field}"
                        ))
                    })?;
                    let source = sources.get(scope).ok_or_else(|| {
                        Error::QueryCapability(format!("unknown flat join source {scope}"))
                    })?;
                    Ok(if column == "_id" {
                        NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
                    } else {
                        source_column_value(schema, source, column, JoinTarget::Column)
                    })
                };
                let (_, right_column) = join.on.right.rsplit_once('.').ok_or_else(|| {
                    Error::QueryCapability(format!(
                        "flat join field must be qualified: {}",
                        join.on.right
                    ))
                })?;
                let join_node = RowSetNodeId(format!("flat_join:{index}:join"));
                nodes.insert(
                    join_node.clone(),
                    RowSetExpr::Join {
                        left: current,
                        right: right_input,
                        mode: NormalizedJoinMode::Inner,
                        on: NormalizedPredicateExpr::Compare {
                            left: value_ref(&join.on.left)?,
                            op: NormalizedComparisonOp::Eq,
                            right: if right_column == "_id" {
                                NormalizedValueRef::RowId(RowIdRef::Source(source.clone()))
                            } else {
                                source_column_value(
                                    schema,
                                    &source,
                                    right_column,
                                    JoinTarget::Column,
                                )
                            },
                        },
                    },
                );
                current = join_node;
                sources.insert(name.clone(), source.clone());
                output_sources.push((name, source.clone()));
                tuple_sources.push(source);
            }

            if let Some(scope) = routed_filters.keys().next() {
                return Err(Error::QueryLowering(format!(
                    "flat join filter references unknown source {scope}"
                )));
            }

            let projection_node = RowSetNodeId("flat_join:output".to_owned());
            let mut columns = Vec::new();
            for (position, source) in tuple_sources.iter().enumerate() {
                columns.push(RowProjection {
                    output: TypedOutputField {
                        name: if position == 0 {
                            "row_uuid".to_owned()
                        } else {
                            format!("__flat_join_row_{position}")
                        },
                        ty: ColumnType::Uuid,
                    },
                    value: NormalizedValueRef::RowId(RowIdRef::Source(source.clone())),
                });
            }
            // Retain the representative root version used by the existing
            // real-row membership envelope. Joined source versions stay in
            // their own source terminals; the rendered tuple itself is kept
            // in the membership payload below.
            for (name, ty) in [
                ("tx_time", ColumnType::U64),
                ("tx_node_id", ColumnType::U64),
            ] {
                columns.push(RowProjection {
                    output: TypedOutputField {
                        name: name.to_owned(),
                        ty,
                    },
                    value: NormalizedValueRef::SourceField {
                        source: root_source.clone(),
                        field: name.to_owned(),
                    },
                });
            }
            for (name, source) in &output_sources {
                let source_schema = schema
                    .tables
                    .iter()
                    .find(|table| table.name == source.table)
                    .ok_or_else(|| {
                        Error::QueryCapability(format!("unknown flat join table {}", source.table))
                    })?;
                for column in source_schema.columns.iter() {
                    columns.push(RowProjection {
                        output: TypedOutputField {
                            name: format!("{name}.{}", column.name),
                            ty: column.column_type.clone(),
                        },
                        value: NormalizedValueRef::SourceField {
                            source: source.clone(),
                            field: column.name.clone(),
                        },
                    });
                }
            }
            nodes.insert(
                projection_node.clone(),
                RowSetExpr::Project {
                    input: current,
                    columns,
                },
            );
            current = projection_node;
        }

        for (index, subquery) in query.array_subqueries.iter().enumerate() {
            current = normalize_array_subquery(
                &mut nodes,
                current,
                schema,
                &root_source,
                subquery,
                &[index],
            )?;
        }

        if query.aggregate.is_none() && !query.order_by.is_empty() {
            let order_node = RowSetNodeId("order".to_owned());
            nodes.insert(
                order_node.clone(),
                RowSetExpr::OrderBy {
                    input: current,
                    keys: query
                        .order_by
                        .iter()
                        .map(|order| normalize_order_key(schema, &root_source, order))
                        .collect::<Result<Vec<_>, Error>>()?,
                },
            );
            current = order_node;
        }
        if query.aggregate.is_none() && (query.limit.is_some() || query.offset != 0) {
            let slice_node = RowSetNodeId("slice".to_owned());
            nodes.insert(
                slice_node.clone(),
                RowSetExpr::Slice {
                    input: current,
                    partition_by: Vec::new(),
                    limit: query.limit.map(|limit| limit.min(u32::MAX as usize) as u32),
                    offset: query.offset.min(u32::MAX as usize) as u32,
                    tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(
                        root_source.clone(),
                    ))],
                    rank_output: None,
                },
            );
            current = slice_node;
        }

        if let Some(marker) = unsupported_policy_branch {
            let node = RowSetNodeId("unsupported:policy_branches".to_owned());
            nodes.insert(
                node.clone(),
                RowSetExpr::Distinct {
                    input: current,
                    keys: vec![NormalizedValueRef::Literal(marker.into_bytes())],
                },
            );
            current = node;
        }

        if let Some(aggregate) = &query.aggregate {
            let aggregate_node = RowSetNodeId("aggregate".to_owned());
            nodes.insert(
                aggregate_node.clone(),
                RowSetExpr::Aggregate {
                    input: current,
                    group_by: normalized_aggregate_group_by(schema, &root_source, aggregate)?,
                    outputs: normalized_aggregate_outputs(schema, &root_source, aggregate)?,
                },
            );
            current = aggregate_node;
        }

        let mut normalized = NormalizedRowSetShape {
            identity: NormalizedShapeIdentity {
                shape_id: shape.shape_id(),
                canonical: shape.canonical_bytes().to_vec(),
            },
            root: current,
            result: ResultId::RealRow {
                table: query.table.clone(),
                row: ResultRowRef::Source(root_source),
            },
            auxiliary_sources,
            closure_paths,
            join_contributions,
            reachable_contributions,
            nodes,
        };
        let claim_params = binding_claim_params_for_shape(&normalized, shape.params());
        let binding_source_shape =
            query_binding_source_shape_for_parts(shape.params(), &claim_params);
        retarget_binding_value_sources(&mut normalized, &binding_source_shape);
        Ok(normalized)
    }

    pub(super) fn normalized_include_deleted_row_set_shape(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<NormalizedRowSetShape, Error> {
        let mut normalized = self.normalized_row_set_shape(shape, binding)?;
        let root_source = root_source_id(&shape.query().table);
        for node in normalized.nodes.values_mut() {
            if let RowSetExpr::Source { source, visibility } = node
                && *source == root_source
            {
                *visibility = RowVisibility::IncludeDeleted;
            }
        }
        Ok(normalized)
    }
}

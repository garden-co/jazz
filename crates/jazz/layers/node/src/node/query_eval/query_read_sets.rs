//! Read-view-specific source selection for query evaluation.

use super::*;

fn canonical_projection(schema_version: SchemaVersionId) -> SchemaProjection<RequestedSourceStage> {
    SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    }
}

/// The sources named by the shape's `RowSetExpr::Source` nodes.
fn node_sources(shape: &NormalizedRowSetShape) -> impl Iterator<Item = &SourceId> {
    shape.nodes.values().filter_map(|node| match node {
        RowSetExpr::Source { source, .. } => Some(source),
        _ => None,
    })
}

/// Node sources followed by the shape's auxiliary sources.
fn all_sources(shape: &NormalizedRowSetShape) -> impl Iterator<Item = &SourceId> {
    node_sources(shape).chain(&shape.auxiliary_sources)
}

/// A primary read view that reads every source in `sources` through `expr`.
fn uniform_read_set<'a>(
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    sources: impl Iterator<Item = &'a SourceId>,
    expr: impl Fn() -> SourceExpr<RequestedSourceStage>,
) -> RequestedReadSet {
    QueryReadSet::primary(ReadView {
        read_schema,
        policy_schema,
        sources: sources.map(|source| (source.clone(), expr())).collect(),
    })
}

pub(super) fn current_query_read_set(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    settled_binding_view: Option<BindingViewKey>,
    settled_authority_result_key: Option<crate::protocol::AuthorityResultKey>,
    settled_current_default: bool,
) -> RequestedReadSet {
    let projection = canonical_projection(read_schema);
    let expr = || match settled_binding_view {
        Some(binding_view) => SourceExpr::SettledBindingView {
            current_default: settled_current_default,
            projection: projection.clone(),
            binding_view,
            authority_result_key: settled_authority_result_key.clone(),
        },
        None => SourceExpr::VisibleCurrent {
            projection: projection.clone(),
            data: DataSource::Current,
            tier,
        },
    };
    uniform_read_set(read_schema, policy_schema, all_sources(shape), expr)
}

pub(super) fn historical_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    position: GlobalTime,
) -> RequestedReadSet {
    let projection = canonical_projection(schema_version);
    // Historical reads cover only the shape's node sources.
    uniform_read_set(schema_version, schema_version, node_sources(shape), || {
        SourceExpr::HistoryCut {
            projection: projection.clone(),
            data: DataSource::Current,
            position,
        }
    })
}

pub(super) fn snapshot_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    snapshot: Snapshot,
) -> RequestedReadSet {
    let projection = canonical_projection(schema_version);
    uniform_read_set(schema_version, schema_version, all_sources(shape), || {
        SourceExpr::SnapshotRef {
            projection: projection.clone(),
            data: DataSource::Current,
            snapshot: snapshot.clone(),
        }
    })
}

pub(super) fn tx_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    tx_id: OpenTransactionId,
    snapshot: Snapshot,
) -> RequestedReadSet {
    let projection = canonical_projection(schema_version);
    uniform_read_set(schema_version, schema_version, all_sources(shape), || {
        SourceExpr::WithOverlays {
            input: Box::new(SourceExpr::SnapshotRef {
                projection: projection.clone(),
                data: DataSource::Current,
                snapshot: snapshot.clone(),
            }),
            overlays: OverlayStack {
                entries: vec![OverlayRef::OpenTransaction(tx_id)],
            },
        }
    })
}

/// Move a branched program's policy-evidence sources into its branch.
///
/// `head_values` are the branch-column values of the enclosing request's
/// table-projected heads. Each branched source table is projected onto them,
/// as an ordinary branch read projects its selector (`INV-BVIEW-8`), so every
/// branched table the program reads observes the same branch head. Sources
/// over unbranched tables keep their current expression. A branched table
/// whose branch columns the heads do not name cannot be placed in that
/// branch; it is refused rather than read from another branch.
pub(super) fn branch_read_view_from_heads(
    read_view: &mut ReadView<RequestedSourceStage>,
    schema: &JazzSchema,
    head_values: &BTreeMap<String, crate::protocol::BranchColumnValue>,
    tier: DurabilityTier,
) -> Result<(), Error> {
    let projection = canonical_projection(read_view.read_schema);
    for (source, expr) in &mut read_view.sources {
        let table_name =
            crate::schema::history::history_source_table(&source.table).unwrap_or(&source.table);
        let table = schema
            .tables
            .iter()
            .find(|table| table.name == table_name)
            .ok_or(Error::InvalidStoredValue(
                "branch view source table missing",
            ))?;
        if table.branch_by.is_empty() {
            continue;
        }
        let values = table
            .branch_by
            .iter()
            .map(|column| Some((column.clone(), head_values.get(column)?.clone())))
            .collect::<Option<BTreeMap<_, _>>>()
            .ok_or_else(|| {
                history::unsupported("a branch read policy over tables with other branch columns")
            })?;
        let (head, _) = schema
            .project_branch_selector(table, &BranchSelector { values })
            .map_err(Error::InvalidBranchKey)?;
        *expr = SourceExpr::BranchView {
            projection: projection.clone(),
            head,
            base: None,
            tier,
        };
    }
    Ok(())
}

pub(super) fn query_read_set_for_read_view(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
    settled_binding_view: Option<BindingViewKey>,
    settled_authority_result_key: Option<crate::protocol::AuthorityResultKey>,
    schema: &JazzSchema,
) -> Result<RequestedReadSet, Error> {
    if matches!(
        &read_view.source,
        ReadViewSourceSpec::BranchView { base: Some(_), .. }
    ) && all_sources(shape).any(|source| crate::schema::history::is_history_table(&source.table))
    {
        return Err(history::unsupported("history with a branch base"));
    }
    // All receiver programs, including aggregates, consume the authority's
    // exact source closure. Aggregate results are computed by the ordinary
    // local graph; they are not a separate authority-provided result payload.
    if settled_binding_view.is_some() {
        // Settled rows are already the authority's effective result for the
        // exact BindingViewKey, whose identity includes the normalized read
        // view. Reapplying branch selection locally would incorrectly treat
        // those effective rows as raw per-branch history.
        return Ok(current_query_read_set(
            shape,
            read_schema,
            policy_schema,
            tier,
            settled_binding_view,
            settled_authority_result_key,
            matches!(read_view.source, ReadViewSourceSpec::Current),
        ));
    }
    match &read_view.source {
        ReadViewSourceSpec::Current => Ok(current_query_read_set(
            shape,
            read_schema,
            policy_schema,
            tier,
            None,
            None,
            false,
        )),
        ReadViewSourceSpec::Snapshot { snapshot } => Ok(snapshot_query_read_set(
            shape,
            read_schema,
            Snapshot {
                owner: snapshot.owner,
                global_base: snapshot.global_base,
                local_base: snapshot.local_base,
                dots: snapshot.dots.clone(),
            },
        )),
        ReadViewSourceSpec::BranchView { head, base } => {
            let projection = canonical_projection(read_schema);
            let sources = all_sources(shape)
                .map(|source| {
                    let table = schema
                        .tables
                        .iter()
                        .find(|table| {
                            table.name
                                == crate::schema::history::history_source_table(&source.table)
                                    .unwrap_or(&source.table)
                        })
                        .ok_or(Error::InvalidStoredValue(
                            "branch view source table missing",
                        ))?;
                    let (head_key, base) =
                        layer_stack::branch_view_stack(schema, table, head, base.as_ref())
                            .map_err(Error::InvalidBranchKey)?;
                    Ok((
                        source.clone(),
                        SourceExpr::BranchView {
                            projection: projection.clone(),
                            head: head_key,
                            base,
                            tier,
                        },
                    ))
                })
                .collect::<Result<_, Error>>()?;
            Ok(QueryReadSet::primary(ReadView {
                read_schema,
                policy_schema,
                sources,
            }))
        }
    }
}

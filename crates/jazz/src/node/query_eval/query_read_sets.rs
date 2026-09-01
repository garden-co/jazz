//! Read-view-specific source selection for query evaluation.

use super::*;
use crate::node::query_engine::BranchViewSourceBase;
use crate::protocol::BranchViewBase;

pub(super) fn current_query_read_set(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    settled_binding_view: Option<BindingViewKey>,
    settled_authority_result_key: Option<crate::protocol::AuthorityResultKey>,
    settled_requires_result_payload: bool,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(read_schema),
        lens: LensSelection::Canonical,
    };
    let mut sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((
                source.clone(),
                if let Some(binding_view) = settled_binding_view {
                    SourceExpr::SettledBindingView {
                        projection: projection.clone(),
                        binding_view,
                        authority_result_key: settled_authority_result_key.clone(),
                        rows: SettledBindingRows::ResultMembers,
                        requires_result_payload: settled_requires_result_payload,
                    }
                } else {
                    SourceExpr::VisibleCurrent {
                        projection: projection.clone(),
                        data: DataSource::Current,
                        tier,
                    }
                },
            )),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for source in &shape.auxiliary_sources {
        if let Some(binding_view) = settled_binding_view
            && let Some(source_index) = flat_tuple_source_index(source)
        {
            sources.insert(
                source.clone(),
                SourceExpr::SettledBindingView {
                    projection: projection.clone(),
                    binding_view,
                    authority_result_key: settled_authority_result_key.clone(),
                    rows: SettledBindingRows::FlatTupleContributor { source_index },
                    requires_result_payload: settled_requires_result_payload,
                },
            );
            continue;
        }
        sources.insert(
            source.clone(),
            SourceExpr::VisibleCurrent {
                projection: projection.clone(),
                data: DataSource::Current,
                tier,
            },
        );
    }
    QueryReadSet::primary(ReadView {
        read_schema,
        policy_schema,
        sources,
    })
}

fn flat_tuple_source_index(source: &SourceId) -> Option<usize> {
    let [SourceRole::Alias(alias)] = source.path.components.as_slice() else {
        return None;
    };
    alias
        .strip_prefix("flat_join:")?
        .split_once(':')?
        .0
        .parse()
        .ok()
}

pub(super) fn historical_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    position: GlobalTime,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    };
    let sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((
                source.clone(),
                SourceExpr::HistoryCut {
                    projection: projection.clone(),
                    data: DataSource::Current,
                    position,
                },
            )),
            _ => None,
        })
        .collect();
    QueryReadSet::primary(ReadView {
        read_schema: schema_version,
        policy_schema: schema_version,
        sources,
    })
}

pub(super) fn snapshot_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    snapshot: Snapshot,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    };
    let source_expr = || SourceExpr::SnapshotRef {
        projection: projection.clone(),
        data: DataSource::Current,
        snapshot: snapshot.clone(),
    };
    let mut sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((source.clone(), source_expr())),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for source in &shape.auxiliary_sources {
        sources.insert(source.clone(), source_expr());
    }
    QueryReadSet::primary(ReadView {
        read_schema: schema_version,
        policy_schema: schema_version,
        sources,
    })
}

pub(super) fn tx_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    tx_id: OpenTransactionId,
    snapshot: Snapshot,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    };
    let overlay = || SourceExpr::WithOverlays {
        input: Box::new(SourceExpr::SnapshotRef {
            projection: projection.clone(),
            data: DataSource::Current,
            snapshot: snapshot.clone(),
        }),
        overlays: OverlayStack {
            entries: vec![OverlayRef::OpenTransaction(tx_id)],
        },
    };
    let mut sources = shape
        .nodes
        .values()
        .filter_map(|node| match node {
            RowSetExpr::Source { source, .. } => Some((source.clone(), overlay())),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for source in &shape.auxiliary_sources {
        sources.insert(source.clone(), overlay());
    }
    QueryReadSet::primary(ReadView {
        read_schema: schema_version,
        policy_schema: schema_version,
        sources,
    })
}

pub(super) fn query_read_set_for_read_view(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
    settled_binding_view: Option<BindingViewKey>,
    settled_authority_result_key: Option<crate::protocol::AuthorityResultKey>,
    aggregate_query: bool,
    schema: &JazzSchema,
) -> Result<RequestedReadSet, Error> {
    // A settled binding view stores aggregate output as synthetic result
    // members, not source-table rows. Re-feeding it through the source graph
    // would turn an aggregate replacement into an empty source and retract
    // the public row. Its authoritative result is materialized directly by
    // the subscription facade instead.
    let settled_binding_view = (!aggregate_query).then_some(settled_binding_view).flatten();
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
            matches!(read_view.source, ReadViewSourceSpec::BranchView { .. }),
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
        ReadViewSourceSpec::Snapshot { .. } => Err(Error::QueryCapability(
            "snapshot read_view requires unified snapshot source lowering".to_owned(),
        )),
        ReadViewSourceSpec::BranchView { head, base } => {
            let projection = SchemaProjection {
                schema_family: SchemaFamilySelection::Current,
                storage: StorageSchemaSelection::Single(read_schema),
                lens: LensSelection::Canonical,
            };
            let mut sources = BTreeMap::new();
            for source in shape
                .nodes
                .values()
                .filter_map(|node| match node {
                    RowSetExpr::Source { source, .. } => Some(source),
                    _ => None,
                })
                .chain(&shape.auxiliary_sources)
            {
                let table = schema
                    .tables
                    .iter()
                    .find(|table| table.name == source.table)
                    .ok_or(Error::InvalidStoredValue(
                        "branch view source table missing",
                    ))?;
                let (head_key, _) = schema
                    .project_branch_view_selector(table, head)
                    .map_err(Error::InvalidBranchKey)?;
                let mut base = base
                    .as_ref()
                    .map(|base| match base {
                        BranchViewBase::Current(selector) => schema
                            .project_branch_view_selector(table, selector)
                            .map(|(key, _)| BranchViewSourceBase::Current(key)),
                        BranchViewBase::Snapshot { branch, snapshot } => schema
                            .project_branch_view_selector(table, branch)
                            .map(|(key, _)| BranchViewSourceBase::Snapshot(key, snapshot.clone())),
                    })
                    .transpose()
                    .map_err(Error::InvalidBranchKey)?;
                if base.as_ref().is_some_and(|base| match base {
                    BranchViewSourceBase::Current(key) | BranchViewSourceBase::Snapshot(key, _) => {
                        key == &head_key
                    }
                }) {
                    base = None;
                }
                sources.insert(
                    source.clone(),
                    SourceExpr::BranchView {
                        projection: projection.clone(),
                        head: head_key,
                        base,
                        tier,
                    },
                );
            }
            Ok(QueryReadSet::primary(ReadView {
                read_schema,
                policy_schema,
                sources,
            }))
        }
    }
}

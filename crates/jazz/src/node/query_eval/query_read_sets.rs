//! Read-view-specific source selection for query evaluation.

use super::*;

pub(super) fn current_query_read_set(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    settled_binding_view: Option<BindingViewKey>,
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
                        rows: SettledBindingRows::ResultMembers,
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
                    rows: SettledBindingRows::FlatTupleContributor { source_index },
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

pub(super) fn branch_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    tier: DurabilityTier,
    branch_id: BranchId,
) -> RequestedReadSet {
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema_version),
        lens: LensSelection::Canonical,
    };
    let source_expr = || SourceExpr::VisibleCurrent {
        projection: projection.clone(),
        data: DataSource::Branch(branch_id),
        tier,
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

pub(super) fn query_read_set_for_read_view(
    shape: &NormalizedRowSetShape,
    read_schema: SchemaVersionId,
    policy_schema: SchemaVersionId,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
    settled_binding_view: Option<BindingViewKey>,
    aggregate_query: bool,
) -> Result<RequestedReadSet, Error> {
    // A settled binding view stores aggregate output as synthetic result
    // members, not source-table rows. Re-feeding it through the source graph
    // would turn an aggregate replacement into an empty source and retract
    // the public row. Its authoritative result is materialized directly by
    // the subscription facade instead.
    let settled_binding_view = (!aggregate_query).then_some(settled_binding_view).flatten();
    // Branch-current v1 deliberately remains an overlay-first source graph at
    // every peer. A selected result may belong to a detached usage site,
    // whereas durable branch history must continue to drive live writes,
    // deletion, and restoration. BranchId remains in the binding/read-view
    // key; this only chooses its live data source.
    if let ReadViewSourceSpec::Branch { branch } = &read_view.source
        && read_view.schema == Default::default()
        && read_view.overlays.is_empty()
    {
        return Ok(branch_query_read_set(
            shape,
            read_schema,
            tier,
            BranchId(*branch),
        ));
    }
    if settled_binding_view.is_some() {
        if !read_view.is_default() {
            return Err(Error::QueryCapability(
                "settled binding view sources do not support non-default read_view yet".to_owned(),
            ));
        }
        return Ok(current_query_read_set(
            shape,
            read_schema,
            policy_schema,
            tier,
            settled_binding_view,
        ));
    }
    match &read_view.source {
        ReadViewSourceSpec::Current => Ok(current_query_read_set(
            shape,
            read_schema,
            policy_schema,
            tier,
            None,
        )),
        ReadViewSourceSpec::Branch { branch }
            if read_view.schema == Default::default() && read_view.overlays.is_empty() =>
        {
            Ok(branch_query_read_set(
                shape,
                read_schema,
                tier,
                BranchId(*branch),
            ))
        }
        ReadViewSourceSpec::MergedBranches { .. } => Err(Error::QueryCapability(
            "merged branch read_view requires unified branch merge source lowering".to_owned(),
        )),
        ReadViewSourceSpec::Snapshot { .. } => Err(Error::QueryCapability(
            "snapshot read_view requires unified snapshot source lowering".to_owned(),
        )),
        ReadViewSourceSpec::Branch { .. } => Err(Error::QueryCapability(
            "branch read_view does not support schema lenses or overlays yet".to_owned(),
        )),
    }
}

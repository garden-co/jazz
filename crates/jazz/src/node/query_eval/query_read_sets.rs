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
        // Auxiliary closure sources are not result members of the settled binding
        // view. Keep the result/root source pinned to the settled view, but read
        // implicit reference targets from current storage so serving can resolve
        // their rows instead of treating missing result-set entries as coverage
        // gaps.
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

pub(super) fn historical_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    position: GlobalSeq,
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

pub(super) fn tx_query_read_set(
    shape: &NormalizedRowSetShape,
    schema_version: SchemaVersionId,
    tx_id: OpenBatchId,
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

//! Turn logical query read requirements into concrete Groove source graphs.
//!
//! This stage chooses physical, inline, historical, branch, or settled-view
//! inputs and applies durability, schema projection, and access-path decisions.
//! It does not normalize query syntax or materialize engine output into public
//! rows.

use super::*;
pub(super) struct CurrentQuerySourceResolver<'a, S> {
    pub(super) node: &'a mut NodeState<S>,
    pub(super) read_view: &'a ReadView<RequestedSourceStage>,
    /// A maintained trusted subscription must remain connected when its first
    /// branch write materializes a sparse partition.  It receives an empty
    /// process-local source at compile time; the durable partition is still
    /// published only by the first write.
    #[allow(dead_code)]
    pub(super) prepare_branch_subscription_sources: bool,
    pub(super) inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
    pub(super) access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    /// Query-local enum boundary targets, keyed by logical source.  Defining
    /// a variant target invalidates table inputs, so reuse it across the main
    /// source, access path, and metadata sidecars of one compiled program.
    pub(super) current_projection_targets: BTreeMap<SourceId, String>,
}

pub(super) struct CurrentSourceGraph {
    pub(super) graph: GraphBuilder,
    pub(super) descriptor: RecordDescriptor,
    pub(super) metadata: BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
}

#[derive(Clone, Debug)]
pub(super) enum CurrentAccessPath {
    PrimaryKey(Vec<Value>),
    Index { column: String, prefix: Vec<Value> },
}

impl<S> SourceResolver for CurrentQuerySourceResolver<'_, S>
where
    S: OrderedKvStorage,
{
    fn resolve_source(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        let Some(source) = self.read_view.sources.get(&request.source) else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        let (projection, graph_tier, history_position, open_tx_overlay, branch_view) = match source
        {
            SourceExpr::VisibleCurrent {
                projection,
                data: DataSource::Current,
                tier,
            } => (projection, Some(*tier), None, None, None),
            SourceExpr::BranchView {
                projection,
                head,
                base,
                tier,
            } => (
                projection,
                Some(*tier),
                None,
                None,
                Some((head, base.as_ref())),
            ),
            SourceExpr::HistoryCut {
                projection,
                data: DataSource::Current,
                position,
            } => (projection, None, Some(*position), None, None),
            SourceExpr::SettledBindingView {
                projection,
                binding_view,
                rows,
            } => {
                if request.visibility != RowVisibility::Visible {
                    return Err(source_resolution_error(request, SourceGap::Coverage));
                }
                if !matches!(projection.schema_family, SchemaFamilySelection::Current)
                    || !matches!(projection.storage, StorageSchemaSelection::Single(_))
                    || !matches!(projection.lens, LensSelection::Canonical)
                {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::SchemaProjection,
                    ));
                }
                match self.node.settled_binding_view_source_rows(
                    &request.source.table,
                    self.read_view.read_schema,
                    *binding_view,
                    *rows,
                ) {
                    Ok(rows) => {
                        let table = self
                            .node
                            .table_in_schema(&request.source.table, self.read_view.read_schema)
                            .map_err(|_| {
                                source_resolution_error(request, SourceGap::SchemaProjection)
                            })?;
                        let schema_version_alias = self
                            .node
                            .ensure_schema_version_alias(self.read_view.read_schema)
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        let (graph, descriptor, metadata) =
                            inline_current_graph_with_source_metadata(
                                &table,
                                rows,
                                schema_version_alias,
                                "settled-binding-view",
                                &request.requirements,
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        return Ok(ResolvedSource {
                            table_schema: table,
                            graph,
                            row_shape: SourceRowShape {
                                source: request.source.clone(),
                                descriptor,
                                row_uuid_field: "row_uuid".to_owned(),
                                metadata,
                            },
                            routing_fields: BTreeSet::new(),
                            content_version: None,
                            deletion_register: None,
                        });
                    }
                    Err(Error::MissingTransaction(_)) => {}
                    Err(_) => {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    if !matches!(projection.schema_family, SchemaFamilySelection::Current)
                        || !matches!(projection.storage, StorageSchemaSelection::Single(_))
                        || !matches!(projection.lens, LensSelection::Canonical)
                    {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::SchemaProjection,
                        ));
                    }
                    match self.node.settled_binding_view_source_rows(
                        &request.source.table,
                        self.read_view.read_schema,
                        *binding_view,
                        *rows,
                    ) {
                        Ok(rows) => {
                            let table = self
                                .node
                                .table_in_schema(&request.source.table, self.read_view.read_schema)
                                .map_err(|_| {
                                    source_resolution_error(request, SourceGap::SchemaProjection)
                                })?;
                            let schema_version_alias = self
                                .node
                                .ensure_schema_version_alias(self.read_view.read_schema)
                                .map_err(|_| {
                                    source_resolution_error(request, SourceGap::Coverage)
                                })?;
                            let (graph, descriptor, metadata) =
                                inline_current_graph_with_source_metadata(
                                    &table,
                                    rows,
                                    schema_version_alias,
                                    "settled-binding-view",
                                    &request.requirements,
                                )
                                .map_err(|_| {
                                    source_resolution_error(request, SourceGap::Coverage)
                                })?;
                            return Ok(ResolvedSource {
                                table_schema: table,
                                graph,
                                row_shape: SourceRowShape {
                                    source: request.source.clone(),
                                    descriptor,
                                    row_uuid_field: "row_uuid".to_owned(),
                                    metadata,
                                },
                                routing_fields: BTreeSet::new(),
                                content_version: None,
                                deletion_register: None,
                            });
                        }
                        Err(Error::MissingTransaction(_)) => {}
                        Err(_) => {
                            return Err(source_resolution_error(request, SourceGap::Coverage));
                        }
                    }
                    (
                        projection,
                        Some(DurabilityTier::Global),
                        None,
                        None,
                        None,
                        None,
                    )
                }
                (projection, Some(DurabilityTier::Global), None, None, None)
            }
            SourceExpr::WithOverlays { input, overlays } => {
                let (projection, tier) = match input.as_ref() {
                    SourceExpr::VisibleCurrent {
                        projection,
                        data: DataSource::Current,
                        tier,
                    } => (projection, Some(*tier)),
                    SourceExpr::SnapshotRef {
                        projection,
                        data: DataSource::Current,
                        snapshot: _,
                    } => (projection, None),
                    _ => {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::TransactionReadOverlay,
                        ));
                    };
                    (projection, tier, None, None, Some(*tx_id), None)
                }
                _ => {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::HistoricalStorageCut,
                    ));
                };
                (projection, tier, None, Some(*tx_id), None)
            }
            _ => {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
        };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(
                projection.storage,
                StorageSchemaSelection::Single(_) | StorageSchemaSelection::CompatiblePartitions
            )
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            return Err(source_resolution_error(
                request,
                SourceGap::SchemaProjection,
            ));
        }
        let table = self
            .node
            .table_in_schema_or_branch_metadata(&request.source.table, self.read_view.read_schema)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        // Policy-proof dependencies are raw evidence for the outer policy.
        // Re-applying their own read policy recursively both changes the
        // outer predicate's meaning and can manufacture a proof cycle.
        let authorization = if matches!(
            request.authorization,
            SourceAuthorizationRequest::PolicyProof { .. }
        ) {
            SourceAuthorizationRequest::System
        } else {
            request.authorization.clone()
        };
        if let Some(rows) = self.inline_sources.get(&request.source) {
            if request.visibility != RowVisibility::Visible
                || !request.requirements.metadata.is_empty()
                || !matches!(authorization, SourceAuthorizationRequest::System)
            {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            let graph = inline_current_graph(&table, rows.clone())
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let descriptor = current_row_descriptor(&table);
            return Ok(ResolvedSource {
                table_schema: table,
                graph,
                row_shape: SourceRowShape {
                    source: request.source.clone(),
                    descriptor,
                    row_uuid_field: "row_uuid".to_owned(),
                    metadata: BTreeMap::new(),
                },
                routing_fields: BTreeSet::new(),
                content_version: None,
                deletion_register: None,
            });
        }
        let (graph, descriptor, metadata, routing_fields) = if let Some((head, base)) = branch_view
        {
            if request.visibility != RowVisibility::Visible
                || !request.requirements.metadata.is_empty()
                || !matches!(authorization, SourceAuthorizationRequest::System)
            {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            let rows = self
                .node
                .branch_view_rows_for_schema(
                    &request.source.table,
                    self.read_view.read_schema,
                    graph_tier.expect("branch view has a current tier"),
                    head,
                    base,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let graph = inline_current_graph(&table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            (
                graph,
                current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else if let Some(position) = history_position {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
            let needs_settle_position = request
                .requirements
                .metadata
                .contains(&SourceMetadataRequirement::SettlePosition);
            let mut metadata = BTreeMap::new();
            if needs_settle_position {
                metadata.insert(
                    SourceMetadataRequirement::SettlePosition,
                    SourceMetadataFields::SettlePosition {
                        settle_position_field: "settle_position".to_owned(),
                    },
                );
            }
            let descriptor = current_row_descriptor_with_hidden_source_fields(&table, &metadata);
            let base = self.projected_historical_source_graph(request, &table, position)?;
            let base = if needs_settle_position {
                base.project_fields(
                    current_row_fields(&table)
                        .into_iter()
                        .map(ProjectField::named)
                        .chain([ProjectField::null_typed(
                            "settle_position",
                            ValueType::Nullable(Box::new(ValueType::U64)),
                        )])
                        .collect::<Vec<_>>(),
                )
            } else {
                base
            };
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::HistoricalStorageCut,
                        ));
                    }
                    let policy_request = self.node.table_read_policy_authorization_request_at(
                        self.read_view.policy_schema,
                        &table.name,
                        *permission_subject,
                        ParamBindingMode::InlineAllReachableSeeds,
                        position,
                        plan.binding_source_shape.clone(),
                        plan.binding_user_params.clone(),
                        plan.binding_claim_params.clone(),
                    );
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &descriptor_field_names(&descriptor).map_err(|_| {
                                source_resolution_error(request, SourceGap::HistoricalStorageCut)
                            })?,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if let Some(tx_id) = open_tx_overlay {
            let include_deleted = request.visibility == RowVisibility::IncludeDeleted;
            let rows = self
                .node
                .tx_current_rows_in_schema_with_options(
                    tx_id,
                    self.read_view.read_schema,
                    &request.source.table,
                    include_deleted,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::TransactionReadOverlay))?;
            let (graph, descriptor) = if include_deleted {
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        let deleted = row.is_deleted();
                        (row, deleted)
                    })
                    .collect();
                (
                    inline_include_deleted_current_graph(&table, rows).map_err(|_| {
                        source_resolution_error(request, SourceGap::TransactionReadOverlay)
                    })?,
                    include_deleted_current_row_descriptor(&table),
                )
            } else {
                (
                    inline_current_graph(&table, rows).map_err(|_| {
                        source_resolution_error(request, SourceGap::TransactionReadOverlay)
                    })?,
                    current_row_descriptor(&table),
                )
            };
            (graph, descriptor, BTreeMap::new(), BTreeSet::new())
        } else if request.visibility == RowVisibility::Visible
            && self.needs_projected_current_source(&request.source.table)
        {
            if !request.requirements.metadata.is_empty() {
                let source = self.projected_maintained_visible_current_source_graph(
                    request,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                )?;
                resolved_current_source_graph(
                    self.node,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                    &request.requirements,
                    &authorization,
                    self.read_view.policy_schema,
                    Some(source.graph),
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            } else {
                let source = self.projected_visible_current_source_graph(
                    request,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                )?;
                let graph = match &authorization {
                    SourceAuthorizationRequest::System => source.graph,
                    SourceAuthorizationRequest::PolicyFiltered {
                        permission_subject,
                        plan,
                    }
                    | SourceAuthorizationRequest::PolicyProof {
                        permission_subject,
                        plan,
                    } => {
                        if plan.protected_source.table != table.name
                            || plan.role != PolicyDecisionRole::Read
                            || plan.protected_row_field != "row_uuid"
                        {
                            return Err(source_resolution_error(request, SourceGap::Coverage));
                        }
                        let policy_request = self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            ParamBindingMode::InlineAllReachableSeeds,
                            graph_tier.expect("visible current source has a tier"),
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                        self.node
                            .policy_filtered_current_source_graph_via_query_engine(
                                policy_request,
                                source.graph,
                                &current_row_fields(&table),
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                (graph, source.descriptor, source.metadata, BTreeSet::new())
            }
        } else if request.visibility == RowVisibility::IncludeDeleted
            && self.needs_projected_current_source(&request.source.table)
        {
            let tier = graph_tier.expect("visible current source has a tier");
            let rows = self
                .node
                .include_deleted_current_rows_for_schema(
                    &request.source.table,
                    self.read_view.read_schema,
                    tier,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let base = inline_include_deleted_current_graph(&table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base.clone(),
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = self
                        .node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base.clone(),
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (
                graph,
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else if request.visibility == RowVisibility::IncludeDeleted {
            let tier = graph_tier.expect("visible current source has a tier");
            let base = include_deleted_current_graph(&table, tier);
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = self
                        .node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .policy_filtered_current_source_graph_via_query_engine(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (
                graph,
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else {
            let selected_base = self.selected_global_current_source_graph(
                request,
                &table,
                graph_tier.expect("visible current source has a tier"),
            )?;
            if selected_base.is_none() {
                self.node.query_engine_read_metrics.source_full_scans += 1;
            }
            resolved_current_source_graph(
                self.node,
                &table,
                graph_tier.expect("visible current source has a tier"),
                &request.requirements,
                &authorization,
                self.read_view.policy_schema,
                selected_base,
            )
            .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
        };
        let deletion_register = self.deletion_register_source_for_request(
            request,
            &table,
            graph_tier,
            history_position,
            open_tx_overlay,
        )?;
        let content_version = self.content_version_source_for_request(
            request,
            &table,
            graph_tier,
            history_position,
            open_tx_overlay,
        )?;
        Ok(ResolvedSource {
            table_schema: table,
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
            routing_fields,
            content_version,
            deletion_register,
        })
    }
}

impl<S> CurrentQuerySourceResolver<'_, S>
where
    S: OrderedKvStorage,
{
    pub(crate) fn current_projection_target(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> Result<String, SourceResolutionError> {
        if let Some(target) = self.current_projection_targets.get(&request.source) {
            return Ok(target.clone());
        }
        let required_fields = self.current_projection_required_fields(request, table);
        let target = self
            .node
            .ensure_physical_current_projection_for_enum_columns(
                self.read_view.read_schema,
                &table.name,
                &required_fields,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        self.current_projection_targets
            .insert(request.source.clone(), target.clone());
        Ok(target)
    }

    pub(crate) fn current_projection_required_fields(
        &self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> BTreeSet<String> {
        // A maintained version witness identifies the immutable stored version
        // and is normalized back to that canonical history record before it is
        // serialized. It therefore must not widen this *current-query* source
        // to every user column: doing so turns an otherwise unselected enum
        // case into a source-level incompatibility and drops the whole row.
        // The app/query requirement remains the compatibility boundary here;
        // `canonical_history_version_for_maintained_witness` supplies the
        // complete authored record at the wire boundary.
        match &request.requirements.app_fields {
            // `All` still goes through the query-local boundary. The durable
            // all-fields projection predates compatibility-sensitive reads and
            // reports a non-total enum remap as an execution error; a read
            // must instead omit precisely that row before its query graph.
            FieldRequirement::All => table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            FieldRequirement::None => BTreeSet::new(),
            FieldRequirement::Fields(fields) => fields.clone(),
        }
    }

    pub(crate) fn selected_global_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<Option<GraphBuilder>, SourceResolutionError> {
        let Some(access_path) = self.access_paths.get(&request.source).cloned() else {
            return Ok(None);
        };
        match access_path {
            CurrentAccessPath::PrimaryKey(prefix) => {
                self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                Ok(Some(selected_visible_current_primary_key_graph(
                    table, tier, prefix,
                )))
            }
            CurrentAccessPath::Index { column, prefix } => {
                if tier != DurabilityTier::Global {
                    return Ok(None);
                }
                let projection_target = self.current_projection_target(request, table)?;
                let rows = self
                    .node
                    .physical_global_current_source_for_index_scan(
                        table,
                        self.read_view.read_schema,
                        &column,
                        &prefix,
                        &projection_target,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                self.node.query_engine_read_metrics.source_index_probes += 1;
                Ok(Some(rows))
            }
        }
    }

    pub(crate) fn deletion_register_source_for_request(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalTime>,
        open_tx_overlay: Option<OpenTransactionId>,
    ) -> Result<Option<DeletionRegisterSource>, SourceResolutionError> {
        if !request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::DeletionMarkers)
        {
            return Ok(None);
        }
        let Some(tier) = graph_tier else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        if request.visibility != RowVisibility::Visible
            || history_position.is_some()
            || open_tx_overlay.is_some()
        {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }
        if self.needs_projected_current_source(&request.source.table) {
            return Ok(Some(DeletionRegisterSource {
                graph: self.projected_deletion_register_current_source_graph(request, tier)?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        let register_table = self
            .node
            .physical_register_table_for_schema(
                self.node.catalogue.current_schema_version_id,
                &table.name,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        Ok(Some(DeletionRegisterSource {
            graph: deletion_register_current_source_graph(&table.name, &register_table, tier),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    pub(crate) fn content_version_source_for_request(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalTime>,
        open_tx_overlay: Option<OpenTransactionId>,
    ) -> Result<Option<ContentVersionSource>, SourceResolutionError> {
        if !request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionPayloads)
        {
            return Ok(None);
        }
        let Some(tier) = graph_tier else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        if request.visibility != RowVisibility::Visible
            || history_position.is_some()
            || open_tx_overlay.is_some()
        {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }
        if self.needs_projected_current_source(&request.source.table) {
            return Ok(Some(ContentVersionSource {
                graph: self
                    .projected_content_current_source_graph(request, table, tier, false, false)?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        Ok(Some(ContentVersionSource {
            graph: content_version_current_source_graph(table, tier, false),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    pub(crate) fn projected_historical_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        position: GlobalTime,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        if self.can_use_bounded_historical_source(&request.source.table) {
            self.node
                .query_engine_read_metrics
                .source_global_time_range_scans += 1;
            let rows = self
                .node
                .bounded_historical_current_rows(&request.source.table, position)
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            return inline_current_graph(table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut));
        }
        self.node.query_engine_read_metrics.source_full_scans += 1;
        let rows = self
            .node
            .projected_historical_current_rows(
                &request.source.table,
                self.read_view.read_schema,
                position,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
        inline_current_graph(table, rows)
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))
    }

    pub(crate) fn projected_maintained_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        // The schema-aware projection already retains the complete current
        // version witness tuple.  Joining it to the generic physical witness
        // graph would independently decode every enum cell, defeating a
        // title-only old-schema subscription before its narrowed source has a
        // chance to replace unused enum values with typed nulls.
        let projected =
            self.projected_content_current_source_graph(request, table, tier, true, true)?;
        Ok(CurrentSourceGraph {
            graph: projected,
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    pub(crate) fn projected_content_current_source_graph(
        &mut self,
        request: &SourceRequest,
        read_table: &TableSchema,
        tier: DurabilityTier,
        include_global_time: bool,
        exclude_deleted: bool,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let fields = global_current_storage_fields(read_table, true, include_global_time);
        // Global current storage has already selected the physical winner.  Apply
        // the ordinary lens-aware projection directly so added-column defaults
        // survive instead of being replaced with physical nulls by the raw
        // winner projection.  Local and Edge reads still need to choose between
        // Global and Ahead candidates before their compatibility boundary.
        if tier == DurabilityTier::Global {
            let projection_target = self.current_projection_target(request, read_table)?;
            let content = match self.access_paths.get(&request.source).cloned() {
                Some(CurrentAccessPath::PrimaryKey(prefix)) => {
                    self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                    self.node
                        .physical_current_source_scan_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Global,
                            projection_target,
                            static_scan_for_prefix(prefix, 1),
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::SchemaProjection)
                        })?
                }
                Some(CurrentAccessPath::Index { column, prefix }) => {
                    self.node.query_engine_read_metrics.source_index_probes += 1;
                    self.node
                        .physical_global_current_source_for_index_scan(
                            read_table,
                            self.read_view.read_schema,
                            &column,
                            &prefix,
                            &projection_target,
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
                }
                None => {
                    self.node.query_engine_read_metrics.source_full_scans += 1;
                    self.node
                        .physical_current_source_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Global,
                            projection_target,
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::SchemaProjection)
                        })?
                }
            };
            if !exclude_deleted {
                return Ok(content.project(fields));
            }
            let deleted_winners = self
                .projected_deletion_register_current_source_graph(request, tier)?
                .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                .project(["row_uuid"]);
            return Ok(GraphBuilder::anti_join(
                content,
                deleted_winners,
                ["row_uuid"],
                ["row_uuid"],
            ));
        }
        let required_fields = self.current_projection_required_fields(request, read_table);
        let (projection_target, physical_fields) = self
            .node
            .ensure_physical_current_winner_projection(
                self.read_view.read_schema,
                &request.source.table,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let post_winner_fields = self
            .node
            .physical_current_post_winner_projection_fields(
                self.read_view.read_schema,
                &request.source.table,
                &required_fields,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let raw_global_output = self
            .node
            .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
            .and_then(|table_id| {
                self.node
                    .database
                    .table_schema(&physical_global_current_table_name(table_id))
                    .map(|schema| schema.record_schema())
                    .map_err(Error::Groove)
            })
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let access_path = self.access_paths.get(&request.source).cloned();
        let global = match &access_path {
            Some(CurrentAccessPath::PrimaryKey(prefix)) => {
                self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                self.node
                    .physical_current_source_scan_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Global,
                        projection_target.clone(),
                        static_scan_for_prefix(prefix.clone(), 1),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?
            }
            Some(CurrentAccessPath::Index { column, prefix }) if tier == DurabilityTier::Global => {
                // A Global index already selects from the canonical settled
                // winner relation. Project those raw physical rows first, then
                // apply the compatibility boundary below.
                self.node.query_engine_read_metrics.source_index_probes += 1;
                self.node
                    .physical_global_current_source_for_index_scan_with_output(
                        read_table,
                        self.read_view.read_schema,
                        column,
                        prefix,
                        &projection_target,
                        raw_global_output.clone(),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            }
            _ => {
                self.node.query_engine_read_metrics.source_full_scans += 1;
                self.node
                    .physical_current_source_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Global,
                        projection_target.clone(),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?
            }
        };
        let content = if tier == DurabilityTier::Global {
            global
        } else {
            let global = global.project(physical_fields.clone());
            let ahead = match &access_path {
                Some(CurrentAccessPath::PrimaryKey(prefix)) => self
                    .node
                    .physical_current_source_scan_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Ahead,
                        projection_target.clone(),
                        static_scan_for_prefix(prefix.clone(), 3),
                    ),
                _ => self
                    .node
                    .physical_current_source_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Ahead,
                        projection_target,
                    ),
            }
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let ahead = if tier == DurabilityTier::Edge {
                edge_visible_ahead_current_source_graph(ahead, physical_fields.clone())
            } else {
                ahead.project(physical_fields.clone())
            };
            GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(physical_fields)
        };
        let content = content.project_fields(post_winner_fields);
        if !exclude_deleted {
            return Ok(content.project(fields));
        }
        let deleted_winners = self
            .projected_deletion_register_current_source_graph(request, tier)?
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]);
        Ok(GraphBuilder::anti_join(
            content,
            deleted_winners,
            ["row_uuid"],
            ["row_uuid"],
        ))
    }

    pub(crate) fn projected_deletion_register_current_source_graph(
        &mut self,
        request: &SourceRequest,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let table_id = self
            .node
            .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let fields = register_storage_fields_for_query_engine("");
        let global = GraphBuilder::table(physical_register_global_current_table_name(table_id));
        if tier == DurabilityTier::Global {
            return Ok(global);
        }
        let global = global.project_fields(fields.clone());
        let ahead = GraphBuilder::table(physical_register_ahead_current_table_name(table_id));
        let ahead = if tier == DurabilityTier::Edge {
            edge_visible_ahead_current_source_graph(ahead, register_storage_field_names())
        } else {
            ahead.project_fields(fields.clone())
        };
        Ok(GraphBuilder::arg_max_by(
            GraphBuilder::union([global, ahead]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .project_fields(fields))
    }

    pub(crate) fn projected_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        Ok(CurrentSourceGraph {
            // Project heterogeneous physical rows at the source boundary so the
            // rest of the query graph retains the requested logical descriptor.
            graph: self
                .projected_content_current_source_graph(request, table, tier, false, true)?
                .project_fields(storage_to_canonical_current_source_fields(
                    table, true, false,
                )),
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    pub(crate) fn can_use_bounded_historical_source(&self, table: &str) -> bool {
        if self.read_view.read_schema != self.node.catalogue.current_schema_version_id {
            return false;
        }
        self.node
            .physical_table_id_for_schema(self.read_view.read_schema, table)
            .is_ok()
    }

    pub(crate) fn needs_projected_current_source(&mut self, table: &str) -> bool {
        self.node
            .physical_table_id_for_schema(self.read_view.read_schema, table)
            .is_ok()
    }
}

fn deletion_register_current_source_graph(
    table: &str,
    physical_register_table: &str,
    tier: DurabilityTier,
) -> GraphBuilder {
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(register_global_current_table_name(table))
            .project_fields(register_storage_fields_for_query_engine(""));
    }
    let current_keys = deletion_register_current_keys_graph(table, tier);
    GraphBuilder::join(
        GraphBuilder::table(physical_register_table),
        current_keys,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(register_storage_fields_for_query_engine("left."))
}

pub(super) fn edge_visible_ahead_current_source_graph(
    source: GraphBuilder,
    fields: Vec<String>,
) -> GraphBuilder {
    GraphBuilder::join(
        source.project(fields.clone()),
        GraphBuilder::table("jazz_transactions")
            .filter(
                PredicateExpr::And(vec![
                    PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
                ])
                .canonicalize(),
            )
            .project(["time", "node_id"]),
        ["tx_time", "tx_node_id"],
        ["time", "node_id"],
    )
    .project_fields(
        fields
            .into_iter()
            .map(|field| ProjectField::renamed(left_field(&field), field)),
    )
}

/// Select branch overlay history by the same fate/durability contract as a
/// root current read before choosing the row winner. Branch partitions retain
/// raw history after rejection, so winner selection over the unfiltered table
/// would expose pending rows at Edge/Global and let a rejected latest version
/// continue masking an earlier accepted winner.
#[allow(dead_code)]
fn tier_visible_branch_history_graph(
    source: GraphBuilder,
    fields: Vec<String>,
    tier: DurabilityTier,
) -> GraphBuilder {
    let eligible = match tier {
        DurabilityTier::None | DurabilityTier::Local => PredicateExpr::Or(vec![
            PredicateExpr::eq("fate", Value::EnumTag(FateTag::Pending as u8)),
            PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
        ])
        .canonicalize(),
        DurabilityTier::Edge => PredicateExpr::And(vec![
            PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
            PredicateExpr::Or(vec![
                PredicateExpr::eq("durability", Value::EnumTag(2)),
                PredicateExpr::eq("durability", Value::EnumTag(3)),
            ])
            .canonicalize(),
        ])
        .canonicalize(),
        DurabilityTier::Global => PredicateExpr::And(vec![
            PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
            PredicateExpr::eq("durability", Value::EnumTag(3)),
        ])
        .canonicalize(),
    };
    GraphBuilder::join(
        source.project(fields.clone()),
        GraphBuilder::table("jazz_transactions")
            .filter(eligible)
            .project(["time", "node_id"]),
        ["tx_time", "tx_node_id"],
        ["time", "node_id"],
    )
    .project_fields(
        fields
            .into_iter()
            .map(|field| ProjectField::renamed(left_field(&field), field)),
    )
}

fn content_version_current_source_graph(
    table: &TableSchema,
    tier: DurabilityTier,
    include_global_time: bool,
) -> GraphBuilder {
    let mut fields = maintained_view_history_storage_field_names(table);
    if include_global_time {
        fields.push("global_time".to_owned());
    }
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(global_current_table_name(&table.name)).project(fields);
    }
    let ahead = if tier == DurabilityTier::Edge {
        GraphBuilder::join(
            GraphBuilder::table(ahead_current_table_name(&table.name)).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .iter()
                .cloned()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    } else {
        GraphBuilder::table(ahead_current_table_name(&table.name)).project(fields.clone())
    };
    GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table(global_current_table_name(&table.name)).project(fields.clone()),
            ahead,
        ]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    )
    .project(fields)
}

fn deletion_register_current_keys_graph(table: &str, tier: DurabilityTier) -> GraphBuilder {
    let key_fields = ["row_uuid", "tx_time", "tx_node_id"];
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(register_global_current_table_name(table)).project(key_fields);
    }
    let ahead = if tier == DurabilityTier::Edge {
        GraphBuilder::join(
            GraphBuilder::table(register_ahead_current_table_name(table)).project(key_fields),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            key_fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    } else {
        GraphBuilder::table(register_ahead_current_table_name(table)).project(key_fields)
    };
    GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table(register_global_current_table_name(table)).project(key_fields),
            ahead,
        ]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    )
    .project(key_fields)
}

fn selected_visible_current_primary_key_graph(
    table: &TableSchema,
    tier: DurabilityTier,
    prefix: Vec<Value>,
) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| user_column_field(&column.name))
        .collect::<Vec<_>>();
    let mut content_fields = vec![
        "row_uuid".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "authored_columns".to_owned(),
    ];
    content_fields.extend(user_fields.iter().cloned());
    content_fields.extend([
        "created_by".to_owned(),
        "created_at".to_owned(),
        "updated_by".to_owned(),
        "updated_at".to_owned(),
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
    ]);
    let content_scan = static_scan_for_prefix(prefix.clone(), 1);
    let deletion_scan = static_scan_for_prefix(prefix, 1);
    let edge_visible_ahead = |table_name: String, fields: Vec<String>, scan: StaticScanSpec| {
        GraphBuilder::join(
            GraphBuilder::table_scan(table_name, scan).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::And(vec![
                        PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                        PredicateExpr::Or(vec![
                            PredicateExpr::eq("durability", Value::EnumTag(2)),
                            PredicateExpr::eq("durability", Value::EnumTag(3)),
                        ])
                        .canonicalize(),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    };
    let (content_current, deleted_winners) = if tier == DurabilityTier::Global {
        (
            GraphBuilder::table_scan(global_current_table_name(&table.name), content_scan)
                .project(content_fields.clone()),
            GraphBuilder::table_scan(
                register_global_current_table_name(&table.name),
                deletion_scan,
            )
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]),
        )
    } else {
        let ahead_content = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_fields.clone(),
                content_scan.clone(),
            )
        } else {
            GraphBuilder::table_scan(ahead_current_table_name(&table.name), content_scan.clone())
                .project(content_fields.clone())
        };
        let deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
            "created_by".to_owned(),
            "created_at".to_owned(),
            "updated_by".to_owned(),
            "updated_at".to_owned(),
            "_deletion".to_owned(),
        ];
        let ahead_deleted = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                deletion_fields.clone(),
                deletion_scan.clone(),
            )
        } else {
            GraphBuilder::table_scan(
                register_ahead_current_table_name(&table.name),
                deletion_scan.clone(),
            )
            .project(deletion_fields.clone())
        };
        (
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table_scan(global_current_table_name(&table.name), content_scan)
                        .project(content_fields.clone()),
                    ahead_content,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(content_fields.clone()),
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table_scan(
                        register_global_current_table_name(&table.name),
                        deletion_scan,
                    )
                    .project(deletion_fields),
                    ahead_deleted,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]),
        )
    };
    GraphBuilder::anti_join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project(content_fields)
}

pub(super) fn register_storage_fields_for_query_engine(prefix: &str) -> Vec<ProjectField> {
    register_storage_field_names()
        .into_iter()
        .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field))
        .collect()
}

pub(super) fn register_storage_field_names() -> Vec<String> {
    [
        "row_uuid",
        "tx_time",
        "tx_node_id",
        "schema_version",
        "parents",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
        "_deletion",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn source_resolution_error(request: &SourceRequest, gap: SourceGap) -> SourceResolutionError {
    SourceResolutionError {
        request: Box::new(request.clone()),
        gap,
    }
}

fn source_resolution_error_from_policy_proof(
    request: &SourceRequest,
    error: Error,
) -> SourceResolutionError {
    match error {
        Error::PolicyProofCycle { table, depth } => {
            source_resolution_error(request, SourceGap::PolicyProofCycle { table, depth })
        }
        Error::QueryCapability(message) => {
            let Some((table, depth)) = policy_proof_cycle_from_capability(&message) else {
                return source_resolution_error(request, SourceGap::Coverage);
            };
            source_resolution_error(request, SourceGap::PolicyProofCycle { table, depth })
        }
        _ => source_resolution_error(request, SourceGap::Coverage),
    }
}

fn policy_proof_cycle_from_capability(message: &str) -> Option<(String, usize)> {
    let (_, suffix) = message.rsplit_once("PolicyProofCycle { table: \"")?;
    let (table, suffix) = suffix.split_once('"')?;
    let (_, suffix) = suffix.split_once(", depth: ")?;
    let depth = suffix
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Some((table.to_owned(), depth))
}

pub(super) fn capability_trace_enabled() -> bool {
    std::env::var_os("JAZZ_CAPABILITY_TRACE").is_some()
        || std::env::var_os("JAZZ_CAPABILITY_TRACE_FILE").is_some()
}

pub(super) fn trace_capability_compile(
    node_uuid: NodeUuid,
    node_alias: Option<NodeAlias>,
    request: &QueryProgramRequest,
    result: Result<&QueryProgram, &CapabilityReport>,
) {
    let Some(path) = std::env::var_os("JAZZ_CAPABILITY_TRACE_FILE") else {
        if std::env::var_os("JAZZ_CAPABILITY_TRACE").is_some() {
            eprintln!(
                "JAZZ_CAPABILITY_TRACE set without JAZZ_CAPABILITY_TRACE_FILE; capability trace skipped"
            );
        }
        return;
    };
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(err) => {
            eprintln!(
                "failed to open JAZZ_CAPABILITY_TRACE_FILE {:?}: {err}",
                path
            );
            return;
        }
    };
    let event = match result {
        Ok(_) => "compile_success",
        Err(_) => "compile_failure",
    };
    let _ = writeln!(
        file,
        "\n=== JAZZ_CAPABILITY_TRACE {event} pid={} node_uuid={:?} node_alias={:?} ===",
        std::process::id(),
        node_uuid,
        node_alias
    );
    let _ = writeln!(file, "request:\n{request:#?}");
    match result {
        Ok(program) => {
            let _ = writeln!(file, "explain:\n{:#?}", program.explain);
            let _ = writeln!(file, "output:\n{:#?}", program.lowered.output);
        }
        Err(report) => {
            let _ = writeln!(file, "report:\n{report:#?}");
        }
    }
    let _ = writeln!(
        file,
        "backtrace:\n{}",
        std::backtrace::Backtrace::force_capture()
    );
}

fn resolved_current_source_graph<S>(
    node: &mut NodeState<S>,
    table: &TableSchema,
    tier: DurabilityTier,
    requirements: &SourceRequirements,
    authorization: &SourceAuthorizationRequest,
    policy_schema: SchemaVersionId,
    selected_base: Option<GraphBuilder>,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
        BTreeSet<String>,
    ),
    Error,
>
where
    S: OrderedKvStorage,
{
    let mut fields = current_row_fields(table)
        .into_iter()
        .map(ProjectField::named)
        .collect::<Vec<_>>();
    let mut metadata = BTreeMap::new();
    let needs_version_witnesses = requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
        || requirements
            .metadata
            .iter()
            .any(|requirement| matches!(requirement, SourceMetadataRequirement::Provenance(_)));
    let needs_settle_position = requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition);

    if needs_version_witnesses {
        fields.extend([
            ProjectField::literal("table", Value::String(table.name.clone())),
            ProjectField::literal("layer", Value::String("content".to_owned())),
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
            ProjectField::renamed("$createdBy", "created_by"),
            ProjectField::renamed("$createdAt", "created_at"),
            ProjectField::renamed("$updatedBy", "updated_by"),
            ProjectField::renamed("$updatedAt", "updated_at"),
        ]);
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: None,
            },
        );
    }
    if needs_settle_position {
        fields.push(ProjectField::named("settle_position"));
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        fields.push(ProjectField::literal(
            "coverage",
            Value::String("visible-current".to_owned()),
        ));
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    // Every resolver branch normalizes storage names before it
                    // exposes this source. Metadata therefore names the
                    // canonical field on the resolved graph, never the
                    // physical storage column used below that boundary.
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }

    let descriptor = current_row_descriptor_with_hidden_source_fields(table, &metadata);
    let (base, routing_fields) = match authorization {
        SourceAuthorizationRequest::System => {
            let graph = if let Some(selected_base) = selected_base.clone() {
                selected_base.project_fields(storage_to_canonical_current_source_fields(
                    table,
                    needs_version_witnesses,
                    needs_settle_position,
                ))
            } else if needs_version_witnesses {
                node.maintained_view_content_current_with_version(table, tier)?
                    .project_fields(storage_to_canonical_current_source_fields(
                        table,
                        true,
                        needs_settle_position,
                    ))
            } else {
                visible_current_graph(table, tier)
                    .project_fields(canonical_current_source_fields(table, false))
            };
            (graph, BTreeSet::new())
        }
        SourceAuthorizationRequest::PolicyFiltered {
            permission_subject,
            plan,
        }
        | SourceAuthorizationRequest::PolicyProof {
            permission_subject,
            plan,
        } => {
            if plan.protected_source.table != table.name
                || plan.role != PolicyDecisionRole::Read
                || plan.protected_row_field != "row_uuid"
            {
                return Err(Error::QueryCapability(
                    "policy authorization plan does not match resolved source".to_owned(),
                ));
            }
            let binding_source_shape = plan.binding_source_shape.clone();
            let binding_user_params = plan.binding_user_params.clone();
            let binding_claim_params = plan.binding_claim_params.clone();
            let param_binding_mode = if binding_source_shape.is_some() {
                ParamBindingMode::RetainAllParams
            } else {
                ParamBindingMode::InlineAllReachableSeeds
            };
            let policy_request = node.table_read_policy_authorization_request(
                policy_schema,
                &table.name,
                *permission_subject,
                param_binding_mode,
                tier,
                binding_source_shape.clone(),
                binding_user_params.clone(),
                binding_claim_params,
            );
            let output_fields = global_current_storage_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            let base = match selected_base {
                Some(selected_base) => selected_base,
                None => node.maintained_view_content_current_with_version(table, tier)?,
            };
            let storage_graph = node.policy_filtered_current_source_graph_via_query_engine(
                policy_request,
                base.clone(),
                &output_fields,
            )?;
            let mut canonical_fields = storage_to_canonical_current_source_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            canonical_fields.extend(
                storage_graph
                    .route_fields
                    .iter()
                    .map(|field| ProjectField::named(field.clone())),
            );
            (
                storage_graph.graph.project_fields(canonical_fields),
                storage_graph.route_fields,
            )
        }
    };
    fields.extend(
        routing_fields
            .iter()
            .map(|field| ProjectField::named(field.clone())),
    );
    let graph = if metadata.is_empty() {
        base
    } else {
        base.project_fields(fields)
    };
    Ok((graph, descriptor, metadata, routing_fields))
}

fn canonical_current_source_fields(
    table: &TableSchema,
    include_version: bool,
) -> Vec<ProjectField> {
    let mut fields = std::iter::once(ProjectField::named("row_uuid"))
        .chain(
            table
                .columns
                .iter()
                .map(|column| ProjectField::named(user_column_field(&column.name))),
        )
        .chain([
            ProjectField::named("$createdBy"),
            ProjectField::named("$createdAt"),
            ProjectField::named("$updatedBy"),
            ProjectField::named("$updatedAt"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
        ])
        .collect::<Vec<_>>();
    if include_version {
        fields.extend([
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
        ]);
    }
    fields
}

fn source_provenance_field(field: ProvenanceField) -> &'static str {
    match field {
        ProvenanceField::CreatedAt => "$createdAt",
        ProvenanceField::CreatedBy => "$createdBy",
        ProvenanceField::UpdatedAt => "$updatedAt",
        ProvenanceField::UpdatedBy => "$updatedBy",
    }
}

fn storage_to_canonical_current_source_fields(
    table: &TableSchema,
    include_version: bool,
    include_settle_position: bool,
) -> Vec<ProjectField> {
    let mut fields = std::iter::once(ProjectField::named("row_uuid"))
        .chain(
            table
                .columns
                .iter()
                .map(|column| ProjectField::named(user_column_field(&column.name))),
        )
        .chain([
            ProjectField::renamed("created_by", "$createdBy"),
            ProjectField::renamed("created_at", "$createdAt"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
        ])
        .collect::<Vec<_>>();
    if include_version {
        fields.extend([
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
        ]);
    }
    if include_settle_position {
        fields.push(ProjectField::renamed("global_time", "settle_position"));
    }
    fields
}

pub(super) fn current_row_descriptor_with_hidden_source_fields(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
) -> RecordDescriptor {
    let mut fields = current_row_descriptor_fields(table);
    if metadata.contains_key(&SourceMetadataRequirement::VersionWitnesses) {
        fields.extend([
            ("table".to_owned(), ValueType::String),
            ("layer".to_owned(), ValueType::String),
            ("schema_version".to_owned(), ValueType::U64),
            (
                "parents".to_owned(),
                ValueType::Array(Box::new(ValueType::Tuple(vec![
                    ValueType::U64,
                    ValueType::Uuid,
                ]))),
            ),
            ("created_by".to_owned(), ValueType::Uuid),
            ("created_at".to_owned(), ValueType::U64),
            ("updated_by".to_owned(), ValueType::Uuid),
            ("updated_at".to_owned(), ValueType::U64),
            (
                "authored_columns".to_owned(),
                ValueType::Nullable(Box::new(ValueType::Bytes)),
            ),
        ]);
        if let Some(SourceMetadataFields::VersionWitnesses {
            branch_or_prefix_field: Some(field),
            ..
        }) = metadata.get(&SourceMetadataRequirement::VersionWitnesses)
        {
            fields.push((
                field.clone(),
                ValueType::Nullable(Box::new(ValueType::Uuid)),
            ));
        }
    }
    if metadata.contains_key(&SourceMetadataRequirement::Coverage) {
        fields.push(("coverage".to_owned(), ValueType::String));
    }
    if metadata.contains_key(&SourceMetadataRequirement::SettlePosition) {
        fields.push((
            "settle_position".to_owned(),
            ValueType::Nullable(Box::new(ValueType::U64)),
        ));
    }
    RecordDescriptor::new(fields)
}

fn current_row_descriptor_fields(table: &TableSchema) -> Vec<(String, ValueType)> {
    std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
        .chain(table.columns.iter().map(|column| {
            (
                user_column_field(&column.name),
                ValueType::Nullable(Box::new(column.column_type.clone())),
            )
        }))
        .chain([
            ("$createdBy".to_owned(), ValueType::Uuid),
            ("$createdAt".to_owned(), ValueType::U64),
            ("$updatedBy".to_owned(), ValueType::Uuid),
            ("$updatedAt".to_owned(), ValueType::U64),
            ("tx_time".to_owned(), ValueType::U64),
            ("tx_node_id".to_owned(), ValueType::U64),
        ])
        .collect()
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn one_shot_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        self.current_query_access_paths(shape, binding, tier)
    }

    fn current_query_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        if tier != DurabilityTier::Global {
            return Ok(BTreeMap::new());
        }
        let query = shape.query();
        if !query.joins.is_empty()
            || !query.policy_branches.is_empty()
            || !query.array_subqueries.is_empty()
            || query.aggregate.is_some()
        {
            return Ok(BTreeMap::new());
        }
        let mut access_paths = self.current_query_primary_key_access_paths(shape, binding)?;
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        let equalities = root_literal_equalities(query, binding)?;
        let Some(access_path) = select_current_access_path(&table, &equalities) else {
            return Ok(access_paths);
        };
        access_paths.insert(root_source_id(&query.table), access_path);
        self.add_reachable_access_paths(query, shape.schema_version(), binding, &mut access_paths)?;
        Ok(access_paths)
    }

    pub(super) fn current_query_primary_key_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        let query = shape.query();
        let mut access_paths = BTreeMap::new();
        let equalities = root_literal_equalities(query, binding)?;
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        let has_declared_id = table.columns.iter().any(|column| column.name == "id");
        if !has_declared_id && let Some(value) = equalities.get("id").cloned() {
            access_paths.insert(
                root_source_id(&query.table),
                CurrentAccessPath::PrimaryKey(vec![value]),
            );
        }
        self.add_reachable_access_paths(query, shape.schema_version(), binding, &mut access_paths)?;
        Ok(access_paths)
    }

    fn add_reachable_access_paths(
        &self,
        query: &JazzQuery,
        schema_version: SchemaVersionId,
        binding: &Binding,
        access_paths: &mut BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<(), Error> {
        for (index, reachable) in query.reachable.iter().enumerate() {
            let reachable_id = format!("reachable:{index}");
            if let Some(seed) = &reachable.seed {
                let source = reachable_seed_source_id(seed, &reachable_id);
                self.add_primary_key_access_path_for_filters(
                    &source,
                    &seed.table,
                    schema_version,
                    &seed.filters,
                    binding,
                    access_paths,
                )?;
            }
            let edge_source = reachable_edge_source_id(reachable, &reachable_id);
            self.add_primary_key_access_path_for_filters(
                &edge_source,
                &reachable.edge_table,
                schema_version,
                &reachable.edge_filters,
                binding,
                access_paths,
            )?;
            let access_source = reachable_access_source_id(reachable, &reachable_id);
            self.add_primary_key_access_path_for_filters(
                &access_source,
                &reachable.access_table,
                schema_version,
                &reachable.access_filters,
                binding,
                access_paths,
            )?;
        }
        Ok(())
    }

    fn add_primary_key_access_path_for_filters(
        &self,
        source: &SourceId,
        table_name: &str,
        schema_version: SchemaVersionId,
        filters: &[Predicate],
        binding: &Binding,
        access_paths: &mut BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<(), Error> {
        let table = self.table_in_schema(table_name, schema_version)?;
        let equalities = literal_equalities_for_filters(filters, binding)?;
        if let Some(access_path) = select_current_access_path(&table, &equalities)
            && matches!(access_path, CurrentAccessPath::PrimaryKey(_))
        {
            access_paths.insert(source.clone(), access_path);
        }
        Ok(())
    }

    fn physical_global_current_source_for_index_scan(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &str,
        prefix: &[Value],
        projection_target: &str,
    ) -> Result<GraphBuilder, Error> {
        self.physical_global_current_source_for_index_scan_with_output(
            table,
            schema_version,
            column,
            prefix,
            projection_target,
            table.global_current_storage_tables()[0].record_schema(),
        )
    }

    fn physical_global_current_source_for_index_scan_with_output(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &str,
        prefix: &[Value],
        projection_target: &str,
        output: RecordDescriptor,
    ) -> Result<GraphBuilder, Error> {
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(&table.name))
            .ok_or(Error::InvalidStoredValue(
                "physical current index table mapping missing",
            ))?;
        let column_id = mapping
            .columns
            .get(column)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current index column mapping missing",
            ))?;
        let storage_table = physical_global_current_table_name(mapping.table_id);
        let indexed = self.database.index_scan_raw(
            &storage_table,
            &physical_current_index_name(column_id),
            prefix,
        )?;
        let mut records = Vec::with_capacity(indexed.len());
        for raw in indexed {
            let variant_tag = raw.variant_tag();
            let record = groove::records::VariantRecord::new(variant_tag, raw.owned_record());
            if let Some(projected) =
                self.database
                    .project_variant_record(&storage_table, projection_target, &record)?
            {
                records.push(projected.raw().to_vec());
            }
        }
        Ok(GraphBuilder::inline_records(output, records))
    }
}

pub(super) fn current_row_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("$createdBy".to_owned());
    fields.push("$createdAt".to_owned());
    fields.push("$updatedBy".to_owned());
    fields.push("$updatedAt".to_owned());
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    fields
}

pub(super) fn global_current_storage_fields(
    table: &TableSchema,
    include_version: bool,
    include_settle_position: bool,
) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    if include_version {
        fields.extend([
            "schema_version".to_owned(),
            "parents".to_owned(),
            "authored_columns".to_owned(),
        ]);
    }
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("created_by".to_owned());
    fields.push("created_at".to_owned());
    fields.push("updated_by".to_owned());
    fields.push("updated_at".to_owned());
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    if include_settle_position {
        fields.push("global_time".to_owned());
    }
    fields
}

fn current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    user_column_field(&column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone())),
                )
            }))
            .chain([
                ("$createdBy".to_owned(), ValueType::Uuid),
                ("$createdAt".to_owned(), ValueType::U64),
                ("$updatedBy".to_owned(), ValueType::Uuid),
                ("$updatedAt".to_owned(), ValueType::U64),
                ("tx_time".to_owned(), ValueType::U64),
                ("tx_node_id".to_owned(), ValueType::U64),
            ]),
    )
}

pub(super) fn empty_authorized_row_id_graph() -> GraphBuilder {
    GraphBuilder::inline_records(
        RecordDescriptor::new([("row_uuid", ValueType::Uuid)]),
        Vec::<Vec<u8>>::new(),
    )
}

fn inline_current_record(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::with_capacity(table.columns.len() + 7);
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
    }
    if let Some(provenance) = row.provenance()? {
        values.push(Value::Uuid(provenance.created_by.0));
        values.push(Value::U64(provenance.created_at.0));
        values.push(Value::Uuid(provenance.updated_by.0));
        values.push(Value::U64(provenance.updated_at.0));
    } else {
        values.push(Value::Uuid(AuthorId::SYSTEM.0));
        values.push(Value::U64(0));
        values.push(Value::Uuid(AuthorId::SYSTEM.0));
        values.push(Value::U64(0));
    }
    let (tx_time, tx_node_alias) = row
        .projected_tx_alias()
        .unwrap_or((TxTime(0), NodeAlias(0)));
    values.push(Value::U64(tx_time.0));
    values.push(Value::U64(tx_node_alias.0));
    Ok(descriptor.create(&values)?)
}

fn inline_current_graph(table: &TableSchema, rows: Vec<CurrentRow>) -> Result<GraphBuilder, Error> {
    let descriptor = current_row_descriptor(table);
    let records = rows
        .iter()
        .map(|row| inline_current_record(table, &descriptor, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(GraphBuilder::inline_records(descriptor, records))
}

fn inline_current_graph_with_source_metadata(
    table: &TableSchema,
    rows: Vec<CurrentRow>,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let mut metadata = BTreeMap::new();
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
    {
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: None,
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition)
    {
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }

    let descriptor = current_row_descriptor_with_hidden_source_fields(table, &metadata);
    let records = rows
        .iter()
        .map(|row| {
            inline_current_record_with_source_metadata(
                table,
                &descriptor,
                row,
                schema_version_alias,
                coverage,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        GraphBuilder::inline_records(descriptor.clone(), records),
        descriptor,
        metadata,
    ))
}

fn inline_current_record_with_source_metadata(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::new();
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
    }
    let provenance = row.provenance()?.unwrap_or(RowProvenance {
        created_by: AuthorId::SYSTEM,
        created_at: TxTime(0),
        updated_by: AuthorId::SYSTEM,
        updated_at: TxTime(0),
    });
    values.extend([
        Value::Uuid(provenance.created_by.0),
        Value::U64(provenance.created_at.0),
        Value::Uuid(provenance.updated_by.0),
        Value::U64(provenance.updated_at.0),
    ]);
    let (tx_time, tx_node_alias) = row
        .projected_tx_alias()
        .unwrap_or((TxTime(0), NodeAlias(0)));
    values.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
    if descriptor.field_index("table").is_some() {
        values.extend([
            Value::String(table.name.clone()),
            Value::String("content".to_owned()),
            Value::U64(schema_version_alias.0),
            Value::Array(Vec::new()),
            Value::Uuid(provenance.created_by.0),
            Value::U64(provenance.created_at.0),
            Value::Uuid(provenance.updated_by.0),
            Value::U64(provenance.updated_at.0),
            Value::Nullable(None),
        ]);
    }
    if descriptor.field_index("coverage").is_some() {
        values.push(Value::String(coverage.to_owned()));
    }
    if descriptor.field_index("settle_position").is_some() {
        values.push(Value::Nullable(None));
    }
    Ok(descriptor.create(&values)?)
}

fn inline_include_deleted_current_graph(
    table: &TableSchema,
    rows: Vec<(CurrentRow, bool)>,
) -> Result<GraphBuilder, Error> {
    let descriptor = include_deleted_current_row_descriptor(table);
    let mut records = Vec::with_capacity(rows.len());
    for (row, deleted) in rows {
        let mut values = Vec::with_capacity(table.columns.len() + 8);
        values.push(Value::Uuid(row.row_uuid().0));
        for column in &table.columns {
            values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
        }
        if let Some(provenance) = row.provenance()? {
            values.push(Value::Uuid(provenance.created_by.0));
            values.push(Value::U64(provenance.created_at.0));
            values.push(Value::Uuid(provenance.updated_by.0));
            values.push(Value::U64(provenance.updated_at.0));
        } else {
            values.push(Value::Uuid(AuthorId::SYSTEM.0));
            values.push(Value::U64(0));
            values.push(Value::Uuid(AuthorId::SYSTEM.0));
            values.push(Value::U64(0));
        }
        let (tx_time, tx_node_alias) = row
            .projected_tx_alias()
            .unwrap_or((TxTime(0), NodeAlias(0)));
        values.push(Value::U64(tx_time.0));
        values.push(Value::U64(tx_node_alias.0));
        values.push(Value::Bool(deleted));
        records.push(descriptor.create(&values)?);
    }
    Ok(GraphBuilder::inline_records(descriptor, records))
}

#[allow(dead_code)]
fn inline_branch_current_graph(
    table: &TableSchema,
    rows: Vec<(CurrentRow, TxTime, NodeAlias, Option<BranchId>)>,
    schema_version_alias: SchemaVersionAlias,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let mut metadata = BTreeMap::new();
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
    {
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: Some("branch_id".to_owned()),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition)
    {
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }
    let descriptor = current_row_descriptor_with_hidden_source_fields(table, &metadata);
    let records = rows
        .iter()
        .map(|(row, tx_time, tx_node, branch)| {
            inline_branch_current_record(
                table,
                &descriptor,
                row,
                schema_version_alias,
                (*tx_time, *tx_node),
                *branch,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        GraphBuilder::inline_records(descriptor.clone(), records),
        descriptor,
        metadata,
    ))
}

#[allow(dead_code)]
pub(super) fn inline_branch_current_record(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
    schema_version_alias: SchemaVersionAlias,
    witness: (TxTime, NodeAlias),
    branch_id: Option<BranchId>,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::new();
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
    }
    let provenance = row.provenance()?.unwrap_or(RowProvenance {
        created_by: AuthorId::SYSTEM,
        created_at: TxTime(0),
        updated_by: AuthorId::SYSTEM,
        updated_at: TxTime(0),
    });
    values.extend([
        Value::Uuid(provenance.created_by.0),
        Value::U64(provenance.created_at.0),
        Value::Uuid(provenance.updated_by.0),
        Value::U64(provenance.updated_at.0),
    ]);
    let (tx_time, tx_node_alias) = witness;
    values.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
    if descriptor.field_index("table").is_some() {
        values.extend([
            Value::String(table.name.clone()),
            Value::String("content".to_owned()),
            Value::U64(schema_version_alias.0),
            Value::Array(Vec::new()),
            Value::Uuid(provenance.created_by.0),
            Value::U64(provenance.created_at.0),
            Value::Uuid(provenance.updated_by.0),
            Value::U64(provenance.updated_at.0),
            Value::Nullable(None),
        ]);
        if descriptor.field_index("branch_id").is_some() {
            values.push(Value::Nullable(
                branch_id.map(|branch| Box::new(Value::Uuid(branch.0))),
            ));
        }
    }
    if descriptor.field_index("coverage").is_some() {
        values.push(Value::String("branch-current".to_owned()));
    }
    if descriptor.field_index("settle_position").is_some() {
        values.push(Value::Nullable(None));
    }
    Ok(descriptor.create(&values)?)
}

#[cfg(test)]
pub(super) fn historical_current_graph_full_scan(
    table: &TableSchema,
    table_id: PhysicalTableId,
    position: GlobalTime,
    history_rows: GraphBuilder,
) -> GraphBuilder {
    let cut_predicate = PredicateExpr::And(vec![
        PredicateExpr::eq("physical_table_id", Value::U64(table_id.0)),
        PredicateExpr::LtEq {
            field: "global_time".to_owned(),
            value: Value::U64(position.0).into(),
        },
    ])
    .canonicalize();
    let changes_for_layer = |layer: &'static str| {
        GraphBuilder::table("jazz_global_changes").filter(
            PredicateExpr::And(vec![
                cut_predicate.clone(),
                PredicateExpr::eq("layer", Value::Bytes(layer.as_bytes().to_vec())),
            ])
            .canonicalize(),
        )
    };
    let nullable_deletion_type = ValueType::Nullable(Box::new(ValueType::EnumTag(
        groove::records::ScalarEnumSchema::new("jazz_deletion", ["deleted", "restored"])
            .expect("valid deletion enum")
            .with_system_registry(groove::records::SystemVariantRegistry::deletion_state()),
    )));
    let content_events = changes_for_layer("content").project_fields([
        ProjectField::named("row_uuid"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::literal("event_layer", Value::String("content".to_owned())),
        ProjectField::null_typed("deletion", nullable_deletion_type.clone()),
    ]);
    let register_events = changes_for_layer("deletion").project_fields([
        ProjectField::named("row_uuid"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::literal("event_layer", Value::String("deletion".to_owned())),
        ProjectField::renamed("_deletion", "deletion"),
    ]);
    let latest_event = GraphBuilder::arg_max_by(
        GraphBuilder::union([content_events.clone(), register_events]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    );
    let content_winners =
        GraphBuilder::arg_max_by(content_events, ["row_uuid"], ["tx_time", "tx_node_id"]);
    let history_rows = history_rows.project(maintained_view_history_storage_field_names(table));
    let content_current = GraphBuilder::join(
        history_rows,
        content_winners,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(
        ["row_uuid".to_owned()]
            .into_iter()
            .chain(
                table
                    .columns
                    .iter()
                    .map(|column| user_column_field(&column.name)),
            )
            .map(|field| ProjectField::renamed(left_field(&field), field))
            .chain([
                ProjectField::renamed("left.created_by", "$createdBy"),
                ProjectField::renamed("left.created_at", "$createdAt"),
                ProjectField::renamed("left.updated_by", "$updatedBy"),
                ProjectField::renamed("left.updated_at", "$updatedAt"),
                ProjectField::renamed("left.tx_time", "tx_time"),
                ProjectField::renamed("left.tx_node_id", "tx_node_id"),
            ]),
    );
    let latest_content = latest_event.clone().filter(PredicateExpr::eq(
        "event_layer",
        Value::String("content".to_owned()),
    ));
    let content_is_latest = GraphBuilder::join(
        content_current.clone(),
        latest_content,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(
        current_row_fields(table)
            .into_iter()
            .map(|field| ProjectField::renamed(left_field(&field), field)),
    );
    let latest_restore = latest_event.filter(
        PredicateExpr::And(vec![
            PredicateExpr::eq("event_layer", Value::String("deletion".to_owned())),
            PredicateExpr::eq(
                "deletion",
                Value::Nullable(Some(Box::new(Value::EnumTag(1)))),
            ),
        ])
        .canonicalize(),
    );
    let restored_content =
        GraphBuilder::join(content_current, latest_restore, ["row_uuid"], ["row_uuid"])
            .project_fields(
                current_row_fields(table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(left_field(&field), field)),
            );
    GraphBuilder::union([content_is_latest, restored_content])
}

fn include_deleted_current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    user_column_field(&column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone())),
                )
            }))
            .chain([
                ("$createdBy".to_owned(), ValueType::Uuid),
                ("$createdAt".to_owned(), ValueType::U64),
                ("$updatedBy".to_owned(), ValueType::Uuid),
                ("$updatedAt".to_owned(), ValueType::U64),
                ("tx_time".to_owned(), ValueType::U64),
                ("tx_node_id".to_owned(), ValueType::U64),
            ])
            .chain([("__jazz_deleted".to_owned(), ValueType::Bool)]),
    )
}

fn include_deleted_current_graph(table: &TableSchema, tier: DurabilityTier) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| user_column_field(&column.name))
        .collect::<Vec<_>>();
    let mut content_storage_fields = vec![
        "row_uuid".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "authored_columns".to_owned(),
    ];
    content_storage_fields.extend(user_fields.iter().cloned());
    content_storage_fields.push("created_by".to_owned());
    content_storage_fields.push("created_at".to_owned());
    content_storage_fields.push("updated_by".to_owned());
    content_storage_fields.push("updated_at".to_owned());
    content_storage_fields.push("tx_time".to_owned());
    content_storage_fields.push("tx_node_id".to_owned());
    let normalize_content_fields = |graph: GraphBuilder| {
        graph.project_fields(
            ["row_uuid".to_owned()]
                .into_iter()
                .chain(user_fields.iter().cloned())
                .map(ProjectField::named)
                .chain([
                    ProjectField::renamed("created_by", "$createdBy"),
                    ProjectField::renamed("created_at", "$createdAt"),
                    ProjectField::renamed("updated_by", "$updatedBy"),
                    ProjectField::renamed("updated_at", "$updatedAt"),
                    ProjectField::named("tx_time"),
                    ProjectField::named("tx_node_id"),
                ]),
        )
    };
    let edge_visible_ahead = |table_name: String, fields: Vec<String>| {
        GraphBuilder::join(
            GraphBuilder::table(table_name).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    };
    let (content_current, deletion_current) = if tier == DurabilityTier::Global {
        (
            normalize_content_fields(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(content_storage_fields.clone()),
            ),
            GraphBuilder::table(register_global_current_table_name(&table.name)),
        )
    } else {
        let ahead_content = if tier == DurabilityTier::Edge {
            normalize_content_fields(edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_storage_fields.clone(),
            ))
        } else {
            normalize_content_fields(
                GraphBuilder::table(ahead_current_table_name(&table.name))
                    .project(content_storage_fields.clone()),
            )
        };
        let deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
            "created_by".to_owned(),
            "created_at".to_owned(),
            "updated_by".to_owned(),
            "updated_at".to_owned(),
            "_deletion".to_owned(),
        ];
        let ahead_deletion = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                deletion_fields.clone(),
            )
        } else {
            GraphBuilder::table(register_ahead_current_table_name(&table.name))
                .project(deletion_fields.clone())
        };
        (
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    normalize_content_fields(
                        GraphBuilder::table(global_current_table_name(&table.name))
                            .project(content_storage_fields.clone()),
                    ),
                    ahead_content,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(current_row_fields(table)),
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table(register_global_current_table_name(&table.name))
                        .project(deletion_fields),
                    ahead_deletion,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ),
        )
    };
    let deleted_winners = deletion_current
        .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
        .project_fields([
            ProjectField::named("row_uuid"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
        ]);
    let undeleted = GraphBuilder::anti_join(
        content_current.clone(),
        deleted_winners.clone(),
        ["row_uuid"],
        ["row_uuid"],
    )
    .project_fields(
        current_row_fields(table)
            .into_iter()
            .map(ProjectField::named)
            .chain([ProjectField::literal("__jazz_deleted", Value::Bool(false))]),
    );
    let deleted = GraphBuilder::join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project_fields(
            current_row_fields(table)
                .into_iter()
                .map(|field| {
                    let source = match field.as_str() {
                        "$updatedBy" | "$updatedAt" | "tx_time" | "tx_node_id" => {
                            right_field(&field)
                        }
                        _ => left_field(&field),
                    };
                    ProjectField::renamed(source, field)
                })
                .chain([ProjectField::literal("__jazz_deleted", Value::Bool(true))]),
        );
    GraphBuilder::union([undeleted, deleted])
}

pub(super) fn maintained_view_history_storage_field_names(table: &TableSchema) -> Vec<String> {
    let mut fields = vec![
        "row_uuid".to_owned(),
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "created_by".to_owned(),
        "created_at".to_owned(),
        "updated_by".to_owned(),
        "updated_at".to_owned(),
    ];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("authored_columns".to_owned());
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This is an internal planner assertion because the selected physical
    /// access path is not observable through the public query result. A
    /// declared `id` must never be mistaken for the storage row UUID.
    #[test]
    fn declared_id_filter_does_not_select_the_physical_primary_key() {
        let table = TableSchema::new("things", [ColumnSchema::new("id", ColumnType::Uuid)]);
        let declared_id = uuid::Uuid::from_u128(0x99);
        let equalities = BTreeMap::from([("id".to_owned(), Value::Uuid(declared_id))]);

        assert!(select_current_access_path(&table, &equalities).is_none());
    }

    /// A table without a declared `id` retains the legacy physical row-id
    /// primary-key access path.
    #[test]
    fn missing_declared_id_filter_selects_the_physical_primary_key() {
        let table = TableSchema::new("things", [ColumnSchema::new("label", ColumnType::String)]);
        let row_id = uuid::Uuid::from_u128(0x9a);
        let equalities = BTreeMap::from([("id".to_owned(), Value::Uuid(row_id))]);

        assert!(matches!(
            select_current_access_path(&table, &equalities),
            Some(CurrentAccessPath::PrimaryKey(values)) if values == vec![Value::Uuid(row_id)]
        ));
    }
}

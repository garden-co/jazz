//! Turn logical query read requirements into concrete Groove source graphs.
//!
//! This stage chooses physical, inline, historical, branch, or settled-view
//! inputs and applies durability, schema projection, and access-path decisions.
//! It does not normalize query syntax or materialize engine output into public
//! rows.

use super::*;
use crate::node::query_engine::BranchViewSourceBase;
pub(super) struct JazzSourceGraphPreparer<'a, S> {
    pub(super) node: &'a mut NodeState<S>,
    pub(super) read_view: &'a ReadView<RequestedSourceStage>,
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
    Index {
        column: String,
        prefix: Vec<Value>,
        intersections: Vec<(String, Vec<Value>)>,
        maintained: bool,
        /// A proved physical source cap for an ordinary one-shot read. This is
        /// never selected by policy compilation or subscriptions.
        source_limit: Option<usize>,
    },
}

impl<S> SourceGraphPreparer for JazzSourceGraphPreparer<'_, S>
where
    S: OrderedKvStorage,
{
    async fn prepare_source_graph(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        let Some(source) = self.read_view.sources.get(&request.source) else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        let (projection, graph_tier, history_position, snapshot, open_tx_overlay, branch_view) =
            match source {
                SourceExpr::VisibleCurrent {
                    projection,
                    data: DataSource::Current,
                    tier,
                } => (projection, Some(*tier), None, None, None, None),
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
                    None,
                    Some((head, base.as_ref())),
                ),
                SourceExpr::HistoryCut {
                    projection,
                    data: DataSource::Current,
                    position,
                } => (projection, None, Some(*position), None, None, None),
                SourceExpr::SnapshotRef {
                    projection,
                    data: DataSource::Current,
                    snapshot,
                } => (projection, None, None, Some(snapshot.clone()), None, None),
                SourceExpr::SettledBindingView {
                    projection,
                    binding_view,
                    rows,
                    requires_result_payload,
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
                    match self
                        .node
                        .settled_binding_view_source_rows(
                            &request.source.table,
                            self.read_view.read_schema,
                            *binding_view,
                            *rows,
                        )
                        .await
                    {
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
                                .await
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
                                requires_result_payload: *requires_result_payload,
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
                        }
                    };
                    let [OverlayRef::OpenTransaction(tx_id)] = overlays.entries.as_slice() else {
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
            .table_in_schema(&request.source.table, self.read_view.read_schema)
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
                || !matches!(authorization, SourceAuthorizationRequest::System)
            {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            let schema_version_alias = self
                .node
                .ensure_schema_version_alias(self.read_view.read_schema)
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let (graph, descriptor, metadata) = inline_current_graph_with_source_metadata(
                &table,
                rows.clone(),
                schema_version_alias,
                "inline-current",
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
                requires_result_payload: false,
                content_version: None,
                deletion_register: None,
            });
        }
        let (graph, descriptor, metadata, routing_fields) = if let Some((head, base)) = branch_view
        {
            if request.visibility == RowVisibility::IncludeDeleted && base.is_none() {
                let tier = graph_tier.expect("branch view has a current tier");
                let head_keys = self
                    .node
                    .equivalent_stored_branch_keys(
                        &request.source.table,
                        self.read_view.read_schema,
                        head,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let content = self
                    .projected_branch_content_source_graph(request, &table, tier, &head_keys)
                    .await?;
                let deletions = self
                    .projected_branch_deletion_source_graph(request, tier, &head_keys)
                    .await?;
                let base = include_deleted_branch_graph(&table, head, content, deletions)
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
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
                        let policy_request = policy_request.map(|mut request| {
                            request.reads.primary = policy_read_view_projected_through(
                                &request.reads.primary,
                                self.read_view,
                            );
                            request
                        });
                        let mut output_fields = current_row_fields(&table);
                        output_fields.push("__jazz_deleted".to_owned());
                        self.node
                            .compose_policy_filtered_current_source_graph(
                                policy_request,
                                base,
                                &output_fields,
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                return Ok(ResolvedSource {
                    table_schema: table.clone(),
                    graph,
                    row_shape: SourceRowShape {
                        source: request.source.clone(),
                        descriptor: include_deleted_current_row_descriptor(&table),
                        row_uuid_field: "row_uuid".to_owned(),
                        metadata: BTreeMap::new(),
                    },
                    routing_fields: BTreeSet::new(),
                    requires_result_payload: false,
                    content_version: None,
                    deletion_register: None,
                });
            }
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            if base.is_none_or(|base| matches!(base, BranchViewSourceBase::Current(_))) {
                let branch_witness_field =
                    (!table.branch_by.is_empty()).then_some("supplying_branch_key");
                let head_keys = self
                    .node
                    .equivalent_stored_branch_keys(
                        &request.source.table,
                        self.read_view.read_schema,
                        head,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let head_content = self
                    .projected_branch_content_source_graph(
                        request,
                        &table,
                        graph_tier.expect("branch view has a current tier"),
                        &head_keys,
                    )
                    .await?;
                let head_deletions = self
                    .projected_branch_deletion_source_graph(
                        request,
                        graph_tier.expect("branch view has a current tier"),
                        &head_keys,
                    )
                    .await?;
                let (content, deletions) = match base {
                    Some(BranchViewSourceBase::Current(base)) if base != head => {
                        let base_keys = self
                            .node
                            .equivalent_stored_branch_keys(
                                &request.source.table,
                                self.read_view.read_schema,
                                base,
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        let base_content = self
                            .projected_branch_content_source_graph(
                                request,
                                &table,
                                graph_tier.expect("branch view has a current tier"),
                                &base_keys,
                            )
                            .await?;
                        let base_deletions = self
                            .projected_branch_deletion_source_graph(
                                request,
                                graph_tier.expect("branch view has a current tier"),
                                &base_keys,
                            )
                            .await?;
                        (
                            GraphBuilder::union([
                                head_content.clone(),
                                GraphBuilder::anti_join(
                                    base_content,
                                    head_content,
                                    ["row_uuid"],
                                    ["row_uuid"],
                                ),
                            ]),
                            GraphBuilder::union([
                                head_deletions.clone(),
                                GraphBuilder::anti_join(
                                    base_deletions,
                                    head_deletions,
                                    ["row_uuid"],
                                    ["row_uuid"],
                                ),
                            ]),
                        )
                    }
                    _ => (head_content, head_deletions),
                };
                let content_version = request
                    .requirements
                    .metadata
                    .iter()
                    .any(|requirement| {
                        matches!(
                            requirement,
                            SourceMetadataRequirement::VersionPayloads
                                | SourceMetadataRequirement::VersionWitnesses
                                | SourceMetadataRequirement::Provenance(_)
                        )
                    })
                    .then(|| ContentVersionSource {
                        graph: content.clone().project_fields(
                            maintained_view_history_storage_field_names(&table)
                                .into_iter()
                                .map(ProjectField::named)
                                .chain(std::iter::once(ProjectField::renamed(
                                    "branch_key",
                                    "supplying_branch_key",
                                ))),
                        ),
                        row_uuid_field: "row_uuid".to_owned(),
                    });
                let deletion_register = request
                    .requirements
                    .metadata
                    .contains(&SourceMetadataRequirement::DeletionMarkers)
                    .then(|| DeletionRegisterSource {
                        graph: deletions.clone().project_fields(
                            register_storage_field_names()
                                .into_iter()
                                .map(ProjectField::named)
                                .chain(std::iter::once(ProjectField::renamed(
                                    "branch_key",
                                    "supplying_branch_key",
                                ))),
                        ),
                        row_uuid_field: "row_uuid".to_owned(),
                    });
                let deleted = deletions
                    .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                    .project(["row_uuid"]);
                let selected_base =
                    GraphBuilder::anti_join(content, deleted, ["row_uuid"], ["row_uuid"])
                        .project_fields(branch_view_storage_source_fields(&table, head).map_err(
                            |_| source_resolution_error(request, SourceGap::SchemaProjection),
                        )?);
                let (graph, descriptor, metadata, routing_fields) = resolved_current_source_graph(
                    self.node,
                    &table,
                    graph_tier.expect("branch view has a current tier"),
                    &request.requirements,
                    &authorization,
                    self.read_view.policy_schema,
                    Some(&self.read_view),
                    branch_witness_field,
                    Some(selected_base),
                )
                .map_err(|error| source_resolution_error_from_policy_proof(request, error))?;
                return Ok(ResolvedSource {
                    table_schema: table.clone(),
                    graph,
                    row_shape: SourceRowShape {
                        source: request.source.clone(),
                        descriptor,
                        row_uuid_field: "row_uuid".to_owned(),
                        metadata,
                    },
                    routing_fields,
                    requires_result_payload: true,
                    content_version,
                    deletion_register,
                });
            }
            if matches!(base, Some(BranchViewSourceBase::Snapshot(_, _))) {
                let branch_witness_field =
                    (!table.branch_by.is_empty()).then_some("supplying_branch_key");
                let tier = graph_tier.expect("branch view has a current tier");
                let frozen_base_key = match base {
                    Some(BranchViewSourceBase::Snapshot(key, _)) => key,
                    _ => unreachable!("guarded frozen branch base"),
                };
                let head_keys = self
                    .node
                    .equivalent_stored_branch_keys(
                        &request.source.table,
                        self.read_view.read_schema,
                        head,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let head_content = self
                    .projected_branch_content_source_graph(request, &table, tier, &head_keys)
                    .await?;
                let head_deletions = self
                    .projected_branch_deletion_source_graph(request, tier, &head_keys)
                    .await?;
                let head_content_presence = head_content.clone().project(["row_uuid"]);
                let head_deletion_presence = head_deletions.clone().project(["row_uuid"]);
                let deleted = head_deletions
                    .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                    .project(["row_uuid"]);
                let selected_head = GraphBuilder::anti_join(
                    head_content.clone(),
                    deleted,
                    ["row_uuid"],
                    ["row_uuid"],
                )
                .project_fields(
                    branch_view_storage_source_fields(&table, head).map_err(|_| {
                        source_resolution_error(request, SourceGap::SchemaProjection)
                    })?,
                );
                let system_authorization = SourceAuthorizationRequest::System;
                let (live_head, _, metadata, routing_fields) = resolved_current_source_graph(
                    self.node,
                    &table,
                    tier,
                    &request.requirements,
                    &system_authorization,
                    self.read_view.policy_schema,
                    Some(&self.read_view),
                    branch_witness_field,
                    Some(selected_head),
                )
                .map_err(|error| source_resolution_error_from_policy_proof(request, error))?;
                let descriptor = current_row_descriptor_with_hidden_source_fields_for_branch(
                    &table,
                    &metadata,
                    branch_witness_field.is_some(),
                );
                // Capture the full opening view once. Anti-joining it against
                // live head presence leaves only inherited frozen rows; all
                // subsequent head changes remain maintained table inputs.
                let opening_rows = self
                    .node
                    .branch_view_rows_for_schema(
                        &request.source.table,
                        self.read_view.read_schema,
                        tier,
                        head,
                        base,
                    )
                    .await
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let schema_version_alias = self
                    .node
                    .ensure_schema_version_alias(self.read_view.read_schema)
                    .await
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let (opening, opening_descriptor, opening_metadata) =
                    inline_current_graph_with_source_metadata_and_branch_witness(
                        &table,
                        opening_rows,
                        schema_version_alias,
                        "frozen-branch-base",
                        &request.requirements,
                        branch_witness_field.map(|field| (field, frozen_base_key)),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                if descriptor != opening_descriptor || metadata != opening_metadata {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::SchemaProjection,
                    ));
                }
                let inherited = GraphBuilder::anti_join(
                    opening,
                    head_content_presence,
                    ["row_uuid"],
                    ["row_uuid"],
                );
                let inherited = GraphBuilder::anti_join(
                    inherited,
                    head_deletion_presence,
                    ["row_uuid"],
                    ["row_uuid"],
                );
                let unfiltered = GraphBuilder::union([live_head, inherited]);
                let graph = match &authorization {
                    SourceAuthorizationRequest::System => unfiltered,
                    SourceAuthorizationRequest::PolicyFiltered {
                        permission_subject,
                        plan,
                    }
                    | SourceAuthorizationRequest::PolicyProof {
                        permission_subject,
                        plan,
                    } => {
                        let param_binding_mode = if plan.binding_source_shape.is_some() {
                            ParamBindingMode::RetainAllParams
                        } else {
                            ParamBindingMode::InlineAllReachableSeeds
                        };
                        let policy_request = self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            param_binding_mode,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                        let policy_request = policy_request.map(|mut request| {
                            request.reads.primary = policy_read_view_projected_through(
                                &request.reads.primary,
                                self.read_view,
                            );
                            request
                        });
                        let mut output_fields = current_row_fields(&table);
                        output_fields.extend(branch_witness_field.map(str::to_owned));
                        self.node
                            .compose_policy_filtered_current_source_graph(
                                policy_request,
                                unfiltered,
                                &output_fields,
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                return Ok(ResolvedSource {
                    table_schema: table.clone(),
                    graph,
                    row_shape: SourceRowShape {
                        source: request.source.clone(),
                        descriptor,
                        row_uuid_field: "row_uuid".to_owned(),
                        metadata,
                    },
                    routing_fields,
                    requires_result_payload: true,
                    content_version: request
                        .requirements
                        .metadata
                        .iter()
                        .any(|requirement| {
                            matches!(
                                requirement,
                                SourceMetadataRequirement::VersionPayloads
                                    | SourceMetadataRequirement::VersionWitnesses
                                    | SourceMetadataRequirement::Provenance(_)
                            )
                        })
                        .then(|| ContentVersionSource {
                            graph: head_content.project_fields(
                                maintained_view_history_storage_field_names(&table)
                                    .into_iter()
                                    .map(ProjectField::named)
                                    .chain(std::iter::once(ProjectField::renamed(
                                        "branch_key",
                                        "supplying_branch_key",
                                    ))),
                            ),
                            row_uuid_field: "row_uuid".to_owned(),
                        }),
                    deletion_register: None,
                });
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
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let (base_graph, descriptor, metadata) = if request.requirements.metadata.is_empty() {
                (
                    inline_current_graph(&table, rows)
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?,
                    current_row_descriptor(&table),
                    BTreeMap::new(),
                )
            } else {
                let schema_version_alias = self
                    .node
                    .ensure_schema_version_alias(self.read_view.read_schema)
                    .await
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                inline_current_graph_with_source_metadata(
                    &table,
                    rows,
                    schema_version_alias,
                    "branch-view",
                    &request.requirements,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            };
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base_graph,
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
                    let param_binding_mode = if plan.binding_source_shape.is_some() {
                        ParamBindingMode::RetainAllParams
                    } else {
                        ParamBindingMode::InlineAllReachableSeeds
                    };
                    let policy_request = self.node.table_read_policy_authorization_request(
                        self.read_view.policy_schema,
                        &table.name,
                        *permission_subject,
                        param_binding_mode,
                        graph_tier.expect("branch view has a current tier"),
                        plan.binding_source_shape.clone(),
                        plan.binding_user_params.clone(),
                        plan.binding_claim_params.clone(),
                    );
                    let policy_request = policy_request.map(|mut request| {
                        request.reads.primary = policy_read_view_projected_through(
                            &request.reads.primary,
                            self.read_view,
                        );
                        request
                    });
                    self.node
                        .compose_policy_filtered_current_source_graph(
                            policy_request,
                            base_graph,
                            &current_row_fields(&table),
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
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
            let base = self
                .projected_historical_source_graph(request, &table, position)
                .await?;
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
                        .compose_policy_filtered_current_source_graph(
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
        } else if let Some(snapshot) = snapshot {
            if request.visibility != RowVisibility::Visible
                || !request.requirements.metadata.is_empty()
                || !matches!(authorization, SourceAuthorizationRequest::System)
            {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
            let rows = self
                .node
                .projected_snapshot_current_rows(
                    &request.source.table,
                    self.read_view.read_schema,
                    &snapshot,
                )
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            let graph = inline_current_graph(&table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            (
                graph,
                current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
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
                .await
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
                    inline_snapshot_include_deleted_current_graph(&table, rows).map_err(|_| {
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
                let source = self
                    .projected_maintained_visible_current_source_graph(
                        request,
                        &table,
                        graph_tier.expect("visible current source has a tier"),
                    )
                    .await?;
                resolved_current_source_graph(
                    self.node,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                    &request.requirements,
                    &authorization,
                    self.read_view.policy_schema,
                    Some(self.read_view),
                    None,
                    Some(source.graph),
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            } else {
                let tier = graph_tier.expect("visible current source has a tier");
                let selected_policy_base = if self.access_paths.contains_key(&request.source) {
                    None
                } else if let Some(access_path) =
                    self.cached_policy_authorization_access_path(request)?
                {
                    self.selected_global_current_source_graph_for_access_path(
                        request,
                        &table,
                        tier,
                        access_path,
                    )
                    .await?
                } else {
                    None
                };
                let source = if let Some(graph) = selected_policy_base {
                    CurrentSourceGraph {
                        graph: graph.project_fields(storage_to_canonical_current_source_fields(
                            &table, true, false,
                        )),
                        descriptor: current_row_descriptor(&table),
                        metadata: BTreeMap::new(),
                    }
                } else {
                    self.projected_visible_current_source_graph(request, &table, tier)
                        .await?
                };
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
                        let param_binding_mode = if plan.binding_source_shape.is_some() {
                            ParamBindingMode::RetainAllParams
                        } else {
                            ParamBindingMode::InlineAllReachableSeeds
                        };
                        let policy_request = self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            param_binding_mode,
                            graph_tier.expect("visible current source has a tier"),
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                        let policy_request = policy_request.map(|mut request| {
                            request.reads.primary = policy_read_view_projected_through(
                                &request.reads.primary,
                                self.read_view,
                            );
                            request
                        });
                        self.node
                            .compose_policy_filtered_current_source_graph(
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
            let base = self
                .projected_include_deleted_current_source_graph(request, &table, tier)
                .await?;
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
                        .compose_policy_filtered_current_source_graph(
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
                        .compose_policy_filtered_current_source_graph(
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
            let tier = graph_tier.expect("visible current source has a tier");
            let selected_base = self
                .selected_global_current_source_graph(request, &table, tier)
                .await?;
            let selected_base = match selected_base {
                Some(selected_base) => Some(selected_base),
                None => match self.cached_policy_authorization_access_path(request)? {
                    Some(access_path) => {
                        self.selected_global_current_source_graph_for_access_path(
                            request,
                            &table,
                            tier,
                            access_path,
                        )
                        .await?
                    }
                    None => None,
                },
            };
            if selected_base.is_none() {
                self.node.query_engine_read_metrics.source_full_scans += 1;
            }
            resolved_current_source_graph(
                self.node,
                &table,
                tier,
                &request.requirements,
                &authorization,
                self.read_view.policy_schema,
                Some(self.read_view),
                None,
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
        let content_version = self
            .content_version_source_for_request(
                request,
                &table,
                graph_tier,
                history_position,
                open_tx_overlay,
            )
            .await?;
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
            requires_result_payload: false,
            content_version,
            deletion_register,
        })
    }
}

impl<S> JazzSourceGraphPreparer<'_, S>
where
    S: OrderedKvStorage,
{
    pub(super) fn policy_dependency_request(
        &mut self,
        request: &SourceRequest,
    ) -> Result<Option<QueryProgramRequest>, Error> {
        let SourceAuthorizationRequest::PolicyFiltered {
            permission_subject,
            plan,
        } = &request.authorization
        else {
            return Ok(None);
        };
        if plan.protected_source.table != request.source.table
            || plan.role != PolicyDecisionRole::Read
            || plan.protected_row_field != "row_uuid"
        {
            return Err(Error::QueryCapability(
                "policy authorization plan does not match source dependency".to_owned(),
            ));
        }
        let source = self
            .read_view
            .sources
            .get(&request.source)
            .ok_or(Error::InvalidStoredValue("query source dependency missing"))?;
        let binding_source_shape = plan.binding_source_shape.clone();
        let binding_user_params = plan.binding_user_params.clone();
        let binding_claim_params = plan.binding_claim_params.clone();
        let dependency = match source {
            SourceExpr::HistoryCut {
                data: DataSource::Current,
                position,
                ..
            } => self.node.table_read_policy_authorization_request_at(
                self.read_view.policy_schema,
                &request.source.table,
                *permission_subject,
                ParamBindingMode::InlineAllReachableSeeds,
                *position,
                binding_source_shape,
                binding_user_params,
                binding_claim_params,
            ),
            SourceExpr::VisibleCurrent { .. }
            | SourceExpr::BranchView { .. }
            | SourceExpr::SettledBindingView { .. } => {
                let tier = source.current_tier().unwrap_or(DurabilityTier::Global);
                if request.visibility == RowVisibility::IncludeDeleted {
                    self.node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &request.source.table,
                            *permission_subject,
                            tier,
                            binding_source_shape,
                            binding_user_params,
                            binding_claim_params,
                        )
                } else {
                    let param_binding_mode = if binding_source_shape.is_some() {
                        ParamBindingMode::RetainAllParams
                    } else {
                        ParamBindingMode::InlineAllReachableSeeds
                    };
                    self.node.table_read_policy_authorization_request(
                        self.read_view.policy_schema,
                        &request.source.table,
                        *permission_subject,
                        param_binding_mode,
                        tier,
                        binding_source_shape,
                        binding_user_params,
                        binding_claim_params,
                    )
                }
            }
            // Transaction overlays already carry explicitly captured rows and
            // do not apply a second read-policy program in source preparation.
            SourceExpr::WithOverlays { .. } => return Ok(None),
            _ => return Ok(None),
        };
        match dependency {
            Ok(mut dependency) => {
                dependency.reads.primary =
                    policy_read_view_projected_through(&dependency.reads.primary, self.read_view);
                Ok(Some(dependency))
            }
            Err(Error::QueryCapability(error)) if error.contains("PolicyProofCycle") => {
                Err(Error::QueryCapability(error))
            }
            Err(Error::QueryCapability(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

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

    pub(crate) async fn selected_global_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<Option<GraphBuilder>, SourceResolutionError> {
        let Some(access_path) = self.access_paths.get(&request.source).cloned() else {
            return Ok(None);
        };
        self.selected_global_current_source_graph_for_access_path(request, table, tier, access_path)
            .await
    }

    /// Reuse the access paths chosen while compiling the already-prepared
    /// policy dependency. Source resolution does not inspect policy syntax or
    /// select a policy-specific path; it only consumes this generic planner
    /// receipt to narrow the protected source before the same proof graph is
    /// joined back in below.
    fn cached_policy_authorization_access_path(
        &mut self,
        request: &SourceRequest,
    ) -> Result<Option<CurrentAccessPath>, SourceResolutionError> {
        let Some(policy_request) = self
            .policy_dependency_request(request)
            .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
        else {
            return Ok(None);
        };
        let cache_key = policy_authorization_graph_cache_key(&policy_request);
        let Some(authorization) = self
            .node
            .query
            .policy_authorization_graph_cache
            .get(&cache_key)
        else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        Ok(authorization.access_paths.get(&request.source).cloned())
    }

    async fn selected_global_current_source_graph_for_access_path(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
        access_path: CurrentAccessPath,
    ) -> Result<Option<GraphBuilder>, SourceResolutionError> {
        match access_path {
            CurrentAccessPath::PrimaryKey(prefix) => {
                self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                Ok(Some(selected_visible_current_primary_key_graph(
                    table, tier, prefix,
                )))
            }
            CurrentAccessPath::Index {
                column,
                prefix,
                intersections,
                maintained,
                source_limit,
            } => {
                if tier != DurabilityTier::Global {
                    return Ok(None);
                }
                let source_limit = (request.visibility == RowVisibility::IncludeDeleted)
                    .then_some(source_limit)
                    .flatten();
                let projection_target = self.current_projection_target(request, table)?;
                let rows = self
                    .node
                    .physical_global_current_source_for_index_scan(
                        table,
                        self.read_view.read_schema,
                        &column,
                        &prefix,
                        &intersections,
                        maintained,
                        source_limit,
                        &projection_target,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                self.node.query_engine_read_metrics.source_index_probes +=
                    1 + intersections.len() as u64;
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

    pub(crate) async fn content_version_source_for_request(
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
                    .projected_content_current_source_graph(request, table, tier, false, false)
                    .await?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        Ok(Some(ContentVersionSource {
            graph: content_version_current_source_graph(table, tier, false),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    pub(crate) async fn projected_historical_source_graph(
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
                .await
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
            .await
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
        inline_current_graph(table, rows)
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))
    }

    pub(crate) async fn projected_maintained_visible_current_source_graph(
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
        let projected = self
            .projected_content_current_source_graph(request, table, tier, true, true)
            .await?;
        Ok(CurrentSourceGraph {
            graph: projected,
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    /// Build the maintained content-winner relation for one logical branch
    /// key. `stored_keys` includes historical short spellings produced before
    /// monotone branch-column additions; they compete as one branch-local row.
    pub(crate) async fn projected_branch_content_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
        stored_keys: &BTreeSet<BranchKey>,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let required_fields = self.current_projection_required_fields(request, table);
        let (projection_target, physical_fields) = self
            .node
            .ensure_physical_current_winner_projection(
                self.read_view.read_schema,
                &request.source.table,
            )
            .await
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let post_winner_fields = self
            .node
            .physical_current_post_winner_projection_fields(
                self.read_view.read_schema,
                &request.source.table,
                &required_fields,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let branch_sources = |class, target: String| {
            stored_keys
                .iter()
                .map(|branch_key| {
                    self.node
                        .physical_current_branch_source_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            class,
                            target.clone(),
                            branch_key,
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(GraphBuilder::union)
        };
        let global = branch_sources(PhysicalCurrentClass::Global, projection_target.clone())?
            .project(physical_fields.clone());
        let content = if tier == DurabilityTier::Global {
            global
        } else {
            let ahead = branch_sources(PhysicalCurrentClass::Ahead, projection_target)?;
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
        Ok(content.project_fields(post_winner_fields))
    }

    /// Build the maintained deletion-register winner relation for one logical
    /// branch key, normalizing historical short key spellings before winner
    /// selection.
    pub(crate) async fn projected_branch_deletion_source_graph(
        &mut self,
        request: &SourceRequest,
        tier: DurabilityTier,
        stored_keys: &BTreeSet<BranchKey>,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let table_id = self
            .node
            .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let fields = std::iter::once("branch_key".to_owned())
            .chain(register_storage_field_names())
            .collect::<Vec<_>>();
        let branch_sources = |table_name: String| {
            GraphBuilder::union(stored_keys.iter().map(|branch_key| {
                GraphBuilder::table_scan(
                    table_name.clone(),
                    groove::ivm::StaticScanSpec::Prefix(vec![groove::ivm::LiteralValue::from(
                        Value::Bytes(branch_key.canonical_bytes()),
                    )]),
                )
            }))
        };
        let global = branch_sources(physical_register_global_current_table_name(table_id))
            .project(fields.clone());
        if tier == DurabilityTier::Global {
            return Ok(global);
        }
        let ahead = branch_sources(physical_register_ahead_current_table_name(table_id));
        let ahead = if tier == DurabilityTier::Edge {
            edge_visible_ahead_current_source_graph(ahead, fields.clone())
        } else {
            ahead.project(fields.clone())
        };
        Ok(GraphBuilder::arg_max_by(
            GraphBuilder::union([global, ahead]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .project(fields))
    }

    pub(crate) async fn projected_content_current_source_graph(
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
                Some(CurrentAccessPath::Index {
                    column,
                    prefix,
                    intersections,
                    maintained,
                    source_limit,
                }) => {
                    let source_limit = (!exclude_deleted).then_some(source_limit).flatten();
                    self.node.query_engine_read_metrics.source_index_probes +=
                        1 + intersections.len() as u64;
                    self.node
                        .physical_global_current_source_for_index_scan(
                            read_table,
                            self.read_view.read_schema,
                            &column,
                            &prefix,
                            &intersections,
                            maintained,
                            source_limit,
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
            .await
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
            Some(CurrentAccessPath::Index {
                column,
                prefix,
                intersections,
                maintained,
                source_limit,
            }) => {
                // Select settled candidates before combining them with the
                // corresponding Local ahead candidates below.
                let source_limit = (!exclude_deleted).then_some(*source_limit).flatten();
                self.node.query_engine_read_metrics.source_index_probes +=
                    1 + intersections.len() as u64;
                self.node
                    .physical_global_current_source_for_index_scan_with_output(
                        read_table,
                        self.read_view.read_schema,
                        column,
                        prefix,
                        intersections,
                        *maintained,
                        source_limit,
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
                Some(CurrentAccessPath::Index {
                    column,
                    prefix,
                    intersections,
                    maintained,
                    source_limit,
                }) => self.node.physical_ahead_current_source_for_index_scan(
                    read_table,
                    self.read_view.read_schema,
                    column,
                    prefix,
                    intersections,
                    *maintained,
                    (!exclude_deleted).then_some(*source_limit).flatten(),
                    &projection_target,
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

    pub(crate) async fn projected_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        Ok(CurrentSourceGraph {
            // Project heterogeneous physical rows at the source boundary so the
            // rest of the query graph retains the requested logical descriptor.
            graph: self
                .projected_content_current_source_graph(request, table, tier, false, true)
                .await?
                .project_fields(storage_to_canonical_current_source_fields(
                    table, true, false,
                )),
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    async fn projected_include_deleted_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let content = self
            .projected_content_current_source_graph(request, table, tier, false, false)
            .await?
            .project_fields(storage_to_canonical_current_source_fields(
                table, true, false,
            ));
        let deleted_winners = self
            .projected_deletion_register_current_source_graph(request, tier)?
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project_fields([
                ProjectField::named("row_uuid"),
                ProjectField::named("tx_time"),
                ProjectField::named("tx_node_id"),
                ProjectField::renamed("updated_by", "$updatedBy"),
                ProjectField::renamed("updated_at", "$updatedAt"),
            ]);
        let undeleted = GraphBuilder::anti_join(
            content.clone(),
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
        let deleted = GraphBuilder::join(content, deleted_winners, ["row_uuid"], ["row_uuid"])
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
        Ok(GraphBuilder::union([undeleted, deleted]))
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

fn policy_read_view_projected_through(
    policy_view: &ReadView<RequestedSourceStage>,
    enclosing_view: &ReadView<RequestedSourceStage>,
) -> ReadView<RequestedSourceStage> {
    let mut projected = policy_view.clone();
    // The outer normalized query and the independently normalized policy proof
    // may use different aliases for one logical table. Preserve every policy
    // source, including policy-only recursive/access tables, while giving each
    // source that the outer query also reads its exact current/live/frozen view.
    for (policy_source, projected_source) in &mut projected.sources {
        if let Some(source) = enclosing_view
            .sources
            .iter()
            .find_map(|(source_id, source)| {
                (source_id.table == policy_source.table).then(|| source.clone())
            })
        {
            *projected_source = source;
        }
    }
    projected
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
    policy_read_view: Option<&ReadView<RequestedSourceStage>>,
    branch_witness_field: Option<&str>,
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
        if let Some(branch_witness_field) = branch_witness_field {
            fields.push(ProjectField::named(branch_witness_field));
        }
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: branch_witness_field.map(str::to_owned),
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

    let descriptor = current_row_descriptor_with_hidden_source_fields_for_branch(
        table,
        &metadata,
        branch_witness_field.is_some(),
    );
    let (base, routing_fields) = match authorization {
        SourceAuthorizationRequest::System => {
            let graph = if let Some(selected_base) = selected_base.clone() {
                let mut selected_fields = storage_to_canonical_current_source_fields(
                    table,
                    needs_version_witnesses,
                    needs_settle_position,
                );
                if let Some(branch_witness_field) = branch_witness_field {
                    selected_fields.push(ProjectField::named(branch_witness_field));
                }
                selected_base.project_fields(selected_fields)
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
            let policy_request = policy_request.map(|mut request| {
                if let Some(read_view) = policy_read_view {
                    request.reads.primary =
                        policy_read_view_projected_through(&request.reads.primary, read_view);
                }
                request
            });
            let mut output_fields = global_current_storage_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            if let Some(branch_witness_field) = branch_witness_field {
                output_fields.push(branch_witness_field.to_owned());
            }
            let base = match selected_base {
                Some(selected_base) => selected_base,
                None => node.maintained_view_content_current_with_version(table, tier)?,
            };
            let storage_graph = node.compose_policy_filtered_current_source_graph(
                policy_request,
                base.clone(),
                &output_fields,
            )?;
            let mut canonical_fields = storage_to_canonical_current_source_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            if let Some(branch_witness_field) = branch_witness_field {
                canonical_fields.push(ProjectField::named(branch_witness_field));
            }
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

fn branch_view_storage_source_fields(
    table: &TableSchema,
    head: &BranchKey,
) -> Result<Vec<ProjectField>, Error> {
    let head_values = head.values.iter().cloned().collect::<BTreeMap<_, _>>();
    let mut fields = vec![
        ProjectField::renamed("branch_key", "supplying_branch_key"),
        ProjectField::named("row_uuid"),
        ProjectField::named("schema_version"),
        ProjectField::named("parents"),
        ProjectField::named("authored_columns"),
    ];
    for column in &table.columns {
        if table.branch_by.contains(&column.name) {
            let value = head_values
                .get(&column.name)
                .ok_or(Error::InvalidBranchKey(
                    "head branch key missing projected table column".to_owned(),
                ))?
                .decode()
                .map_err(|_| {
                    Error::InvalidBranchKey("invalid head branch value encoding".to_owned())
                })?;
            fields.push(ProjectField::literal(
                user_column_field(&column.name),
                value,
            ));
        } else {
            fields.push(ProjectField::named(user_column_field(&column.name)));
        }
    }
    fields.extend([
        ProjectField::named("created_by"),
        ProjectField::named("created_at"),
        ProjectField::named("updated_by"),
        ProjectField::named("updated_at"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::named("global_time"),
    ]);
    Ok(fields)
}

pub(super) fn current_row_descriptor_with_hidden_source_fields(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
) -> RecordDescriptor {
    current_row_descriptor_with_hidden_source_fields_for_branch(table, metadata, false)
}

fn current_row_descriptor_with_hidden_source_fields_for_branch(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    branch_columns_nonnullable: bool,
) -> RecordDescriptor {
    let mut fields = std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
        .chain(table.columns.iter().map(|column| {
            let value_type = if branch_columns_nonnullable && table.branch_by.contains(&column.name)
            {
                column.column_type.clone()
            } else {
                ValueType::Nullable(Box::new(column.column_type.clone()))
            };
            (user_column_field(&column.name), value_type)
        }))
        .chain([
            ("$createdBy".to_owned(), ValueType::String),
            ("$createdAt".to_owned(), ValueType::U64),
            ("$updatedBy".to_owned(), ValueType::String),
            ("$updatedAt".to_owned(), ValueType::U64),
            ("tx_time".to_owned(), ValueType::U64),
            ("tx_node_id".to_owned(), ValueType::U64),
        ])
        .collect::<Vec<_>>();
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
            (
                "authored_columns".to_owned(),
                ValueType::Nullable(Box::new(ValueType::Bytes)),
            ),
            ("created_by".to_owned(), ValueType::String),
            ("created_at".to_owned(), ValueType::U64),
            ("updated_by".to_owned(), ValueType::String),
            ("updated_at".to_owned(), ValueType::U64),
        ]);
        if let Some(SourceMetadataFields::VersionWitnesses {
            branch_or_prefix_field: Some(field),
            ..
        }) = metadata.get(&SourceMetadataRequirement::VersionWitnesses)
        {
            fields.push((field.clone(), ValueType::Bytes));
        }
    }
    if branch_columns_nonnullable
        && !metadata.contains_key(&SourceMetadataRequirement::VersionWitnesses)
    {
        fields.push(("supplying_branch_key".to_owned(), ValueType::Bytes));
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

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Select physical current-source access paths from the normalized program
    /// itself. `Access path` is standard query-planner terminology, but this
    /// deliberately trivial, rule-based planner only recognizes supported
    /// indexed equality constraints and otherwise falls back to a full scan;
    /// it makes no cardinality/selectivity estimates or cost comparisons.
    /// This intentionally has no policy branch: an authorization proof is just
    /// another normalized program with a different policy context. Every
    /// selected path merely narrows candidate rows; the lowered predicate graph
    /// still decides membership.
    pub(super) fn query_program_access_paths(
        &self,
        request: &QueryProgramRequest,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        // A policy proof may contain both an owner arm and a correlated
        // membership arm for the same protected source. Walking its nested
        // nodes independently would see the owner's claim equality and apply
        // that index path to the entire union, incorrectly excluding rows
        // that only the membership arm authorizes. The small planner does not
        // prove per-arm coverage, so any alternative or relational proof
        // retains full source scans.
        if request.input.shape.nodes.values().any(|node| {
            matches!(
                node,
                RowSetExpr::Union { .. }
                    | RowSetExpr::Join { .. }
                    | RowSetExpr::RecursiveRelation { .. }
            )
        }) {
            return Ok(BTreeMap::new());
        }
        self.normalized_program_access_paths(
            &request.input,
            &request.reads.primary,
            &request.policy,
            false,
        )
    }

    fn normalized_program_access_paths(
        &self,
        input: &RowSetProgramInput,
        read_view: &ReadView<RequestedSourceStage>,
        policy: &PolicyContext,
        allow_local: bool,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        let mut equalities_by_source = BTreeMap::new();
        // This deliberately small access-path selector only recognizes a
        // source's own Filter -> Source pipeline.  Inspecting every normalized
        // node lets independent recursive seed/step pipelines participate,
        // while still refusing to infer restrictions through joins, unions,
        // or other relational operators.
        for node_id in input.shape.nodes.keys() {
            let Some(equalities) =
                normalized_program_equalities(&input.shape, node_id, &input.binding, policy)?
            else {
                continue;
            };
            for (source, equalities) in equalities {
                equalities_by_source
                    .entry(source)
                    .or_insert_with(BTreeMap::new)
                    .extend(equalities);
            }
        }

        let mut paths = BTreeMap::new();
        for (source, equalities) in equalities_by_source {
            let Some(tier) = read_view.source_current_tier(&source) else {
                continue;
            };
            let table = self.table_in_schema(&source.table, read_view.read_schema)?;
            let Some(mut path) = select_current_access_path(&table, &equalities) else {
                continue;
            };
            if !allow_local && let CurrentAccessPath::Index { maintained, .. } = &mut path {
                *maintained = true;
            }
            // Local sources arrange equivalent intersections over settled and
            // ahead physical rows before winner selection; Edge still requires
            // its host-owned coverage route.
            if matches!(tier, DurabilityTier::Global | DurabilityTier::Local) {
                paths.insert(source, path);
            }
        }
        Ok(paths)
    }

    pub(super) fn one_shot_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        if !matches!(tier, DurabilityTier::Local | DurabilityTier::Global) {
            return Ok(BTreeMap::new());
        }
        let normalized = self.normalized_row_set_shape(shape, binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                shape,
                binding,
                None,
                BTreeMap::new(),
                BTreeMap::new(),
            ),
            shape: normalized,
        };
        let reads = current_query_read_set(
            &input.shape,
            shape.schema_version(),
            shape.schema_version(),
            tier,
            None,
            false,
        );
        let mut paths = self.normalized_program_access_paths(
            &input,
            &reads.primary,
            &PolicyContext::System,
            true,
        )?;

        // A source cap is stronger than an index access path: it is only safe
        // when the source is itself the final result prefix. In particular,
        // ordinary visible reads retain their unbounded path because the
        // deletion anti-join can discard sparse candidates after this source.
        // `projected_content_current_source_graph` drops this cap whenever that
        // anti-join is present, leaving the narrow IncludeDeleted one-shot
        // shape below as the initial receipt.
        let query = shape.query();
        let Some(limit) = query.limit else {
            return Ok(paths);
        };
        if query.offset != 0
            || !query.order_by.is_empty()
            || !query.joins.is_empty()
            || query.flat_join.is_some()
            || !query.policy_branches.is_empty()
            || !query.reachable.is_empty()
            || !query.inherits.is_empty()
            || !query.includes.is_empty()
            || !query.array_subqueries.is_empty()
            || query.aggregate.is_some()
        {
            return Ok(paths);
        }
        let root = root_source_id(&query.table);
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        if table.has_any_policy() {
            return Ok(paths);
        }
        if let Some(CurrentAccessPath::Index { source_limit, .. }) = paths.get_mut(&root) {
            *source_limit = Some(limit);
        }
        Ok(paths)
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
        intersections: &[(String, Vec<Value>)],
        maintained: bool,
        source_limit: Option<usize>,
        projection_target: &str,
    ) -> Result<GraphBuilder, Error> {
        self.physical_global_current_source_for_index_scan_with_output(
            table,
            schema_version,
            column,
            prefix,
            intersections,
            maintained,
            source_limit,
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
        intersections: &[(String, Vec<Value>)],
        maintained: bool,
        source_limit: Option<usize>,
        projection_target: &str,
        _output: RecordDescriptor,
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
        let scan = match source_limit {
            Some(max_items) => StaticScanSpec::PrefixLimit {
                prefix: prefix.iter().cloned().map(LiteralValue::from).collect(),
                max_items,
            },
            None => {
                StaticScanSpec::Prefix(prefix.iter().cloned().map(LiteralValue::from).collect())
            }
        };
        let intersections = intersections
            .iter()
            .map(|(column, prefix)| {
                let column_id =
                    mapping
                        .columns
                        .get(column)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical current intersected index column mapping missing",
                        ))?;
                Ok((
                    physical_current_index_name(column_id),
                    StaticScanSpec::Prefix(
                        prefix.iter().cloned().map(LiteralValue::from).collect(),
                    ),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let primary_index = physical_current_index_name(column_id);
        if maintained {
            let mut graph = GraphBuilder::variant_index_scan(
                storage_table.clone(),
                primary_index,
                projection_target,
                scan,
            );
            for (index, scan) in intersections {
                let right = GraphBuilder::variant_index_scan(
                    storage_table.clone(),
                    index,
                    projection_target,
                    scan,
                );
                graph = GraphBuilder::semi_join(graph, right, ["row_uuid"], ["row_uuid"]);
            }
            Ok(graph)
        } else {
            Ok(GraphBuilder::variant_index_intersection_scan(
                storage_table,
                primary_index,
                scan,
                intersections,
                projection_target,
            ))
        }
    }

    fn physical_ahead_current_source_for_index_scan(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &str,
        prefix: &[Value],
        intersections: &[(String, Vec<Value>)],
        maintained: bool,
        source_limit: Option<usize>,
        projection_target: &str,
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
        let scan = match source_limit {
            Some(max_items) => StaticScanSpec::PrefixLimit {
                prefix: prefix.iter().cloned().map(LiteralValue::from).collect(),
                max_items,
            },
            None => {
                StaticScanSpec::Prefix(prefix.iter().cloned().map(LiteralValue::from).collect())
            }
        };
        let intersections = intersections
            .iter()
            .map(|(column, prefix)| {
                let column_id =
                    mapping
                        .columns
                        .get(column)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical ahead intersected index column mapping missing",
                        ))?;
                Ok((
                    physical_current_index_name(column_id),
                    StaticScanSpec::Prefix(
                        prefix.iter().cloned().map(LiteralValue::from).collect(),
                    ),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let storage_table = physical_ahead_current_table_name(mapping.table_id);
        let primary_index = physical_current_index_name(column_id);
        if maintained {
            let mut graph = GraphBuilder::variant_index_scan(
                storage_table.clone(),
                primary_index,
                projection_target,
                scan,
            );
            for (index, scan) in intersections {
                let right = GraphBuilder::variant_index_scan(
                    storage_table.clone(),
                    index,
                    projection_target,
                    scan,
                );
                graph = GraphBuilder::semi_join(graph, right, ["row_uuid"], ["row_uuid"]);
            }
            Ok(graph)
        } else {
            Ok(GraphBuilder::variant_index_intersection_scan(
                storage_table,
                primary_index,
                scan,
                intersections,
                projection_target,
            ))
        }
    }
}

/// Return only restrictions that are plainly safe to push into a physical
/// source.  The result is intentionally conservative: this planner does not
/// reason through relational operators or alternative predicate branches. It
/// may retain equality restrictions from an enclosing conjunction. A full
/// normalized program still runs after the scan, so this is a physical
/// candidate-selection optimization, never a second evaluator.
fn normalized_program_equalities(
    shape: &NormalizedRowSetShape,
    node_id: &RowSetNodeId,
    binding: &ProgramBinding,
    policy: &PolicyContext,
) -> Result<Option<BTreeMap<SourceId, BTreeMap<String, Value>>>, Error> {
    let node = shape.nodes.get(node_id).ok_or(Error::InvalidStoredValue(
        "normalized access-path node missing",
    ))?;
    match node {
        RowSetExpr::Filter { input, predicate } => {
            let Some(mut equalities) =
                normalized_program_equalities(shape, input, binding, policy)?
            else {
                return Ok(None);
            };
            collect_normalized_program_equalities(predicate, binding, policy, &mut equalities)?;
            Ok(Some(equalities))
        }
        RowSetExpr::Distinct { input, .. }
        | RowSetExpr::OrderBy { input, .. }
        | RowSetExpr::Project { input, .. }
        | RowSetExpr::Slice { input, .. } => {
            normalized_program_equalities(shape, input, binding, policy)
        }
        RowSetExpr::Source { source, .. } => {
            Ok(Some(BTreeMap::from([(source.clone(), BTreeMap::new())])))
        }
        RowSetExpr::Aggregate { .. }
        | RowSetExpr::CorrelatedPathProjection { .. }
        | RowSetExpr::FrontierSource { .. }
        | RowSetExpr::Join { .. }
        | RowSetExpr::RecursiveRelation { .. }
        | RowSetExpr::Union { .. }
        | RowSetExpr::ValueSource { .. } => Ok(None),
    }
}

fn collect_normalized_program_equalities(
    predicate: &NormalizedPredicateExpr,
    binding: &ProgramBinding,
    policy: &PolicyContext,
    equalities: &mut BTreeMap<SourceId, BTreeMap<String, Value>>,
) -> Result<(), Error> {
    match predicate {
        NormalizedPredicateExpr::And(predicates) => {
            for predicate in predicates {
                collect_normalized_program_equalities(predicate, binding, policy, equalities)?;
            }
        }
        NormalizedPredicateExpr::Compare {
            left,
            op: NormalizedComparisonOp::Eq,
            right,
        } => {
            if let Some((source, field, value)) =
                normalized_program_equality(left, right, binding, policy)?
            {
                equalities
                    .entry(source)
                    .or_default()
                    .entry(field)
                    .or_insert(value);
            } else if let Some((source, field, value)) =
                normalized_program_equality(right, left, binding, policy)?
            {
                equalities
                    .entry(source)
                    .or_default()
                    .entry(field)
                    .or_insert(value);
            }
        }
        NormalizedPredicateExpr::ArrayContains { .. }
        | NormalizedPredicateExpr::Compare { .. }
        | NormalizedPredicateExpr::EnumMatch { .. }
        | NormalizedPredicateExpr::False
        | NormalizedPredicateExpr::In { .. }
        | NormalizedPredicateExpr::IsNotNull(_)
        | NormalizedPredicateExpr::IsNull(_)
        | NormalizedPredicateExpr::Not(_)
        | NormalizedPredicateExpr::TextContains { .. } => {}
        // Do not descend into a disjunction: an equality within only one
        // branch is not a restriction on the other branch. An enclosing AND
        // may still contribute an independent safe equality.
        NormalizedPredicateExpr::Or(_) => {}
        NormalizedPredicateExpr::True => {}
    }
    Ok(())
}

fn normalized_program_equality(
    field: &NormalizedValueRef,
    value: &NormalizedValueRef,
    binding: &ProgramBinding,
    policy: &PolicyContext,
) -> Result<Option<(SourceId, String, Value)>, Error> {
    let Some((source, field)) = normalized_program_source_field(field) else {
        return Ok(None);
    };
    let Some(value) = normalized_bound_value(value, binding, policy)? else {
        return Ok(None);
    };
    if matches!(value, Value::Nullable(_)) {
        // The physical current index stores its own nullable envelope.  Do
        // not manufacture a nested nullable prefix for a nullable/missing
        // claim; retain the ordinary scan and predicate instead.
        return Ok(None);
    }
    Ok(Some((source, field, value)))
}

fn normalized_program_source_field(value: &NormalizedValueRef) -> Option<(SourceId, String)> {
    match value {
        NormalizedValueRef::SourceField { source, field } => Some((source.clone(), field.clone())),
        NormalizedValueRef::RowId(RowIdRef::Source(source)) => {
            Some((source.clone(), "id".to_owned()))
        }
        _ => None,
    }
}

fn normalized_bound_value(
    value: &NormalizedValueRef,
    binding: &ProgramBinding,
    policy: &PolicyContext,
) -> Result<Option<Value>, Error> {
    match value {
        NormalizedValueRef::Param(name) => Ok(binding.values.get(name).cloned()),
        NormalizedValueRef::Literal(bytes) => postcard::from_bytes::<Value>(bytes)
            .map(Some)
            .map_err(|err| Error::QueryLowering(format!("literal decoding failed: {err}"))),
        // Access-path selection is optional.  System reads have no identity
        // from which to bind a claim, so they retain the ordinary scan rather
        // than failing while considering this candidate optimization.
        NormalizedValueRef::Claim(_) if matches!(policy, PolicyContext::System) => Ok(None),
        NormalizedValueRef::Claim(path) => prepared_claim_value(path, policy),
        _ => Ok(None),
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
                ("$createdBy".to_owned(), ValueType::String),
                ("$createdAt".to_owned(), ValueType::U64),
                ("$updatedBy".to_owned(), ValueType::String),
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
        values.push(Value::String(provenance.created_by.canonical().to_owned()));
        values.push(Value::U64(provenance.created_at));
        values.push(Value::String(provenance.updated_by.canonical().to_owned()));
        values.push(Value::U64(provenance.updated_at));
    } else {
        values.push(Value::String(AuthorSubject::SYSTEM.canonical().to_owned()));
        values.push(Value::U64(0));
        values.push(Value::String(AuthorSubject::SYSTEM.canonical().to_owned()));
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
    inline_current_graph_with_source_metadata_and_branch_witness(
        table,
        rows,
        schema_version_alias,
        coverage,
        requirements,
        None,
    )
}

#[cfg(test)]
pub(super) fn inline_current_graph_with_source_metadata_for_test(
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
    inline_current_graph_with_source_metadata(
        table,
        rows,
        schema_version_alias,
        coverage,
        requirements,
    )
}

fn inline_current_graph_with_source_metadata_and_branch_witness(
    table: &TableSchema,
    rows: Vec<CurrentRow>,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    requirements: &SourceRequirements,
    branch_witness: Option<(&str, &BranchKey)>,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let mut metadata = BTreeMap::new();
    // Provenance is carried by the same content-version witness as ordinary
    // table sources.  Inline candidates must expose that full capability too:
    // a provenance-only requirement still needs the hidden version fields the
    // query program uses to prove and evaluate the source.
    let needs_version_witnesses = requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
        || requirements
            .metadata
            .iter()
            .any(|requirement| matches!(requirement, SourceMetadataRequirement::Provenance(_)));
    if needs_version_witnesses {
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: branch_witness.map(|(field, _)| field.to_owned()),
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

    let descriptor = current_row_descriptor_with_hidden_source_fields_for_branch(
        table,
        &metadata,
        branch_witness.is_some(),
    );
    let records = rows
        .iter()
        .map(|row| {
            inline_current_record_with_source_metadata(
                table,
                &descriptor,
                row,
                schema_version_alias,
                coverage,
                branch_witness,
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
    branch_witness: Option<(&str, &BranchKey)>,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::new();
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        let value = row.cell(table, &column.name);
        if branch_witness.is_some() && table.branch_by.contains(&column.name) {
            values.push(value.ok_or(Error::InvalidStoredValue(
                "frozen branch row is missing a branch column value",
            ))?);
        } else {
            values.push(Value::Nullable(value.map(Box::new)));
        }
    }
    let provenance = row.provenance()?.unwrap_or(RowProvenance {
        created_by: AuthorSubject::SYSTEM,
        created_at: 0,
        updated_by: AuthorSubject::SYSTEM,
        updated_at: 0,
    });
    values.extend([
        Value::String(provenance.created_by.canonical().to_owned()),
        Value::U64(provenance.created_at),
        Value::String(provenance.updated_by.canonical().to_owned()),
        Value::U64(provenance.updated_at),
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
            Value::Nullable(None),
            Value::String(provenance.created_by.canonical().to_owned()),
            Value::U64(provenance.created_at),
            Value::String(provenance.updated_by.canonical().to_owned()),
            Value::U64(provenance.updated_at),
        ]);
    }
    if let Some((_, branch_key)) = branch_witness {
        values.push(Value::Bytes(branch_key.canonical_bytes()));
    }
    if descriptor.field_index("coverage").is_some() {
        values.push(Value::String(coverage.to_owned()));
    }
    if descriptor.field_index("settle_position").is_some() {
        values.push(Value::Nullable(None));
    }
    Ok(descriptor.create(&values)?)
}

fn inline_snapshot_include_deleted_current_graph(
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
            values.push(Value::String(provenance.created_by.canonical().to_owned()));
            values.push(Value::U64(provenance.created_at));
            values.push(Value::String(provenance.updated_by.canonical().to_owned()));
            values.push(Value::U64(provenance.updated_at));
        } else {
            values.push(Value::String(AuthorSubject::SYSTEM.canonical().to_owned()));
            values.push(Value::U64(0));
            values.push(Value::String(AuthorSubject::SYSTEM.canonical().to_owned()));
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
                ("$createdBy".to_owned(), ValueType::String),
                ("$createdAt".to_owned(), ValueType::U64),
                ("$updatedBy".to_owned(), ValueType::String),
                ("$updatedAt".to_owned(), ValueType::U64),
                ("tx_time".to_owned(), ValueType::U64),
                ("tx_node_id".to_owned(), ValueType::U64),
            ])
            .chain([("__jazz_deleted".to_owned(), ValueType::Bool)]),
    )
}

fn include_deleted_branch_graph(
    table: &TableSchema,
    head: &BranchKey,
    content: GraphBuilder,
    deletions: GraphBuilder,
) -> Result<GraphBuilder, Error> {
    let content = content
        .project_fields(branch_view_storage_source_fields(table, head)?)
        .project_fields(storage_to_canonical_current_source_fields(
            table, false, false,
        ));
    let deleted_winners = deletions
        .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
        .project_fields([
            ProjectField::named("row_uuid"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
        ]);
    let undeleted = GraphBuilder::anti_join(
        content.clone(),
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
    let deleted = GraphBuilder::join(content, deleted_winners, ["row_uuid"], ["row_uuid"])
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
    Ok(GraphBuilder::union([undeleted, deleted]))
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

    /// This is an internal planner assertion because the fallback is only
    /// observable as the absence of an optional physical access path. A
    /// system query has no identity claims, so considering a claim predicate
    /// must retain its ordinary scan rather than fail the query.
    #[test]
    fn system_context_claim_declines_an_access_path_without_failing() {
        let binding = ProgramBinding {
            id: BindingId(uuid::Uuid::nil()),
            source_shape: None,
            extra_user_params: BTreeMap::new(),
            param_types: BTreeMap::new(),
            claim_params: BTreeMap::new(),
            values: BTreeMap::new(),
        };

        assert_eq!(
            normalized_bound_value(
                &NormalizedValueRef::Claim(ClaimPath(vec!["sub".to_owned()])),
                &binding,
                &PolicyContext::System,
            )
            .unwrap(),
            None,
        );
    }
}

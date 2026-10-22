//! Bounded history source preparation. Query operators and result materialization
//! belong to the ordinary relational pipeline, not to this source adapter.

use super::*;
use crate::node::query_engine::AuthorizedDeletionPreimage;
use crate::schema::history::HistoryMetadataColumn;

pub(super) fn unsupported(pattern: &str) -> Error {
    QueryError::UnsupportedRelationQuery(format!(
        "Reading {pattern} is not supported in history queries yet"
    ))
    .into()
}

impl<S: OrderedKvStorage> JazzSourceGraphPreparer<'_, S> {
    pub(super) async fn prepare_history_source_graph(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, Error> {
        let schema_version = self.read_view.read_schema;
        let table = self
            .node
            .table_in_schema(&request.source.table, schema_version)?;
        let base_name = crate::schema::history::history_source_table(&request.source.table)
            .ok_or_else(|| unsupported("unknown history source"))?;
        let base_table = self.node.table_in_schema(base_name, schema_version)?;
        if let Some(column) = read_sources::requested_deletion_column(request)
            && request.visibility != RowVisibility::IncludeDeleted
        {
            return Err(QueryError::UnsupportedRelationQuery(format!(
                "Reading {column} requires includeDeleted"
            ))
            .into());
        }
        let path = self
            .access_paths
            .get(&request.source)
            .ok_or_else(|| unsupported("history without a bounded row access path"))?;
        // The current history storage index is bounded by row UUID. Keep this
        // restriction here, rather than in the shared lookup representation.
        let keys = match path {
            SourceAccessPath::PrimaryKey(values) => read_sources::primary_key_rows(values)?,
            SourceAccessPath::Lookup(lookup) if lookup.fields == ["row_uuid"] => {
                lookup.keys.clone()
            }
            _ => return Err(unsupported("history without a bounded row access path")),
        };
        // ClientLocal lowering supplies System, just as for ordinary local
        // sources: retained data is not filtered by client-side policy checks.
        // Trusted serving sources retain their PolicyFiltered identity.
        let identity = match &request.authorization {
            SourceAuthorizationRequest::System => AuthorSubject::SYSTEM,
            SourceAuthorizationRequest::PolicyFiltered {
                permission_subject, ..
            } => *permission_subject,
            _ => return Err(unsupported("history as policy evidence")),
        };
        let branch = match self.read_view.sources.get(&request.source) {
            Some(SourceExpr::BranchView {
                head, base: None, ..
            }) => head.clone(),
            Some(SourceExpr::BranchView { base: Some(_), .. }) => {
                return Err(unsupported("history with a branch base"));
            }
            _ => BranchKey::default(),
        };
        let branch_witness = !base_table.branch_by.is_empty();
        let local_first = matches!(self.read_view.sources.get(&request.source),
            Some(SourceExpr::WithOverlays { overlays, .. })
                if overlays.entries == [OverlayRef::PendingLocal]);
        // Once upstream coverage is installed, history uses its admitted
        // versions just like ordinary sources. No pending history is exposed.
        let covered = self.covered_input_sources.get(&request.source).cloned();
        let authorized = if let Some(covered) = &covered {
            // Covered reads consume only the exact authority-admitted versions.
            // Their row IDs bound physical access; the version join below
            // excludes other history this client happens to retain.
            covered.clone().project(["row_uuid"])
        } else {
            let tier = if local_first {
                DurabilityTier::Local
            } else {
                self.read_view
                    .source_current_tier(&request.source)
                    .ok_or_else(|| unsupported("history at a snapshot cut"))?
            };

            let schema = &self
                .node
                .catalogue
                .catalogue_schemas
                .get(&schema_version)
                .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?
                .schema;
            let visibility = crate::query::Query::from(base_name)
                .select(["id"])
                .validate_with_schema_version(schema, schema_version)?;
            let binding = visibility.bind(BTreeMap::new())?;
            // Compile the ordinary authorized point read into the source graph. No
            // query is executed while preparing history, and permission changes
            // remain dependencies of the resulting graph.
            let mut visibility_request = self
                .node
                .current_query_program_request_with_inline_binding_source(
                    &visibility,
                    &binding,
                    tier,
                    identity,
                    CurrentQueryProgramOutput::AuthorizedRows,
                    &ReadViewSpec::default(),
                    QueryAuthorizationMode::TrustedServing,
                )?;
            // A branch read authorizes against the branch's current rows, so
            // the policy's evidence (`allowedTo.read`, `exists`) over other
            // branched tables must read that branch too, not the default key.
            // The root then reuses its already-resolved branch expression.
            if let Some(source @ SourceExpr::BranchView { .. }) =
                self.read_view.sources.get(&request.source)
            {
                let head_values = self
                    .read_view
                    .sources
                    .values()
                    .filter_map(|source| match source {
                        SourceExpr::BranchView { head, .. } => Some(head.values.iter().cloned()),
                        _ => None,
                    })
                    .flatten()
                    .collect::<BTreeMap<_, _>>();
                branch_read_view_from_heads(
                    &mut visibility_request.reads.primary,
                    schema,
                    &head_values,
                    tier,
                )?;
                visibility_request
                    .reads
                    .primary
                    .sources
                    .insert(root_source_id(base_name), source.clone());
            }
            // Reuse ordinary deleted-row visibility and policy evaluation for the
            // current-row gate; historical content eligibility remains unchanged.
            if request.visibility == RowVisibility::IncludeDeleted {
                visibility_request.input.shape = self
                    .node
                    .normalized_include_deleted_row_set_shape(&visibility, &binding)?;
            }
            let visibility_program =
                Box::pin(self.node.compile_query_program_request_with_access_paths(
                    visibility_request,
                    BTreeMap::from([(
                        root_source_id(base_name),
                        SourceAccessPath::Lookup(SourceLookup {
                            fields: vec!["row_uuid".into()],
                            keys: keys.clone(),
                        }),
                    )]),
                ))
                .await?;
            self.nested_program_dependencies.tables.extend(
                visibility_program
                    .lowered
                    .targeted_refresh_tables
                    .iter()
                    .cloned(),
            );
            self.nested_program_dependencies.uncertain |=
                visibility_program.lowered.targeted_refresh_uncertain;
            lowering::lowered_terminal_graph(&visibility_program, "policy.authorized_rows")?
                .project(["row_uuid"])
        };
        // The fixed branch prefix plus the live authorized row IDs form the
        // physical history lookup. No whole history scan enters the graph.
        let history_source = self.node.physical_history_branch_source_graph(
            schema_version,
            base_name,
            if covered.is_some() && branch_witness {
                None
            } else {
                Some(&branch)
            },
        )?;
        // Covered inputs carry exact supplying branches. Probe those branch/row
        // prefixes directly; a remote receiver must not reopen sibling branches.
        let (authorized, lookup_fields) = if let Some(covered) = &covered
            && branch_witness
        {
            (
                covered.clone().project_fields([
                    ProjectField::renamed("supplying_branch_key", "branch_key"),
                    ProjectField::named("row_uuid"),
                ]),
                vec!["branch_key", "row_uuid"],
            )
        } else {
            (authorized, vec!["row_uuid"])
        };
        let history = GraphBuilder::lookup(history_source, authorized, lookup_fields)
            .ok_or(Error::InvalidStoredValue(
                "history lookup requires a physical source",
            ))?
            .filter(PredicateExpr::gt("seq", Value::U64(0)));
        let history_descriptor = self.node.database.graph_output_descriptor(&history)?;
        let history_fields = descriptor_field_names(&history_descriptor)?;
        let history = if let Some(covered) = &covered {
            let mut left_keys = vec!["row_uuid", "tx_time", "tx_node_id"];
            let mut right_keys = left_keys.clone();
            if branch_witness {
                left_keys.push("branch_key");
                right_keys.push("supplying_branch_key");
            }
            GraphBuilder::join(
                history,
                covered.clone().project(right_keys.clone()),
                left_keys,
                right_keys,
            )
            .project_fields(
                history_fields
                    .iter()
                    .map(|name| ProjectField::renamed(format!("left.{name}"), name)),
            )
        } else {
            history
        };
        let history = history.filter(self.content_version_predicate(schema_version, &base_table)?);

        // Metadata lookups are keyed by versions already selected under the
        // exact row prefix; neither transaction nor node tables are scanned.
        let transactions = GraphBuilder::lookup(
            GraphBuilder::table("jazz_transactions"),
            history.clone().project(["tx_time", "tx_node_id"]),
            ["tx_time", "tx_node_id"],
        )
        .ok_or(Error::InvalidStoredValue(
            "transaction lookup requires a table source",
        ))?;
        let with_author = GraphBuilder::join(
            history,
            transactions,
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            history_fields
                .iter()
                .map(|name| ProjectField::renamed(format!("left.{name}"), name))
                .chain([ProjectField::renamed("right.made_by", "transaction_author")]),
        );
        let nodes = GraphBuilder::lookup(
            GraphBuilder::table("jazz_nodes"),
            with_author.clone().project(["tx_node_id"]),
            ["tx_node_id"],
        )
        .ok_or(Error::InvalidStoredValue(
            "node lookup requires a table source",
        ))?;
        let source = GraphBuilder::join(with_author, nodes, ["tx_node_id"], ["id"]).project_fields(
            history_fields
                .iter()
                .map(|name| ProjectField::renamed(format!("left.{name}"), name))
                .chain([
                    ProjectField::renamed("left.transaction_author", "transaction_author"),
                    ProjectField::renamed("right.uuid", "transaction_node"),
                ]),
        );
        let needs_version_witnesses = request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionWitnesses);
        let needs_settle_position = request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::SettlePosition);
        let projection = |stored_author: bool| {
            let mut fields = vec![ProjectField::named("row_uuid")];
            fields.extend(base_table.columns.iter().map(|column| {
                ProjectField::named_with_identity(
                    user_column_field(&column.name),
                    records::FieldIdentity::Name(column.name.clone()),
                )
            }));
            fields.extend(HistoryMetadataColumn::ALL.map(|column| {
                let expression = match column {
                    HistoryMetadataColumn::TransactionId => groove::ivm::ProjectExpr::Tuple(vec![
                        groove::ivm::FieldRef::name("tx_time"),
                        groove::ivm::FieldRef::name("transaction_node"),
                    ]),
                    HistoryMetadataColumn::AcceptedAt => {
                        groove::ivm::ProjectExpr::Field(groove::ivm::FieldRef::name("seq"))
                    }
                };
                ProjectField {
                    expression,
                    output_name: user_column_field(column.name()),
                    output_identity: records::FieldIdentity::Name(column.name().to_owned()),
                }
            }));
            // History stores provenance as HLCs. Expose physical milliseconds,
            // matching ordinary source provenance before predicates and ordering.
            fields.extend([
                ProjectField::renamed("created_by", "$createdBy"),
                ProjectField {
                    expression: groove::ivm::ProjectExpr::U64ShiftRight {
                        source: groove::ivm::FieldRef::name("created_at"),
                        bits: crate::time::HLC_COUNTER_BITS as u32,
                    },
                    output_name: "$createdAt".to_owned(),
                    output_identity: records::FieldIdentity::Name("$createdAt".to_owned()),
                },
                ProjectField::renamed(
                    if stored_author {
                        "updated_by"
                    } else {
                        "transaction_author"
                    },
                    "$updatedBy",
                ),
                ProjectField {
                    expression: groove::ivm::ProjectExpr::U64ShiftRight {
                        source: groove::ivm::FieldRef::name("updated_at"),
                        bits: crate::time::HLC_COUNTER_BITS as u32,
                    },
                    output_name: "$updatedAt".to_owned(),
                    output_identity: records::FieldIdentity::Name("$updatedAt".to_owned()),
                },
                ProjectField::named("tx_time"),
                ProjectField::named("tx_node_id"),
                ProjectField::named("transaction_node"),
            ]);
            if needs_version_witnesses {
                if branch_witness {
                    fields.push(ProjectField::named("branch_key"));
                }
                fields.extend([
                    ProjectField::named("schema_version"),
                    ProjectField::named("authored_columns"),
                    ProjectField::named("created_by"),
                    ProjectField::named("created_at"),
                    ProjectField::renamed(
                        if stored_author {
                            "updated_by"
                        } else {
                            "transaction_author"
                        },
                        "updated_by",
                    ),
                    ProjectField::named("updated_at"),
                ]);
            }
            if needs_settle_position {
                fields.push(ProjectField::nullable("seq", "settle_position"));
            }
            fields
        };
        let graph = GraphBuilder::union([
            source
                .clone()
                .filter(PredicateExpr::is_not_null("updated_by"))
                .unwrap_nullable("updated_by")
                .project_fields(projection(true)),
            source
                .filter(PredicateExpr::is_null("updated_by"))
                .project_fields(projection(false)),
        ]);
        let (graph, deletion_register, authorized_deletion_preimage) =
            if read_sources::requested_deletion_column(request).is_some() {
                // A historical version's provenance cannot supply deletion metadata, because
                // deletion state depends on the current state of the row.
                // There are two sources for that state:
                // - With upstream coverage: this information is already available
                // - Without coverage: prepare an ordinary include-deleted current source,
                //   restricted to the history query's row IDs and read view. Restrict those rows
                //   again to the history graph's already-authorized rows.
                let (deleted, register, preimage) = if let Some(covered) = &covered {
                    (
                        covered
                            .clone()
                            .filter(PredicateExpr::eq("covered_is_current", Value::Bool(true)))
                            .filter(PredicateExpr::eq("__jazz_deleted", Value::Bool(true))),
                        None,
                        None,
                    )
                } else {
                    // Resolve the bounded current view through the ordinary source path.
                    // History's existing authorization gate still limits the rows below.
                    let source_id = root_source_id(base_name);
                    let mut read_view = self.read_view.clone();
                    read_view.sources.insert(
                        source_id.clone(),
                        self.read_view.sources[&request.source].clone(),
                    );
                    let current_request = SourceRequest {
                        source: source_id.clone(),
                        visibility: RowVisibility::IncludeDeleted,
                        authorization: SourceAuthorizationRequest::System,
                        requirements: SourceRequirements {
                            app_fields: FieldRequirement::None,
                            magic_fields: BTreeSet::new(),
                            metadata: BTreeSet::new(),
                        },
                    };
                    let mut preparer = JazzSourceGraphPreparer {
                        node: self.node,
                        read_view: &read_view,
                        local_unavailable_scope: self.local_unavailable_scope.clone(),
                        inline_sources: BTreeMap::new(),
                        covered_input_sources: BTreeMap::new(),
                        covered_input_descriptors: BTreeMap::new(),
                        access_paths: BTreeMap::from([(
                            source_id,
                            SourceAccessPath::Lookup(SourceLookup {
                                fields: vec!["row_uuid".to_owned()],
                                keys,
                            }),
                        )]),
                        bounded_deletion_register: None,
                        count_access_path_metrics: false,
                        current_projection_targets: BTreeMap::new(),
                        nested_program_dependencies: Default::default(),
                        policy_subplan: false,
                        transaction_overlay: Default::default(),
                    };
                    let current = preparer.prepare_source_graph(&current_request).await?;
                    let deleted = GraphBuilder::semi_join(
                        current
                            .graph
                            .filter(PredicateExpr::eq("__jazz_deleted", Value::Bool(true))),
                        graph.clone().project(["row_uuid"]),
                        ["row_uuid"],
                        ["row_uuid"],
                    );
                    let tier = self
                        .read_view
                        .source_current_tier(&request.source)
                        .unwrap_or(DurabilityTier::Local);
                    let (_, register) = preparer
                        .live_branch_view_registers(
                            &current_request,
                            &base_table,
                            tier,
                            &branch,
                            None,
                        )
                        .await?;
                    (
                        deleted.clone(),
                        Some(DeletionRegisterSource {
                            graph: register,
                            row_uuid_field: "row_uuid".to_owned(),
                        }),
                        Some(AuthorizedDeletionPreimage {
                            graph: deleted,
                            routing_fields: BTreeSet::new(),
                        }),
                    )
                };
                let descriptor = self.node.database.graph_output_descriptor(&graph)?;
                let fields = descriptor.fields();
                let deleted = deleted.project(["row_uuid", "$updatedAt", "$updatedBy"]);
                let live = GraphBuilder::anti_join(
                    graph.clone(),
                    deleted.clone(),
                    ["row_uuid"],
                    ["row_uuid"],
                )
                .project_fields(
                    fields
                        .iter()
                        .map(|field| {
                            ProjectField::named_with_identity(
                                field.name.clone().expect("named history field"),
                                field.identity.clone().expect("history identity"),
                            )
                        })
                        .chain(read_sources::deletion_metadata_projections(request, None)),
                );
                let deleted = GraphBuilder::join(graph, deleted, ["row_uuid"], ["row_uuid"])
                    .project_fields(
                        fields
                            .iter()
                            .map(|field| {
                                ProjectField::renamed_with_identity(
                                    format!(
                                        "left.{}",
                                        field.name.as_ref().expect("named history field")
                                    ),
                                    field.name.clone().expect("named history field"),
                                    field.identity.clone().expect("history identity"),
                                )
                            })
                            .chain(read_sources::deletion_metadata_projections(
                                request,
                                Some("right."),
                            )),
                    );
                (GraphBuilder::union([live, deleted]), register, preimage)
            } else {
                (graph, None, None)
            };
        let descriptor = self.node.database.graph_output_descriptor(&graph)?;
        let mut metadata = BTreeMap::new();
        if needs_version_witnesses {
            metadata.insert(
                SourceMetadataRequirement::VersionWitnesses,
                SourceMetadataFields::VersionWitnesses {
                    schema_version_field: "schema_version".to_owned(),
                    tx_time_field: "tx_time".to_owned(),
                    tx_node_field: "tx_node_id".to_owned(),
                    branch_or_prefix_field: branch_witness.then(|| "branch_key".to_owned()),
                },
            );
        }
        if needs_settle_position {
            metadata.insert(
                SourceMetadataRequirement::SettlePosition,
                SourceMetadataFields::SettlePosition {
                    settle_position_field: "settle_position".to_owned(),
                },
            );
        }
        let (graph, descriptor, metadata) = read_sources::with_requested_author_paths(
            graph,
            descriptor,
            metadata,
            &request.requirements,
        )?;
        Ok(ResolvedSource {
            stored_column_ids: self.stored_column_ids_for_read_table(request, &base_table)?,
            table_schema: table.into(),
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                occurrence_key: crate::node::query_engine::SourceOccurrenceKey::History {
                    row: "row_uuid".to_owned(),
                    time: "tx_time".to_owned(),
                    node: "transaction_node".to_owned(),
                },
                metadata,
            },
            routing_fields: BTreeSet::new(),
            requires_result_payload: false,
            content_version: None,
            deletion_register,
            authorized_deletion_preimage,
        })
    }

    /// Keep content images while excluding deletion-only writes, including
    /// branch coordinates automatically authored with a deletion event.
    fn content_version_predicate(
        &self,
        schema_version: SchemaVersionId,
        table: &TableSchema,
    ) -> Result<PredicateExpr, Error> {
        // Branch coordinates are automatically authored even by deletion-only
        // writes. They identify the image; they do not make it a content version.
        let mut deletion_only_sets = vec![BTreeSet::from([DELETION_COLUMN_ID])];
        if !table.branch_by.is_empty() {
            let mut names = table.branch_by.iter().cloned().collect::<BTreeSet<_>>();
            names.insert(DELETION_COLUMN_NAME.to_owned());
            if let Some(ids) = self.node.authored_column_ids_for_names(
                schema_version,
                &table.name,
                Some(&names),
            )? {
                deletion_only_sets.push(ids);
            }
        }
        // Complete received images may omit authored-column metadata.
        Ok(PredicateExpr::Or(vec![
            PredicateExpr::is_null("authored_columns"),
            PredicateExpr::And(
                deletion_only_sets
                    .into_iter()
                    .map(|ids| PredicateExpr::Neq {
                        field: "authored_columns".to_owned(),
                        value: Value::Nullable(Some(Box::new(Value::Array(
                            ids.into_iter().map(|id| Value::U64(id.0)).collect(),
                        ))))
                        .into(),
                    })
                    .collect(),
            ),
        ]))
    }
}

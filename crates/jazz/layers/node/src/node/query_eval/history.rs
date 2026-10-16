//! Bounded history source preparation. Query operators and result materialization
//! belong to the ordinary relational pipeline, not to this source adapter.

use super::*;
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
        if schema_version != self.node.catalogue.local_schema_version_id {
            return Err(unsupported("history through a different read schema"));
        }
        let table = self
            .node
            .table_in_schema(&request.source.table, schema_version)?;
        let base_name = request
            .source
            .table
            .strip_prefix(crate::schema::history::HISTORY_PREFIX)
            .ok_or_else(|| unsupported("unknown history source"))?;
        let base_table = self.node.table_in_schema(base_name, schema_version)?;
        let Some(CurrentAccessPath::PrimaryKey(prefix)) = self.access_paths.get(&request.source)
        else {
            return Err(unsupported("history without an exact row access path"));
        };
        let [Value::Uuid(id)] = prefix.as_slice() else {
            return Err(unsupported("a history row id that is not a bound UUID"));
        };
        let row = RowUuid(*id);
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
        let tier = self
            .read_view
            .source_current_tier(&request.source)
            .ok_or_else(|| unsupported("history at a snapshot cut"))?;
        let schema = &self
            .node
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?
            .schema;
        let visibility = crate::query::Query::from(base_name)
            .filter(crate::query::eq(
                crate::query::col("id"),
                crate::query::lit(Value::Uuid(row.0)),
            ))
            .select(["id"])
            .validate_with_schema_version(schema, schema_version)?;
        let binding = visibility.bind(BTreeMap::new())?;
        // Compile the ordinary authorized point read into the source graph. No
        // query is executed while preparing history, and permission changes
        // remain dependencies of the resulting graph.
        let visibility_request = self
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
        let visibility_program =
            Box::pin(self.node.compile_query_program_request_with_access_paths(
                visibility_request,
                BTreeMap::from([(
                    root_source_id(base_name),
                    CurrentAccessPath::PrimaryKey(vec![Value::Uuid(row.0)]),
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
        let authorized =
            lowering::lowered_terminal_graph(&visibility_program, "policy.authorized_rows")?
                .project(["row_uuid"]);
        let history = self.node.physical_history_row_source_graph(
            schema_version,
            base_name,
            &BranchKey::default(),
            row,
        )?;
        let history = GraphBuilder::semi_join(history, authorized, ["row_uuid"], ["row_uuid"])
            .filter(PredicateExpr::gt("seq", Value::U64(0)));
        let history_descriptor = self.node.database.graph_output_descriptor(&history)?;
        let history_fields = descriptor_field_names(&history_descriptor)?;
        // Complete received images may omit authored-column metadata. Preserve
        // that representation; only an explicit deletion-only image is excluded.
        let history = history.filter(PredicateExpr::Or(vec![
            PredicateExpr::is_null("authored_columns"),
            PredicateExpr::Neq {
                field: "authored_columns".to_owned(),
                value: Value::Nullable(Some(Box::new(Value::Array(vec![Value::U64(
                    DELETION_COLUMN_ID.0,
                )]))))
                .into(),
            },
        ]));

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
            fields.extend([
                ProjectField::renamed("created_by", "$createdBy"),
                ProjectField::renamed("created_at", "$createdAt"),
                ProjectField::renamed(
                    if stored_author {
                        "updated_by"
                    } else {
                        "transaction_author"
                    },
                    "$updatedBy",
                ),
                ProjectField::renamed("updated_at", "$updatedAt"),
                ProjectField::named("tx_time"),
                ProjectField::named("tx_node_id"),
                ProjectField::named("transaction_node"),
            ]);
            if needs_version_witnesses {
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
        let descriptor = self.node.database.graph_output_descriptor(&graph)?;
        let mut metadata = BTreeMap::new();
        if needs_version_witnesses {
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
            metadata.insert(
                SourceMetadataRequirement::SettlePosition,
                SourceMetadataFields::SettlePosition {
                    settle_position_field: "settle_position".to_owned(),
                },
            );
        }
        Ok(ResolvedSource {
            stored_column_ids: BTreeMap::new(),
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
            deletion_register: None,
            authorized_deletion_preimage: None,
        })
    }
}

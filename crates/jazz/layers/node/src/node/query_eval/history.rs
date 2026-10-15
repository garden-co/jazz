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
        let visible = Box::pin(self.node.query_row_visibility_for_link_physical_row(
            &visibility,
            &binding,
            tier,
            identity,
            row,
        ))
        .await?;
        let descriptor = crate::node::codec::current_row_descriptor(&table);
        let mut rows = Vec::new();
        if !visible.is_empty() {
            for version in self.node.query_row_versions(base_name, row).await? {
                let seq = version.seq()?;
                if seq == GlobalTime(0) {
                    continue;
                }
                if self
                    .node
                    .schema_version_for_alias(version.schema_version_alias())
                    != Some(schema_version)
                {
                    return Err(unsupported(
                        "historical versions authored in a different schema",
                    ));
                }
                let authored = version.authored_column_ids()?.ok_or_else(|| {
                    unsupported("versions without exact authored-column metadata")
                })?;
                if authored.contains(&DELETION_COLUMN_ID) && authored.len() == 1 {
                    continue;
                }
                let tx = self.node.version_tx_id(&version)?;
                let mut values = crate::node::codec::current_row_prefix_and_cells_from_version(
                    &base_table,
                    &version,
                )?;
                values.extend(HistoryMetadataColumn::ALL.map(|column| {
                    let value = match column {
                        HistoryMetadataColumn::TransactionId => tx_id_value(tx),
                        HistoryMetadataColumn::Sequence => Value::U64(seq.0),
                    };
                    Value::Nullable(Some(Box::new(value)))
                }));
                crate::node::codec::append_current_row_provenance(&mut values, &version);
                let raw = descriptor.create(&values)?;
                rows.push(CurrentRow::new(
                    table.name.clone(),
                    OwnedRecord::new(raw, descriptor),
                ));
            }
        }
        let alias = self
            .node
            .ensure_schema_version_alias(schema_version)
            .await?;
        let (graph, descriptor, metadata) =
            read_sources::inline_current_graph_with_source_metadata(
                &table,
                rows,
                alias,
                "history",
                &request.requirements,
            )?;
        Ok(ResolvedSource {
            stored_column_ids: BTreeMap::new(),
            table_schema: table.into(),
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
            authorized_deletion_preimage: None,
        })
    }
}

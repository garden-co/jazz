//! Durable physical identity metadata and Groove history-table lowering.

use super::*;
use crate::ids::{PhysicalColumnId, PhysicalTableId};
use groove::schema::{ColumnSchema as GrooveColumnSchema, TableSchema as GrooveTableSchema};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct SchemaPhysicalMapping {
    pub(super) tables: BTreeMap<String, TablePhysicalMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct TablePhysicalMapping {
    pub(super) table_id: PhysicalTableId,
    pub(super) columns: BTreeMap<String, PhysicalColumnId>,
}

#[derive(Clone, Debug)]
pub(super) struct PhysicalHistoryBinding {
    pub(super) storage_table: String,
    pub(super) descriptor: records::RecordDescriptor,
}

pub(super) fn physical_history_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_history", table_id.0)
}

pub(super) fn physical_register_table_name(table_id: PhysicalTableId) -> String {
    format!("jazz_physical_{}_register", table_id.0)
}

pub(super) fn physical_user_column_field(column_id: PhysicalColumnId) -> String {
    format!("user_{}", column_id.0)
}

pub(super) fn physical_history_projection_target(
    schema_alias: SchemaVersionAlias,
    logical_table: &str,
) -> String {
    format!("schema_{}_{}_history", schema_alias.0, logical_table)
}

pub(super) fn allocate_provisional_physical_mapping(
    schema: &JazzSchema,
    next_table_id: &mut u64,
    next_column_id: &mut u64,
) -> Result<SchemaPhysicalMapping, Error> {
    let mut tables = BTreeMap::new();
    for table in &schema.tables {
        let table_id = PhysicalTableId(*next_table_id);
        *next_table_id = next_table_id
            .checked_add(1)
            .ok_or(Error::InvalidStoredValue("physical table id exhausted"))?;
        let mut columns = BTreeMap::new();
        for column in &table.columns {
            let column_id = PhysicalColumnId(*next_column_id);
            *next_column_id = next_column_id
                .checked_add(1)
                .ok_or(Error::InvalidStoredValue("physical column id exhausted"))?;
            columns.insert(column.name.clone(), column_id);
        }
        tables.insert(
            table.name.clone(),
            TablePhysicalMapping { table_id, columns },
        );
    }
    Ok(SchemaPhysicalMapping { tables })
}

pub(super) fn physical_history_binding(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    schema_version_aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    schema_version: SchemaVersionId,
    logical_table: &str,
) -> Result<PhysicalHistoryBinding, Error> {
    let schema = catalogue_schemas
        .get(&schema_version)
        .ok_or(Error::InvalidStoredValue("physical history schema missing"))?;
    let table = schema
        .schema
        .tables
        .iter()
        .find(|table| table.name == logical_table)
        .ok_or_else(|| Error::TableNotFound(logical_table.to_owned()))?;
    let mapping = physical_mappings
        .get(&schema_version)
        .and_then(|mapping| mapping.tables.get(logical_table))
        .ok_or(Error::InvalidStoredValue(
            "physical history table mapping missing",
        ))?;
    let alias =
        schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical history schema alias missing",
            ))?;
    Ok(PhysicalHistoryBinding {
        storage_table: physical_history_table_name(mapping.table_id),
        descriptor: physical_history_descriptor(table, mapping, alias)?,
    })
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn physical_register_table_for_schema(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
    ) -> Result<String, Error> {
        self.table_in_schema(logical_table, schema_version)?;
        let table_id = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(logical_table))
            .map(|mapping| mapping.table_id)
            .ok_or(Error::InvalidStoredValue(
                "physical register table mapping missing",
            ))?;
        Ok(physical_register_table_name(table_id))
    }

    pub(super) fn physical_history_source_graph(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
    ) -> Result<GraphBuilder, Error> {
        let alias = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical history source schema alias missing",
            ))?;
        let binding = physical_history_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
        )?;
        Ok(GraphBuilder::variant_project(
            binding.storage_table,
            physical_history_projection_target(alias, logical_table),
        ))
    }

    pub(super) fn register_physical_history_variant_projections(&mut self) -> Result<(), Error> {
        let targets = self
            .catalogue
            .physical_mappings
            .iter()
            .flat_map(|(schema_version, mapping)| {
                mapping.tables.iter().map(|(logical_table, table)| {
                    (*schema_version, logical_table.clone(), table.clone())
                })
            })
            .collect::<Vec<_>>();
        for (target_schema, target_table_name, target_mapping) in targets {
            let target_alias = self
                .catalogue
                .schema_version_aliases
                .get(&target_schema)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical projection target schema alias missing",
                ))?;
            let target_table = self.table_in_schema(&target_table_name, target_schema)?;
            let storage_table = physical_history_table_name(target_mapping.table_id);
            let projection_target =
                physical_history_projection_target(target_alias, &target_table_name);
            self.database.define_variant_projection(
                &storage_table,
                &projection_target,
                target_table.history_storage_table().record_schema(),
            )?;

            let sources = self
                .catalogue
                .physical_mappings
                .iter()
                .flat_map(|(schema_version, mapping)| {
                    mapping
                        .tables
                        .iter()
                        .filter(|(_, table)| table.table_id == target_mapping.table_id)
                        .map(|(logical_table, table)| {
                            (*schema_version, logical_table.clone(), table.clone())
                        })
                })
                .collect::<Vec<_>>();
            for (source_schema, source_table_name, source_mapping) in sources {
                let source_alias = self
                    .catalogue
                    .schema_version_aliases
                    .get(&source_schema)
                    .copied()
                    .ok_or(Error::InvalidStoredValue(
                        "physical projection source schema alias missing",
                    ))?;
                let Some(fields) = self.physical_history_projection_case(
                    source_schema,
                    &source_table_name,
                    &source_mapping,
                    target_schema,
                    &target_table_name,
                )?
                else {
                    self.database.register_variant_projection_ignore_case(
                        &storage_table,
                        &projection_target,
                        source_alias.0,
                    )?;
                    continue;
                };
                self.database.register_variant_projection_case(
                    &storage_table,
                    &projection_target,
                    source_alias.0,
                    fields,
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn synchronize_physical_version_tables(&mut self) -> Result<(), Error> {
        for desired in physical_version_storage_tables(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
        )? {
            let existing = match self.database.table_schema(&desired.name) {
                Ok(existing) => Some(existing.clone()),
                Err(GrooveDbError::TableNotFound(_)) => None,
                Err(error) => return Err(error.into()),
            };
            let Some(existing) = existing else {
                self.database.register_table(desired)?;
                continue;
            };
            let added_columns = desired
                .columns
                .iter()
                .filter(|column| {
                    existing
                        .columns
                        .iter()
                        .all(|candidate| candidate.name != column.name)
                })
                .cloned()
                .collect::<Vec<_>>();
            for schema_version in desired.schema_versions {
                if existing.schema_version(schema_version.version).is_some() {
                    continue;
                }
                self.database.register_table_schema_version_with_columns(
                    &desired.name,
                    added_columns.clone(),
                    schema_version,
                )?;
            }
        }
        self.register_physical_history_variant_projections()
    }

    pub(super) fn synchronize_partition_storage_tables(&mut self) -> Result<(), Error> {
        let lowered = self.catalogue.schema.lower_to_groove_with_partitions(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.partitions,
            &self.branches.branch_partitions,
        );
        for table in lowered.tables {
            match self.database.table_schema(&table.name) {
                Ok(_) => {}
                Err(GrooveDbError::TableNotFound(_)) => self.database.register_table(table)?,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub(super) fn discard_unmapped_physical_version_tables(
        &mut self,
        candidates: impl IntoIterator<Item = PhysicalTableId>,
    ) -> Result<(), Error> {
        let live = self
            .catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values().map(|table| table.table_id))
            .collect::<BTreeSet<_>>();
        for table_id in candidates {
            if live.contains(&table_id) {
                continue;
            }
            for table in [
                physical_history_table_name(table_id),
                physical_register_table_name(table_id),
            ] {
                let rows = match self.database.primary_key_scan_raw(&table, &[]) {
                    Ok(rows) => rows
                        .into_iter()
                        .map(|row| row.owned_record())
                        .collect::<Vec<_>>(),
                    Err(GrooveDbError::TableNotFound(_)) => continue,
                    Err(error) => return Err(error.into()),
                };
                if rows.is_empty() {
                    continue;
                }
                let mut batch = self.database.open_batch();
                for row in rows {
                    let record = row.borrowed();
                    batch.delete(
                        &table,
                        PrimaryKeyValue::Composite(vec![
                            PrimaryKeyValue::Uuid(
                                record.get_uuid(HistoryRowRecord::FIELD_ROW_UUID_IDX)?,
                            ),
                            PrimaryKeyValue::U64(
                                record.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?,
                            ),
                            PrimaryKeyValue::U64(
                                record.get_u64(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?,
                            ),
                        ]),
                    );
                }
                self.database.commit_batch(batch)?;
            }
        }
        Ok(())
    }

    fn physical_history_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        #[derive(Clone)]
        enum CellProjection {
            Field(String),
            Literal(Value),
        }

        let source_table = self.table_in_schema(source_table_name, source_schema)?;
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let mut cells = source_table
            .columns
            .iter()
            .map(|column| {
                let column_id = source_mapping.columns.get(&column.name).copied().ok_or(
                    Error::InvalidStoredValue("physical projection column mapping missing"),
                )?;
                Ok((
                    column.name.clone(),
                    CellProjection::Field(physical_user_column_field(column_id)),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        if source_schema != target_schema || source_table_name != target_table_name {
            let mut path = None;
            for direction in [LensPathDirection::Forward, LensPathDirection::Reverse] {
                if let Some(candidate) = self.compiled_lens_path(
                    source_schema,
                    target_schema,
                    direction,
                    source_table_name,
                )? && candidate.target_table == target_table_name
                {
                    path = Some(candidate);
                    break;
                }
            }
            let Some(path) = path else {
                return Ok(None);
            };
            for op in path.ops {
                match op {
                    CompiledLensOp::Rename { from, to } => {
                        if let Some(value) = cells.remove(&from) {
                            cells.insert(to, value);
                        }
                    }
                    CompiledLensOp::Copy { from, to } => {
                        if let Some(value) = cells.get(&from).cloned() {
                            cells.insert(to, value);
                        }
                    }
                    CompiledLensOp::Add { column, default } => {
                        cells
                            .entry(column)
                            .or_insert(CellProjection::Literal(default));
                    }
                    CompiledLensOp::Drop { column } => {
                        cells.remove(&column);
                    }
                }
            }
        }

        let mut fields = target_table
            .history_storage_table()
            .record_schema()
            .fields()
            .iter()
            .take(HistoryRowRecord::USER_CELLS)
            .map(|field| {
                ProjectField::named(
                    field
                        .name
                        .clone()
                        .expect("Jazz history system fields are named"),
                )
            })
            .collect::<Vec<_>>();
        for column in &target_table.columns {
            let output = user_column_field(&column.name);
            let Some(projection) = cells.remove(&column.name) else {
                return Ok(None);
            };
            match projection {
                CellProjection::Field(source) => {
                    fields.push(ProjectField::renamed(source, output));
                }
                CellProjection::Literal(value) => fields.push(ProjectField::literal_typed(
                    output,
                    Value::Nullable(Some(Box::new(value))),
                    records::ValueType::Nullable(Box::new(column.column_type.value_type())),
                )),
            }
        }
        Ok(Some(fields))
    }

    pub(super) fn version_storage_table_for_row(
        &mut self,
        version: &VersionRow,
    ) -> Result<groove::Intern<String>, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing",
            ))?;
        if version.layer() == VersionLayer::Deletion {
            return Ok(groove::Intern::new(
                self.physical_register_table_for_schema(schema_version, version.table())?,
            ));
        }
        Ok(groove::Intern::new(
            physical_history_binding(
                &self.catalogue.catalogue_schemas,
                &self.catalogue.schema_version_aliases,
                &self.catalogue.physical_mappings,
                schema_version,
                version.table(),
            )?
            .storage_table,
        ))
    }

    pub(super) fn version_storage_write_binding(
        &mut self,
        version: &VersionRow,
    ) -> Result<(groove::Intern<String>, groove::records::VersionedRecord), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing",
            ))?;
        if version.layer() == VersionLayer::Deletion {
            let table = self.version_storage_table_for_row(version)?;
            return Ok((table, version.groove_record()));
        }

        let binding = physical_history_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
            schema_version,
            version.table(),
        )?;
        let record = OwnedRecord::new(version.record.raw().to_vec(), binding.descriptor);
        Ok((
            groove::Intern::new(binding.storage_table),
            groove::records::VersionedRecord::new(version.schema_version_alias().0, record),
        ))
    }
}

pub(super) fn physical_version_storage_tables(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    schema_version_aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
) -> Result<Vec<GrooveTableSchema>, Error> {
    let mut lineages = BTreeMap::<
        PhysicalTableId,
        Vec<(SchemaVersionId, &TableSchema, &TablePhysicalMapping)>,
    >::new();
    for (schema_version, mapping) in physical_mappings {
        let schema = catalogue_schemas
            .get(schema_version)
            .ok_or(Error::InvalidStoredValue(
                "physical mapping schema payload missing",
            ))?;
        for (logical_table, table_mapping) in &mapping.tables {
            let table = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == *logical_table)
                .ok_or(Error::InvalidStoredValue(
                    "physical mapping logical table missing",
                ))?;
            lineages.entry(table_mapping.table_id).or_default().push((
                *schema_version,
                table,
                table_mapping,
            ));
        }
    }

    let mut tables = Vec::with_capacity(lineages.len());
    for (table_id, variants) in lineages {
        let (_, template_table, _) = variants
            .first()
            .ok_or(Error::InvalidStoredValue("physical history lineage empty"))?;
        let template = template_table.history_storage_table();
        let system_columns = template
            .columns
            .iter()
            .take(HistoryRowRecord::USER_CELLS)
            .cloned()
            .collect::<Vec<_>>();
        let mut physical_columns = BTreeMap::new();
        for (_, logical_table, mapping) in &variants {
            for column in &logical_table.columns {
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical history column mapping missing",
                        ))?;
                let storage_type = column.column_type.clone().nullable();
                if let Some(existing) = physical_columns.insert(column_id, storage_type.clone())
                    && existing != storage_type
                {
                    return Err(Error::InvalidStoredValue(
                        "physical history column type mismatch",
                    ));
                }
            }
        }
        let columns = system_columns
            .into_iter()
            .chain(
                physical_columns
                    .into_iter()
                    .map(|(column_id, column_type)| {
                        GrooveColumnSchema::new(physical_user_column_field(column_id), column_type)
                    }),
            );
        let mut physical = GrooveTableSchema::new(physical_history_table_name(table_id), columns);
        physical.primary_key = template.primary_key.clone();
        physical.indices = template.indices.clone();
        let mut register = template_table.register_storage_table();
        register.name = physical_register_table_name(table_id);

        let mut layouts_by_alias = BTreeMap::new();
        for (schema_version, logical_table, mapping) in variants {
            let alias = schema_version_aliases.get(&schema_version).copied().ok_or(
                Error::InvalidStoredValue("physical history schema alias missing"),
            )?;
            let fields = physical_history_field_names(logical_table, mapping)?;
            if layouts_by_alias.insert(alias, fields).is_some() {
                return Err(Error::InvalidStoredValue(
                    "schema maps multiple logical tables to one physical lineage",
                ));
            }
        }
        for (alias, fields) in layouts_by_alias {
            physical = physical.with_schema_version(alias.0, fields);
        }
        tables.push(physical);
        tables.push(register);
    }
    Ok(tables)
}

fn physical_history_descriptor(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    _alias: SchemaVersionAlias,
) -> Result<records::RecordDescriptor, Error> {
    let logical_descriptor = table.history_storage_table().record_schema();
    let physical_names = physical_history_field_names(table, mapping)?;
    if logical_descriptor.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical history descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        physical_names.into_iter().zip(
            logical_descriptor
                .fields()
                .iter()
                .map(|field| field.value_type.clone()),
        ),
    ))
}

fn physical_history_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    let logical_descriptor = table.history_storage_table().record_schema();
    let mut fields = logical_descriptor
        .fields()
        .iter()
        .take(HistoryRowRecord::USER_CELLS)
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical history system field unnamed",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for column in &table.columns {
        let column_id =
            mapping
                .columns
                .get(&column.name)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical history column mapping missing",
                ))?;
        fields.push(physical_user_column_field(column_id));
    }
    Ok(fields)
}

pub(super) fn physical_column_epoch_is_compatible(
    source_table: &TableSchema,
    source_column_name: &str,
    target_table: &TableSchema,
    target_column_name: &str,
) -> bool {
    let Some(source_column) = source_table
        .columns
        .iter()
        .find(|column| column.name == source_column_name)
    else {
        return false;
    };
    let Some(target_column) = target_table
        .columns
        .iter()
        .find(|column| column.name == target_column_name)
    else {
        return false;
    };

    source_column.column_type == target_column.column_type
        && source_column.large_value == target_column.large_value
        && source_column.text_merge_spec == target_column.text_merge_spec
        && source_table.merge_strategy(source_column_name)
            == target_table.merge_strategy(target_column_name)
}

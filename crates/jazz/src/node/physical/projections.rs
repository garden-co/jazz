impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn physical_table_id_for_schema(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
    ) -> Result<PhysicalTableId, Error> {
        self.table_in_schema(logical_table, schema_version)?;
        self.catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(logical_table))
            .map(|mapping| mapping.table_id)
            .ok_or(Error::InvalidStoredValue("physical table mapping missing"))
    }

    pub(super) fn physical_table_id_for_version(
        &self,
        version: &VersionRow,
    ) -> Result<PhysicalTableId, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing while resolving physical table",
            ))?;
        self.physical_table_id_for_schema(schema_version, version.table())
    }

    pub(super) fn physical_register_table_for_schema(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
    ) -> Result<String, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, logical_table)?;
        Ok(physical_register_table_name(table_id))
    }

    pub(super) fn physical_current_table_for_schema(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        layer: VersionLayer,
        class: PhysicalCurrentClass,
    ) -> Result<String, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, logical_table)?;
        Ok(match (class, layer) {
            (PhysicalCurrentClass::Global, VersionLayer::Content) => {
                physical_global_current_table_name(table_id)
            }
            (PhysicalCurrentClass::Global, VersionLayer::Deletion) => {
                physical_register_global_current_table_name(table_id)
            }
            (PhysicalCurrentClass::Ahead, VersionLayer::Content) => {
                physical_ahead_current_table_name(table_id)
            }
            (PhysicalCurrentClass::Ahead, VersionLayer::Deletion) => {
                physical_register_ahead_current_table_name(table_id)
            }
        })
    }

    pub(super) fn physical_current_source_graph(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
    ) -> Result<GraphBuilder, Error> {
        let alias = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current source schema alias missing",
            ))?;
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_source_scan(
            binding.storage_table,
            physical_current_projection_target(alias, logical_table),
            shared_branch_scan(None),
        ))
    }

    pub(super) fn physical_current_source_graph_with_projection_target(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
        projection_target: impl Into<String>,
    ) -> Result<GraphBuilder, Error> {
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_source_scan(
            binding.storage_table,
            projection_target,
            shared_branch_scan(None),
        ))
    }

    pub(super) fn physical_current_branch_source_graph_with_projection_target(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
        projection_target: impl Into<String>,
        branch_key: &BranchKey,
    ) -> Result<GraphBuilder, Error> {
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_source_scan(
            binding.storage_table,
            projection_target,
            branch_scan(branch_key, None),
        ))
    }

    pub(super) fn physical_current_source_scan_graph(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
        scan: groove::ivm::StaticScanSpec,
    ) -> Result<GraphBuilder, Error> {
        let alias = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current source schema alias missing",
            ))?;
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_source_scan(
            binding.storage_table,
            physical_current_projection_target(alias, logical_table),
            shared_branch_scan(Some(scan)),
        ))
    }

    pub(super) fn physical_current_source_scan_graph_with_projection_target(
        &self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        class: PhysicalCurrentClass,
        projection_target: impl Into<String>,
        scan: groove::ivm::StaticScanSpec,
    ) -> Result<GraphBuilder, Error> {
        let binding = physical_current_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            logical_table,
            class,
        )?;
        Ok(GraphBuilder::variant_source_scan(
            binding.storage_table,
            projection_target,
            shared_branch_scan(Some(scan)),
        ))
    }

    pub(super) fn logical_table_for_physical_alias(
        &self,
        table_id: PhysicalTableId,
        alias: SchemaVersionAlias,
    ) -> Result<String, Error> {
        let schema_version =
            self.schema_version_for_alias(alias)
                .ok_or(Error::InvalidStoredValue(
                    "physical row schema version alias missing",
                ))?;
        self.catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| {
                mapping.tables.iter().find_map(|(logical_table, mapping)| {
                    (mapping.table_id == table_id).then(|| logical_table.clone())
                })
            })
            .ok_or(Error::InvalidStoredValue(
                "physical row logical table mapping missing",
            ))
    }

    pub(super) fn physical_table_ids(&self) -> BTreeSet<PhysicalTableId> {
        self.catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values().map(|table| table.table_id))
            .collect()
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
        Ok(GraphBuilder::variant_source(
            binding.storage_table,
            physical_history_projection_target(alias, logical_table),
        ))
    }

    pub(super) async fn register_physical_history_variant_projections(
        &mut self,
    ) -> Result<(), Error> {
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
            let projection_target =
                physical_history_projection_target(target_alias, &target_table_name);
            let logical_output = target_table.history_storage_table().record_schema();
            let physical_names = physical_history_field_names(&target_table, &target_mapping)?;
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
            let storage_tables = [physical_history_table_name(target_mapping.table_id)];
            for storage_table in storage_tables {
                let output = widened_projection_descriptor(
                    &logical_output,
                    &physical_names,
                    self.database.table_schema(&storage_table)?,
                )?;
                self.database
                    .define_variant_projection(&storage_table, &projection_target, output)
                    ?;
                for (source_schema, source_table_name, source_mapping) in &sources {
                    let source_alias = self
                        .catalogue
                        .schema_version_aliases
                        .get(source_schema)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical projection source schema alias missing",
                        ))?;
                    let cases = if source_mapping.variant_cases.is_empty() {
                        vec![(groove_variant_tag(source_alias)?, None)]
                    } else {
                        source_mapping
                            .variant_cases
                            .iter()
                            .map(|case| (case.tag, Some(&case.fields)))
                            .collect()
                    };
                    for (tag, present) in cases {
                        let Some(fields) = self.physical_history_projection_case(
                            *source_schema,
                            source_table_name,
                            source_mapping,
                            target_schema,
                            &target_table_name,
                            present,
                        )?
                        else {
                            self.database
                                .register_variant_ignore_case(
                                    &storage_table,
                                    &projection_target,
                                    tag,
                                )?;
                            continue;
                        };
                        self.database
                            .register_variant_case(
                                &storage_table,
                                &projection_target,
                                tag,
                                fields,
                            )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn register_physical_current_variant_projections(
        &mut self,
    ) -> Result<(), Error> {
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
                    "physical current projection target schema alias missing",
                ))?;
            let target_table = self.table_in_schema(&target_table_name, target_schema)?;
            let projection_target =
                physical_current_projection_target(target_alias, &target_table_name);
            let storage_tables = [
                physical_global_current_table_name(target_mapping.table_id),
                physical_ahead_current_table_name(target_mapping.table_id),
            ];
            for storage_table in &storage_tables {
                let logical_output =
                    target_table.global_current_storage_tables()[0].record_schema();
                let physical_names = physical_current_field_names(&target_table, &target_mapping)?;
                let output = widened_projection_descriptor(
                    &logical_output,
                    &physical_names,
                    self.database.table_schema(storage_table)?,
                )?;
                self.database
                    .define_variant_projection(storage_table, &projection_target, output)
                    ?;
            }

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
                        "physical current projection source schema alias missing",
                    ))?;
                let cases = if source_mapping.variant_cases.is_empty() {
                    vec![(groove_variant_tag(source_alias)?, None)]
                } else {
                    source_mapping
                        .variant_cases
                        .iter()
                        .map(|case| (case.tag, Some(&case.fields)))
                        .collect()
                };
                for (tag, present) in cases {
                    let fields = self.physical_current_projection_case(
                        source_schema,
                        &source_table_name,
                        &source_mapping,
                        target_schema,
                        &target_table_name,
                        present,
                    )?;
                    for storage_table in &storage_tables {
                        if let Some(fields) = fields.clone() {
                            self.database
                                .register_variant_case(
                                    storage_table,
                                    &projection_target,
                                    tag,
                                    fields,
                                )?;
                        } else {
                            self.database
                                .register_variant_ignore_case(
                                    storage_table,
                                    &projection_target,
                                    tag,
                                )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Register (or refresh) a current-source projection that decodes only
    /// the enum columns required by one query source.  The fixed output shape
    /// deliberately retains the ordinary logical row descriptor: callers can
    /// share the normal current-source lowering, while enum values outside the
    /// requirement closure are represented by typed nulls and never expose a
    /// physical tag.
    pub(super) fn ensure_physical_current_projection_for_enum_columns(
        &mut self,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        required_fields: &BTreeSet<String>,
    ) -> Result<String, Error> {
        let target_alias = self
            .catalogue
            .schema_version_aliases
            .get(&target_schema)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current projection target schema alias missing",
            ))?;
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "target enum physical mapping missing",
            ))?;
        let required_enum_columns = target_table
            .columns
            .iter()
            .filter(|column| required_fields.contains(&column.name))
            .filter_map(|column| {
                let column_id = target_mapping.columns.get(&column.name).copied()?;
                let has_enum_boundary =
                    physical_mapping_has_enum_boundary(&target_mapping, column_id)
                    || value_type_has_enum_boundary(&column.column_type);
                has_enum_boundary.then_some(column_id)
            })
            .collect::<BTreeSet<_>>();
        let target_has_any_enum_boundary = target_table.columns.iter().any(|column| {
            target_mapping
                .columns
                .get(&column.name)
                .is_some_and(|column_id| {
                    physical_mapping_has_enum_boundary(&target_mapping, *column_id)
                })
                || value_type_has_enum_boundary(&column.column_type)
        });
        // A schema with no enum boundary can use its durable target directly.
        // For an enum-bearing table, an empty requirement set is itself a
        // query-local compatibility boundary: unrequested enum cells must be
        // typed-null before an old reader decodes the row.
        if required_enum_columns.is_empty() && !target_has_any_enum_boundary {
            return Ok(physical_current_projection_target(
                target_alias,
                target_table_name,
            ));
        }
        let projection_target = physical_current_projection_target_for_enum_columns(
            target_alias,
            target_table_name,
            &required_enum_columns,
        );
        let storage_tables = [
            physical_global_current_table_name(target_mapping.table_id),
            physical_ahead_current_table_name(target_mapping.table_id),
        ];
        for storage_table in &storage_tables {
            let logical_output = target_table.global_current_storage_tables()[0].record_schema();
            // This query-local target is the semantic read boundary. Unlike
            // the durable all-fields storage target, it must expose the
            // authored descriptor itself: enum tags are translated into that
            // descriptor, and an absent target case excludes only this row.
            let output = logical_output;
            self.database
                .define_variant_projection(storage_table, &projection_target, output)?;
        }
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
                    "physical current projection source schema alias missing",
                ))?;
            let cases = if source_mapping.variant_cases.is_empty() {
                vec![(groove_variant_tag(source_alias)?, None)]
            } else {
                source_mapping
                    .variant_cases
                    .iter()
                    .map(|case| (case.tag, Some(&case.fields)))
                    .collect()
            };
            for (tag, present) in cases {
                let fields = self.physical_current_projection_case_for_enum_columns(
                    source_schema,
                    &source_table_name,
                    &source_mapping,
                    target_schema,
                    target_table_name,
                    present,
                    Some(&required_enum_columns),
                )?;
                for storage_table in &storage_tables {
                    if let Some(fields) = fields.clone() {
                        self.database
                            .register_variant_projection_case_omitting_unrepresentable_enums(
                                storage_table,
                                &projection_target,
                                tag,
                                fields,
                            )?;
                    } else {
                        self.database.register_variant_ignore_case(
                            storage_table,
                            &projection_target,
                            tag,
                        )?;
                    }
                }
            }
        }
        Ok(projection_target)
    }

    /// Register the common physical descriptor used to choose the latest
    /// Global/Ahead version before a query-local old-schema projection can
    /// omit an unrepresentable enum case.
    pub(super) async fn ensure_physical_current_winner_projection(
        &mut self,
        target_schema: SchemaVersionId,
        target_table_name: &str,
    ) -> Result<(String, Vec<String>), Error> {
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "target current winner physical mapping missing",
            ))?;
        let storage_tables = [
            physical_global_current_table_name(target_mapping.table_id),
            physical_ahead_current_table_name(target_mapping.table_id),
        ];
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let authored_output = physical_current_descriptor(&target_table, &target_mapping)?;
        let physical_fields = authored_output
            .fields()
            .iter()
            .map(|field| {
                field.name.clone().ok_or(Error::InvalidStoredValue(
                    "physical current winner field unnamed",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Winner selection operates on raw physical data. Keep the target
        // layout fixed, but take enum registries from the actual evolved
        // storage descriptor so later tags can reach the logical omission
        // boundary without being decoded against an old registry.
        let output = physical_write_descriptor(
            &authored_output,
            &physical_fields,
            self.database.table_schema(&storage_tables[0])?,
        )?;
        let projection_target =
            physical_current_winner_projection_target(target_mapping.table_id, &physical_fields);
        let mut output_fields = None;
        for storage_table in &storage_tables {
            let fields = output
                .fields()
                .iter()
                .map(|field| {
                    field.name.clone().ok_or(Error::InvalidStoredValue(
                        "physical current winner field unnamed",
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(existing) = &output_fields {
                if existing != &fields {
                    return Err(Error::InvalidStoredValue(
                        "physical current winner descriptors disagree",
                    ));
                }
            } else {
                output_fields = Some(fields);
            }
            self.database
                .define_variant_projection(storage_table, &projection_target, output)
                ?;
        }

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
                    "physical current winner source schema alias missing",
                ))?;
            let source_table = self.table_in_schema(&source_table_name, source_schema)?;
            let target_columns_by_physical_field = target_mapping
                .columns
                .iter()
                .map(|(column, id)| (physical_user_column_field(*id), column.clone()))
                .collect::<BTreeMap<_, _>>();
            let cases = if source_mapping.variant_cases.is_empty() {
                vec![(groove_variant_tag(source_alias)?, None)]
            } else {
                source_mapping
                    .variant_cases
                    .iter()
                    .map(|case| (case.tag, Some(&case.fields)))
                    .collect()
            };
            for (tag, present) in cases {
                let available =
                    physical_current_field_names_for_case(&source_table, &source_mapping, present)?
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                for storage_table in &storage_tables {
                    let fields = output
                        .fields()
                        .iter()
                        .map(|field| {
                            let name = field.name.clone().ok_or(Error::InvalidStoredValue(
                                "physical current winner field unnamed",
                            ))?;
                            if available.contains(&name) {
                                Ok(ProjectField::named(name))
                            } else if let Some(column) = target_columns_by_physical_field.get(&name)
                            {
                                // Only mapped user columns can be absent because
                                // of a lens. Witness and system fields must remain
                                // raw physical fields. The resulting field may be
                                // an Add default or a value carried through a
                                // Rename/Copy chain from the source variant.
                                Ok(self
                                    .lens_projection_for_missing_current_field(
                                        source_schema,
                                        &source_table_name,
                                        &source_mapping,
                                        &available,
                                        target_schema,
                                        target_table_name,
                                        &target_mapping,
                                        column,
                                        name.clone(),
                                        field.value_type.clone(),
                                    )?
                                    .unwrap_or_else(|| {
                                        ProjectField::literal_typed(
                                            name,
                                            Value::Nullable(None),
                                            field.value_type.clone(),
                                        )
                                    }))
                            } else if matches!(field.value_type, records::ValueType::Nullable(_)) {
                                Ok(ProjectField::literal_typed(
                                    name,
                                    Value::Nullable(None),
                                    field.value_type.clone(),
                                ))
                            } else {
                                Err(Error::InvalidStoredValue(
                                    "physical current winner source misses required field",
                                ))
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    self.database
                        .refresh_variant_case_for_registry_evolution(
                            storage_table,
                            &projection_target,
                            tag,
                            fields,
                        )?;
                }
            }
        }
        Ok((projection_target, output_fields.unwrap_or_default()))
    }

    /// Resolve a missing target user field through the migration path before
    /// the Global/Ahead arg-max. This deliberately yields a projection field,
    /// not only a literal: `CopyColumn` chains must read their actual source
    /// physical field while the winner still has its source variant layout.
    fn lens_projection_for_missing_current_field(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        available: &BTreeSet<String>,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        target_mapping: &TablePhysicalMapping,
        target_column: &str,
        output_name: String,
        output_type: records::ValueType,
    ) -> Result<Option<ProjectField>, Error> {
        let source_table = self.table_in_schema(source_table_name, source_schema)?;
        let mut cells = source_table
            .columns
            .iter()
            .map(|column| {
                let projection = source_mapping
                    .columns
                    .get(&column.name)
                    .and_then(|column_id| {
                        let name = physical_user_column_field(*column_id);
                        available
                            .contains(&name)
                            .then_some(CurrentWinnerCellProjection::Field {
                                name,
                                column_id: *column_id,
                                column_type: column.column_type.clone(),
                            })
                    })
                    .unwrap_or(CurrentWinnerCellProjection::Null);
                (column.name.clone(), projection)
            })
            .collect::<BTreeMap<_, _>>();
        for direction in [LensPathDirection::Forward, LensPathDirection::Reverse] {
            let Some(path) = self.compiled_lens_path(
                source_schema,
                target_schema,
                direction,
                source_table_name,
            )?
            else {
                continue;
            };
            if path.target_table != target_table_name {
                continue;
            }
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
                            .or_insert(CurrentWinnerCellProjection::Literal(default));
                    }
                    CompiledLensOp::Drop { column } => {
                        cells.remove(&column);
                    }
                }
            }
            return Ok(match cells.remove(target_column) {
                Some(CurrentWinnerCellProjection::Field {
                    name: source,
                    column_id: source_column_id,
                    column_type: source_column_type,
                }) => {
                    let target_column_id =
                        target_mapping.columns.get(target_column).copied().ok_or(
                            Error::InvalidStoredValue(
                                "target current winner column mapping missing",
                            ),
                        )?;
                    let target_column_type = self
                        .table_in_schema(target_table_name, target_schema)?
                        .columns
                        .iter()
                        .find(|column| column.name == target_column)
                        .map(|column| column.column_type.clone())
                        .ok_or(Error::InvalidStoredValue(
                            "target current winner column schema missing",
                        ))?;
                    if source_column_id == target_column_id
                        || !physical_mapping_has_enum_boundary(source_mapping, source_column_id)
                            && !physical_mapping_has_enum_boundary(target_mapping, target_column_id)
                            && !value_type_has_enum_boundary(&source_column_type)
                            && !value_type_has_enum_boundary(&target_column_type)
                    {
                        Some(ProjectField::renamed(source, output_name))
                    } else {
                        Some(ProjectField::recursive_enum_remap(
                            source,
                            output_name,
                            output_type,
                            self.physical_copy_enum_remaps(
                                source_mapping,
                                source_column_id,
                                target_mapping,
                                target_column_id,
                                &source_column_type,
                                &target_column_type,
                            )?,
                        ))
                    }
                }
                Some(CurrentWinnerCellProjection::Literal(default)) => {
                    let default = if matches!(output_type, records::ValueType::Nullable(_)) {
                        // A current-winner cell has one outer nullable layer
                        // for source-field presence.  A nullable lens default
                        // is its *inner* logical value, so it must be wrapped
                        // too; otherwise a default null is indistinguishable
                        // from an absent field and is lost by a partial write.
                        Value::Nullable(Some(Box::new(default)))
                    } else {
                        default
                    };
                    Some(ProjectField::literal_typed(
                        output_name,
                        default,
                        output_type,
                    ))
                }
                Some(CurrentWinnerCellProjection::Null) | None => None,
            });
        }
        Ok(None)
    }

    /// Build the logical query-local projection placed after physical current
    /// winner selection. Its enum remaps are intentionally non-total row
    /// omissions, never generic query errors.
    pub(super) fn physical_current_post_winner_projection_fields(
        &mut self,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        required_fields: &BTreeSet<String>,
    ) -> Result<Vec<ProjectField>, Error> {
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .cloned()
            .ok_or(Error::InvalidStoredValue(
                "target post-winner physical mapping missing",
            ))?;
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let required_enum_columns = target_table
            .columns
            .iter()
            .filter(|column| required_fields.contains(&column.name))
            .filter_map(|column| target_mapping.columns.get(&column.name).copied())
            .collect::<BTreeSet<_>>();
        self.physical_current_projection_case_for_enum_columns(
            target_schema,
            target_table_name,
            &target_mapping,
            target_schema,
            target_table_name,
            None,
            Some(&required_enum_columns),
        )?
        .ok_or(Error::InvalidStoredValue(
            "post-winner current projection unexpectedly lacks fields",
        ))
    }

    pub(super) async fn synchronize_physical_version_tables(&mut self) -> Result<(), Error> {
        // A physical schema is a coupled registry: tables, variants, enum
        // registries, indices, and projection cases all become observable by
        // the same live runtime.  Do not leave a prefix behind if any later
        // member fails validation.  Catalogue activation has an additional
        // durable boundary and retains this checkpoint until its commit; this
        // local boundary also protects callers which only need registration.
        let checkpoint = self.database.runtime_registry_checkpoint();
        let result = self.synchronize_physical_version_tables_inner().await;
        if result.is_err() {
            self.database.restore_runtime_registry(checkpoint);
        }
        result
    }

    async fn synchronize_physical_version_tables_inner(&mut self) -> Result<(), Error> {
        let desired_tables = physical_version_storage_tables(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
        )?;
        for desired in desired_tables {
            let existing = match self.database.table_schema(&desired.name) {
                Ok(existing) => Some(existing.clone()),
                Err(GrooveDbError::TableNotFound(_)) => None,
                Err(error) => return Err(error.into()),
            };
            let Some(existing) = existing else {
                self.database.register_table(desired)?;
                continue;
            };
            self.database
                .evolve_table_variant_registries(&desired.name, &desired.columns)?;
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
            for schema_version in desired.variants {
                if existing.variant(schema_version.tag).is_some() {
                    continue;
                }
                self.database
                    .register_table_variant_with_columns(
                        &desired.name,
                        added_columns.clone(),
                        schema_version,
                    )?;
            }
            for index in desired.indices {
                if existing
                    .indices
                    .iter()
                    .any(|candidate| candidate.name == index.name)
                {
                    continue;
                }
                self.database
                    .register_table_index(&desired.name, index)
                    .await?;
            }
        }
        #[cfg(any(test, feature = "testing"))]
        if self.catalogue_activation_failpoint
            == Some(CatalogueActivationFailpoint::AfterPhysicalRegistryRegistration)
        {
            self.catalogue_activation_failpoint = None;
            return Err(Error::CatalogueActivationFailed);
        }
        self.register_physical_history_variant_projections().await?;
        self.register_physical_current_variant_projections().await?;
        self.register_physical_current_winner_projections().await
    }

    /// Keep the raw Global/Ahead winner targets live as the physical enum
    /// registries evolve, so a subsequent query-local lowering pass can read
    /// every newly introduced source variant.
    async fn register_physical_current_winner_projections(&mut self) -> Result<(), Error> {
        let targets = self
            .catalogue
            .physical_mappings
            .iter()
            .flat_map(|(schema_version, mapping)| {
                mapping
                    .tables
                    .keys()
                    .map(|table_name| (*schema_version, table_name.clone()))
            })
            .collect::<BTreeSet<_>>();
        for (schema_version, table_name) in targets {
            self.ensure_physical_current_winner_projection(schema_version, &table_name)
                .await?;
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
        present: Option<&BTreeSet<String>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::History,
            present,
            None,
        )
    }

    #[allow(dead_code)]
    fn physical_history_projection_case_for_enum_columns(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        present: Option<&BTreeSet<String>>,
        required_enum_columns: Option<&BTreeSet<PhysicalColumnId>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::History,
            present,
            required_enum_columns,
        )
    }

    fn physical_current_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        present: Option<&BTreeSet<String>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_current_projection_case_for_enum_columns(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            present,
            None,
        )
    }

    fn physical_current_projection_case_for_enum_columns(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        present: Option<&BTreeSet<String>>,
        required_enum_columns: Option<&BTreeSet<PhysicalColumnId>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        self.physical_content_projection_case(
            source_schema,
            source_table_name,
            source_mapping,
            target_schema,
            target_table_name,
            ContentProjectionShape::Current,
            present,
            required_enum_columns,
        )
    }

    fn physical_content_projection_case(
        &mut self,
        source_schema: SchemaVersionId,
        source_table_name: &str,
        source_mapping: &TablePhysicalMapping,
        target_schema: SchemaVersionId,
        target_table_name: &str,
        shape: ContentProjectionShape,
        present: Option<&BTreeSet<String>>,
        required_enum_columns: Option<&BTreeSet<PhysicalColumnId>>,
    ) -> Result<Option<Vec<ProjectField>>, Error> {
        #[derive(Clone)]
        enum CellProjection {
            Field(String),
            /// The source variant did not author this cell, so it remains
            /// absent in the target descriptor.
            Missing,
            /// A lens supplied this cell.  Its value (including a nullable
            /// null) is a present logical value and must retain that fact.
            Literal(Value),
        }

        let source_table = self.table_in_schema(source_table_name, source_schema)?;
        let target_table = self.table_in_schema(target_table_name, target_schema)?;
        let mut cells = source_table
            .columns
            .iter()
            .filter(|column| present.is_none_or(|present| present.contains(&column.name)))
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
        let target_storage = match shape {
            ContentProjectionShape::History => target_table.history_storage_table(),
            ContentProjectionShape::Current => {
                target_table.global_current_storage_tables()[0].clone()
            }
        };
        let user_cells = match shape {
            ContentProjectionShape::History => HistoryRowRecord::USER_CELLS,
            ContentProjectionShape::Current => GlobalCurrentRowRecord::USER_CELLS,
        };
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target_schema)
            .and_then(|mapping| mapping.tables.get(target_table_name))
            .ok_or(Error::InvalidStoredValue(
                "target enum physical mapping missing",
            ))?;
        let physical_names = match shape {
            ContentProjectionShape::History => {
                physical_history_field_names(&target_table, target_mapping)?
            }
            ContentProjectionShape::Current => {
                physical_current_field_names(&target_table, target_mapping)?
            }
        };
        let physical_storage = match shape {
            ContentProjectionShape::History => physical_history_table_name(target_mapping.table_id),
            ContentProjectionShape::Current => {
                physical_global_current_table_name(target_mapping.table_id)
            }
        };
        let projection_output = if required_enum_columns.is_some() {
            // Query-local enum targets are authored-descriptor boundaries;
            // their recursive remap must validate/encode against the old
            // schema rather than the physical registry descriptor.
            match shape {
                ContentProjectionShape::History => {
                    authored_history_projection_descriptor(&target_table)
                }
                ContentProjectionShape::Current => target_storage.record_schema(),
            }
        } else {
            widened_projection_descriptor(
                &target_storage.record_schema(),
                &physical_names,
                self.database.table_schema(&physical_storage)?,
            )?
        };
        let mut fields = target_storage
            .record_schema()
            .fields()
            .iter()
            .take(user_cells)
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
            let projection = match cells.remove(&column.name) {
                Some(projection) => projection,
                None if present.is_some() => CellProjection::Missing,
                None => return Ok(None),
            };
            match projection {
                CellProjection::Field(source) => {
                    let column_id = target_mapping.columns.get(&column.name).copied().ok_or(
                        Error::InvalidStoredValue("target enum physical column mapping missing"),
                    )?;
                    let has_enum_boundary =
                        physical_mapping_has_enum_boundary(target_mapping, column_id);
                    let direct_enum = matches!(
                        column.column_type,
                        records::ValueType::EnumTag(_) | records::ValueType::Enum(_)
                    );
                    if has_enum_boundary || direct_enum {
                        if required_enum_columns
                            .is_some_and(|required| !required.contains(&column_id))
                        {
                            // This source does not semantically consume the
                            // cell.  Do not decode an unknown physical case
                            // merely to populate an otherwise unused logical
                            // field, and do not let the physical tag cross the
                            // boundary as an authored value.
                            fields.push(ProjectField::literal_typed(
                                output,
                                Value::Nullable(None),
                                records::ValueType::Nullable(Box::new(column.column_type.clone())),
                            ));
                            continue;
                        }
                        let remaps = if has_enum_boundary {
                            self.physical_to_authored_enum_remaps(target_mapping, column_id)?
                        } else {
                            // Initial table construction precedes durable
                            // registry hydration. At that bootstrap boundary
                            // the sole physical descriptor uses exactly this
                            // authored tag order, so an explicit identity map
                            // both disables raw copying and records the same
                            // descriptor-aware operation used after hydration.
                            match &column.column_type {
                                records::ValueType::EnumTag(schema) => EnumOccurrenceRemaps {
                                    scalar: BTreeMap::from([(
                                        "root".to_owned(),
                                        (0..schema.variants.len())
                                            .map(|tag| u8::try_from(tag).ok())
                                            .collect(),
                                    )]),
                                    payload: BTreeMap::new(),
                                    payload_children: BTreeMap::new(),
                                },
                                records::ValueType::Enum(schema) => EnumOccurrenceRemaps {
                                    scalar: BTreeMap::new(),
                                    payload: BTreeMap::from([(
                                        "root".to_owned(),
                                        (0..schema.cases.len())
                                            .map(|tag| u32::try_from(tag).ok())
                                            .collect(),
                                    )]),
                                    payload_children: BTreeMap::from([(
                                        "root".to_owned(),
                                        (0..schema.cases.len())
                                            .map(|tag| Some(format!("root/case/bootstrap/{tag}")))
                                            .collect(),
                                    )]),
                                },
                                _ => unreachable!("direct enum checked above"),
                            }
                        };
                        let target = projection_output
                            .field_index(&user_column_field(&column.name))
                            .and_then(|index| projection_output.fields().get(index))
                            .ok_or(Error::InvalidStoredValue(
                                "target enum projection output field missing",
                            ))?
                            .value_type
                            .clone();
                        fields.push(if required_enum_columns.is_some() {
                            ProjectField::recursive_enum_remap_omitting_unrepresentable(
                                source, output, target, remaps,
                            )
                        } else {
                            ProjectField::recursive_enum_remap(source, output, target, remaps)
                        });
                    } else {
                        fields.push(ProjectField::renamed(source, output));
                    }
                }
                CellProjection::Missing => {
                    fields.push(ProjectField::literal_typed(
                        output,
                        Value::Nullable(None),
                        records::ValueType::Nullable(Box::new(column.column_type.clone())),
                    ));
                }
                CellProjection::Literal(value) => {
                    fields.push(ProjectField::literal_typed(
                        output,
                        Value::Nullable(Some(Box::new(value))),
                        records::ValueType::Nullable(Box::new(column.column_type.clone())),
                    ));
                }
            }
        }
        fields.extend(
            target_storage
                .record_schema()
                .fields()
                .iter()
                .skip(user_cells + target_table.columns.len())
                .map(|field| {
                    ProjectField::named(
                        field
                            .name
                            .clone()
                            .expect("Jazz trailing storage fields are named"),
                    )
                }),
        );
        Ok(Some(fields))
    }
}

pub(super) fn shared_branch_scan(
    scan: Option<groove::ivm::StaticScanSpec>,
) -> groove::ivm::StaticScanSpec {
    branch_scan(&BranchKey::default(), scan)
}

fn branch_scan(
    branch_key: &BranchKey,
    scan: Option<groove::ivm::StaticScanSpec>,
) -> groove::ivm::StaticScanSpec {
    use groove::ivm::{LiteralValue, StaticScanSpec};

    let branch = LiteralValue::from(Value::Bytes(branch_key.canonical_bytes()));
    let prepend = |mut values: Vec<LiteralValue>| {
        values.insert(0, branch.clone());
        values
    };
    match scan {
        None => StaticScanSpec::Prefix(vec![branch]),
        Some(StaticScanSpec::Point(values)) => StaticScanSpec::Point(prepend(values)),
        Some(StaticScanSpec::Prefix(values)) => StaticScanSpec::Prefix(prepend(values)),
        Some(StaticScanSpec::PrefixLimit { prefix, max_items }) => {
            StaticScanSpec::PrefixLimit {
                prefix: prepend(prefix),
                max_items,
            }
        }
        Some(StaticScanSpec::Range { start, end }) => StaticScanSpec::Range {
            start: prepend(start),
            end: prepend(end),
        },
    }
}

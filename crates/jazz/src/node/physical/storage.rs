impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn prepared_physical_write_plan(
        &mut self,
        schema_version: SchemaVersionId,
        logical_table: &str,
        target: PhysicalWriteTarget,
    ) -> Result<Arc<PreparedPhysicalWritePlan>, Error> {
        if let Some(plan) = self
            .catalogue
            .physical_write_plan_cache
            .get(&schema_version)
            .and_then(|tables| tables.get(logical_table))
            .and_then(|targets| targets.get(&target))
        {
            return Ok(Arc::clone(plan));
        }

        let source_table = Arc::new(self.table_in_schema(logical_table, schema_version)?);
        let source_mapping = Arc::new(
            self.catalogue
                .physical_mappings
                .get(&schema_version)
                .and_then(|mapping| mapping.tables.get(logical_table))
                .cloned()
                .ok_or(Error::InvalidStoredValue(
                    "physical write table mapping missing",
                ))?,
        );
        let storage_table = match target {
            PhysicalWriteTarget::History => physical_history_storage_table(
                &self.catalogue.physical_mappings,
                schema_version,
                logical_table,
            )?,
            PhysicalWriteTarget::GlobalCurrent => physical_current_storage_table(
                &self.catalogue.physical_mappings,
                schema_version,
                logical_table,
                PhysicalCurrentClass::Global,
            )?,
            PhysicalWriteTarget::AheadCurrent => physical_current_storage_table(
                &self.catalogue.physical_mappings,
                schema_version,
                logical_table,
                PhysicalCurrentClass::Ahead,
            )?,
        };
        let physical_table = Arc::new(self.database.table_schema(&storage_table)?.clone());
        let logical_descriptor = match target {
            PhysicalWriteTarget::History => source_table.history_storage_table().record_schema(),
            PhysicalWriteTarget::GlobalCurrent => {
                source_table.global_current_storage_tables()[0].record_schema()
            }
            PhysicalWriteTarget::AheadCurrent => {
                source_table.ahead_current_storage_tables()[0].record_schema()
            }
        };
        let physical_names = match target {
            PhysicalWriteTarget::History => {
                physical_history_field_names(&source_table, &source_mapping)?
            }
            PhysicalWriteTarget::GlobalCurrent | PhysicalWriteTarget::AheadCurrent => {
                physical_current_field_names(&source_table, &source_mapping)?
            }
        };
        let physical_descriptor = physical_write_descriptor(
            &logical_descriptor,
            &physical_names,
            &physical_table,
        )?;
        let plan = Arc::new(PreparedPhysicalWritePlan {
            storage_table,
            source_table,
            source_mapping,
            physical_table,
            logical_descriptor,
            physical_descriptor,
        });
        self.catalogue
            .physical_write_plan_cache
            .entry(schema_version)
            .or_default()
            .entry(logical_table.to_owned())
            .or_default()
            .insert(target, Arc::clone(&plan));
        Ok(plan)
    }

    pub(super) fn version_storage_table_for_row(
        &mut self,
        version: &VersionRow,
    ) -> Result<groove::Intern<String>, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing while resolving storage table",
            ))?;
        if version.layer() == VersionLayer::Deletion {
            return Ok(groove::Intern::new(
                SHARED_DELETION_HISTORY_TABLE.to_owned(),
            ));
        }
        Ok(groove::Intern::new(physical_history_storage_table(
            &self.catalogue.physical_mappings,
            schema_version,
            version.table(),
        )?))
    }

    pub(super) fn version_storage_primary_key(
        &self,
        version: &VersionRow,
    ) -> Result<PrimaryKeyValue, Error> {
        if version.layer() == VersionLayer::Deletion {
            return Ok(shared_deletion_history_primary_key(
                self.physical_table_id_for_version(version)?,
                version,
            ));
        }
        Ok(history_primary_key(version))
    }

    pub(super) fn version_storage_primary_key_values(
        &self,
        version: &VersionRow,
    ) -> Result<Vec<Value>, Error> {
        if version.layer() == VersionLayer::Deletion {
            let table_id = self.physical_table_id_for_version(version)?;
            return Ok(vec![
                Value::Bytes(version.branch_key().canonical_bytes()),
                Value::U64(table_id.0),
                Value::Uuid(version.row_uuid().0),
                Value::U64(version.tx_time().0),
                Value::U64(version.tx_node_alias().0),
            ]);
        }
        Ok(vec![
            Value::Bytes(version.branch_key().canonical_bytes()),
            Value::Uuid(version.row_uuid().0),
            Value::U64(version.tx_time().0),
            Value::U64(version.tx_node_alias().0),
        ])
    }

    /// Re-encode every enum occurrence in a logical storage record before it
    /// crosses into a physical table.  History, settled-current and
    /// ahead-current writes share this boundary; allowing one of those paths
    /// to raw-copy an authored tag would make the durable table internally
    /// inconsistent after concurrent schema introductions.
    pub(super) fn remap_authored_enum_cells_for_physical(
        &self,
        values: &mut [Value],
        source_table: &TableSchema,
        source_mapping: &TablePhysicalMapping,
        physical_table: &GrooveTableSchema,
        user_cells: usize,
    ) -> Result<(), Error> {
        for (column_index, column) in source_table.columns.iter().enumerate() {
            let column_id = source_mapping.columns.get(&column.name).copied().ok_or(
                Error::InvalidStoredValue("physical enum write column mapping missing"),
            )?;
            let value_index = user_cells + column_index;
            let value = values
                .get_mut(value_index)
                .ok_or(Error::InvalidStoredValue(
                    "physical enum write field missing",
                ))?;
            let mut remaps = EnumOccurrenceRemaps::default();
            if let Some(authored_cases) = source_mapping.scalar_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_scalar_enum_cases(source_mapping.table_id, column_id)?;
                remaps.scalar.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u8::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
            if let Some(authored_cases) = source_mapping.payload_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_payload_enum_cases(source_mapping.table_id, column_id)?;
                remaps.payload.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u32::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| Some(global_case_path("root", identity)))
                        .collect(),
                );
            }
            if let Some(authored_paths) = source_mapping.nested_scalar_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_scalar_enum_cases(
                        source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.scalar.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u8::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested scalar enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                }
            }
            if let Some(authored_paths) = source_mapping.nested_payload_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_payload_enum_cases(
                        source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.payload.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u32::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested payload enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                    remaps.payload_children.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| Some(global_case_path(path, identity)))
                            .collect(),
                    );
                }
            }
            if remaps.scalar.is_empty() && remaps.payload.is_empty() {
                continue;
            }
            let physical_type = physical_table
                .columns
                .iter()
                .find(|physical| physical.name == physical_user_column_field(column_id))
                .map(|physical| &physical.column_type)
                .ok_or(Error::InvalidStoredValue(
                    "physical enum write column missing",
                ))?;
            let (Value::Nullable(Some(inner)), records::ValueType::Nullable(physical)) =
                (value.clone(), physical_type)
            else {
                continue;
            };
            *value = Value::Nullable(Some(Box::new(remap_nested_enum_value(
                *inner,
                &column.column_type,
                physical,
                &remaps,
                "root",
            )?)));
        }
        Ok(())
    }

    pub(super) fn version_storage_write_binding(
        &mut self,
        version: &VersionRow,
    ) -> Result<
        (
            groove::Intern<String>,
            groove::records::ValidatedVariantRecord,
        ),
        Error,
    > {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored row schema version alias missing while preparing storage write",
            ))?;
        if version.layer() == VersionLayer::Deletion {
            return self.shared_deletion_history_write_binding(version);
        }

        let plan = self.prepared_physical_write_plan(
            schema_version,
            version.table(),
            PhysicalWriteTarget::History,
        )?;
        // The authored row carries declaration-local enum ordinals.  Rewrite
        // those cells through their durable schema-qualified identities before
        // giving the record to the physical table; raw-copying would alias two
        // concurrent siblings which both authored ordinal 2.
        let mut values = version.record.to_values()?;
        for (column_index, column) in plan.source_table.columns.iter().enumerate() {
            let column_id = plan.source_mapping.columns.get(&column.name).copied().ok_or(
                Error::InvalidStoredValue("physical scalar enum write column mapping missing"),
            )?;
            let value_index = HistoryRowRecord::USER_CELLS + column_index;
            let value = values
                .get_mut(value_index)
                .ok_or(Error::InvalidStoredValue(
                    "history scalar enum write field missing",
                ))?;
            let mut remaps = EnumOccurrenceRemaps::default();
            if let Some(authored_cases) = plan.source_mapping.scalar_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_scalar_enum_cases(plan.source_mapping.table_id, column_id)?;
                remaps.scalar.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u8::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical scalar enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
            }
            if let Some(authored_cases) = plan.source_mapping.payload_enum_cases.get(&column_id) {
                let physical_cases =
                    self.physical_payload_enum_cases(plan.source_mapping.table_id, column_id)?;
                remaps.payload.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| {
                            physical_cases
                                .iter()
                                .position(|candidate| candidate == identity)
                                .map(|tag| {
                                    u32::try_from(tag).map_err(|_| {
                                        Error::InvalidStoredValue(
                                            "physical payload enum tag exhausted",
                                        )
                                    })
                                })
                                .transpose()
                        })
                        .collect::<Result<_, _>>()?,
                );
                remaps.payload_children.insert(
                    "root".to_owned(),
                    authored_cases
                        .iter()
                        .map(|identity| Some(global_case_path("root", identity)))
                        .collect(),
                );
            }
            if let Some(authored_paths) = plan.source_mapping.nested_scalar_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_scalar_enum_cases(
                        plan.source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.scalar.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u8::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested scalar enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                }
            }
            if let Some(authored_paths) = plan.source_mapping.nested_payload_enum_cases.get(&column_id) {
                for (path, authored_cases) in authored_paths {
                    let physical_cases = self.physical_nested_payload_enum_cases(
                        plan.source_mapping.table_id,
                        column_id,
                        path,
                    )?;
                    remaps.payload.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| {
                                physical_cases
                                    .iter()
                                    .position(|candidate| candidate == identity)
                                    .map(|tag| {
                                        u32::try_from(tag).map_err(|_| {
                                            Error::InvalidStoredValue(
                                                "physical nested payload enum tag exhausted",
                                            )
                                        })
                                    })
                                    .transpose()
                            })
                            .collect::<Result<_, _>>()?,
                    );
                    remaps.payload_children.insert(
                        path.clone(),
                        authored_cases
                            .iter()
                            .map(|identity| Some(global_case_path(path, identity)))
                            .collect(),
                    );
                }
            }
            if remaps.scalar.is_empty() && remaps.payload.is_empty() {
                continue;
            }
            let physical_type = plan.physical_table
                .columns
                .iter()
                .find(|physical| physical.name == physical_user_column_field(column_id))
                .map(|physical| &physical.column_type)
                .ok_or(Error::InvalidStoredValue(
                    "physical enum write column missing",
                ))?;
            let (Value::Nullable(Some(inner)), records::ValueType::Nullable(physical)) =
                (value.clone(), physical_type)
            else {
                continue;
            };
            *value = Value::Nullable(Some(Box::new(remap_nested_enum_value(
                *inner,
                &column.column_type,
                physical,
                &remaps,
                "root",
            )?)));
        }
        Ok((
            groove::Intern::new(plan.storage_table.clone()),
            groove::records::ValidatedVariantRecord::create(
                groove_variant_tag(version.schema_version_alias())?,
                plan.physical_descriptor,
                &values,
            )?,
        ))
    }

    /// Encode a deletion/register version into the fixed shared history table.
    /// The wire and in-memory `VersionRow` stay logical-table scoped; this is
    /// the sole physical boundary that adds local routing identity.
    pub(super) fn shared_deletion_history_write_binding(
        &mut self,
        version: &VersionRow,
    ) -> Result<
        (
            groove::Intern<String>,
            groove::records::ValidatedVariantRecord,
        ),
        Error,
    > {
        debug_assert_eq!(version.layer(), VersionLayer::Deletion);
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "stored register schema version alias missing while preparing shared deletion write",
            ))?;
        let table_id = self.physical_table_id_for_schema(schema_version, version.table())?;
        let mut values = version.record.to_values()?;
        values.insert(1, Value::U64(table_id.0));
        let descriptor = self
            .database
            .table_schema(SHARED_DELETION_HISTORY_TABLE)?
            .record_schema();
        Ok((
            groove::Intern::new(SHARED_DELETION_HISTORY_TABLE.to_owned()),
            groove::records::ValidatedVariantRecord::create(
                groove_variant_tag(version.schema_version_alias())?,
                descriptor,
                &values,
            )?,
        ))
    }

    pub(super) fn rejected_version_storage_write_binding(
        &self,
        version: &VersionRow,
        logical_record: &OwnedRecord,
    ) -> Result<(groove::Intern<String>, groove::records::VariantRecord), Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "rejected row schema version alias missing",
            ))?;
        let binding = physical_rejected_version_binding(
            &self.catalogue.catalogue_schemas,
            &self.catalogue.physical_mappings,
            schema_version,
            version.table(),
        )?;
        let record = OwnedRecord::new(logical_record.raw().to_vec(), binding.descriptor);
        Ok((
            groove::Intern::new(binding.storage_table),
            groove::records::VariantRecord::new(
                groove_variant_tag(version.schema_version_alias())?,
                record,
            ),
        ))
    }

}

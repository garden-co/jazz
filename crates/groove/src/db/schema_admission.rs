use super::*;

pub(super) fn validate_durable_key_schema(schema: &DatabaseSchema) -> Result<(), Error> {
    for store in &schema.direct_record_stores {
        if store
            .key
            .iter()
            .any(|(_, value_type)| value_type.contains_record())
        {
            return Err(Error::InvalidDirectRecordStoreKey(store.name.clone()));
        }
    }
    for table in &schema.tables {
        validate_table_schema_variants(table)?;
    }
    Ok(())
}

pub(super) fn validate_table_schema_variants(table: &TableSchema) -> Result<(), Error> {
    if !table.has_variants() {
        return Ok(());
    }
    if !table.foreign_keys.is_empty() {
        return Err(Error::UnsupportedSchemaVariantTableFeature(
            table.name.clone(),
        ));
    }

    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;
    let mut versions = HashSet::new();
    for variant_tag in &table.variants {
        if variant_tag.tag == 0 {
            return Err(Error::ReservedTableVariant(table.name.clone()));
        }
        if !versions.insert(variant_tag.tag) {
            return Err(Error::DuplicateTableVariant {
                table: table.name.clone(),
                version: u64::from(variant_tag.tag),
            });
        }
        let mut fields = HashSet::new();
        if variant_tag.payload_fields.is_empty() {
            for field in &variant_tag.fields {
                if !fields.insert(field.as_str())
                    || !table.columns.iter().any(|column| column.name == *field)
                {
                    return Err(Error::InvalidTableVariantField {
                        table: table.name.clone(),
                        version: u64::from(variant_tag.tag),
                        field: field.clone(),
                    });
                }
            }
        } else {
            let mut local = HashSet::new();
            for field in &variant_tag.payload_fields {
                if !local.insert(field.name.as_str()) {
                    return Err(Error::InvalidTableVariantField {
                        table: table.name.clone(),
                        version: u64::from(variant_tag.tag),
                        field: field.name.clone(),
                    });
                }
                let Some(shared) = &field.shared_column else {
                    continue;
                };
                let valid = table.columns.iter().any(|column| {
                    column.name == *shared
                        && column
                            .column_type
                            .registry_compatible_with(&field.value_type)
                });
                if !valid || !fields.insert(shared.as_str()) {
                    return Err(Error::InvalidTableVariantField {
                        table: table.name.clone(),
                        version: u64::from(variant_tag.tag),
                        field: shared.clone(),
                    });
                }
            }
        }
        for column in &primary_key.columns {
            if !fields.contains(column.column.as_str()) {
                return Err(Error::SchemaVersionMissingPrimaryKey {
                    table: table.name.clone(),
                    version: u64::from(variant_tag.tag),
                    column: column.column.clone(),
                });
            }
        }
    }
    Ok(())
}

impl Database {
    /// Return the live schema for a table.
    pub fn table_schema(&self, table: &str) -> Result<&TableSchema, Error> {
        self.table(table)
    }

    /// Append a new table to the live runtime schema.
    ///
    /// The backing storage layout must already be able to route the table's
    /// logical family (for example through a shared class family).
    pub fn register_table(&mut self, table: TableSchema) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        if self.ivm_runtime.table(&table.name).is_some() {
            return Err(Error::TableAlreadyExists(table.name));
        }
        let mut updated = self.ivm_runtime.schema().clone();
        updated.tables.push(table.clone());
        validate_durable_key_schema(&updated)?;
        self.ivm_runtime
            .register_table(table)
            .map_err(Error::IvmRuntime)
    }

    /// Append one whole-row enum case to an already variant table.
    ///
    /// Registration is process-local schema metadata. Callers must restore all
    /// durable variants before opening reads after restart, and must register
    /// every required projection case before writing the first row of a newly
    /// registered version.
    pub fn register_table_variant(
        &mut self,
        table: &str,
        variant: TableVariant,
    ) -> Result<(), Error> {
        self.register_table_variant_with_columns(table, [], variant)
    }

    /// Append stable catalogue fields and one row layout to a live variant table.
    ///
    /// Existing fields and layouts remain immutable. This is the live-schema
    /// path for stores whose variant registry grows before the first row of a
    /// newly registered version is written.
    pub fn register_table_variant_with_columns(
        &mut self,
        table: &str,
        columns: impl IntoIterator<Item = crate::schema::ColumnSchema>,
        variant: TableVariant,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let mut updated = self.table(table)?.clone();
        if !updated.has_variants() {
            return Err(Error::CannotPromoteLiveTableToSchemaVariants(
                table.to_owned(),
            ));
        }
        let mut added_columns = Vec::new();
        let mut evolved_columns = Vec::new();
        for column in columns {
            match updated
                .columns
                .iter_mut()
                .find(|existing| existing.name == column.name)
            {
                Some(existing) if existing == &column => {}
                Some(existing)
                    if existing
                        .column_type
                        .can_evolve_registry_to(&column.column_type) =>
                {
                    *existing = column.clone();
                    evolved_columns.push(column);
                }
                Some(_) => {
                    return Err(Error::TableFieldDefinitionMismatch {
                        table: table.to_owned(),
                        field: column.name,
                    });
                }
                None => {
                    updated.columns.push(column.clone());
                    added_columns.push(column);
                }
            }
        }
        updated.variants.push(variant.clone());
        validate_table_schema_variants(&updated)?;
        if !evolved_columns.is_empty() {
            self.ivm_runtime
                .evolve_table_variant_registries(table, &evolved_columns)
                .map_err(Error::IvmRuntime)?;
        }
        self.ivm_runtime
            .register_table_variant_with_columns(table, added_columns, variant)
            .map_err(Error::IvmRuntime)
    }

    /// Append cases to nested enum registries without changing physical
    /// column identity or rewriting existing row payload descriptors.
    pub fn evolve_table_variant_registries(
        &mut self,
        table: &str,
        columns: &[crate::schema::ColumnSchema],
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let existing = self.table(table)?;
        for desired in columns {
            if let Some(current) = existing
                .columns
                .iter()
                .find(|column| column.name == desired.name)
                && !current
                    .column_type
                    .can_evolve_registry_to(&desired.column_type)
            {
                return Err(Error::TableFieldDefinitionMismatch {
                    table: table.to_owned(),
                    field: desired.name.clone(),
                });
            }
        }
        self.ivm_runtime
            .evolve_table_variant_registries(table, columns)
            .map_err(Error::IvmRuntime)
    }

    /// Append and backfill one durable secondary index on a live table.
    ///
    /// Existing rows are indexed before this method returns. Schema-variant
    /// tables use their registered layouts: variants missing any indexed field
    /// are ignored, while later variants automatically register their own case.
    /// Re-registering the same definition is idempotent; changing an existing
    /// index definition is rejected. The storage supplied when opening the
    /// database must already provide the shared `indices` column family.
    pub async fn register_table_index(
        &mut self,
        table: &str,
        index: IndexSchema,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let existing = self.table(table)?.clone();
        if let Some(registered) = existing
            .indices
            .iter()
            .find(|registered| registered.name == index.name)
        {
            return if registered == &index {
                Ok(())
            } else {
                Err(Error::TableIndexDefinitionMismatch {
                    table: table.to_owned(),
                    index: index.name,
                })
            };
        }
        for column in &index.columns {
            if existing
                .columns
                .iter()
                .all(|candidate| candidate.name != *column)
            {
                return Err(Error::TableIndexFieldNotFound {
                    table: table.to_owned(),
                    index: index.name,
                    field: column.clone(),
                });
            }
        }
        if !self.resident_publications.is_empty() {
            return Err(Error::TableIndexRegistrationWhilePublicationsResident {
                table: table.to_owned(),
                index: index.name,
            });
        }
        if let Err(error) = self
            .ivm_runtime
            .register_table_index(table, index, &self.storage)
            .await
        {
            self.poisoned = true;
            return Err(Error::IvmRuntime(error));
        }
        Ok(())
    }

    /// Define one fixed-output projection family for a heterogeneous table.
    ///
    /// The family identity and output descriptor are immutable. Source-version
    /// cases may be appended later without replacing active graph nodes.
    pub fn define_variant_projection(
        &mut self,
        table: &str,
        target: &str,
        output: RecordDescriptor,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .define_variant_projection(table, target, output)
            .map_err(Error::IvmRuntime)
    }

    /// Append one source-version case to a fixed-output variant projection.
    pub fn register_variant_projection_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: impl IntoIterator<Item = ProjectField>,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let fields = fields.into_iter().collect::<Vec<_>>();
        self.ivm_runtime
            .register_variant_projection_case(table, target, variant_tag, &fields)
            .map_err(Error::IvmRuntime)
    }

    /// Append a schema-read projection case that omits only rows containing an
    /// enum case the target descriptor cannot represent. Other projection
    /// errors remain errors.
    pub fn register_variant_projection_case_omitting_unrepresentable_enums(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: impl IntoIterator<Item = ProjectField>,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let fields = fields.into_iter().collect::<Vec<_>>();
        self.ivm_runtime
            .register_variant_projection_case_omitting_unrepresentable_enums(
                table,
                target,
                variant_tag,
                &fields,
            )
            .map_err(Error::IvmRuntime)
    }

    /// Append one generic source-case mapping to a fixed-output projection.
    pub fn register_variant_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: impl IntoIterator<Item = ProjectField>,
    ) -> Result<(), Error> {
        self.register_variant_projection_case(table, target, variant_tag, fields)
    }

    /// Refresh an existing raw projection case after its source descriptor
    /// grows only by append-only enum registry evolution.
    ///
    /// This is deliberately narrower than normal case registration: it never
    /// accepts a changed projection mapping or incompatible field/type change.
    pub fn refresh_variant_case_for_registry_evolution(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: impl IntoIterator<Item = ProjectField>,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let fields = fields.into_iter().collect::<Vec<_>>();
        self.ivm_runtime
            .refresh_variant_projection_case_for_registry_evolution(
                table,
                target,
                variant_tag,
                &fields,
            )
            .map_err(Error::IvmRuntime)
    }

    /// Append a physical source case that constructs one stable logical enum
    /// value in the projection's fixed output descriptor.
    ///
    /// The output must consist of `enum_field: Enum(enum_schema)`. `fields`
    /// select and name the dense source payload for the selected named case.
    #[allow(clippy::too_many_arguments)]
    pub fn register_variant_projection_enum_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        enum_field: &str,
        enum_schema: &EnumSchema,
        case: &str,
        fields: impl IntoIterator<Item = ProjectField>,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        let fields = fields.into_iter().collect::<Vec<_>>();
        self.ivm_runtime
            .register_variant_projection_enum_case(
                table,
                target,
                variant_tag,
                enum_field,
                enum_schema,
                case,
                &fields,
            )
            .map_err(Error::IvmRuntime)
    }

    /// `u32`-tag convenience alias for generic table variants.
    #[allow(clippy::too_many_arguments)]
    pub fn register_variant_enum_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        enum_field: &str,
        enum_schema: &EnumSchema,
        case: &str,
        fields: impl IntoIterator<Item = ProjectField>,
    ) -> Result<(), Error> {
        self.register_variant_projection_enum_case(
            table,
            target,
            variant_tag,
            enum_field,
            enum_schema,
            case,
            fields,
        )
    }

    /// Mark one source version as intentionally absent from a fixed-output
    /// projection.
    ///
    /// An `Ignore` case emits no rows. This remains distinct from an
    /// unregistered case, which is an error when that source version is read or
    /// written.
    pub fn register_variant_projection_ignore_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
    ) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .register_variant_projection_ignore_case(table, target, variant_tag)
            .map_err(Error::IvmRuntime)
    }

    pub fn register_variant_ignore_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
    ) -> Result<(), Error> {
        self.register_variant_projection_ignore_case(table, target, variant_tag)
    }

    /// Apply one registered fixed-output projection to an already decoded
    /// heterogeneous row. `Ignore` returns `None`.
    pub fn project_variant_record(
        &self,
        table: &str,
        target: &str,
        record: &VariantRecord,
    ) -> Result<Option<OwnedRecord>, Error> {
        self.ensure_not_poisoned()?;
        self.ivm_runtime
            .project_variant_record(table, target, record)
            .map_err(Error::IvmRuntime)
    }
}

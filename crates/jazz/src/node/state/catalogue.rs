impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Reservation history for admitting `candidate_schema`. Descendant parked
    /// envelopes are excluded: they are allowed to inherit identities minted
    /// by this candidate, even when they arrived out of order. Unrelated
    /// parked/staged envelopes remain reservations and therefore cannot claim
    /// one of its new UUIDs first.
    pub(super) fn physical_identity_history_for_candidate(
        &self,
        candidate_schema: SchemaVersionId,
        omit: Option<crate::ids::SchemaLineagePublicationId>,
    ) -> Vec<PhysicalIdentityManifest> {
        let mut parents = BTreeMap::new();
        for lineage in self.catalogue.active_lineages_by_target.values() {
            parents.insert(lineage.publication.schema.id, lineage.publication.lens.source);
        }
        for lineage in self.catalogue.staged_lineages.values() {
            parents.insert(lineage.publication.schema.id, lineage.publication.lens.source);
        }
        for lineage in self.catalogue.pending_lineages.values() {
            parents.insert(lineage.publication.schema.id, lineage.publication.lens.source);
        }
        let is_descendant = |schema: SchemaVersionId| {
            let mut cursor = schema;
            let mut visited = BTreeSet::new();
            while visited.insert(cursor) {
                let Some(parent) = parents.get(&cursor).copied() else {
                    return false;
                };
                if parent == candidate_schema {
                    return true;
                }
                cursor = parent;
            }
            false
        };
        self.catalogue
            .physical_mappings
            .values()
            .map(|mapping| mapping.identities.clone())
            .chain(
                self.catalogue
                    .staged_lineages
                    .values()
                    .filter(|staged| {
                        Some(staged.publication.id) != omit
                            && !is_descendant(staged.publication.schema.id)
                    })
                    .map(|staged| staged.publication.physical_identities.clone()),
            )
            .chain(
                self.catalogue
                    .pending_lineages
                    .values()
                    .filter(|pending| {
                        Some(pending.publication.id) != omit
                            && !is_descendant(pending.publication.schema.id)
                    })
                    .map(|pending| pending.publication.physical_identities.clone()),
            )
            .collect()
    }

    /// Author a descendant lineage from this authority's complete durable
    /// catalogue history. This is the only NodeState construction path for a
    /// non-genesis publication: it preserves mapped physical UUIDs and mints
    /// identities only for genuinely new entities, never identities retired
    /// by older active, staged, or parked publications.
    pub fn author_schema_lineage_publication(
        &self,
        schema: SchemaVersion,
        lens: MigrationLens,
        new_tables: impl IntoIterator<Item = impl Into<String>>,
        dropped_tables: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<SchemaLineagePublication, Error> {
        let source = self
            .catalogue
            .catalogue_schemas
            .get(&lens.source)
            .ok_or(Error::InvalidCatalogueUpdate(
                "schema lineage source is missing",
            ))?;
        let identities = &self
            .catalogue
            .physical_mappings
            .get(&lens.source)
            .ok_or(Error::InvalidCatalogueUpdate(
                "schema lineage source identities are missing",
            ))?
            .identities;
        let history = self.physical_identity_history_for_candidate(schema.id, None);
        SchemaLineagePublication::author_from_prior_with_history(
            &source.schema,
            identities,
            history,
            schema,
            lens,
            new_tables,
            dropped_tables,
        )
        .map_err(Error::InvalidCatalogueUpdate)
    }

    async fn persist_catalogue_schema(&mut self, schema: &SchemaVersion) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::Schema.key()),
                Value::Uuid(schema.id.0),
                Value::Bytes(codec::encode_catalogue_schema(schema)?),
            ],
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        Ok(())
    }

    fn hydrate_enum_case_mapping(
        &mut self,
        schema_version: SchemaVersionId,
    ) -> Result<(), Error> {
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue(
                "physical mapping schema payload missing",
            ))?
            .schema
            .clone();
        let mapping = self
            .catalogue
            .physical_mappings
            .get_mut(&schema_version)
            .ok_or(Error::InvalidStoredValue("physical mapping missing"))?;
        for table in &schema.tables {
            let table_identities = mapping
                .identities
                .tables
                .get(&table.name)
                .ok_or(Error::InvalidStoredValue("physical table identities missing"))?
                .clone();
            let Some(physical) = mapping.tables.get_mut(&table.name) else {
                continue;
            };
            for column in &table.columns {
                let records::ValueType::EnumTag(enum_schema) = &column.column_type else {
                    continue;
                };
                let Some(id) = physical.columns.get(&column.name).copied() else {
                    continue;
                };
                let ids = &table_identities.columns[&column.name].enum_variants["root"];
                if ids.len() != enum_schema.variants.len() {
                    return Err(Error::InvalidStoredValue("scalar enum authority identity width mismatch"));
                }
                install_enum_case_ids(physical.scalar_enum_cases.entry(id).or_default(), ids, schema_version)?;
            }
            for column in &table.columns {
                let records::ValueType::Enum(enum_schema) = &column.column_type else {
                    continue;
                };
                let Some(id) = physical.columns.get(&column.name).copied() else {
                    continue;
                };
                let identities = &table_identities.columns[&column.name].enum_variants;
                let ids = &identities["root"];
                if ids.len() != enum_schema.cases.len() {
                    return Err(Error::InvalidStoredValue("payload enum authority identity width mismatch"));
                }
                install_payload_enum_case_ids(
                    physical.payload_enum_cases.entry(id).or_default(),
                    ids,
                    schema_version,
                )?;
                let nested = physical.nested_scalar_enum_cases.entry(id).or_default();
                hydrate_nested_scalar_enum_cases(
                    &column.column_type,
                    identities,
                    schema_version,
                    "root",
                    "root",
                    nested,
                )?;
                let nested_payload = physical.nested_payload_enum_cases.entry(id).or_default();
                hydrate_nested_payload_enum_cases(
                    &column.column_type,
                    identities,
                    schema_version,
                    "root",
                    "root",
                    nested_payload,
                )?;
            }
            for column in &table.columns {
                if matches!(column.column_type, records::ValueType::Enum(_)) {
                    continue;
                }
                let Some(id) = physical.columns.get(&column.name).copied() else {
                    continue;
                };
                let nested = physical.nested_scalar_enum_cases.entry(id).or_default();
                let identities = &table_identities.columns[&column.name].enum_variants;
                hydrate_nested_scalar_enum_cases(
                    &column.column_type,
                    identities,
                    schema_version,
                    "root",
                    "root",
                    nested,
                )?;
                let nested_payload = physical.nested_payload_enum_cases.entry(id).or_default();
                hydrate_nested_payload_enum_cases(
                    &column.column_type,
                    identities,
                    schema_version,
                    "root",
                    "root",
                    nested_payload,
                )?;
            }
        }
        Ok(())
    }

    async fn ensure_provisional_physical_mapping(
        &mut self,
        schema_version: SchemaVersionId,
    ) -> Result<(), Error> {
        if self
            .catalogue
            .physical_mappings
            .contains_key(&schema_version)
            && self
                .catalogue
                .schema_version_aliases
                .contains_key(&schema_version)
        {
            self.hydrate_enum_case_mapping(schema_version)?;
            return Ok(());
        }
        let mapping = match self.catalogue.physical_mappings.get(&schema_version) {
            Some(mapping) => mapping.clone(),
            None => {
                let schema = self
                    .catalogue
                    .catalogue_schemas
                    .get(&schema_version)
                    .ok_or(Error::InvalidStoredValue(
                        "physical mapping schema payload missing",
                    ))?
                    .schema
                    .clone();
                let mut tables = BTreeMap::new();
                for table in &schema.tables {
                    let table_id = self.allocate_physical_table_id()?;
                    let mut columns = BTreeMap::new();
                    for column in &table.columns {
                        columns.insert(column.name.clone(), self.allocate_physical_column_id()?);
                    }
                    tables.insert(
                        table.name.clone(),
                        TablePhysicalMapping {
                            table_id,
                            columns,
                            variant_cases: Vec::new(),
                            scalar_enum_cases: BTreeMap::new(),
                            payload_enum_cases: BTreeMap::new(),
                            nested_scalar_enum_cases: BTreeMap::new(),
                            nested_payload_enum_cases: BTreeMap::new(),
                        },
                    );
                }
                SchemaPhysicalMapping {
                    identities: PhysicalIdentityManifest::allocate(&schema),
                    tables,
                }
            }
        };
        let alias = match self.catalogue.schema_version_aliases.get(&schema_version) {
            Some(alias) => *alias,
            None => SchemaVersionAlias(
                self.catalogue
                    .schema_version_aliases
                    .values()
                    .map(|alias| alias.0)
                    .max()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or(Error::InvalidStoredValue("schema version alias exhausted"))?,
            ),
        };
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue(
                "physical mapping schema payload missing",
            ))?
            .schema
            .clone();
        let mut candidate_mappings = self.catalogue.physical_mappings.clone();
        candidate_mappings.insert(schema_version, mapping);
        let mut candidate_aliases = self.catalogue.schema_version_aliases.clone();
        candidate_aliases.insert(schema_version, alias);
        for table in &schema.tables {
            allocate_physical_variant_cases(
                &mut candidate_mappings,
                &candidate_aliases,
                schema_version,
                &table.name,
                table
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
            )?;
        }
        let mapping =
            candidate_mappings
                .remove(&schema_version)
                .ok_or(Error::InvalidStoredValue(
                    "allocated physical mapping disappeared",
                ))?;
        let mut batch = self.database.open_batch();
        Self::write_schema_version_mapping_to_batch(&mut batch, alias, schema_version, &mapping)?;
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        self.catalogue
            .schema_version_aliases
            .insert(schema_version, alias);
        if schema_version == self.catalogue.current_schema_version_id {
            self.catalogue.current_schema_version_alias = Some(alias);
        }
        self.catalogue
            .physical_mappings
            .insert(schema_version, mapping);
        Ok(())
    }

    fn next_schema_version_alias(&self) -> Result<SchemaVersionAlias, Error> {
        Ok(SchemaVersionAlias(
            self.catalogue
                .schema_version_aliases
                .values()
                .map(|alias| alias.0)
                .chain(
                    self.catalogue
                        .physical_mappings
                        .values()
                        .flat_map(|mapping| mapping.tables.values())
                        .flat_map(|table| table.variant_cases.iter())
                        .map(|case| u64::from(case.tag)),
                )
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(Error::InvalidStoredValue("schema version alias exhausted"))?,
        ))
    }

    fn allocate_physical_table_id(&mut self) -> Result<PhysicalTableId, Error> {
        let id = PhysicalTableId(self.catalogue.next_physical_table_id);
        self.catalogue.next_physical_table_id = self
            .catalogue
            .next_physical_table_id
            .checked_add(1)
            .ok_or(Error::InvalidStoredValue("physical table id exhausted"))?;
        Ok(id)
    }

    fn allocate_physical_column_id(&mut self) -> Result<PhysicalColumnId, Error> {
        let id = PhysicalColumnId(self.catalogue.next_physical_column_id);
        self.catalogue.next_physical_column_id = self
            .catalogue
            .next_physical_column_id
            .checked_add(1)
            .ok_or(Error::InvalidStoredValue("physical column id exhausted"))?;
        Ok(id)
    }

    fn validate_pending_schema_lineage_physical_mapping(
        &self,
        pending: &PendingSchemaLineage,
    ) -> Result<(), Error> {
        if !self
            .catalogue
            .catalogue_schemas
            .contains_key(&pending.publication.lens.source)
        {
            return Ok(());
        }
        // Admission must not reserve durable catalogue state or consume local
        // physical ids for a lineage whose shared scalar registry cannot be
        // encoded. Allocate against copies; activation performs the identical
        // reconciliation after the pending envelope has passed this boundary.
        let mut next_table = self.catalogue.next_physical_table_id;
        let mut next_column = self.catalogue.next_physical_column_id;
        let provisional = allocate_provisional_physical_mapping(
            &pending.publication.schema.schema,
            pending.publication.physical_identities.clone(),
            &mut next_table,
            &mut next_column,
        )?;
        self.reconcile_physical_mapping_for_lens_payload(
            &pending.publication.lens,
            &pending.publication.schema,
            &provisional,
        )
        .map(|_| ())
    }

    fn validate_candidate_scalar_enum_capacity(
        catalogue: &SchemaCatalogue,
        candidate: &SchemaPhysicalMapping,
    ) -> Result<(), Error> {
        const SCALAR_CASE_CAPACITY: usize = u8::MAX as usize + 1;

        for candidate_table in candidate.tables.values() {
            for column_id in candidate_table.scalar_enum_cases.keys() {
                let mut cases = BTreeSet::new();
                for mapping in catalogue
                    .physical_mappings
                    .values()
                    .chain(std::iter::once(candidate))
                {
                    for table in mapping
                        .tables
                        .values()
                        .filter(|table| table.table_id == candidate_table.table_id)
                    {
                        if let Some(column_cases) = table.scalar_enum_cases.get(column_id) {
                            cases.extend(column_cases.iter().cloned());
                        }
                    }
                }
                if cases.len() > SCALAR_CASE_CAPACITY {
                    return Err(Error::InvalidCatalogueUpdate(
                        "physical scalar enum registry exceeds u8 capacity",
                    ));
                }
            }

            for (column_id, candidate_paths) in &candidate_table.nested_scalar_enum_cases {
                for path in candidate_paths.keys() {
                    let mut cases = BTreeSet::new();
                    for mapping in catalogue
                        .physical_mappings
                        .values()
                        .chain(std::iter::once(candidate))
                    {
                        for table in mapping
                            .tables
                            .values()
                            .filter(|table| table.table_id == candidate_table.table_id)
                        {
                            if let Some(column_cases) = table
                                .nested_scalar_enum_cases
                                .get(column_id)
                                .and_then(|paths| paths.get(path))
                            {
                                cases.extend(column_cases.iter().cloned());
                            }
                        }
                    }
                    if cases.len() > SCALAR_CASE_CAPACITY {
                        return Err(Error::InvalidCatalogueUpdate(
                            "physical scalar enum registry exceeds u8 capacity",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn reconcile_physical_mapping_for_lens(
        &mut self,
        lens: &MigrationLens,
    ) -> Result<SchemaPhysicalMapping, Error> {
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&lens.target)
            .ok_or(Error::InvalidStoredValue("target physical mapping missing"))?
            .clone();
        let target_schema = self
            .catalogue
            .catalogue_schemas
            .get(&lens.target)
            .ok_or(Error::InvalidStoredValue(
                "target physical mapping schema missing",
            ))?
            .clone();
        self.reconcile_physical_mapping_for_lens_payload(lens, &target_schema, &target_mapping)
    }

    fn reconcile_physical_mapping_for_lens_payload(
        &self,
        lens: &MigrationLens,
        target_schema_version: &SchemaVersion,
        provisional_target_mapping: &SchemaPhysicalMapping,
    ) -> Result<SchemaPhysicalMapping, Error> {
        Self::reconcile_physical_mapping_for_lens_payload_in_catalogue(
            &self.catalogue,
            lens,
            target_schema_version,
            provisional_target_mapping,
        )
    }

    fn reconcile_physical_mapping_for_lens_payload_in_catalogue(
        catalogue: &SchemaCatalogue,
        lens: &MigrationLens,
        target_schema_version: &SchemaVersion,
        provisional_target_mapping: &SchemaPhysicalMapping,
    ) -> Result<SchemaPhysicalMapping, Error> {
        let source_mapping = catalogue
            .physical_mappings
            .get(&lens.source)
            .ok_or(Error::InvalidStoredValue("source physical mapping missing"))?
            .clone();
        let mut target_mapping = provisional_target_mapping.clone();
        let source_schema = catalogue
            .catalogue_schemas
            .get(&lens.source)
            .ok_or(Error::InvalidStoredValue(
                "source physical mapping schema missing",
            ))?
            .schema
            .clone();
        let target_schema = &target_schema_version.schema;
        for table_lens in &lens.table_lenses {
            let source_table = source_mapping.tables.get(&table_lens.source_table).ok_or(
                Error::InvalidStoredValue("source physical table mapping missing"),
            )?;
            let provisional_target_table = target_mapping
                .tables
                .get(&table_lens.target_table)
                .ok_or(Error::InvalidStoredValue(
                    "target physical table mapping missing",
                ))?
                .clone();
            let source_table_schema = source_schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.source_table)
                .ok_or(Error::InvalidStoredValue(
                    "source physical table schema missing",
                ))?;
            let target_table_schema = target_schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.target_table)
                .ok_or(Error::InvalidStoredValue(
                    "target physical table schema missing",
                ))?;
            let target_table_identities = target_mapping
                .identities
                .tables
                .get(&table_lens.target_table)
                .ok_or(Error::InvalidStoredValue("target physical table identities missing"))?
                .clone();
            let mut columns = source_table
                .columns
                .iter()
                .map(|(name, id)| (name.clone(), (*id, Some(name.clone()))))
                .collect::<BTreeMap<_, _>>();
            for op in &table_lens.ops {
                match op {
                    LensOp::RenameTable { .. }
                    | LensOp::TransformColumn { .. }
                    | LensOp::RejectSourceDelta { .. } => {}
                    LensOp::RenameColumn { from, to } => {
                        if let Some(column_id) = columns.remove(from) {
                            columns.insert(to.clone(), column_id);
                        }
                    }
                    LensOp::CopyColumn { to, .. } | LensOp::AddColumn { column: to, .. } => {
                        let column_id = *provisional_target_table.columns.get(to).ok_or(
                            Error::InvalidStoredValue(
                                "target provisional physical column mapping missing",
                            ),
                        )?;
                        columns.insert(to.clone(), (column_id, None));
                    }
                    LensOp::DropColumn { column, .. } => {
                        columns.remove(column);
                    }
                }
            }
            let target_column_names = target_table_schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<BTreeSet<_>>();
            columns.retain(|column, _| target_column_names.contains(column));
            for column in &target_table_schema.columns {
                if !columns.contains_key(&column.name) {
                    let column_id = *provisional_target_table.columns.get(&column.name).ok_or(
                        Error::InvalidStoredValue(
                            "target provisional physical column mapping missing",
                        ),
                    )?;
                    columns.insert(column.name.clone(), (column_id, None));
                }
            }
            let columns = columns
                .into_iter()
                .map(|(target_name, (inherited_id, source_name))| {
                    let column_id = match source_name {
                        Some(source_name)
                            if physical_column_epoch_is_compatible(
                                source_table_schema,
                                &source_name,
                                target_table_schema,
                                &target_name,
                            ) =>
                        {
                            inherited_id
                        }
                        _ => *provisional_target_table.columns.get(&target_name).ok_or(
                            Error::InvalidStoredValue(
                                "target provisional physical column mapping missing",
                            ),
                        )?,
                    };
                    Ok((target_name, column_id))
                })
                .collect::<Result<BTreeMap<_, _>, Error>>()?;
            let mut scalar_enum_cases = BTreeMap::new();
            let mut payload_enum_cases = BTreeMap::new();
            for column in &target_table_schema.columns {
                let id = *columns
                    .get(&column.name)
                    .ok_or(Error::InvalidStoredValue("enum physical column missing"))?;
                match &column.column_type {
                    records::ValueType::EnumTag(enum_schema) => {
                        let mut cases = source_table
                            .scalar_enum_cases
                            .get(&id)
                            .cloned()
                            .unwrap_or_default();
                        if cases.len() > enum_schema.variants.len() {
                            return Err(Error::InvalidStoredValue(
                                "scalar enum registry changed non-additively",
                            ));
                        }
                        let ids = &target_table_identities.columns[&column.name].enum_variants["root"];
                        install_enum_case_ids(&mut cases, ids, target_schema_version.id)?;
                        scalar_enum_cases.insert(id, cases);
                    }
                    records::ValueType::Enum(enum_schema) => {
                        let mut cases = source_table
                            .payload_enum_cases
                            .get(&id)
                            .cloned()
                            .unwrap_or_default();
                        if cases.len() > enum_schema.cases.len() {
                            return Err(Error::InvalidStoredValue(
                                "payload enum registry changed non-additively",
                            ));
                        }
                        let ids = &target_table_identities.columns[&column.name].enum_variants["root"];
                        install_payload_enum_case_ids(
                            &mut cases,
                            ids,
                            target_schema_version.id,
                        )?;
                        payload_enum_cases.insert(id, cases);
                    }
                    _ => {}
                }
            }
            let mut nested_scalar_enum_cases = source_table.nested_scalar_enum_cases.clone();
            let mut nested_payload_enum_cases = source_table.nested_payload_enum_cases.clone();
            for column in &target_table_schema.columns {
                let id = *columns.get(&column.name).ok_or(Error::InvalidStoredValue(
                    "nested enum physical column missing",
                ))?;
                let nested = nested_scalar_enum_cases.entry(id).or_default();
                let identities = &target_table_identities.columns[&column.name].enum_variants;
                hydrate_nested_scalar_enum_cases(
                    &column.column_type,
                    identities,
                    target_schema_version.id,
                    "root",
                    "root",
                    nested,
                )?;
                let nested_payload = nested_payload_enum_cases.entry(id).or_default();
                hydrate_nested_payload_enum_cases(
                    &column.column_type,
                    identities,
                    target_schema_version.id,
                    "root",
                    "root",
                    nested_payload,
                )?;
            }
            // Registry entries belong to a live physical column.  Leaving an
            // entry behind for a dropped column makes otherwise identical
            // cross-lens mappings compare unequal solely because one path had
            // an intermediate column epoch.
            let live_columns = columns.values().copied().collect::<BTreeSet<_>>();
            scalar_enum_cases.retain(|column, _| live_columns.contains(column));
            payload_enum_cases.retain(|column, _| live_columns.contains(column));
            nested_scalar_enum_cases.retain(|column, _| live_columns.contains(column));
            nested_payload_enum_cases.retain(|column, _| live_columns.contains(column));
            target_mapping.tables.insert(
                table_lens.target_table.clone(),
                TablePhysicalMapping {
                    table_id: source_table.table_id,
                    columns,
                    variant_cases: Vec::new(),
                    scalar_enum_cases,
                    payload_enum_cases,
                    nested_scalar_enum_cases,
                    nested_payload_enum_cases,
                },
            );
        }
        Self::validate_candidate_scalar_enum_capacity(catalogue, &target_mapping)?;
        Ok(target_mapping)
    }

    fn reconcile_source_physical_mapping_for_lens_payload(
        lens: &MigrationLens,
        source_schema_version: &SchemaVersion,
        target_schema_version: &SchemaVersion,
        provisional_source_mapping: &SchemaPhysicalMapping,
        target_mapping: &SchemaPhysicalMapping,
    ) -> Result<SchemaPhysicalMapping, Error> {
        let mut source_mapping = provisional_source_mapping.clone();
        for table_lens in &lens.table_lenses {
            let provisional_source_table = source_mapping
                .tables
                .get(&table_lens.source_table)
                .ok_or(Error::InvalidStoredValue(
                    "source provisional physical table mapping missing",
                ))?
                .clone();
            let target_table = target_mapping.tables.get(&table_lens.target_table).ok_or(
                Error::InvalidStoredValue("target physical table mapping missing"),
            )?;
            let source_table_schema = source_schema_version
                .schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.source_table)
                .ok_or(Error::InvalidStoredValue(
                    "source physical table schema missing",
                ))?;
            let target_table_schema = target_schema_version
                .schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.target_table)
                .ok_or(Error::InvalidStoredValue(
                    "target physical table schema missing",
                ))?;

            let mut target_name_by_source = source_table_schema
                .columns
                .iter()
                .map(|column| (column.name.clone(), Some(column.name.clone())))
                .collect::<BTreeMap<_, _>>();
            for op in &table_lens.ops {
                match op {
                    LensOp::RenameColumn { from, to } => {
                        for target_name in target_name_by_source.values_mut() {
                            if target_name.as_deref() == Some(from.as_str()) {
                                *target_name = Some(to.clone());
                                break;
                            }
                        }
                    }
                    LensOp::DropColumn { column, .. } => {
                        for target_name in target_name_by_source.values_mut() {
                            if target_name.as_deref() == Some(column.as_str()) {
                                *target_name = None;
                                break;
                            }
                        }
                    }
                    LensOp::RenameTable { .. }
                    | LensOp::CopyColumn { .. }
                    | LensOp::AddColumn { .. }
                    | LensOp::TransformColumn { .. }
                    | LensOp::RejectSourceDelta { .. } => {}
                }
            }

            let columns = source_table_schema
                .columns
                .iter()
                .map(|source_column| {
                    let provisional_id = provisional_source_table
                        .columns
                        .get(&source_column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "source provisional physical column mapping missing",
                        ))?;
                    let column_id = target_name_by_source
                        .get(&source_column.name)
                        .and_then(|target_name| target_name.as_deref())
                        .and_then(|target_name| {
                            physical_column_epoch_is_compatible(
                                source_table_schema,
                                &source_column.name,
                                target_table_schema,
                                target_name,
                            )
                            .then(|| target_table.columns.get(target_name).copied())
                            .flatten()
                        })
                        .unwrap_or(provisional_id);
                    Ok((source_column.name.clone(), column_id))
                })
                .collect::<Result<BTreeMap<_, _>, Error>>()?;
            let live_column_ids = columns.values().copied().collect::<BTreeSet<_>>();
            let mut scalar_enum_cases = provisional_source_table.scalar_enum_cases.clone();
            scalar_enum_cases.extend(target_table.scalar_enum_cases.clone());
            scalar_enum_cases.retain(|id, _| live_column_ids.contains(id));
            let mut payload_enum_cases = provisional_source_table.payload_enum_cases.clone();
            payload_enum_cases.extend(target_table.payload_enum_cases.clone());
            payload_enum_cases.retain(|id, _| live_column_ids.contains(id));
            let mut nested_scalar_enum_cases =
                provisional_source_table.nested_scalar_enum_cases.clone();
            nested_scalar_enum_cases.extend(target_table.nested_scalar_enum_cases.clone());
            nested_scalar_enum_cases.retain(|id, _| live_column_ids.contains(id));
            let mut nested_payload_enum_cases =
                provisional_source_table.nested_payload_enum_cases.clone();
            nested_payload_enum_cases.extend(target_table.nested_payload_enum_cases.clone());
            nested_payload_enum_cases.retain(|id, _| live_column_ids.contains(id));
            source_mapping.tables.insert(
                table_lens.source_table.clone(),
                TablePhysicalMapping {
                    table_id: target_table.table_id,
                    columns,
                    variant_cases: target_table.variant_cases.clone(),
                    scalar_enum_cases,
                    payload_enum_cases,
                    nested_scalar_enum_cases,
                    nested_payload_enum_cases,
                },
            );
        }
        Ok(source_mapping)
    }

    async fn persist_catalogue_schema_lineage(
        &mut self,
        staged: &StagedSchemaLineage,
    ) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::SchemaLineageStaged.key()),
                Value::Uuid(staged.publication.id.0),
                Value::Bytes(codec::encode_catalogue_staged_lineage(staged)?),
            ],
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        Ok(())
    }

    async fn persist_pending_schema_lineage(
        &mut self,
        pending: &PendingSchemaLineage,
    ) -> Result<(), Error> {
        self.validate_pending_schema_lineage_physical_mapping(pending)?;
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::SchemaLineagePending.key()),
                Value::Uuid(pending.publication.id.0),
                Value::Bytes(codec::encode_catalogue_pending_lineage(pending)?),
            ],
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        Ok(())
    }

    async fn remove_pending_schema_lineage(
        &mut self,
        catalogue_seq: u64,
        publication_id: SchemaLineagePublicationId,
    ) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.delete(
            "jazz_catalogue",
            PrimaryKeyValue::Composite(vec![
                PrimaryKeyValue::U64(codec::CatalogueRecordKind::SchemaLineagePending.key()),
                PrimaryKeyValue::Uuid(publication_id.0),
            ]),
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        self.catalogue.pending_lineages.remove(&catalogue_seq);
        Ok(())
    }

    fn write_active_schema_lineage_to_batch(
        batch: &mut DatabaseBatch,
        staged: &StagedSchemaLineage,
    ) -> Result<(), Error> {
        let schema = &staged.publication.schema;
        let lens = &staged.publication.lens;
        // The active receipt retains the canonical staged payload as its
        // durable witness.  The pending envelope, however, is an obligation
        // to activate: leaving it behind after this batch commits would make
        // reopen retain a stale retry candidate beside an already-active
        // lineage.  Delete it in the same batch that makes the schema, lens,
        // mapping, and active receipt visible, so an active recovery state
        // never also carries a stale pending obligation.
        batch.delete(
            "jazz_catalogue",
            PrimaryKeyValue::Composite(vec![
                PrimaryKeyValue::U64(codec::CatalogueRecordKind::SchemaLineagePending.key()),
                PrimaryKeyValue::Uuid(staged.publication.id.0),
            ]),
        );
        // The active marker only carries an id and sequence. Persist its
        // canonical payload in the same durable activation batch so recovery
        // can prove both the prefix identity and its exact sequence.
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::SchemaLineageStaged.key()),
                Value::Uuid(staged.publication.id.0),
                Value::Bytes(codec::encode_catalogue_staged_lineage(staged)?),
            ],
        );
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::Schema.key()),
                Value::Uuid(schema.id.0),
                Value::Bytes(codec::encode_catalogue_schema(schema)?),
            ],
        );
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::Lens.key()),
                Value::Uuid(lens.id.0),
                Value::Bytes(codec::encode_catalogue_lens(lens)),
            ],
        );
        Self::write_schema_version_mapping_to_batch(
            batch,
            staged.alias,
            schema.id,
            &staged.mapping,
        )?;
        let active = SchemaLineageActivation {
            id: staged.publication.id,
            catalogue_seq: staged.catalogue_seq,
        };
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::SchemaLineageActive.key()),
                Value::Uuid(active.id.0),
                Value::Bytes(codec::encode_catalogue_lineage_activation(active)),
            ],
        );
        Ok(())
    }

    async fn persist_catalogue_lens_with_physical_metadata(
        &mut self,
        lens: &MigrationLens,
        mapping: Option<&SchemaPhysicalMapping>,
    ) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::Lens.key()),
                Value::Uuid(lens.id.0),
                Value::Bytes(codec::encode_catalogue_lens(lens)),
            ],
        );
        if let Some(mapping) = mapping {
            let alias = *self
                .catalogue
                .schema_version_aliases
                .get(&lens.target)
                .ok_or(Error::InvalidStoredValue(
                    "physical mapping schema alias missing",
                ))?;
            Self::write_schema_version_mapping_to_batch(&mut batch, alias, lens.target, mapping)?;
        }
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        Ok(())
    }

    fn write_schema_version_mapping_to_batch(
        batch: &mut DatabaseBatch,
        alias: SchemaVersionAlias,
        schema_version: SchemaVersionId,
        mapping: &SchemaPhysicalMapping,
    ) -> Result<(), Error> {
        batch.update(
            "jazz_schema_versions",
            vec![
                Value::U64(alias.0),
                Value::Uuid(schema_version.0),
                Value::Bytes(codec::encode_physical_mapping(mapping)?),
            ],
        );
        Ok(())
    }

    async fn persist_catalogue_pointer(
        &mut self,
        pointer: CurrentWriteSchema,
    ) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue_pointer",
            vec![Value::U64(pointer.revision), Value::Uuid(pointer.schema.0)],
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        Ok(())
    }

    async fn persist_pending_catalogue_pointer(
        &mut self,
        pointer: CurrentWriteSchema,
    ) -> Result<(), Error> {
        let id = codec::catalogue_write_pointer_id(pointer);
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue",
            vec![
                Value::U64(codec::CatalogueRecordKind::WritePointerPending.key()),
                Value::Uuid(id),
                Value::Bytes(codec::encode_catalogue_write_pointer(pointer)),
            ],
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        Ok(())
    }

    async fn ensure_node_alias(&mut self, node_uuid: NodeUuid) -> Result<NodeAlias, Error> {
        if node_uuid == self.node_uuid
            && let Some(alias) = self.self_node_alias
        {
            return Ok(alias);
        }
        if let Some(alias) = self.node_aliases.get(&node_uuid) {
            if node_uuid == self.node_uuid {
                self.self_node_alias = Some(*alias);
            }
            return Ok(*alias);
        }
        let mut max_alias = self
            .node_aliases
            .values()
            .map(|alias| alias.0)
            .max()
            .unwrap_or(0);
        for raw in self
            .database
            .primary_key_scan_raw("jazz_nodes", &[])
            .await?
        {
            let record = raw.record();
            let alias = NodeAlias(record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?);
            max_alias = max_alias.max(alias.0);
            if record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)? == node_uuid.0 {
                self.node_aliases.insert(node_uuid, alias);
                if node_uuid == self.node_uuid {
                    self.self_node_alias = Some(alias);
                }
                return Ok(alias);
            }
        }
        let alias = NodeAlias(
            max_alias
                .checked_add(1)
                .ok_or(Error::InvalidStoredValue("node alias exhausted"))?,
        );
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_nodes",
            vec![Value::U64(alias.0), Value::Uuid(node_uuid.0)],
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        // This mapping is a durable prerequisite for every later row that
        // contains the compact alias.  Do not leave an in-memory alias behind
        // if resident application or ordered persistence fails: the caller may
        // only observe it after its catalogue row is durable.
        self.node_aliases.insert(node_uuid, alias);
        if node_uuid == self.node_uuid {
            self.self_node_alias = Some(alias);
        }
        Ok(alias)
    }

    async fn ensure_schema_version_alias(
        &mut self,
        schema_version_id: SchemaVersionId,
    ) -> Result<SchemaVersionAlias, Error> {
        if let Some(alias) = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version_id)
        {
            if schema_version_id == self.catalogue.current_schema_version_id {
                self.catalogue.current_schema_version_alias = Some(*alias);
            }
            return Ok(*alias);
        }
        self.ensure_provisional_physical_mapping(schema_version_id)
            .await?;
        self.catalogue
            .schema_version_aliases
            .get(&schema_version_id)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "schema version alias allocation failed",
            ))
    }

    pub(super) fn schema_version_for_alias(
        &self,
        alias: SchemaVersionAlias,
    ) -> Option<SchemaVersionId> {
        self.catalogue
            .schema_version_aliases
            .iter()
            .find_map(|(id, candidate)| (*candidate == alias).then_some(*id))
    }

    async fn record_child_edges(&mut self, child: TxId, parents: impl IntoIterator<Item = TxId>) {
        if self
            .query_transaction(child)
            .await
            .ok()
            .flatten()
            .is_some_and(|tx| !matches!(tx.fate, Fate::Pending))
        {
            return;
        }
        for parent in parents {
            if self
                .query_transaction(parent)
                .await
                .ok()
                .flatten()
                .is_some_and(|tx| !matches!(tx.fate, Fate::Pending))
            {
                continue;
            }
            self.rejections
                .child_txs_by_parent
                .entry(parent)
                .or_default()
                .insert(child);
        }
    }

    fn prune_child_edges(&mut self, child: TxId) {
        self.rejections.child_txs_by_parent.retain(|_, children| {
            children.remove(&child);
            !children.is_empty()
        });
    }

}

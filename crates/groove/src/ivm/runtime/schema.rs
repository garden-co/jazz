//! Runtime schema registration, variant projection, and snapshot reads.

use super::*;

impl IvmRuntime {
    pub(crate) fn register_table(&mut self, table: TableSchema) -> Result<(), IvmRuntimeError> {
        if self.schema.table(&table.name).is_some() {
            return Err(IvmRuntimeError::TableAlreadyExists(table.name));
        }
        self.table_storage_descriptors
            .insert(table.name.clone(), table.record_schema());
        if !table.has_variants() {
            self.table_descriptors
                .insert(table.name.clone(), table.record_schema());
        } else {
            let descriptors = self
                .variant_descriptors
                .entry(table.name.clone())
                .or_default();
            for variant in &table.variants {
                if let Some(descriptor) = table.record_schema_for_variant(variant.tag) {
                    descriptors.insert(variant.tag, descriptor);
                }
            }
        }
        self.schema.tables.push(table);
        self.define_schema_index_variant_projections()?;
        self.add_dedup_schema_indices()?;
        Ok(())
    }

    pub(crate) fn register_table_variant_with_columns(
        &mut self,
        table: &str,
        columns: Vec<crate::schema::ColumnSchema>,
        variant_tag: crate::schema::TableVariant,
    ) -> Result<(), IvmRuntimeError> {
        let table_schema = self
            .schema
            .tables
            .iter_mut()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?;
        if table_schema.variant(variant_tag.tag).is_some() {
            return Err(IvmRuntimeError::DuplicateTableVariant {
                table: table.to_owned(),
                version: u64::from(variant_tag.tag),
            });
        }
        for column in columns {
            if table_schema
                .columns
                .iter()
                .any(|existing| existing.name == column.name)
            {
                return Err(IvmRuntimeError::TableFieldAlreadyExists {
                    table: table.to_owned(),
                    field: column.name,
                });
            }
            table_schema.columns.push(column);
        }
        let version = variant_tag.tag;
        for field in &variant_tag.payload_fields {
            field
                .value_type
                .collect_variant_registries(&mut table_schema.value_variant_registries);
        }
        table_schema.variants.push(variant_tag);
        self.table_storage_descriptors
            .insert(table.to_owned(), table_schema.record_schema());
        let descriptor = table_schema
            .record_schema_for_variant(version)
            .expect("the newly registered variant exists");
        self.variant_descriptors
            .entry(table.to_owned())
            .or_default()
            .insert(version, descriptor);
        let table_schema = table_schema.clone();
        for index in &table_schema.indices {
            self.register_schema_index_variant_case(&table_schema, index, version)?;
        }
        self.invalidate_table_inputs(table);
        Ok(())
    }

    pub(crate) fn evolve_table_variant_registries(
        &mut self,
        table: &str,
        columns: &[crate::schema::ColumnSchema],
    ) -> Result<(), IvmRuntimeError> {
        let table_schema = self
            .schema
            .tables
            .iter_mut()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?;
        for desired in columns {
            let Some(existing) = table_schema
                .columns
                .iter()
                .find(|column| column.name == desired.name)
            else {
                continue;
            };
            if !existing
                .column_type
                .can_evolve_registry_to(&desired.column_type)
            {
                return Err(IvmRuntimeError::TableFieldAlreadyExists {
                    table: table.to_owned(),
                    field: desired.name.clone(),
                });
            }
            for variant in &table_schema.variants {
                for field in &variant.payload_fields {
                    if field.shared_column.as_deref() != Some(desired.name.as_str()) {
                        continue;
                    }
                    if !field
                        .value_type
                        .can_evolve_registry_to(&desired.column_type)
                    {
                        return Err(IvmRuntimeError::TableFieldAlreadyExists {
                            table: table.to_owned(),
                            field: desired.name.clone(),
                        });
                    }
                }
            }
        }
        for desired in columns {
            let Some(existing) = table_schema
                .columns
                .iter_mut()
                .find(|column| column.name == desired.name)
            else {
                continue;
            };
            existing.column_type = desired.column_type.clone();
            for variant in &mut table_schema.variants {
                for field in &mut variant.payload_fields {
                    if field.shared_column.as_deref() == Some(desired.name.as_str()) {
                        field.value_type = desired.column_type.clone();
                    }
                }
            }
            desired
                .column_type
                .collect_variant_registries(&mut table_schema.value_variant_registries);
        }
        self.table_storage_descriptors
            .insert(table.to_owned(), table_schema.record_schema());
        let mut descriptors = HashMap::default();
        for variant in &table_schema.variants {
            let descriptor = table_schema
                .record_schema_for_variant(variant.tag)
                .expect("registered variants have valid descriptors");
            descriptors.insert(variant.tag, descriptor);
        }
        self.variant_descriptors
            .insert(table.to_owned(), descriptors);
        self.invalidate_table_inputs(table);
        Ok(())
    }

    pub(crate) async fn register_table_index<S>(
        &mut self,
        table: &str,
        index: IndexSchema,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let mut staged = self.clone();
        staged
            .register_table_index_staged(table, index, storage)
            .await?;
        *self = staged;
        Ok(())
    }

    async fn register_table_index_staged<S>(
        &mut self,
        table: &str,
        index: IndexSchema,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let table_position = self
            .schema
            .tables
            .iter()
            .position(|candidate| candidate.name == table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?;
        self.schema.tables[table_position]
            .indices
            .push(index.clone());
        let table_schema = self.schema.tables[table_position].clone();
        if table_schema.has_variants() {
            let target = VariantProjectionTarget::SchemaIndex(index.name.clone());
            self.define_variant_projection_target(
                table,
                target,
                schema_index_input_descriptor(&table_schema, &index)?,
            )?;
            for variant_tag in &table_schema.variants {
                self.register_schema_index_variant_case(&table_schema, &index, variant_tag.tag)?;
            }
        }
        let persist = self.add_dedup_schema_index(&table_schema, &index)?;
        self.invalidate_table_inputs(table);
        let persist_node = self
            .graph
            .node(persist)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(persist))?;
        let OpType::Persist(persist_op) = persist_node.descriptor.operator.clone() else {
            return Err(IvmRuntimeError::UnsupportedOperator);
        };
        let [input] = persist_node.descriptor.inputs.as_slice() else {
            return Err(IvmRuntimeError::GraphInputArityMismatch(persist));
        };
        let input = *input;
        let snapshot = self
            .hydration_snapshot(input, storage, HydrationMode::Ordinary)
            .await?;
        apply_persist_delta(
            storage,
            &persist_op.storage,
            &persist_op.key_fields,
            persist_op.unique,
            &snapshot,
        )
        .await?;
        Ok(())
    }

    pub(crate) fn define_variant_projection(
        &mut self,
        table: &str,
        target: &str,
        output: RecordDescriptor,
    ) -> Result<(), IvmRuntimeError> {
        self.define_variant_projection_target(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            output,
        )
    }

    fn define_variant_projection_target(
        &mut self,
        table: &str,
        target: VariantProjectionTarget,
        output: RecordDescriptor,
    ) -> Result<(), IvmRuntimeError> {
        if self.schema.table(table).is_none() {
            return Err(IvmRuntimeError::TableNotFound(table.to_owned()));
        }
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: target.clone(),
        };
        match self.variant_projections.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(VariantProjection {
                    output,
                    cases: HashMap::default(),
                });
            }
            std::collections::hash_map::Entry::Occupied(mut entry)
                if record_descriptors_registry_compatible(&entry.get().output, &output) =>
            {
                entry.get_mut().output = output;
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(IvmRuntimeError::VariantProjectionOutputMismatch {
                    table: table.to_owned(),
                    target: variant_projection_target_name(&target).to_owned(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn register_variant_projection_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: &[ProjectField],
    ) -> Result<(), IvmRuntimeError> {
        self.register_variant_projection_target_case(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            variant_tag,
            Some(fields),
            false,
            false,
        )
    }

    /// Refresh a raw variant source descriptor after append-only enum registry
    /// evolution. The existing mapping must remain byte-for-byte identical.
    pub(crate) fn refresh_variant_projection_case_for_registry_evolution(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: &[ProjectField],
    ) -> Result<(), IvmRuntimeError> {
        self.register_variant_projection_target_case(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            variant_tag,
            Some(fields),
            false,
            true,
        )
    }

    /// Register a schema-read case whose unrepresentable enum values are
    /// source-local row exclusions. This keeps old-client compatibility at
    /// the source boundary rather than swallowing errors downstream.
    pub(crate) fn register_variant_projection_case_omitting_unrepresentable_enums(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: &[ProjectField],
    ) -> Result<(), IvmRuntimeError> {
        self.register_variant_projection_target_case(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            variant_tag,
            Some(fields),
            true,
            false,
        )
    }

    /// Append a physical source case that constructs one stable logical enum
    /// value. The outer projection descriptor stays fixed; the selected enum
    /// case owns the dense payload descriptor used for this source layout.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register_variant_projection_enum_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        enum_field: &str,
        enum_schema: &EnumSchema,
        enum_case: &str,
        fields: &[ProjectField],
    ) -> Result<(), IvmRuntimeError> {
        let source = self
            .schema
            .table(table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?
            .record_schema_for_variant(variant_tag)
            .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                table: table.to_owned(),
                version: u64::from(variant_tag),
            })?;
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: VariantProjectionTarget::Named(target.to_owned()),
        };
        let projection = self.variant_projections.get_mut(&key).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
            }
        })?;
        let output_fields = projection.output.fields();
        let Some(union_idx) = projection.output.field_index(enum_field) else {
            return Err(IvmRuntimeError::VariantProjectionEnumFieldNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
                field: enum_field.to_owned(),
            });
        };
        if output_fields.len() != 1 {
            return Err(
                IvmRuntimeError::VariantProjectionEnumOutputMustBeSingleField {
                    table: table.to_owned(),
                    target: target.to_owned(),
                },
            );
        }
        let ValueType::Enum(output_schema) = &output_fields[union_idx].value_type else {
            return Err(IvmRuntimeError::VariantProjectionEnumFieldTypeMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                field: enum_field.to_owned(),
            });
        };
        if output_schema.as_ref() != enum_schema {
            return Err(IvmRuntimeError::VariantProjectionEnumSchemaMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                field: enum_field.to_owned(),
            });
        }
        let tag = enum_schema.tag(enum_case)?;
        let payload = enum_schema.case(tag)?.payload;
        let projected = project_descriptor(&source, fields)?;
        if projected != payload {
            return Err(IvmRuntimeError::VariantProjectionEnumPayloadMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                case: enum_case.to_owned(),
            });
        }
        let mapping = fields
            .iter()
            .filter_map(|field| {
                field.source().map(|source_field| {
                    resolve_field_ref(&source, source_field).map(|idx| (0, idx))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let project = MapProjectOp {
            expressions: fields
                .iter()
                .map(|field| {
                    project_field_expr(&source, field).map(|expression| ProjectionExpr {
                        expression,
                        output_name: Some(field.output_name.clone()),
                    })
                })
                .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
            mapping,
        };
        let raw_projection = raw_projection_fields(&project, &source, payload)?
            .map(Arc::<[RawProjectionField]>::from);
        let case = VariantProjectionCase::Enum {
            source,
            tag,
            payload,
            project,
            raw_projection,
        };
        match projection.cases.entry(variant_tag) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(case);
            }
            std::collections::hash_map::Entry::Occupied(entry) if entry.get() == &case => {}
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(IvmRuntimeError::VariantProjectionCaseAlreadyRegistered {
                    table: table.to_owned(),
                    target: target.to_owned(),
                    version: u64::from(variant_tag),
                });
            }
        }
        self.invalidate_table_inputs(table);
        Ok(())
    }

    pub(crate) fn register_variant_projection_ignore_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
    ) -> Result<(), IvmRuntimeError> {
        self.register_variant_projection_target_case(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            variant_tag,
            None,
            false,
            false,
        )
    }

    pub(crate) fn project_variant_record(
        &self,
        table: &str,
        target: &str,
        record: &records::VariantRecord,
    ) -> Result<Option<OwnedRecord>, IvmRuntimeError> {
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: VariantProjectionTarget::Named(target.to_owned()),
        };
        let projection = self.variant_projections.get(&key).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
            }
        })?;
        let case = projection.cases.get(&record.variant_tag()).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionCaseNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
                version: u64::from(record.variant_tag()),
            }
        })?;
        if !case
            .source()
            .registry_compatible_with(record.record().descriptor())
        {
            return Err(IvmRuntimeError::VariantProjectionSourceMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                version: u64::from(record.variant_tag()),
            });
        }
        let input = RecordDeltas {
            descriptor: *record.record().descriptor(),
            deltas: vec![RecordDelta {
                record: Bytes::copy_from_slice(record.record().raw()),
                weight: 1,
            }],
        };
        let projected = match case {
            VariantProjectionCase::Project {
                project,
                raw_projection,
                omit_unrepresentable_enum_rows,
                ..
            } => NodeState::update_map_project(
                project,
                projection.output,
                &input,
                raw_projection.as_deref(),
                *omit_unrepresentable_enum_rows,
            )?,
            VariantProjectionCase::Enum {
                tag,
                payload,
                project,
                raw_projection,
                ..
            } => NodeState::update_variant_enum_project(
                *tag,
                *payload,
                project,
                projection.output,
                &input,
                raw_projection.as_deref(),
            )?,
            VariantProjectionCase::Ignore { .. } => return Ok(None),
        };
        let Some(record) = projected.deltas.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(OwnedRecord::new(
            record.record.to_vec(),
            projection.output,
        )))
    }

    fn register_variant_projection_target_case(
        &mut self,
        table: &str,
        target: VariantProjectionTarget,
        variant_tag: u32,
        fields: Option<&[ProjectField]>,
        omit_unrepresentable_enum_rows: bool,
        allow_registry_refresh: bool,
    ) -> Result<(), IvmRuntimeError> {
        let source = self
            .schema
            .table(table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?
            .record_schema_for_variant(variant_tag)
            .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                table: table.to_owned(),
                version: u64::from(variant_tag),
            })?;
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: target.clone(),
        };
        let projection = self.variant_projections.get_mut(&key).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionNotFound {
                table: table.to_owned(),
                target: variant_projection_target_name(&target).to_owned(),
            }
        })?;
        // An explicitly typed literal is also a descriptor boundary: it does
        // not copy the source cell at all, so a physical enum registry in the
        // input cannot leak through it.  Jazz uses this for requirement-none
        // auxiliary sources, whose enum cells are deliberately typed-null.
        let allow_recursive_replacement = fields.is_some_and(|fields| {
            fields.iter().any(|field| {
                matches!(
                    field.expression,
                    ProjectExpr::RecursiveEnumRemap { .. } | ProjectExpr::TypedLiteral { .. }
                )
            })
        });
        let case = if let Some(fields) = fields {
            let projected = project_descriptor(&source, fields)?;
            // A recursive enum projector is an explicit descriptor boundary:
            // its source and output may intentionally name different enum
            // registries. The operation validates and re-encodes the values
            // against the fixed output descriptor at execution time, so the
            // usual raw-descriptor compatibility shortcut is inapplicable.
            if !allow_recursive_replacement
                && !record_descriptors_registry_compatible(&projected, &projection.output)
            {
                return Err(IvmRuntimeError::VariantProjectionOutputMismatch {
                    table: table.to_owned(),
                    target: variant_projection_target_name(&target).to_owned(),
                });
            }
            let mapping = fields
                .iter()
                .filter_map(|field| {
                    field.source().map(|source_field| {
                        resolve_field_ref(&source, source_field).map(|idx| (0, idx))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let project = MapProjectOp {
                expressions: fields
                    .iter()
                    .map(|field| {
                        project_field_expr(&source, field).map(|expression| ProjectionExpr {
                            expression,
                            output_name: Some(field.output_name.clone()),
                        })
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                mapping,
            };
            let raw_projection = raw_projection_fields(&project, &source, projection.output)?
                .map(Arc::<[RawProjectionField]>::from);
            VariantProjectionCase::Project {
                source,
                project,
                raw_projection,
                omit_unrepresentable_enum_rows,
            }
        } else {
            VariantProjectionCase::Ignore { source }
        };
        let changed = match projection.cases.entry(variant_tag) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(case);
                true
            }
            std::collections::hash_map::Entry::Occupied(entry) if entry.get() == &case => false,
            std::collections::hash_map::Entry::Occupied(mut entry)
                if allow_recursive_replacement =>
            {
                // The physical registry can append while this authored target
                // stays fixed. Refresh its non-total tag map in place.
                entry.insert(case);
                true
            }
            std::collections::hash_map::Entry::Occupied(mut entry)
                if allow_registry_refresh && entry.get().can_refresh_registries_to(&case) =>
            {
                entry.insert(case);
                true
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(IvmRuntimeError::VariantProjectionCaseAlreadyRegistered {
                    table: table.to_owned(),
                    target: variant_projection_target_name(&target).to_owned(),
                    version: u64::from(variant_tag),
                });
            }
        };
        if changed {
            self.invalidate_table_inputs(table);
        }
        Ok(())
    }

    pub(super) fn define_schema_index_variant_projections(
        &mut self,
    ) -> Result<(), IvmRuntimeError> {
        for table in self.schema.tables.clone() {
            if !table.has_variants() {
                continue;
            }
            for index in &table.indices {
                let target = VariantProjectionTarget::SchemaIndex(index.name.clone());
                self.define_variant_projection_target(
                    &table.name,
                    target,
                    schema_index_input_descriptor(&table, index)?,
                )?;
                for variant_tag in &table.variants {
                    self.register_schema_index_variant_case(&table, index, variant_tag.tag)?;
                }
            }
        }
        Ok(())
    }

    fn register_schema_index_variant_case(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
        variant_tag: u32,
    ) -> Result<(), IvmRuntimeError> {
        let version =
            table
                .variant(variant_tag)
                .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                    table: table.name.clone(),
                    version: u64::from(variant_tag),
                })?;
        let fields = schema_index_input_fields(table, index)?
            .into_iter()
            .map(|shared| {
                version
                    .payload_name_for_shared(&shared)
                    .map(|local| ProjectField::renamed(local, shared))
            })
            .collect::<Option<Vec<_>>>();
        self.register_variant_projection_target_case(
            &table.name,
            VariantProjectionTarget::SchemaIndex(index.name.clone()),
            variant_tag,
            fields.as_deref(),
            false,
            false,
        )
    }

    fn invalidate_table_inputs(&mut self, table: &str) {
        *self.table_frontiers.entry(table.to_owned()).or_default() += 1;
        self.eval_memo.retain(|key, _| {
            self.node_meta
                .get(&key.node)
                .and_then(|meta| meta.input_signature.as_ref())
                .is_none_or(|signature| {
                    !signature.tables.iter().any(|candidate| candidate == table)
                })
        });
        self.eval_memo_bytes = self
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
    }

    pub fn index(&self, table: &str, index_name: &str) -> Option<&IndexSchema> {
        self.table(table)?
            .indices
            .iter()
            .find(|index| index.name == index_name)
    }

    pub fn direct_record_store(
        &self,
        store: &str,
    ) -> Option<&crate::schema::DirectRecordStoreSchema> {
        self.schema.direct_record_store(store)
    }

    pub async fn query_snapshot<S>(
        &mut self,
        graph: GraphBuilder,
        storage: &S,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.flush_pending_binding_retractions(storage).await?;
        if builder_contains_binding_source(&graph) {
            return Err(IvmRuntimeError::BindingSourceRequiresPrepare);
        }
        let mut query = super::graph_lifecycle::EphemeralGraphInstall::new(self);
        let runtime = query.runtime();
        runtime.logical_nodes_requested += count_builder_nodes(&graph) as u64;
        let CompiledNode {
            output,
            node: output_node,
            ..
        } = runtime.add_dedup_graph(&graph)?;
        let records = runtime
            .hydration_snapshot(output_node, storage, HydrationMode::Ordinary)
            .await?;
        if !records.descriptor.registry_compatible_with(&output) {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        Ok(records)
    }

    pub async fn query_snapshots<I, K, S>(
        &mut self,
        sinks: I,
        storage: &S,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
        S: OrderedKvStorage,
    {
        let sinks = sinks
            .into_iter()
            .map(|(sink, graph)| (sink.into(), graph))
            .collect::<Vec<_>>();
        self.flush_pending_binding_retractions(storage).await?;
        if sinks.is_empty() {
            return Err(IvmRuntimeError::EmptyMultisinkSubscription);
        }
        let mut sink_names = HashSet::new();
        for (sink, graph) in &sinks {
            if !sink_names.insert(sink.clone()) {
                return Err(IvmRuntimeError::DuplicateMultisinkSink(sink.clone()));
            }
            if builder_contains_binding_source(graph) {
                return Err(IvmRuntimeError::MultisinkSinkRequiresPrepare(sink.clone()));
            }
        }
        let mut query = super::graph_lifecycle::EphemeralGraphInstall::new(self);
        let runtime = query.runtime();
        runtime.logical_nodes_requested += sinks
            .iter()
            .map(|(_, graph)| count_builder_nodes(graph))
            .sum::<usize>() as u64;
        let mut outputs = BTreeMap::new();
        for (sink, graph) in sinks {
            outputs.insert(sink, runtime.add_dedup_graph(&graph)?);
        }
        runtime
            .hydration_snapshots(&outputs, storage, HydrationMode::Ordinary)
            .await
    }
}

fn record_descriptors_registry_compatible(
    left: &RecordDescriptor,
    right: &RecordDescriptor,
) -> bool {
    left.registry_compatible_with(right)
}

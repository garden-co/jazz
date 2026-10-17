// The columns of a table's union row case and how a cell moves between the
// lineage's physical columns (SPEC 10 §10.3, "Union row cases").

/// Where a column's cell comes from in an image of another schema version,
/// along the lens path between them.
#[derive(Clone, Debug)]
pub(super) enum LensCellSource {
    /// The image's cell of physical column `id`. `copied` when the path
    /// copies it (`CopyColumn`) rather than carrying the same column on
    /// (identity, rename, or a bijective transform).
    Column { id: PhysicalColumnId, copied: bool },
    /// The lens default.
    Default(Value),
}

/// One physical column of a union row and the schema version that defines it.
#[derive(Clone, Debug)]
pub(super) struct UnionColumn {
    pub(super) id: PhysicalColumnId,
    /// The schema version ranked last in the catalogue, no later than the
    /// layout, mapping it.
    pub(super) schema: SchemaVersionId,
    pub(super) table: String,
    pub(super) name: String,
    /// The column's position in that schema's table.
    pub(super) index: usize,
    pub(super) column_type: ValueType,
    pub(super) strategy: crate::schema::MergeStrategy,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Where column `target_name` of `target_schema`'s `target_table` comes
    /// from in an image of `source`'s `source_table`. `None` when no lens
    /// path reaches it.
    pub(super) fn lens_cell_source(
        &mut self,
        source: SchemaVersionId,
        source_table: &str,
        target_schema: SchemaVersionId,
        target_table: &str,
        target_name: &str,
    ) -> Result<Option<LensCellSource>, Error> {
        self.lens_cell_source_within(source, source_table, target_schema, target_table, target_name, None)
    }

    /// `lens_cell_source` along a path through schemas ranked no later than
    /// `within` only (`compiled_lens_path_within`).
    fn lens_cell_source_within(
        &mut self,
        source: SchemaVersionId,
        source_table: &str,
        target_schema: SchemaVersionId,
        target_table: &str,
        target_name: &str,
        within: Option<u64>,
    ) -> Result<Option<LensCellSource>, Error> {
        let source_columns = self.table_in_schema(source_table, source)?.columns;
        let source_mapping = self
            .catalogue
            .physical_mappings
            .get(&source)
            .and_then(|mapping| mapping.tables.get(source_table))
            .map(|mapping| mapping.columns.clone())
            .ok_or(Error::InvalidStoredValue("lens cell source mapping missing"))?;
        let mut derived = source_columns
            .iter()
            .filter_map(|column| {
                source_mapping.get(&column.name).map(|id| {
                    (
                        column.name.clone(),
                        LensCellSource::Column {
                            id: *id,
                            copied: false,
                        },
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        if source == target_schema {
            return Ok((source_table == target_table)
                .then(|| derived.remove(target_name))
                .flatten());
        }
        let Some(path) =
            self.compiled_lens_path_within(source, target_schema, source_table, within)?
        else {
            return Ok(None);
        };
        if path.target_table != target_table {
            return Ok(None);
        }
        for op in path.ops {
            match op {
                CompiledLensOp::Rename { from, to } => {
                    if let Some(cell) = derived.remove(&from) {
                        derived.insert(to, cell);
                    }
                }
                CompiledLensOp::Copy { from, to } => {
                    let copied = match derived.get(&from) {
                        Some(LensCellSource::Column { id, .. }) => {
                            LensCellSource::Column { id: *id, copied: true }
                        }
                        Some(LensCellSource::Default(default)) => {
                            LensCellSource::Default(default.clone())
                        }
                        None => continue,
                    };
                    derived.insert(to, copied);
                }
                CompiledLensOp::Add { column, default } => {
                    derived
                        .entry(column)
                        .or_insert(LensCellSource::Default(default));
                }
                CompiledLensOp::Drop { column } => {
                    derived.remove(&column);
                }
            }
        }
        Ok(derived.remove(target_name))
    }

    /// The schema version ranked last in the catalogue, no later than
    /// `layout`, that maps the lineage column `id` of `layout`'s
    /// `layout_table`, with its name for the table and the column.
    fn union_column_definer(
        &self,
        layout: SchemaVersionId,
        layout_table: &str,
        id: PhysicalColumnId,
    ) -> Result<(SchemaVersionId, String, String), Error> {
        let table_id = self.physical_table_id_for_schema(layout, layout_table)?;
        let ranks = self.catalogue_ranks();
        let layout_rank = ranks
            .get(&layout)
            .copied()
            .ok_or(Error::InvalidStoredValue("union layout schema is not published"))?;
        let mut definer: Option<(u64, SchemaVersionId, String, String)> = None;
        for (candidate, mapping) in &self.catalogue.physical_mappings {
            let Some(rank) = ranks.get(candidate).copied() else {
                continue;
            };
            if rank > layout_rank {
                continue;
            }
            for (table_name, table_mapping) in &mapping.tables {
                if table_mapping.table_id != table_id {
                    continue;
                }
                let Some((column, _)) = table_mapping
                    .columns
                    .iter()
                    .find(|(_, column_id)| **column_id == id)
                else {
                    continue;
                };
                if definer.as_ref().is_none_or(|(newest, ..)| rank > *newest) {
                    definer = Some((rank, *candidate, table_name.clone(), column.clone()));
                }
            }
        }
        definer
            .map(|(_, schema, table, column)| (schema, table, column))
            .ok_or(Error::InvalidStoredValue("union column has no schema"))
    }

    /// Each published schema version's place in the catalogue order
    /// (`catalogue_ranks`), from the lineages this node has staged or
    /// activated.
    pub(in crate::node) fn catalogue_ranks(&self) -> BTreeMap<SchemaVersionId, u64> {
        catalogue_ranks(
            self.catalogue
                .active_lineages_by_target
                .values()
                .chain(self.catalogue.staged_lineages.values()),
        )
    }

    /// The union extras of `layout`'s `layout_table` whose cells a union row
    /// actually holds: every lineage column the layout does not carry on.
    /// That is a column `layout` dropped, one a sibling schema added, and a
    /// copy (`CopyColumn`) target, which is a column of its own (SPEC 4 §4.6).
    /// A lineage column the layout carries on under a new physical id (a
    /// rename chain or a bijective transform) is derived from the layout's
    /// cell by readers, as for any other row of the layout, so the union row
    /// leaves its field null.
    ///
    /// The lens path is taken through schemas ranked no later than `layout`
    /// only. Those schemas and their lenses are all published before
    /// `layout`, so the set is fixed with `layout`'s union case: a later
    /// schema with several predecessors cannot open a shorter route that
    /// would turn a stored extra into a derived one (or the reverse) under
    /// existing union rows, and every node that knows `layout` derives the
    /// same set.
    pub(super) fn union_stored_extras(
        &mut self,
        layout: SchemaVersionId,
        layout_table: &str,
    ) -> Result<Vec<PhysicalColumnId>, Error> {
        let ranks = self.catalogue_ranks();
        let extras = union_extra_columns(
            &self.catalogue.physical_mappings,
            &ranks,
            layout,
            layout_table,
        )?;
        let Some(within) = ranks.get(&layout).copied() else {
            return Ok(extras);
        };
        let mut stored = Vec::with_capacity(extras.len());
        for id in extras {
            let (schema, table, column) = self.union_column_definer(layout, layout_table, id)?;
            if !matches!(
                self.lens_cell_source_within(
                    layout,
                    layout_table,
                    schema,
                    &table,
                    &column,
                    Some(within)
                )?,
                Some(LensCellSource::Column { copied: false, .. })
            ) {
                stored.push(id);
            }
        }
        Ok(stored)
    }

    /// The columns a union row of `layout`'s `layout_table` holds: the
    /// layout's own, in schema order, then its stored extras
    /// (`union_stored_extras`), in physical id order.
    pub(super) fn union_columns(
        &mut self,
        layout: SchemaVersionId,
        layout_table: &str,
    ) -> Result<Vec<UnionColumn>, Error> {
        let table = self.table_in_schema(layout_table, layout)?;
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&layout)
            .and_then(|mapping| mapping.tables.get(layout_table))
            .map(|mapping| mapping.columns.clone())
            .ok_or(Error::InvalidStoredValue("union layout table mapping missing"))?;
        let mut columns = Vec::new();
        for (index, column) in table.columns.iter().enumerate() {
            columns.push(UnionColumn {
                id: *mapping
                    .get(&column.name)
                    .ok_or(Error::InvalidStoredValue("union column mapping missing"))?,
                schema: layout,
                table: layout_table.to_owned(),
                name: column.name.clone(),
                index,
                column_type: column.column_type.clone(),
                strategy: table.merge_strategy(&column.name),
            });
        }
        for id in self.union_stored_extras(layout, layout_table)? {
            let (schema, table_name, column_name) =
                self.union_column_definer(layout, layout_table, id)?;
            let defining = self.table_in_schema(&table_name, schema)?;
            let index = defining
                .columns
                .iter()
                .position(|column| column.name == column_name)
                .ok_or(Error::InvalidStoredValue("union extra column missing"))?;
            columns.push(UnionColumn {
                id,
                schema,
                table: table_name,
                index,
                column_type: defining.columns[index].column_type.clone(),
                strategy: defining.merge_strategy(&column_name),
                name: column_name,
            });
        }
        Ok(columns)
    }

    /// The authored spelling of `value` turned into the physical spelling of
    /// its column (enum tags re-tagged to the lineage's physical tags). A
    /// cell without an enum boundary is returned unchanged.
    pub(super) fn authored_cell_to_physical(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        column: usize,
        value: Value,
    ) -> Result<Value, Error> {
        let plan =
            self.prepared_physical_write_plan(schema_version, table, PhysicalWriteTarget::History)?;
        let Some(remaps) = plan.enum_remaps.get(column) else {
            return Ok(value);
        };
        if remaps.scalar.is_empty() && remaps.payload.is_empty() {
            return Ok(value);
        }
        let physical_type = plan.physical_descriptor.fields()[HistoryRowRecord::USER_CELLS + column]
            .value_type
            .clone();
        match (value, physical_type) {
            (Value::Nullable(Some(inner)), records::ValueType::Nullable(physical)) => {
                Ok(Value::Nullable(Some(Box::new(remap_nested_enum_value(
                    *inner,
                    &plan.source_table.columns[column].column_type,
                    &physical,
                    remaps,
                    "root",
                )?))))
            }
            (value, _) => Ok(value),
        }
    }

    /// The stored (physical, nullable) type of column `index` of
    /// `schema_version`'s `table`.
    fn physical_cell_type(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        index: usize,
    ) -> Result<ValueType, Error> {
        let plan =
            self.prepared_physical_write_plan(schema_version, table, PhysicalWriteTarget::History)?;
        Ok(plan.physical_descriptor.fields()[HistoryRowRecord::USER_CELLS + index]
            .value_type
            .clone())
    }

    /// `value`, a stored cell of column `source_index` (physical id
    /// `source_id`) of `source`'s `source_table`, as a cell of `target`.
    /// Enum tags are re-tagged between the two columns' registries as a
    /// reader's lens projection does; `None` when the value has no case in
    /// `target`'s registry.
    pub(super) fn physical_cell_for_column(
        &mut self,
        value: Value,
        source: SchemaVersionId,
        source_table: &str,
        source_index: usize,
        source_id: PhysicalColumnId,
        target: &UnionColumn,
    ) -> Result<Option<Value>, Error> {
        if source_id == target.id || value == Value::Nullable(None) {
            return Ok(Some(value));
        }
        let source_type = self.table_in_schema_ref(source_table, source)?.columns[source_index]
            .column_type
            .clone();
        let source_mapping = self
            .catalogue
            .physical_mappings
            .get(&source)
            .and_then(|mapping| mapping.tables.get(source_table))
            .cloned()
            .ok_or(Error::InvalidStoredValue("cell source mapping missing"))?;
        let target_mapping = self
            .catalogue
            .physical_mappings
            .get(&target.schema)
            .and_then(|mapping| mapping.tables.get(&target.table))
            .cloned()
            .ok_or(Error::InvalidStoredValue("cell target mapping missing"))?;
        if !physical_mapping_has_enum_boundary(&source_mapping, source_id)
            && !physical_mapping_has_enum_boundary(&target_mapping, target.id)
            && !value_type_has_enum_boundary(&source_type)
            && !value_type_has_enum_boundary(&target.column_type)
        {
            return Ok(Some(value));
        }
        let remaps = self.physical_copy_enum_remaps(
            &source_mapping,
            source_id,
            &target_mapping,
            target.id,
            &source_type,
            &target.column_type,
        )?;
        let source_physical = self.physical_cell_type(source, source_table, source_index)?;
        let target_physical = self.physical_cell_type(target.schema, &target.table, target.index)?;
        match (value, source_physical, target_physical) {
            (
                Value::Nullable(Some(inner)),
                records::ValueType::Nullable(source_physical),
                records::ValueType::Nullable(target_physical),
            ) => Ok(remap_nested_enum_value(
                *inner,
                &source_physical,
                &target_physical,
                &remaps,
                "root",
            )
            .ok()
            .map(|value| Value::Nullable(Some(Box::new(value))))),
            _ => Err(Error::InvalidStoredValue("stored cell is not nullable")),
        }
    }

    /// The union cells of a stored row image for the wire: its stored
    /// extras, grouped by the schema version that defines each column and
    /// spelled as that schema's authored cells (`UnionExtraCells`). Empty for
    /// a row stored in its schema's own case.
    pub(super) fn union_extra_cells_for_wire(
        &mut self,
        version: &VersionRow,
    ) -> Result<Vec<crate::protocol::UnionExtraCells>, Error> {
        let extras = version.union_extra_cells()?;
        if extras.is_empty() {
            return Ok(Vec::new());
        }
        let layout = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("union row schema alias missing"))?;
        let stored = self
            .union_stored_extras(layout, version.table())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut groups = BTreeMap::<(SchemaVersionId, String), BTreeMap<usize, Value>>::new();
        for (id, value) in extras {
            if !stored.contains(&id) {
                continue;
            }
            let (schema, table, column) = self.union_column_definer(layout, version.table(), id)?;
            let index = self
                .table_in_schema_ref(&table, schema)?
                .columns
                .iter()
                .position(|candidate| candidate.name == column)
                .ok_or(Error::InvalidStoredValue("union extra column missing"))?;
            let value = self.physical_cell_to_authored(schema, &table, index, value)?;
            groups.entry((schema, table)).or_default().insert(index, value);
        }
        let mut wire = Vec::with_capacity(groups.len());
        for ((schema, table), cells) in groups {
            let descriptor = self.table_in_schema_ref(&table, schema)?.wire_record_descriptor();
            let fields = cells
                .keys()
                .map(|index| descriptor.fields()[WireRowRecord::USER_CELLS + index].clone())
                .collect::<Vec<_>>();
            let descriptor = records::RecordDescriptor::new(fields.into_iter().map(|field| {
                (
                    field.name.expect("wire user cells are named"),
                    field.value_type,
                )
            }));
            let values = cells.into_values().collect::<Vec<_>>();
            let raw = descriptor.create(&values)?;
            wire.push(crate::protocol::UnionExtraCells::new(
                schema,
                table,
                OwnedRecord::new(raw, descriptor),
            ));
        }
        Ok(wire)
    }

    /// The physical cells a received version's union cells name, keyed by
    /// this node's physical column ids: each entry's schema version must be
    /// known, its table must be `table_id`'s lineage, and its cells must be
    /// columns of that table, in strictly increasing column order, each with
    /// that schema's exact wire type.
    pub(super) fn union_cells_from_wire(
        &mut self,
        version: &crate::protocol::VersionRecord,
        table_id: PhysicalTableId,
    ) -> Result<BTreeMap<PhysicalColumnId, Value>, Error> {
        let malformed = || Error::MalformedViewUpdate("row version union cells do not match their schema");
        let mut cells = BTreeMap::new();
        for extra in version.extra_cells() {
            let schema = extra.schema_version();
            let table = self.table_in_schema(extra.table(), schema).map_err(|_| malformed())?;
            let mapping = self
                .catalogue
                .physical_mappings
                .get(&schema)
                .and_then(|mapping| mapping.tables.get(extra.table()))
                .cloned()
                .ok_or_else(malformed)?;
            if mapping.table_id != table_id {
                return Err(malformed());
            }
            let descriptor = table.wire_record_descriptor();
            let record = extra.record().borrowed();
            // Cells are in the schema's column order, each column once: the
            // encoder's only spelling (`union_extra_cells_for_wire`).
            let mut previous_index = None;
            for (field_index, field) in extra.record().descriptor().fields().iter().enumerate() {
                let index = table
                    .columns
                    .iter()
                    .position(|column| {
                        Some(crate::schema::app_storage_column_name(&column.name).as_str()) == field.name.as_deref()
                    })
                    .ok_or_else(malformed)?;
                if previous_index.is_some_and(|previous| previous >= index) {
                    return Err(malformed());
                }
                previous_index = Some(index);
                if descriptor.fields()[WireRowRecord::USER_CELLS + index].value_type
                    != field.value_type
                {
                    return Err(malformed());
                }
                let value = record.get_idx(field_index)?;
                if let Value::Nullable(Some(inner)) = &value {
                    validate_cell_value(&table.columns[index], inner)?;
                }
                let id = *mapping.columns.get(&table.columns[index].name).ok_or_else(malformed)?;
                let value = self.authored_cell_to_physical(schema, extra.table(), index, value)?;
                if cells.insert(id, value).is_some() {
                    return Err(malformed());
                }
            }
        }
        Ok(cells)
    }
}

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
    /// The newest schema version (no newer than the layout) mapping it.
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
        let Some(path) = self.compiled_lens_path(source, target_schema, source_table)? else {
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

    /// The newest schema version, no newer than `layout`, that maps the
    /// lineage column `id` of `layout`'s `layout_table`, with its name for
    /// the table and the column.
    fn union_column_definer(
        &self,
        layout: SchemaVersionId,
        layout_table: &str,
        id: PhysicalColumnId,
    ) -> Result<(SchemaVersionId, String, String), Error> {
        let table_id = self.physical_table_id_for_schema(layout, layout_table)?;
        let layout_alias = self
            .catalogue
            .schema_version_aliases
            .get(&layout)
            .copied()
            .ok_or(Error::InvalidStoredValue("union layout schema alias missing"))?;
        let mut definer: Option<(SchemaVersionAlias, SchemaVersionId, String, String)> = None;
        for (candidate, mapping) in &self.catalogue.physical_mappings {
            let Some(alias) = self.catalogue.schema_version_aliases.get(candidate).copied() else {
                continue;
            };
            if alias > layout_alias {
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
                if definer.as_ref().is_none_or(|(newest, ..)| alias > *newest) {
                    definer = Some((alias, *candidate, table_name.clone(), column.clone()));
                }
            }
        }
        definer
            .map(|(_, schema, table, column)| (schema, table, column))
            .ok_or(Error::InvalidStoredValue("union column has no schema"))
    }

    /// The union extras of `layout`'s `layout_table` whose cells a union row
    /// actually holds: the lineage columns no lens path derives from one of
    /// `layout`'s own columns (a column `layout` dropped, or one a sibling
    /// schema added). A lineage column the layout carries on under a new
    /// physical id (a bijective transform) or copies is derived from the
    /// layout's cell by readers, as for any other row of the layout, so the
    /// union row leaves its field null.
    pub(super) fn union_stored_extras(
        &mut self,
        layout: SchemaVersionId,
        layout_table: &str,
    ) -> Result<Vec<PhysicalColumnId>, Error> {
        let extras = union_extra_columns(
            &self.catalogue.physical_mappings,
            &self.catalogue.schema_version_aliases,
            layout,
            layout_table,
        )?;
        let mut stored = Vec::with_capacity(extras.len());
        for id in extras {
            let (schema, table, column) = self.union_column_definer(layout, layout_table, id)?;
            if !matches!(
                self.lens_cell_source(layout, layout_table, schema, &table, &column)?,
                Some(LensCellSource::Column { .. })
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
}

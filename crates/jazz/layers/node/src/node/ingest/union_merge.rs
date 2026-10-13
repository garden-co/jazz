// Core's row image in the physical union layout (SPEC 4 §4.6, "Across schema
// versions"; SPEC 10 §10.3, "Union row cases"). A write merges into the row
// one physical column at a time, whatever schema version it was authored
// under: the cells it authored take its value (merge columns apply its op),
// and every other column of the table's lineage keeps the row's value. The
// post-image is laid out in the lineage's newest schema version, followed by
// the lineage's columns that schema does not map.

/// A write merged into a row image, one cell per union column, nullable as
/// stored and in physical spelling.
struct UnionMerge {
    layout: SchemaVersionId,
    layout_table: String,
    cells: BTreeMap<PhysicalColumnId, Value>,
    deletion: Value,
}

/// Why a write cannot be merged into the row's image.
enum UnionMergeRefusal {
    /// The cell of `column` could not be derived without guessing.
    NotSupported(String),
    /// A counter op leaves its type's range over the row's value.
    CounterOutOfRange {
        column: String,
        column_type: ValueType,
        previous: Value,
        op: Value,
        negative: bool,
    },
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// The newest schema version (by local alias, which follows activation
    /// order) that maps the physical table `table` has under
    /// `schema_version`, and its name for the table.
    fn union_layout_schema(
        &self,
        schema_version: SchemaVersionId,
        table: &str,
    ) -> Result<(SchemaVersionId, String), Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, table)?;
        let mut newest: Option<(SchemaVersionAlias, SchemaVersionId, String)> = None;
        for (candidate, mapping) in &self.catalogue.physical_mappings {
            let Some(alias) = self.catalogue.schema_version_aliases.get(candidate).copied() else {
                continue;
            };
            let Some((name, _)) = mapping
                .tables
                .iter()
                .find(|(_, mapping)| mapping.table_id == table_id)
            else {
                continue;
            };
            if newest.as_ref().is_none_or(|(newest, _, _)| alias > *newest) {
                newest = Some((alias, *candidate, name.clone()));
            }
        }
        newest
            .map(|(_, schema, name)| (schema, name))
            .ok_or(Error::InvalidStoredValue("union layout schema missing"))
    }

    /// `image`'s cells by physical column, nullable as stored and in physical
    /// spelling: its own columns and, for a union row, its stored extras.
    fn image_union_cells(
        &mut self,
        image: &VersionRow,
    ) -> Result<(SchemaVersionId, BTreeMap<PhysicalColumnId, Value>), Error> {
        let schema_version = self
            .schema_version_for_alias(image.schema_version_alias())
            .ok_or(Error::InvalidStoredValue("row image schema alias missing"))?;
        let table = self.table_in_schema(image.table(), schema_version)?;
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(image.table()))
            .map(|mapping| mapping.columns.clone())
            .ok_or(Error::InvalidStoredValue("row image physical mapping missing"))?;
        // An uploaded write spells enum cells with its schema's authored
        // tags; a stored image with the lineage's physical tags.
        let authored = *image.record.descriptor()
            == super::codec::history_record_descriptor(&table);
        let record = image.record.borrowed();
        let mut cells = BTreeMap::new();
        for (index, column) in table.columns.iter().enumerate() {
            let id = *mapping
                .get(&column.name)
                .ok_or(Error::InvalidStoredValue("row image column mapping missing"))?;
            let value = record.get_idx(HistoryRowRecord::USER_CELLS + index)?;
            let value = if authored {
                self.authored_cell_to_physical(schema_version, image.table(), index, value)?
            } else {
                value
            };
            cells.insert(id, value);
        }
        let extras = image.union_extra_cells()?;
        if !extras.is_empty() {
            let stored = self
                .union_stored_extras(schema_version, image.table())?
                .into_iter()
                .collect::<BTreeSet<_>>();
            cells.extend(extras.into_iter().filter(|(id, _)| stored.contains(id)));
        }
        Ok((schema_version, cells))
    }

    /// Complete `cells` (an image of `source`'s table `source_table`) with
    /// every column of `columns` it lacks, as a reader of that column's
    /// schema sees it through the lens path from `source`: the carried-on or
    /// copied cell (enum tags re-tagged), or the lens default. A column no
    /// lens path reaches is null.
    fn complete_union_cells(
        &mut self,
        source: SchemaVersionId,
        source_table: &str,
        cells: &mut BTreeMap<PhysicalColumnId, Value>,
        columns: &[UnionColumn],
    ) -> Result<Result<(), UnionMergeRefusal>, Error> {
        for column in columns {
            if cells.contains_key(&column.id) {
                continue;
            }
            let value = match self.lens_cell_source(
                source,
                source_table,
                column.schema,
                &column.table,
                &column.name,
            )? {
                None => Value::Nullable(None),
                Some(LensCellSource::Default(default)) => {
                    let value = Value::Nullable(Some(Box::new(default)));
                    self.authored_cell_to_physical(column.schema, &column.table, column.index, value)?
                }
                Some(LensCellSource::Column { id, .. }) => {
                    let value = cells.get(&id).cloned().unwrap_or(Value::Nullable(None));
                    let Some(index) = self.column_index_for_id(source, source_table, id)? else {
                        return Err(Error::InvalidStoredValue("lens source column missing"));
                    };
                    match self.physical_cell_for_column(value, source, source_table, index, id, column)? {
                        Some(value) => value,
                        None => return Ok(Err(UnionMergeRefusal::NotSupported(column.name.clone()))),
                    }
                }
            };
            cells.insert(column.id, value);
        }
        Ok(Ok(()))
    }

    /// The position of physical column `id` in `schema_version`'s `table`.
    fn column_index_for_id(
        &self,
        schema_version: SchemaVersionId,
        table: &str,
        id: PhysicalColumnId,
    ) -> Result<Option<usize>, Error> {
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(table))
            .ok_or(Error::InvalidStoredValue("column index physical mapping missing"))?;
        Ok(self
            .table_in_schema_ref(table, schema_version)?
            .columns
            .iter()
            .position(|column| mapping.columns.get(&column.name) == Some(&id)))
    }

    /// Merge `incoming` (authored under `schema_version`'s `table_schema`)
    /// into the row image `previous`, one physical column at a time. A union
    /// column takes the incoming write's cell when the write authored the
    /// column it is carried on from (the same physical column, or one the
    /// lens path renames or transforms into it); a copy is a column of its
    /// own and keeps the row's value.
    async fn union_merge(
        &mut self,
        schema_version: SchemaVersionId,
        table_schema: &TableSchema,
        incoming: &VersionRow,
        previous: &VersionRow,
    ) -> Result<Result<UnionMerge, UnionMergeRefusal>, Error> {
        let (layout, layout_table) = self.union_layout_schema(schema_version, &table_schema.name)?;
        let columns = self.union_columns(layout, &layout_table)?;
        let (previous_schema, mut previous_cells) = self.image_union_cells(previous)?;
        if let Err(refusal) = self.complete_union_cells(
            previous_schema,
            previous.table(),
            &mut previous_cells,
            &columns,
        )? {
            return Ok(Err(refusal));
        }
        let (_, incoming_cells) = self.image_union_cells(incoming)?;
        let authored_ids = incoming.authored_column_ids()?;
        let authors_id = |id: PhysicalColumnId| authored_ids.as_ref().is_none_or(|ids| ids.contains(&id));
        let counter_signs = incoming.counter_signs()?;
        let mut cells = BTreeMap::new();
        for column in &columns {
            let previous_value = previous_cells
                .get(&column.id)
                .cloned()
                .unwrap_or(Value::Nullable(None));
            // The incoming write's column this one is carried on from.
            let source_id = match self.lens_cell_source(
                schema_version,
                &table_schema.name,
                column.schema,
                &column.table,
                &column.name,
            )? {
                Some(LensCellSource::Column { id, copied: false }) if authors_id(id) => Some(id),
                _ => None,
            };
            let Some(source_id) = source_id else {
                cells.insert(column.id, previous_value);
                continue;
            };
            let incoming_index = self
                .column_index_for_id(schema_version, &table_schema.name, source_id)?
                .ok_or(Error::InvalidStoredValue("incoming source column missing"))?;
            let incoming_value = incoming_cells
                .get(&source_id)
                .cloned()
                .unwrap_or(Value::Nullable(None));
            let Some(incoming_value) = self.physical_cell_for_column(
                incoming_value,
                schema_version,
                &table_schema.name,
                incoming_index,
                source_id,
                column,
            )?
            else {
                return Ok(Err(UnionMergeRefusal::NotSupported(column.name.clone())));
            };
            let merged = match column.strategy {
                crate::schema::MergeStrategy::Lww => incoming_value,
                strategy => {
                    let negative = crate::node::merge_ops::counter_sign(
                        table_schema,
                        &counter_signs,
                        incoming_index,
                    );
                    let applied = if strategy == crate::schema::MergeStrategy::Counter {
                        match crate::node::merge_ops::counter_after_op(
                            &column.column_type,
                            &previous_value,
                            &incoming_value,
                            negative,
                        )? {
                            Some(applied) => applied,
                            None => {
                                return Ok(Err(UnionMergeRefusal::CounterOutOfRange {
                                    column: table_schema.columns[incoming_index].name.clone(),
                                    column_type: column.column_type.clone(),
                                    previous: previous_value,
                                    op: incoming_value,
                                    negative,
                                }));
                            }
                        }
                    } else {
                        crate::node::merge_ops::apply_merge_op(
                            strategy,
                            &column.column_type,
                            &previous_value,
                            &incoming_value,
                            negative,
                        )?
                    };
                    // History cells are stored nullable.
                    Value::Nullable(Some(Box::new(applied)))
                }
            };
            cells.insert(column.id, merged);
        }
        // An authored `_deletion` applies; otherwise the row keeps its
        // deletion state (SPEC 4 §4.6, "Edit after delete").
        let deletion = if authors_id(DELETION_COLUMN_ID) {
            incoming.record.borrowed().get_idx(HistoryRowRecord::FIELD__DELETION_IDX)?
        } else {
            previous.record.borrowed().get_idx(HistoryRowRecord::FIELD__DELETION_IDX)?
        };
        Ok(Ok(UnionMerge {
            layout,
            layout_table,
            cells,
            deletion,
        }))
    }

    /// The post-image of a union merge: `incoming`'s identity and provenance
    /// (keeping the row's creation), laid out in the merge's layout schema,
    /// at `global_time`.
    fn union_post_image(
        &mut self,
        incoming: &VersionRow,
        previous: &VersionRow,
        merge: UnionMerge,
        global_time: GlobalTime,
    ) -> Result<VersionRow, Error> {
        self.union_image_row(
            merge.layout,
            &merge.layout_table,
            &merge.cells,
            merge.deletion,
            incoming,
            previous,
        )?
        .with_merge_fields(global_time, incoming.base()?)
    }

    /// A row image in `layout`'s union layout: `identity`'s prefix and
    /// merge fields, `created`'s creation provenance, `deletion`, and each
    /// union column's cell from `cells` (physical spelling; missing cells
    /// and derived extras are null).
    fn union_image_row(
        &mut self,
        layout: SchemaVersionId,
        layout_table: &str,
        cells: &BTreeMap<PhysicalColumnId, Value>,
        deletion: Value,
        identity: &VersionRow,
        created: &VersionRow,
    ) -> Result<VersionRow, Error> {
        let layout_alias = self
            .catalogue
            .schema_version_aliases
            .get(&layout)
            .copied()
            .ok_or(Error::InvalidStoredValue("union layout schema alias missing"))?;
        let layout_columns = self.union_columns(layout, layout_table)?;
        let layout_width = self.table_in_schema_ref(layout_table, layout)?.columns.len();
        // Every extra field of the union case, in its order; the ones the
        // row derives from its own cells stay null (`union_stored_extras`).
        let extras = union_extra_columns(
            &self.catalogue.physical_mappings,
            &self.catalogue.schema_version_aliases,
            layout,
            layout_table,
        )?;
        let plan = self.prepared_physical_write_plan_for_layout(
            layout,
            layout_table,
            PhysicalWriteTarget::History,
            !extras.is_empty(),
        )?;
        let descriptor = plan.logical_descriptor.clone();
        let identity_values = identity.record.to_values()?;
        let created_record = created.record.borrowed();
        let identity_table_width = identity_values.len()
            - HistoryRowRecord::USER_CELLS
            - identity.union_extra_cells()?.len()
            - HISTORY_SUFFIX_FIELDS;
        let cell = |id: &PhysicalColumnId| cells.get(id).cloned().unwrap_or(Value::Nullable(None));
        let mut values = Vec::with_capacity(descriptor.fields().len());
        values.extend(identity_values[..HistoryRowRecord::USER_CELLS].iter().cloned());
        values[HistoryRowRecord::FIELD_SCHEMA_VERSION_IDX] = Value::U64(layout_alias.0);
        values[HistoryRowRecord::FIELD__DELETION_IDX] = deletion;
        for index in [
            HistoryRowRecord::FIELD_CREATED_BY_IDX,
            HistoryRowRecord::FIELD_CREATED_AT_IDX,
        ] {
            values[index] = created_record.get_idx(index)?;
        }
        for column in &layout_columns[..layout_width] {
            values.push(self.physical_cell_to_authored(
                layout,
                layout_table,
                column.index,
                cell(&column.id),
            )?);
        }
        let suffix_start = HistoryRowRecord::USER_CELLS + identity_table_width;
        values.extend(
            identity_values[suffix_start..suffix_start + HISTORY_SUFFIX_FIELDS]
                .iter()
                .cloned(),
        );
        values.extend(extras.iter().map(cell));
        crate::node::merge_ops::clear_counter_signs(&mut values, &descriptor);
        let raw = descriptor.create(&values)?;
        Ok(VersionRow {
            table: groove::Intern::new(layout_table.to_owned()),
            branch_key: identity.branch_key().clone(),
            record: OwnedRecord::new(raw, descriptor),
        })
    }

    /// A received version's row (`VersionRow::from_wire_with_schema_version`)
    /// as this node stores it: unchanged, or for a Core image with union
    /// cells, laid out in this node's own union layout of the table (its
    /// newest schema version mapping it), deriving the cells the image lacks
    /// as a reader of each column would.
    pub(super) fn with_wire_union_cells(
        &mut self,
        row: VersionRow,
        version: &VersionRecord,
    ) -> Result<VersionRow, Error> {
        if version.extra_cells().is_empty() {
            return Ok(row);
        }
        let schema_version = version.schema_version();
        let table_id = self.physical_table_id_for_schema(schema_version, version.table())?;
        let (_, mut cells) = self.image_union_cells(&row)?;
        for (id, value) in self.union_cells_from_wire(version, table_id)? {
            if cells.insert(id, value).is_some() {
                return Err(Error::MalformedViewUpdate(
                    "row version union cells repeat a column of its schema",
                ));
            }
        }
        let (layout, layout_table) = self.union_layout_schema(schema_version, version.table())?;
        let columns = self.union_columns(layout, &layout_table)?;
        if self
            .complete_union_cells(schema_version, version.table(), &mut cells, &columns)?
            .is_err()
        {
            return Err(Error::MalformedViewUpdate(
                "row version union cells cannot be laid out on this node",
            ));
        }
        let deletion = row.record.borrowed().get_idx(HistoryRowRecord::FIELD__DELETION_IDX)?;
        self.union_image_row(layout, &layout_table, &cells, deletion, &row, &row)
    }

    /// Why Core refuses to merge `incoming` into `previous`, if it does.
    async fn union_merge_rejection(
        &mut self,
        schema_version: SchemaVersionId,
        table_schema: &TableSchema,
        incoming: &VersionRow,
        previous: &VersionRow,
    ) -> Result<Option<RejectionReason>, Error> {
        Ok(
            match self
                .union_merge(schema_version, table_schema, incoming, previous)
                .await?
            {
                Ok(_) => None,
                Err(UnionMergeRefusal::NotSupported(column)) => {
                    Some(RejectionReason::MalformedCommit(format!(
                        "column '{}.{column}' gets an enum value with no case in its own enum; writing it across schema versions is not supported yet",
                        incoming.table(),
                    )))
                }
                Err(UnionMergeRefusal::CounterOutOfRange {
                    column,
                    column_type,
                    previous,
                    op,
                    negative,
                }) => Some(RejectionReason::MalformedCommit(format!(
                    "counter '{}.{}' would be out of range for its {:?} column: {} {:+} leaves the type's range, so the write is rejected rather than wrapped",
                    incoming.table(),
                    column,
                    column_type,
                    crate::node::merge_ops::counter_to_i128(&previous)?,
                    crate::node::merge_ops::counter_op_delta(&column_type, &op, negative)?,
                ))),
            },
        )
    }
}

/// History's trailing fields after the user cells: `authored_columns`,
/// `counter_signs`, `seq`, `base_seq`, `base_pending`.
const HISTORY_SUFFIX_FIELDS: usize = 5;

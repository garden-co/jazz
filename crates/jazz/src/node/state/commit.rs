struct NodeLargeValueReader<'a, S>
where
    S: OrderedKvStorage,
{
    state: std::cell::RefCell<&'a mut NodeState<S>>,
}

impl<'a, S> NodeLargeValueReader<'a, S>
where
    S: OrderedKvStorage,
{
    fn new(state: &'a mut NodeState<S>) -> Self {
        Self {
            state: std::cell::RefCell::new(state),
        }
    }
}

impl<S> crate::large_values::LargeValueNodeRows for NodeLargeValueReader<'_, S>
where
    S: OrderedKvStorage,
{
    fn get(
        &self,
        domain: &crate::large_values::LargeValueOwnerDomain,
        id: crate::large_values::LargeValueNodeId,
    ) -> Result<Option<Vec<u8>>, crate::large_values::ContentError> {
        let table_name = crate::large_values::large_value_node_table_name(domain.owner_table());
        let mut state = self.state.borrow_mut();
        let table = state
            .table_in_schema(&table_name, state.catalogue.current_write_schema.schema)
            .map_err(|error| crate::large_values::ContentError::Storage(error.to_string()))?;
        let Some(row) = state
            .local_current_row(&table_name, RowUuid(id.row_id()))
            .map_err(|error| crate::large_values::ContentError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let malformed = || {
            crate::large_values::ContentError::MalformedNode(
                "hidden node row does not match its owner or identity".to_owned(),
            )
        };
        if row.cell(&table, "owner") != Some(Value::Uuid(domain.owner_row()))
            || row.cell(&table, "format")
                != Some(Value::U8(
                    crate::large_values::LARGE_VALUE_TREE_FORMAT_VERSION,
                ))
        {
            return Err(malformed());
        }
        let Some(Value::Bytes(content_id)) = row.cell(&table, "content_id") else {
            return Err(malformed());
        };
        if crate::large_values::LargeValueNodeId::from_bytes(&content_id)? != id {
            return Err(malformed());
        }
        let Some(Value::Bytes(payload)) = row.cell(&table, "payload") else {
            return Err(malformed());
        };
        let expected = crate::large_values::LargeValueNodeRow {
            row_id: id.row_id(),
            owner: domain.clone(),
            content_id: id,
            payload: payload.clone(),
        }
        .cells(Default::default())?;
        if expected
            .iter()
            .any(|(column, value)| row.cell(&table, column) != Some(value.clone()))
        {
            return Err(malformed());
        }
        Ok(Some(payload))
    }

    fn put_if_absent_or_identical(
        &mut self,
        _row: &crate::large_values::LargeValueNodeRow,
    ) -> Result<(), crate::large_values::ContentError> {
        Err(crate::large_values::ContentError::Storage(
            "query readers cannot publish node rows".to_owned(),
        ))
    }
}

#[cfg(feature = "runtime")]
struct NodeLargeValueEditor<'a, S>
where
    S: OrderedKvStorage,
{
    reader: NodeLargeValueReader<'a, S>,
    rows: BTreeMap<crate::large_values::LargeValueNodeId, crate::large_values::LargeValueNodeRow>,
}

#[cfg(feature = "runtime")]
impl<'a, S> NodeLargeValueEditor<'a, S>
where
    S: OrderedKvStorage,
{
    fn new(state: &'a mut NodeState<S>) -> Self {
        Self {
            reader: NodeLargeValueReader::new(state),
            rows: BTreeMap::new(),
        }
    }

    fn into_rows(self) -> Vec<crate::large_values::LargeValueNodeRow> {
        self.rows.into_values().collect()
    }
}

#[cfg(feature = "runtime")]
impl<S> crate::large_values::LargeValueNodeRows for NodeLargeValueEditor<'_, S>
where
    S: OrderedKvStorage,
{
    fn get(
        &self,
        domain: &crate::large_values::LargeValueOwnerDomain,
        id: crate::large_values::LargeValueNodeId,
    ) -> Result<Option<Vec<u8>>, crate::large_values::ContentError> {
        if let Some(row) = self.rows.get(&id) {
            return Ok(Some(row.payload.clone()));
        }
        crate::large_values::LargeValueNodeRows::get(&self.reader, domain, id)
    }

    fn put_if_absent_or_identical(
        &mut self,
        row: &crate::large_values::LargeValueNodeRow,
    ) -> Result<(), crate::large_values::ContentError> {
        if let Some(existing) = self.get(&row.owner, row.content_id)? {
            return if existing == row.payload {
                Ok(())
            } else {
                Err(crate::large_values::ContentError::ImmutableCollision(
                    row.content_id,
                ))
            };
        }
        self.rows.insert(row.content_id, row.clone());
        Ok(())
    }
}

/// Validate the system-owned half of a large-value transaction before any
/// version enters local history.  Hidden node tables are ordinary Jazz tables
/// for storage, sync, and policy purposes, but they are not an application
/// mutation surface: their rows are admitted only as the exact immutable
/// closure of framed owner cells in this same transaction.
fn validate_generated_large_value_commit_shape<S>(
    state: &mut NodeState<S>,
    commits: &[(SchemaVersionId, MergeableCommit)],
) -> Result<(), Error>
where
    S: OrderedKvStorage,
{
    let mut pending = BTreeMap::<
        (
            crate::large_values::LargeValueOwnerDomain,
            crate::large_values::LargeValueNodeId,
        ),
        crate::large_values::LargeValueNodeRow,
    >::new();
    let mut roots = Vec::<(
        crate::large_values::LargeValueOwnerDomain,
        crate::large_values::LargeValueNodeId,
    )>::new();

    for (schema_version, commit) in commits {
        let Some(owner_table_name) =
            crate::large_values::large_value_node_owner_table(&commit.table)
        else {
            let table = state.table_in_schema(&commit.table, *schema_version)?;
            if commit.deletion.is_some() {
                continue;
            }
            let domain = crate::large_values::LargeValueOwnerDomain::new(
                table.name.clone(),
                commit.row_uuid.0,
            )
            .map_err(|_| Error::InvalidMergeableCommit("invalid large-value owner domain"))?;
            for column in &table.columns {
                let Some(schema) = &column.large_value else {
                    continue;
                };
                let Some(stored) = commit.cells.get(&column.name).and_then(large_value_leaf)
                else {
                    continue;
                };
                if !crate::large_values::LargeValue::storage_value_is_framed(schema, stored) {
                    continue;
                }
                let value = crate::large_values::LargeValue::decode_storage_value(schema, stored)
                    .map_err(|_| Error::InvalidMergeableCommit("invalid large-value descriptor"))?;
                if let crate::large_values::LargeValue::Chunked(value) = value {
                    roots.push((domain.clone(), value.root));
                }
            }
            continue;
        };

        let owner_table = state.table_in_schema(&owner_table_name, *schema_version)?;
        if !owner_table
            .columns
            .iter()
            .any(|column| column.large_value.is_some())
        {
            return Err(Error::InvalidMergeableCommit(
                "generated large-value table does not name a large-value owner table",
            ));
        }
        if commit.deletion.is_some() || !commit.parents.is_empty() || commit.branch != BranchSelector::default() {
            return Err(Error::InvalidMergeableCommit(
                "generated large-value nodes are insert-only and unbranched",
            ));
        }
        let Some(Value::Uuid(owner_row)) = commit.cells.get("owner") else {
            return Err(Error::InvalidMergeableCommit(
                "generated large-value node lacks its owner row",
            ));
        };
        let Some(Value::Bytes(content_id)) = commit.cells.get("content_id") else {
            return Err(Error::InvalidMergeableCommit(
                "generated large-value node lacks its content id",
            ));
        };
        let content_id = crate::large_values::LargeValueNodeId::from_bytes(content_id)
            .map_err(|_| Error::InvalidMergeableCommit("invalid generated large-value node id"))?;
        let Some(Value::Bytes(payload)) = commit.cells.get("payload") else {
            return Err(Error::InvalidMergeableCommit(
                "generated large-value node lacks its payload",
            ));
        };
        let owner = crate::large_values::LargeValueOwnerDomain::new(owner_table_name, *owner_row)
            .map_err(|_| Error::InvalidMergeableCommit("invalid generated large-value owner"))?;
        let node = crate::large_values::LargeValueNodeRow {
            row_id: commit.row_uuid.0,
            owner: owner.clone(),
            content_id,
            payload: payload.clone(),
        };
        let expected = node
            .cells(Default::default())
            .map_err(|_| Error::InvalidMergeableCommit("generated large-value node is not canonical"))?;
        if commit.cells != expected
            || commit.authored_columns.as_ref().is_some_and(|authored| {
                *authored != expected.keys().cloned().collect()
            })
        {
            return Err(Error::InvalidMergeableCommit(
                "generated large-value node does not carry its exact canonical cells",
            ));
        }
        if pending.insert((owner, content_id), node).is_some() {
            return Err(Error::InvalidMergeableCommit(
                "generated large-value transaction repeats a node identity",
            ));
        }
    }

    validate_generated_large_value_node_closure(state, pending, roots)
}

fn validate_generated_large_value_node_closure<S>(
    state: &mut NodeState<S>,
    pending: BTreeMap<
        (
            crate::large_values::LargeValueOwnerDomain,
            crate::large_values::LargeValueNodeId,
        ),
        crate::large_values::LargeValueNodeRow,
    >,
    roots: Vec<(
        crate::large_values::LargeValueOwnerDomain,
        crate::large_values::LargeValueNodeId,
    )>,
) -> Result<(), Error>
where
    S: OrderedKvStorage,
{
    if pending.is_empty() {
        return Ok(());
    }
    let reader = NodeLargeValueReader::new(state);
    let mut reachable = BTreeSet::new();
    let mut stack = roots;
    while let Some((owner, id)) = stack.pop() {
        if !reachable.insert((owner.clone(), id)) {
            continue;
        }
        if let Some(node) = pending.get(&(owner.clone(), id)) {
            for child in node
                .child_ids(Default::default())
                .map_err(|_| Error::InvalidMergeableCommit("generated large-value node payload is malformed"))?
            {
                stack.push((owner.clone(), child));
            }
        } else if crate::large_values::LargeValueNodeRows::get(&reader, &owner, id)
            .map_err(|_| Error::InvalidMergeableCommit("large-value descriptor references an invalid stored node"))?
            .is_none()
        {
            return Err(Error::InvalidMergeableCommit(
                "large-value descriptor references a missing node",
            ));
        }
    }
    if pending.keys().any(|key| !reachable.contains(key)) {
        return Err(Error::InvalidMergeableCommit(
            "generated large-value transaction contains an orphan node",
        ));
    }
    Ok(())
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply the generated-node admission invariant to an arriving complete
    /// wire transaction.  This is intentionally independent of local commit
    /// construction: peers can serialize `VersionRecord`s directly.
    fn validate_generated_large_value_version_shape(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<(), Error> {
        let mut pending = BTreeMap::<
            (
                crate::large_values::LargeValueOwnerDomain,
                crate::large_values::LargeValueNodeId,
            ),
            crate::large_values::LargeValueNodeRow,
        >::new();
        let mut roots = Vec::<(
            crate::large_values::LargeValueOwnerDomain,
            crate::large_values::LargeValueNodeId,
        )>::new();

        for version in versions {
            let Some(schema) = self.catalogue.catalogue_schemas.get(&version.schema_version()) else {
                // Unknown authored schemas are parked until their catalogue
                // lineage arrives, at which point this validation runs again.
                continue;
            };
            let Some(table) = schema.schema.tables.iter().find(|table| table.name == version.table()) else {
                continue;
            };
            let cells = table
                .columns
                .iter()
                .enumerate()
                .filter_map(|(index, column)| {
                    version.cell_at(index).map(|value| (column.name.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            let Some(owner_table_name) =
                crate::large_values::large_value_node_owner_table(&table.name)
            else {
                if version.deletion().is_some() {
                    continue;
                }
                let domain = crate::large_values::LargeValueOwnerDomain::new(
                    table.name.clone(),
                    version.row_uuid().0,
                )
                .map_err(|_| Error::InvalidStoredValue("invalid large-value owner domain"))?;
                for column in &table.columns {
                    let Some(large_schema) = &column.large_value else {
                        continue;
                    };
                    let Some(stored) = cells.get(&column.name).and_then(large_value_leaf) else {
                        continue;
                    };
                    if !crate::large_values::LargeValue::storage_value_is_framed(large_schema, stored) {
                        continue;
                    }
                    let value = crate::large_values::LargeValue::decode_storage_value(large_schema, stored)
                        .map_err(|_| Error::InvalidStoredValue("invalid large-value descriptor"))?;
                    if let crate::large_values::LargeValue::Chunked(value) = value {
                        roots.push((domain.clone(), value.root));
                    }
                }
                continue;
            };

            let owner_table = schema
                .schema
                .tables
                .iter()
                .find(|candidate| candidate.name == owner_table_name)
                .ok_or(Error::InvalidStoredValue(
                    "generated large-value node table lacks its owner table",
                ))?;
            if !owner_table
                .columns
                .iter()
                .any(|column| column.large_value.is_some())
                || version.deletion().is_some()
                || !version.parents().is_empty()
                || version.branch_key() != &BranchKey::default()
            {
                return Err(Error::InvalidStoredValue(
                    "generated large-value node is not an unbranched insert",
                ));
            }
            let Some(Value::Uuid(owner_row)) = cells.get("owner") else {
                return Err(Error::InvalidStoredValue("generated large-value node lacks owner"));
            };
            let Some(Value::Bytes(content_id)) = cells.get("content_id") else {
                return Err(Error::InvalidStoredValue("generated large-value node lacks content id"));
            };
            let content_id = crate::large_values::LargeValueNodeId::from_bytes(content_id)
                .map_err(|_| Error::InvalidStoredValue("invalid generated large-value node id"))?;
            let Some(Value::Bytes(payload)) = cells.get("payload") else {
                return Err(Error::InvalidStoredValue("generated large-value node lacks payload"));
            };
            let owner = crate::large_values::LargeValueOwnerDomain::new(owner_table_name, *owner_row)
                .map_err(|_| Error::InvalidStoredValue("invalid generated large-value owner"))?;
            let node = crate::large_values::LargeValueNodeRow {
                row_id: version.row_uuid().0,
                owner: owner.clone(),
                content_id,
                payload: payload.clone(),
            };
            let expected = node
                .cells(Default::default())
                .map_err(|_| Error::InvalidStoredValue("generated large-value node is not canonical"))?;
            if cells != expected {
                return Err(Error::InvalidStoredValue(
                    "generated large-value node does not carry canonical cells",
                ));
            }
            if pending.insert((owner, content_id), node).is_some() {
                return Err(Error::InvalidStoredValue(
                    "generated large-value transaction repeats a node identity",
                ));
            }
        }
        validate_generated_large_value_node_closure(self, pending, roots)
    }

    #[cfg(feature = "runtime")]
    pub(crate) fn prepare_large_value_edit(
        &mut self,
        table: &str,
        row: RowUuid,
        column: &str,
        edit: crate::large_values::ValueEdit,
    ) -> Result<(Value, Vec<crate::large_values::LargeValueNodeRow>), Error> {
        let table_schema = self.table(table)?.clone();
        let column_schema = table_schema
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
            .ok_or(Error::InvalidStoredValue("unknown large-value column"))?;
        let schema = column_schema
            .large_value
            .as_ref()
            .ok_or(Error::InvalidStoredValue("column is not a large value"))?;
        let cells = self
            .physical_current_cells(table, row)?
            .ok_or(Error::InvalidStoredValue("large-value owner row is absent"))?;
        let stored = cells
            .get(column)
            .ok_or(Error::InvalidStoredValue("large-value cell is absent"))?;
        let (stored, nullable) = match stored {
            Value::Nullable(Some(value)) => (value.as_ref(), true),
            Value::Nullable(None) => {
                return Err(Error::InvalidStoredValue("cannot edit a null large value"));
            }
            value => (value, false),
        };
        let value = crate::large_values::LargeValue::decode_storage_value(schema, stored)
            .map_err(|_| Error::InvalidStoredValue("edit source is not a large-value envelope"))?;
        let domain = crate::large_values::LargeValueOwnerDomain::new(table, row.0)
            .map_err(|_| Error::InvalidStoredValue("invalid large-value owner domain"))?;
        let tree = crate::large_values::ContentTree::new(Default::default())
            .map_err(|_| Error::InvalidStoredValue("invalid large-value tree profile"))?;
        let mut editor = NodeLargeValueEditor::new(self);
        let patch = value
            .lower_edit(schema.kind, edit, &domain, tree, &editor)
            .map_err(|_| Error::InvalidStoredValue("invalid large-value edit"))?;
        let value = value
            .apply_edit(
                schema.kind,
                &domain,
                patch,
                usize::try_from(schema.inline_up_to).expect("u32 fits usize"),
                schema.tail_bounds(),
                tree,
                &mut editor,
            )
            .map_err(|_| Error::InvalidStoredValue("invalid large-value edit"))?;
        let stored = value
            .encode_storage_value(schema)
            .map_err(|_| Error::InvalidStoredValue("large-value cell encoding failed"))?;
        crate::large_values::LargeValue::decode_storage_value(schema, &stored)
            .map_err(|_| Error::InvalidStoredValue("large-value cell failed its own round trip"))?;
        let rows = editor.into_rows();
        Ok((
            if nullable {
                Value::Nullable(Some(Box::new(stored)))
            } else {
                stored
            },
            rows,
        ))
    }

    /// Return the exact immutable generated-node rows required to materialize
    /// the chunked large values in `versions`.
    ///
    /// The rows remain ordinary, policy-authorized Jazz history rows. This
    /// merely augments a view-scoped bundle with the transitive data dependency
    /// of an already-authorized owner version, rather than teaching sync a
    /// large-value-specific payload format or broadening the hidden table's
    /// query result set.
    pub(super) fn large_value_node_closure_for_versions(
        &mut self,
        versions: &[VersionRow],
    ) -> Result<BTreeSet<(String, RowUuid, TxId)>, Error> {
        let mut roots = Vec::new();
        for version in versions {
            if version.deletion().is_some()
                || version
                    .table()
                    .starts_with(crate::large_values::LARGE_VALUE_NODE_TABLE_PREFIX)
            {
                continue;
            }
            let schema_version = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "large-value owner version schema alias must exist",
                ))?;
            let table = self.table_in_schema(version.table(), schema_version)?.clone();
            let cells = version.cells(&table)?;
            for column in &table.columns {
                let Some(large_schema) = &column.large_value else {
                    continue;
                };
                let Some(value) = cells.get(&column.name) else {
                    continue;
                };
                let value = match value {
                    Value::Nullable(Some(value)) => value.as_ref(),
                    Value::Nullable(None) => continue,
                    value => value,
                };
                if !crate::large_values::LargeValue::storage_value_is_framed(large_schema, value)
                {
                    continue;
                }
                let crate::large_values::LargeValue::Chunked(chunked) =
                    crate::large_values::LargeValue::decode_storage_value(large_schema, value)
                        .map_err(|_| Error::InvalidStoredValue("invalid large-value owner cell"))?
                else {
                    continue;
                };
                roots.push((
                    crate::large_values::LargeValueOwnerDomain::new(
                        version.table(),
                        version.row_uuid().0,
                    )
                    .map_err(|_| Error::InvalidStoredValue("invalid large-value owner domain"))?,
                    chunked.root,
                ));
            }
        }

        let mut closure = BTreeSet::new();
        let mut pending = roots;
        let mut visited = BTreeSet::new();
        while let Some((domain, id)) = pending.pop() {
            if !visited.insert((domain.clone(), id)) {
                continue;
            }
            let payload = {
                let reader = NodeLargeValueReader::new(self);
                crate::large_values::LargeValueNodeRows::get(&reader, &domain, id)
                    .map_err(|_| Error::InvalidStoredValue("missing or invalid large-value node row"))?
                    .ok_or(Error::InvalidStoredValue("missing large-value node row"))?
            };
            let table = crate::large_values::large_value_node_table_name(domain.owner_table());
            let row = self
                .local_current_row(&table, RowUuid(id.row_id()))?
                .ok_or(Error::InvalidStoredValue("missing large-value node current row"))?;
            let tx_id = self
                .current_row_tx_id(&row)
                .ok_or(Error::InvalidStoredValue("large-value node lacks transaction provenance"))?;
            closure.insert((table, RowUuid(id.row_id()), tx_id));
            for child in crate::large_values::child_node_ids(&payload)
                .map_err(|_| Error::InvalidStoredValue("invalid large-value node payload"))?
            {
                pending.push((domain.clone(), child));
            }
        }
        Ok(closure)
    }

    /// Commit a local mergeable write and leave its fate pending.
    pub fn commit_mergeable(&mut self, commit: MergeableCommit) -> Result<TxId, Error> {
        commit.validate()?;
        self.merge_commit_parent_times(std::slice::from_ref(&commit))?;
        let made_at = self.mint_tx_time(commit.now_ms);
        self.commit_mergeable_at(commit, made_at)
    }

    /// Commit one local mergeable write under an admitted authored schema.
    ///
    /// Client database handles retain the schema they were opened with even
    /// when an authority later advances its separate current-write pointer.
    /// Their canonical versions must retain that authored schema so receivers
    /// can reconstruct through the ordered catalogue lineage.
    pub(crate) fn commit_mergeable_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        commit: MergeableCommit,
    ) -> Result<TxId, Error> {
        self.commit_mergeable_many_in_schema(schema_version, vec![commit])
    }

    /// Commit multiple local mergeable writes as one transaction.
    pub fn commit_mergeable_many(&mut self, commits: Vec<MergeableCommit>) -> Result<TxId, Error> {
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        self.merge_commit_parent_times(&commits)?;
        let made_at = self.mint_tx_time(commits[0].now_ms);
        self.commit_mergeable_many_at(commits, made_at)
    }

    /// Commit the already-calculated output of the high-level contribution
    /// merge helper as one ordinary mergeable transaction.
    pub(crate) fn commit_calculated_merge_many(
        &mut self,
        commits: Vec<MergeableCommit>,
        provenance: ContributionMergeProvenance,
    ) -> Result<TxId, Error> {
        self.require_catalogue_ready()?;
        provenance.validate().map_err(Error::InvalidMergeableCommit)?;
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "calculated merge requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "calculated merge permission subjects must match",
                ));
            }
        }
        let schema_version = self.catalogue.current_write_schema.schema;
        let mut emitted = BTreeSet::new();
        for commit in &commits {
            let table = self.table_in_schema(&commit.table, schema_version)?;
            let schema = &self
                .catalogue
                .catalogue_schemas
                .get(&schema_version)
                .ok_or(Error::InvalidStoredValue("current write schema missing"))?
                .schema;
            let (branch_key, _) = schema
                .project_branch_selector(&table, &commit.branch)
                .map_err(Error::InvalidBranchKey)?;
            let layer = if commit.deletion.is_some() {
                MergeAspect::Deletion
            } else {
                MergeAspect::Content
            };
            if layer == MergeAspect::Deletion {
                emitted.insert(ContributionCoordinate {
                    branch_key,
                    table: commit.table.clone(),
                    row_uuid: commit.row_uuid,
                    layer,
                    component: ContributionComponent::Register,
                });
            } else {
                let authored = commit
                    .authored_columns
                    .clone()
                    .unwrap_or_else(|| commit.cells.keys().cloned().collect());
                for column in authored {
                    let components = match table.merge_strategy(&column) {
                        MergeStrategy::Lww => vec![ContributionComponent::Column(column)],
                        MergeStrategy::Counter => {
                            vec![ContributionComponent::Operation(column.into_bytes())]
                        }
                        MergeStrategy::GSet => match commit.cells.get(&column) {
                            Some(Value::Array(elements)) => elements
                                .iter()
                                .map(|element| {
                                    postcard::to_allocvec(&(column.as_str(), element)).map(
                                        ContributionComponent::Operation,
                                    )
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|_| {
                                    Error::InvalidMergeableCommit(
                                        "g-set contribution operation must encode",
                                    )
                                })?,
                            _ => {
                                return Err(Error::InvalidMergeableCommit(
                                    "g-set calculated merge value must be an array",
                                ));
                            }
                        },
                    };
                    emitted.extend(components.into_iter().map(|component| ContributionCoordinate {
                        branch_key: branch_key.clone(),
                        table: commit.table.clone(),
                        row_uuid: commit.row_uuid,
                        layer,
                        component,
                    }));
                }
            }
        }
        if provenance
            .substitutions
            .iter()
            .any(|substitution| !emitted.contains(&substitution.target))
        {
            return Err(Error::InvalidMergeableCommit(
                "contribution substitution target was not emitted",
            ));
        }
        self.merge_commit_parent_times(&commits)?;
        let made_at = self.mint_tx_time(commits[0].now_ms);
        self.commit_mergeable_many_at_with_schema_versions_and_provenance(
            commits
                .into_iter()
                .map(|commit| (schema_version, commit))
                .collect(),
            made_at,
            Some(provenance),
        )
    }

    /// Commit local mergeable writes under one admitted authored schema.
    pub(crate) fn commit_mergeable_many_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        commits: Vec<MergeableCommit>,
    ) -> Result<TxId, Error> {
        self.require_catalogue_ready()?;
        if !self
            .catalogue
            .catalogue_schemas
            .contains_key(&schema_version)
        {
            return Err(Error::InvalidMergeableCommit(
                "authored schema version is not admitted",
            ));
        }
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        self.merge_commit_parent_times(&commits)?;
        let made_at = self.mint_tx_time(commits[0].now_ms);
        self.commit_mergeable_many_at_with_schema_versions(
            commits
                .into_iter()
                .map(|commit| (schema_version, commit))
            .collect(),
            made_at,
        )
    }

    fn merge_commit_parent_times(&mut self, commits: &[MergeableCommit]) -> Result<(), Error> {
        for commit in commits {
            if !commit.parents.is_empty() {
                for parent in &commit.parents {
                    self.merge_tx_time(parent.time);
                }
            }
        }
        Ok(())
    }

    fn commit_mergeable_at(
        &mut self,
        commit: MergeableCommit,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        self.commit_mergeable_many_at(vec![commit], made_at)
    }

    fn commit_mergeable_many_at(
        &mut self,
        commits: Vec<MergeableCommit>,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        self.require_catalogue_ready()?;
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let commits = commits
            .into_iter()
            .map(|commit| (write_schema_version, commit))
            .collect();
        self.commit_mergeable_many_at_with_schema_versions(commits, made_at)
    }

    pub(super) fn commit_mergeable_many_at_with_schema_versions(
        &mut self,
        commits: Vec<(SchemaVersionId, MergeableCommit)>,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        self.commit_mergeable_many_at_with_schema_versions_and_provenance(
            commits, made_at, None,
        )
    }

    pub(super) fn commit_mergeable_many_at_with_schema_versions_and_provenance(
        &mut self,
        commits: Vec<(SchemaVersionId, MergeableCommit)>,
        made_at: TxTime,
        contribution_merge: Option<ContributionMergeProvenance>,
    ) -> Result<TxId, Error> {
        validate_generated_large_value_commit_shape(self, &commits)?;
        let tx_id = TxId::new(made_at, self.node_uuid);
        let made_by = commits[0].1.made_by;
        let permission_subject = commits[0].1.effective_permission_subject();
        let user_metadata_json = commits[0].1.user_metadata_json.clone();
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: commits.len().try_into().map_err(|_| {
                Error::InvalidMergeableCommit("transaction write count exceeds u32")
            })?,
            made_by,
            permission_subject: commits[0].1.permission_subject,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json,
            contribution_merge,
        };
        let tx_node_alias = self.ensure_node_alias(tx_id.node)?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                Fate::Pending,
                None,
                self.authored_commit_durability,
            ),
        );
        let mut stored_versions = Vec::new();
        let mut pending_parents = BTreeSet::new();
        for (write_schema_version, commit) in commits {
            let schema_version_alias = self.ensure_schema_version_alias(write_schema_version)?;
            let table_schema = self.table_in_schema(&commit.table, write_schema_version)?;
            let schema = &self
                .catalogue
                .catalogue_schemas
                .get(&write_schema_version)
                .ok_or(Error::InvalidStoredValue("commit schema missing"))?
                .schema;
            let (branch_key, branch_cells) = schema
                .project_branch_selector(&table_schema, &commit.branch)
                .map_err(Error::InvalidBranchKey)?;
            let table_id = self.physical_table_id_for_schema(
                write_schema_version,
                &table_schema.name,
            )?;
            for parent in &commit.parents {
                let parent_versions = self.query_versions_for_tx(*parent)?;
                let same_row = parent_versions.iter().filter(|version| {
                    version.row_uuid() == commit.row_uuid
                        && self.physical_table_id_for_version(version).ok() == Some(table_id)
                });
                if same_row.clone().next().is_some()
                    && !same_row.into_iter().any(|version| version.branch_key() == &branch_key)
                {
                    return Err(Error::InvalidMergeableCommit(
                        "version parent belongs to a different branch-local row",
                    ));
                }
            }
            let layer = VersionLayer::for_commit(&commit);
            let previous_current =
                match self.query_local_layer_winner_in_branch(
                    &table_schema.name,
                    &branch_key,
                    commit.row_uuid,
                    layer,
                )? {
                    Some(previous) => Some(previous),
                    None => self.query_global_layer_winner_in_branch(
                        &table_schema.name,
                        &branch_key,
                        commit.row_uuid,
                        layer,
                    )?,
                };
            let creator_source = if let Some(previous) = previous_current.as_ref() {
                Some(previous.clone())
            } else if layer == VersionLayer::Deletion {
                match self.query_local_layer_winner_in_branch(
                    &table_schema.name,
                    &branch_key,
                    commit.row_uuid,
                    VersionLayer::Content,
                )? {
                    Some(previous) => Some(previous),
                    None => self.query_global_layer_winner_in_branch(
                        &table_schema.name,
                        &branch_key,
                        commit.row_uuid,
                        VersionLayer::Content,
                    )?,
                }
            } else {
                None
            };
            let (created_by, created_at) = creator_source
                .as_ref()
                .map(|version| (version.created_by(), version.created_at()))
                .unwrap_or((commit.made_by, TxTime(commit.now_ms)));

            let parents = if commit.parents.is_empty() {
                Vec::new()
            } else {
                commit.parents
            };
            let mut cells = commit.cells;
            for (column, value) in branch_cells {
                if let Some(authored) = cells.get(&column)
                    && authored != &value
                {
                    return Err(Error::InvalidMergeableCommit(
                        "branch column does not match exact branch key",
                    ));
                }
                cells.insert(column, value);
            }
            let authored_columns = Some(
                commit
                    .authored_columns
                    .clone()
                    .unwrap_or_else(|| cells.keys().cloned().collect()),
            );
            let stored = VersionRow::from_parts_with_schema_version(
                &table_schema,
                VersionRowParts {
                    table: commit.table,
                    branch_key,
                    row_uuid: commit.row_uuid,
                    tx_node_alias,
                    schema_version_alias,
                    tx_time: made_at,
                    parents,
                    created_by,
                    created_at,
                    updated_by: commit.made_by,
                    updated_at: TxTime(commit.now_ms),
                    cells,
                    authored_columns,
                    deletion: commit.deletion,
                },
                (write_schema_version != self.catalogue.current_schema_version_id)
                    .then_some(write_schema_version),
            )?;
            let previous_winner = if let Some(previous) = previous_current.as_ref() {
                Some((
                    previous,
                    self.version_tx_id(previous)?,
                    self.version_made_at(previous)?,
                ))
            } else {
                None
            };
            let new_is_current =
                version_wins_over_open_winner(&stored, tx_id, made_at, previous_winner);
            let _ = (new_is_current, previous_current);
            let (history_table, groove_record) = self.version_storage_write_binding(&stored)?;
            batch.insert_raw(
                history_table.as_ref(),
                self.version_storage_primary_key(&stored)?,
                groove_record,
            );
            self.update_merge_heads_for_content_version(&mut batch, &stored)?;
            self.write_ahead_current_insert(&mut batch, &stored)?;
            pending_parents.extend(stored.parents());
            stored_versions.push(stored);
        }
        for parent in pending_parents {
            if let Some(parent_alias) = self.node_aliases.get(&parent.node).copied() {
                batch.insert(
                    "jazz_pending_edges",
                    pending_edge_values(tx_node_alias, tx_id, parent_alias, parent),
                );
            }
        }
        self.database.commit_batch(batch)?;
        self.cache_tx_versions(tx_id, stored_versions.clone());
        if permission_subject != made_by {
            self.open_tx
                .local_permission_subjects
                .insert(tx_id, permission_subject);
        }
        for stored in &stored_versions {
            self.record_child_edges(tx_id, stored.parents());
        }
        Ok(tx_id)
    }

    /// Commit a local mergeable write and return its sync commit unit.
    pub fn commit_mergeable_unit(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<(TxId, SyncMessage), Error> {
        let tx_id = self.commit_mergeable(commit)?;
        Ok((tx_id, self.commit_unit_for(tx_id)?))
    }

    /// Rebuild the sync commit unit for an already-committed local transaction
    /// from its stored versions.
    ///
    /// Used by the `Db` sync surface to upload a client's local writes upstream
    /// on a connection. Unlike [`NodeState::commit_mergeable_unit`] this reads the
    /// stored versions, so the shipped
    /// unit matches what the author actually stored.
    pub fn commit_unit_for(&mut self, tx_id: TxId) -> Result<SyncMessage, Error> {
        let tx = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?
            .tx
            .clone();
        let versions = self
            .query_versions_for_tx(tx_id)?
            .into_iter()
            .map(|row| self.version_record_from_row(&row))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SyncMessage::CommitUnit { tx, versions })
    }

    /// Open an exclusive transaction over the current snapshot.
    pub fn visible_current_cells(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        Ok(self
            .current_rows(table, DurabilityTier::Local)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
            .map(|row| {
                let table_schema = self.table(table).expect("table exists");
                table_schema
                    .columns
                    .iter()
                    .filter_map(|column| {
                        row.cell(table_schema, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect()
            }))
    }

    /// Read the exact physical cells used to author a replacement version.
    ///
    /// This is mutation bookkeeping only: callers must separately authorize
    /// the logical read before using values from this map.
    pub(crate) fn physical_current_cells(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(table, schema_version)?;
        let Some((row, _)) = self.local_current_content_row_candidate(
            &table_schema,
            row_uuid,
            schema_version,
        )? else {
            return Ok(None);
        };
        Ok(Some(
            table_schema
                .columns
                .iter()
                .filter_map(|column| {
                    row.cell(&table_schema, &column.name)
                        .map(|value| (column.name.clone(), value))
                })
                .collect(),
        ))
    }

    /// Read one exact branch-local row for mutation preparation.
    pub fn visible_current_cells_in_branch(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(table, schema_version)?;
        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("current write schema missing"))?
            .schema;
        let (branch_key, _) = schema
            .project_branch_selector(&table_schema, branch)
            .map_err(Error::InvalidBranchKey)?;
        let deletion = match self.query_local_layer_winner_in_branch(
            table,
            &branch_key,
            row_uuid,
            VersionLayer::Deletion,
        )? {
            Some(version) => Some(version),
            None => self.query_global_layer_winner_in_branch(
                table,
                &branch_key,
                row_uuid,
                VersionLayer::Deletion,
            )?,
        };
        if deletion.is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted)) {
            return Ok(None);
        }
        let content = match self.query_local_layer_winner_in_branch(
            table,
            &branch_key,
            row_uuid,
            VersionLayer::Content,
        )? {
            Some(version) => Some(version),
            None => self.query_global_layer_winner_in_branch(
                table,
                &branch_key,
                row_uuid,
                VersionLayer::Content,
            )?,
        };
        let Some(content) = content
        else {
            return Ok(None);
        };
        self.materialized_cells_for_version(&table_schema, &content)
            .map(Some)
    }

    /// Return the exact local content parent for a branch-local row.
    pub fn local_content_winner_tx_id_in_branch(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id_in_branch_selector(
            table,
            branch,
            row_uuid,
            VersionLayer::Content,
        )
    }

    /// Return the exact local deletion parent for a branch-local row.
    pub fn local_deletion_winner_tx_id_in_branch(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id_in_branch_selector(
            table,
            branch,
            row_uuid,
            VersionLayer::Deletion,
        )
    }

    fn local_layer_winner_tx_id_in_branch_selector(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<TxId>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(table, schema_version)?;
        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("current write schema missing"))?
            .schema;
        let (branch_key, _) = schema
            .project_branch_selector(&table_schema, branch)
            .map_err(Error::InvalidBranchKey)?;
        self.query_local_layer_winner_in_branch(table, &branch_key, row_uuid, layer)?
            .as_ref()
            .map(|version| self.version_tx_id(version))
            .transpose()
    }

    /// Return current rows at the requested durability tier.
    pub fn current_rows(
        &mut self,
        table: &str,
        settled: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.require_catalogue_ready()?;
        let shape = crate::query::Query::from(table).validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.query_rows(&shape, &binding, settled)
    }

    fn local_layer_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<TxId>, Error> {
        self.query_local_layer_winner(table, row_uuid, layer)?
            .as_ref()
            .map(|version| self.version_tx_id(version))
            .transpose()
    }

    pub(crate) fn local_content_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id(table, row_uuid, VersionLayer::Content)
    }

    pub(crate) fn local_deletion_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id(table, row_uuid, VersionLayer::Deletion)
    }

    fn rebuild_ahead_current_keys(&mut self) -> Result<(), Error> {
        #[cfg(feature = "testing")]
        {
            self.rebuild_ahead_current_keys_inner(None)
        }
        #[cfg(not(feature = "testing"))]
        self.rebuild_ahead_current_keys_inner()
    }

    #[cfg(feature = "testing")]
    fn rebuild_ahead_current_keys_with_receipt(
        &mut self,
        receipt: &mut NodeOpenReceipt,
    ) -> Result<(), Error> {
        self.rebuild_ahead_current_keys_inner(Some(receipt))
    }

    fn rebuild_ahead_current_keys_inner(
        &mut self,
        #[cfg(feature = "testing")] mut receipt: Option<&mut NodeOpenReceipt>,
    ) -> Result<(), Error> {
        self.ahead_current_keys.clear();
        self.ahead_current_rows.clear();
        self.ahead_current_latest.clear();
        let physical_table_ids = self
            .catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values().map(|table| table.table_id))
            .collect::<BTreeSet<_>>();
        for table_id in physical_table_ids {
            let content_rows = self
                .database
                .primary_key_scan_raw(&physical_ahead_current_table_name(table_id), &[])?
                .into_iter()
                .map(|raw| {
                    let record = raw.record();
                    Ok((
                        BranchKey::from_canonical_bytes(
                            record.get_bytes(GlobalCurrentRowRecord::FIELD_BRANCH_KEY_IDX)?,
                        )
                        .map_err(|_| Error::InvalidStoredValue("invalid ahead-current branch key"))?,
                        SchemaVersionAlias(
                            record.get_u64(GlobalCurrentRowRecord::FIELD_SCHEMA_VERSION_IDX)?,
                        ),
                        RowUuid(record.get_uuid(GlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?),
                        TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
                        NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            for (branch_key, alias, row_uuid, tx_time, tx_node_alias) in content_rows {
                #[cfg(feature = "testing")]
                if let Some(receipt) = &mut receipt {
                    receipt.ahead_current_entries += 1;
                }
                self.insert_ahead_current_key(
                    self.logical_table_for_physical_alias(table_id, alias)?,
                    branch_key,
                    VersionLayer::Content,
                    row_uuid,
                    tx_time,
                    tx_node_alias,
                );
            }
            let deletion_rows = self
                .database
                .primary_key_scan_raw(&physical_register_ahead_current_table_name(table_id), &[])?
                .into_iter()
                .map(|raw| {
                    let record = raw.record();
                    Ok((
                        BranchKey::from_canonical_bytes(
                            record.get_bytes(
                                RegisterGlobalCurrentRowRecord::FIELD_BRANCH_KEY_IDX,
                            )?,
                        )
                        .map_err(|_| Error::InvalidStoredValue("invalid ahead-current branch key"))?,
                        SchemaVersionAlias(
                            record.get_u64(
                                RegisterGlobalCurrentRowRecord::FIELD_SCHEMA_VERSION_IDX,
                            )?,
                        ),
                        RowUuid(
                            record.get_uuid(RegisterGlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?,
                        ),
                        TxTime(record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
                        NodeAlias(
                            record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?,
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            for (branch_key, alias, row_uuid, tx_time, tx_node_alias) in deletion_rows {
                #[cfg(feature = "testing")]
                if let Some(receipt) = &mut receipt {
                    receipt.ahead_current_entries += 1;
                }
                self.insert_ahead_current_key(
                    self.logical_table_for_physical_alias(table_id, alias)?,
                    branch_key,
                    VersionLayer::Deletion,
                    row_uuid,
                    tx_time,
                    tx_node_alias,
                );
            }
        }
        Ok(())
    }

    fn insert_ahead_current_key(
        &mut self,
        table: String,
        branch_key: BranchKey,
        layer: VersionLayer,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) {
        self.ahead_current_keys
            .insert((table.clone(), branch_key, layer, row_uuid, tx_time, tx_node_alias));
        self.ahead_current_rows.insert((table.clone(), row_uuid));
        self.ahead_current_latest
            .entry((table, layer, row_uuid))
            .and_modify(|latest| {
                if (tx_time, tx_node_alias) > *latest {
                    *latest = (tx_time, tx_node_alias);
                }
            })
            .or_insert((tx_time, tx_node_alias));
    }

    fn remove_ahead_current_key(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        layer: VersionLayer,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) {
        let table_key = table.to_owned();
        self.ahead_current_keys.remove(&(
            table_key.clone(),
            branch_key.clone(),
            layer,
            row_uuid,
            tx_time,
            tx_node_alias,
        ));
        let latest_key = (table_key.clone(), layer, row_uuid);
        if self.ahead_current_latest.get(&latest_key) == Some(&(tx_time, tx_node_alias)) {
            if let Some((_, _, _, _, next_time, next_alias)) = self
                .ahead_current_keys
                .iter()
                .filter(|(candidate_table, _, candidate_layer, candidate_row, _, _)| {
                    candidate_table == &table_key
                        && *candidate_layer == layer
                        && *candidate_row == row_uuid
                })
                .max_by_key(|(_, _, _, _, time, alias)| (*time, *alias))
                .cloned()
            {
                self.ahead_current_latest
                    .insert(latest_key, (next_time, next_alias));
            } else {
                self.ahead_current_latest.remove(&latest_key);
            }
        }
        if !self.ahead_current_latest.contains_key(&(
            table_key.clone(),
            VersionLayer::Content,
            row_uuid,
        )) && !self.ahead_current_latest.contains_key(&(
            table_key.clone(),
            VersionLayer::Deletion,
            row_uuid,
        )) {
            self.ahead_current_rows.remove(&(table_key, row_uuid));
        }
    }

    pub(super) fn cached_tx_version_tables(&self, tx_id: TxId) -> Option<BTreeSet<String>> {
        self.query.tx_version_tables_cache.get(&tx_id).cloned()
    }

    pub(super) fn cached_tx_versions(&self, tx_id: TxId) -> Option<Vec<VersionRow>> {
        self.query.tx_versions_cache.get(&tx_id).cloned()
    }

    pub(super) fn cache_tx_version_tables(&mut self, tx_id: TxId, tables: BTreeSet<String>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_version_tables_cache.insert(tx_id, tables);
        self.bound_tx_version_cache();
    }

    pub(super) fn cache_tx_versions(&mut self, tx_id: TxId, versions: Vec<VersionRow>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_versions_cache.insert(tx_id, versions);
        self.bound_tx_version_cache();
    }

    fn touch_tx_version_cache_entry(&mut self, tx_id: TxId) {
        if self.query.tx_version_tables_cache_order_set.insert(tx_id) {
            self.query.tx_version_tables_cache_order.push_back(tx_id);
        }
    }

    fn bound_tx_version_cache(&mut self) {
        while self.query.tx_version_tables_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
            || self.query.tx_versions_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
        {
            let Some(oldest) = self.query.tx_version_tables_cache_order.pop_front() else {
                break;
            };
            if !self.query.tx_version_tables_cache_order_set.remove(&oldest) {
                continue;
            }
            self.query.tx_version_tables_cache.remove(&oldest);
            self.query.tx_versions_cache.remove(&oldest);
        }
    }

    pub(super) fn invalidate_tx_version_tables_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
        self.query.tx_versions_cache.remove(&tx_id);
        self.query.tx_version_tables_cache_order_set.remove(&tx_id);
    }

    pub(super) fn invalidate_tx_version_table_names_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
    }

    fn materialize_current_row(
        &mut self,
        table: &TableSchema,
        row: CurrentRow,
    ) -> Result<CurrentRow, Error> {
        if !table.columns.iter().any(|column| column.large_value.is_some()) {
            return Ok(row);
        }
        let (descriptor, raw) = row.encoded_record();
        let borrowed = BorrowedRecord::new(raw, descriptor);
        let mut values = (0..descriptor.fields().len())
            .map(|index| borrowed.get_idx(index))
            .collect::<Result<Vec<_>, _>>()?;
        let mut changed = false;
        for column in &table.columns {
            let Some(schema) = &column.large_value else {
                continue;
            };
            let field = descriptor
                .field_index(&user_column_field(&column.name))
                .or_else(|| descriptor.field_index(&column.name));
            let Some(field) = field else {
                continue;
            };
            // `CurrentRow::cell` resolves the incoming descriptor by name and
            // removes its projection-level nullable wrapper.
            let Some(value) = row.cell(table, &column.name) else {
                continue;
            };
            let stored = match large_value_leaf(&value) {
                Some(value @ (Value::Bytes(_) | Value::String(_))) => value.clone(),
                None => continue,
                // Query-engine projections may reuse an application column's
                // output name for a differently typed intermediate slot. It
                // is not a stored cell and therefore needs no materialization.
                _ => continue,
            };
            if !crate::large_values::LargeValue::storage_value_is_framed(schema, &stored) {
                continue;
            }
            let row_uuid = row.row_uuid();
            let domain = crate::large_values::LargeValueOwnerDomain::new(&table.name, row_uuid.0)
            .map_err(|_| Error::InvalidStoredValue("invalid large-value owner domain"))?;
            let value = crate::large_values::LargeValue::decode_storage_value(schema, &stored)
                .map_err(|_| Error::InvalidStoredValue("invalid large-value cell"))?;
            let tree = crate::large_values::ContentTree::new(Default::default())
                .map_err(|_| Error::InvalidStoredValue("invalid large-value tree profile"))?;
            let reader = NodeLargeValueReader::new(self);
            let bytes = value
                .materialize(schema.kind, &domain, tree, &reader)
                .map_err(|_| Error::InvalidStoredValue("invalid or missing large-value node row"))?;
            let logical = schema
                .kind
                .logical_value(bytes)
                .map_err(|_| Error::InvalidStoredValue("invalid materialized large value"))?;
            values[field] = replace_large_value_leaf(&values[field], logical);
            changed = true;
        }
        if !changed {
            return Ok(row);
        }
        let deleted = row.is_deleted();
        let raw = descriptor.create(&values)?;
        let row = CurrentRow::new(
            row.table().to_owned(),
            OwnedRecord::new(raw, *descriptor),
        );
        Ok(if deleted { row.into_deleted() } else { row })
    }

    fn current_row_from_materialized_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<CurrentRow, Error> {
        let row = current_row_from_version_projection(table, version)?;
        self.materialize_current_row(table, row)
    }

    fn materialized_cells_for_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<BTreeMap<String, Value>, Error> {
        let row = current_row_from_version_projection(table, version)?;
        let row = self.materialize_current_row(table, row)?;
        Ok(table
            .columns
            .iter()
            .filter_map(|column| {
                row.cell(table, &column.name)
                    .map(|value| (column.name.clone(), value))
            })
            .collect())
    }

    pub(crate) fn local_current_row(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.local_current_row_in_schema(
            table,
            row_uuid,
            self.catalogue.current_write_schema.schema,
        )
    }

    pub(crate) fn local_current_row_in_schema(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<CurrentRow>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        let content =
            self.local_current_content_row_candidate(&table_schema, row_uuid, schema_version)?;
        let deletion =
            self.local_current_deletion_candidate(&table_schema, row_uuid, schema_version)?;
        if let (Some((_, content_tx)), Some((deletion, deletion_tx))) = (&content, &deletion)
            && deletion_tx > content_tx
            && *deletion == DeletionEvent::Deleted
        {
            return Ok(None);
        }
        content
            .map(|(row, _)| self.materialize_current_row(&table_schema, row))
            .transpose()
    }

    fn local_current_content_row_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<(CurrentRow, (TxTime, NodeUuid))>, Error> {
        let prefix = vec![groove::ivm::LiteralValue::from(Value::Uuid(row_uuid.0))];
        let global = self.physical_current_source_scan_graph(
            schema_version,
            &table.name,
            PhysicalCurrentClass::Global,
            groove::ivm::StaticScanSpec::Point(prefix.clone()),
        )?;
        let ahead = self.physical_current_source_scan_graph(
            schema_version,
            &table.name,
            PhysicalCurrentClass::Ahead,
            groove::ivm::StaticScanSpec::Prefix(prefix),
        )?;
        let result = self
            .database
            .query_graph(GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ))
            .map_err(|error| Self::malformed_current_query_error(&table.name, row_uuid, error))?;
        let Some(delta) = result.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let record = BorrowedRecord::new(&delta.record, &result.descriptor);
        let tx = self.current_record_sort_key(&table.name, row_uuid, record)?;
        Ok(Some((decode_current_row(table, record)?, tx)))
    }

    fn local_current_deletion_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<(DeletionEvent, (TxTime, NodeUuid))>, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, &table.name)?;
        let prefix = vec![groove::ivm::LiteralValue::from(Value::Uuid(row_uuid.0))];
        let global = GraphBuilder::table_scan(
            physical_register_global_current_table_name(table_id),
            groove::ivm::StaticScanSpec::Point(prefix.clone()),
        );
        let ahead = GraphBuilder::table_scan(
            physical_register_ahead_current_table_name(table_id),
            groove::ivm::StaticScanSpec::Prefix(prefix),
        );
        let result = self
            .database
            .query_graph(GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ))
            .map_err(|error| Self::malformed_current_query_error(&table.name, row_uuid, error))?;
        let Some(delta) = result.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let record = BorrowedRecord::new(&delta.record, &result.descriptor);
        Ok(Some((
            deletion_event_from_value(
                record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
            )?,
            self.current_record_sort_key(&table.name, row_uuid, record)?,
        )))
    }

    fn current_record_sort_key(
        &self,
        table: &str,
        row_uuid: RowUuid,
        record: BorrowedRecord<'_>,
    ) -> Result<(TxTime, NodeUuid), Error> {
        let malformed = |source| {
            Error::MalformedCurrentRow(Box::new(MalformedCurrentRow {
                table: table.to_owned(),
                row_uuid,
                source,
            }))
        };
        let tx_time = TxTime(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)
                .map_err(malformed)?,
        );
        let tx_node_alias = NodeAlias(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)
                .map_err(malformed)?,
        );
        let tx_node = self
            .node_aliases
            .iter()
            .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
            .ok_or(Error::InvalidStoredValue(
                "current row references unknown node alias",
            ))?;
        Ok((tx_time, tx_node))
    }

    fn malformed_current_query_error(
        table: &str,
        row_uuid: RowUuid,
        error: GrooveDbError,
    ) -> Error {
        let source = match error {
            GrooveDbError::RecordEncoding(source)
            | GrooveDbError::IvmRuntime(groove::ivm::IvmRuntimeError::RecordEncoding(source)) => {
                source
            }
            error => return Error::Groove(error),
        };
        Error::MalformedCurrentRow(Box::new(MalformedCurrentRow {
            table: table.to_owned(),
            row_uuid,
            source,
        }))
    }

}

fn large_value_leaf(value: &Value) -> Option<&Value> {
    match value {
        Value::Nullable(Some(value)) => large_value_leaf(value),
        Value::Nullable(None) => None,
        value => Some(value),
    }
}

fn replace_large_value_leaf(template: &Value, replacement: Value) -> Value {
    match template {
        Value::Nullable(Some(value)) => Value::Nullable(Some(Box::new(
            replace_large_value_leaf(value, replacement),
        ))),
        Value::Nullable(None) => template.clone(),
        _ => replacement,
    }
}

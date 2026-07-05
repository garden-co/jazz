//! Read/write policy evaluation for stored versions and view emission. This
//! module owns owner/claim predicate matching, policy joins, policy-atomic
//! exclusive visibility, and memoized policy checks; policy declarations live in
//! [`crate::schema`], pure query syntax in [`crate::query`], and global/current
//! row lookup in [`super::global_state`] and [`super::currency`]. It is the node
//! layer's authorization boundary before data is accepted or shipped.

use super::*;

#[derive(Default)]
pub(super) struct ViewEvaluationContext {
    pub(super) tx_rows: BTreeMap<TxId, Option<StoredTransaction>>,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn write_policy_allows_version_record(
        &mut self,
        version: &VersionRecord,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        let (table, cells) = self.policy_projection_for_version_record(version)?;
        if version.deletion() == Some(DeletionEvent::Deleted) {
            let Some(policy) = table.write_policies.delete_using.clone() else {
                return Ok(true);
            };
            let current = match self.policy_delete_subject_row(&table, version)? {
                Some(current) => current,
                None => current_row_from_cells(&table, version.row_uuid(), &cells)?,
            };
            return self.policy_allows_current_row(&table, &policy, &current, author);
        }
        let is_update = self
            .policy_previous_content_subject_row(&table, version)?
            .is_some();
        if is_update {
            let Some(previous) = self.policy_previous_content_subject_row(&table, version)? else {
                return Ok(false);
            };
            if let Some(policy) = table.write_policies.update_using.clone() {
                if !self.policy_allows_current_row(&table, &policy, &previous, author)? {
                    return Ok(false);
                }
            }
            let Some(policy) = table.write_policies.update_check.clone() else {
                return Ok(true);
            };
            return self.policy_allows(&table, &policy, version.row_uuid(), author, |column| {
                cells
                    .get(column)
                    .cloned()
                    .or_else(|| policy_join_row_value(&previous, &table, column))
            });
        }
        let Some(policy) = table.write_policies.insert_check.clone() else {
            return Ok(true);
        };
        self.write_policy_query_allows_insert_candidate(
            &table,
            &policy,
            version.row_uuid(),
            &cells,
            author,
        )
    }

    pub(crate) fn dry_run_insert_allows(&mut self, commit: MergeableCommit) -> Result<bool, Error> {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table = self.table_in_schema(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(&version, commit.effective_permission_subject())
    }

    pub(crate) fn dry_run_mergeable_write_allows(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table = self.table_in_schema(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(&version, commit.effective_permission_subject())
    }

    pub(crate) fn dry_run_read_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let shape = crate::query::Query::from(table_name)
            .filter(crate::query::eq(
                crate::query::col("id"),
                crate::query::lit(Value::Uuid(row_uuid.0)),
            ))
            .validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.query_rows_for_link(&shape, &binding, DurabilityTier::Local, identity)
            .map(|rows| !rows.is_empty())
    }

    pub(crate) fn dry_run_write_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
        let Some(row) = self
            .current_rows(table_name, DurabilityTier::Local)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
        else {
            return Ok(false);
        };
        let Some(policy) = table.write_policies.update_using.clone() else {
            return Ok(false);
        };
        self.write_policy_query_allows_current_row(&policy, row.row_uuid(), author)
    }

    pub(crate) fn dry_run_delete_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
        let Some(row) = self
            .current_rows(table_name, DurabilityTier::Local)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
        else {
            return Ok(false);
        };
        let Some(policy) = table.write_policies.delete_using.clone() else {
            return Ok(true);
        };
        self.write_policy_query_allows_current_row(&policy, row.row_uuid(), author)
    }

    fn policy_projection_for_version_row(
        &mut self,
        version: &VersionRow,
    ) -> Result<(TableSchema, BTreeMap<String, Value>), Error> {
        let source_schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "history schema version alias must exist",
            ))?;
        let source_table = self.table_in_schema(version.table(), source_schema)?;
        self.translate_policy_cells(
            source_schema,
            version.table(),
            &source_table,
            version.cells(&source_table)?,
        )
    }

    fn policy_projection_for_version_record(
        &mut self,
        version: &VersionRecord,
    ) -> Result<(TableSchema, BTreeMap<String, Value>), Error> {
        let source_schema = version.schema_version();
        let source_table = self.table_in_schema(version.table(), source_schema)?;
        let cells = source_table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| {
                version
                    .optional_cell_at(idx)
                    .map(|value| (column.name.clone(), value))
            })
            .collect::<BTreeMap<_, _>>();
        self.translate_policy_cells(source_schema, version.table(), &source_table, cells)
    }

    fn translate_policy_cells(
        &mut self,
        source: SchemaVersionId,
        table: &str,
        _source_table: &TableSchema,
        mut cells: BTreeMap<String, Value>,
    ) -> Result<(TableSchema, BTreeMap<String, Value>), Error> {
        let target = self.catalogue.current_schema_version_id;
        if source == target {
            return Ok((self.table_in_schema(table, target)?, cells));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Forward, table)?
        {
            let forward_table = apply_compiled_lens_path(&path, &mut cells);
            let table = self.table_in_schema(&forward_table, target)?;
            return Ok((table, cells));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Reverse, table)?
        {
            let reverse_table = apply_compiled_lens_path(&path, &mut cells);
            let table = self.table_in_schema(&reverse_table, target)?;
            return Ok((table, cells));
        }

        let target_table = self.table_in_schema(table, target)?;
        if policy_tables_are_directly_compatible(_source_table, &target_table) {
            return Ok((target_table, cells));
        }

        Err(Error::InvalidCatalogueUpdate("lens chain is unknown"))
    }

    fn policy_current_row(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        tier: DurabilityTier,
    ) -> Result<Option<CurrentRow>, Error> {
        Ok(self
            .current_rows_for_schema(&table.name, self.catalogue.current_schema_version_id, tier)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid))
    }

    fn policy_delete_subject_row(
        &mut self,
        table: &TableSchema,
        version: &VersionRecord,
    ) -> Result<Option<CurrentRow>, Error> {
        self.policy_previous_content_subject_row(table, version)
    }

    fn policy_previous_content_subject_row(
        &mut self,
        table: &TableSchema,
        version: &VersionRecord,
    ) -> Result<Option<CurrentRow>, Error> {
        for parent in version.parents() {
            for parent_version in self.query_versions_for_tx(parent)? {
                if parent_version.table() != table.name
                    || parent_version.row_uuid() != version.row_uuid()
                    || parent_version.layer() != VersionLayer::Content
                {
                    continue;
                }
                let (projected_table, cells) =
                    match self.policy_projection_for_version_row(&parent_version) {
                        Ok(projected) => projected,
                        Err(Error::InvalidCatalogueUpdate("lens chain is unknown")) => {
                            let source_schema = self
                                .schema_version_for_alias(parent_version.schema_version_alias())
                                .ok_or(Error::InvalidStoredValue(
                                    "history schema version alias must exist",
                                ))?;
                            let source_table =
                                self.table_in_schema(parent_version.table(), source_schema)?;
                            if !policy_tables_are_directly_compatible(&source_table, table) {
                                return Err(Error::InvalidCatalogueUpdate("lens chain is unknown"));
                            }
                            (table.clone(), parent_version.cells(&source_table)?)
                        }
                        Err(error) => return Err(error),
                    };
                if projected_table.name != table.name {
                    continue;
                }
                return current_row_from_cells(table, version.row_uuid(), &cells).map(Some);
            }
        }

        if let Some(current_version) =
            self.query_local_layer_winner(&table.name, version.row_uuid(), VersionLayer::Content)?
        {
            let (projected_table, cells) =
                self.policy_projection_for_version_row(&current_version)?;
            if projected_table.name == table.name {
                return current_row_from_cells(table, version.row_uuid(), &cells).map(Some);
            }
        }

        if let Some(current) =
            self.policy_current_row(table, version.row_uuid(), DurabilityTier::Global)?
        {
            return Ok(Some(current));
        }

        Ok(None)
    }

    fn policy_allows_current_row(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row: &CurrentRow,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        self.policy_allows(table, policy, row.row_uuid(), identity, |column| {
            policy_join_row_value(row, table, column)
        })
    }

    pub(super) fn policy_allows(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        mut column_value: impl FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        if !policy.policy_branches.is_empty() {
            if self.policy_base_allows(table, policy, row_uuid, identity, &mut column_value)? {
                return Ok(true);
            }
            for branch in &policy.policy_branches {
                let branch_policy = branch.as_query(&policy.table);
                if self.policy_base_allows(
                    table,
                    &branch_policy,
                    row_uuid,
                    identity,
                    &mut column_value,
                )? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        self.policy_base_allows(table, policy, row_uuid, identity, &mut column_value)
    }

    pub(super) fn policy_allows_insert_candidate(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        cells: &BTreeMap<String, Value>,
    ) -> Result<bool, Error> {
        if !policy.policy_branches.is_empty() {
            let mut column_value = |column: &str| cells.get(column).cloned();
            if self.policy_base_allows_insert_candidate(
                table,
                policy,
                row_uuid,
                identity,
                &mut column_value,
            )? {
                return Ok(true);
            }
            for branch in &policy.policy_branches {
                let branch_policy = branch.as_query(&policy.table);
                let mut column_value = |column: &str| cells.get(column).cloned();
                if self.policy_base_allows_insert_candidate(
                    table,
                    &branch_policy,
                    row_uuid,
                    identity,
                    &mut column_value,
                )? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        let mut column_value = |column: &str| cells.get(column).cloned();
        self.policy_base_allows_insert_candidate(
            table,
            policy,
            row_uuid,
            identity,
            &mut column_value,
        )
    }

    fn policy_base_allows(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        if !self.policy_filters_allow(table, policy, identity, &mut *column_value)? {
            return Ok(false);
        }
        if !self.policy_joins_allow(table, policy, row_uuid, identity, column_value)? {
            return Ok(false);
        }
        if !self.policy_inherits_allow(
            table,
            policy,
            row_uuid,
            identity,
            &mut *column_value,
            DurabilityTier::Local,
        )? {
            return Ok(false);
        }
        self.policy_reachable_allow(
            table,
            policy,
            row_uuid,
            identity,
            column_value,
            DurabilityTier::Local,
        )
    }

    fn policy_base_allows_insert_candidate(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        if !self.policy_filters_allow(table, policy, identity, &mut *column_value)? {
            return Ok(false);
        }
        if !self.policy_joins_allow(table, policy, row_uuid, identity, column_value)? {
            return Ok(false);
        }
        if !self.policy_insert_inherits_allow(table, policy, identity, &mut *column_value)? {
            return Ok(false);
        }
        self.policy_reachable_allow(
            table,
            policy,
            row_uuid,
            identity,
            column_value,
            DurabilityTier::Local,
        )
    }

    pub(super) fn policy_filters_allow_current_row(
        &self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row: &CurrentRow,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        self.policy_filters_allow(table, policy, identity, |column| {
            policy_join_row_value(row, table, column)
        })
    }

    pub(super) fn policy_filters_allow(
        &self,
        table: &TableSchema,
        policy: &crate::query::Query,
        identity: AuthorId,
        mut column_value: impl FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        for predicate in &policy.filters {
            if !self.policy_predicate_matches(table, predicate, identity, &mut column_value)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn policy_predicate_matches(
        &self,
        table: &TableSchema,
        predicate: &crate::query::Predicate,
        identity: AuthorId,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        match predicate {
            crate::query::Predicate::All(predicates) => {
                for predicate in predicates {
                    if !self.policy_predicate_matches(table, predicate, identity, column_value)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            crate::query::Predicate::Any(predicates) => {
                for predicate in predicates {
                    if self.policy_predicate_matches(table, predicate, identity, column_value)? {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            crate::query::Predicate::Not(predicate) => {
                return self
                    .policy_predicate_matches(table, predicate, identity, column_value)
                    .map(|matches| !matches);
            }
            crate::query::Predicate::In(left, values) => {
                let Some(left_value) =
                    self.policy_operand_value(table, left, identity, &mut *column_value)
                else {
                    return Ok(false);
                };
                return Ok(values.iter().any(|value| {
                    self.policy_operand_value(table, value, identity, &mut *column_value)
                        .is_some_and(|value| {
                            policy_values_equal(&left_value, &value)
                                || policy_value_contains(&left_value, &value)
                        })
                }));
            }
            crate::query::Predicate::Gt(_, _)
            | crate::query::Predicate::Gte(_, _)
            | crate::query::Predicate::Lt(_, _)
            | crate::query::Predicate::Lte(_, _)
            | crate::query::Predicate::IsNull(_) => return Ok(false),
            crate::query::Predicate::Contains(left, right) => {
                let Some(left_value) =
                    self.policy_operand_value(table, left, identity, &mut *column_value)
                else {
                    return Ok(false);
                };
                let Some(right_value) =
                    self.policy_operand_value(table, right, identity, &mut *column_value)
                else {
                    return Ok(false);
                };
                return Ok(policy_value_contains(&left_value, &right_value));
            }
            crate::query::Predicate::Eq(_, _) | crate::query::Predicate::Ne(_, _) => {}
        }
        let (left, right, equal) = match predicate {
            crate::query::Predicate::Eq(left, right) => (left, right, true),
            crate::query::Predicate::Ne(left, right) => (left, right, false),
            _ => unreachable!("handled above"),
        };
        let Some(left_value) = self.policy_operand_value(table, left, identity, &mut *column_value)
        else {
            return Ok(false);
        };
        let Some(right_value) =
            self.policy_operand_value(table, right, identity, &mut *column_value)
        else {
            return Ok(false);
        };
        Ok(policy_values_equal(&left_value, &right_value) == equal)
    }

    pub(super) fn policy_operand_value(
        &self,
        _table: &TableSchema,
        operand: &crate::query::Operand,
        identity: AuthorId,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Option<Value> {
        match operand {
            crate::query::Operand::Column(column) => column_value(column),
            crate::query::Operand::Claim(name) if name == "sub" => Some(Value::Uuid(identity.0)),
            crate::query::Operand::Claim(name) => self
                .session_claims
                .get(&identity)
                .and_then(|claims| claims.get(name))
                .cloned()
                .or_else(|| match name.as_str() {
                    "user_id" => Some(Value::String(identity.0.to_string())),
                    _ => None,
                }),
            crate::query::Operand::Literal(value) => Some(value.clone()),
            crate::query::Operand::Param(_) => None,
        }
    }

    fn policy_joins_allow(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        for join in &policy.joins {
            let join_table = self.table(&join.table)?.clone();
            let target = self.policy_join_target_value(
                table,
                join,
                row_uuid,
                column_value,
                DurabilityTier::Local,
            )?;
            let Some(target) = target else {
                return Ok(false);
            };
            let join_policy = crate::query::Query {
                table: join.table.clone(),
                filters: join.filters.clone(),
                joins: join.nested_joins.clone(),
                policy_branches: Vec::new(),
                reachable: Vec::new(),
                inherits: Vec::new(),
                includes: Vec::new(),
                array_subqueries: Vec::new(),
                select: None,
                order_by: Vec::new(),
                aggregate: None,
                limit: None,
                offset: 0,
            };
            let mut found = false;
            for row in self.current_rows_for_schema(
                &join.table,
                self.catalogue.current_schema_version_id,
                DurabilityTier::Local,
            )? {
                let reaches_row = policy_join_row_value(&row, &join_table, &join.on_column)
                    == Some(target.clone());
                if reaches_row
                    && policy_join_correlations_allow(table, join, &row, &join_table, column_value)
                    && self.policy_allows_current_row(&join_table, &join_policy, &row, identity)?
                {
                    found = true;
                    break;
                }
            }
            if !found {
                return Ok(false);
            }
        }
        let _ = table;
        Ok(true)
    }

    fn policy_inherits_allow(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        _row_uuid: RowUuid,
        identity: AuthorId,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
        tier: DurabilityTier,
    ) -> Result<bool, Error> {
        for inherits in &policy.inherits {
            let Some(Value::Uuid(parent_row_uuid)) = column_value(&inherits.parent_column) else {
                return Ok(false);
            };
            let Some(parent_table_name) = table.references.get(&inherits.parent_column).cloned()
            else {
                return Ok(false);
            };
            let parent_table = self.table(&parent_table_name)?.clone();
            let Some(parent_row) =
                self.policy_current_row(&parent_table, RowUuid(parent_row_uuid), tier)?
            else {
                return Ok(false);
            };
            if let Some(parent_policy) = parent_table.read_policy.clone()
                && !self.policy_allows_current_row(
                    &parent_table,
                    &parent_policy,
                    &parent_row,
                    identity,
                )?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn policy_insert_inherits_allow(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        identity: AuthorId,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        for inherits in &policy.inherits {
            let Some(Value::Uuid(parent_row_uuid)) = column_value(&inherits.parent_column) else {
                return Ok(false);
            };
            let Some(parent_table_name) = table.references.get(&inherits.parent_column).cloned()
            else {
                return Ok(false);
            };
            let parent_table = self.table(&parent_table_name)?.clone();
            let Some(parent_row) = self.policy_current_row(
                &parent_table,
                RowUuid(parent_row_uuid),
                DurabilityTier::Local,
            )?
            else {
                return Ok(false);
            };
            if let Some(update_using) = parent_table.write_policies.update_using.clone()
                && !self.policy_allows_current_row(
                    &parent_table,
                    &update_using,
                    &parent_row,
                    identity,
                )?
            {
                return Ok(false);
            }
            // Child insert inherits parent updateability from whereOld only:
            // parent state is unchanged, so parent update_check/whereNew is
            // intentionally not evaluated here.
        }
        Ok(true)
    }

    fn policy_join_target_value(
        &mut self,
        table: &TableSchema,
        join: &crate::query::JoinVia,
        row_uuid: RowUuid,
        column_value: &mut dyn FnMut(&str) -> Option<Value>,
        tier: DurabilityTier,
    ) -> Result<Option<Value>, Error> {
        if let Some(lookup) = &join.source_lookup {
            let Some(Value::Uuid(parent_row_uuid)) = column_value(&lookup.row_id_source_column)
            else {
                return Ok(None);
            };
            let lookup_table = self.table(&lookup.table)?.clone();
            let Some(parent_row) =
                self.policy_current_row(&lookup_table, RowUuid(parent_row_uuid), tier)?
            else {
                return Ok(None);
            };
            if lookup.value_column == "id" {
                return Ok(Some(Value::Uuid(parent_row.row_uuid().0)));
            }
            return Ok(parent_row.cell(&lookup_table, &lookup.value_column));
        }
        if let Some(source_column) = &join.source_column {
            if source_column == "id" {
                return Ok(Some(Value::Uuid(row_uuid.0)));
            }
            return Ok(column_value(source_column));
        }
        let _ = table;
        Ok(Some(Value::Uuid(row_uuid.0)))
    }

    fn policy_reachable_allow(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        mut column_value: impl FnMut(&str) -> Option<Value>,
        tier: DurabilityTier,
    ) -> Result<bool, Error> {
        for reachable in &policy.reachable {
            let mut reachable_teams = BTreeSet::new();
            if let Some(seed) = &reachable.seed {
                let seed_table = self.table(&seed.table)?.clone();
                let seed_policy = crate::query::Query {
                    table: seed.table.clone(),
                    filters: seed.filters.clone(),
                    joins: Vec::new(),
                    policy_branches: Vec::new(),
                    reachable: Vec::new(),
                    inherits: Vec::new(),
                    includes: Vec::new(),
                    array_subqueries: Vec::new(),
                    select: None,
                    order_by: Vec::new(),
                    aggregate: None,
                    limit: None,
                    offset: 0,
                };
                for seed_row in self.current_rows_for_schema(
                    &seed.table,
                    self.catalogue.current_schema_version_id,
                    tier,
                )? {
                    if !self.policy_filters_allow_current_row(
                        &seed_table,
                        &seed_policy,
                        &seed_row,
                        identity,
                    )? {
                        continue;
                    }
                    if let Some(Value::Uuid(seed_team)) =
                        policy_join_row_value(&seed_row, &seed_table, &seed.team_column)
                    {
                        reachable_teams.insert(seed_team);
                    }
                }
            } else {
                let Some(Value::Uuid(seed)) =
                    self.policy_operand_value(table, &reachable.from, identity, &mut column_value)
                else {
                    return Ok(false);
                };
                reachable_teams.insert(seed);
            }
            if reachable_teams.is_empty() {
                return Ok(false);
            }
            let edge_table = self.table(&reachable.edge_table)?.clone();
            let edge_policy = crate::query::Query {
                table: reachable.edge_table.clone(),
                filters: reachable.edge_filters.clone(),
                joins: Vec::new(),
                policy_branches: Vec::new(),
                reachable: Vec::new(),
                inherits: Vec::new(),
                includes: Vec::new(),
                array_subqueries: Vec::new(),
                select: None,
                order_by: Vec::new(),
                aggregate: None,
                limit: None,
                offset: 0,
            };
            let edge_rows = self.current_rows_for_schema(
                &reachable.edge_table,
                self.catalogue.current_schema_version_id,
                tier,
            )?;
            for _ in 0..reachable.bound.iteration_cap() {
                let before = reachable_teams.len();
                for edge_row in &edge_rows {
                    if !self.policy_filters_allow_current_row(
                        &edge_table,
                        &edge_policy,
                        edge_row,
                        identity,
                    )? {
                        continue;
                    }
                    let Some(Value::Uuid(member)) =
                        policy_join_row_value(edge_row, &edge_table, &reachable.edge_member_column)
                    else {
                        continue;
                    };
                    if !reachable_teams.contains(&member) {
                        continue;
                    }
                    let Some(Value::Uuid(parent)) =
                        policy_join_row_value(edge_row, &edge_table, &reachable.edge_parent_column)
                    else {
                        continue;
                    };
                    reachable_teams.insert(parent);
                }
                if reachable_teams.len() == before {
                    break;
                }
            }

            let access_table = self.table(&reachable.access_table)?.clone();
            let access_policy = crate::query::Query {
                table: reachable.access_table.clone(),
                filters: reachable.access_filters.clone(),
                joins: Vec::new(),
                policy_branches: Vec::new(),
                reachable: Vec::new(),
                inherits: Vec::new(),
                includes: Vec::new(),
                array_subqueries: Vec::new(),
                select: None,
                order_by: Vec::new(),
                aggregate: None,
                limit: None,
                offset: 0,
            };
            let mut found = false;
            for access_row in self.current_rows_for_schema(
                &reachable.access_table,
                self.catalogue.current_schema_version_id,
                tier,
            )? {
                if !self.policy_filters_allow_current_row(
                    &access_table,
                    &access_policy,
                    &access_row,
                    identity,
                )? {
                    continue;
                }
                let Some(Value::Uuid(access_row_uuid)) =
                    policy_join_row_value(&access_row, &access_table, &reachable.access_row_column)
                else {
                    continue;
                };
                if access_row_uuid != row_uuid.0 {
                    continue;
                }
                let access_team = match reachable.access_team_target {
                    crate::query::JoinTarget::Column => {
                        let Some(Value::Uuid(access_team)) = policy_join_row_value(
                            &access_row,
                            &access_table,
                            &reachable.access_team_column,
                        ) else {
                            continue;
                        };
                        access_team
                    }
                    crate::query::JoinTarget::RowId => access_row.row_uuid().0,
                };
                if reachable_teams.contains(&access_team) {
                    found = true;
                    break;
                }
            }
            if !found {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn query_transaction_memo(
        &mut self,
        tx_id: TxId,
        context: &mut ViewEvaluationContext,
    ) -> Result<Option<StoredTransaction>, Error> {
        if let std::collections::btree_map::Entry::Vacant(entry) = context.tx_rows.entry(tx_id) {
            entry.insert(self.query_transaction(tx_id)?);
        }
        Ok(context
            .tx_rows
            .get(&tx_id)
            .expect("tx row memo populated")
            .clone())
    }
}

fn policy_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nullable(Some(left)), right) => policy_values_equal(left, right),
        (left, Value::Nullable(Some(right))) => policy_values_equal(left, right),
        (Value::Uuid(left), Value::String(right)) => uuid::Uuid::parse_str(right) == Ok(*left),
        (Value::String(left), Value::Uuid(right)) => uuid::Uuid::parse_str(left) == Ok(*right),
        _ => left == right,
    }
}

fn policy_value_contains(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nullable(Some(left)), right) => policy_value_contains(left, right),
        (left, Value::Nullable(Some(right))) => policy_value_contains(left, right),
        (Value::Array(values), right) => {
            values.iter().any(|value| policy_values_equal(value, right))
        }
        (Value::String(left), Value::String(right)) => left.contains(right),
        _ => false,
    }
}

pub(super) fn policy_join_row_value(
    row: &CurrentRow,
    table: &TableSchema,
    column: &str,
) -> Option<Value> {
    if column == "id" {
        Some(Value::Uuid(row.row_uuid().0))
    } else {
        row.cell(table, column)
    }
}

fn policy_join_correlations_allow(
    table: &TableSchema,
    join: &crate::query::JoinVia,
    join_row: &CurrentRow,
    join_table: &TableSchema,
    column_value: &mut dyn FnMut(&str) -> Option<Value>,
) -> bool {
    let _ = table;
    join.correlated_filters.iter().all(|correlation| {
        let Some(source_value) = column_value(&correlation.source_column) else {
            return false;
        };
        policy_join_row_value(join_row, join_table, &correlation.join_column) == Some(source_value)
    })
}

fn policy_tables_are_directly_compatible(source: &TableSchema, target: &TableSchema) -> bool {
    source.name == target.name
        && source.columns.len() == target.columns.len()
        && source
            .columns
            .iter()
            .zip(target.columns.iter())
            .all(|(source, target)| {
                source.name == target.name
                    && source.column_type == target.column_type
                    && source.large_value == target.large_value
            })
}

pub(super) fn policy_value_key(value: &Value) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    match value {
        Value::U8(value) => {
            bytes.push(0);
            bytes.push(*value);
        }
        Value::U16(value) => {
            bytes.push(1);
            bytes.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            bytes.push(2);
            bytes.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            bytes.push(3);
            bytes.extend(value.to_be_bytes());
        }
        Value::F64(value) if !value.is_nan() => {
            bytes.push(4);
            bytes.extend(value.to_bits().to_be_bytes());
        }
        Value::Bool(value) => {
            bytes.push(5);
            bytes.push(u8::from(*value));
        }
        Value::String(value) => {
            bytes.push(6);
            bytes.extend(value.as_bytes());
        }
        Value::Bytes(value) => {
            bytes.push(7);
            bytes.extend(value);
        }
        Value::Uuid(value) => {
            bytes.push(8);
            bytes.extend(value.as_bytes());
        }
        Value::Enum(value) => {
            bytes.push(9);
            bytes.push(*value);
        }
        Value::Tuple(values) => {
            bytes.push(10);
            for value in values {
                let child = policy_value_key(value)?;
                bytes.extend((child.len() as u64).to_be_bytes());
                bytes.extend(child);
            }
        }
        Value::Array(values) => {
            bytes.push(11);
            for value in values {
                let child = policy_value_key(value)?;
                bytes.extend((child.len() as u64).to_be_bytes());
                bytes.extend(child);
            }
        }
        Value::Nullable(None) => bytes.push(12),
        Value::Nullable(Some(value)) => {
            bytes.push(13);
            bytes.extend(policy_value_key(value)?);
        }
        Value::F64(_) => return None,
    }
    Some(bytes)
}

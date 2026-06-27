//! Read/write policy evaluation for stored versions and view emission. This
//! module owns owner/claim predicate matching, policy joins, policy-atomic
//! exclusive visibility, and memoized policy checks; policy declarations live in
//! [`crate::schema`], pure query syntax in [`crate::query`], and global/current
//! row lookup in [`super::global_state`] and [`super::currency`]. It is the node
//! layer's authorization boundary before data is accepted or shipped.

use super::*;

#[derive(Default)]
pub(super) struct ViewEvaluationContext {
    pub(super) tx_read_policy_atomic: BTreeMap<(TxId, AuthorId), bool>,
    pub(super) tx_rows: BTreeMap<TxId, Option<StoredTransaction>>,
    tx_versions: BTreeMap<TxId, Vec<VersionRow>>,
    result_entry_read_policy: BTreeMap<(String, RowUuid, TxId, AuthorId), bool>,
    version_read_policy: BTreeMap<(String, RowUuid, TxId, VersionLayer, AuthorId, bool), bool>,
    policy_join_rows_by_value: BTreeMap<PolicyJoinRowsKey, BTreeMap<Vec<u8>, Vec<CurrentRow>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PolicyJoinRowsKey {
    schema_version: SchemaVersionId,
    table: String,
    on_column: String,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn retain_policy_atomic_rows(
        &mut self,
        set: &mut BTreeSet<ResultRowEntry>,
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<(), Error> {
        if identity == AuthorId::SYSTEM {
            return Ok(());
        }
        let mut row_readable = BTreeMap::new();
        for (table_name, row_uuid, tx_id) in set.iter() {
            row_readable.insert(
                (*table_name, *row_uuid, *tx_id),
                self.result_set_entry_read_policy_allows_memo(
                    table_name, *row_uuid, *tx_id, identity, context,
                )?,
            );
        }
        set.retain(|(table_name, row_uuid, tx_id)| {
            row_readable
                .get(&(*table_name, *row_uuid, *tx_id))
                .copied()
                .unwrap_or(false)
        });
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn result_set_entry_read_policy_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let mut context = ViewEvaluationContext::default();
        self.result_set_entry_read_policy_allows_memo(
            table_name,
            row_uuid,
            tx_id,
            identity,
            &mut context,
        )
    }

    pub(super) fn result_set_entry_read_policy_allows_memo(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        let key = (table_name.to_owned(), row_uuid, tx_id, identity);
        if let Some(allows) = context.result_entry_read_policy.get(&key) {
            return Ok(*allows);
        }
        let table = self.table(table_name)?.clone();
        for version in self.query_versions_for_tx_memo_cloned(tx_id, context)? {
            if version.table() != table_name || version.row_uuid() != row_uuid {
                continue;
            }
            match version.layer() {
                VersionLayer::Content
                    if self
                        .read_policy_allows_version_memo(&table, &version, identity, context)? =>
                {
                    context.result_entry_read_policy.insert(key, true);
                    return Ok(true);
                }
                VersionLayer::Deletion
                    if self.read_policy_allows_deletion_version_memo(
                        &table, &version, identity, context,
                    )? =>
                {
                    context.result_entry_read_policy.insert(key, true);
                    return Ok(true);
                }
                _ => {}
            }
        }
        context.result_entry_read_policy.insert(key, false);
        Ok(false)
    }

    pub(super) fn write_policy_allows_version_record(
        &mut self,
        version: &VersionRecord,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        let (table, cells) = self.policy_projection_for_version_record(version)?;
        let Some(policy) = table.write_policy.clone() else {
            return Ok(true);
        };
        if version.deletion() == Some(DeletionEvent::Deleted) {
            let current = match self.policy_delete_subject_row(&table, version)? {
                Some(current) => current,
                None => current_row_from_cells(&table, version.row_uuid(), &cells)?,
            };
            return self.policy_allows_current_row(&table, &policy, &current, author);
        }
        self.policy_allows(&table, &policy, version.row_uuid(), author, |column| {
            cells.get(column).cloned()
        })
    }

    pub(crate) fn dry_run_insert_allows(&mut self, commit: MergeableCommit) -> Result<bool, Error> {
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
        let table = self.table(table_name)?.clone();
        let Some(version) =
            self.query_local_layer_winner(table_name, row_uuid, VersionLayer::Content)?
        else {
            return Ok(false);
        };
        self.read_policy_allows_version(&table, &version, identity)
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
        let Some(policy) = table.write_policy.clone() else {
            return Ok(true);
        };
        self.policy_allows_current_row(&table, &policy, &row, author)
    }

    pub(crate) fn dry_run_delete_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        self.dry_run_write_current_allows(table_name, row_uuid, author)
    }

    pub(super) fn read_policy_allows_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        if identity == AuthorId::SYSTEM {
            return Ok(true);
        }
        let _ = table;
        let mut context = ViewEvaluationContext::default();
        self.read_policy_allows_version_memo(table, version, identity, &mut context)
    }

    pub(super) fn read_policy_allows_version_memo(
        &mut self,
        _table: &TableSchema,
        version: &VersionRow,
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        if identity == AuthorId::SYSTEM {
            return Ok(true);
        }
        let tx_id = self.version_tx_id(version)?;
        let key = (
            version.table().to_owned(),
            version.row_uuid(),
            tx_id,
            version.layer(),
            identity,
            false,
        );
        if let Some(allows) = context.version_read_policy.get(&key) {
            return Ok(*allows);
        }
        let (table, cells) = self.policy_projection_for_version_row(version)?;
        let allows = if let Some(policy) = table.read_policy.clone() {
            if policy.joins.is_empty() && policy.reachable.is_empty() {
                self.policy_filters_allow(&table, &policy, identity, |column| {
                    cells.get(column).cloned()
                })?
            } else {
                self.policy_allows_memo(
                    &table,
                    &policy,
                    version.row_uuid(),
                    identity,
                    |column| cells.get(column).cloned(),
                    context,
                )?
            }
        } else {
            true
        };
        context.version_read_policy.insert(key, allows);
        Ok(allows)
    }

    #[allow(dead_code)]
    pub(super) fn read_policy_allows_deletion_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let mut context = ViewEvaluationContext::default();
        self.read_policy_allows_deletion_version_memo(table, version, identity, &mut context)
    }

    pub(super) fn read_policy_allows_deletion_version_memo(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        if version.deletion().is_none() {
            return Ok(false);
        }
        if identity == AuthorId::SYSTEM {
            return Ok(true);
        };
        let tx_id = self.version_tx_id(version)?;
        let key = (
            version.table().to_owned(),
            version.row_uuid(),
            tx_id,
            version.layer(),
            identity,
            true,
        );
        if let Some(allows) = context.version_read_policy.get(&key) {
            return Ok(*allows);
        }
        let Some(content) =
            self.query_global_layer_winner(&table.name, version.row_uuid(), VersionLayer::Content)?
        else {
            context.version_read_policy.insert(key, false);
            return Ok(false);
        };
        let allows = self.read_policy_allows_version_memo(table, &content, identity, context)?;
        context.version_read_policy.insert(key, allows);
        Ok(allows)
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
    ) -> Result<Option<CurrentRow>, Error> {
        for tier in [
            DurabilityTier::Global,
            DurabilityTier::Edge,
            DurabilityTier::Local,
        ] {
            if let Some(row) = self
                .current_rows_for_schema(
                    &table.name,
                    self.catalogue.current_schema_version_id,
                    tier,
                )?
                .into_iter()
                .find(|row| row.row_uuid() == row_uuid)
            {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn policy_delete_subject_row(
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

        if let Some(current) = self.policy_current_row(table, version.row_uuid())? {
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
            row.cell(table, column)
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
        if !self.policy_filters_allow(table, policy, identity, &mut column_value)? {
            return Ok(false);
        }
        if !self.policy_joins_allow(table, policy, row_uuid, identity, &mut column_value)? {
            return Ok(false);
        }
        self.policy_reachable_allow(table, policy, row_uuid, identity, column_value)
    }

    fn policy_allows_memo(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        mut column_value: impl FnMut(&str) -> Option<Value>,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        if !self.policy_filters_allow(table, policy, identity, &mut column_value)? {
            return Ok(false);
        }
        if !self.policy_joins_allow_memo(
            table,
            policy,
            row_uuid,
            identity,
            &mut column_value,
            context,
        )? {
            return Ok(false);
        }
        self.policy_reachable_allow(table, policy, row_uuid, identity, column_value)
    }

    pub(super) fn policy_filters_allow_current_row(
        &self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row: &CurrentRow,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        self.policy_filters_allow(table, policy, identity, |column| row.cell(table, column))
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
        mut column_value: &mut dyn FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        match predicate {
            crate::query::Predicate::All(predicates) => {
                for predicate in predicates {
                    if !self.policy_predicate_matches(
                        table,
                        predicate,
                        identity,
                        &mut column_value,
                    )? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            crate::query::Predicate::Any(predicates) => {
                for predicate in predicates {
                    if self.policy_predicate_matches(
                        table,
                        predicate,
                        identity,
                        &mut column_value,
                    )? {
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
            crate::query::Predicate::In(_, _)
            | crate::query::Predicate::Gt(_, _)
            | crate::query::Predicate::Gte(_, _)
            | crate::query::Predicate::Lt(_, _)
            | crate::query::Predicate::Lte(_, _)
            | crate::query::Predicate::Contains(_, _)
            | crate::query::Predicate::IsNull(_) => return Ok(false),
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
            crate::query::Operand::Claim(name) if name == "user_id" => {
                Some(Value::String(identity.0.to_string()))
            }
            crate::query::Operand::Claim(name) => self
                .session_claims
                .get(&identity)
                .and_then(|claims| claims.get(name))
                .cloned(),
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
        mut column_value: impl FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        for join in &policy.joins {
            let join_table = self.table(&join.table)?.clone();
            let target = if let Some(source_column) = &join.source_column {
                column_value(source_column)
            } else {
                Some(Value::Uuid(row_uuid.0))
            };
            let Some(target) = target else {
                return Ok(false);
            };
            let mut found = false;
            for row in self.current_rows_for_schema(
                &join.table,
                self.catalogue.current_schema_version_id,
                DurabilityTier::Global,
            )? {
                let reaches_row = row.cell(&join_table, &join.on_column) == Some(target.clone());
                if reaches_row
                    && self.policy_filters_allow_current_row(
                        &join_table,
                        &crate::query::Query {
                            table: join.table.clone(),
                            filters: join.filters.clone(),
                            joins: Vec::new(),
                            reachable: Vec::new(),
                            includes: Vec::new(),
                            select: None,
                            order_by: Vec::new(),
                            aggregate: None,
                            limit: None,
                            offset: 0,
                        },
                        &row,
                        identity,
                    )?
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

    fn policy_reachable_allow(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        mut column_value: impl FnMut(&str) -> Option<Value>,
    ) -> Result<bool, Error> {
        for reachable in &policy.reachable {
            let Some(Value::Uuid(seed)) =
                self.policy_operand_value(table, &reachable.from, identity, &mut column_value)
            else {
                return Ok(false);
            };
            let mut reachable_teams = BTreeSet::from([seed]);
            let edge_table = self.table(&reachable.edge_table)?.clone();
            let edge_policy = crate::query::Query {
                table: reachable.edge_table.clone(),
                filters: reachable.edge_filters.clone(),
                joins: Vec::new(),
                reachable: Vec::new(),
                includes: Vec::new(),
                select: None,
                order_by: Vec::new(),
                aggregate: None,
                limit: None,
                offset: 0,
            };
            let edge_rows = self.current_rows_for_schema(
                &reachable.edge_table,
                self.catalogue.current_schema_version_id,
                DurabilityTier::Global,
            )?;
            for _ in 0..reachable.max_depth.max(1) {
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
                        edge_row.cell(&edge_table, &reachable.edge_member_column)
                    else {
                        continue;
                    };
                    if !reachable_teams.contains(&member) {
                        continue;
                    }
                    let Some(Value::Uuid(parent)) =
                        edge_row.cell(&edge_table, &reachable.edge_parent_column)
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
                reachable: Vec::new(),
                includes: Vec::new(),
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
                DurabilityTier::Global,
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
                    access_row.cell(&access_table, &reachable.access_row_column)
                else {
                    continue;
                };
                if access_row_uuid != row_uuid.0 {
                    continue;
                }
                let Some(Value::Uuid(access_team)) =
                    access_row.cell(&access_table, &reachable.access_team_column)
                else {
                    continue;
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

    fn policy_joins_allow_memo(
        &mut self,
        table: &TableSchema,
        policy: &crate::query::Query,
        row_uuid: RowUuid,
        identity: AuthorId,
        mut column_value: impl FnMut(&str) -> Option<Value>,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        for join in &policy.joins {
            let join_table = self.table(&join.table)?.clone();
            let target = if let Some(source_column) = &join.source_column {
                column_value(source_column)
            } else {
                Some(Value::Uuid(row_uuid.0))
            };
            let Some(target) = target else {
                return Ok(false);
            };
            let Some(target_key) = policy_value_key(&target) else {
                return Ok(false);
            };
            let join_policy = crate::query::Query {
                table: join.table.clone(),
                filters: join.filters.clone(),
                joins: Vec::new(),
                reachable: Vec::new(),
                includes: Vec::new(),
                select: None,
                order_by: Vec::new(),
                aggregate: None,
                limit: None,
                offset: 0,
            };
            let mut found = false;
            let candidates = self.policy_join_rows_by_target_memo(&join_table, join, context)?;
            if let Some(rows) = candidates.get(&target_key) {
                for row in rows {
                    if self.policy_filters_allow_current_row(
                        &join_table,
                        &join_policy,
                        row,
                        identity,
                    )? {
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                return Ok(false);
            }
        }
        let _ = table;
        Ok(true)
    }

    fn policy_join_rows_by_target_memo<'a>(
        &mut self,
        join_table: &TableSchema,
        join: &crate::query::JoinVia,
        context: &'a mut ViewEvaluationContext,
    ) -> Result<&'a BTreeMap<Vec<u8>, Vec<CurrentRow>>, Error> {
        let key = PolicyJoinRowsKey {
            schema_version: self.catalogue.current_schema_version_id,
            table: join.table.clone(),
            on_column: join.on_column.clone(),
        };
        if !context.policy_join_rows_by_value.contains_key(&key) {
            let mut rows_by_target: BTreeMap<Vec<u8>, Vec<CurrentRow>> = BTreeMap::new();
            for row in self.current_rows_for_schema(
                &join.table,
                self.catalogue.current_schema_version_id,
                DurabilityTier::Global,
            )? {
                if let Some(value) = row.cell(join_table, &join.on_column)
                    && let Some(key) = policy_value_key(&value)
                {
                    rows_by_target.entry(key).or_default().push(row);
                }
            }
            context
                .policy_join_rows_by_value
                .insert(key.clone(), rows_by_target);
        }
        Ok(context
            .policy_join_rows_by_value
            .get(&key)
            .expect("policy join rows memo populated"))
    }

    fn transaction_read_policy_atomic_for_link_inner(
        &mut self,
        tx_id: TxId,
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        let Some(tx) = self.query_transaction(tx_id)? else {
            return Ok(false);
        };
        if tx.tx.kind != TxKind::Exclusive || identity == AuthorId::SYSTEM {
            return Ok(true);
        }
        for version in self.query_versions_for_tx_memo_cloned(tx_id, context)? {
            let table = self.table(version.table())?.clone();
            if !(self.read_policy_allows_version_memo(&table, &version, identity, context)?
                || self.read_policy_allows_deletion_version_memo(
                    &table, &version, identity, context,
                )?)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn transaction_read_policy_atomic_for_link_memo(
        &mut self,
        tx_id: TxId,
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        let key = (tx_id, identity);
        if let Some(allows) = context.tx_read_policy_atomic.get(&key) {
            return Ok(*allows);
        }
        let allows =
            self.transaction_read_policy_atomic_for_link_inner(tx_id, identity, context)?;
        context.tx_read_policy_atomic.insert(key, allows);
        Ok(allows)
    }

    #[allow(dead_code)]
    pub(super) fn transaction_read_policy_atomic_for_link_with_versions_memo(
        &mut self,
        tx_id: TxId,
        tx_versions: &[VersionRow],
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<bool, Error> {
        let key = (tx_id, identity);
        if let Some(allows) = context.tx_read_policy_atomic.get(&key) {
            return Ok(*allows);
        }
        let Some(tx) = self.query_transaction_memo(tx_id, context)? else {
            context.tx_read_policy_atomic.insert(key, false);
            return Ok(false);
        };
        if tx.tx.kind != TxKind::Exclusive || identity == AuthorId::SYSTEM {
            context.tx_read_policy_atomic.insert(key, true);
            return Ok(true);
        }
        let tx_versions = if tx_versions.len() as u32 == tx.tx.n_total_writes {
            tx_versions.to_vec()
        } else {
            self.query_versions_for_tx_memo_cloned(tx_id, context)?
        };
        for version in tx_versions {
            let table = self.table(version.table())?.clone();
            if !(self.read_policy_allows_version_memo(&table, &version, identity, context)?
                || self.read_policy_allows_deletion_version_memo(
                    &table, &version, identity, context,
                )?)
            {
                context.tx_read_policy_atomic.insert(key, false);
                return Ok(false);
            }
        }
        context.tx_read_policy_atomic.insert(key, true);
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

    pub(super) fn query_versions_for_tx_memo_cloned(
        &mut self,
        tx_id: TxId,
        context: &mut ViewEvaluationContext,
    ) -> Result<Vec<VersionRow>, Error> {
        if let std::collections::btree_map::Entry::Vacant(entry) = context.tx_versions.entry(tx_id)
        {
            entry.insert(self.query_versions_for_tx(tx_id)?);
        }
        Ok(context
            .tx_versions
            .get(&tx_id)
            .expect("tx versions memo populated")
            .clone())
    }
}

fn policy_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Uuid(left), Value::String(right)) => uuid::Uuid::parse_str(right) == Ok(*left),
        (Value::String(left), Value::Uuid(right)) => uuid::Uuid::parse_str(left) == Ok(*right),
        _ => left == right,
    }
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

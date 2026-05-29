use super::*;

impl Runtime {
    pub(super) fn query_context(&self) -> query::QueryContext<'_> {
        self.query_context_at_tier(ReadTier::Local)
    }

    pub(crate) fn query_context_at_tier(&self, read_tier: ReadTier) -> query::QueryContext<'_> {
        query::QueryContext {
            conn: &self.conn,
            schema: &self.schema,
            branch_num: self.branch_num,
            user: self.policy_user(),
            bypass_policy: self.bypasses_policy(),
            read_tier,
        }
    }

    pub(super) fn read_visibility(&self) -> ReadVisibility<'_> {
        ReadVisibility {
            conn: &self.conn,
            schema: &self.schema,
            branch_num: self.branch_num,
            user: self.policy_user(),
            bypass_policy: self.bypasses_policy(),
        }
    }

    pub fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
        self.query_context().read_rows(table_name)
    }

    pub fn read_rows_at_tier(&self, table_name: &str, tier: ReadTier) -> Result<Vec<RowView>> {
        self.query_context_at_tier(tier).read_rows(table_name)
    }

    pub fn read_rows_require_ref(
        &self,
        table_name: &str,
        ref_field_name: &str,
    ) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let ref_field = table
            .fields
            .iter()
            .find(|field| field.name == ref_field_name)
            .ok_or_else(|| {
                crate::Error::new(format!(
                    "unknown field {ref_field_name} on table {table_name}"
                ))
            })?;
        let FieldKind::Ref {
            table: target_table,
        } = &ref_field.kind
        else {
            return Err(crate::Error::new(format!(
                "field {ref_field_name} on table {table_name} is not a ref"
            )));
        };
        let visible_targets = self
            .query_context()
            .read_rows(target_table)?
            .into_iter()
            .map(|row| row.id)
            .collect::<BTreeSet<_>>();
        Ok(self
            .query_context()
            .read_rows(table_name)?
            .into_iter()
            .filter(|row| {
                row.values
                    .get(ref_field_name)
                    .and_then(JsonValue::as_str)
                    .is_some_and(|id| visible_targets.contains(id))
            })
            .collect())
    }

    pub(crate) fn read_rows_for_built_query(&self, query: &BuiltQuery) -> Result<Vec<RowView>> {
        self.query_context().read_rows_for_built_query(query)
    }

    pub fn read_rows_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_eq(table_name, field_name, value)
    }

    pub fn read_rows_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_contains(table_name, field_name, needle)
    }

    pub fn read_rows_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_in(table_name, field_name, values)
    }

    pub fn read_rows_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_ne(table_name, field_name, value)
    }

    pub fn read_rows_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_eq_top_created_at_desc(table_name, field_name, value, limit)
    }

    pub fn read_rows_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Vec<RowView>> {
        self.query_context().read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value,
            order_field_name,
            limit,
        )
    }

    pub fn read_recursive_refs(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &str,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_recursive_refs(table_name, root_id, parent_field)
    }

    pub fn read_row_candidates(&self, table_name: &str, id: &str) -> Result<Vec<RowView>> {
        self.query_context().read_row_candidates(table_name, id)
    }

    pub fn read_rows_with_conflict_meta(&self, table_name: &str) -> Result<Vec<RowView>> {
        let mut rows = self.read_rows(table_name)?;
        if rows.is_empty() {
            let mut candidate_ids = self.conflict_candidate_row_ids(table_name)?;
            candidate_ids.sort();
            candidate_ids.dedup();
            for row_id in candidate_ids {
                let candidates = self.read_row_candidates(table_name, &row_id)?;
                if candidates.len() > 1 {
                    rows.extend(candidates);
                }
            }
        }
        for row in &mut rows {
            if self.row_has_current_branch_value(table_name, &row.id)? {
                continue;
            }
            let candidate_count = self.read_row_candidates(table_name, &row.id)?.len();
            if candidate_count > 1 {
                row.conflict_count = candidate_count;
            }
        }
        Ok(rows)
    }

    pub(super) fn row_has_current_branch_value(&self, table_name: &str, id: &str) -> Result<bool> {
        self.schema.table_def(table_name)?;
        let Ok(row_num) = row_num(&self.conn, id) else {
            return Ok(false);
        };
        let count: i64 = self.conn.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {}
                 WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
                crate::schema::current_table(table_name)
            ),
            params![row_num, self.branch_num],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn conflict_candidate_row_ids(&self, table_name: &str) -> Result<Vec<String>> {
        self.schema.table_def(table_name)?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT DISTINCT ids.row_id
             FROM jazz_branch_source source
             JOIN {} current ON current.j_branch_num = source.source_branch_num
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE source.branch_num = ?
               AND current.is_deleted = 0
               AND tx.outcome != ?
             ORDER BY ids.row_id",
            crate::schema::current_table(table_name)
        ))?;
        let rows = stmt.query_map(params![self.branch_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, String>(0)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

//! Ordinary policy equality over verified logical values, not write evidence.
//! Every input belongs to the requested root query or an indexed policy edge.
//! Discovery and evaluation use one synchronous read context and visibility cut.
use super::*;
use crate::compile::PolicyLiteral;
use crate::values::quote;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const ROWS: usize = 512;
const METADATA_BYTES: usize = 8 * 1024 * 1024;
const LOGICAL_BYTES: usize = 8 * 1024 * 1024;
const CHUNK_BYTES: usize = 16 * 1024 * 1024;
const CHUNK_REQUESTS: usize = 1024;
const PREPARATION_STEPS: usize = 65_536;
const SCRATCH_ANSWERS: usize = 4096;
const VM_TICKS: usize = 4096; // 256 instructions per tick.

fn limit() -> Error {
    unsupported("ordinary long-literal read policy exceeds its row/byte/chunk/work budget")
}

#[derive(Default)]
struct Budget {
    work: usize,
    rows: usize,
    metadata: usize,
    logical: usize,
    chunks: usize,
    encoded: usize,
}

fn add(used: &mut usize, amount: usize, max: usize) -> Result<(), Error> {
    *used = used.checked_add(amount).ok_or_else(limit)?;
    if *used > max {
        return Err(limit());
    }
    Ok(())
}

type Input = (String, Vec<(String, Sql)>);

impl Inner {
    /// The caller supplies the actual governing schema and authenticated
    /// session. This builds an ordinary source at its exact cut; it never
    /// constructs CandidateEvidence or replaces the caller's policy identity.
    fn literal_policy_source_ctx<'a>(
        &'a self,
        schema: &'a Schema,
        session: &'a Session,
        binding: &'a BTreeMap<String, Value>,
        read: &ReadSpec,
    ) -> Ctx<'a> {
        Ctx::new(schema, None, session, binding)
            .with_catalogue(self)
            .with_withheld(self.withheld.get())
            .with_view(read.view.clone())
            .with_cut(read.cut.clone(), read.settled_only)
            .with_history_origin(self.history_origin())
    }

    /// The subscriber's supporting rows use the same SELECT contract as its
    /// root query. Keep point id/branch, trusted governing schema and historical
    /// cut exact instead of evaluating them under a different capability.
    pub(super) fn ordinary_select_point(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        cut: Option<Cut>,
    ) -> Result<Option<bool>, Error> {
        if self.candidate_read_evidence.is_some() {
            return Err(unsupported(
                "ordinary SELECT point check in a candidate read scope",
            ));
        }
        let session = self.session(author);
        let binding = BTreeMap::new();
        // Governance is passed explicitly by the trusted point-check caller
        // as `schema`; this spec supplies only ordinary source/cut options.
        let mut read = ReadSpec::new(Governance::CurrentOwner);
        read.cut = cut;
        let mut ctx = self
            .literal_policy_source_ctx(schema, &session, &binding, &read)
            .with_ordinary_policy_literals();
        let condition = ctx.operation_policy(table, Operation::Select, "a", &session)?;
        let source = ctx.source(table)?;
        let questions = ctx.policy_literals;
        let mut params = ctx.params;
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "SELECT COALESCE({condition},0) FROM {source} AS a WHERE a.id=?{} AND a._branch=?{}",
            params.len() - 1,
            params.len()
        );
        let answer = || {
            self.conn
                .cached_row(&sql, params_from_iter(params.iter()), |r| {
                    r.get::<_, i64>(0)
                })
                .optional()
                .map_err(storage)
                .map(|v| v.is_some_and(|v| v != 0))
        };
        if questions.is_empty() {
            // Preserve the established point-check path for every unrelated
            // ordinary policy; this unit owns only actual logical questions.
            return Ok(None);
        }
        if !table.branch_by.is_empty() {
            return Err(unsupported(
                "ordinary long-literal policy on branched point rows",
            ));
        }
        let mut raw = self.literal_policy_source_ctx(schema, &session, &binding, &read);
        let raw_source = raw.source(table)?;
        let mut raw_params = raw.params;
        raw_params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        raw_params.push(Sql::Blob(key.to_vec()));
        let raw_sql = format!(
            "SELECT * FROM {raw_source} AS a WHERE a.id=?{} AND a._branch=?{}",
            raw_params.len() - 1,
            raw_params.len()
        );
        let governing = schema.policy_schema();
        let root = governing
            .by_stored(&table.stored)
            .ok_or_else(|| unsupported("ordinary point policy lacks its actual governing table"))?;
        self.with_read_policy_literals(
            &binding,
            schema,
            root,
            &session,
            &read,
            &raw_sql,
            &raw_params,
            &questions,
            answer,
        )
        .map(Some)
    }

    /// Direct execution must not observe unanswered scratch lookups. All root
    /// and fanout execution goes through this context-bearing entry instead.
    pub(super) fn execute_policy_literals(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        compiled: &Compiled,
    ) -> Result<Vec<CurrentRow>, Error> {
        if compiled.policy_literals.is_empty() {
            return self.execute(compiled, &prepared.query.table);
        }
        if self.candidate_read_evidence.is_some() || !compiled.preps.is_empty() {
            return Err(unsupported(
                "mixing ordinary long-literal reads with candidate evidence",
            ));
        }
        let query = &prepared.query;
        if !query.joins.is_empty()
            || query.flat_join.is_some()
            || !query.reachable.is_empty()
            || !query.policy_branches.is_empty()
            || !query.inherits.is_empty()
            || !query.includes.is_empty()
            || !query.partial.is_empty()
            || !query.array_subqueries.is_empty()
            || query.aggregate.is_some()
            || query.relation.is_some()
            || crate::schema::history::history_source_table(&query.table).is_some()
        {
            return Err(unsupported(
                "ordinary long-literal read policies in joins, relations, includes, partial projections, arrays, aggregates or history queries",
            ));
        }
        let (policy, claims) = self.read_sessions(prepared, identity);
        let session = policy.as_ref().ok_or_else(|| {
            unsupported("ordinary long-literal policy without an authenticated reader")
        })?;
        let schema = self.read_schema(prepared, read);
        let table = schema.table(&query.table)?;
        if !table.branch_by.is_empty() {
            return Err(unsupported(
                "ordinary long-literal policy on branched root rows",
            ));
        }
        let governing = schema.policy_schema();
        let root = governing.by_stored(&table.stored).ok_or_else(|| {
            unsupported("ordinary long-literal policy without its governing table")
        })?;
        // The root selection is an internal dependency query, not a read grant.
        // Keep all real caller filters and the same source/cut, remove windows
        // so pagination cannot hide an unverified dependency, and use a sentinel.
        let mut discovery = query.clone();
        discovery.select = None;
        discovery.partial.clear();
        discovery.limit = Some(ROWS + 1);
        discovery.offset = 0;
        discovery.order_by.clear();
        let raw = self
            .read_ctx(prepared, &schema, None, &claims, read)
            .compile_query(&discovery, read.include_deleted)?;
        if !raw.policy_literals.is_empty() || !raw.preps.is_empty() {
            return Err(unsupported(
                "recursive preparation of ordinary policy dependency roots",
            ));
        }

        self.with_read_policy_literals(
            &prepared.binding,
            &schema,
            root,
            session,
            read,
            &raw.sql,
            &raw.params,
            &compiled.policy_literals,
            || {
                let mut answered = compiled.clone();
                answered.policy_literals.clear();
                self.execute_with_policy_snapshot(&answered, &query.table, true)
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_read_policy_literals<T>(
        &self,
        binding: &BTreeMap<String, Value>,
        schema: &Schema,
        root: &Table,
        session: &Session,
        read: &ReadSpec,
        raw_sql: &str,
        raw_params: &[Sql],
        questions: &[PolicyLiteral],
        body: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        let condition = root
            .policy(Operation::Select)
            .ok_or_else(|| unsupported("ordinary long-literal policy without SELECT admission"))?;
        // One cumulative VM budget covers read-only discovery and final SQL.
        // Scratch writes have a separate finite answer budget and never run
        // under SQLITE_INTERRUPT, which can roll back an enclosing overlay.
        let ticks = Arc::new(AtomicUsize::new(VM_TICKS));
        let exceeded = Arc::new(AtomicBool::new(false));
        // Pin the SQLite snapshot before the first dependency read, including
        // when another persistent connection writes between our statements.
        self.conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS _jazz_read_policy_literal_v1(slot INTEGER NOT NULL,cell INTEGER NOT NULL,answer INTEGER NOT NULL,PRIMARY KEY(slot,cell)); SAVEPOINT jazz_read_policy_literals").map_err(storage)?;
        struct Reset<'a>(&'a Connection, bool);
        impl Reset<'_> {
            fn finish(mut self) -> Result<(), Error> {
                self.0
                    .progress_handler(0, None::<fn() -> bool>)
                    .map_err(storage)?;
                self.0
                    .execute_batch(
                        "ROLLBACK TO jazz_read_policy_literals; RELEASE jazz_read_policy_literals",
                    )
                    .map_err(storage)?;
                self.1 = false;
                Ok(())
            }
        }
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                let _ = self.0.progress_handler(0, None::<fn() -> bool>);
                if self.1 {
                    let _ = self.0.execute_batch(
                        "ROLLBACK TO jazz_read_policy_literals; RELEASE jazz_read_policy_literals",
                    );
                }
            }
        }
        let reset = Reset(&self.conn, true);
        let arm_reads = || {
            let count = ticks.clone();
            let flag = exceeded.clone();
            self.conn
                .progress_handler(
                    256,
                    Some(move || {
                        let stop = count
                            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                                n.checked_sub(1)
                            })
                            .is_err();
                        if stop {
                            flag.store(true, Ordering::Relaxed);
                        }
                        stop
                    }),
                )
                .map_err(storage)
        };
        arm_reads()?;
        let result = (|| {
            let mut budget = Budget::default();
            let roots = self.policy_literal_rows(raw_sql, raw_params, &mut budget)?;
            let mut inputs = Vec::new();
            for row in roots {
                self.policy_literal_inputs(
                    binding,
                    schema,
                    root,
                    &row,
                    session,
                    read,
                    condition,
                    crate::compile::DEFAULT_INHERITS_DEPTH,
                    &mut budget,
                    &mut inputs,
                )?;
                inputs.push((root.stored.clone(), row));
            }
            self.with_policy_literal_answers(questions, &inputs, &mut budget, arm_reads, body)
        })();
        let cleanup = reset.finish();
        if exceeded.load(Ordering::Relaxed) {
            return Err(limit());
        }
        match (result, cleanup) {
            (Err(e), _) | (_, Err(e)) => Err(e),
            (Ok(v), Ok(())) => Ok(v),
        }
    }

    fn policy_literal_rows(
        &self,
        sql: &str,
        params: &[Sql],
        budget: &mut Budget,
    ) -> Result<Vec<Vec<(String, Sql)>>, Error> {
        let mut statement = self.conn.prepare_cached(sql).map_err(storage)?;
        let names = statement
            .column_names()
            .iter()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>();
        let mut rows = statement.query(params_from_iter(params)).map_err(storage)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(storage)? {
            add(&mut budget.rows, 1, ROWS)?;
            let mut values = Vec::new();
            for (i, name) in names.iter().enumerate() {
                add(&mut budget.work, 1, PREPARATION_STEPS)?;
                let value = row.get_ref(i).map_err(storage)?;
                let size = match value {
                    ValueRef::Text(b) | ValueRef::Blob(b) => b.len(),
                    _ => 8,
                };
                add(
                    &mut budget.metadata,
                    name.len().checked_add(size).ok_or_else(limit)?,
                    METADATA_BYTES,
                )?;
                values.push((name.clone(), value.into()));
            }
            out.push(values);
        }
        Ok(out)
    }

    /// Traverse only mandatory indexed scalar correlations. No session/schema
    /// replacement, candidate source, created-row grant or ambient claims.
    #[allow(clippy::too_many_arguments)]
    fn policy_literal_inputs(
        &self,
        binding: &BTreeMap<String, Value>,
        schema: &Schema,
        table: &Table,
        row: &[(String, Sql)],
        session: &Session,
        read: &ReadSpec,
        policy: &PolicyExpr,
        depth: usize,
        budget: &mut Budget,
        inputs: &mut Vec<Input>,
    ) -> Result<(), Error> {
        add(&mut budget.work, 1, PREPARATION_STEPS)?;
        let governing = schema.policy_schema();
        let (target, column, parent, condition, next_depth) = match policy {
            PolicyExpr::And(items) | PolicyExpr::Or(items) => {
                for item in items {
                    self.policy_literal_inputs(
                        binding, schema, table, row, session, read, item, depth, budget, inputs,
                    )?;
                }
                return Ok(());
            }
            PolicyExpr::Not(item) => {
                return self.policy_literal_inputs(
                    binding, schema, table, row, session, read, item, depth, budget, inputs,
                );
            }
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => {
                let depth = max_depth.unwrap_or(depth).min(depth);
                if depth == 0 {
                    return Ok(());
                }
                if crate::compile::is_array(table, via_column) {
                    return Err(unsupported(
                        "array inheritance in ordinary long-literal read policies",
                    ));
                }
                let name = table.references.get(via_column).ok_or_else(|| {
                    unsupported("ordinary policy inheritance without a declared reference")
                })?;
                let target = governing.table(name)?;
                let Some(condition) = target.policy(*operation) else {
                    return Ok(());
                };
                (target, "id", via_column.as_str(), condition, depth - 1)
            }
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => {
                let depth = max_depth.unwrap_or(depth).min(depth);
                if depth == 0 {
                    return Ok(());
                }
                let target = governing.table(source_table)?;
                if crate::compile::is_array(target, via_column) {
                    return Err(unsupported(
                        "reverse-array inheritance in ordinary long-literal read policies",
                    ));
                }
                (
                    target,
                    via_column.as_str(),
                    "id",
                    match target.policy(*operation) {
                        Some(p) => p,
                        None => return Ok(()),
                    },
                    depth - 1,
                )
            }
            PolicyExpr::Exists {
                table: target,
                condition,
            } => {
                let target = governing.table(target)?;
                let (column, parent) = Self::policy_literal_correlation(condition, table, target)
                    .ok_or_else(|| unsupported("ordinary long-literal policy EXISTS without a mandatory indexed UUID correlation"))?;
                (target, column, parent, condition.as_ref(), depth)
            }
            PolicyExpr::ExistsRel { .. } | PolicyExpr::ExistsIncludingCreated { .. } => {
                return Err(unsupported(
                    "relation or created-row evidence in ordinary long-literal read policies",
                ));
            }
            PolicyExpr::Cmp { .. }
            | PolicyExpr::SessionCmp { .. }
            | PolicyExpr::IsNull { .. }
            | PolicyExpr::IsNotNull { .. }
            | PolicyExpr::SessionIsNull { .. }
            | PolicyExpr::SessionIsNotNull { .. }
            | PolicyExpr::Contains { .. }
            | PolicyExpr::SessionContains { .. }
            | PolicyExpr::In { .. }
            | PolicyExpr::InList { .. }
            | PolicyExpr::SessionInList { .. }
            | PolicyExpr::True
            | PolicyExpr::False => return Ok(()),
        };
        if !target.branch_by.is_empty() {
            return Err(unsupported(
                "branched dependencies in ordinary long-literal policies",
            ));
        }
        if column != "id"
            && (!target.branch_by.is_empty()
                || !target
                    .indexes
                    .iter()
                    .any(|i| i.first().is_some_and(|c| c == column)))
        {
            return Err(unsupported(
                "ordinary policy dependency without a leading declared index",
            ));
        }
        add(&mut budget.work, row.len(), PREPARATION_STEPS)?;
        let value = row
            .iter()
            .find(|(k, _)| k == table.stored_name(parent))
            .map(|(_, v)| v)
            .ok_or_else(|| {
                unsupported("ordinary policy dependency key absent from captured root")
            })?;
        if matches!(value, Sql::Null) {
            return Err(unsupported(
                "nullable correlations in ordinary long-literal read policies",
            ));
        }
        if !matches!(value, Sql::Blob(b) if b.len() == 16) {
            return Err(unsupported(
                "ordinary policy dependency key is not a scalar UUID",
            ));
        }
        let mut ctx = self.literal_policy_source_ctx(schema, session, binding, read);
        let source = ctx.source(target)?;
        ctx.params.push(value.clone());
        let parameter = ctx.params.len();
        let sql = format!(
            "SELECT * FROM {source} AS dependency WHERE dependency.{}=?{parameter} AND (dependency._state & {STATE_DELETED})=0 LIMIT {}",
            quote(target.stored_name(column)),
            ROWS - budget.rows + 1
        );
        for found in self.policy_literal_rows(&sql, &ctx.params, budget)? {
            self.policy_literal_inputs(
                binding, schema, target, &found, session, read, condition, next_depth, budget,
                inputs,
            )?;
            inputs.push((target.stored.clone(), found));
        }
        Ok(())
    }

    fn policy_literal_correlation<'a>(
        condition: &'a PolicyExpr,
        outer: &Table,
        target: &Table,
    ) -> Option<(&'a str, &'a str)> {
        match condition {
            PolicyExpr::And(items) => items
                .iter()
                .find_map(|p| Self::policy_literal_correlation(p, outer, target)),
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value: crate::tools::PolicyValue::SessionRef(path),
            } if path.len() == 2 && path[0] == "__jazz_outer_row" => {
                let uuid = |t: &Table, c: &str| {
                    c == "id" || t.column(c).is_some_and(|c| matches!(&c.ty, ValueType::Uuid) || matches!(&c.ty, ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Uuid)))
                };
                (uuid(outer, &path[1]) && uuid(target, column))
                    .then_some((column.as_str(), path[1].as_str()))
            }
            _ => None,
        }
    }

    fn with_policy_literal_answers<T>(
        &self,
        questions: &[PolicyLiteral],
        inputs: &[Input],
        budget: &mut Budget,
        arm_reads: impl FnOnce() -> Result<(), Error>,
        body: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut answers = Vec::new();
        for q in questions {
            add(&mut budget.work, 1, PREPARATION_STEPS)?;
            let mut cells = BTreeSet::new();
            for (table, row) in inputs {
                add(&mut budget.work, 1, PREPARATION_STEPS)?;
                if table == &q.table {
                    add(&mut budget.work, row.len(), PREPARATION_STEPS)?;
                    let value = row.iter().find(|(column, _)| column == &q.column)
                        .map(|(_, value)| value)
                        .ok_or_else(|| unsupported("ordinary policy column is absent from its captured schema-view root"))?;
                    match value {
                        Sql::Integer(cell) => {
                            cells.insert(*cell);
                        }
                        Sql::Null => {}
                        Sql::Text(_) if !q.bytes => {}
                        Sql::Blob(_) if q.bytes => {}
                        _ => {
                            return Err(unsupported(
                                "ordinary policy has an unknown logical cell type",
                            ));
                        }
                    }
                }
            }
            for cell in cells {
                add(&mut budget.work, 1, PREPARATION_STEPS)?;
                let value =
                    crate::large::policy_literal_descriptor_at(&self.conn, cell, &mut |bytes| {
                        add(&mut budget.metadata, bytes, METADATA_BYTES)
                    })?;
                let expected_kind = if q.bytes {
                    jazz_types::large_values::LargeValueKind::Bytes
                } else {
                    jazz_types::large_values::LargeValueKind::String
                };
                if value.kind != expected_kind {
                    return Err(unsupported(
                        "ordinary policy descriptor has an unexpected content kind",
                    ));
                }
                let len = usize::try_from(value.byte_length).map_err(|_| limit())?;
                add(&mut budget.logical, len, LOGICAL_BYTES)?;
                let bytes = self
                    .chunks
                    .policy_literal_bytes(&value, &mut |size| match size {
                        None => add(&mut budget.chunks, 1, CHUNK_REQUESTS),
                        Some(bytes) => add(&mut budget.encoded, bytes, CHUNK_BYTES),
                    })?;
                let answer = match q.op {
                    CmpOp::Eq => bytes == q.literal,
                    CmpOp::Ne => bytes != q.literal,
                    _ => {
                        return Err(unsupported(
                            "non-equality ordinary logical policy comparison",
                        ));
                    }
                };
                if answers.len() == SCRATCH_ANSWERS {
                    return Err(limit());
                }
                answers.push((q.slot as i64, cell, answer));
            }
        }
        // All discovery/materialisation has completed. Never interrupt a
        // scratch INSERT: SQLite may otherwise retire the caller's savepoints.
        self.conn
            .progress_handler(0, None::<fn() -> bool>)
            .map_err(storage)?;
        for (slot, cell, answer) in answers {
            self.conn
                .execute(
                    "INSERT INTO _jazz_read_policy_literal_v1(slot,cell,answer) VALUES(?1,?2,?3)",
                    rusqlite::params![slot, cell, answer],
                )
                .map_err(storage)?;
        }
        arm_reads()?;
        body()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Db, DbConfig, DbIdentity, block_on};
    use crate::ids::{AuthorSubject, NodeUuid};
    use jazz_model::model::public_schema::{
        ColumnType, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    };

    fn fixture() -> Db {
        let select = || TablePolicies::new().with_select(PolicyExpr::True);
        let public = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("roots")
                    .nullable_column("key", ColumnType::Uuid)
                    .policies(select()),
            )
            .table(
                TableSchemaBuilder::new("support")
                    .nullable_column("key", ColumnType::Uuid)
                    .array_fk_column("roots", "roots")
                    .column("body", ColumnType::Text)
                    .index_only(["key", "roots"])
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_literal(
                        "body",
                        crate::tools::Value::Text("a".repeat(70 * 1024)),
                    ))),
            )
            .build();
        block_on(Db::open(DbConfig::new(
            crate::schema::JazzSchema::new(&public).unwrap(),
            DbIdentity {
                node: NodeUuid::from_bytes([95; 16]),
                author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
            },
        )))
        .unwrap()
    }

    /// Private-helper test: the public converter refuses NOT EXISTS today,
    /// so a public Db query cannot exercise this defensive discovery branch.
    /// Alice's NULL correlation must refuse, never become an empty proof.
    #[test]
    fn nullable_negative_discovery_refuses_instead_of_skipping() {
        let db = fixture();
        let inner = db.inner();
        let root = inner.schema.table("roots").unwrap();
        let session =
            inner.session(AuthorSubject::authenticated("https://issuer.example", "alice").unwrap());
        let policy = PolicyExpr::Not(Box::new(PolicyExpr::Exists {
            table: "support".into(),
            condition: Box::new(PolicyExpr::And(vec![
                PolicyExpr::Cmp {
                    column: "key".into(),
                    op: CmpOp::Eq,
                    value: crate::tools::PolicyValue::SessionRef(vec![
                        "__jazz_outer_row".into(),
                        "key".into(),
                    ]),
                },
                PolicyExpr::eq_literal("body", crate::tools::Value::Text("a".repeat(70 * 1024))),
            ])),
        }));
        let error = inner
            .policy_literal_inputs(
                &BTreeMap::new(),
                &inner.schema,
                root,
                &[(root.stored_name("key").to_owned(), Sql::Null)],
                &session,
                &ReadSpec::new(Governance::CurrentOwner),
                &policy,
                crate::compile::DEFAULT_INHERITS_DEPTH,
                &mut Budget::default(),
                &mut Vec::new(),
            )
            .unwrap_err();
        assert!(error.message.contains("nullable correlations"), "{error}");
    }

    /// Private-helper test: the public converter also refuses NOT reverse
    /// inheritance. Exercise the real walker directly, without adding an API
    /// or changing policies: Alice must not skip an array-valued indexed edge.
    #[test]
    fn reverse_array_negative_discovery_refuses_scalar_lookup() {
        let db = fixture();
        let inner = db.inner();
        let root = inner.schema.table("roots").unwrap();
        let session =
            inner.session(AuthorSubject::authenticated("https://issuer.example", "alice").unwrap());
        let policy = PolicyExpr::Not(Box::new(PolicyExpr::InheritsReferencing {
            operation: Operation::Select,
            source_table: "support".into(),
            via_column: "roots".into(),
            max_depth: Some(1),
        }));
        let error = inner
            .policy_literal_inputs(
                &BTreeMap::new(),
                &inner.schema,
                root,
                &[("id".into(), Sql::Blob(vec![1; 16]))],
                &session,
                &ReadSpec::new(Governance::CurrentOwner),
                &policy,
                crate::compile::DEFAULT_INHERITS_DEPTH,
                &mut Budget::default(),
                &mut Vec::new(),
            )
            .unwrap_err();
        assert!(
            error.message.contains("reverse-array inheritance"),
            "{error}"
        );
    }
}

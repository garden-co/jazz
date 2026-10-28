//! Same-unit creates are permission evidence only after independent permission
//! checks. SQLite supplies atomic absence and the complete serializable unit;
//! the proof is a bounded least fixed point, followed by post-state revalidation.
use super::*;
use crate::compile::{CandidateEvidence, PermissionEvidence};

// Connection-local scratch data, never persisted or sent to another runtime.
pub(super) const TEMP_DDL: &str = "CREATE TEMP TABLE IF NOT EXISTS _jazz_candidate_created_v1(t TEXT NOT NULL,id BLOB NOT NULL,PRIMARY KEY(t,id)) WITHOUT ROWID; CREATE TEMP TABLE IF NOT EXISTS _jazz_candidate_qualified_v1(t TEXT NOT NULL,id BLOB NOT NULL,PRIMARY KEY(t,id)) WITHOUT ROWID;";
const MAX_PROOF_WORK: usize = 16_384;
type Coordinate = (String, RowUuid);

pub(super) struct DeferredCheck {
    schema: Rc<Schema>,
    table: String,
    coordinate: Coordinate,
    author: AuthorSubject,
    clause: Option<PolicyExpr>,
    root: Vec<(String, Sql)>,
}

fn marked(schema: &Schema) -> bool {
    schema
        .policy_schema()
        .jazz
        .may_have_authorized_created_insert_sources()
}
fn charge(budget: &mut usize, cost: usize) -> Result<(), Error> {
    *budget = budget.checked_sub(cost).ok_or_else(|| {
        unsupported("authorized-created proof exceeds its bounded work or dependency limit")
    })?;
    Ok(())
}

impl Inner {
    /// Capture the exact policy and root image at the normal check site. The
    /// executor resolves its dependencies only after the complete unit exists.
    pub(super) fn defer_candidate_check(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        clause: Option<&PolicyExpr>,
    ) -> Result<bool, Error> {
        if self.candidate_checks.borrow().is_none() {
            return Ok(false);
        }
        let mut statement = self
            .conn
            .prepare_cached(&format!(
                "SELECT * FROM {} WHERE id=?1 AND _b=?2",
                quote(&table.stored)
            ))
            .map_err(storage)?;
        let names = statement
            .column_names()
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>();
        let root = statement
            .query_row(rusqlite::params![row.as_bytes().as_slice(), key], |r| {
                names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| r.get::<_, Sql>(i).map(|value| (name.clone(), value)))
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(storage)?;
        let mut collector = self.candidate_checks.borrow_mut();
        let checks = collector.as_mut().expect("active collector");
        if checks.len() >= MAX_PROOF_WORK {
            return Err(unsupported(
                "authorized-created proof exceeds its bounded work or dependency limit",
            ));
        }
        checks.push(DeferredCheck {
            schema: self.schema.clone(),
            table: table.name.clone(),
            coordinate: (table.stored.clone(), row),
            author,
            clause: clause.cloned(),
            root,
        });
        Ok(true)
    }

    /// Callback transactions are mergeable. They collect exact check images
    /// until the callback ends but never promote new rows to witnesses.
    pub(super) fn begin_candidate_callback(&mut self, seq: u64) -> Result<(), Error> {
        if self.client.is_some()
            || !self
                .admitted
                .iter()
                .any(|(_, schema)| schema.may_have_authorized_created_insert_sources())
        {
            return Ok(());
        }
        self.conn
            .execute_batch(
                "DELETE FROM _jazz_candidate_created_v1; DELETE FROM _jazz_candidate_qualified_v1;",
            )
            .map_err(storage)?;
        *self.candidate_checks.borrow_mut() = Some(Vec::new());
        self.candidate_read_evidence = Some(CandidateEvidence {
            accepted: self.seq,
            publication: seq,
            post: false,
        });
        self.candidate_callback_budget = Some(MAX_PROOF_WORK);
        Ok(())
    }

    pub(super) fn abandon_candidate_callback(&mut self) {
        if self.candidate_callback_budget.take().is_some() {
            self.candidate_checks.borrow_mut().take();
            self.candidate_read_evidence = None;
        }
    }

    pub(super) fn finish_candidate_callback(&mut self) -> Result<(), Error> {
        let Some(mut budget) = self.candidate_callback_budget.take() else {
            return Ok(());
        };
        let checks = self
            .candidate_checks
            .borrow_mut()
            .take()
            .expect("callback collector");
        let scope = self.candidate_read_evidence.take().expect("callback scope");
        let eligible = BTreeSet::new();
        self.candidate_fixed_point(&checks, &eligible, scope, &mut budget)?;
        self.candidate_fixed_point(
            &checks,
            &eligible,
            CandidateEvidence {
                post: true,
                ..scope
            },
            &mut budget,
        )?;
        self.conn
            .execute_batch(
                "DELETE FROM _jazz_candidate_created_v1; DELETE FROM _jazz_candidate_qualified_v1;",
            )
            .map_err(storage)?;
        Ok(())
    }

    fn candidate_callback_step<T>(
        &mut self,
        ops: &[(Rc<Schema>, StagedOp)],
        apply: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        for (schema, op) in ops {
            charge(
                self.candidate_callback_budget
                    .as_mut()
                    .expect("callback budget"),
                1,
            )?;
            if matches!(op, StagedOp::Move(..)) {
                return Err(unsupported(
                    "candidate-created authorization for branch moves",
                ));
            }
            let table = schema.table(op.table())?;
            if let Some(row) = op.row()
                && matches!(op, StagedOp::Insert(..) | StagedOp::Upsert(..))
                && self.newest_seq(&table.stored, row)?.is_none()
            {
                self.conn
                    .execute(
                        "INSERT OR IGNORE INTO _jazz_candidate_created_v1(t,id) VALUES(?1,?2)",
                        rusqlite::params![table.stored, row.as_bytes().as_slice()],
                    )
                    .map_err(storage)?;
            }
        }
        let mark = self
            .candidate_checks
            .borrow()
            .as_ref()
            .expect("callback collector")
            .len();
        let result = apply(self);
        if result.is_err() {
            // A caller may catch a failed write and keep the callback open.
            // Its row/scratch savepoint rolls back; its captured checks must too.
            self.candidate_checks
                .borrow_mut()
                .as_mut()
                .expect("callback collector")
                .truncate(mark);
        }
        result
    }

    /// Existing ordinary schemas keep their existing executor. A marked schema
    /// installs a private scope only while this savepoint's unit is processed.
    pub(super) fn candidate_unit<T>(
        &mut self,
        ops: &[(Rc<Schema>, StagedOp)],
        seq: u64,
        serializable: bool,
        provisional: bool,
        apply: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self.client.is_some() {
            return apply(self);
        }
        if self.candidate_callback_budget.is_some() {
            return self.candidate_callback_step(ops, apply);
        }
        if !ops.iter().any(|(schema, _)| marked(schema)) {
            return apply(self);
        }
        if self.tx.is_some() {
            return Err(unsupported(
                "candidate permission schemas introduced during an ordinary callback",
            ));
        }
        if self.candidate_checks.borrow().is_some() || self.candidate_read_evidence.is_some() {
            return Err(unsupported("nested SQLite candidate authorization units"));
        }
        let mut budget = MAX_PROOF_WORK;
        let mut counts = BTreeMap::<Coordinate, usize>::new();
        let mut created = BTreeSet::new();
        let mut roots = BTreeSet::new();
        for (schema, op) in ops {
            charge(&mut budget, 1)?;
            let table = schema.table(op.table())?;
            if matches!(op, StagedOp::Move(..)) {
                return Err(unsupported(
                    "candidate-created authorization for branch moves",
                ));
            }
            let Some(row) = op.row() else { continue };
            let coordinate = (table.stored.clone(), row);
            if table.branch_by.is_empty() {
                roots.insert(coordinate.clone());
            }
            *counts.entry(coordinate.clone()).or_default() += 1;
            if matches!(op, StagedOp::Insert(..) | StagedOp::Upsert(..))
                && self.newest_seq(&table.stored, row)?.is_none()
            {
                created.insert(coordinate);
            }
        }
        self.conn
            .execute_batch(
                "DELETE FROM _jazz_candidate_created_v1; DELETE FROM _jazz_candidate_qualified_v1;",
            )
            .map_err(storage)?;
        for (table, row) in &created {
            self.conn
                .execute(
                    "INSERT INTO _jazz_candidate_created_v1(t,id) VALUES(?1,?2)",
                    rusqlite::params![table, row.as_bytes().as_slice()],
                )
                .map_err(storage)?;
        }
        let scope = CandidateEvidence {
            accepted: self.seq,
            publication: seq,
            post: false,
        };
        *self.candidate_checks.borrow_mut() = Some(Vec::new());
        self.candidate_read_evidence = Some(scope);
        let result = apply(self);
        self.candidate_read_evidence = None;
        let checks = self
            .candidate_checks
            .borrow_mut()
            .take()
            .expect("active collector");
        let result = result.and_then(|value| {
            if provisional {
                return Ok(value);
            }
            let eligible = if serializable {
                created
                    .into_iter()
                    .filter(|c| counts[c] == 1 && roots.contains(c))
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            // Each phase starts from no new witnesses. Independent baseline
            // proofs and surviving post-state proofs may have different orders.
            self.candidate_fixed_point(&checks, &eligible, scope, &mut budget)?;
            self.candidate_fixed_point(
                &checks,
                &eligible,
                CandidateEvidence {
                    post: true,
                    ..scope
                },
                &mut budget,
            )?;
            Ok(value)
        });
        let cleanup = self
            .conn
            .execute_batch(
                "DELETE FROM _jazz_candidate_created_v1; DELETE FROM _jazz_candidate_qualified_v1;",
            )
            .map_err(storage);
        match result {
            Err(error) => Err(error),
            Ok(value) => cleanup.map(|()| value),
        }
    }

    fn candidate_fixed_point(
        &self,
        checks: &[DeferredCheck],
        eligible: &BTreeSet<Coordinate>,
        scope: CandidateEvidence,
        budget: &mut usize,
    ) -> Result<(), Error> {
        self.conn
            .execute("DELETE FROM _jazz_candidate_qualified_v1", [])
            .map_err(storage)?;
        let mut remaining = (0..checks.len()).collect::<BTreeSet<_>>();
        let mut qualified = BTreeSet::new();
        // A trusted SYSTEM write has no session checks: that authorization is
        // independent, not a provenance claim from an ordinary connection.
        let mut pending_by_row = BTreeMap::<Coordinate, usize>::new();
        for check in checks {
            *pending_by_row.entry(check.coordinate.clone()).or_default() += 1;
        }
        for coordinate in eligible {
            if !pending_by_row.contains_key(coordinate) {
                self.qualify_candidate(coordinate)?;
                qualified.insert(coordinate.clone());
            }
        }
        loop {
            let mut advanced = false;
            for index in remaining.iter().copied().collect::<Vec<_>>() {
                charge(budget, 1)?;
                if !self.candidate_check(&checks[index], scope, budget)? {
                    continue;
                }
                remaining.remove(&index);
                let coordinate = &checks[index].coordinate;
                let pending = pending_by_row.get_mut(coordinate).expect("counted check");
                *pending -= 1;
                if *pending == 0
                    && eligible.contains(coordinate)
                    && qualified.insert(coordinate.clone())
                {
                    self.qualify_candidate(coordinate)?;
                    advanced = true;
                }
            }
            if remaining.is_empty() {
                return Ok(());
            }
            if !advanced {
                return Err(denied(
                    "independent same-unit INSERT authorization was not established",
                ));
            }
        }
    }

    fn qualify_candidate(&self, coordinate: &Coordinate) -> Result<(), Error> {
        self.conn
            .execute(
                "INSERT INTO _jazz_candidate_qualified_v1(t,id) VALUES(?1,?2)",
                rusqlite::params![coordinate.0, coordinate.1.as_bytes().as_slice()],
            )
            .map_err(storage)?;
        Ok(())
    }

    fn candidate_check(
        &self,
        check: &DeferredCheck,
        scope: CandidateEvidence,
        budget: &mut usize,
    ) -> Result<bool, Error> {
        let table = check.schema.table(&check.table)?;
        let session = self.session(check.author);
        let binding = BTreeMap::new();
        let mut ctx = Ctx::new(&check.schema, None, &session, &binding)
            .with_catalogue(self)
            .with_candidate_evidence(Some(scope));
        let condition = ctx.policy_clause(check.clause.as_ref(), table, "a", &session)?;
        // Generated SQL and preparation work are bounded too, not just the
        // number of passes through the candidate graph.
        charge(
            budget,
            1 + ctx.tables.len() + ctx.params.len() + ctx.preps.len(),
        )?;
        let preps = std::mem::take(&mut ctx.preps);
        let mut params = ctx.params;
        let columns = check
            .root
            .iter()
            .map(|(name, value)| {
                params.push(value.clone());
                format!("?{} AS {}", params.len(), quote(name))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT COALESCE({condition},0) FROM (SELECT {columns}) AS a");
        let inputs = if preps.is_empty() {
            Vec::new()
        } else {
            let schema = check.schema.policy_schema();
            let governing = schema
                .by_stored(&table.stored)
                .ok_or_else(|| unsupported("candidate proof under a missing governing table"))?;
            let mut inputs = vec![(table.stored.clone(), check.root.clone())];
            if let Some(clause) = &check.clause {
                self.candidate_inputs(
                    schema,
                    governing,
                    &check.root,
                    None,
                    &session,
                    clause,
                    scope.into(),
                    crate::compile::DEFAULT_INHERITS_DEPTH,
                    budget,
                    &mut inputs,
                )?;
            }
            inputs
        };
        let value: i64 = self.with_candidate_sql_budget(budget, || {
            self.with_candidate_preps(&preps, &inputs, || {
                self.conn
                    .query_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
                    .map_err(storage)
            })
        })?;
        Ok(value != 0)
    }
}

pub(super) type PolicyInput = (String, Vec<(String, Sql)>);

impl Inner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn candidate_inputs(
        &self,
        schema: &Schema,
        table: &Table,
        root: &[(String, Sql)],
        outer: Option<(&Table, &[(String, Sql)])>,
        session: &Session,
        policy: &PolicyExpr,
        scope: PermissionEvidence,
        depth: usize,
        budget: &mut usize,
        inputs: &mut Vec<PolicyInput>,
    ) -> Result<(), Error> {
        charge(budget, 1)?;
        // Match the compiler's inheritance depth and operation selection. An
        // inherited UPDATE capability uses USING, even for an immutable root
        // whose own WITH CHECK refuses all actual updates.
        let (target, column, parent, condition, created, next_depth) = match policy {
            PolicyExpr::And(items) | PolicyExpr::Or(items) => {
                for item in items {
                    self.candidate_inputs(
                        schema, table, root, outer, session, item, scope, depth, budget, inputs,
                    )?;
                }
                return Ok(());
            }
            PolicyExpr::Not(item) => {
                return self.candidate_inputs(
                    schema, table, root, outer, session, item, scope, depth, budget, inputs,
                );
            }
            PolicyExpr::Exists {
                table: target,
                condition,
            }
            | PolicyExpr::ExistsIncludingCreated {
                table: target,
                condition,
            } => {
                let target = schema.table(target)?;
                let (column,parent)=crate::compile::candidate_correlation(condition,table,target)
                    .ok_or_else(||unsupported("large-value candidate permission dependencies without a scalar declared-reference correlation or indexed UUID equality"))?;
                (
                    target,
                    column,
                    parent,
                    condition.as_ref(),
                    matches!(policy, PolicyExpr::ExistsIncludingCreated { .. }),
                    depth,
                )
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
                let name = table.references.get(via_column).ok_or_else(|| {
                    unsupported(
                        "inheritance without a declared reference in candidate authorization units",
                    )
                })?;
                let target = schema.table(name)?;
                let Some(condition) = target.inherited_policy(*operation) else {
                    return Ok(());
                };
                (
                    target,
                    "id",
                    via_column.as_str(),
                    condition,
                    false,
                    depth - 1,
                )
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
                let target = schema.table(source_table)?;
                let Some(condition) = target.inherited_policy(*operation) else {
                    return Ok(());
                };
                (
                    target,
                    via_column.as_str(),
                    "id",
                    condition,
                    false,
                    depth - 1,
                )
            }
            PolicyExpr::ExistsRel { rel } => {
                let (table, root) = outer.unwrap_or((table, root));
                let binding = BTreeMap::new();
                let (queries, params) = Ctx::new(schema, None, session, &binding)
                    .with_catalogue(self)
                    .with_permission_evidence(scope)
                    .candidate_relation_inputs(rel, table, root, session)?;
                charge(budget, 1 + queries.len() + params.len())?;
                for (table, sql) in queries {
                    let sql = format!("{sql} LIMIT {}", *budget + 1);
                    let found = self.with_candidate_sql_budget(budget, || {
                        let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
                        let names = statement
                            .column_names()
                            .iter()
                            .map(|name| (*name).to_owned())
                            .collect::<Vec<_>>();
                        statement
                            .query_map(params_from_iter(params.iter()), |row| {
                                names
                                    .iter()
                                    .enumerate()
                                    .map(|(i, name)| {
                                        row.get::<_, Sql>(i).map(|value| (name.clone(), value))
                                    })
                                    .collect::<rusqlite::Result<Vec<_>>>()
                            })
                            .map_err(storage)?
                            .collect::<rusqlite::Result<Vec<_>>>()
                            .map_err(storage)
                    })?;
                    for row in found {
                        charge(budget, 1 + row.len())?;
                        inputs.push((table.clone(), row));
                    }
                }
                return Ok(());
            }
            _ => return Ok(()),
        };
        let value = root
            .iter()
            .find(|(name, _)| name == table.stored_name(parent))
            .map(|(_, v)| v)
            .ok_or_else(|| {
                unsupported("candidate permission correlation absent from its root image")
            })?;
        if matches!(value, Sql::Null) {
            return Ok(());
        }
        let values = if matches!(policy, PolicyExpr::Inherits { .. })
            && crate::compile::is_array(table, parent)
        {
            crate::array_index::members(value)?
        } else {
            vec![value.clone()]
        };
        charge(budget, values.len())?;
        for value in values {
            let claims = Session::default();
            let binding = BTreeMap::new();
            let mut ctx = Ctx::new(schema, None, &claims, &binding)
                .with_catalogue(self)
                .with_permission_evidence(scope);
            ctx.params.push(value);
            let bound = format!("?{}", ctx.params.len());
            let source = ctx.candidate_source(target, created, column, &bound)?;
            let params = ctx.params;
            let sql = format!("SELECT * FROM {source} WHERE _del=0 LIMIT {}", *budget + 1);
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
            let names = statement
                .column_names()
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>();
            let matches = statement
                .query_map(params_from_iter(params.iter()), |row| {
                    names
                        .iter()
                        .enumerate()
                        .map(|(i, name)| row.get::<_, Sql>(i).map(|v| (name.clone(), v)))
                        .collect::<Result<Vec<_>, _>>()
                })
                .map_err(storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?;
            drop(statement);
            for found in matches {
                charge(budget, 1 + found.len())?;
                self.candidate_inputs(
                    schema,
                    target,
                    &found,
                    matches!(
                        policy,
                        PolicyExpr::Exists { .. } | PolicyExpr::ExistsIncludingCreated { .. }
                    )
                    .then_some((table, root)),
                    session,
                    condition,
                    scope,
                    next_depth,
                    budget,
                    inputs,
                )?;
                inputs.push((target.stored.clone(), found));
            }
        }
        Ok(())
    }

    /// Count VM work as well as Rust proof steps. Recursive relation queries
    /// can revisit a finite graph many times before their depth bound, so each
    /// batch of 256 SQLite instructions also consumes one unit of proof budget.
    pub(super) fn with_candidate_sql_budget<T>(
        &self,
        budget: &mut usize,
        body: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        let initial = *budget;
        let counter = Arc::new((AtomicUsize::new(initial), AtomicBool::new(false)));
        let meter = counter.clone();
        self.conn
            .progress_handler(
                256,
                Some(move || {
                    let exhausted = meter
                        .0
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                            left.checked_sub(1)
                        })
                        .is_err();
                    if exhausted {
                        meter.1.store(true, Ordering::Relaxed);
                    }
                    exhausted
                }),
            )
            .map_err(storage)?;
        // Also remove the callback if a test or host unwinds this call.
        struct Reset<'a>(&'a rusqlite::Connection);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                let _ = self.0.progress_handler(0, None::<fn() -> bool>);
            }
        }
        let reset = Reset(&self.conn);
        let result = body();
        drop(reset);
        charge(budget, initial - counter.0.load(Ordering::Relaxed))?;
        if counter.1.load(Ordering::Relaxed) {
            return Err(unsupported(
                "authorized-created proof exceeds its bounded work or dependency limit (SQL instruction budget)",
            ));
        }
        result
    }

    pub(super) fn with_candidate_preps<T>(
        &self,
        preps: &[crate::compile::Prep],
        inputs: &[PolicyInput],
        body: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        if preps
            .iter()
            .any(|prep| matches!(prep, crate::compile::Prep::Rank { .. }))
        {
            return Err(unsupported(
                "rank preparations in candidate-created permission proofs",
            ));
        }
        self.with_bounded_preps(preps, inputs, |_| body())
    }

    /// Prepare comparisons and ordering from an explicit evidence set. The body
    /// receives whether distinct chunked cells tie on their stored sort prefix.
    pub(super) fn with_bounded_preps<T>(
        &self,
        preps: &[crate::compile::Prep],
        inputs: &[PolicyInput],
        body: impl FnOnce(bool) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if preps.is_empty() {
            return body(false);
        }
        self.conn
            .execute_batch("SAVEPOINT jazz_candidate_scratch")
            .map_err(storage)?;
        let result = (|| {
            let mut ties = false;
            for prep in preps {
                let columns = match prep {
                    crate::compile::Prep::Class { columns, .. } => columns.clone(),
                    crate::compile::Prep::Match { table, column, .. }
                    | crate::compile::Prep::Rank { table, column, .. } => {
                        vec![(table.clone(), column.clone())]
                    }
                };
                let mut cells = BTreeSet::new();
                for (table, root) in inputs {
                    for (name, value) in root {
                        if columns.iter().any(|(t, c)| t == table && c == name)
                            && let Sql::Text(bytes) = value
                        {
                            cells.insert(bytes.clone());
                        }
                    }
                }
                ties |= crate::large::answer_cells(&self.conn, &self.chunks, prep, &cells)?;
            }
            body(ties)
        })();
        for prep in preps {
            self.conn
                .execute("DELETE FROM _jazz_lv_scratch WHERE slot=?1", [prep.slot()])
                .map_err(storage)?;
        }
        self.conn
            .execute_batch("RELEASE jazz_candidate_scratch")
            .map_err(storage)?;
        result
    }
}

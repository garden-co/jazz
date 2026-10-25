//! Ordinary transaction policy evidence (INV-RLS-9).
//!
//! A private savepoint presents accepted rows and independently grounded writes
//! to the normal policy compiler. Only this unit's exact coordinates change;
//! rollback restores its final rows before settlement, without publishing them.
use super::*;

pub(super) type Coordinate = (String, RowUuid, Vec<u8>);
type Image = Vec<(String, Sql)>;
const MAX_OVERLAY_ROWS: usize = 262_144;

#[derive(Clone)]
pub(super) struct Check {
    schema: Rc<Schema>,
    table: String,
    coordinate: Coordinate,
    author: AuthorSubject,
    clause: Option<PolicyExpr>,
    with_check: bool,
    root: Image,
    denial: String,
}
struct Images {
    before: Option<Image>,
    after: Image,
    evidence: Option<Image>,
}
struct PreparedCheck {
    check: Check,
    sql: String,
    params: Vec<Sql>,
    preps: Vec<crate::compile::Prep>,
    dependencies: BTreeSet<String>,
}
fn overlay_limit() -> Error {
    unsupported(
        "Reading more than 262144 rows of a transaction's own writes in its write-policy checks",
    )
}

impl Inner {
    pub(super) fn defer_ordinary_operation(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        operation: Operation,
        denial: &str,
    ) -> Result<bool, Error> {
        if self.ordinary_checks.borrow().is_none() {
            return Ok(false);
        }
        if matches!(operation, Operation::Update) {
            let policy = &table.policies.update;
            if policy.using.is_none() && policy.with_check.is_none() {
                self.defer_ordinary_check(table, row, key, author, None, false, denial)?;
            }
            if let Some(using) = &policy.using {
                self.defer_ordinary_check(table, row, key, author, Some(using), false, denial)?;
            }
            if let Some(check) = &policy.with_check {
                self.defer_ordinary_check(table, row, key, author, Some(check), true, denial)?;
            }
            return Ok(true);
        }
        self.defer_ordinary_check(
            table,
            row,
            key,
            author,
            table.policy(operation),
            matches!(operation, Operation::Insert),
            denial,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn defer_ordinary_check(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        clause: Option<&PolicyExpr>,
        with_check: bool,
        denial: &str,
    ) -> Result<bool, Error> {
        if self.ordinary_checks.borrow().is_none() {
            return Ok(false);
        }
        let coordinate = (table.stored.clone(), row, key.to_vec());
        let root = self.ordinary_image(&coordinate)?.ok_or_else(|| {
            Error::new(
                ErrorCode::Storage,
                "policy target disappeared inside its unit",
            )
        })?;
        let check = Check {
            schema: self.schema.clone(),
            table: table.name.clone(),
            coordinate,
            author,
            clause: clause.cloned(),
            with_check: with_check && table.branch_by.is_empty(),
            root,
            denial: denial.to_owned(),
        };
        // A predicate with no other-row dependencies cannot gain evidence from
        // later writes. Preserve immediate denial during staged attachment as
        // well as final whole-unit validation.
        let prepared = self.prepare_ordinary_check(check.clone())?;
        if prepared.dependencies.is_empty() && !self.run_ordinary_check(&prepared, self.seq + 1)? {
            return Err(denied(&check.denial));
        }
        self.ordinary_checks
            .borrow_mut()
            .as_mut()
            .expect("collector")
            .push(check);
        Ok(true)
    }

    pub(super) fn ordinary_unit<T>(
        &mut self,
        seq: u64,
        provisional: bool,
        apply: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        // Callback transactions own one collector until their outer commit.
        if self.ordinary_checks.borrow().is_some() {
            return apply(self);
        }
        *self.ordinary_checks.borrow_mut() = Some(Vec::new());
        let mark = self.changed.borrow().len();
        let result = apply(self);
        let checks = self.ordinary_checks.borrow_mut().take().expect("collector");
        let value = result?;
        if !provisional {
            let rows = self.changed.borrow()[mark..].iter().cloned().collect();
            self.validate_ordinary_unit(seq, rows, checks)?;
        }
        Ok(value)
    }

    pub(super) fn begin_ordinary_callback(&self) {
        if self.client.is_none()
            && !self
                .schema
                .policy_schema()
                .jazz
                .may_have_authorized_created_insert_sources()
        {
            *self.ordinary_checks.borrow_mut() = Some(Vec::new());
        }
    }
    pub(super) fn finish_ordinary_callback(
        &self,
        seq: u64,
        tables: &BTreeSet<String>,
        checks: Option<Vec<Check>>,
    ) -> Result<(), Error> {
        let Some(checks) = checks.filter(|checks| !checks.is_empty()) else {
            return Ok(());
        };
        // Notifications may have been drained inside the callback. These exact
        // sequence lookups retain its complete touched set without a table scan.
        let mut rows = BTreeSet::new();
        for table in tables {
            let mut statement = self
                .conn
                .prepare_cached(&format!(
                    "SELECT id,_b FROM {} INDEXED BY {} WHERE _seq=?1",
                    quote(table),
                    quote(&format!("{table}__by_seq")),
                ))
                .map_err(storage)?;
            let found = statement
                .query_map([seq as i64], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(storage)?;
            for found in found {
                let (row, key) = found.map_err(storage)?;
                let row = uuid::Uuid::from_slice(&row)
                    .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
                rows.insert((table.clone(), RowUuid(row), key));
            }
        }
        self.validate_ordinary_unit(seq, rows, checks)
    }

    fn ordinary_image(&self, (table, row, key): &Coordinate) -> Result<Option<Image>, Error> {
        let mut statement = self
            .conn
            .prepare_cached(&format!(
                "SELECT * FROM {} WHERE id=?1 AND _b=?2",
                quote(table)
            ))
            .map_err(storage)?;
        let names = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        statement
            .query_row(rusqlite::params![row.as_bytes().as_slice(), key], |row| {
                names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| Ok((name.clone(), row.get(i)?)))
                    .collect()
            })
            .optional()
            .map_err(storage)
    }
    fn ordinary_before(
        &self,
        coordinate: &Coordinate,
        after: &Image,
    ) -> Result<Option<Image>, Error> {
        let (table, row, key) = coordinate;
        if after.iter().any(|(name, value)| {
            name == "_seq" && matches!(value, Sql::Integer(seq) if *seq <= self.seq as i64)
        }) {
            return Ok(Some(after.clone()));
        }
        let rid = after
            .iter()
            .find(|(name, _)| name == "rid")
            .map(|(_, value)| value.clone())
            .unwrap_or(Sql::Null);
        let columns = after
            .iter()
            .map(|(name, _)| {
                if name == "rid" {
                    "?4 AS rid".into()
                } else {
                    quote(name)
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        self.conn.query_row(&format!(
            "SELECT {columns} FROM {} WHERE id=?1 AND _b=?2 AND _seq<=?3 ORDER BY _seq DESC LIMIT 1", quote(&format!("{table}__h")),
        ), rusqlite::params![row.as_bytes().as_slice(), key, self.seq as i64, rid], |row| {
            after.iter().enumerate().map(|(i, (name, _))| Ok((name.clone(), row.get(i)?))).collect()
        }).optional().map_err(storage)
    }
    fn install_ordinary_image(
        &self,
        (table, row, key): &Coordinate,
        image: Option<&Image>,
    ) -> Result<(), Error> {
        self.conn
            .execute(
                &format!("DELETE FROM {} WHERE id=?1 AND _b=?2", quote(table)),
                rusqlite::params![row.as_bytes().as_slice(), key],
            )
            .map_err(storage)?;
        if let Some(image) = image {
            let columns = image
                .iter()
                .map(|(name, _)| quote(name))
                .collect::<Vec<_>>()
                .join(",");
            let params = (1..=image.len())
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(",");
            self.conn
                .execute(
                    &format!("INSERT INTO {} ({columns}) VALUES ({params})", quote(table)),
                    params_from_iter(image.iter().map(|(_, value)| value)),
                )
                .map_err(storage)?;
        }
        Ok(())
    }
    fn prepare_ordinary_check(&self, check: Check) -> Result<PreparedCheck, Error> {
        let table = check.schema.table(&check.table)?;
        let session = self.session(check.author);
        let binding = BTreeMap::new();
        let mut ctx = Ctx::new(&check.schema, None, &session, &binding).with_catalogue(self);
        let condition = ctx.policy_clause(check.clause.as_ref(), table, "a", &session)?;
        let preps = std::mem::take(&mut ctx.preps);
        let dependencies = std::mem::take(&mut ctx.tables);
        let mut params = ctx.params;
        let columns = check
            .root
            .iter()
            .map(|(name, value)| {
                params.push(value.clone());
                format!("?{} AS {}", params.len(), quote(name))
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(PreparedCheck {
            check,
            sql: format!("SELECT COALESCE({condition},0) FROM (SELECT {columns}) AS a"),
            params,
            preps,
            dependencies,
        })
    }
    fn run_ordinary_check(&self, check: &PreparedCheck, seq: u64) -> Result<bool, Error> {
        let read = || {
            self.conn
                .query_row(&check.sql, params_from_iter(check.params.iter()), |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(storage)
                .map(|value| value != 0)
        };
        if check.preps.is_empty() {
            return read();
        }
        // Chunked comparisons prepare this root and bounded dependencies only.
        let table = check.check.schema.table(&check.check.table)?;
        let mut inputs = vec![(table.stored.clone(), check.check.root.clone())];
        let mut budget = MAX_OVERLAY_ROWS;
        if let Some(clause) = &check.check.clause {
            let schema = check.check.schema.policy_schema();
            let governing = schema.by_stored(&table.stored).ok_or_else(|| {
                unsupported("ordinary unit policy under a missing governing table")
            })?;
            self.candidate_inputs(
                schema,
                governing,
                &check.check.root,
                None,
                &self.session(check.check.author),
                clause,
                crate::compile::PermissionEvidence::Accepted {
                    through: seq,
                    own: seq + 1,
                },
                crate::compile::DEFAULT_INHERITS_DEPTH,
                &mut budget,
                &mut inputs,
            )?;
        }
        self.with_candidate_preps(&check.preps, &inputs, read)
    }

    fn validate_ordinary_unit(
        &self,
        seq: u64,
        coordinates: BTreeSet<Coordinate>,
        checks: Vec<Check>,
    ) -> Result<(), Error> {
        if checks.is_empty() {
            return Ok(());
        }
        let mut rows = BTreeMap::new();
        for coordinate in coordinates {
            let after = self.ordinary_image(&coordinate)?.ok_or_else(|| {
                Error::new(
                    ErrorCode::Storage,
                    "unit row disappeared before authorization",
                )
            })?;
            let before = self.ordinary_before(&coordinate, &after)?;
            let deleted = after
                .iter()
                .any(|(name, value)| name == "_del" && matches!(value, Sql::Integer(v) if *v != 0));
            let evidence = if deleted || !coordinate.2.is_empty() {
                before.clone()
            } else {
                Some(after.clone())
            };
            rows.insert(
                coordinate,
                Images {
                    before,
                    after,
                    evidence,
                },
            );
        }
        let checks = checks
            .into_iter()
            .map(|mut check| {
                if let Some(images) = rows.get(&check.coordinate) {
                    if check.with_check {
                        check.root = images.after.clone();
                    } else if let Some(before) = &images.before {
                        check.root = before.clone();
                    }
                }
                self.prepare_ordinary_check(check)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if checks.iter().all(|check| check.dependencies.is_empty()) {
            for check in &checks {
                if !self.run_ordinary_check(check, seq)? {
                    return Err(denied(&check.check.denial));
                }
            }
            return Ok(());
        }
        self.conn
            .execute_batch("SAVEPOINT ordinary_policy_unit")
            .map_err(storage)?;
        let result = self.ground_ordinary_unit(seq, &rows, &checks);
        self.rollback_savepoint("ordinary_policy_unit")
            .map_err(storage)?;
        result
    }
    fn ground_ordinary_unit(
        &self,
        seq: u64,
        rows: &BTreeMap<Coordinate, Images>,
        checks: &[PreparedCheck],
    ) -> Result<(), Error> {
        let mut table_counts = BTreeMap::<String, usize>::new();
        for (coordinate, images) in rows {
            self.install_ordinary_image(coordinate, images.before.as_ref())?;
            *table_counts.entry(coordinate.0.clone()).or_default() += 1;
        }
        let mut remaining = BTreeSet::new();
        let mut pending = BTreeMap::<Coordinate, usize>::new();
        for (index, check) in checks.iter().enumerate() {
            if check.check.with_check {
                remaining.insert(index);
                *pending.entry(check.check.coordinate.clone()).or_default() += 1;
            } else if !self.run_ordinary_check(check, seq)? {
                return Err(denied(&check.check.denial));
            }
        }
        let unconditional = rows
            .keys()
            .filter(|coordinate| !pending.contains_key(*coordinate))
            .cloned()
            .collect::<Vec<_>>();
        let mut grounded = BTreeSet::new();
        let mut budget = MAX_OVERLAY_ROWS;
        let mut first = true;
        loop {
            let mut ready = if first {
                unconditional.clone()
            } else {
                Vec::new()
            };
            for index in remaining.iter().copied().collect::<Vec<_>>() {
                let check = &checks[index];
                if !first {
                    charge_overlay(&mut budget, check, &table_counts)?;
                }
                let own = rows
                    .get(&check.check.coordinate)
                    .filter(|_| grounded.contains(&check.check.coordinate));
                if let Some(own) = own {
                    self.install_ordinary_image(&check.check.coordinate, own.before.as_ref())?;
                }
                let result = self.run_ordinary_check(check, seq);
                if let Some(own) = own {
                    self.install_ordinary_image(&check.check.coordinate, own.evidence.as_ref())?;
                }
                if result? {
                    remaining.remove(&index);
                    let count = pending.get_mut(&check.check.coordinate).expect("counted");
                    *count -= 1;
                    if *count == 0 {
                        ready.push(check.check.coordinate.clone());
                    }
                }
            }
            // Each round observes one grounded set. No same-round grants leak.
            let advanced = !ready.is_empty();
            for coordinate in ready {
                if grounded.insert(coordinate.clone())
                    && let Some(images) = rows.get(&coordinate)
                {
                    self.install_ordinary_image(&coordinate, images.evidence.as_ref())?;
                }
            }
            if remaining.is_empty() {
                break;
            }
            if !advanced {
                return Err(denied(format!(
                    "{}: no independent grounding in the transaction",
                    checks[*remaining.first().expect("unproved check")]
                        .check
                        .denial
                )));
            }
            first = false;
        }
        // Final revalidation catches an independently permitted update that
        // revoked evidence used by an earlier passing write.
        for check in checks
            .iter()
            .filter(|check| check.check.with_check && !check.dependencies.is_empty())
        {
            charge_overlay(&mut budget, check, &table_counts)?;
            let own = rows.get(&check.check.coordinate);
            if let Some(own) = own {
                self.install_ordinary_image(&check.check.coordinate, own.before.as_ref())?;
            }
            let result = self.run_ordinary_check(check, seq);
            if let Some(own) = own {
                self.install_ordinary_image(&check.check.coordinate, own.evidence.as_ref())?;
            }
            if !result? {
                return Err(denied(format!(
                    "{}: transaction post-state revoked the permission",
                    check.check.denial
                )));
            }
        }
        Ok(())
    }
}
fn charge_overlay(
    budget: &mut usize,
    check: &PreparedCheck,
    table_counts: &BTreeMap<String, usize>,
) -> Result<(), Error> {
    let count = check
        .dependencies
        .iter()
        .try_fold(0usize, |total, table| {
            total.checked_add(table_counts.get(table).copied().unwrap_or(0))
        })
        .ok_or_else(overlay_limit)?;
    *budget = budget.checked_sub(count).ok_or_else(overlay_limit)?;
    Ok(())
}

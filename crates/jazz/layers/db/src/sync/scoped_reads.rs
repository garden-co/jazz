//! Request-local permission closures for a trusted backend's Remote reads.
//! The authority still authorizes the query. Contents use normal row/chunk sync;
//! this receipt restricts local policy re-evaluation to its exact evidence set.
use super::super::candidates::PolicyInput;
use super::*;
use crate::compile::{CandidateEvidence, PermissionEvidence};

pub(super) const MAX_EVIDENCE: usize = 16_384;

/// A coordinate in jazz.sqlite.sync.scoped-evidence-answer.v1. Names are mapped
/// through the peer's schema exactly like ordinary synchronized row images.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScopedReadRowV1 {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub seq: u64,
}

pub(super) enum ScopedEvidence {
    Collect,
    Supplied(Vec<ScopedReadRowV1>),
}

pub(super) fn validate_evidence(through: u64, rows: &[ScopedReadRowV1]) -> Result<(), Error> {
    if through >= LOCAL_BASE || rows.len() > MAX_EVIDENCE {
        return Err(Error::new(
            ErrorCode::Protocol,
            "invalid scoped evidence cut or size",
        ));
    }
    let mut seen = BTreeSet::new();
    for row in rows {
        if row.seq == 0 || row.seq > through || !seen.insert((&row.table, row.id, &row.key)) {
            return Err(Error::new(
                ErrorCode::Protocol,
                "invalid or duplicate scoped evidence coordinate",
            ));
        }
    }
    Ok(())
}

impl Inner {
    fn scoped_input(&self, row: &ScopedReadRowV1) -> Result<PolicyInput, Error> {
        for history in [false, true] {
            let table = if history {
                format!("{}__h", row.table)
            } else {
                row.table.clone()
            };
            let sql = format!(
                "SELECT * FROM {} WHERE id=?1 AND _b=?2 AND +_seq=?3",
                quote(&table)
            );
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
            let names = statement
                .column_names()
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>();
            let found = statement
                .query_row(
                    rusqlite::params![row.id.as_slice(), &row.key, row.seq as i64],
                    |row| {
                        names
                            .iter()
                            .enumerate()
                            .map(|(i, name)| {
                                row.get::<_, Sql>(i).map(|value| (name.clone(), value))
                            })
                            .collect::<rusqlite::Result<Vec<_>>>()
                    },
                )
                .optional()
                .map_err(storage)?;
            if let Some(found) = found {
                return Ok((row.table.clone(), found));
            }
        }
        Err(Error::new(
            ErrorCode::NotObserved,
            "scoped permission evidence version is not resident",
        ))
    }

    pub(super) fn scoped_evidence_image(&self, row: &ScopedReadRowV1) -> Result<Vec<Image>, Error> {
        for history in [false, true] {
            let found = self.images_where(
                &row.table,
                history,
                "id=?1 AND _b=?2 AND +_seq=?3",
                &[&row.id.as_slice(), &row.key, &(row.seq as i64)],
            )?;
            if !found.is_empty() {
                return Ok(found);
            }
        }
        Err(Error::new(
            ErrorCode::NotObserved,
            "scoped permission evidence image is not resident",
        ))
    }

    pub(super) fn permission_evidence_with_budget(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        roots: &[Image],
        through: u64,
        budget: &mut usize,
    ) -> Result<Vec<ScopedReadRowV1>, Error> {
        if !self.authority || self.client.is_some() || through > self.seq || through >= LOCAL_BASE {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "scoped evidence requires the authority",
            ));
        }
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let governing = schema.policy_schema();
        let (_, session) = self.read_sessions(prepared, identity);
        let scope = if through == self.seq {
            PermissionEvidence::Candidate(CandidateEvidence {
                accepted: through,
                publication: through + 1,
                post: false,
            })
        } else {
            PermissionEvidence::Accepted { through, own: 0 }
        };
        let mut inputs = Vec::new();
        for root in roots {
            let coordinate = ScopedReadRowV1 {
                table: root.table.clone(),
                id: root.id,
                key: root.key.clone(),
                seq: root.seq,
            };
            let input = self.scoped_input(&coordinate)?;
            *budget = budget.checked_sub(1 + input.1.len()).ok_or_else(|| {
                unsupported("scoped permission evidence exceeds its bounded work limit")
            })?;
            let table = governing.by_stored(&root.table).ok_or_else(|| {
                unsupported("scoped permission evidence under a missing governing table")
            })?;
            if let Some(policy) = table.policy(Operation::Select) {
                self.candidate_inputs(
                    governing,
                    table,
                    &input.1,
                    None,
                    &session,
                    policy,
                    scope,
                    crate::compile::DEFAULT_INHERITS_DEPTH,
                    budget,
                    &mut inputs,
                )?;
            }
            inputs.push(input);
        }
        let mut unique = BTreeMap::new();
        for (table, fields) in inputs {
            let get = |name| {
                fields
                    .iter()
                    .find(|(column, _)| column == name)
                    .map(|(_, value)| value)
            };
            let (Some(Sql::Blob(id)), Some(Sql::Blob(key)), Some(Sql::Integer(seq))) =
                (get("id"), get("_b"), get("_seq"))
            else {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "invalid scoped permission input",
                ));
            };
            let id: [u8; 16] = id
                .as_slice()
                .try_into()
                .map_err(|_| Error::new(ErrorCode::Storage, "invalid scoped permission row id"))?;
            if !governing
                .by_stored(&table)
                .ok_or_else(|| unsupported("missing scoped permission table"))?
                .branch_by
                .is_empty()
            {
                return Err(unsupported("branched permission evidence in scoped reads"));
            }
            unique.insert(
                (table.clone(), id, key.clone()),
                ScopedReadRowV1 {
                    table,
                    id,
                    key: key.clone(),
                    seq: *seq as u64,
                },
            );
        }
        let rows = unique.into_values().collect::<Vec<_>>();
        validate_evidence(through, &rows)?;
        Ok(rows)
    }

    pub(in crate::db) fn run_qualified_snapshot(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        include_deleted: bool,
        snapshot: &QualifiedReadSnapshot,
    ) -> Result<Vec<CurrentRow>, Error> {
        let exact = if let Some(occurrences) = &snapshot.union {
            unions::exact(prepared, occurrences)?
        } else {
            // Non-UNION paired reads hydrate a prefix and retain its window.
            let mut exact = prepared.clone();
            exact.query.filters.push(crate::query::in_list(
                crate::query::col("id"),
                snapshot
                    .roots
                    .iter()
                    .map(|id| crate::query::lit(Value::Uuid(RowUuid::from_bytes(*id).0))),
            ));
            exact
        };
        let read = ReadSpec {
            cut: Some(Cut::at(snapshot.through)),
            include_deleted,
            ..ReadSpec::default()
        };
        let rows = match &snapshot.evidence {
            Some(evidence) => self.run_scoped_evidence(&exact, identity, &read, evidence)?,
            None => self.run(&exact, identity, &read)?,
        };
        if let Some(occurrences) = &snapshot.union {
            unions::check(&rows, occurrences)?;
            return Ok(union_page(&prepared.query, rows));
        }
        Ok(rows)
    }

    pub(super) fn run_scoped_evidence(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        evidence: &[ScopedReadRowV1],
    ) -> Result<Vec<CurrentRow>, Error> {
        let cut = read.cut.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::Protocol, "scoped evidence needs its exact cut")
        })?;
        validate_evidence(cut.seq, evidence)?;
        let mut inputs = Vec::new();
        for row in evidence {
            // Resolve only admitted tables before interpolating stored identifiers.
            if self.owner.by_stored(&row.table).is_none() {
                return Err(unsupported("unknown scoped permission evidence table"));
            }
            inputs.push(self.scoped_input(row)?);
        }
        self.conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS _jazz_scoped_read_evidence_v1(t TEXT NOT NULL,id BLOB NOT NULL,k BLOB NOT NULL,seq INTEGER NOT NULL,PRIMARY KEY(t,id,k)) WITHOUT ROWID;").map_err(storage)?;
        self.conn
            .execute_batch("SAVEPOINT jazz_scoped_read_evidence")
            .map_err(storage)?;
        let result = (|| {
            self.conn
                .execute("DELETE FROM _jazz_scoped_read_evidence_v1", [])
                .map_err(storage)?;
            let mut insert = self
                .conn
                .prepare_cached(
                    "INSERT INTO _jazz_scoped_read_evidence_v1(t,id,k,seq) VALUES(?1,?2,?3,?4)",
                )
                .map_err(storage)?;
            for row in evidence {
                insert
                    .execute(rusqlite::params![
                        &row.table,
                        row.id.as_slice(),
                        &row.key,
                        row.seq as i64
                    ])
                    .map_err(storage)?;
            }
            drop(insert);
            let read = ReadSpec {
                scoped_evidence: true,
                ..read.clone()
            };
            let compiled = self.compile(prepared, identity, &read)?;
            let mut budget = MAX_EVIDENCE;
            self.with_candidate_sql_budget(&mut budget, || {
                self.with_bounded_preps(&compiled.preps, &inputs, |ties| {
                    let sql = match &compiled.ranked_sql {
                        Some(ranked) if ties => ranked,
                        _ => &compiled.sql,
                    };
                    self.execute_sql(sql, &compiled, &prepared.query.table)
                })
            })
        })();
        self.conn
            .execute_batch(
                "ROLLBACK TO jazz_scoped_read_evidence; RELEASE jazz_scoped_read_evidence",
            )
            .map_err(storage)?;
        result
    }
}

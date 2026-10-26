//! UNION prefix coverage for cold reads and scoped live subscriptions. The
//! authority ships the prefix before the requested outer window. Qualified
//! reads pair its semantic occurrences with permission witnesses at one cut;
//! replicas check that prefix before applying their requested outer window.

use super::*;
use crate::query::{
    RelationColumnRef, RelationExpr, RelationPredicate, RelationQuery, RelationUnionArm,
    RelationValueRef, compose_union_arm_path, relation_output_projection, relation_query_to_query,
    relation_union_parts,
};

/// One occurrence in jazz.sqlite.sync.union-answer.v1. A root may occur once
/// in each semantic leaf arm; publication positions are carried by row images.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnionOccurrenceV1 {
    pub row: [u8; 16],
    pub arm: String,
}

#[derive(Clone, Debug)]
struct LiveCoverage {
    through: u64,
    occurrences: Vec<UnionOccurrenceV1>,
    global: bool,
    evidence: Option<Vec<ScopedReadRowV1>>,
}

#[derive(Clone, Debug)]
struct PendingCoverage {
    through: u64,
    occurrences: Vec<UnionOccurrenceV1>,
    evidence: Option<Vec<ScopedReadRowV1>>,
}

/// Runtime-only coverage shared by attachments to one upstream query. Weak
/// group handles route receipt changes without waking unrelated subscriptions.
#[derive(Default)]
pub(crate) struct UnionLive {
    pending: RefCell<Option<PendingCoverage>>,
    scoped: bool,
    coverage: RefCell<Option<LiveCoverage>>,
    pub(in crate::db) optimistic: StdCell<bool>,
    pub(in crate::db) tables: RefCell<BTreeSet<String>>,
    pub(in crate::db) groups: RefCell<Vec<Weak<RefCell<fanout::Group>>>>,
}

impl std::fmt::Debug for UnionLive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionLive").finish_non_exhaustive()
    }
}

impl UnionLive {
    pub(super) fn new(scoped: bool) -> Self {
        Self {
            scoped,
            ..Self::default()
        }
    }
    pub(super) fn scoped_coverage(
        &self,
    ) -> Option<(u64, Vec<UnionOccurrenceV1>, Vec<ScopedReadRowV1>)> {
        let coverage = self.coverage.borrow();
        let coverage = coverage.as_ref()?;
        if !coverage.global {
            return None;
        }
        Some((
            coverage.through,
            coverage.occurrences.clone(),
            coverage.evidence.clone()?,
        ))
    }
    pub(in crate::db) fn cut(&self) -> Option<u64> {
        self.coverage
            .borrow()
            .as_ref()
            .map(|coverage| coverage.through)
    }
    pub(super) fn retire_pending(&self) {
        self.pending.borrow_mut().take();
    }
}

pub(super) enum LiveUpdate {
    OrdinarySettled {
        id: u64,
        through: u64,
        global: bool,
    },
    Rejected {
        id: u64,
        error: Error,
    },
    Receipt {
        id: u64,
        through: u64,
        occurrences: Vec<UnionOccurrenceV1>,
    },
    Evidence {
        id: u64,
        through: u64,
        rows: Vec<[u8; 16]>,
        evidence: Vec<ScopedReadRowV1>,
    },
    Settled {
        id: u64,
        through: u64,
        global: bool,
    },
}

const WAITING: &str = "UNION occurrence coverage is not materialized yet";

pub(crate) fn waiting(error: &Error) -> bool {
    error.code == ErrorCode::NotObserved && error.message == WAITING
}
fn wait_for_coverage() -> Error {
    Error::new(ErrorCode::NotObserved, WAITING)
}

impl Inner {
    pub(in crate::db) fn union_has_pending_inputs(
        &self,
        tables: &BTreeSet<String>,
    ) -> Result<bool, Error> {
        // Both indexes start with the physical table. Never scan unrelated
        // pending uploads to classify a query's optimistic preview.
        for table in tables {
            if self
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM _jazz_shadow WHERE t=?1) OR \
                     EXISTS(SELECT 1 FROM _jazz_owner_input_rows_v1 \
                     WHERE t=?1 AND pending=1 AND writes=1)",
                    [table],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(storage)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(in crate::db) fn live_union(
        &self,
        prepared: &PreparedQuery,
        deleted: bool,
        scope: &RemoteScope,
    ) -> Option<Rc<UnionLive>> {
        self.client
            .as_ref()?
            .remote
            .get(&scoped_remote_key(
                &remote_query(&prepared.query),
                &prepared.binding,
                deleted,
                scope,
                RemoteReach::REMOTE,
            ))?
            .union
            .clone()
    }

    /// Receipts become usable only after the sync savepoint commits. A plain
    /// settlement or a mismatched cut cannot promote old occurrence coverage.
    pub(super) fn apply_union_coverage(&mut self, updates: Vec<LiveUpdate>) -> Vec<Rc<UnionLive>> {
        let Some(client) = self.client.as_mut() else {
            return Vec::new();
        };
        let mut changed = BTreeMap::new();
        for update in updates {
            let id = match &update {
                LiveUpdate::OrdinarySettled { id, .. }
                | LiveUpdate::Rejected { id, .. }
                | LiveUpdate::Receipt { id, .. }
                | LiveUpdate::Evidence { id, .. }
                | LiveUpdate::Settled { id, .. } => *id,
            };
            if let LiveUpdate::OrdinarySettled {
                through, global, ..
            } = update
            {
                if let Some(remote) = client
                    .remote_ids
                    .get(&id)
                    .and_then(|key| client.remote.get_mut(key))
                {
                    remote.answered.set(true);
                    remote.global.set(global);
                    remote.through = Some(through);
                    remote.resumed = false;
                    // Coverage can settle an empty downstream read without a
                    // changed row. Notify the relay only after this evidence
                    // batch has committed, just like a relayed fetch answer.
                    client.relay_ready = true;
                }
                continue;
            }
            let Some(key) = client.remote_ids.get(&id) else {
                continue;
            };
            let Some(remote) = client.remote.get_mut(key) else {
                continue;
            };
            let Some(live) = &remote.union else {
                continue;
            };
            if remote.refused.borrow().is_some() {
                continue;
            }
            let error = match update {
                LiveUpdate::OrdinarySettled { .. } => unreachable!("handled above"),
                LiveUpdate::Rejected { error, .. } => Some(error),
                LiveUpdate::Receipt {
                    through,
                    occurrences,
                    ..
                } => match occurrence_ids(&remote.query, &occurrences) {
                    Ok(_) if !live.scoped || live.pending.borrow().is_none() => {
                        *live.pending.borrow_mut() = Some(PendingCoverage {
                            through,
                            occurrences,
                            evidence: None,
                        });
                        None
                    }
                    Ok(_) => Some(Error::new(
                        ErrorCode::Protocol,
                        "duplicate scoped live UNION occurrence receipt",
                    )),
                    Err(error) => Some(error),
                },
                LiveUpdate::Evidence {
                    through,
                    rows,
                    evidence,
                    ..
                } => {
                    let mut pending = live.pending.borrow_mut();
                    match pending.as_mut() {
                        Some(pending)
                            if live.scoped
                                && pending.through == through
                                && pending.evidence.is_none()
                                && pending
                                    .occurrences
                                    .iter()
                                    .map(|item| item.row)
                                    .eq(rows.iter().copied()) =>
                        {
                            let covered =
                                evidence.iter().map(|row| row.id).collect::<BTreeSet<_>>();
                            if rows.iter().all(|row| covered.contains(row)) {
                                pending.evidence = Some(evidence);
                                None
                            } else {
                                Some(Error::new(
                                    ErrorCode::Protocol,
                                    "scoped live UNION evidence omits a covered root",
                                ))
                            }
                        }
                        _ => Some(Error::new(
                            ErrorCode::Protocol,
                            "scoped live UNION evidence lacks its matching occurrence receipt",
                        )),
                    }
                }
                LiveUpdate::Settled {
                    through, global, ..
                } => match live.pending.borrow_mut().take() {
                    Some(pending)
                        if pending.through == through
                            && through < LOCAL_BASE
                            && (!live.scoped
                                || (global
                                    && pending.evidence.is_some()
                                    && live
                                        .coverage
                                        .borrow()
                                        .as_ref()
                                        .is_none_or(|prior| through >= prior.through))) =>
                    {
                        *live.coverage.borrow_mut() = Some(LiveCoverage {
                            through,
                            occurrences: pending.occurrences,
                            global,
                            evidence: pending.evidence,
                        });
                        remote.answered.set(true);
                        remote.global.set(global);
                        remote.through = Some(through);
                        remote.resumed = false;
                        client.known = client.known.max(through);
                        changed.insert(Rc::as_ptr(live) as usize, live.clone());
                        client.relay_ready = true;
                        None
                    }
                    _ => Some(Error::new(
                        ErrorCode::NotObserved,
                        "UNION settlement lacks its matching occurrence receipt",
                    )),
                },
            };
            if let Some(error) = error {
                remote.answered.set(false);
                remote.global.set(false);
                live.retire_pending();
                *remote.refused.borrow_mut() = Some(error);
                client.relay_ready = true;
            }
        }
        changed.into_values().collect()
    }

    pub(in crate::db) fn live_union_rows(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        live: &UnionLive,
    ) -> Result<(Vec<CurrentRow>, BTreeSet<String>), Error> {
        let pending = self.union_has_pending_inputs(&live.tables.borrow())?;
        live.optimistic.set(pending);
        let coverage = live.coverage.borrow().clone();
        let mut materialization = read.clone();
        materialization.union_live = None;
        if read.union_preview && (pending || coverage.is_none()) {
            // A preview is the ordinary local query over admitted cached and
            // pending inputs. It does not claim authority occurrence coverage.
            let mut preview = prepared.clone();
            if live.scoped {
                preview.authority_policies = self.needs_authority_policies(&preview, identity)?;
            }
            return self.run_with_deps(&preview, identity, &materialization);
        }
        let coverage = coverage.ok_or_else(wait_for_coverage)?;
        if !coverage.global && !read.union_preview {
            return Err(wait_for_coverage());
        }
        let mut exact = exact(prepared, &coverage.occurrences)?;
        if coverage.global {
            materialization.cut = Some(Cut::at(coverage.through));
            exact.authority_policies = self.needs_authority_policies(&exact, identity)?;
        }
        let (rows, dependencies) = if let Some(evidence) = &coverage.evidence {
            let rows = self.run_scoped_evidence(&exact, identity, &materialization, evidence)?;
            let dependencies = self.compile(&exact, identity, &materialization)?.tables;
            (rows, dependencies)
        } else {
            self.run_with_deps(&exact, identity, &materialization)?
        };
        // Scoped coverage includes a complete permission witness set. Once it
        // evaluates, a different result is invalid evidence, not a request to
        // wait for another receipt. Missing chunks still return their own
        // retryable error above, before this comparison.
        check(&rows, &coverage.occurrences).map_err(|error| {
            if live.scoped {
                error
            } else {
                wait_for_coverage()
            }
        })?;
        Ok((union_page(&prepared.query, rows), dependencies))
    }
}

fn occurrence_ids(
    query: &Query,
    expected: &[UnionOccurrenceV1],
) -> Result<BTreeMap<String, BTreeSet<[u8; 16]>>, Error> {
    let mut ids: BTreeMap<String, BTreeSet<[u8; 16]>> = leaves(query)?
        .into_iter()
        .map(|(arm, _)| (arm, BTreeSet::new()))
        .collect();
    for occurrence in expected {
        if !ids
            .get_mut(&occurrence.arm)
            .ok_or_else(|| Error::new(ErrorCode::Protocol, "unknown UNION receipt arm"))?
            .insert(occurrence.row)
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                "duplicate UNION receipt occurrence",
            ));
        }
    }
    Ok(ids)
}

fn child_path(parent: Option<&str>, child: &str) -> String {
    parent.map_or_else(
        || child.to_owned(),
        |parent| compose_union_arm_path(parent, child),
    )
}

pub(super) fn leaves(query: &Query) -> Result<Vec<(String, Query)>, Error> {
    fn walk(
        expr: &RelationExpr,
        path: Option<&str>,
        out: &mut Vec<(String, Query)>,
    ) -> Result<(), Error> {
        if let Some(parts) = relation_union_parts(expr) {
            for arm in parts.inputs {
                walk(&arm.input, Some(&child_path(path, &arm.label)), out)?;
            }
        } else {
            let relation = RelationQuery { rel: expr.clone() };
            let query = relation_query_to_query(&relation)
                .map_err(|error| query_error(error.to_string()))?;
            out.push((
                path.ok_or_else(|| query_error("expected a retained UNION"))?
                    .to_owned(),
                query,
            ));
        }
        Ok(())
    }
    let mut out = Vec::new();
    let relation = query
        .relation
        .as_ref()
        .ok_or_else(|| query_error("expected a retained UNION"))?;
    walk(&relation.rel, None, &mut out)?;
    Ok(out)
}

pub(super) fn occurrences(rows: &[CurrentRow]) -> Result<Vec<UnionOccurrenceV1>, Error> {
    rows.iter()
        .map(|row| {
            Ok(UnionOccurrenceV1 {
                row: *row.id.as_bytes(),
                arm: row
                    .union_arm()
                    .ok_or_else(|| query_error("missing UNION arm"))?
                    .to_owned(),
            })
        })
        .collect()
}

pub(super) fn check(rows: &[CurrentRow], expected: &[UnionOccurrenceV1]) -> Result<(), Error> {
    if occurrences(rows)? != expected {
        return Err(Error::new(
            ErrorCode::NotObserved,
            "UNION inputs do not reproduce the authority occurrences",
        ));
    }
    Ok(())
}

/// Remove already-applied windows without changing filters, projection or
/// ordering. Joins retain their own inputs: supported facade windows belong
/// to the output chain, never to independent joined inputs.
fn without_window(expr: RelationExpr) -> RelationExpr {
    match expr {
        RelationExpr::Offset { input, .. } | RelationExpr::Limit { input, .. } => {
            without_window(*input)
        }
        RelationExpr::OrderBy { input, terms } => RelationExpr::OrderBy {
            input: Box::new(without_window(*input)),
            terms,
        },
        RelationExpr::Project { input, columns } => RelationExpr::Project {
            input: Box::new(without_window(*input)),
            columns,
        },
        RelationExpr::Filter { input, predicate } => RelationExpr::Filter {
            input: Box::new(without_window(*input)),
            predicate,
        },
        RelationExpr::Distinct { input, key } => RelationExpr::Distinct {
            input: Box::new(without_window(*input)),
            key,
        },
        expr => expr,
    }
}

/// This is a derived read only. Serializable evidence must retain the original
/// query, not this receipt-constrained materialization query.
pub(super) fn exact(
    prepared: &PreparedQuery,
    expected: &[UnionOccurrenceV1],
) -> Result<PreparedQuery, Error> {
    let ids = occurrence_ids(&prepared.query, expected)?;
    fn walk(
        expr: &RelationExpr,
        path: Option<&str>,
        ids: &BTreeMap<String, BTreeSet<[u8; 16]>>,
    ) -> Result<RelationExpr, Error> {
        if let Some(parts) = relation_union_parts(expr) {
            let mut output = RelationExpr::Union {
                inputs: parts
                    .inputs
                    .into_iter()
                    .map(|arm| {
                        Ok(RelationUnionArm {
                            input: walk(&arm.input, Some(&child_path(path, &arm.label)), ids)?,
                            label: arm.label,
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?,
            };
            if let Some(terms) = parts.order_by {
                output = RelationExpr::OrderBy {
                    input: Box::new(output),
                    terms,
                };
            }
            return Ok(output);
        }
        let relation = RelationQuery { rel: expr.clone() };
        let (scope, _) = relation_output_projection(&relation)
            .map_err(|error| query_error(error.to_string()))?;
        let values = ids
            .get(path.ok_or_else(|| query_error("expected a UNION leaf"))?)
            .ok_or_else(|| query_error("missing UNION leaf"))?;
        Ok(RelationExpr::Filter {
            input: Box::new(without_window(expr.clone())),
            predicate: RelationPredicate::In {
                left: RelationColumnRef {
                    scope: Some(scope),
                    column: "id".to_owned(),
                },
                values: values
                    .iter()
                    .map(|row| {
                        RelationValueRef::literal(crate::tools::Value::Uuid(
                            jazz_types::object::ObjectId::from_uuid(RowUuid::from_bytes(*row).0),
                        ))
                    })
                    .collect(),
            },
        })
    }
    let relation = RelationQuery {
        rel: walk(
            &prepared
                .query
                .relation
                .as_ref()
                .expect("validated relation")
                .rel,
            None,
            &ids,
        )?,
    };
    let mut exact = prepared.clone();
    exact.query =
        relation_query_to_query(&relation).map_err(|error| query_error(error.to_string()))?;
    Ok(exact)
}

/// Ordinary receipts list prefix roots rather than arm occurrences. Constrain
/// each leaf to those roots, retaining the requested outer window. Inner
/// offsets use separately hydrated predecessors and require checking the
/// evaluated prefix against the receipt before publishing it.
pub(super) fn prefix_roots(
    prepared: &PreparedQuery,
    roots: &[[u8; 16]],
) -> Result<PreparedQuery, Error> {
    // Inner windows also consume predecessor support, which is deliberately
    // absent from the final result's root list. Evaluate their original shape
    // over that support, then check the complete result against the receipt.
    if !predecessor_queries(&prepared.query)?.is_empty() {
        return Ok(prepared.clone());
    }
    fn restrict(expr: &RelationExpr, roots: &[[u8; 16]]) -> Result<RelationExpr, Error> {
        if let Some(parts) = relation_union_parts(expr) {
            let mut output = RelationExpr::Union {
                inputs: parts
                    .inputs
                    .into_iter()
                    .map(|arm| {
                        Ok(RelationUnionArm {
                            input: restrict(&arm.input, roots)?,
                            label: arm.label,
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?,
            };
            if let Some(terms) = parts.order_by {
                output = RelationExpr::OrderBy {
                    input: Box::new(output),
                    terms,
                };
            }
            if let Some(offset) = parts.offset {
                output = RelationExpr::Offset {
                    input: Box::new(output),
                    offset,
                };
            }
            if let Some(limit) = parts.limit {
                output = RelationExpr::Limit {
                    input: Box::new(output),
                    limit,
                };
            }
            return Ok(output);
        }
        let relation = RelationQuery { rel: expr.clone() };
        let (scope, _) = relation_output_projection(&relation)
            .map_err(|error| query_error(error.to_string()))?;
        Ok(RelationExpr::Filter {
            input: Box::new(expr.clone()),
            predicate: RelationPredicate::In {
                left: RelationColumnRef {
                    scope: Some(scope),
                    column: "id".to_owned(),
                },
                values: roots
                    .iter()
                    .map(|row| {
                        RelationValueRef::literal(crate::tools::Value::Uuid(
                            jazz_types::object::ObjectId::from_uuid(RowUuid::from_bytes(*row).0),
                        ))
                    })
                    .collect(),
            },
        })
    }
    let relation = RelationQuery {
        rel: restrict(
            &prepared.query.relation.as_ref().expect("UNION query").rel,
            roots,
        )?,
    };
    let mut bounded = prepared.clone();
    bounded.query =
        relation_query_to_query(&relation).map_err(|error| query_error(error.to_string()))?;
    Ok(bounded)
}

/// Each inner offset needs the prefix before that window as well as its output.
/// The window must bound this extra work; the outer page alone cannot bound an
/// arbitrarily ordered, unbounded inner result.
fn predecessor_queries(query: &Query) -> Result<Vec<Query>, Error> {
    fn walk(expr: &RelationExpr, outer: bool, out: &mut Vec<Query>) -> Result<(), Error> {
        let relation = RelationQuery { rel: expr.clone() };
        let query =
            relation_query_to_query(&relation).map_err(|error| query_error(error.to_string()))?;
        let window = relation_union_parts(expr)
            .map(|parts| (parts.offset.unwrap_or(0), parts.limit))
            .unwrap_or((query.offset, query.limit));
        if !outer && window.0 > 0 {
            let limit = window.1.ok_or_else(|| {
                unsupported("remote UNION reads with an inner offset without a limit")
            })?;
            let size = window.0.checked_add(limit).ok_or_else(|| {
                unsupported("remote UNION inner prefixes exceeding the evidence budget")
            })?;
            if size > scoped_reads::MAX_EVIDENCE {
                return Err(unsupported(
                    "remote UNION inner prefixes exceeding the evidence budget",
                ));
            }
            out.push(remote_query(&query).into_owned());
        }
        if let Some(parts) = relation_union_parts(expr) {
            for arm in parts.inputs {
                walk(&arm.input, false, out)?;
            }
        }
        Ok(())
    }
    let mut queries = Vec::new();
    if let Some(relation) = &query.relation {
        walk(&relation.rel, true, &mut queries)?;
    }
    Ok(queries)
}

impl Db {
    pub(in crate::db) fn remote_fetch_shape(&self, query: &Query) -> Result<(), Error> {
        if !crate::db::relation::is_union(query) {
            return self.remote_shape(query);
        }
        for (_, leaf) in leaves(query)? {
            let mut leaf = leaf;
            leaf.relation = None;
            self.remote_shape(&leaf)?;
            // Snapshot witness hydration uses the existing bounded indexed
            // existential traversal. Recursive inputs need their own receipt.
            if !leaf.reachable.is_empty() || !leaf.inherits.is_empty() {
                return Err(unsupported("remote UNION arms with recursive inputs"));
            }
        }
        Ok(())
    }
}

impl Inner {
    /// Ship only explicitly bounded inner prefixes, with their normal query
    /// witnesses and read policies. These rows support reconstruction and do
    /// not enlarge the final result's receipt.
    pub(super) fn union_predecessor_inputs(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Vec<Image>, Error> {
        let mut images = Vec::new();
        let mut budget = scoped_reads::MAX_EVIDENCE;
        for query in predecessor_queries(&prepared.query)? {
            let requested = query
                .relation
                .as_ref()
                .and_then(|rel| relation_union_parts(&rel.rel))
                .and_then(|parts| parts.limit)
                .or(query.limit)
                .ok_or_else(|| unsupported("unbounded remote UNION predecessor query"))?;
            budget = budget.checked_sub(requested).ok_or_else(|| {
                unsupported("remote UNION inner prefixes exceeding the evidence budget")
            })?;
            let mut prefix = prepared.clone();
            prefix.query = normalize_relation(&query, &self.schema)?.unwrap_or(query);
            let rows = self.run(&prefix, identity, read)?;
            let table = self.schema.stored_table(&prefix.query.table)?;
            let mut roots = Vec::new();
            for row in &rows {
                let id = row.id.as_bytes().to_vec();
                let seq = row.seq as i64;
                let mut found = self.images_where(table, false, ID_AT_SEQ, &[&id, &seq])?;
                if found.is_empty() {
                    found = self.images_where(table, true, ID_AT_SEQ, &[&id, &seq])?;
                }
                roots.extend(found);
            }
            let through = read.cut.as_ref().map_or(self.seq, |cut| cut.seq);
            let inputs = if crate::db::relation::is_union(&prefix.query) {
                self.union_inputs_with_budget(&prefix, identity, &rows, through, &mut budget)?
            } else {
                self.scoped_query_images(&prefix, identity, &roots, through, &mut budget)?
            };
            images.extend(roots);
            images.extend(inputs);
        }
        Ok(images)
    }

    /// Validate all witness lookups before executing even an empty UNION.
    pub(in crate::db) fn check_union_shape(&self, prepared: &PreparedQuery) -> Result<(), Error> {
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        for (_, query) in leaves(&prepared.query)? {
            let root = schema.table(&query.table)?;
            scoped_queries::tables(&schema, root, &query.joins)?;
        }
        Ok(())
    }

    pub(super) fn union_inputs(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        rows: &[CurrentRow],
        through: u64,
    ) -> Result<Vec<Image>, Error> {
        let mut budget = scoped_reads::MAX_EVIDENCE;
        self.union_inputs_with_budget(prepared, identity, rows, through, &mut budget)
    }

    pub(super) fn union_inputs_with_budget(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        rows: &[CurrentRow],
        through: u64,
        budget: &mut usize,
    ) -> Result<Vec<Image>, Error> {
        let mut inputs = Vec::new();
        for (arm, query) in leaves(&prepared.query)? {
            let mut leaf = prepared.clone();
            leaf.query = normalize_relation(&query, &self.schema)?.unwrap_or(query);
            let mut roots = Vec::new();
            let table = self.schema.stored_table(&leaf.query.table)?;
            for row in rows.iter().filter(|row| row.union_arm() == Some(&arm)) {
                let id = row.id.as_bytes().to_vec();
                let seq = row.seq as i64;
                let mut found = self.images_where(table, false, ID_AT_SEQ, &[&id, &seq])?;
                if found.is_empty() {
                    found = self.images_where(table, true, ID_AT_SEQ, &[&id, &seq])?;
                }
                roots.extend(found);
            }
            inputs.extend(self.scoped_query_images(&leaf, identity, &roots, through, budget)?);
        }
        Ok(inputs)
    }
}

impl Client {
    /// Publish only after the sync savepoint commits. Qualified UNION receipts
    /// may span ticks, but never connections or cancelled request lifetimes.
    pub(super) fn publish_fetch_answers(
        &mut self,
        answers: Vec<(u64, Result<FetchAnswer, Error>)>,
    ) {
        for (id, answer) in answers {
            let Some(slot) = self.answers.get_mut(&id) else {
                continue;
            };
            if !self.union_answers.contains(&id)
                || (!self.scoped_answers.contains(&id) && !self.accepted_answers.contains(&id))
            {
                *slot = Some(answer);
                continue;
            }
            if slot.is_some() {
                // In particular, a later valid pair cannot overwrite an earlier
                // refusal or malformed frame from this same committed batch.
                continue;
            }
            let failure = |message| Error::new(ErrorCode::Protocol, message);
            let answer = match answer {
                Ok((through, _, None, Some(occurrences))) => {
                    if self
                        .qualified_union_receipts
                        .insert(id, (through, occurrences))
                        .is_none()
                    {
                        continue;
                    }
                    Err(failure("duplicate qualified UNION occurrence receipt"))
                }
                Ok((through, rows, evidence, None)) => {
                    match self.qualified_union_receipts.remove(&id) {
                        Some((cut, occurrences))
                            if cut == through
                                && (evidence.is_some() || self.accepted_answers.contains(&id))
                                && occurrences
                                    .iter()
                                    .map(|occurrence| occurrence.row)
                                    .eq(rows.iter().copied()) =>
                        {
                            self.known = self.known.max(through);
                            Ok((through, rows, evidence, Some(occurrences)))
                        }
                        _ => Err(failure(
                            "qualified UNION reply lacks matching occurrence coverage",
                        )),
                    }
                }
                Err(error) => Err(error),
                _ => Err(failure("invalid qualified UNION reply sequence")),
            };
            self.qualified_union_receipts.remove(&id);
            *slot = Some(answer);
        }
    }
}

//! Authority-qualified row evidence and paired E2EE transaction reads.
//!
//! Ordinary origins identify a writer, not authority acceptance. These records
//! are issued only by an authority, or forwarded unchanged from its evidence.
use super::*;
use jazz_types::ids::NodeUuid;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

/// `jazz.sqlite.sync.accepted-row.v1`: an authority-qualified row state and the
/// transaction behind its content. This does not assert query completeness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedRow {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    pub row_seq: u64,
    pub content: AcceptedContent,
}

/// The exact durable `jazz.sqlite.accepted-row.v1` value. The SQL key carries
/// the table, row, branch and state sequence; `observed` captures local admission
/// order so a transaction can exclude evidence received after it began.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedContent {
    pub seq: u64,
    pub node: [u8; 16],
    pub tx: u64,
}

impl AcceptedContent {
    fn valid_at(&self, row_seq: u64) -> bool {
        self.seq > 0
            && self.seq <= row_seq
            && row_seq < LOCAL_BASE
            && self.tx > 0
            && self.tx <= i64::MAX as u64
    }
}

pub(super) const ACCEPTED_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_accepted_rows_v1 (\
        observed INTEGER PRIMARY KEY AUTOINCREMENT, t TEXT NOT NULL, id BLOB NOT NULL,\
        k BLOB NOT NULL, seq INTEGER NOT NULL, content BLOB NOT NULL, UNIQUE (t,id,k,seq));\
    CREATE INDEX IF NOT EXISTS _jazz_accepted_rows_by_seq_v1 \
        ON _jazz_accepted_rows_v1(seq,observed);";

#[derive(Clone, Copy, PartialEq, Eq)]
enum SettledSnapshot {
    Transaction,
    ObservedAtBegin,
    AuthorityAtBegin,
}

impl Inner {
    pub(super) fn accepted_content(
        &self,
        table: &str,
        id: &[u8; 16],
        key: &[u8],
        row_seq: u64,
        observed: u64,
    ) -> Result<Option<AcceptedContent>, Error> {
        let bytes = self.conn.query_row(
            "SELECT content FROM _jazz_accepted_rows_v1 WHERE t=?1 AND id=?2 AND k=?3 AND seq=?4 AND observed<=?5",
            rusqlite::params![table, id.as_slice(), key, row_seq as i64, observed as i64],
            |r| r.get::<_, Vec<u8>>(0),
        ).optional().map_err(storage)?;
        bytes
            .map(|bytes| {
                let (content, rest) = postcard::take_from_bytes::<AcceptedContent>(&bytes)
                    .map_err(|e| {
                        Error::new(ErrorCode::Storage, format!("invalid accepted-row-v1: {e}"))
                    })?;
                if !rest.is_empty() || !content.valid_at(row_seq) {
                    return Err(Error::new(
                        ErrorCode::Storage,
                        "invalid accepted-row-v1 value",
                    ));
                }
                Ok(content)
            })
            .transpose()
    }

    pub(super) fn record_accepted_row(&self, row: &AcceptedRow) -> Result<(), Error> {
        if !row.content.valid_at(row.row_seq) {
            return Err(Error::new(
                ErrorCode::Protocol,
                "invalid accepted-row-v1 sequence",
            ));
        }
        if let Some(previous) =
            self.accepted_content(&row.table, &row.id, &row.key, row.row_seq, i64::MAX as u64)?
        {
            if previous != row.content {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "conflicting accepted-row-v1 evidence",
                ));
            }
            return Ok(());
        }
        let bytes = postcard::to_allocvec(&row.content)
            .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
        self.conn
            .execute(
                "INSERT INTO _jazz_accepted_rows_v1 (t,id,k,seq,content) VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![
                    &row.table,
                    row.id.as_slice(),
                    &row.key,
                    row.row_seq as i64,
                    bytes
                ],
            )
            .map_err(storage)?;
        Ok(())
    }

    /// Complete authority stores can resolve content from their history. A
    /// partial replica can only return the evidence it actually received.
    pub(super) fn accepted_image_content(
        &self,
        image: &Image,
    ) -> Result<Option<AcceptedContent>, Error> {
        if image.seq >= LOCAL_BASE {
            return Ok(None);
        }
        if self.client.is_some() {
            return self.accepted_content(
                &image.table,
                &image.id,
                &image.key,
                image.seq,
                i64::MAX as u64,
            );
        }
        if !self.authority {
            return Ok(None);
        }
        let seq = if image.content {
            image.seq
        } else {
            let preceding = self
                .images_where(
                    &image.table,
                    true,
                    "id=?1 AND _b=?2 AND _seq<?3 AND _c=1 ORDER BY _seq DESC LIMIT 1",
                    &[&image.id.as_slice(), &image.key, &(image.seq as i64)],
                )?
                .pop();
            let Some(preceding) = preceding else {
                return Ok(None);
            };
            preceding.seq
        };
        let origin = self.origins(&BTreeSet::from([seq]))?.pop();
        Ok(origin.map(|(_, node, tx)| AcceptedContent { seq, node, tx }))
    }

    pub(super) fn accepted_images(&self, images: &[Image]) -> Result<Vec<AcceptedRow>, Error> {
        let mut accepted = Vec::new();
        for image in images {
            if let Some(content) = self.accepted_image_content(image)? {
                accepted.push(AcceptedRow {
                    table: image.table.clone(),
                    id: image.id,
                    key: image.key.clone(),
                    row_seq: image.seq,
                    content,
                });
            }
        }
        Ok(accepted)
    }
}

impl Inner {
    /// The local evidence frontier, distinct from the authority's commit order.
    /// Duplicate proof deliveries do not move it; rollback reverts it with rows.
    pub(super) fn accepted_observed(&self) -> Result<u64, Error> {
        self.conn
            .query_row(
                "SELECT COALESCE(MAX(observed),0) FROM _jazz_accepted_rows_v1",
                [],
                |r| r.get::<_, i64>(0),
            )
            .map(|n| n as u64)
            .map_err(storage)
    }

    /// Newly usable evidence: both its proof and its authority coordinate must
    /// have arrived. Separate indexed ranges cover late proofs at a known cut
    /// and earlier proofs whose cut just became known. Neither visits unrelated
    /// retained history; rollback and duplicate delivery cannot advance them.
    pub(super) fn accepted_notifications(
        &mut self,
    ) -> Result<Vec<(String, RowUuid, Vec<u8>)>, Error> {
        let through = self.accepted_observed()?;
        let known = self.client.as_ref().map_or(self.seq, |client| client.known);
        let rows = {
            let mut statement = self
                .conn
                .prepare_cached(
                    "SELECT t,id,k FROM _jazz_accepted_rows_v1 WHERE observed>?1 AND observed<=?2 AND seq<=?4 \
                     UNION ALL SELECT t,id,k FROM _jazz_accepted_rows_v1 INDEXED BY _jazz_accepted_rows_by_seq_v1 \
                     WHERE seq>?3 AND seq<=?4 AND observed<=?1",
                )
                .map_err(storage)?;
            statement
                .query_map(
                    rusqlite::params![
                        self.accepted_notifications_through as i64,
                        through as i64,
                        self.accepted_notifications_known as i64,
                        known as i64
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, [u8; 16]>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .map_err(storage)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(storage)?
        };
        self.accepted_notifications_through = through;
        self.accepted_notifications_known = known;
        Ok(rows
            .into_iter()
            .map(|(table, id, key)| (table, RowUuid::from_bytes(id), key))
            .collect())
    }

    pub(super) fn accepted_cut(&self, seq: u64) -> Result<Cut, Error> {
        Ok(Cut {
            seq,
            accepted_observed: self
                .client
                .as_ref()
                .map(|_| self.accepted_observed())
                .transpose()?,
            ..Cut::default()
        })
    }
}

impl Db {
    /// Each returned occurrence describes one stored root. UNIONs retain their
    /// semantic arm; nested output and effective branches need a witness tree.
    pub(super) fn check_accepted_query(&self, prepared: &PreparedQuery) -> Result<(), Error> {
        let q = &prepared.query;
        if crate::db::relation::is_union(q) {
            self.check_scoped_fetch(prepared)?;
            return self.inner().check_union_shape(prepared);
        }
        if crate::schema::history::is_history_table(&q.table)
            || q.aggregate.is_some()
            || q.flat_join.is_some()
            || !q.includes.is_empty()
            || !q.joins.is_empty()
            || !q.array_subqueries.is_empty()
            || !q.reachable.is_empty()
            || !q.inherits.is_empty()
            || !q.policy_branches.is_empty()
        {
            return Err(unsupported(
                "history, aggregates, joins or relationships in paired accepted reads",
            ));
        }
        let inner = self.inner();
        let schema = prepared.schema.as_ref().unwrap_or(&inner.owner);
        if !schema.table(&q.table)?.branch_by.is_empty() {
            return Err(unsupported("branched tables in paired accepted reads"));
        }
        Ok(())
    }
}

use crate::tx::TxId;
use jazz_types::time::{GlobalTime, TxTime};
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::task::{Poll, Waker};

/// Rows and content settlements from one transaction evaluation. Callers must
/// still await the transaction's Global acceptance before publishing a decision.
#[derive(Debug, PartialEq)]
pub struct BindingSettledRead {
    pub rows: Vec<CurrentRow>,
    pub settlements: Vec<(TxId, GlobalTime)>,
}

#[derive(Default)]
pub(super) struct ReadState {
    next: u64,
    queue: VecDeque<u64>,
    wakers: Vec<Waker>,
    failed: Option<Error>,
}
impl ReadState {
    pub(super) fn check_commit(&self) -> Result<(), Error> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if !self.queue.is_empty() {
            return Err(not_observed("transaction has an unfinished read"));
        }
        Ok(())
    }
}

fn not_observed(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::NotObserved, message)
}

pub(super) struct ReadGuard {
    state: Rc<RefCell<ReadState>>,
    ticket: u64,
    poison: bool,
    finished: bool,
}
impl ReadGuard {
    pub(super) fn require_success(&mut self) {
        self.poison = true;
    }
    pub(super) fn watch(&self, waker: &Waker) {
        let mut state = self.state.borrow_mut();
        if !state.wakers.iter().any(|w| w.will_wake(waker)) {
            state.wakers.push(waker.clone());
        }
    }
    pub(super) async fn wait(&self) -> Result<(), Error> {
        poll_fn(|cx| {
            let mut state = self.state.borrow_mut();
            if let Some(error) = &state.failed {
                return Poll::Ready(Err(error.clone()));
            }
            if state.queue.front() == Some(&self.ticket) {
                return Poll::Ready(Ok(()));
            }
            if !state.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                state.wakers.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }
    pub(super) fn finish<T>(&mut self, result: &Result<T, Error>) {
        self.release(result.as_ref().err().cloned());
    }
    fn release(&mut self, error: Option<Error>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let wakers = {
            let mut state = self.state.borrow_mut();
            if self.poison && state.failed.is_none() {
                state.failed = error;
            }
            state.queue.retain(|ticket| *ticket != self.ticket);
            std::mem::take(&mut state.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}
impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.release(Some(not_observed("paired transaction read was cancelled")));
    }
}

/// Exactly as a served request, a batch's frozen claims apply only while its
/// synchronous commit is evaluated and encoded; ambient claims are restored.
pub(super) struct ClaimsScope {
    inner: Rc<RefCell<Inner>>,
    previous: Option<(AuthorSubject, Option<BTreeMap<String, Value>>)>,
}
impl ClaimsScope {
    pub(super) fn new(db: &Db, scope: Option<&(AuthorSubject, BTreeMap<String, Value>)>) -> Self {
        let previous = scope
            .map(|(author, claims)| (*author, db.inner().claims.insert(*author, claims.clone())));
        Self {
            inner: db.inner.clone(),
            previous,
        }
    }
}
impl Drop for ClaimsScope {
    fn drop(&mut self) {
        if let Some((author, previous)) = self.previous.take() {
            let mut inner = self.inner.borrow_mut();
            if let Some(previous) = previous {
                inner.claims.insert(author, previous);
            } else {
                inner.claims.remove(&author);
            }
        }
    }
}

impl Db {
    pub(super) fn transaction_read_guard(
        &self,
        id: OpenTransactionId,
        poison: bool,
    ) -> Result<ReadGuard, Error> {
        let inner = self.inner();
        let batch = inner
            .batches
            .get(&id)
            .ok_or_else(|| rejected("transaction is not open"))?;
        let state = batch.reads_state.clone();
        let ticket = {
            let mut state = state.borrow_mut();
            if let Some(error) = &state.failed {
                return Err(error.clone());
            }
            let ticket = state.next;
            state.next += 1;
            state.queue.push_back(ticket);
            ticket
        };
        Ok(ReadGuard {
            state,
            ticket,
            poison,
            finished: false,
        })
    }

    pub(super) fn check_read_guard(
        &self,
        id: OpenTransactionId,
        guard: &ReadGuard,
    ) -> Result<(), Error> {
        let inner = self.inner();
        let batch = inner
            .batches
            .get(&id)
            .filter(|batch| Rc::ptr_eq(&batch.reads_state, &guard.state))
            .ok_or_else(|| not_observed("transaction closed while reading"))?;
        if let Some(error) = &guard.state.borrow().failed {
            return Err(error.clone());
        }
        if let (Some(scope), BatchKind::Serializable { reads, .. }) =
            (&batch.paired_scope, &batch.kind)
            && reads.iter().any(|read| {
                read.prepared
                    .request_claims
                    .as_ref()
                    .is_some_and(|prior| prior != scope)
            })
        {
            return Err(rejected("transaction reads use different claim scopes"));
        }
        Ok(())
    }

    /// Register a paired read immediately, even before its future is polled.
    /// Concurrent reads queue at one transaction snapshot. LocalOnly observes
    /// only evidence admitted at begin; Remote requires an authority answer.
    pub fn all_settled(
        &self,
        prepared: PreparedQuery,
        opts: ReadOpts,
        id: OpenTransactionId,
        snapshot_at_begin: bool,
        author: Option<AuthorSubject>,
    ) -> Result<impl Future<Output = Result<BindingSettledRead, Error>> + 'static, Error> {
        self.all_settled_with_snapshot(
            prepared,
            opts,
            id,
            if snapshot_at_begin {
                SettledSnapshot::ObservedAtBegin
            } else {
                SettledSnapshot::Transaction
            },
            author,
        )
    }

    /// Hydrate accepted history at the authority coordinate captured at begin.
    /// Unlike local observation, this may fetch previously unseen evidence, but
    /// never content accepted after that coordinate. Requires remote coverage;
    /// an earlier read at another cut cannot be silently rewound.
    pub fn all_settled_at_authority_begin(
        &self,
        prepared: PreparedQuery,
        opts: ReadOpts,
        id: OpenTransactionId,
        author: Option<AuthorSubject>,
    ) -> Result<impl Future<Output = Result<BindingSettledRead, Error>> + 'static, Error> {
        self.all_settled_with_snapshot(
            prepared,
            opts,
            id,
            SettledSnapshot::AuthorityAtBegin,
            author,
        )
    }

    fn all_settled_with_snapshot(
        &self,
        mut prepared: PreparedQuery,
        opts: ReadOpts,
        id: OpenTransactionId,
        snapshot: SettledSnapshot,
        author: Option<AuthorSubject>,
    ) -> Result<impl Future<Output = Result<BindingSettledRead, Error>> + 'static, Error> {
        let mut guard = self.transaction_read_guard(id, true)?;
        let admission = (|| {
            self.check_accepted_query(&prepared)?;
            if snapshot == SettledSnapshot::AuthorityAtBegin && opts.tier != ReadTier::Remote {
                return Err(unsupported(
                    "authority-at-begin history without remote coverage",
                ));
            }
            if opts.read_view.source != Default::default() {
                return Err(unsupported(
                    "branch or snapshot read views in paired accepted reads",
                ));
            }
            let mut inner = self.inner();
            if inner.client.is_none() && !inner.authority {
                return Err(not_observed(
                    "paired reads require authority acceptance evidence",
                ));
            }
            let batch = inner.batches.get(&id).expect("guarded");
            let BatchKind::Serializable { reads, .. } = &batch.kind else {
                return Err(unsupported(
                    "paired accepted reads in mergeable transactions",
                ));
            };
            if author.is_some_and(|author| {
                (author != AuthorSubject::SYSTEM).then_some(author) != batch.reader()
            }) {
                return Err(rejected(
                    "paired read identity differs from its transaction",
                ));
            }
            let claim_author = batch.reader().unwrap_or(inner.identity.author);
            let claims = match &prepared.request_claims {
                Some((requested, claims)) if *requested == claim_author => claims.clone(),
                Some(_) => {
                    return Err(rejected(
                        "paired read claims differ from its transaction identity",
                    ));
                }
                None => batch
                    .paired_scope
                    .as_ref()
                    .map(|(_, claims)| claims.clone())
                    .unwrap_or_else(|| {
                        inner.claims.get(&claim_author).cloned().unwrap_or_default()
                    }),
            };
            let scope = (claim_author, claims);
            if batch
                .paired_scope
                .as_ref()
                .is_some_and(|prior| *prior != scope)
                || reads.iter().any(|read| {
                    read.prepared
                        .request_claims
                        .as_ref()
                        .is_some_and(|prior| *prior != scope)
                })
            {
                return Err(rejected(
                    "paired reads require one immutable transaction claim scope",
                ));
            }
            prepared = prepared
                .clone()
                .with_identity_claims(scope.0, scope.1.clone());
            inner.batches.get_mut(&id).expect("guarded").paired_scope = Some(scope);
            Ok(())
        })();
        if let Err(error) = admission {
            guard.finish::<()>(&Err(error.clone()));
            return Err(error);
        }
        let db = Db {
            inner: self.inner.clone(),
            view: self.view.clone(),
        };
        Ok(async move {
            let result = async {
                guard.wait().await?;
                db.check_read_guard(id, &guard)?;
                db.settled_read(&prepared, opts, id, snapshot, &guard).await
            }
            .await;
            guard.finish(&result);
            result
        })
    }

    async fn settled_read(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        id: OpenTransactionId,
        snapshot_mode: SettledSnapshot,
        guard: &ReadGuard,
    ) -> Result<BindingSettledRead, Error> {
        let include_deleted = opts.read_view.include_deleted;
        let local = opts.tier == ReadTier::LocalOnly;
        let deadline = web_time::Instant::now() + std::time::Duration::from_secs(15);
        let _wake = self.read_deadline((!local).then_some(deadline));
        let (snapshot, pinned, begin, reader) = {
            let inner = self.inner();
            let batch = inner.batches.get(&id).expect("guarded");
            let BatchKind::Serializable {
                snapshot,
                pinned,
                accepted_begin,
                ..
            } = &batch.kind
            else {
                unreachable!()
            };
            (*snapshot, *pinned, accepted_begin.clone(), batch.reader())
        };
        let authority_begin = snapshot_mode == SettledSnapshot::AuthorityAtBegin;
        if authority_begin && pinned && snapshot != begin.seq {
            return Err(not_observed(
                "transaction already read beyond its begin coordinate",
            ));
        }
        let mut expected = None;
        let mut qualified = None;
        let mut bound_prepared = prepared.clone();
        if !local && self.is_client() {
            let scope = reader.map(|author| {
                (
                    author,
                    prepared.request_claims.as_ref().expect("bound").1.clone(),
                )
            });
            let mut fetch = std::pin::pin!(self.fetch_answer(
                prepared,
                if authority_begin {
                    Some(begin.seq)
                } else {
                    pinned.then_some(snapshot)
                },
                include_deleted,
                Some(deadline),
                scope,
                true,
            ));
            let answer = poll_fn(|cx| {
                guard.watch(cx.waker());
                if let Err(error) = self.check_read_guard(id, guard) {
                    return Poll::Ready(Err(error));
                }
                fetch.as_mut().poll(cx)
            })
            .await?
            .ok_or_else(|| not_observed("authority coverage unavailable for paired read"))?;
            self.check_read_guard(id, guard)?;
            let (through, rows, evidence, union) = answer;
            if authority_begin && through != begin.seq {
                return Err(not_observed(
                    "authority answered beyond the begin coordinate",
                ));
            }
            bound_prepared.authority_policies =
                self.inner().needs_authority_policies(prepared, reader)?;
            if evidence.is_some() || union.is_some() {
                bound_prepared.authority_policies |= evidence.is_some();
                qualified = Some(QualifiedReadSnapshot {
                    through,
                    roots: rows.clone(),
                    evidence,
                    union,
                });
            }
            let mut inner = self.inner();
            let batch = inner.batches.get_mut(&id).expect("guarded");
            if let BatchKind::Serializable {
                snapshot, pinned, ..
            } = &mut batch.kind
            {
                if !*pinned {
                    *snapshot = through;
                    *pinned = true;
                } else if *snapshot != through {
                    return Err(not_observed(
                        "authority answered a different transaction cut",
                    ));
                }
            }
            // Both ordinary and UNION answers hydrate a prefix. Compare the
            // requested page, while retaining the full query for commit-time
            // read and phantom validation.
            expected = Some(if crate::db::relation::is_union(&prepared.query) {
                union_page(&prepared.query, rows)
            } else {
                rows.into_iter().skip(prepared.query.offset).collect()
            });
        }
        let prepared = &bound_prepared;
        let cut = {
            let mut inner = self.inner();
            let batch = inner.batches.get_mut(&id).expect("guarded");
            let BatchKind::Serializable {
                snapshot,
                pinned,
                reads,
                ..
            } = &mut batch.kind
            else {
                unreachable!()
            };
            if local {
                if *pinned && *snapshot != begin.seq && !reads.is_empty() {
                    return Err(not_observed(
                        "local observation differs from the transaction snapshot",
                    ));
                }
                *snapshot = begin.seq;
                *pinned = true;
                begin.clone()
            } else if authority_begin {
                // Pending own writes and late acceptance cannot extend the
                // recipient-eligibility cut beyond the captured global coordinate.
                Cut::at(begin.seq)
            } else {
                // Paired reads return only the authority's pinned state. Own
                // writes pending at begin may settle after that cut; including
                // them would disagree with the authority answer or substitute
                // later content for a row whose id still matches. Ordinary
                // transaction reads retain their read-your-writes overlay.
                Cut::at(*snapshot)
            }
        };
        let overlay_checked = if qualified.is_some() {
            let needs_check = {
                let inner = self.inner();
                let dependencies = inner
                    .compile(
                        prepared,
                        reader,
                        &ReadSpec {
                            cut: Some(cut.clone()),
                            include_deleted,
                            ..ReadSpec::default()
                        },
                    )?
                    .tables;
                inner
                    .batches
                    .get(&id)
                    .expect("guarded")
                    .ops
                    .iter()
                    .any(|(schema, op)| {
                        schema
                            .stored_table(op.table())
                            .is_ok_and(|table| dependencies.contains(table))
                    })
            };
            if needs_check {
                let (count, ops) = self.accepted_overlay_ops(id)?;
                let mut check = std::pin::pin!(self.check_accepted_overlay(
                    prepared,
                    cut.seq,
                    include_deleted,
                    deadline,
                    ops
                ));
                poll_fn(|cx| {
                    guard.watch(cx.waker());
                    if let Err(error) = self.check_read_guard(id, guard) {
                        return Poll::Ready(Err(error));
                    }
                    check.as_mut().poll(cx)
                })
                .await?;
                self.check_read_guard(id, guard)?;
                Some(count)
            } else {
                None
            }
        } else {
            None
        };
        poll_fn(|cx| {
            guard.watch(cx.waker());
            let result = (|| {
                self.check_read_guard(id, guard)?;
                let rows = if let Some(qualified) = &qualified {
                    let inner = self.inner();
                    let dependencies = inner
                        .compile(
                            prepared,
                            reader,
                            &ReadSpec {
                                cut: Some(cut.clone()),
                                include_deleted,
                                ..ReadSpec::default()
                            },
                        )?
                        .tables;
                    let batch = inner.batches.get(&id).expect("guarded");
                    let touches = batch.ops.iter().any(|(schema, op)| {
                        schema
                            .stored_table(op.table())
                            .is_ok_and(|table| dependencies.contains(table))
                    });
                    if touches && overlay_checked != Some(batch.ops.len()) {
                        return Err(not_observed(
                            "staged writes changed while awaiting authority overlay evidence",
                        ));
                    }
                    inner.run_qualified_snapshot(prepared, reader, include_deleted, qualified)?
                } else {
                    // Existing local/ordinary paired reads compare their full
                    // staged projection with the accepted snapshot.
                    let actual = self.in_overlay(id, None, |inner, overlay| {
                        let mut overlaid = cut.clone();
                        overlaid.own = overlay.map_or(0, |cut| cut.own);
                        inner.run(
                            prepared,
                            reader,
                            &ReadSpec {
                                cut: Some(overlaid),
                                include_deleted,
                                ..ReadSpec::default()
                            },
                        )
                    })?;
                    let rows = self.inner().run(
                        prepared,
                        reader,
                        &ReadSpec {
                            cut: Some(cut.clone()),
                            include_deleted,
                            ..ReadSpec::default()
                        },
                    )?;
                    if rows != actual {
                        return Err(not_observed(
                            "paired read contains staged or provisional content",
                        ));
                    }
                    rows
                };
                let mut inner = self.inner();
                if let Some(expected) = &expected {
                    let actual: Vec<_> = rows.iter().map(|row| *row.id.0.as_bytes()).collect();
                    if actual.as_slice() != expected.as_slice() {
                        return Err(not_observed(
                            "replica lacks the complete accepted query result",
                        ));
                    }
                }
                let settlements = inner.settlements(prepared, &rows, &cut)?;
                if !local && snapshot_mode == SettledSnapshot::ObservedAtBegin {
                    let observed = inner.run(
                        prepared,
                        reader,
                        &ReadSpec {
                            cut: Some(begin.clone()),
                            include_deleted,
                            ..ReadSpec::default()
                        },
                    )?;
                    let earlier = inner.settlements(prepared, &observed, &begin)?;
                    if rows != observed || settlements != earlier {
                        return Err(not_observed(
                            "content was not accepted at the original transaction snapshot",
                        ));
                    }
                }
                let batch = inner.batches.get_mut(&id).expect("guarded");
                let BatchKind::Serializable { reads, .. } = &mut batch.kind else {
                    unreachable!()
                };
                reads.push(ReadRecord {
                    prepared: prepared.clone(),
                    qualified: qualified.clone(),
                    include_deleted,
                    rows: versions(&rows),
                    content: read_contents(&rows)?,
                });
                Ok(BindingSettledRead { rows, settlements })
            })();
            match result {
                Err(error) if !local && crate::large::is_missing(&error) => {
                    if web_time::Instant::now() >= deadline
                        || self
                            .link_hint()
                            .is_none_or(|link| !sync::may_wait(link.get()))
                    {
                        return Poll::Ready(Err(not_observed(
                            "accepted content hydration unavailable",
                        )));
                    }
                    self.schedule_pending_tick();
                    Poll::Pending
                }
                result => Poll::Ready(result),
            }
        })
        .await
    }
}

impl Inner {
    fn settlements(
        &self,
        prepared: &PreparedQuery,
        rows: &[CurrentRow],
        cut: &Cut,
    ) -> Result<Vec<(TxId, GlobalTime)>, Error> {
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
        let table = schema.stored_table(&prepared.query.table)?;
        rows.iter()
            .map(|row| {
                if row.seq >= LOCAL_BASE {
                    return Err(not_observed("content is not authority accepted"));
                }
                let id = row.id.0.as_bytes();
                let content = if self.client.is_some() {
                    // Unbranched root tables have the empty key; the query admission
                    // above refuses effective branch states until they have witnesses.
                    self.accepted_content(
                        table,
                        id,
                        &[],
                        row.seq,
                        cut.accepted_observed.unwrap_or(i64::MAX as u64),
                    )?
                } else {
                    let mut image = self.images_where(
                        table,
                        false,
                        "id=?1 AND _seq=?2",
                        &[&id.as_slice(), &(row.seq as i64)],
                    )?;
                    if image.is_empty() {
                        image = self.images_where(
                            table,
                            true,
                            "id=?1 AND _seq=?2",
                            &[&id.as_slice(), &(row.seq as i64)],
                        )?;
                    }
                    match image.pop() {
                        Some(image) => self.accepted_image_content(&image)?,
                        None => None,
                    }
                }
                .ok_or_else(|| {
                    not_observed("authority settlement unavailable for content witness")
                })?;
                Ok((
                    TxId::new(TxTime(content.tx), NodeUuid::from_bytes(content.node)),
                    GlobalTime(content.seq),
                ))
            })
            .collect()
    }
}

pub(super) fn close_pending_reads(state: &Rc<RefCell<ReadState>>) {
    let wakers = {
        let mut state = state.borrow_mut();
        if !state.queue.is_empty() && state.failed.is_none() {
            state.failed = Some(not_observed("transaction closed with an unfinished read"));
        }
        std::mem::take(&mut state.wakers)
    };
    for waker in wakers {
        waker.wake();
    }
}

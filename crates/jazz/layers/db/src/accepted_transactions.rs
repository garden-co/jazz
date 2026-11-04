//! Accepted evidence is a capability of the existing serializable batch.
//! Its queue, cut, handle lifetime and read set remain the ordinary ones.
use super::*;
use std::future::{Future, poll_fn};
use std::task::Poll;
use web_time::Instant;

#[derive(Clone, PartialEq)]
pub(super) struct AcceptedScope {
    schema: [u8; 16],
    scope: AcceptedReadScope,
}

impl Inner {
    pub(super) fn accepted_scope_of(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
    ) -> AcceptedReadScope {
        AcceptedReadScope {
            author,
            claims: prepared
                .request_claims
                .as_ref()
                .filter(|(requested, _)| *requested == author)
                .map(|(_, claims)| claims.clone())
                .unwrap_or_else(|| self.claims.get(&author).cloned().unwrap_or_default()),
        }
    }
}

fn not_observed(message: &str) -> Error {
    Error::new(ErrorCode::NotObserved, message)
}

impl Db {
    /// Admit an authority accepted read into an explicitly identity-bound
    /// serializable transaction. The timeout starts now, including queue time.
    /// Dropping this future or failing its read invalidates this batch's
    /// accepted capability; ordinary transactions retain their own contract.
    /// This operation never falls back to an owner-local observation.
    pub fn accepted_transaction_query(
        &self,
        id: OpenTransactionId,
        mut prepared: PreparedQuery,
        include_deleted: bool,
        timeout_ms: u64,
    ) -> Result<impl Future<Output = Result<EncodedAcceptedRead, Error>> + 'static, Error> {
        // An unsupported call on an ordinary batch admits no accepted read.
        let author = {
            let inner = self.inner();
            let batch = inner
                .batches
                .get(&id)
                .ok_or_else(|| rejected("transaction is not open"))?;
            if !matches!(batch.kind, BatchKind::Serializable { .. }) {
                return Err(unsupported("accepted evidence in a mergeable transaction"));
            }
            match batch.identity {
                Some(WriteIdentity::Session(author)) => author,
                _ => {
                    return Err(unsupported(
                        "accepted evidence without an explicitly bound session identity",
                    ));
                }
            }
        };
        let until = Instant::now()
            .checked_add(std::time::Duration::from_millis(timeout_ms))
            .ok_or_else(|| unsupported("accepted-read timeout outside the clock range"))?;
        let mut guard = self.transaction_read_guard(id)?;
        guard.require_evidence();
        let admission = (|| {
            let mut inner = self.inner();
            let schema = prepared
                .schema
                .get_or_insert_with(|| inner.schema.clone())
                .clone();
            let version = *schema.jazz.version_id().as_bytes();
            accepted_read::check_accepted_shape(&prepared, &schema)?;
            if prepared
                .request_claims
                .as_ref()
                .is_some_and(|(requested, _)| *requested != author)
            {
                return Err(rejected(
                    "accepted read identity differs from its bound transaction",
                ));
            }
            let context = AcceptedScope {
                schema: version,
                scope: inner.accepted_scope_of(&prepared, author),
            };
            let batch = inner.batches.get_mut(&id).expect("checked open batch");
            if !batch.ops.is_empty() {
                return Err(not_observed(
                    "accepted reads cannot substitute already staged content",
                ));
            }
            if batch
                .accepted
                .as_ref()
                .is_some_and(|prior| prior != &context)
            {
                return Err(rejected(
                    "accepted reads require one frozen schema, identity and claims scope",
                ));
            }
            batch.accepted.get_or_insert(context.clone());
            Ok(context)
        })();
        if let Err(error) = &admission {
            guard.finish(&admission);
            return Err(error.clone());
        }
        let context = admission.expect("checked admission");
        let deadline = self.read_deadline(Some(until)).expect("bounded wait");
        let db = Db {
            inner: self.inner.clone(),
            view: self.view.clone(),
            governance: self.governance.clone(),
        };
        Ok(async move {
            let result = async {
                let waiting = guard.wait();
                let mut waiting = std::pin::pin!(waiting);
                poll_fn(|cx| {
                    deadline.watch(cx.waker());
                    guard.watch(cx.waker());
                    db.check_read_guard(id, &guard)?;
                    if Instant::now() >= until {
                        return Poll::Ready(Err(not_observed(
                            "accepted transaction read expired in its queue",
                        )));
                    }
                    waiting.as_mut().poll(cx)
                })
                .await?;
                let at = db.accepted_transaction_cut(id, &guard)?;
                let remaining = until
                    .saturating_duration_since(Instant::now())
                    .as_nanos()
                    .div_ceil(1_000_000)
                    .min(u128::from(u64::MAX)) as u64;
                let read = db.accepted_query_with_timeout(
                    &prepared,
                    at,
                    include_deleted,
                    Some(context.scope.clone()),
                    remaining,
                );
                let mut read = std::pin::pin!(read);
                let reply = poll_fn(|cx| {
                    deadline.watch(cx.waker());
                    guard.watch(cx.waker());
                    db.check_read_guard(id, &guard)?;
                    if Instant::now() >= until {
                        return Poll::Ready(Err(not_observed("accepted transaction read expired")));
                    }
                    read.as_mut().poll(cx)
                })
                .await?;
                db.accepted_transaction_cut(id, &guard)?;
                if reply.schema != context.schema
                    || reply.scope != context.scope
                    || at.is_some_and(|at| at != reply.through)
                {
                    return Err(not_observed(
                        "authority evidence differs from the admitted transaction scope or cut",
                    ));
                }
                let mut inner = db.inner();
                let batch = inner.batches.get_mut(&id).expect("checked read guard");
                let BatchKind::Serializable {
                    snapshot,
                    pinned,
                    own,
                    reads,
                } = &mut batch.kind
                else {
                    unreachable!()
                };
                *snapshot = Pos::at(reply.through);
                *pinned = true;
                own.clear();
                reads.push(ReadRecord {
                    prepared: prepared
                        .clone()
                        .with_identity_claims(context.scope.author, context.scope.claims.clone()),
                    include_deleted,
                    rows: reply
                        .evidence
                        .iter()
                        .map(|evidence| (evidence.row, (Pos::at(evidence.state.sequence), None)))
                        .collect(),
                    scope: context.scope.clone(),
                    schema: context.schema,
                    accepted_evidence: true,
                });
                Ok(reply)
            }
            .await;
            guard.finish(&result);
            result
        })
    }

    fn accepted_transaction_cut(
        &self,
        id: OpenTransactionId,
        guard: &transaction_reads::ReadGuard,
    ) -> Result<Option<u64>, Error> {
        self.check_read_guard(id, guard)?;
        let inner = self.inner();
        let batch = inner.batches.get(&id).expect("checked read guard");
        Self::check_accepted_batch_inner(&inner, batch)?;
        let BatchKind::Serializable {
            snapshot,
            pinned,
            own,
            reads,
        } = &batch.kind
        else {
            unreachable!()
        };
        if !own.is_empty() && reads.iter().any(|read| !read.accepted_evidence) {
            return Err(not_observed(
                "accepted evidence cannot include ordinary reads of unsettled own commits",
            ));
        }
        Ok(pinned.then_some(snapshot.seq))
    }

    pub(super) fn check_accepted_batch(&self, batch: &Batch) -> Result<(), Error> {
        Self::check_accepted_batch_inner(&self.inner(), batch)
    }

    fn check_accepted_batch_inner(inner: &Inner, batch: &Batch) -> Result<(), Error> {
        let Some(context) = &batch.accepted else {
            return Ok(());
        };
        // A frozen claim scope identifies what was read. It cannot preserve a
        // revoked credential for commit-time policy checks.
        if inner
            .claims
            .get(&context.scope.author)
            .cloned()
            .unwrap_or_default()
            != context.scope.claims
        {
            return Err(rejected(
                "accepted transaction claims changed before validation",
            ));
        }
        if batch
            .ops
            .iter()
            .any(|(schema, _)| *schema.jazz.version_id().as_bytes() != context.schema)
        {
            return Err(rejected(
                "accepted transaction writes require the admitted schema",
            ));
        }
        if let BatchKind::Serializable { reads, .. } = &batch.kind
            && reads
                .iter()
                .any(|read| read.schema != context.schema || read.scope != context.scope)
        {
            return Err(rejected(
                "ordinary and accepted predicates require the same schema and claims scope",
            ));
        }
        Ok(())
    }

    pub(super) fn check_mixed_accepted_read(
        &self,
        id: OpenTransactionId,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<(), Error> {
        let inner = self.inner.staging();
        let batch = inner
            .batches
            .get(&id)
            .ok_or_else(|| rejected("transaction is not open"))?;
        let Some(context) = &batch.accepted else {
            return Ok(());
        };
        let schema = *prepared
            .schema
            .as_ref()
            .unwrap_or(&inner.owner)
            .jazz
            .version_id()
            .as_bytes();
        let scope = inner.accepted_scope_of(prepared, identity.unwrap_or(inner.identity.author));
        if schema != context.schema || scope != context.scope {
            let error = rejected(
                "ordinary read differs from the accepted transaction schema or claims scope",
            );
            let state = batch.reads_state.clone();
            drop(inner);
            transaction_reads::fail_pending_reads(&state, error.clone());
            return Err(error);
        }
        Ok(())
    }
}

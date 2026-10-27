//! Authority checks for staged projections of paired scoped reads. The result
//! is a rollback-only observation, never a write or a Global commit receipt.
use super::*;

const MAX_OPS: usize = 1024;
const MAX_BYTES: usize = 4 * 1024 * 1024;

fn cells(op: &mut WireOp) -> Option<&mut BTreeMap<String, Value>> {
    match op {
        WireOp::Insert { cells, .. }
        | WireOp::Update { cells, .. }
        | WireOp::Upsert { cells, .. } => Some(cells),
        WireOp::Restore { cells, .. } => cells.as_mut(),
        _ => None,
    }
}

pub(super) fn validate_ops(ops: &[WireOp]) -> Result<(), Error> {
    if ops.len() > MAX_OPS
        || postcard::to_allocvec(ops)
            .map_err(|e| query_error(e.to_string()))?
            .len()
            > MAX_BYTES
    {
        return Err(unsupported(
            "accepted overlay checks beyond 1024 writes or 4 MiB of staged values",
        ));
    }
    fn logical(value: &Value) -> bool {
        match value {
            Value::Large(_) => false,
            Value::Nullable(Some(value)) => logical(value),
            Value::Array(values) | Value::Tuple(values) => values.iter().all(logical),
            _ => true,
        }
    }
    for op in ops {
        let values = match op {
            WireOp::Insert { cells, .. }
            | WireOp::Update { cells, .. }
            | WireOp::Upsert { cells, .. } => Some(cells),
            WireOp::Restore { cells, .. } => cells.as_ref(),
            _ => None,
        };
        if values.is_some_and(|values| values.values().any(|value| !logical(value))) {
            return Err(unsupported(
                "indirect value descriptors in accepted overlay checks; send bounded logical values",
            ));
        }
        let target = match op {
            WireOp::Insert { target, .. }
            | WireOp::Update { target, .. }
            | WireOp::Upsert { target, .. }
            | WireOp::Delete { target, .. }
            | WireOp::Restore { target, .. } => target,
            WireOp::Move { .. } => {
                return Err(unsupported("branch moves in accepted overlay checks"));
            }
        };
        if !matches!(target, WireTarget::Root) {
            return Err(unsupported("branch targets in accepted overlay checks"));
        }
    }
    Ok(())
}

// Materialize only values explicitly staged by this batch. Its private overlay
// installs derived nodes for the duration; no unrelated cache is traversed.
fn materialize(inner: &Inner, value: &mut Value, budget: &mut usize) -> Result<(), Error> {
    match value {
        Value::Large(reference) => {
            let size = usize::try_from(reference.byte_length)
                .map_err(|_| unsupported("oversized accepted overlay value"))?;
            *budget = budget
                .checked_sub(size)
                .ok_or_else(|| unsupported("accepted overlay staged values exceed 4 MiB"))?;
            let bytes = inner.chunks.value_bytes(reference)?;
            *value = match reference.kind {
                jazz_types::large_values::LargeValueKind::Bytes => Value::Bytes(bytes),
                _ => Value::String(
                    String::from_utf8(bytes).map_err(|_| query_error("invalid staged text"))?,
                ),
            };
        }
        Value::Nullable(Some(value)) => materialize(inner, value, budget)?,
        Value::Array(values) | Value::Tuple(values) => {
            for value in values {
                materialize(inner, value, budget)?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl Db {
    pub(in crate::db) fn accepted_overlay_ops(
        &self,
        id: OpenTransactionId,
    ) -> Result<(usize, Vec<WireOp>), Error> {
        let (count, mut ops) = {
            let inner = self.inner();
            let batch = inner
                .batches
                .get(&id)
                .ok_or_else(|| rejected("accepted overlay transaction closed"))?;
            if batch.ops.len() > MAX_OPS {
                return Err(unsupported("accepted overlay checks beyond 1024 writes"));
            }
            let ops = batch
                .ops
                .iter()
                .map(|(schema, op)| {
                    if schema.jazz.version_id() != inner.schema.jazz.version_id() {
                        return Err(unsupported(
                            "accepted overlay writes across different schema versions",
                        ));
                    }
                    to_wire(op)
                })
                .collect::<Result<Vec<_>, _>>()?;
            (batch.ops.len(), ops)
        };
        self.in_overlay(id, None, |inner, _| {
            let mut budget = MAX_BYTES;
            for op in &mut ops {
                if let Some(cells) = cells(op) {
                    for value in cells.values_mut() {
                        materialize(inner, value, &mut budget)?;
                    }
                }
            }
            Ok(())
        })?;
        validate_ops(&ops)?;
        Ok((count, ops))
    }

    pub(super) fn validate_accepted_overlay(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        through: u64,
        include_deleted: bool,
        wire: &[WireOp],
    ) -> Result<(), Error> {
        let mut inner = self.inner();
        if inner.client.is_some()
            || !inner.authority
            || through > inner.seq
            || through >= LOCAL_BASE
        {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "accepted overlay check requires the authority at the requested cut",
            ));
        }
        validate_ops(wire)?;
        let ops = wire
            .iter()
            .cloned()
            .map(from_wire)
            .collect::<Result<Vec<_>, _>>()?;
        for op in &ops {
            let table = inner.schema.table(op.table())?;
            if !table.branch_by.is_empty() {
                return Err(unsupported("branched tables in accepted overlay checks"));
            }
            if let Some(row) = op.row()
                && inner
                    .newest_seq(&table.stored, row)?
                    .is_some_and(|seq| seq > through)
            {
                return Err(conflict(
                    "accepted overlay write target changed after the pinned cut",
                ));
            }
        }
        let mut budget = scoped_reads::MAX_EVIDENCE;
        let read = ReadSpec {
            cut: Some(Cut::at(through)),
            permission_cut: Some(through),
            include_deleted,
            ..ReadSpec::default()
        };
        let before = inner
            .with_candidate_sql_budget(&mut budget, || inner.run(prepared, Some(author), &read))?;
        let own = inner.seq + 1;
        inner
            .conn
            .execute_batch("SAVEPOINT accepted_overlay")
            .map_err(storage)?;
        let mark = inner.changed.borrow().len();
        let gone = inner.gone.borrow().len();
        let stamp = inner.last_stamp;
        let previous = inner.claims.insert(
            author,
            prepared
                .request_claims
                .as_ref()
                .expect("bound overlay scope")
                .1
                .clone(),
        );
        let unit = ops
            .iter()
            .cloned()
            .map(|op| (inner.schema.clone(), op))
            .collect::<Vec<_>>();
        let result = inner.candidate_unit(&unit, own, true, false, |inner| {
            ops.iter().try_for_each(|op| inner.apply_as(op, Who::Session(author), own).map(drop))
        }).and_then(|()| {
            let read = ReadSpec { cut: Some(Cut { seq: through, own, ..Cut::default() }), permission_cut: Some(through), permission_own: own, include_deleted, ..ReadSpec::default() };
            let after = inner.with_candidate_sql_budget(&mut budget, || inner.run(prepared, Some(author), &read))?;
            if before != after {
                return Err(Error::new(ErrorCode::NotObserved, "authority overlay evidence shows staged or provisional content in the paired read"));
            }
            Ok(())
        });
        let rolled_back = inner
            .rollback_savepoint("accepted_overlay")
            .map_err(storage);
        inner.changed.borrow_mut().truncate(mark);
        inner.gone.borrow_mut().truncate(gone);
        inner.last_stamp = stamp;
        inner.writer = None;
        match previous {
            Some(claims) => {
                inner.claims.insert(author, claims);
            }
            None => {
                inner.claims.remove(&author);
            }
        }
        rolled_back?;
        result
    }

    pub(in crate::db) async fn check_accepted_overlay(
        &self,
        prepared: &PreparedQuery,
        through: u64,
        include_deleted: bool,
        deadline: Instant,
        ops: Vec<WireOp>,
    ) -> Result<(), Error> {
        let (author, claims) = prepared
            .request_claims
            .as_ref()
            .expect("bound paired scope");
        let (id, connection) = {
            let mut inner = self.inner();
            let client = inner.client.as_mut().ok_or_else(|| {
                Error::new(
                    ErrorCode::NotObserved,
                    "accepted overlay needs an authority connection",
                )
            })?;
            if client.transport.is_none() || !may_wait(client.hint.get()) {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "accepted overlay authority unavailable",
                ));
            }
            let id = client.next_id;
            client.next_id += 1;
            client.answers.insert(id, None);
            client.overlay_answers.insert(id);
            client.outbox.push(SyncMessage::AcceptedOverlayCheckV1 {
                id,
                query: prepared.query.clone(),
                binding: prepared.binding.clone(),
                through,
                include_deleted,
                author: author.canonical().to_owned(),
                claims: claims.clone(),
                ops,
            });
            (id, client.connection)
        };
        self.schedule_pending_tick();
        let _deadline = self.read_deadline(Some(deadline));
        let _attempt_deadline = self.read_attempt_deadline();
        let _request = FetchRequest {
            inner: self.inner.clone(),
            id,
        };
        poll_fn(|_| {
            self.ensure_storage_owner_active()?;
            let mut inner = self.inner();
            let client = inner.client.as_mut().expect("client");
            if client.connection != connection
                || !may_wait(client.hint.get())
                || Instant::now() >= deadline
            {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::NotObserved,
                    "accepted overlay check interrupted or timed out",
                )));
            }
            match client.answers.get_mut(&id).and_then(Option::take) {
                Some(Ok((cut, _, _, _))) if cut == through => Poll::Ready(Ok(())),
                Some(Ok(_)) => Poll::Ready(Err(Error::new(
                    ErrorCode::Protocol,
                    "accepted overlay response has a different cut",
                ))),
                Some(Err(error)) => Poll::Ready(Err(error)),
                None => Poll::Pending,
            }
        })
        .await
    }
}

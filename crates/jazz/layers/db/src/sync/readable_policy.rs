//! Readable policy dependencies for an ordinary session's private owner.
use super::*;

impl Inner {
    /// Reject unindexed dependency shapes even when the result is empty.
    pub(super) fn check_readable_policy_shape(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<(), Error> {
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let table = schema
            .table(history_source_table(&prepared.query.table).unwrap_or(&prepared.query.table))?;
        let (_, session) = self.read_sessions(prepared, identity);
        Ctx::new(&schema, None, &session, &prepared.binding)
            .with_catalogue(self)
            .with_candidate_evidence(Some(crate::compile::CandidateEvidence {
                accepted: self.seq,
                publication: self.seq + 1,
                post: false,
            }))
            .operation_policy(table, Operation::Select, "t", &session)?;
        Ok(())
    }

    /// Build the dependency closure before publishing any of it. Every image
    /// must be readable under the requesting connection's current policy. The
    /// same indexed collector and one shared work budget bound the whole walk.
    pub(super) fn subscription_policy_inputs(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        group: &Weak<RefCell<fanout::Group>>,
    ) -> Result<Vec<Image>, Error> {
        let effective = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let root = effective
            .table(history_source_table(&prepared.query.table).unwrap_or(&prepared.query.table))?;
        if effective
            .policy_schema()
            .by_stored(&root.stored)
            .and_then(|table| table.policy(Operation::Select))
            .is_none_or(row_local_policy)
        {
            // No row dependencies are needed. Keep the ordinary query and
            // branch transport intact, including its frozen owner inputs.
            return Ok(Vec::new());
        }
        let Some(group) = group.upgrade() else {
            return Ok(Vec::new());
        };
        let group = group.borrow();
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
        let mut queue = VecDeque::new();
        let mut budget = scoped_reads::MAX_EVIDENCE;
        for row in &group.rows {
            if queue.len() >= budget {
                return Err(unsupported(
                    "readable permission inputs exceeding their bounded work limit",
                ));
            }
            let table = schema.table(history_source_table(&row.table).unwrap_or(&row.table))?;
            if !table.branch_by.is_empty() {
                return Err(unsupported(
                    "readable permission subscriptions of branched rows",
                ));
            }
            let coordinate = ScopedReadRowV1 {
                table: table.stored.clone(),
                id: *row.id.as_bytes(),
                key: Vec::new(),
                seq: row.seq,
            };
            queue.extend(self.scoped_evidence_image(&coordinate)?);
        }
        drop(group);
        let mut visited = BTreeSet::new();
        let mut out = Vec::new();
        while let Some(image) = queue.pop_front() {
            if !visited.insert((image.table.clone(), image.id, image.key.clone(), image.seq)) {
                continue;
            }
            let evidence = self.permission_evidence_with_budget(
                prepared,
                identity,
                std::slice::from_ref(&image),
                self.seq,
                &mut budget,
            )?;
            if !readable(
                self,
                &image.table,
                RowUuid::from_bytes(image.id),
                &image.key,
                identity,
            )? {
                return Err(unsupported(
                    "a readable permission subscription whose dependency is not readable by its session",
                ));
            }
            for coordinate in evidence {
                if !visited.contains(&(
                    coordinate.table.clone(),
                    coordinate.id,
                    coordinate.key.clone(),
                    coordinate.seq,
                )) {
                    queue.extend(self.scoped_evidence_image(&coordinate)?);
                }
            }
            out.push(image);
        }
        Ok(out)
    }

    /// A refused read is terminal for this ID, but leaves other reads and
    /// writes on the connection usable. Relays propagate the same refusal.
    pub(super) fn refuse_subscription(&mut self, index: usize, id: u64, error: Error) {
        self.sessions[index].support.remove(&id);
        self.sessions[index].readable_policy.remove(&id);
        self.sessions[index].query_support.remove(&id);
        self.sessions[index].scoped_unions.remove(&id);
        self.sessions[index].resume.remove(&id);
        if self.sessions[index].subs.remove(&id).is_some() {
            self.fan.dropped.set(self.fan.dropped.get() + 1);
        }
        if let Some((query, binding, deleted, scope)) = self.sessions[index].relayed.remove(&id) {
            self.release_remote(&query, &binding, deleted, &scope, RemoteReach::REMOTE);
        }
        self.sessions[index].outbox.push(SyncMessage::Refused {
            id,
            code: error.code,
            message: error.message,
        });
    }

    /// A shared row/evidence frame cannot be partially delivered. Refuse the
    /// affected peer's reads and release its interests before closing it.
    pub(super) fn refuse_session_reads(&mut self, index: usize, error: Error) {
        let session = &mut self.sessions[index];
        let mut ids = session.subs.keys().copied().collect::<BTreeSet<_>>();
        ids.extend(session.outbox.iter().filter_map(|message| match message {
            SyncMessage::Answer { id, .. }
            | SyncMessage::AcceptedAnswerV1 { id, .. }
            | SyncMessage::ScopedEvidenceAnswerV1 { id, .. }
            | SyncMessage::AcceptedScopedEvidenceAnswerV1 { id, .. } => Some(*id),
            _ => None,
        }));
        for id in ids {
            let _ = session.transport.send(SyncMessage::Refused {
                id,
                code: error.code,
                message: error.message.clone(),
            });
        }
        let released = session.close(&self.fan.dropped);
        self.release_session_reads(released);
    }
}

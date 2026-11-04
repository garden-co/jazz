//! Owner-local evidence provenance. Submission records describe an operation's
//! original schema and local scope; they are never evidence or permission.
use super::*;
use crate::db::Store;
use std::rc::Weak;

/// Effective operands for this local observation, not a transport capability
/// or evidence that a caller-selected policy bundle was authenticated.
#[derive(Clone, Debug, PartialEq)]
struct ObservationScope {
    author: AuthorSubject,
    claims: BTreeMap<String, Value>,
}

impl Inner {
    fn observation_scope_of(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
    ) -> ObservationScope {
        let (_, session) = self.read_sessions(prepared, Some(author));
        ObservationScope {
            author,
            claims: session.claims,
        }
    }
}

/// An explicitly privileged, in-process host attachment between two actual
/// owners and one live admitted receiver session. A subscriber id alone is not
/// proof of locality. This value has no byte or deserialization constructor.
#[derive(Clone)]
pub struct LocalSubmissionRoute(Rc<LocalAttachment>);

struct LocalAttachment {
    producer: Weak<Store>,
    receiver: Weak<Store>,
    producer_epoch: u64,
    receiver_epoch: u64,
    namespace: [u8; 16],
    node: [u8; 16],
    subscriber: SubscriberId,
}

/// Core-generated original operation provenance bound to one exact pending
/// Commit and local route. It cannot be decoded from arbitrary peer metadata.
#[derive(Clone)]
pub struct LocalSubmissionExport {
    route: LocalSubmissionRoute,
    id: TxId,
    fingerprint: [u8; 32],
    operations: Vec<Option<SubmissionProvenance>>,
}

impl LocalSubmissionRoute {
    fn live(&self, receiver: &Rc<Store>) -> Result<bool, Error> {
        let Some(source) = self.0.producer.upgrade() else {
            return Ok(false);
        };
        let Some(target) = self.0.receiver.upgrade() else {
            return Ok(false);
        };
        if !Rc::ptr_eq(receiver, &target)
            || source.1.get() != self.0.producer_epoch
            || target.1.get() != self.0.receiver_epoch
            || self.0.producer_epoch == u64::MAX
            || self.0.receiver_epoch == u64::MAX
        {
            return Ok(false);
        }
        let source = source.borrow();
        let target = target.borrow();
        Ok(source.identity.node.as_bytes() == &self.0.node
            && source.namespace()? == Some(self.0.namespace)
            && target.namespace()? == Some(self.0.namespace)
            && target
                .sessions
                .get(self.0.subscriber.slot)
                .is_some_and(|session| {
                    session.open
                        && session.generation == self.0.subscriber.generation
                        && session.identity == source.identity.author
                }))
    }
}

// A content-free state change updates the normal $updated* stamp, while its
// application body and creation provenance still come from the exact content
// image. Equal coordinates must match every cell; a content-bearing state
// cannot name an earlier image as its content. No other cell is ignored.
fn observation_content_matches(state: &Image, content: &Image) -> bool {
    if state.at == content.at {
        return state.cells == content.cells;
    }
    !state.content
        && state
            .cells
            .iter()
            .filter(|(name, _)| !matches!(name.as_str(), "$updatedAt" | "$updatedBy"))
            .eq(content
                .cells
                .iter()
                .filter(|(name, _)| !matches!(name.as_str(), "$updatedAt" | "$updatedBy")))
}

fn commit_fingerprint(message: &SyncMessage) -> [u8; 32] {
    // Ephemeral comparison only: this is not a new storage or wire encoding.
    jazz_types::sha256::derive_key("jazz.local-submission-frame.v1", &encode(message))
}

impl Db {
    /// Trusted local host attachment, after ordinary session authentication.
    /// The caller must own both in-process Db handles and the admitted link.
    /// This does not promote session trust, select a policy bundle, or bypass
    /// normal queued Commit processing. Cross-worker hosts need a separately
    /// authenticated adapter; a decoded companion is not this capability.
    pub fn trusted_local_submission_route(
        &self,
        producer: &Db,
        subscriber: SubscriberId,
    ) -> Result<LocalSubmissionRoute, Error> {
        if Rc::ptr_eq(&self.inner, &producer.inner) {
            return Err(unobserved(
                "a local submission route requires distinct owners",
            ));
        }
        let namespace = self
            .inner
            .borrow()
            .namespace()?
            .ok_or_else(|| unobserved("the receiver has no authority namespace"))?;
        let route = LocalSubmissionRoute(Rc::new(LocalAttachment {
            producer: Rc::downgrade(&producer.inner),
            receiver: Rc::downgrade(&self.inner),
            producer_epoch: producer.inner.1.get(),
            receiver_epoch: self.inner.1.get(),
            namespace,
            node: *producer.inner.borrow().identity.node.as_bytes(),
            subscriber,
        }));
        if !route.live(&self.inner)? {
            return Err(unobserved(
                "local submission owners or admitted session do not match",
            ));
        }
        Ok(route)
    }

    /// Export from the producer's actual pending header/items and captured
    /// per-operation records. Missing/legacy/ambiguous records stay unknown.
    pub fn export_local_submission(
        &self,
        route: &LocalSubmissionRoute,
        receipt: TxReceipt,
    ) -> Result<Option<LocalSubmissionExport>, Error> {
        let Some(producer) = route.0.producer.upgrade() else {
            return Ok(None);
        };
        let Some(receiver) = route.0.receiver.upgrade() else {
            return Ok(None);
        };
        if !Rc::ptr_eq(&producer, &self.inner) || !route.live(&receiver)? {
            return Ok(None);
        }
        let inner = self.inner.borrow();
        let header: Option<Vec<u8>> = inner
            .conn
            .cached_row(
                "SELECT header FROM _jazz_pending WHERE tx=?1",
                [receipt.0 as i64],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        let Some(header) = header else {
            return Ok(None);
        };
        let message = inner.pending_commit(receipt.0, &header)?;
        let SyncMessage::Commit { id, ref ops, .. } = message else {
            return Ok(None);
        };
        if id.node.as_bytes() != &route.0.node {
            return Ok(None);
        }
        let operations = inner.submission_records(receipt.0, ops.len())?;
        if !operations.iter().any(Option::is_some) {
            return Ok(None);
        }
        Ok(Some(LocalSubmissionExport {
            route: route.clone(),
            id,
            fingerprint: commit_fingerprint(&message),
            operations,
        }))
    }

    /// Attach one companion before its normal frame is served. Replacing the
    /// slot discards only optional provenance, never an ordinary Commit. The
    /// normal queue remains the sole executor; held frames keep the companion
    /// until served, and retired/wrong/mismatched routes supply no evidence.
    pub fn attach_local_submission(
        &self,
        route: &LocalSubmissionRoute,
        export: LocalSubmissionExport,
    ) -> Result<bool, Error> {
        if !Rc::ptr_eq(&route.0, &export.route.0) || !route.live(&self.inner)? {
            return Ok(false);
        }
        self.inner.borrow_mut().sessions[route.0.subscriber.slot].local_submission = Some(export);
        Ok(true)
    }

    /// Invalidate local cuts/attachments in all views of this owner. Closing
    /// or stopping mutation admission calls this; future worker retirement
    /// must do the same before replacing its storage owner. Retirement is
    /// terminal for this owner's local capabilities; reopening creates a new
    /// owner. Ordinary sync/commit semantics are unchanged.
    pub fn retire_local_observation_capabilities(&self) {
        self.inner.1.set(u64::MAX);
    }

    pub(super) fn local_submission_for(
        &self,
        index: usize,
        message: &SyncMessage,
    ) -> Result<Option<Vec<Option<SubmissionProvenance>>>, Error> {
        let export = self.inner.borrow().sessions[index].local_submission.clone();
        let Some(export) = export else {
            return Ok(None);
        };
        let matching = matches!(message, SyncMessage::Commit { id, ops, .. }
            if *id == export.id && ops.len() == export.operations.len())
            && export.route.0.subscriber.slot == index
            && export.route.live(&self.inner)?
            && export.fingerprint == commit_fingerprint(message);
        if !matching {
            self.inner.borrow_mut().sessions[index].local_submission = None;
            return Ok(None);
        }
        Ok(Some(export.operations))
    }
}

impl Inner {
    fn submission_records(
        &self,
        tx: u64,
        count: usize,
    ) -> Result<Vec<Option<SubmissionProvenance>>, Error> {
        let mut out = vec![None; count];
        if !self.has_observation_table("_jazz_observation_pending_v1")? {
            return Ok(out);
        }
        let mut query = self
            .conn
            .prepare_cached(
                "SELECT op,schema,scope FROM _jazz_observation_pending_v1 WHERE tx=?1 ORDER BY op",
            )
            .map_err(storage)?;
        let mut rows = query.query([tx as i64]).map_err(storage)?;
        while let Some(row) = rows.next().map_err(storage)? {
            let decoded = (|| -> rusqlite::Result<_> {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })();
            let Ok((op, schema, scope)) = decoded else {
                continue;
            };
            let (Ok(op), Ok(schema), Ok(scope)) =
                (usize::try_from(op), schema.try_into(), scope.try_into())
            else {
                continue;
            };
            if let Some(slot) = out.get_mut(op) {
                *slot = Some(SubmissionProvenance { schema, scope });
            }
        }
        Ok(out)
    }
}

fn submission_preimage(
    scope: &ObservationScope,
    owner_schema: [u8; 16],
    bundle: Option<[u8; 32]>,
    authority: bool,
) -> Option<Vec<u8>> {
    let mut input = scope_digest(scope)?.to_vec();
    // Actual local governing context is provenance, not a SELECT verdict.
    match bundle {
        None => {
            input.push(0);
            input.extend_from_slice(&owner_schema);
        }
        Some(bundle) => {
            input.push(1);
            input.extend_from_slice(&bundle);
        }
    }
    // Receipt-local UNKNOWN remains distinct from a known authority context.
    input.push(u8::from(authority));
    Some(input)
}

fn submission_scope(
    scope: &ObservationScope,
    governance: &Governance,
    inner: &Inner,
) -> Option<[u8; 32]> {
    let input = submission_preimage(
        scope,
        *inner.owner.jazz.version_id().as_bytes(),
        governance.key().map(|bundle| bundle.0),
        inner.client.is_none(),
    )?;
    Some(jazz_types::sha256::derive_key(
        "jazz.sqlite.observation-submission-scope.v1",
        &input,
    ))
}

/// One operation before stored-name lowering. There is deliberately no
/// evidence admission number until a genuine accepted result is reconciled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SubmissionProvenance {
    schema: [u8; 16],
    scope: [u8; 32],
}

/// Additive `jazz.sqlite.observation-submission.v1`. The trigger also runs when
/// an older writer forgets a pending header, preventing stale provenance reuse.
const SUBMISSION_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_observation_pending_v1 (\
     tx INTEGER NOT NULL CHECK(typeof(tx)='integer'), op INTEGER NOT NULL CHECK(typeof(op)='integer' AND op >= 0), \
     schema BLOB NOT NULL CHECK(typeof(schema) = 'blob' AND length(schema) = 16), \
     scope BLOB NOT NULL CHECK(typeof(scope) = 'blob' AND length(scope) = 32), \
     PRIMARY KEY(tx, op)) WITHOUT ROWID; \
    CREATE TRIGGER IF NOT EXISTS _jazz_observation_pending_forget_v1 \
     AFTER DELETE ON _jazz_pending BEGIN \
     DELETE FROM _jazz_observation_pending_v1 WHERE tx = OLD.tx; END;";

/// `jazz.sqlite.accepted-observation.v1`: only admitted evidence, never bodies.
/// AUTOINCREMENT allocates inside the caller's savepoint and survives deletion
/// without admitting a late receipt under an earlier observation watermark.
const EVIDENCE_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_observation_evidence_v1 (\
     admission INTEGER PRIMARY KEY AUTOINCREMENT CHECK(typeof(admission)='integer' AND admission > 0), \
     schema BLOB NOT NULL CHECK(typeof(schema)='blob' AND length(schema)=16), \
     tbl TEXT NOT NULL CHECK(typeof(tbl)='text'), id BLOB NOT NULL CHECK(typeof(id)='blob' AND length(id)=16), \
     state INTEGER NOT NULL CHECK(typeof(state)='integer' AND state > 0 AND state <= 4503599627370495), \
     scope BLOB NOT NULL CHECK(typeof(scope)='blob' AND length(scope)=32), \
     proof INTEGER NOT NULL CHECK(typeof(proof)='integer' AND proof IN (0,1)), \
     state_node INTEGER NOT NULL CHECK(typeof(state_node)='integer' AND state_node > 0), state_tx INTEGER NOT NULL CHECK(typeof(state_tx)='integer'), \
     content_node INTEGER NOT NULL CHECK(typeof(content_node)='integer' AND content_node > 0), content_tx INTEGER NOT NULL CHECK(typeof(content_tx)='integer'), \
     content INTEGER NOT NULL CHECK(typeof(content)='integer' AND content > 0 AND content <= state), \
     UNIQUE(schema,tbl,id,state,scope,proof)); \
    CREATE INDEX IF NOT EXISTS _jazz_observation_state_v1 ON _jazz_observation_evidence_v1(state);";

fn unobserved(message: &str) -> Error {
    Error::new(ErrorCode::NotObserved, message)
}

/// Why a held version has accepted origins. Neither kind is a fresh query,
/// complete membership result or reusable authorization decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalEvidenceProvenance {
    /// Genuine Global Fate, qualified by the local operation's original scope.
    LocalSubmission,
    /// A validated authority read paired with an already-held exact body.
    AuthorityRead,
}

impl LocalEvidenceProvenance {
    fn tag(self) -> i64 {
        match self {
            Self::LocalSubmission => 0,
            Self::AuthorityRead => 1,
        }
    }
}

impl Inner {
    fn has_observation_table(&self, name: &str) -> Result<bool, Error> {
        self.conn
            .cached_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                [name],
                |r| r.get(0),
            )
            .map_err(storage)
    }

    /// Only genuine Global reconciliation calls this, after as_written/whole
    /// images are installed and before pending provenance can be forgotten.
    pub(super) fn observe_global_submission(
        &self,
        tx: u64,
        id: jazz_model::tx::TxId,
        through: u64,
    ) -> Result<(), Error> {
        if !self.has_observation_table("_jazz_observation_pending_v1")? {
            return Ok(());
        }
        // Join the existing per-op primary key, touching only this commit's
        // submitted operations, including after reopen or resend.
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT p.schema,p.scope,i.item FROM _jazz_observation_pending_v1 p \
             JOIN _jazz_pending_items i ON i.tx=p.tx AND i.part=?2 AND i.n=p.op \
             WHERE p.tx=?1 ORDER BY p.op",
            )
            .map_err(storage)?;
        let mut records = statement
            .query(rusqlite::params![tx as i64, PendingPart::Op as i64])
            .map_err(storage)?;
        while let Some(record) = records.next().map_err(storage)? {
            let decoded = (|| -> rusqlite::Result<_> {
                Ok((
                    record.get::<_, Vec<u8>>(0)?,
                    record.get::<_, Vec<u8>>(1)?,
                    record.get::<_, Vec<u8>>(2)?,
                ))
            })();
            // An unavailable/malformed optional companion does not change an
            // otherwise valid ordinary receipt. SQLite execution failures do.
            let Ok((schema, scope, item)) = decoded else {
                continue;
            };
            let (Ok(schema), Ok(scope)) = (schema.try_into(), scope.try_into()) else {
                continue;
            };
            let op: WireOp = decode_stored(&item)?;
            let Some(row) = wire_row(&op) else {
                continue;
            };
            let Some(table) = self.physical.tables.get(op.table()) else {
                continue;
            };
            if !table.branch_by.is_empty() {
                continue;
            }
            let Some(image) = self.observation_held_image(op.table(), row, through)? else {
                continue;
            };
            // A content-free receipt cannot establish the earlier content's
            // exact origin. Only a validated paired read can add that evidence.
            if !image.content || image.written.is_some() {
                continue;
            }
            self.record_settled(*id.node.as_bytes(), id.time.0, Some(Pos::at(through)))?;
            let version = crate::db::AcceptedVersion {
                transaction: id,
                sequence: through,
            };
            self.admit_observation(
                schema,
                op.table(),
                scope,
                LocalEvidenceProvenance::LocalSubmission,
                &crate::db::AcceptedRowEvidence {
                    row,
                    state: version,
                    content: version,
                },
            )?;
        }
        Ok(())
    }

    /// Normal receipt reconciliation can hold an accepted image in the
    /// existing baseline while its pending prefix still overlays SQLite. The
    /// same outer sync_tick savepoint rebases that exact image into ordinary
    /// storage before committing this evidence. No extra body cache or replay
    /// pass is introduced, and optimistic pending images never qualify.
    fn observation_held_image(
        &self,
        table: &str,
        row: RowUuid,
        at: u64,
    ) -> Result<Option<Image>, Error> {
        let stored = self.version_image(table, row, &[], Pos::at(at))?;
        let baseline = self
            .client
            .as_ref()
            .and_then(|client| client.baselines.get(&(table.to_owned(), row, Vec::new())))
            .and_then(Option::as_ref)
            .filter(|image| image.at == Pos::at(at) && image.written.is_none());
        if let (Some(stored), Some(baseline)) = (&stored, baseline)
            && (stored.cells != baseline.cells
                || stored.content != baseline.content
                || stored.deleted != baseline.deleted)
        {
            return Err(unobserved(
                "accepted baseline conflicts with its retained exact body",
            ));
        }
        Ok(stored.or_else(|| baseline.cloned()))
    }

    /// The caller owns the savepoint. Exact forward origin references must
    /// already exist; an arbitrary reverse position lookup is never evidence.
    fn admit_observation(
        &self,
        schema: [u8; 16],
        table: &str,
        scope: [u8; 32],
        proof: LocalEvidenceProvenance,
        pair: &crate::db::AcceptedRowEvidence,
    ) -> Result<(), Error> {
        if pair.content.sequence == 0
            || pair.content.sequence > pair.state.sequence
            || pair.state.sequence > MAX_SEQ
        {
            return Err(unobserved(
                "local observation has invalid accepted coordinates",
            ));
        }
        let exact = |version: crate::db::AcceptedVersion| -> Result<i64, Error> {
            let found = self
                .conn
                .cached_row(
                    "SELECT t.n FROM _jazz_transactions t JOIN _jazz_nodes n ON n.n=t.n \
                 WHERE n.node=?1 AND t.tx=?2 AND t.t=?3",
                    rusqlite::params![
                        version.transaction.node.as_bytes().as_slice(),
                        version.transaction.time.0 as i64,
                        version.sequence as i64
                    ],
                    |r| r.get(0),
                )
                .optional()
                .map_err(storage)?;
            found.ok_or_else(|| unobserved("exact forward accepted origin is unavailable"))
        };
        let state_node = exact(pair.state)?;
        let content_node = exact(pair.content)?;
        let state = self
            .observation_held_image(table, pair.row, pair.state.sequence)?
            .ok_or_else(|| unobserved("accepted state body is not retained"))?;
        let content = self
            .observation_held_image(table, pair.row, pair.content.sequence)?
            .ok_or_else(|| unobserved("accepted content body is not retained"))?;
        if state.written.is_some()
            || content.written.is_some()
            || !content.content
            || !observation_content_matches(&state, &content)
        {
            return Err(unobserved(
                "held state and content do not match accepted pairing",
            ));
        }
        self.conn.execute_batch(EVIDENCE_DDL).map_err(storage)?;
        // The unique key's prefix bounds this to the two named proof kinds.
        // Check both atomically, so a stronger proof never hides a conflict.
        let mut query = self
            .conn
            .prepare_cached(
                "SELECT proof,state_node,state_tx,content_node,content_tx,content \
             FROM _jazz_observation_evidence_v1 \
             WHERE schema=?1 AND tbl=?2 AND id=?3 AND state=?4 AND scope=?5 LIMIT 3",
            )
            .map_err(storage)?;
        let existing = query
            .query_map(
                rusqlite::params![
                    schema.as_slice(),
                    table,
                    pair.row.0.as_bytes().as_slice(),
                    pair.state.sequence as i64,
                    scope.as_slice()
                ],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, i64>(5)?,
                    ))
                },
            )
            .map_err(storage)?;
        let mut duplicate = false;
        for row in existing {
            let (kind, sn, st, cn, ct, content) = row.map_err(storage)?;
            if !(0..=1).contains(&kind)
                || (sn, st, cn, ct, content)
                    != (
                        state_node,
                        pair.state.transaction.time.0 as i64,
                        content_node,
                        pair.content.transaction.time.0 as i64,
                        pair.content.sequence as i64,
                    )
            {
                return Err(unobserved("conflicting accepted observation evidence"));
            }
            duplicate |= kind == proof.tag();
        }
        if duplicate {
            return Ok(());
        }
        self.exec(
            "INSERT INTO _jazz_observation_evidence_v1 \
            (schema,tbl,id,state,scope,proof,state_node,state_tx,content_node,content_tx,content) \
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            rusqlite::params![
                schema.as_slice(),
                table,
                pair.row.0.as_bytes().as_slice(),
                pair.state.sequence as i64,
                scope.as_slice(),
                proof.tag(),
                state_node,
                pair.state.transaction.time.0 as i64,
                content_node,
                pair.content.transaction.time.0 as i64,
                pair.content.sequence as i64
            ],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// Capture only the operation's known local submitting principal. An
    /// attribution is not a session; unsupported provenance leaves ordinary
    /// writes alone and cannot later manufacture observation evidence.
    pub(crate) fn observation_submission(
        &self,
        schema: &Schema,
        op: &StagedOp,
        governance: &Governance,
    ) -> Option<SubmissionProvenance> {
        let author = match op.identity()? {
            // Main's who() and stamp() treat Session(SYSTEM) as local, so
            // its submitting author is this database's actual identity.
            WriteIdentity::Database => self.identity.author,
            WriteIdentity::Session(author) => {
                if author == AuthorSubject::SYSTEM {
                    self.identity.author
                } else {
                    author
                }
            }
            WriteIdentity::Attribution(_) => return None,
        };
        let claims = self.claims.get(&author).cloned().unwrap_or_default();
        Some(SubmissionProvenance {
            schema: *schema.jazz.version_id().as_bytes(),
            scope: submission_scope(&ObservationScope { author, claims }, governance, self)?,
        })
    }

    /// Called inside the same existing upload savepoint as `persist_pending`.
    /// Per-op indices keep mixed-schema batches exact without changing Commit.
    pub(super) fn persist_observation_submission(
        &self,
        tx: u64,
        provenance: Option<&[Option<SubmissionProvenance>]>,
    ) -> Result<(), Error> {
        let Some(provenance) = provenance.filter(|p| p.iter().any(Option::is_some)) else {
            return Ok(());
        };
        self.conn.execute_batch(SUBMISSION_DDL).map_err(storage)?;
        for (op, provenance) in provenance.iter().enumerate() {
            if let Some(provenance) = provenance {
                self.exec(
                    "INSERT INTO _jazz_observation_pending_v1 (tx,op,schema,scope) VALUES (?1,?2,?3,?4)",
                    rusqlite::params![
                        tx as i64,
                        op as i64,
                        provenance.schema.as_slice(),
                        provenance.scope.as_slice()
                    ],
                )
                .map_err(storage)?;
            }
        }
        Ok(())
    }
}

/// One owner-local, accepted-only cut. Its coordinate is a locally known
/// authority position, not fresh coverage; its second watermark freezes when
/// evidence arrived. It cannot enrol authority transaction predicates.
pub struct LocalObservationCut {
    owner: Weak<Store>,
    epoch: u64,
    namespace: [u8; 16],
    schema: [u8; 16],
    scope: ObservationScope,
    digest: [u8; 32],
    through: u64,
    admission: i64,
}

/// Partial, possibly stale local candidates, each paired with exact accepted
/// state/content origins. This is neither complete history nor SELECT proof.
#[derive(Debug)]
pub struct LocalObservation {
    pub through: u64,
    pub rows: Vec<CurrentRow>,
    pub evidence: Vec<crate::db::AcceptedRowEvidence>,
    pub provenance: Vec<LocalEvidenceProvenance>,
}

impl Db {
    /// Freeze one local cut for several prepared metadata queries. No network,
    /// hidden hydration, transaction admission or freshness promise is made.
    /// An active SQLite transaction refuses capture: its evidence admission
    /// numbers may be reused after rollback and cannot escape in a cut.
    pub fn local_observation_cut(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<LocalObservationCut, Error> {
        if self.inner.1.get() == u64::MAX {
            return Err(unobserved("local observation owner was retired"));
        }
        let inner = self.inner();
        // The Store gate has settled temporary batch overlays. Any remaining
        // transaction can still roll back evidence and its admission sequence.
        if !inner.conn.is_autocommit() {
            return Err(unobserved(
                "local observation capture requires committed evidence",
            ));
        }
        let schema = prepared.schema.as_ref().unwrap_or(&inner.owner);
        crate::db::accepted_read::check_accepted_shape(prepared, schema)?;
        let author = identity.unwrap_or(inner.identity.author);
        let scope = inner.observation_scope_of(prepared, author);
        let digest = submission_scope(&scope, &self.governance, &inner)
            .ok_or_else(|| unobserved("unsupported local observation scope"))?;
        let namespace = inner
            .namespace()?
            .ok_or_else(|| unobserved("no authority namespace is held"))?;
        let (admission, evidence_through) = inner.observation_watermarks()?;
        Ok(LocalObservationCut {
            owner: Rc::downgrade(&self.inner),
            epoch: self.inner.1.get(),
            namespace,
            schema: *schema.jazz.version_id().as_bytes(),
            scope,
            digest,
            through: inner.served_through().max(evidence_through),
            admission,
        })
    }

    /// Run against an existing local cut with current applicable policies.
    /// Missing exact evidence refuses observation; pending prefixes are never
    /// used. Ordinary transactions and their pending reads are unchanged.
    pub fn observe_local(
        &self,
        cut: &LocalObservationCut,
        prepared: &PreparedQuery,
        include_deleted: bool,
    ) -> Result<LocalObservation, Error> {
        let owner = cut
            .owner
            .upgrade()
            .ok_or_else(|| unobserved("observation owner was replaced"))?;
        if !Rc::ptr_eq(&owner, &self.inner)
            || cut.epoch != self.inner.1.get()
            || cut.epoch == u64::MAX
        {
            return Err(unobserved("observation owner was retired or replaced"));
        }
        let inner = self.inner();
        let schema = prepared.schema.as_ref().unwrap_or(&inner.owner);
        crate::db::accepted_read::check_accepted_shape(prepared, schema)?;
        let scope = inner.observation_scope_of(prepared, cut.scope.author);
        if schema.jazz.version_id().as_bytes() != &cut.schema
            || inner.namespace()? != Some(cut.namespace)
            || scope != cut.scope
            || submission_scope(&scope, &self.governance, &inner) != Some(cut.digest)
        {
            return Err(unobserved(
                "observation schema, namespace or governing scope changed",
            ));
        }
        let read = ReadSpec {
            cut: Some(Cut::at(Pos::at(cut.through))),
            include_deleted,
            settled_only: true,
            ..ReadSpec::new(self.governance.clone())
        };
        let rows = inner.run(prepared, Some(scope.author), &read)?;
        let table = schema.table(&prepared.query.table)?;
        let mut evidence = Vec::with_capacity(rows.len());
        let mut provenance = Vec::with_capacity(rows.len());
        for row in &rows {
            if !row.seq.accepted()
                || row
                    .writer
                    .as_deref()
                    .is_some_and(|writer| !writer.is_empty())
            {
                return Err(unobserved("observation returned a pending row"));
            }
            let (pair, proof) = inner.observation_pair(cut, &table.stored, row.id, row.seq.seq)?;
            evidence.push(pair);
            provenance.push(proof);
        }
        Ok(LocalObservation {
            through: cut.through,
            rows,
            evidence,
            provenance,
        })
    }

    /// Remember only evidence evaluated here by the actual authority handle.
    /// Remote accepted-read envelopes carry no authenticated BundleId and
    /// cannot use this entrypoint. Bodies remain in ordinary SQLite history.
    pub fn retain_local_authority_read(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        identity: Option<AuthorSubject>,
    ) -> Result<crate::db::AcceptedRead, Error> {
        if self.inner.1.get() == u64::MAX {
            return Err(unobserved("local observation owner was retired"));
        }
        let author = identity.unwrap_or(self.inner.borrow().identity.author);
        let read = self.accepted_read(prepared, None, include_deleted, Some(author))?;
        let inner = self.inner();
        let schema = prepared.schema.as_ref().unwrap_or(&inner.owner);
        let scope = inner.observation_scope_of(prepared, author);
        let digest = submission_scope(&scope, &self.governance, &inner)
            .ok_or_else(|| unobserved("unsupported authority observation scope"))?;
        let table = schema.table(&prepared.query.table)?;
        begin_savepoint(&inner.conn, "local_authority_observation")?;
        let stored = read.evidence.iter().try_for_each(|pair| {
            for origin in [pair.state, pair.content] {
                inner.record_settled(
                    *origin.transaction.node.as_bytes(),
                    origin.transaction.time.0,
                    Some(Pos::at(origin.sequence)),
                )?;
            }
            inner.admit_observation(
                *schema.jazz.version_id().as_bytes(),
                &table.stored,
                digest,
                LocalEvidenceProvenance::AuthorityRead,
                pair,
            )
        });
        end_savepoint(&inner.conn, "local_authority_observation", stored.is_ok())?;
        stored?;
        Ok(read)
    }
}

impl Inner {
    fn observation_watermarks(&self) -> Result<(i64, u64), Error> {
        if !self.has_observation_table("_jazz_observation_evidence_v1")? {
            return Ok((0, 0));
        }
        // sqlite_sequence survives deletions; no cached counter or pending-row
        // allocation can move a cut's evidence watermark backwards.
        let admission = self
            .conn
            .cached_row(
                "SELECT seq FROM sqlite_sequence WHERE name='_jazz_observation_evidence_v1'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage)?
            .unwrap_or(0);
        let through = self
            .conn
            .cached_row(
                "SELECT MAX(state) FROM _jazz_observation_evidence_v1",
                [],
                |r| r.get::<_, Option<i64>>(0),
            )
            .map_err(storage)?
            .unwrap_or(0);
        if admission < 0 || through < 0 || through as u64 > MAX_SEQ {
            return Err(unobserved("invalid observation watermarks"));
        }
        Ok((admission, through as u64))
    }

    fn observation_pair(
        &self,
        cut: &LocalObservationCut,
        table: &str,
        row: RowUuid,
        state: u64,
    ) -> Result<(crate::db::AcceptedRowEvidence, LocalEvidenceProvenance), Error> {
        if !self.has_observation_table("_jazz_observation_evidence_v1")? {
            return Err(unobserved("accepted local provenance is unavailable"));
        }
        let mut query = self.conn.prepare_cached(
            "SELECT e.proof,e.state_tx,e.content_tx,e.content,s.node,c.node \
             FROM _jazz_observation_evidence_v1 e \
             JOIN _jazz_nodes s ON s.n=e.state_node JOIN _jazz_nodes c ON c.n=e.content_node \
             JOIN _jazz_transactions st ON st.n=e.state_node AND st.tx=e.state_tx AND st.t=e.state \
             JOIN _jazz_transactions ct ON ct.n=e.content_node AND ct.tx=e.content_tx AND ct.t=e.content \
             WHERE e.schema=?1 AND e.tbl=?2 AND e.id=?3 AND e.state=?4 AND e.scope=?5 AND e.admission<=?6 \
             ORDER BY e.admission LIMIT 2"
        ).map_err(storage)?;
        let mut results = query
            .query(rusqlite::params![
                cut.schema.as_slice(),
                table,
                row.0.as_bytes().as_slice(),
                state as i64,
                cut.digest.as_slice(),
                cut.admission
            ])
            .map_err(storage)?;
        let Some(result) = results.next().map_err(storage)? else {
            return Err(unobserved(
                "exact accepted provenance was not admitted at this cut",
            ));
        };
        let proof = match result.get::<_, i64>(0).map_err(storage)? {
            0 => LocalEvidenceProvenance::LocalSubmission,
            1 => LocalEvidenceProvenance::AuthorityRead,
            _ => return Err(unobserved("unknown observation proof kind")),
        };
        let content = result.get::<_, i64>(3).map_err(storage)?;
        if content <= 0 || content as u64 > state || state > MAX_SEQ {
            return Err(unobserved("invalid accepted coordinates"));
        }
        let origin = |column, tx_column, sequence| -> Result<crate::db::AcceptedVersion, Error> {
            let bytes: Vec<u8> = result.get(column).map_err(storage)?;
            let node = bytes
                .try_into()
                .map_err(|_| unobserved("invalid accepted node identity"))?;
            Ok(crate::db::AcceptedVersion {
                transaction: TxId::new(
                    TxTime(result.get::<_, i64>(tx_column).map_err(storage)? as u64),
                    crate::ids::NodeUuid::from_bytes(node),
                ),
                sequence,
            })
        };
        let pair = crate::db::AcceptedRowEvidence {
            row,
            state: origin(4, 1, state)?,
            content: origin(5, 2, content as u64)?,
        };
        let state_image = self
            .version_image(table, row, &[], Pos::at(state))?
            .ok_or_else(|| unobserved("exact accepted state is no longer held"))?;
        let content_image = self
            .version_image(table, row, &[], Pos::at(content as u64))?
            .ok_or_else(|| unobserved("exact accepted content is no longer held"))?;
        if state_image.written.is_some()
            || content_image.written.is_some()
            || !content_image.content
            || !observation_content_matches(&state_image, &content_image)
        {
            return Err(unobserved("accepted pairing no longer matches held bodies"));
        }
        Ok((pair, proof))
    }
}

const SCOPE_DOMAIN: &[u8] = b"jazz.sqlite.observation-scope.v1\0";
const MAX_SCOPE_BYTES: usize = 1 << 20;

/// Explicit scope codec; it does not depend on serde enum order. Engine-only
/// large/record/enum carriers are not JSON session claims and fail observation
/// admission closed. Ordinary writes with such claims retain their contract.
fn scope_preimage(scope: &ObservationScope) -> Option<Vec<u8>> {
    fn count(out: &mut Vec<u8>, n: usize) -> Option<()> {
        out.extend_from_slice(&u32::try_from(n).ok()?.to_le_bytes());
        Some(())
    }
    fn bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Option<()> {
        if out.len().checked_add(bytes.len())? > MAX_SCOPE_BYTES {
            return None;
        }
        count(out, bytes.len())?;
        out.extend_from_slice(bytes);
        Some(())
    }
    fn value(out: &mut Vec<u8>, v: &Value, depth: usize) -> Option<()> {
        if depth > 64 || out.len() > MAX_SCOPE_BYTES {
            return None;
        }
        match v {
            Value::Nullable(None) => out.push(0),
            Value::Bool(v) => out.extend_from_slice(&[1, u8::from(*v)]),
            Value::U8(v) => out.extend_from_slice(&[2, *v]),
            Value::U16(v) => {
                out.push(3);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::U32(v) => {
                out.push(4);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::U64(v) => {
                out.push(5);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::I32(v) => {
                out.push(6);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::I64(v) => {
                out.push(7);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::F64(v) => {
                out.push(8);
                out.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            Value::String(v) => {
                out.push(9);
                bytes(out, v.as_bytes())?;
            }
            Value::Bytes(v) => {
                out.push(10);
                bytes(out, v)?;
            }
            Value::Uuid(v) => {
                out.push(11);
                out.extend_from_slice(v.as_bytes());
            }
            Value::Tuple(values) | Value::Array(values) => {
                out.push(if matches!(v, Value::Tuple(_)) { 12 } else { 13 });
                count(out, values.len())?;
                for v in values {
                    value(out, v, depth + 1)?;
                }
            }
            Value::Nullable(Some(v)) => {
                out.push(14);
                value(out, v, depth + 1)?;
            }
            Value::EnumTag(v) => out.extend_from_slice(&[15, *v]),
            Value::U48(v) if *v < (1 << 48) => {
                out.push(16);
                out.extend_from_slice(&v.to_le_bytes()[..6]);
            }
            Value::U48(_) | Value::Large(_) | Value::Record(_) | Value::Enum(_) => return None,
        }
        (out.len() <= MAX_SCOPE_BYTES).then_some(())
    }
    let mut out = SCOPE_DOMAIN.to_vec();
    bytes(&mut out, scope.author.canonical().as_bytes())?;
    count(&mut out, scope.claims.len())?;
    for (key, v) in &scope.claims {
        bytes(&mut out, key.as_bytes())?;
        value(&mut out, v, 0)?;
    }
    Some(out)
}

fn scope_digest(scope: &ObservationScope) -> Option<[u8; 32]> {
    Some(jazz_types::sha256::derive_key(
        "jazz.sqlite.observation-scope.v1",
        &scope_preimage(scope)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // These lower-level tests pin the durable hash preimage independently of
    // runtime queries, which deliberately cannot expose private claim hashes.
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Alice's scalar and nested claims have an independently assembled byte
    /// contract, including exact full-u64 values and negative-zero float bits.
    #[test]
    fn scope_v1_independent_preimages_and_hashes() {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/observation_scope_v1.json"))
                .unwrap();
        let values = vec![
            Value::Nullable(None),
            Value::Bool(true),
            Value::U8(u8::MAX),
            Value::U16(u16::MAX),
            Value::U32(u32::MAX),
            Value::U64(u64::MAX),
            Value::I32(i32::MIN),
            Value::I64(i64::MIN),
            Value::F64(-0.0),
            Value::String("é".into()),
            Value::Bytes(vec![255, 0]),
            Value::Uuid(uuid::Uuid::from_bytes([17; 16])),
            Value::Tuple(vec![]),
            Value::Array(vec![]),
            Value::Nullable(Some(Box::new(Value::U8(1)))),
            Value::EnumTag(7),
            Value::U48((1 << 48) - 1),
        ];
        for (name, claims) in [
            ("empty", BTreeMap::new()),
            (
                "values",
                BTreeMap::from([("a".into(), Value::Array(values))]),
            ),
        ] {
            let scope = ObservationScope {
                author: AuthorSubject::SYSTEM,
                claims,
            };
            assert_eq!(
                hex(&scope_preimage(&scope).unwrap()),
                fixtures[name]["preimage_hex"]
            );
            assert_eq!(
                hex(&scope_digest(&scope).unwrap()),
                fixtures[name]["sha256_hex"]
            );
        }
    }

    /// Alice's scope cannot alias Bob's, another value width or a null/Some
    /// distinction. Claim insertion order alone does not change provenance.
    #[test]
    fn scope_v1_distinguishes_identity_types_and_preserves_sorted_keys() {
        let scope = |author, value| ObservationScope {
            author,
            claims: BTreeMap::from([("claim".into(), value)]),
        };
        let alice = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let bob = AuthorSubject::authenticated("https://issuer.example", "bob").unwrap();
        let baseline = scope_digest(&scope(alice, Value::U8(1))).unwrap();
        for changed in [
            scope(bob, Value::U8(1)),
            scope(alice, Value::U64(1)),
            scope(alice, Value::Nullable(Some(Box::new(Value::U8(1))))),
            scope(alice, Value::Nullable(None)),
        ] {
            assert_ne!(Some(baseline), scope_digest(&changed));
        }
        let mut a = scope(alice, Value::U8(1));
        a.claims.insert("z".into(), Value::Bool(true));
        let mut b = scope(alice, Value::U8(1));
        b.claims = a
            .claims
            .iter()
            .rev()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(scope_digest(&a), scope_digest(&b));
    }

    /// Alice's unsupported or malformed engine-only claim cannot fabricate a
    /// digest; capture can omit it without changing her ordinary commit.
    #[test]
    fn scope_v1_refuses_unsupported_or_unbounded_claims() {
        let mut deep = Value::Bool(true);
        for _ in 0..66 {
            deep = Value::Array(vec![deep]);
        }
        for value in [
            Value::U48(1 << 48),
            Value::Bytes(vec![0; MAX_SCOPE_BYTES]),
            deep,
        ] {
            assert!(
                scope_digest(&ObservationScope {
                    author: AuthorSubject::SYSTEM,
                    claims: BTreeMap::from([("unsupported".into(), value)]),
                })
                .is_none()
            );
        }
    }
    fn owner() -> Db {
        use jazz_model::model::public_schema::{
            ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
        };
        let schema = crate::schema::JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("body", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::True)
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build(),
        )
        .unwrap();
        block_on(Db::open_history_complete(DbConfig::new(
            schema,
            DbIdentity {
                node: crate::ids::NodeUuid::from_bytes([21; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap()
    }

    /// Alice's already-held accepted row becomes observable only after evidence
    /// admission. Private SQL inspection pins the transactional durable counter
    /// because the public cut intentionally does not expose admission numbers.
    #[test]
    fn late_admission_cannot_enter_an_older_cut_and_duplicates_keep_admission() {
        let db = owner();
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("held".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        let before = db.local_observation_cut(&query, None).unwrap();
        assert_eq!(before.admission, 0);
        db.retain_local_authority_read(&query, false, None).unwrap();
        assert_eq!(
            db.observe_local(&before, &query, false).unwrap_err().code,
            ErrorCode::NotObserved
        );
        let admitted = db.local_observation_cut(&query, None).unwrap();
        assert!(admitted.admission > 0);
        let rows = db.observe_local(&admitted, &query, false).unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(
            rows.provenance,
            vec![LocalEvidenceProvenance::AuthorityRead]
        );
        db.retain_local_authority_read(&query, false, None).unwrap();
        assert_eq!(
            db.local_observation_cut(&query, None).unwrap().admission,
            admitted.admission
        );
        db.set_identity_claims(
            AuthorSubject::SYSTEM,
            [("changed".into(), Value::Bool(true))].into(),
        );
        assert_eq!(
            db.observe_local(&admitted, &query, false).unwrap_err().code,
            ErrorCode::NotObserved
        );
    }

    /// Alice's authority evidence write fails midway through a two-row result.
    /// Inspect SQLite directly to prove neither the first row nor its allocated
    /// admission survives the savepoint; the next successful cut starts fresh.
    #[test]
    fn failed_evidence_write_rolls_back_rows_and_durable_admission() {
        let db = owner();
        for body in ["one", "two"] {
            block_on(db.insert(
                "notes",
                [("body".into(), Value::String(body.into()))].into(),
                Default::default(),
            ))
            .unwrap();
        }
        db.inner.borrow().conn.execute_batch(EVIDENCE_DDL).unwrap();
        db.inner.borrow().conn.execute_batch("CREATE TEMP TRIGGER fail_second BEFORE INSERT ON _jazz_observation_evidence_v1 WHEN (SELECT count(*) FROM _jazz_observation_evidence_v1)=1 BEGIN SELECT RAISE(ABORT,'second evidence fails'); END").unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        let before = db.local_observation_cut(&query, None).unwrap();
        assert!(db.retain_local_authority_read(&query, false, None).is_err());
        let after = db.local_observation_cut(&query, None).unwrap();
        assert_eq!(before.admission, after.admission);
        assert_eq!(
            db.inner
                .borrow()
                .conn
                .query_row(
                    "SELECT count(*) FROM _jazz_observation_evidence_v1",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        db.inner
            .borrow()
            .conn
            .execute_batch("DROP TRIGGER fail_second")
            .unwrap();
        db.retain_local_authority_read(&query, false, None).unwrap();
        let cut = db.local_observation_cut(&query, None).unwrap();
        assert_eq!(db.observe_local(&cut, &query, false).unwrap().rows.len(), 2);
        assert_eq!(cut.admission, 2);
    }

    /// Alice's admission proof cannot be swapped for another exact transaction,
    /// even at an identical body coordinate or under a different proof tag.
    /// This private test exercises the bounded metadata conflict seam directly;
    /// all initial bodies and paired origins come from a real authority read.
    #[test]
    fn conflicting_proof_kinds_refuse_without_upgrading_an_old_cut() {
        let db = owner();
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("held".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        let read = db
            .accepted_read(&query, None, false, Some(AuthorSubject::SYSTEM))
            .unwrap();
        let pair = read.evidence[0];
        // Exercise only the private cross-kind storage invariant: origins and
        // held bodies are genuine, but this fixture directly supplies the weak
        // proof tag rather than claiming an authority read was a Global Fate.
        let first = db.local_observation_cut(&query, None).unwrap();
        {
            let inner = db.inner();
            let table = inner.owner.table("notes").unwrap();
            inner
                .record_settled(
                    *pair.state.transaction.node.as_bytes(),
                    pair.state.transaction.time.0,
                    Some(Pos::at(pair.state.sequence)),
                )
                .unwrap();
            inner
                .admit_observation(
                    first.schema,
                    &table.stored,
                    first.digest,
                    LocalEvidenceProvenance::LocalSubmission,
                    &pair,
                )
                .unwrap();
        }
        let cut = db.local_observation_cut(&query, None).unwrap();
        let inner = db.inner();
        let table = inner.owner.table("notes").unwrap();
        let digest = cut.digest;
        let mut conflicting = pair;
        conflicting.content.transaction.time.0 = u64::MAX;
        // This deliberately conflicting forward origin must not pollute the
        // authority's later real paired read after the rejection assertion.
        begin_savepoint(&inner.conn, "conflicting_fixture").unwrap();
        inner
            .record_settled(
                *conflicting.content.transaction.node.as_bytes(),
                u64::MAX,
                Some(Pos::at(conflicting.content.sequence)),
            )
            .unwrap();
        assert_eq!(
            inner
                .admit_observation(
                    cut.schema,
                    &table.stored,
                    digest,
                    LocalEvidenceProvenance::AuthorityRead,
                    &conflicting
                )
                .unwrap_err()
                .code,
            ErrorCode::NotObserved
        );
        assert_eq!(inner.observation_watermarks().unwrap().0, cut.admission);
        end_savepoint(&inner.conn, "conflicting_fixture", false).unwrap();
        drop(inner);
        db.retain_local_authority_read(&query, false, None).unwrap();
        assert_eq!(
            db.observe_local(&cut, &query, false).unwrap().provenance,
            vec![LocalEvidenceProvenance::LocalSubmission]
        );
    }

    /// Alice's sidecars reject malformed lengths/tags/coordinates and preserve
    /// all u64 transaction bits. These SQL-level checks are needed because
    /// declaring BLOB(16) alone does not constrain SQLite values.
    #[test]
    fn storage_constraints_and_full_u64_reference_encoding() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE _jazz_pending(tx INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute_batch(SUBMISSION_DDL).unwrap();
        conn.execute_batch(EVIDENCE_DDL).unwrap();
        for (schema, scope) in [
            (vec![0u8; 15], vec![0u8; 32]),
            (vec![0u8; 16], vec![0u8; 31]),
        ] {
            assert!(
                conn.execute(
                    "INSERT INTO _jazz_observation_pending_v1 VALUES(1,0,?1,?2)",
                    rusqlite::params![schema, scope]
                )
                .is_err()
            );
        }
        for (state, content, proof) in [(0, 0, 0), (MAX_SEQ + 1, 1, 0), (1, 2, 0), (1, 1, 2)] {
            assert!(conn.execute("INSERT INTO _jazz_observation_evidence_v1(schema,tbl,id,state,scope,proof,state_node,state_tx,content_node,content_tx,content) VALUES(zeroblob(16),'notes',zeroblob(16),?1,zeroblob(32),?2,1,-1,1,-1,?3)", rusqlite::params![state as i64,proof,content as i64]).is_err());
        }
        for (index, tx) in [0, i64::MAX as u64, (i64::MAX as u64) + 1, u64::MAX]
            .into_iter()
            .enumerate()
        {
            conn.execute(
                "INSERT INTO _jazz_observation_pending_v1 VALUES(?1,?2,zeroblob(16),zeroblob(32))",
                rusqlite::params![tx as i64, index as i64],
            )
            .unwrap();
            let roundtrip: i64 = conn
                .query_row(
                    "SELECT tx FROM _jazz_observation_pending_v1 WHERE op=?1",
                    [index as i64],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(roundtrip as u64, tx);
        }
        conn.execute("INSERT INTO _jazz_pending VALUES(-1)", [])
            .unwrap();
        // This is the unchanged legacy writer's delete statement. A separately
        // executed old-writer/reopen probe is still required before publication.
        conn.execute("DELETE FROM _jazz_pending WHERE tx=-1", [])
            .unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM _jazz_observation_pending_v1 WHERE tx=-1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
    /// Alice's two real backend admissions may share names and claims while
    /// selecting different permission bundles. The private session Db is used
    /// to prove actual admission; public pinned_view selection is not session
    /// authentication and is not a substitute for this fixture.
    #[test]
    fn provenance_distinguishes_actual_selected_bundles_and_unknown_upstream() {
        use jazz_model::model::public_schema::{
            ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
        };
        let db = owner();
        let alice = AuthorSubject::for_test_bytes([7; 16]);
        let schema = |may_insert| {
            crate::schema::JazzSchema::new(
                &SchemaBuilder::new()
                    .table(
                        TableSchemaBuilder::new("notes")
                            .column("body", ColumnType::Text)
                            .policies(
                                TablePolicies::new()
                                    .with_select(PolicyExpr::True)
                                    .with_insert(if may_insert {
                                        PolicyExpr::True
                                    } else {
                                        PolicyExpr::False
                                    })
                                    .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                    .with_delete(PolicyExpr::True),
                            ),
                    )
                    .build(),
            )
            .unwrap()
        };
        let mut held_peers = Vec::new();
        let mut handles = Vec::new();
        for allow in [true, false] {
            let bundle = block_on(db.publish_bundle(schema(allow).public_schema())).unwrap();
            let (peer, transport) = duplex();
            let id = db
                .accept_pinned_subscriber(transport, alice, Some(BTreeMap::new()), Some(bundle))
                .unwrap();
            held_peers.push(peer);
            handles.push(db.session_db(id.slot));
        }
        let scope = ObservationScope {
            author: alice,
            claims: BTreeMap::new(),
        };
        let digests: Vec<_> = handles
            .iter()
            .map(|h| submission_scope(&scope, &h.governance, &h.inner()).unwrap())
            .collect();
        assert_ne!(digests[0], digests[1]);
        assert_ne!(
            digests[0],
            submission_scope(&scope, &db.governance, &db.inner()).unwrap()
        );
        // Becoming a replica says nothing about the upstream authenticated pin.
        // A known local Selected bundle must not turn UNKNOWN into that pin.
        db.become_replica();
        for (handle, known) in handles.iter().zip(digests) {
            assert_ne!(
                submission_scope(&scope, &handle.governance, &handle.inner()).unwrap(),
                known
            );
        }
    }

    /// Alice's old observation cannot cross a namespace replacement, and a
    /// fresh cut never uses an absent forward content origin or retained body.
    /// Private storage perturbations model sparse/corrupt history explicitly,
    /// without manufacturing a successful observation from those fixtures.
    #[test]
    fn namespace_and_sparse_forward_origins_fail_closed() {
        let db = owner();
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("held".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        let read = db.retain_local_authority_read(&query, false, None).unwrap();
        let cut = db.local_observation_cut(&query, None).unwrap();
        assert_eq!(db.observe_local(&cut, &query, false).unwrap().rows.len(), 1);
        db.inner.borrow().persist_namespace([42; 16]).unwrap();
        assert_eq!(
            db.observe_local(&cut, &query, false).unwrap_err().code,
            ErrorCode::NotObserved
        );
        db.inner.borrow().persist_namespace(cut.namespace).unwrap();
        let version = read.evidence[0].content;
        db.inner
            .borrow()
            .conn
            .execute(
                "DELETE FROM _jazz_transactions WHERE tx=?1",
                [version.transaction.time.0 as i64],
            )
            .unwrap();
        assert_eq!(
            db.observe_local(&cut, &query, false).unwrap_err().code,
            ErrorCode::NotObserved
        );
    }

    /// Alice's accepted body is still checked against current owner SELECT
    /// policies. The stored proof never freezes an authorization verdict.
    #[test]
    fn current_policy_revocation_does_not_replay_observed_rows() {
        use jazz_model::model::public_schema::{
            ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
        };
        let db = owner();
        let alice = AuthorSubject::for_test_bytes([8; 16]);
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("held".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        db.retain_local_authority_read(&query, false, Some(alice))
            .unwrap();
        let cut = db.local_observation_cut(&query, Some(alice)).unwrap();
        assert_eq!(db.observe_local(&cut, &query, false).unwrap().rows.len(), 1);
        let denied = crate::schema::JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("body", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::False)
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build(),
        )
        .unwrap();
        block_on(db.activate_schema(denied)).unwrap();
        match db.observe_local(&cut, &query, false) {
            Ok(observation) => assert!(observation.rows.is_empty()),
            Err(error) => assert_eq!(error.code, ErrorCode::NotObserved),
        }
    }
    /// Alice's authority context and unknown upstream context cannot alias,
    /// even when the local owner/schema/claims or selected bundle are equal.
    /// These pure codec checks use independently assembled Python fixtures;
    /// actual pinned-session admission is covered separately above.
    #[test]
    fn governing_context_and_full_u64_forward_reference_fixtures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/observation_scope_v1.json"))
                .unwrap();
        let scope = ObservationScope {
            author: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        };
        for (name, bundle) in [("owner", None), ("selected", Some([51; 32]))] {
            for authority in [false, true] {
                let name = format!(
                    "{name}{}",
                    if authority {
                        "_authority"
                    } else {
                        "_unknown_upstream"
                    }
                );
                let input = submission_preimage(&scope, [34; 16], bundle, authority).unwrap();
                assert_eq!(
                    hex(&input),
                    fixture["submission_contexts"][&name]["preimage_hex"]
                );
                assert_eq!(
                    hex(&jazz_types::sha256::derive_key(
                        "jazz.sqlite.observation-submission-scope.v1",
                        &input
                    )),
                    fixture["submission_contexts"][&name]["sha256_hex"]
                );
            }
        }
        let db = owner();
        let inner = db.inner();
        let node = [61; 16];
        for fixture in fixture["forward_tx_ids"].as_array().unwrap() {
            let tx: u64 = fixture["u64"].as_str().unwrap().parse().unwrap();
            let stored: i64 = fixture["sqlite_i64"].as_str().unwrap().parse().unwrap();
            assert_eq!(hex(&tx.to_le_bytes()), fixture["little_endian_hex"]);
            inner.record_settled(node, tx, Some(Pos::at(17))).unwrap();
            let (actual_tx, actual_at): (i64,i64) = inner.conn.query_row(
                "SELECT t.tx,t.t FROM _jazz_transactions t JOIN _jazz_nodes n ON n.n=t.n WHERE n.node=?1 AND t.tx=?2",
                rusqlite::params![node.as_slice(),stored], |r|Ok((r.get(0)?,r.get(1)?)),
            ).unwrap();
            assert_eq!(actual_tx, stored);
            assert_eq!(actual_tx as u64, tx);
            assert_eq!(actual_at, 17);
        }
    }

    /// Alice reopens storage written against the independently specified v1
    /// schema. Pin every table/index/trigger token, beyond SQLite's permissive
    /// type names; formatting whitespace is not a durable semantic difference.
    #[test]
    fn independent_storage_ddl_and_trigger_fixture() {
        fn schema(ddl: &str) -> Vec<(String, String)> {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE _jazz_pending(tx INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute_batch(ddl).unwrap();
            let mut q = conn.prepare("SELECT name,sql FROM sqlite_master WHERE name LIKE '_jazz_observation_%' AND sql IS NOT NULL ORDER BY name").unwrap();
            q.query_map([], |r| {
                let sql: String = r.get(1)?;
                Ok((
                    r.get(0)?,
                    sql.chars().filter(|c| !c.is_ascii_whitespace()).collect(),
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
        }
        let actual = schema(&format!("{SUBMISSION_DDL}{EVIDENCE_DDL}"));
        let expected = schema(include_str!("../../../fixtures/local_observation_v1.sql"));
        assert_eq!(actual.len(), 4);
        assert_eq!(actual, expected);
    }
    /// Alice deletes and restores without replacement: accepted state moves
    /// while the exact content origin remains the earlier insert. Removing
    /// that exact retained content must refuse, never choose a sparse maximum.
    #[test]
    fn distinct_state_content_pair_requires_the_exact_retained_content() {
        let db = owner();
        let write = block_on(db.insert(
            "notes",
            [("body".into(), Value::String("original".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        let initial = db.retain_local_authority_read(&query, false, None).unwrap();
        block_on(db.delete("notes", write.row_uuid(), Default::default())).unwrap();
        let deleted = db.retain_local_authority_read(&query, true, None).unwrap();
        assert_eq!(deleted.evidence.len(), 1);
        assert_eq!(deleted.evidence[0].content, initial.evidence[0].content);
        assert!(deleted.evidence[0].state.sequence > deleted.evidence[0].content.sequence);
        block_on(db.restore("notes", write.row_uuid(), None, Default::default())).unwrap();
        let restored = db.retain_local_authority_read(&query, false, None).unwrap();
        assert_eq!(restored.evidence.len(), 1);
        assert_eq!(restored.evidence[0].content, initial.evidence[0].content);
        assert!(restored.evidence[0].state.sequence > deleted.evidence[0].state.sequence);
        let cut = db.local_observation_cut(&query, None).unwrap();
        let observed = db.observe_local(&cut, &query, false).unwrap();
        assert_eq!(observed.evidence, restored.evidence);
        assert_eq!(
            observed.rows[0].cell("body"),
            Some(Value::String("original".into()))
        );
        {
            let inner = db.inner();
            let table = inner.owner.table("notes").unwrap();
            begin_savepoint(&inner.conn, "mismatched_body_fixture").unwrap();
            inner
                .conn
                .execute(
                    &format!(
                        "UPDATE {} SET {}='different' WHERE id=?1",
                        quote(&table.stored),
                        quote(table.stored_name("body"))
                    ),
                    [write.row_uuid().0.as_bytes().as_slice()],
                )
                .unwrap();
        }
        assert_eq!(
            db.observe_local(&cut, &query, false).unwrap_err().code,
            ErrorCode::NotObserved
        );
        end_savepoint(&db.inner().conn, "mismatched_body_fixture", false).unwrap();
        let inner = db.inner();
        let table = &inner.owner.table("notes").unwrap().stored;
        inner
            .conn
            .execute(
                &format!(
                    "DELETE FROM {} WHERE id=?1 AND _clock=?2",
                    quote(&format!("{table}__h"))
                ),
                rusqlite::params![
                    write.row_uuid().0.as_bytes().as_slice(),
                    initial.evidence[0].content.sequence as i64
                ],
            )
            .unwrap();
        drop(inner);
        assert_eq!(
            db.observe_local(&cut, &query, false).unwrap_err().code,
            ErrorCode::NotObserved
        );
    }

    /// Alice retires an actual owner's local capability lifetime. Existing
    /// cuts and new paired admissions refuse; private sequence inspection
    /// proves retirement cannot allocate evidence for a later reopened owner.
    #[test]
    fn retired_owner_cannot_create_new_authority_observation_evidence() {
        let db = owner();
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("held".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        db.retain_local_authority_read(&query, false, None).unwrap();
        let cut = db.local_observation_cut(&query, None).unwrap();
        let before = db.inner().observation_watermarks().unwrap();
        db.retire_local_observation_capabilities();
        assert!(db.local_observation_cut(&query, None).is_err());
        assert_eq!(
            db.observe_local(&cut, &query, false).unwrap_err().code,
            ErrorCode::NotObserved
        );
        assert_eq!(
            db.retain_local_authority_read(&query, false, None)
                .unwrap_err()
                .code,
            ErrorCode::NotObserved
        );
        assert_eq!(db.inner().observation_watermarks().unwrap(), before);
    }

    /// Alice's empty local query is a partial empty candidate set, not a
    /// completeness proof. It neither allocates evidence nor freezes later
    /// accepted rows into the already-created coordinate/admission cut.
    #[test]
    fn empty_local_observation_allocates_no_evidence_and_keeps_its_cut() {
        let db = owner();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        let cut = db.local_observation_cut(&query, None).unwrap();
        let before = db.inner().observation_watermarks().unwrap();
        let empty = db.observe_local(&cut, &query, false).unwrap();
        assert!(empty.rows.is_empty() && empty.evidence.is_empty() && empty.provenance.is_empty());
        assert_eq!(db.inner().observation_watermarks().unwrap(), before);
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("later".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        db.retain_local_authority_read(&query, false, None).unwrap();
        assert!(
            db.observe_local(&cut, &query, false)
                .unwrap()
                .rows
                .is_empty()
        );
    }

    /// Alice's evidence cannot escape nested SQLite savepoints even without
    /// a callback transaction. Private access is needed to distinguish raw
    /// SQLite nesting from Inner::tx and inspect actual admission reuse.
    #[test]
    fn nested_savepoints_refuse_cuts_until_the_outer_admission_commits() {
        let db = owner();
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("accepted".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        let query = db
            .prepare_query(&crate::query::Query::from("notes"))
            .unwrap();
        let before = db.inner().observation_watermarks().unwrap();
        {
            let inner = db.inner();
            assert!(inner.tx.is_none());
            begin_savepoint(&inner.conn, "observation_outer").unwrap();
            begin_savepoint(&inner.conn, "observation_inner").unwrap();
        }
        db.retain_local_authority_read(&query, false, None).unwrap();
        let uncommitted = db.inner().observation_watermarks().unwrap().0;
        assert!(uncommitted > before.0);
        assert_eq!(
            db.local_observation_cut(&query, None)
                .err()
                .expect("nested capture refused")
                .code,
            ErrorCode::NotObserved
        );
        end_savepoint(&db.inner().conn, "observation_inner", true).unwrap();
        assert_eq!(
            db.local_observation_cut(&query, None)
                .err()
                .expect("outer capture refused")
                .code,
            ErrorCode::NotObserved
        );
        end_savepoint(&db.inner().conn, "observation_outer", false).unwrap();
        assert!(db.inner().conn.is_autocommit());
        assert_eq!(db.inner().observation_watermarks().unwrap(), before);
        db.retain_local_authority_read(&query, false, None).unwrap();
        let cut = db.local_observation_cut(&query, None).unwrap();
        assert_eq!(
            cut.admission, uncommitted,
            "rolled-back admission was reused"
        );
        assert_eq!(db.observe_local(&cut, &query, false).unwrap().rows.len(), 1);
    }
}

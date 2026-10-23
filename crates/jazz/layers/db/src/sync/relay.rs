//! Durable upload forwarding. A relay retains units; only the authority decides
//! their fate. Pending units never become shared, accepted policy evidence.
use super::*;
fn corrupt(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Storage, error.to_string())
}

// jazz.sqlite.relay-upload.v1: explicit magic/version followed by exactly one
// existing Commit wire message. The version pins the nested wire layout too.
const JOURNAL_PREFIX: &[u8] = b"JRU\x01";

fn journal_bytes(message: &SyncMessage) -> Vec<u8> {
    let mut bytes = JOURNAL_PREFIX.to_vec();
    bytes.extend(encode(message));
    bytes
}

fn decode_upload(bytes: &[u8], id: UploadId) -> Result<SyncMessage, Error> {
    let payload = bytes
        .strip_prefix(JOURNAL_PREFIX)
        .ok_or_else(|| corrupt("invalid relay upload encoding version"))?;
    let (message, trailing): (SyncMessage, _) =
        postcard::take_from_bytes(payload).map_err(corrupt)?;
    if !trailing.is_empty() || upload_id(&message) != Some(id) {
        return Err(corrupt("invalid relay upload identity or bytes"));
    }
    Ok(message)
}

pub(super) type UploadId = ([u8; 16], u64);

pub(super) struct RelayUpload {
    pub(super) order: u64,
    pub(super) schema: crate::ids::SchemaVersionId,
    pub(super) message: SyncMessage,
    pub(super) sent: bool,
    pub(super) locally_durable: bool,
}

pub(super) const DDL: &str = "\
CREATE TABLE IF NOT EXISTS _jazz_relay_upload_v1 (\
 ordinal INTEGER PRIMARY KEY AUTOINCREMENT, node BLOB NOT NULL CHECK(length(node)=16),\
 tx INTEGER NOT NULL CHECK(tx>0), source TEXT NOT NULL, schema_id BLOB NOT NULL CHECK(length(schema_id)=16),\
 request BLOB NOT NULL, commit_msg BLOB NOT NULL, pending INTEGER NOT NULL CHECK(pending IN(0,1)), UNIQUE(node,tx));\
CREATE INDEX IF NOT EXISTS _jazz_relay_pending_v1 ON _jazz_relay_upload_v1(ordinal) WHERE pending=1;";

pub(super) fn upload_id(message: &SyncMessage) -> Option<UploadId> {
    match message {
        SyncMessage::Commit { node, tx, .. } => Some((*node, *tx)),
        _ => None,
    }
}

pub(super) fn large_values(message: &SyncMessage) -> Vec<&jazz_types::large_values::LargeValueRef> {
    fn collect<'a>(value: &'a Value, out: &mut Vec<&'a jazz_types::large_values::LargeValueRef>) {
        match value {
            Value::Large(large) => out.push(large),
            Value::Nullable(Some(inner)) => collect(inner, out),
            _ => {}
        }
    }
    let mut out = Vec::new();
    if let SyncMessage::Commit { ops, .. } = message {
        for op in ops {
            let cells = match op {
                WireOp::Insert { cells, .. }
                | WireOp::Update { cells, .. }
                | WireOp::Upsert { cells, .. } => Some(cells),
                WireOp::Restore { cells, .. } => cells.as_ref(),
                _ => None,
            };
            for cell in cells.into_iter().flat_map(|cells| cells.values()) {
                collect(cell, &mut out);
            }
        }
    }
    out
}

impl Inner {
    pub(super) fn load_relay_uploads(&self) -> Result<BTreeMap<UploadId, RelayUpload>, Error> {
        let mut statement = self.conn.prepare("SELECT ordinal,node,tx,schema_id,commit_msg,source,request FROM _jazz_relay_upload_v1 WHERE pending=1 ORDER BY ordinal").map_err(storage)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                ))
            })
            .map_err(storage)?;
        let mut uploads = BTreeMap::new();
        for row in rows {
            let (order, node, tx, schema, bytes, source, request) = row.map_err(storage)?;
            let node: [u8; 16] = node
                .try_into()
                .map_err(|_| corrupt("invalid relay upload node"))?;
            let schema: [u8; 16] = schema
                .try_into()
                .map_err(|_| corrupt("invalid relay upload schema"))?;
            if order <= 0 || tx <= 0 || tx == i64::MAX {
                return Err(corrupt("invalid relay upload identity"));
            }
            let id = (node, tx as u64);
            let message = decode_upload(&bytes, id)?;
            let mut request = decode_upload(&request, id)?;
            let source = AuthorSubject::from_canonical(&source).map_err(corrupt)?;
            if let (
                SyncMessage::Commit {
                    author, session, ..
                },
                SyncMessage::Commit {
                    author: prior_author,
                    session: prior_session,
                    ..
                },
            ) = (&message, &mut request)
            {
                commit_who(
                    (source != AuthorSubject::SYSTEM).then_some(source),
                    prior_author.as_deref(),
                    prior_session.is_some(),
                )
                .map_err(corrupt)?;
                if source != AuthorSubject::SYSTEM
                    && (author.as_deref() != Some(source.canonical()) || session.is_none())
                {
                    return Err(corrupt(
                        "relay upload scope differs from its authenticated sender",
                    ));
                }
                commit_who(None, author.as_deref(), session.is_some()).map_err(corrupt)?;
                *prior_author = author.clone();
                *prior_session = session.clone();
            }
            if encode(&message) != encode(&request) || self.applied_fate(node, tx as u64)?.is_some()
            {
                return Err(corrupt(
                    "relay upload request or pending fate is inconsistent",
                ));
            }
            uploads.insert(
                (node, tx as u64),
                RelayUpload {
                    order: order as u64,
                    schema: crate::ids::SchemaVersionId::from_bytes(schema),
                    message,
                    sent: false,
                    locally_durable: false,
                },
            );
        }
        Ok(uploads)
    }

    pub(super) fn relay_pushes(&self, message: &SyncMessage) -> Result<Vec<SyncMessage>, Error> {
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        let mut size = 0;
        let mut seen = BTreeSet::new();
        for value in large_values(message) {
            for (request, bytes) in crate::large::relay_nodes(&self.conn, value)? {
                if !seen.insert(request.locator) {
                    continue;
                }
                size += bytes.len();
                batch.push(WireChunk {
                    locator: request.locator,
                    hash: request.object_hash,
                    bytes,
                });
                if size >= CHUNK_BATCH_BYTES {
                    batches.push(SyncMessage::PushChunks {
                        nodes: std::mem::take(&mut batch),
                    });
                    size = 0;
                }
            }
        }
        if !batch.is_empty() {
            batches.push(SyncMessage::PushChunks { nodes: batch });
        }
        Ok(batches)
    }

    /// The caller commits this with the authority fate and settled position.
    pub(super) fn finish_relay_upload(&self, id: UploadId) -> Result<(), Error> {
        self.conn
            .execute(
                "UPDATE _jazz_relay_upload_v1 SET pending=0 WHERE node=?1 AND tx=?2",
                rusqlite::params![id.0.as_slice(), id.1 as i64],
            )
            .map_err(storage)?;
        self.conn.execute("INSERT OR IGNORE INTO _jazz_lv_reclaim(root) SELECT root FROM _jazz_lv_relay_root_v1 WHERE node=?1 AND tx=?2",rusqlite::params![id.0.as_slice(),id.1 as i64]).map_err(storage)?;
        self.conn
            .execute(
                "DELETE FROM _jazz_lv_relay_root_v1 WHERE node=?1 AND tx=?2",
                rusqlite::params![id.0.as_slice(), id.1 as i64],
            )
            .map_err(storage)?;
        Ok(())
    }

    /// Only exact write coordinates are inspected; unrelated rows never enter
    /// a relay receipt's reconciliation work.
    fn relay_touched(&self, message: &SyncMessage) -> Result<Vec<RowKey>, Error> {
        let SyncMessage::Commit { ops, .. } = message else {
            return Err(corrupt("invalid relay unit"));
        };
        let mut rows = BTreeSet::new();
        for wire in ops {
            let op = from_wire(wire.clone())?;
            let Some(row) = op.row() else { continue };
            let table = self.owner.stored_table(op.table())?;
            for image in self.images_where(table, false, "id=?1", &[&row.as_bytes().to_vec()])? {
                rows.insert((table.to_owned(), row, image.key));
            }
        }
        Ok(rows.into_iter().collect())
    }
}

impl Db {
    pub(super) fn forward_upload(
        &self,
        index: usize,
        policy: Option<AuthorSubject>,
        request: SyncMessage,
    ) -> Result<(), Error> {
        let SyncMessage::Commit {
            node,
            tx,
            kind,
            ops,
            author,
            session,
            updated_at,
        } = request.clone()
        else {
            unreachable!()
        };
        if tx == 0 || tx >= i64::MAX as u64 {
            return Err(query_error("invalid relay transaction number"));
        }
        let who = commit_who(policy, author.as_deref(), session.is_some())?;
        let mut inner = self.inner();
        if inner.non_durable {
            return Err(unsupported(
                "upload forwarding from a non-durable foreground",
            ));
        }
        let version = inner.schema.jazz.version_id();
        if version != inner.owner.jazz.version_id()
            || inner
                .client
                .as_ref()
                .and_then(|client| client.upstream_schema)
                .is_some_and(|upstream| upstream != version)
        {
            return Err(unsupported(
                "relay uploads under a schema different from the relay upstream",
            ));
        }
        if node == *inner.identity.node.as_bytes()
            || inner
                .local_position(TxId::new(TxTime(tx), NodeUuid::from_bytes(node)))?
                .is_some()
        {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "downstream upload uses the relay owner's transaction identity",
            ));
        }
        let source = inner.sessions[index].identity.canonical().to_owned();
        // The authenticated connection, never caller-supplied session claims,
        // determines an ordinary sender's forwarded scope.
        let (author, session) = match who {
            Who::Session(author) => {
                let claims = if policy.is_some() {
                    inner.claims.get(&author).cloned().unwrap_or_default()
                } else {
                    session.unwrap_or_default()
                };
                (Some(author.canonical().to_owned()), Some(claims))
            }
            Who::Attributed(author) => (Some(author.canonical().to_owned()), None),
            Who::Local => (None, None),
        };
        let message = SyncMessage::Commit {
            node,
            tx,
            kind,
            ops,
            author,
            session,
            updated_at,
        };
        // Reject unsupported coordinates before journalling, so a rejected Move
        // or unknown table cannot later poison receipt reconciliation.
        if let SyncMessage::Commit { ops, .. } = &message {
            for op in ops {
                let op = from_wire(op.clone())?;
                inner.owner.stored_table(op.table())?;
            }
        }
        let bytes = journal_bytes(&request);
        let existing:Option<(i64,String,Vec<u8>,Vec<u8>)> = inner.conn.query_row("SELECT pending,source,schema_id,request FROM _jazz_relay_upload_v1 WHERE node=?1 AND tx=?2",rusqlite::params![node.as_slice(),tx as i64],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional().map_err(storage)?;
        if let Some((pending, owner, schema, prior)) = existing {
            if owner != source || schema.as_slice() != version.as_bytes() || prior != bytes {
                return Err(Error::new(
                    ErrorCode::WriteRejected,
                    "relay transaction identity is already bound to another sender or unit",
                ));
            }
            if pending == 0 && inner.applied_fate(node, tx)?.is_none() {
                return Err(corrupt(
                    "completed relay upload is missing its authority fate",
                ));
            }
        } else {
            inner
                .conn
                .execute_batch("SAVEPOINT relay_upload")
                .map_err(storage)?;
            let persisted = (|| {
                inner.conn.execute("INSERT INTO _jazz_relay_upload_v1(node,tx,source,schema_id,request,commit_msg,pending) VALUES(?1,?2,?3,?4,?5,?6,1)",rusqlite::params![node.as_slice(),tx as i64,source,version.as_bytes().as_slice(),bytes,journal_bytes(&message)]).map_err(storage)?;
                let order = inner.conn.last_insert_rowid() as u64;
                for value in large_values(&message) {
                    inner.conn.execute("INSERT OR IGNORE INTO _jazz_lv_relay_root_v1(node,tx,root) VALUES(?1,?2,?3)",rusqlite::params![node.as_slice(),tx as i64,value.root.locator.as_bytes().as_slice()]).map_err(storage)?;
                }
                Ok::<_, Error>(order)
            })();
            let order = match persisted {
                Ok(order) => {
                    inner
                        .conn
                        .execute_batch("RELEASE relay_upload")
                        .map_err(storage)?;
                    order
                }
                Err(error) => {
                    inner.rollback_savepoint("relay_upload").map_err(storage)?;
                    return Err(error);
                }
            };
            inner.client.as_mut().expect("relay").relay_uploads.insert(
                (node, tx),
                RelayUpload {
                    order,
                    schema: version,
                    message,
                    sent: false,
                    locally_durable: false,
                },
            );
        }
        inner.sessions[index].relay_uploads.insert((node, tx));
        Ok(())
    }

    pub(super) fn progress_relay_uploads(&self) -> Result<(), Error> {
        let mut inner = self.inner();
        for index in 0..inner.sessions.len() {
            if !inner.sessions[index].open {
                continue;
            }
            let uploads = inner.sessions[index]
                .relay_uploads
                .iter()
                .copied()
                .collect::<Vec<_>>();
            for (node, tx) in uploads {
                let Some(result) = inner.applied_fate(node, tx)? else {
                    continue;
                };
                let through = match (result.is_ok(), inner.settled(node, tx)?) {
                    (true, Some(Some(through))) if through < LOCAL_BASE => through,
                    (false, Some(None)) => inner
                        .client
                        .as_ref()
                        .map_or(inner.seq, |client| client.known),
                    _ => {
                        return Err(corrupt(
                            "relay authority fate has no matching settled position",
                        ));
                    }
                };
                let bytes: Vec<u8> = inner
                    .conn
                    .query_row(
                        "SELECT commit_msg FROM _jazz_relay_upload_v1 WHERE node=?1 AND tx=?2",
                        rusqlite::params![node.as_slice(), tx as i64],
                        |row| row.get(0),
                    )
                    .map_err(storage)?;
                let message = decode_upload(&bytes, (node, tx))?;
                let rows = inner.relay_touched(&message)?;
                let session = &mut inner.sessions[index];
                if result.is_ok() {
                    session.committed.extend(
                        rows.iter()
                            .map(|(table, row, key)| (table.clone(), *row, key.clone(), through)),
                    );
                }
                session.touched.extend(rows);
                session.outbox.push(SyncMessage::FateV1 {
                    node,
                    tx,
                    through,
                    result,
                });
                session.relay_uploads.remove(&(node, tx));
                session.owner_acked.remove(&(node, tx));
            }
        }
        Ok(())
    }
}

/// An opaque set of complete relay uploads captured before a storage flush.
/// Confirming it after the flush cannot acknowledge later arrivals or another owner.
pub struct OwnerDurabilityBarrier {
    owner: std::rc::Weak<std::cell::RefCell<Inner>>,
    uploads: Vec<UploadId>,
}

impl Db {
    /// Require an explicit backing-store flush before acknowledging relay storage.
    /// Async storage hosts call this before accepting any peer traffic.
    pub fn require_owner_durability_flush(&self) {
        self.inner().deferred_owner_durability = true;
    }

    /// Capture complete, still-pending uploads for the next backing-store barrier.
    /// Incomplete uploads wait for ordinary chunk delivery and cannot receive a
    /// local durability claim.
    pub fn prepare_owner_durability(&self) -> Result<Option<OwnerDurabilityBarrier>, Error> {
        let inner = self.inner();
        if inner.retired {
            return Err(Error::new(ErrorCode::Storage, "storage owner is retired"));
        }
        let Some(client) = &inner.client else {
            return Ok(None);
        };
        if inner.non_durable {
            return Ok(None);
        }
        let mut uploads = Vec::new();
        for (id, upload) in &client.relay_uploads {
            if upload.locally_durable {
                continue;
            }
            let mut complete = true;
            for value in large_values(&upload.message) {
                if let Err(error) = crate::large::admit(&inner.conn, &inner.chunks, value) {
                    if error.code == ErrorCode::Storage {
                        return Err(error);
                    }
                    // Authority validation still decides the unit's fate. Local
                    // retention is never asserted for missing or malformed data.
                    complete = false;
                    break;
                }
            }
            if complete {
                uploads.push(*id);
            }
        }
        Ok((!uploads.is_empty()).then(|| OwnerDurabilityBarrier {
            owner: Rc::downgrade(&self.inner),
            uploads,
        }))
    }

    /// Confirm exactly the captured uploads after successful backing-store flush.
    /// A failed flush must never call this method. Retirement invalidates tokens.
    pub fn confirm_owner_durability(&self, barrier: OwnerDurabilityBarrier) -> Result<(), Error> {
        if !barrier.owner.ptr_eq(&Rc::downgrade(&self.inner)) {
            return Err(Error::new(
                ErrorCode::Storage,
                "durability barrier belongs to another owner",
            ));
        }
        let mut inner = self.inner();
        if inner.retired {
            return Err(Error::new(
                ErrorCode::Storage,
                "storage owner retired during flush",
            ));
        }
        if inner.non_durable {
            return Err(Error::new(
                ErrorCode::Storage,
                "foreground cannot confirm owner durability",
            ));
        }
        if let Some(client) = &mut inner.client {
            for id in barrier.uploads {
                if let Some(upload) = client.relay_uploads.get_mut(&id) {
                    upload.locally_durable = true;
                }
            }
        }
        Ok(())
    }

    /// Ship newly confirmed storage receipts through normal session output.
    pub fn flush_owner_durability(&self) -> Result<(), Error> {
        self.server_tick()
    }

    pub(super) fn progress_owner_durability(&self) {
        let mut inner = self.inner();
        let Some(client) = &inner.client else { return };
        let stored: BTreeSet<_> = client
            .relay_uploads
            .iter()
            .filter_map(|(id, upload)| upload.locally_durable.then_some(*id))
            .collect();
        for session in &mut inner.sessions {
            if !session.open {
                continue;
            }
            for &(node, tx) in &session.relay_uploads {
                if stored.contains(&(node, tx)) && session.owner_acked.insert((node, tx)) {
                    session.outbox.push(SyncMessage::OwnerStoredV1 { node, tx });
                }
            }
        }
    }
}

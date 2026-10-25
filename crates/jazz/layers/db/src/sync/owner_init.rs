//! Foreground initialization reservations live at the private durable owner.
use super::*;
use crate::db::{InitializationSeal, InitializationTransactionStatus, ReservedTxId};

// jazz.sqlite.owner-initialization.v1. The nonce is a publication capability;
// the public ReservedTxId alone cannot publish or replace a reserved unit.
pub(in crate::db) const DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_owner_initialization_v1 (node BLOB NOT NULL CHECK(length(node)=16), tx INTEGER NOT NULL CHECK(tx>0), nonce BLOB NOT NULL UNIQUE CHECK(length(nonce)=16), subject TEXT NOT NULL, schema_id BLOB NOT NULL CHECK(length(schema_id)=16), state INTEGER NOT NULL DEFAULT 0 CHECK(state IN(0,1,2)), PRIMARY KEY(node,tx)) WITHOUT ROWID; CREATE INDEX IF NOT EXISTS _jazz_owner_initialization_pending_v1 ON _jazz_owner_initialization_v1(node,tx) WHERE state=1;";

pub(super) struct ReservationAnswer {
    nonce: [u8; 16],
    node: [u8; 16],
    minimum: u64,
    result: Option<Result<ReservedTxId, Error>>,
    resolved: bool,
    abandoned: bool,
}
#[derive(Clone)]
pub(super) struct OwnerReservation {
    pub(super) nonce: [u8; 16],
    pub(super) origin: UploadId,
    pub(super) durable: bool,
}
struct ReservationWait {
    inner: Weak<RefCell<Inner>>,
    id: u64,
}
impl Drop for ReservationWait {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut inner = inner.borrow_mut();
            if let Some(client) = &mut inner.client
                && let Some(answer) = client.initialization_reservations.get_mut(&self.id)
            {
                answer.abandoned = true;
                if answer.resolved {
                    client.initialization_reservations.remove(&self.id);
                }
            }
        }
    }
}

impl Db {
    /// Freeze a foreground batch and wait until its private owner's real storage
    /// barrier has retained a non-reusable reservation. Drop/cancel publishes no
    /// rows. Durable standalone owners keep the existing synchronous primitive.
    pub async fn seal_initialization_transaction_with_owner(
        &self,
        open: OpenTransactionId,
    ) -> Result<InitializationSeal, Error> {
        self.inner().require_mutation_admission()?;
        if !self.inner().non_durable {
            return self.seal_initialization_transaction(open);
        }
        let (batch, id, nonce, connection) = {
            let mut inner = self.inner();
            inner.require_foreground_writes("initialization reservations")?;
            if inner.tx.is_some() {
                return Err(rejected(
                    "initialization seal inside an active callback transaction",
                ));
            }
            let batch = inner
                .batches
                .get(&open)
                .ok_or_else(|| rejected("initialization transaction is not open"))?;
            if !matches!(batch.kind, BatchKind::Serializable { .. }) {
                return Err(rejected(
                    "initialization requires a serializable transaction",
                ));
            }
            batch.reads_state.borrow().check_commit()?;
            match batch.identity {
                WriteIdentity::Database => {}
                WriteIdentity::Session(author) if author == inner.identity.author => {}
                _ => {
                    return Err(rejected(
                        "initialization requires the admitted owner's own identity",
                    ));
                }
            }
            let node = *inner.identity.node.as_bytes();
            let minimum = inner
                .seq
                .checked_add(1)
                .filter(|n| *n < i64::MAX as u64)
                .ok_or_else(|| query_error("initialization identity range exhausted"))?;
            let client = inner.client.as_mut().expect("foreground");
            if client.initialization_reservations.len() >= 64 {
                return Err(query_error(
                    "at most 64 initialization reservations may be outstanding",
                ));
            }
            let id = client.next_id;
            client.next_id += 1;
            let nonce = *OpenTransactionId::new().as_bytes();
            client.initialization_reservations.insert(
                id,
                ReservationAnswer {
                    nonce,
                    node,
                    minimum,
                    result: None,
                    resolved: false,
                    abandoned: false,
                },
            );
            client
                .outbox
                .push(SyncMessage::OwnerInitializationReserveV1 {
                    id,
                    nonce,
                    node,
                    minimum,
                });
            let connection = client.connection;
            let batch = inner.batches.remove(&open).expect("checked batch");
            inner.retired_initializations.insert(open);
            (batch, id, nonce, connection)
        };
        let _wait = ReservationWait {
            inner: Rc::downgrade(&self.inner),
            id,
        };
        self.schedule_pending_tick();
        let deadline = self
            .read_deadline(Some(Instant::now() + std::time::Duration::from_secs(15)))
            .expect("deadline");
        let reservation = poll_fn(|_| {
            let mut inner = self.inner();
            if let Err(error) = inner.require_mutation_admission() {
                return Poll::Ready(Err(error));
            }
            if let Err(error) = inner.require_foreground_writes("initialization reservations") {
                return Poll::Ready(Err(error));
            }
            let client = inner.client.as_mut().expect("foreground");
            if client.connection != connection || *deadline <= Instant::now() {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::NotObserved,
                    "initialization reservation lost its owner connection or timed out",
                )));
            }
            match client
                .initialization_reservations
                .get_mut(&id)
                .and_then(|a| a.result.take())
            {
                Some(result) => Poll::Ready(result),
                None => Poll::Pending,
            }
        })
        .await?;
        Ok(InitializationSeal {
            owner: Rc::downgrade(&self.inner),
            reservation,
            batch,
            owner_nonce: Some(nonce),
        })
    }

    pub(super) fn reserve_owner_initialization(
        &self,
        index: usize,
        id: u64,
        nonce: [u8; 16],
        node: [u8; 16],
        minimum: u64,
    ) -> Result<(), Error> {
        let mut inner = self.inner();
        if !inner.scope_isolated_owner || inner.non_durable || inner.retired {
            return Err(unsupported(
                "initialization reservation without an admitted private durable owner",
            ));
        }
        inner.require_owner_subject(inner.sessions[index].identity)?;
        let version = inner.owner.jazz.version_id();
        if inner.sessions[index].version != Some(version)
            || inner.schema.jazz.version_id() != version
        {
            return Err(unsupported(
                "initialization reservation under another schema",
            ));
        }
        if node == *inner.identity.node.as_bytes()
            || node == [0; 16]
            || nonce == [0; 16]
            || minimum < LOCAL_BASE
            || minimum >= i64::MAX as u64
        {
            return Err(query_error("invalid foreground initialization reservation"));
        }
        if inner.sessions[index].initialization_reservations.len() >= 64
            || inner.sessions[index]
                .initialization_reservations
                .contains_key(&id)
        {
            return Err(query_error(
                "too many or duplicate initialization reservation requests",
            ));
        }
        let source = inner.identity.author.canonical().to_owned();
        let previous: Option<(Vec<u8>, i64, String, Vec<u8>)> = inner.conn.query_row(
            "SELECT node,tx,subject,schema_id FROM _jazz_owner_initialization_v1 WHERE nonce=?1", [nonce.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        ).optional().map_err(storage)?;
        let tx = if let Some((previous_node, tx, subject, schema)) = previous {
            if previous_node != node
                || tx < minimum as i64
                || subject != source
                || schema != version.as_bytes()
            {
                return Err(rejected(
                    "initialization reservation capability belongs to another request",
                ));
            }
            tx as u64
        } else {
            // Reservations use the original writer's namespace, not the
            // owner's lower publication range. Both lookups hit (node, tx)
            // indexes; retained reservations prevent reuse after cancellation.
            let reserved: Option<i64> = inner.conn.query_row(
                "SELECT tx FROM _jazz_owner_initialization_v1 WHERE node=?1 ORDER BY tx DESC LIMIT 1", [node.as_slice()], |r|r.get(0),
            ).optional().map_err(storage)?;
            let uploaded: Option<i64> = inner
                .conn
                .query_row(
                    "SELECT tx FROM _jazz_relay_upload_v1 WHERE node=?1 ORDER BY tx DESC LIMIT 1",
                    [node.as_slice()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(storage)?;
            let floor = reserved.into_iter().chain(uploaded).max().unwrap_or(0);
            if floor < 0 {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "invalid initialization allocator floor",
                ));
            }
            let tx = (floor as u64)
                .checked_add(1)
                .map(|n| n.max(minimum))
                .filter(|n| *n < i64::MAX as u64)
                .ok_or_else(|| query_error("initialization identity range exhausted"))?;
            // The retained row is the allocator floor and capability together.
            // No publication, upload forwarding or transaction fate happens here.
            inner
                .conn
                .execute_batch("SAVEPOINT owner_initialization")
                .map_err(storage)?;
            let result = (|| {
                inner.conn.execute("INSERT INTO _jazz_owner_initialization_v1(node,tx,nonce,subject,schema_id) VALUES(?1,?2,?3,?4,?5)", rusqlite::params![node.as_slice(),tx as i64,nonce.as_slice(),source,version.as_bytes().as_slice()]).map_err(storage)?;
                Ok::<_, Error>(())
            })();
            if let Err(error) = result {
                inner
                    .rollback_savepoint("owner_initialization")
                    .map_err(storage)?;
                return Err(error);
            }
            inner
                .conn
                .execute_batch("RELEASE owner_initialization")
                .map_err(storage)?;
            tx
        };
        inner.sessions[index].initialization_reservations.insert(
            id,
            OwnerReservation {
                nonce,
                origin: (node, tx),
                durable: false,
            },
        );
        Ok(())
    }

    pub(super) fn publish_owner_initialization(
        &self,
        index: usize,
        policy: Option<AuthorSubject>,
        nonce: [u8; 16],
        bytes: &[u8],
    ) -> Result<(), Error> {
        let (message, rest): (SyncMessage, _) =
            postcard::take_from_bytes(bytes).map_err(|e| query_error(e.to_string()))?;
        let Some((node, tx)) = relay::upload_id(&message).filter(|_| rest.is_empty()) else {
            return Err(query_error(
                "initialization publication requires exactly one normal Commit",
            ));
        };
        if let SyncMessage::Commit {
            kind: WireKind::Mergeable,
            ..
        } = message
        {
            return Err(rejected(
                "initialization publication requires a serializable unit",
            ));
        }
        if let Err(error) = self.forward_upload(index, policy, message, Some(nonce)) {
            let mut inner = self.inner();
            if inner.owner_projection_error.is_some() {
                return Err(error);
            }
            let through = inner.client.as_ref().map_or(0, |c| c.known);
            inner.sessions[index].outbox.push(SyncMessage::FateV1 {
                node,
                tx,
                through,
                result: Err(wire_error(&error)),
            });
        }
        Ok(())
    }
}

impl Inner {
    pub(in crate::db) fn require_initialization_allocator(&self) -> Result<(), Error> {
        if self.client.as_ref().is_some_and(|client| {
            client
                .initialization_reservations
                .values()
                .any(|r| !r.resolved)
        }) {
            return Err(unsupported(
                "committing foreground writes while an initialization reservation awaits its durable owner identity",
            ));
        }
        Ok(())
    }

    pub(super) fn resend_initialization_reservations(&mut self) {
        let Some(client) = &mut self.client else {
            return;
        };
        client.outbox.retain(|m| {
            !matches!(
                m,
                SyncMessage::OwnerInitializationReserveV1 { .. }
                    | SyncMessage::OwnerInitializationPublishV1 { .. }
            )
        });
        for (&id, answer) in &client.initialization_reservations {
            if !answer.resolved {
                client
                    .outbox
                    .push(SyncMessage::OwnerInitializationReserveV1 {
                        id,
                        nonce: answer.nonce,
                        node: answer.node,
                        minimum: answer.minimum,
                    });
            }
        }
    }

    pub(super) fn check_owner_initialization_recovery(&self) -> Result<(), Error> {
        let mut statement = self
            .conn
            .prepare("SELECT node,tx,subject FROM _jazz_owner_initialization_v1 WHERE state=1")
            .map_err(storage)?;
        let entries = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(storage)?;
        for entry in entries {
            let (node, tx, subject) = entry.map_err(storage)?;
            if subject != self.identity.author.canonical() || tx <= 0 {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "pending owner initialization is misbound",
                ));
            }
            let node: [u8; 16] = node
                .try_into()
                .map_err(|_| Error::new(ErrorCode::Storage, "invalid owner initialization node"))?;
            let unit = self
                .client
                .as_ref()
                .and_then(|c| c.relay_uploads.get(&(node, tx as u64)));
            let Some(unit) = unit else {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "pending owner initialization lacks its exact upload",
                ));
            };
            if matches!(
                unit.message,
                SyncMessage::Commit {
                    kind: WireKind::Mergeable,
                    ..
                }
            ) || !self.upload_is_complete(&unit.message)?
            {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "pending owner initialization upload is incomplete",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn receive_initialization_reservation(
        &mut self,
        id: u64,
        nonce: [u8; 16],
        result: Result<UploadId, WireError>,
    ) -> Result<(), Error> {
        self.require_foreground_writes("initialization reservation replies")?;
        if !self.non_durable {
            return Err(query_error(
                "initialization reply without a foreground request",
            ));
        }
        let client = self.client.as_ref().expect("foreground");
        let Some(answer) = client.initialization_reservations.get(&id) else {
            return Ok(());
        };
        if nonce != answer.nonce {
            return Err(query_error("initialization reservation nonce mismatch"));
        }
        let result = match result {
            Ok((node, tx))
                if node == answer.node && tx >= answer.minimum && tx < i64::MAX as u64 =>
            {
                Ok(ReservedTxId(TxId::new(
                    TxTime(tx),
                    NodeUuid::from_bytes(node),
                )))
            }
            Ok(_) => return Err(query_error("initialization reservation identity mismatch")),
            Err(error) => Err(from_wire_error(error)),
        };
        if let Ok(reservation) = &result {
            // Advance before handing the seal to its caller, even if that
            // caller cancelled. Until this reply, ordinary allocations are
            // fenced: the owner's retained floor may exceed our local one.
            self.record_issued_seq(reservation.0.time.0)?;
            self.seq = self.seq.max(reservation.0.time.0);
        }
        let client = self.client.as_mut().expect("foreground");
        let answer = client
            .initialization_reservations
            .get_mut(&id)
            .expect("request");
        answer.resolved = result.is_ok();
        answer.result = Some(result);
        if answer.abandoned && answer.resolved {
            client.initialization_reservations.remove(&id);
        }
        Ok(())
    }

    pub(super) fn check_owner_initialization_upload(
        &self,
        index: usize,
        (node, tx): UploadId,
        nonce: Option<[u8; 16]>,
    ) -> Result<(), Error> {
        let stored: Option<(Vec<u8>, String, Vec<u8>)> = self.conn.query_row(
            "SELECT nonce,subject,schema_id FROM _jazz_owner_initialization_v1 WHERE node=?1 AND tx=?2", rusqlite::params![node.as_slice(), tx as i64],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
        ).optional().map_err(storage)?;
        match (stored, nonce) {
            (None, None) => Ok(()),
            (Some((stored, subject, schema)), Some(nonce))
                if self.scope_isolated_owner
                    && stored == nonce
                    && subject == self.sessions[index].identity.canonical()
                    && schema == self.owner.jazz.version_id().as_bytes() =>
            {
                Ok(())
            }
            _ => Err(rejected(
                "initialization upload lacks its exact owner reservation capability",
            )),
        }
    }

    pub(in crate::db) fn owner_initialization_status(
        &self,
        id: ReservedTxId,
    ) -> Result<InitializationTransactionStatus, Error> {
        if !self.scope_isolated_owner {
            return Ok(InitializationTransactionStatus::NotObserved);
        }
        self.require_owner_subject(self.identity.author)?;
        let node = id.0.node.as_bytes();
        let tx = id.0.time.0 as i64;
        let state: Option<i64> = self.conn.query_row("SELECT state FROM _jazz_owner_initialization_v1 WHERE node=?1 AND tx=?2 AND subject=?3", rusqlite::params![node.as_slice(),tx,self.identity.author.canonical()], |r|r.get(0)).optional().map_err(storage)?;
        let state = match state {
            None | Some(0) => return Ok(InitializationTransactionStatus::NotObserved),
            Some(state @ (1 | 2)) => state,
            _ => {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "invalid initialization state",
                ));
            }
        };
        let recorded: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT source,pending FROM _jazz_relay_upload_v1 WHERE node=?1 AND tx=?2",
                rusqlite::params![node.as_slice(), tx],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        let Some((subject, pending)) = recorded else {
            return Ok(InitializationTransactionStatus::Incomplete);
        };
        if (state == 1) != (pending == 1) {
            return Err(Error::new(
                ErrorCode::Storage,
                "initialization state disagrees with its upload",
            ));
        }
        if subject != self.identity.author.canonical() {
            return Err(Error::new(
                ErrorCode::Storage,
                "initialization unit belongs to another owner",
            ));
        }
        let Some(position) = self.local_position(id.0)? else {
            return Ok(InitializationTransactionStatus::Incomplete);
        };
        if pending != 0 {
            let Some(upload) = self
                .client
                .as_ref()
                .and_then(|c| c.relay_uploads.get(&(*node, id.0.time.0)))
            else {
                return Ok(InitializationTransactionStatus::Incomplete);
            };
            if matches!(
                upload.message,
                SyncMessage::Commit {
                    kind: WireKind::Mergeable,
                    ..
                }
            ) || !self.upload_is_complete(&upload.message)?
            {
                return Ok(InitializationTransactionStatus::Incomplete);
            }
        } else if self.settled(*node, id.0.time.0)?.is_none() {
            return Err(Error::new(
                ErrorCode::Storage,
                "terminal initialization lacks its authority receipt",
            ));
        }
        self.write_state(position)
            .map(InitializationTransactionStatus::Complete)
    }
}

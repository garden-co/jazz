//! An initialization seal freezes a staged serializable transaction, not its
//! owner. Reserved identities and publication positions are separate: other
//! commits may land while the host durably journals the reservation.
use super::*;
use crate::ids::NodeUuid;
use crate::tx::TxId;
use jazz_types::time::TxTime;

/// `jazz.sqlite.local-commit.v1`: the durable origin and admitted author of a
/// local publication. Reservations alone have no row in this table.
pub(super) const DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_local_commits_v1 (seq INTEGER PRIMARY KEY CHECK(seq>0), node BLOB NOT NULL CHECK(length(node)=16), tx INTEGER NOT NULL CHECK(tx>0), author TEXT NOT NULL, initialization INTEGER NOT NULL CHECK(initialization IN (0,1)), pending INTEGER NOT NULL CHECK(pending IN (0,1)), UNIQUE(node,tx), UNIQUE(tx)); CREATE INDEX IF NOT EXISTS _jazz_initialization_pending_v1 ON _jazz_local_commits_v1(seq) WHERE initialization=1 AND pending=1";

/// Private journal linkage. Knowing these bytes is not a publication capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReservedTxId(pub(super) TxId);
impl ReservedTxId {
    /// `jazz.sqlite.initialization-reservation.v1`, canonical UUID and decimal u64.
    pub fn encode(self) -> String {
        format!("jazz-sqlite-init-v1:{}:{}", self.0.node.0, self.0.time.0)
    }
    pub fn decode(value: &str) -> Result<Self, Error> {
        let invalid = || {
            Error::new(
                ErrorCode::Protocol,
                "invalid SQLite initialization reservation",
            )
        };
        let mut fields = value.split(':');
        if fields.next() != Some("jazz-sqlite-init-v1") {
            return Err(invalid());
        }
        let node = fields
            .next()
            .ok_or_else(invalid)?
            .parse::<uuid::Uuid>()
            .map_err(|_| invalid())?;
        let seq = fields
            .next()
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        if fields.next().is_some() || seq == 0 || seq >= i64::MAX as u64 {
            return Err(invalid());
        }
        let id = Self(TxId::new(TxTime(seq), NodeUuid(node)));
        if id.encode() != value {
            return Err(invalid());
        }
        Ok(id)
    }
    pub fn transaction_id(self) -> TxId {
        self.0
    }
}

/// Owner-bound, single-use capability. The frozen batch is no longer reachable
/// by its old open id. Dropping or cancelling a seal publishes nothing and drops
/// its staged upload leases. Its durable allocator reservation is never reused.
pub struct InitializationSeal {
    pub(super) owner: Weak<RefCell<Inner>>,
    pub(super) reservation: ReservedTxId,
    pub(super) batch: Batch,
    pub(super) owner_nonce: Option<[u8; 16]>,
}
impl InitializationSeal {
    pub fn reservation(&self) -> ReservedTxId {
        self.reservation
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitializationTransactionStatus {
    NotObserved,
    Incomplete,
    Complete(WriteState),
}

impl Inner {
    /// Origin for a local publication position. Old ordinary SQLite positions
    /// retain the previous identity convention until explicitly recorded.
    pub(super) fn local_origin(&self, seq: u64) -> Result<TxId, Error> {
        let record: Option<(Vec<u8>, i64)> = self
            .conn
            .query_row(
                "SELECT node,tx FROM _jazz_local_commits_v1 WHERE seq=?1 UNION ALL SELECT node,tx FROM _jazz_owner_positions_v1 WHERE seq=?1",
                [seq as i64],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        match record {
            Some((node, tx)) => {
                if tx <= 0 {
                    return Err(Error::new(
                        ErrorCode::Storage,
                        "invalid local commit identity",
                    ));
                }
                let node = uuid::Uuid::from_slice(&node)
                    .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
                Ok(TxId::new(TxTime(tx as u64), NodeUuid(node)))
            }
            None => Ok(TxId::new(TxTime(seq), self.identity.node)),
        }
    }
    pub(super) fn record_local_commit(
        &self,
        seq: u64,
        reservation: Option<ReservedTxId>,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        let id = reservation.map_or_else(|| TxId::new(TxTime(seq), self.identity.node), |r| r.0);
        self.conn.execute(
            "INSERT INTO _jazz_local_commits_v1(seq,node,tx,author,initialization,pending) VALUES(?1,?2,?3,?4,?5,?6)",
            rusqlite::params![seq as i64,id.node.as_bytes().as_slice(),id.time.0 as i64,author.canonical(),reservation.is_some(),reservation.is_some() && self.client.is_some()],
        ).map_err(storage)?;
        Ok(())
    }
    pub(super) fn local_position(&self, origin: TxId) -> Result<Option<u64>, Error> {
        let value: Option<i64> = self
            .conn
            .query_row(
                "SELECT seq FROM _jazz_local_commits_v1 WHERE node=?1 AND tx=?2 UNION ALL SELECT seq FROM _jazz_owner_positions_v1 WHERE node=?1 AND tx=?2",
                rusqlite::params![origin.node.as_bytes().as_slice(), origin.time.0 as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        Ok(value.map(|v| v as u64))
    }
    fn initialization_author(&self, batch: &Batch) -> Result<AuthorSubject, Error> {
        match batch.identity {
            WriteIdentity::Database => Ok(self.identity.author),
            WriteIdentity::Session(author) if author == self.identity.author => Ok(author),
            _ => Err(rejected(
                "initialization requires the admitted owner's own identity",
            )),
        }
    }
}

impl Db {
    /// Stable transaction identity behind a local publication position.
    pub fn committed_transaction_id(&self, seq: u64) -> Result<TxId, Error> {
        self.inner().local_origin(seq)
    }

    /// Freeze a prepared serializable batch after its preparation/read queue has
    /// drained. No row is published and no upload is queued by this operation.
    pub fn seal_initialization_transaction(
        &self,
        open: OpenTransactionId,
    ) -> Result<InitializationSeal, Error> {
        let inner = self.inner();
        inner.require_mutation_admission()?;
        if inner.non_durable {
            return Err(unsupported(
                "initialization seals on a non-durable foreground",
            ));
        }
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
        inner.initialization_author(batch)?;
        drop(inner);
        self.admit_initialization_replica()?;
        let mut inner = self.inner();
        let next = inner.seq.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorCode::Storage,
                "initialization identity range exhausted",
            )
        })?;
        inner.record_issued_seq(next)?;
        inner.seq = next;
        let reservation = ReservedTxId(TxId::new(TxTime(next), inner.identity.node));
        let batch = inner.batches.remove(&open).expect("checked batch");
        inner.retired_initializations.insert(open);
        Ok(InitializationSeal {
            owner: Rc::downgrade(&self.inner),
            reservation,
            batch,
            owner_nonce: None,
        })
    }

    pub fn publish_initialization_transaction(
        &self,
        seal: InitializationSeal,
    ) -> Result<TxReceipt, Error> {
        self.inner().require_mutation_admission()?;
        self.check_initialization_owner(&seal)?;
        let id = (
            *seal.reservation.0.node.as_bytes(),
            seal.reservation.0.time.0,
        );
        if let Some(nonce) = seal.owner_nonce {
            let mut inner = self.inner();
            inner.require_foreground_writes("initialization publication")?;
            inner
                .client
                .as_mut()
                .expect("foreground")
                .initialization_uploads
                .insert(id, nonce);
        }
        let result = self.commit_serializable_batch(&seal.batch, Some(seal.reservation));
        if result.is_err() && seal.owner_nonce.is_some() {
            self.inner()
                .client
                .as_mut()
                .expect("foreground")
                .initialization_uploads
                .remove(&id);
        }
        result
    }
    pub fn cancel_initialization_transaction(&self, seal: InitializationSeal) -> Result<(), Error> {
        self.check_initialization_owner(&seal)
    }
    fn check_initialization_owner(&self, seal: &InitializationSeal) -> Result<(), Error> {
        if !Weak::ptr_eq(&seal.owner, &Rc::downgrade(&self.inner)) {
            return Err(rejected(
                "initialization seal belongs to another runtime owner",
            ));
        }
        Ok(())
    }

    /// Record exact provisional absence, including policy-hidden/deleted rows.
    /// This never claims remote coverage: the ordinary serializable read set and
    /// INSERT validation still run at the authority when the commit uploads.
    pub fn prepare_initialization_insert(
        &self,
        open: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        let inner = self.inner();
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
        inner.initialization_author(batch)?;
        inner.require_catalogue_schema(&inner.schema)?;
        let stored = inner.schema.stored_table(table)?;
        if inner.newest_seq(stored, row)?.is_some()
            || batch.ops.iter().any(|(schema, op)| {
                op.row() == Some(row) && schema.stored_table(op.table()).ok() == Some(stored)
            })
        {
            return Err(conflict(
                "initialization insert coordinate already exists or is staged",
            ));
        }
        drop(inner);
        self.admit_initialization_replica()?;
        let query = Query::from(table).filter(crate::query::eq(
            crate::query::col("id"),
            crate::query::lit(Value::Uuid(row.0)),
        ));
        let prepared = self.prepare_query(&query)?;
        self.record_read(open, &prepared, true)
    }

    /// Exact, bounded recovery lookup scoped to this admitted owner. A foreign
    /// author and an absent reservation are deliberately indistinguishable.
    pub fn initialization_transaction_status(
        &self,
        ids: &[ReservedTxId],
    ) -> Result<Vec<InitializationTransactionStatus>, Error> {
        if ids.len() > 64 {
            return Err(Error::new(
                ErrorCode::Protocol,
                "initialization status accepts at most 64 identities",
            ));
        }
        let inner = self.inner();
        if inner.non_durable {
            return Err(unsupported(
                "initialization status without an admitted durable owner",
            ));
        }
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            let position:Option<i64>=inner.conn.query_row(
                "SELECT seq FROM _jazz_local_commits_v1 WHERE node=?1 AND tx=?2 AND author=?3 AND initialization=1",
                rusqlite::params![id.0.node.as_bytes().as_slice(),id.0.time.0 as i64,inner.identity.author.canonical()],|r|r.get(0),
            ).optional().map_err(storage)?;
            results.push(match position {
                None => inner.owner_initialization_status(*id)?,
                Some(seq) => inner.initialization_publication_status(seq as u64)?,
            });
        }
        Ok(results)
    }
}

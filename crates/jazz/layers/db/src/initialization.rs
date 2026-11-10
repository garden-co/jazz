//! Owner-bound frozen serializable publications. Reservation identity is not
//! publication position, accepted evidence, or a Global receipt.
use super::*;
use crate::ids::NodeUuid;
use crate::tx::TxId;
use jazz_types::time::TxTime;

/// Private durable-journal coordinate, never a publication capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReservedTxId(TxId);

impl ReservedTxId {
    /// `jazz.sqlite.initialization-reservation.v1`: canonical UUID and decimal u64.
    pub fn encode(self) -> String {
        format!("jazz-sqlite-init-v1:{}:{}", self.0.node.0, self.0.time.0)
    }

    /// Decode only canonical reservation metadata; this does not grant a seal.
    pub fn decode(value: &str) -> Result<Self, Error> {
        let invalid = || {
            Error::new(
                ErrorCode::Protocol,
                "invalid SQLite initialization reservation",
            )
        };
        let mut parts = value.split(':');
        if parts.next() != Some("jazz-sqlite-init-v1") {
            return Err(invalid());
        }
        let node = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<uuid::Uuid>()
            .map_err(|_| invalid())?;
        let time = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        if parts.next().is_some() || time == 0 || time >= i64::MAX as u64 {
            return Err(invalid());
        }
        let id = Self(TxId::new(TxTime(time), NodeUuid(node)));
        if id.encode() != value {
            return Err(invalid());
        }
        Ok(id)
    }

    /// The exact original identity, independent of its publication position.
    pub fn transaction_id(self) -> TxId {
        self.0
    }
}

/// Single-use, process-owner capability. The original mutable open handle is
/// retired. Dropping releases staged upload leases and publishes no rows.
#[must_use = "publish, cancel or drop this single-use frozen batch"]
pub struct InitializationSeal {
    open: OpenTransactionId,
    owner: Weak<Store>,
    reservation: ReservedTxId,
    batch: Batch,
    view: Option<Rc<Schema>>,
    governance: Governance,
    author: AuthorSubject,
}

/// Current SQLite outcomes preserve exact typed errors rather than guessing a
/// legacy model rejection category.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitializationFate {
    /// Locally published, with no genuine authority decision yet.
    Pending,
    /// Genuinely accepted by the authority.
    Accepted,
    /// Exact current refusal, without a guessed legacy rejection category.
    Rejected {
        /// Stable current engine code.
        code: ErrorCode,
        /// Stable finer reason when present.
        reason: Option<Reason>,
        /// Actual refusal detail.
        message: String,
    },
}
/// Owner-scoped publication and actual authority outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitializationWriteState {
    /// Original reserved identity.
    pub transaction_id: TxId,
    /// Position of the publication in this physical owner's local order.
    pub publication_position: u64,
    /// Actual current outcome.
    pub fate: InitializationFate,
    /// Observed durability; local pending never means Global.
    pub durability: DurabilityTier,
    /// Real authority position only after genuine Global acceptance.
    pub global_position: Option<u64>,
}
/// Bounded exact-owner status; reservation possession alone grants no lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitializationTransactionStatus {
    /// Unpublished, unknown or not this authenticated principal's reservation.
    NotObserved,
    /// Missing original pending proof or terminal facts; never an empty proof.
    Incomplete,
    /// Complete locally retained publication/outcome facts.
    Complete(InitializationWriteState),
}

impl InitializationSeal {
    /// Metadata to journal before publishing this owner-local capability.
    pub fn reservation(&self) -> ReservedTxId {
        self.reservation
    }
}

// Optional, versioned allocator/publication metadata. It does not reinterpret
// _jazz_transactions: a NULL-t transaction there means rejection, not a reserve.
const ALLOCATION_MARKER: &str = "jazz.sqlite.initialization-allocation.v1";
const ALLOCATION_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_initialization_issued_v1(node BLOB PRIMARY KEY CHECK(length(node)=16),seq INTEGER NOT NULL CHECK(seq>0)) WITHOUT ROWID; CREATE TABLE IF NOT EXISTS _jazz_initialization_publications_v1(node BLOB NOT NULL CHECK(length(node)=16),tx INTEGER NOT NULL CHECK(tx>0),author TEXT NOT NULL,position INTEGER UNIQUE CHECK(position>0),initialization INTEGER NOT NULL CHECK(initialization IN(0,1)),terminal INTEGER NOT NULL DEFAULT 0 CHECK(terminal IN(0,1,2)),PRIMARY KEY(node,tx)) WITHOUT ROWID; CREATE INDEX IF NOT EXISTS _jazz_initialization_outstanding_v1 ON _jazz_initialization_publications_v1(position) WHERE terminal=0 AND position IS NOT NULL; CREATE TABLE IF NOT EXISTS _jazz_initialization_pending_v1(tx INTEGER PRIMARY KEY CHECK(tx>0),digest BLOB NOT NULL CHECK(length(digest)=32));";

pub(super) fn allocation_active(conn: &Connection) -> Result<bool, Error> {
    let marker: Option<Vec<u8>> = conn
        .query_row(
            "SELECT v FROM _jazz_meta WHERE k=?1",
            [ALLOCATION_MARKER],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    match marker {
        None => {
            let partial: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name IN ('_jazz_initialization_issued_v1','_jazz_initialization_publications_v1','_jazz_initialization_pending_v1'))",
                [], |row| row.get(0),
            ).map_err(storage)?;
            if partial {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "initialization allocation tables have no supported marker",
                ));
            }
            Ok(false)
        }
        // Pin the complete marker rather than using meta()'s permissive decode.
        Some(bytes) if bytes == [1] => {
            let count:i64 = conn.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('_jazz_initialization_issued_v1','_jazz_initialization_publications_v1','_jazz_initialization_pending_v1')",[],|row| row.get(0)).map_err(storage)?;
            if count != 3 {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "initialization journal is incomplete",
                ));
            }
            Ok(true)
        }
        Some(_) => Err(Error::new(
            ErrorCode::Storage,
            "unsupported initialization allocation marker",
        )),
    }
}

/// The optional allocator only extends this actual node's existing high water.
/// It supplies no accepted position, completed fate or row visibility.
pub(super) fn allocation_floor(conn: &Connection, node: NodeUuid) -> Result<u64, Error> {
    if !allocation_active(conn)? {
        return Ok(0);
    }
    let floor: Option<i64> = conn
        .query_row(
            "SELECT seq FROM _jazz_initialization_issued_v1 WHERE node=?1",
            [node.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    let position: Option<i64> = conn
        .query_row(
            "SELECT MAX(position) FROM _jazz_initialization_publications_v1",
            [],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if position.is_some_and(|position| position <= 0) {
        return Err(Error::new(
            ErrorCode::Storage,
            "invalid publication high water",
        ));
    }
    match floor {
        None => Ok(position.unwrap_or(0) as u64),
        Some(value) if value > 0 => Ok(value.max(position.unwrap_or(0)) as u64),
        Some(_) => Err(Error::new(
            ErrorCode::Storage,
            "invalid initialization allocation high water",
        )),
    }
}

impl Inner {
    fn initialization_author(&self, batch: &Batch) -> Result<AuthorSubject, Error> {
        let author = self.identity.author;
        if !matches!(author, AuthorSubject::Authenticated(_)) {
            return Err(rejected(
                "initialization requires its authenticated owner's identity",
            ));
        }
        let own = |identity| {
            matches!(identity, WriteIdentity::Database)
                || matches!(identity, WriteIdentity::Session(selected) if selected == author)
        };
        if batch.identity.is_some_and(|identity| !own(identity))
            || batch
                .ops
                .iter()
                .any(|(_, op)| op.identity().is_some_and(|identity| !own(identity)))
        {
            return Err(rejected(
                "initialization requires the admitted owner's own identity",
            ));
        }
        Ok(author)
    }

    fn activate_initialization(&mut self) -> Result<(), Error> {
        self.require_mutation_lifetime()?;
        if self.initialization_enabled {
            if !allocation_active(&self.conn)? {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "initialization allocation marker was lost",
                ));
            }
            return Ok(());
        }
        self.check_initialization_activation_prefix()?;
        // Enabling before the first private upload avoids inventing a complete
        // original proof for a legacy pending packet whose parts may be absent.
        begin_savepoint(&self.conn, "jazz_initialization_enable")?;
        let enabled: Result<(), Error> = (|| {
            if allocation_active(&self.conn)? {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "initialization role changed outside its owner",
                ));
            }
            self.conn.execute_batch(ALLOCATION_DDL).map_err(storage)?;
            self.conn
                .execute(
                    "INSERT INTO _jazz_meta(k,v) VALUES(?1,?2)",
                    rusqlite::params![ALLOCATION_MARKER, vec![1u8]],
                )
                .map_err(storage)?;
            Ok(())
        })();
        end_savepoint(&self.conn, "jazz_initialization_enable", enabled.is_ok())?;
        enabled?;
        self.initialization_enabled = true;
        if let Some(client) = self.client.as_mut() {
            client.origin_aware = true;
        }
        Ok(())
    }

    fn reserve_initialization(&mut self, author: AuthorSubject) -> Result<ReservedTxId, Error> {
        self.activate_initialization()?;
        let next = self
            .seq
            .checked_add(1)
            .filter(|next| *next < i64::MAX as u64)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Storage,
                    "initialization identity range exhausted",
                )
            })?;
        let id = ReservedTxId(TxId::new(TxTime(next), self.identity.node));
        begin_savepoint(&self.conn, "jazz_initialization_reserve")?;
        let result: Result<(), Error> = (|| {
            self.conn.execute(
                "INSERT INTO _jazz_initialization_issued_v1(node,seq) VALUES(?1,?2) ON CONFLICT(node) DO UPDATE SET seq=MAX(seq,excluded.seq)",
                rusqlite::params![id.0.node.as_bytes().as_slice(),next as i64],
            ).map_err(storage)?;
            self.conn.execute(
                "INSERT INTO _jazz_initialization_publications_v1(node,tx,author,position,initialization) VALUES(?1,?2,?3,NULL,1)",
                rusqlite::params![id.0.node.as_bytes().as_slice(),next as i64,author.canonical()],
            ).map_err(storage)?;
            Ok(())
        })();
        end_savepoint(&self.conn, "jazz_initialization_reserve", result.is_ok())?;
        result?;
        self.seq = next;
        Ok(id)
    }

    pub(super) fn local_publication_origin(&self, position: u64) -> Result<TxId, Error> {
        if position == 0 || position >= i64::MAX as u64 {
            return Err(rejected("invalid local publication position"));
        }
        if !self.initialization_enabled {
            return Ok(TxId::new(TxTime(position), self.identity.node));
        }
        let active = allocation_active(&self.conn)?;
        if !active {
            return Err(Error::new(
                ErrorCode::Storage,
                "initialization allocation marker was lost",
            ));
        }
        if let Some(&(_, node, tx)) = self.own_order.get(&position) {
            return Ok(TxId::new(TxTime(tx), NodeUuid::from_bytes(node)));
        }
        if active {
            let origin: Option<(Vec<u8>, i64)> = self
                .conn
                .query_row(
                    "SELECT node,tx FROM _jazz_initialization_publications_v1 WHERE position=?1",
                    [position as i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(storage)?;
            if let Some((node, tx)) = origin {
                let node = uuid::Uuid::from_slice(&node).map_err(|_| {
                    Error::new(ErrorCode::Storage, "invalid local publication node")
                })?;
                if tx <= 0 {
                    return Err(Error::new(
                        ErrorCode::Storage,
                        "invalid local publication identity",
                    ));
                }
                return Ok(TxId::new(TxTime(tx as u64), NodeUuid(node)));
            }
        }
        if active {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "local publication has no retained original identity",
            ));
        }
        Ok(TxId::new(TxTime(position), self.identity.node))
    }

    /// Runs inside the ordinary commit's upload savepoint, before enqueueing
    /// in-memory pending state, so a metadata failure cannot strand a fake upload.
    pub(super) fn record_local_publication(
        &self,
        position: u64,
        original: TxId,
        reservation: Option<ReservedTxId>,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        if let Some(id) = reservation {
            if original != id.transaction_id() {
                return Err(rejected("initialization original identity changed"));
            }
            let changed = self.conn.execute(
                "UPDATE _jazz_initialization_publications_v1 SET position=?4 WHERE node=?1 AND tx=?2 AND author=?3 AND initialization=1 AND position IS NULL",
                rusqlite::params![id.0.node.as_bytes().as_slice(),id.0.time.0 as i64,author.canonical(),position as i64],
            ).map_err(storage)?;
            if changed != 1 {
                return Err(rejected(
                    "initialization reservation is not this owner's unpublished reservation",
                ));
            }
        } else if self.initialization_enabled {
            if !allocation_active(&self.conn)? {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "initialization allocation marker was lost",
                ));
            }
            self.conn.execute(
                "INSERT INTO _jazz_initialization_publications_v1(node,tx,author,position,initialization) VALUES(?1,?2,?3,?4,0)",
                rusqlite::params![original.node.as_bytes().as_slice(),original.time.0 as i64,author.canonical(),position as i64],
            ).map_err(storage)?;
        }
        Ok(())
    }
}

// On an operation error, restore the exact append-only upload suffix after
// confirmed SQL rollback. If rollback or release cannot be confirmed, fence
// the owner instead of allowing stale in-memory state to be reused.
struct PublicationGuard<'a> {
    db: &'a Db,
    active: bool,
    finalizing: bool,
    prior: u64,
    position: u64,
    origin: Option<(u64, [u8; 16], u64)>,
    changed: usize,
    upload: Option<sync::InitializationUploadCheckpoint>,
}
impl Drop for PublicationGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let rolled_back = end_savepoint(
            &self.db.inner.borrow().conn,
            "initialization_publish",
            false,
        )
        .is_ok();
        if rolled_back && !self.finalizing {
            let mut inner = self.db.inner();
            inner.seq = self.prior;
            inner.changed.borrow_mut().truncate(self.changed);
            match self.origin {
                Some(origin) => {
                    inner.own_order.insert(self.position, origin);
                }
                None => {
                    inner.own_order.remove(&self.position);
                }
            }
            if let Some(upload) = &self.upload {
                inner.rollback_initialization_upload(self.position, upload);
            }
        } else {
            let _ = self.db.retire_storage_owner();
            self.db.inner.borrow_mut().retired = true;
        }
    }
}

impl Db {
    pub(crate) fn owns_initialization_runtime(&self) -> bool {
        self.view.is_none()
    }
    pub(crate) fn ensure_initialization_owner_active(&self) -> Result<(), Error> {
        self.ensure_storage_owner_active()?;
        if self.inner.borrow().initialization_owner_retired {
            return Err(Error::new(
                ErrorCode::Storage,
                "initialization owner is closed",
            ));
        }
        Ok(())
    }

    pub(super) fn commit_initialization_ops(
        &self,
        batch: &Batch,
        kind: sync::Kind,
        reservation: ReservedTxId,
    ) -> Result<TxReceipt, Error> {
        let mut guard = {
            let inner = self.inner();
            inner.require_mutation_lifetime()?;
            if !inner.initialization_enabled
                || (!inner.authority && inner.client.is_none())
                || (inner.authority && (inner.client.is_some() || self.view.is_some()))
                || inner.tx.is_some()
                || inner.non_durable
            {
                return Err(rejected(
                    "initialization publication requires its admitted durable owner",
                ));
            }
            inner.initialization_author(batch)?;
            begin_savepoint(&inner.conn, "initialization_publish")?;
            let position = inner.next_seq();
            PublicationGuard {
                db: self,
                active: true,
                finalizing: false,
                prior: inner.seq,
                position,
                origin: inner.own_order.get(&position).copied(),
                changed: inner.changed.borrow().len(),
                upload: inner.initialization_upload_checkpoint(),
            }
        };
        let receipt = self.commit_ops_now(batch, kind, Some((reservation, guard.position)))?;
        guard.finalizing = true;
        end_savepoint(&self.inner.borrow().conn, "initialization_publish", true)?;
        guard.active = false;
        self.notify();
        Ok(receipt)
    }

    /// The exact original identity of a committed local publication.
    pub fn committed_transaction_id(&self, position: u64) -> Result<TxId, Error> {
        self.ensure_initialization_owner_active()?;
        self.inner().local_publication_origin(position)
    }

    /// Bounded owner-scoped status. Possessing reservation bytes does not grant
    /// access to another principal's rows, proof or terminal result.
    pub fn initialization_transaction_status(
        &self,
        ids: &[ReservedTxId],
    ) -> Result<Vec<InitializationTransactionStatus>, Error> {
        self.ensure_initialization_owner_active()?;
        if ids.len() > 64 {
            return Err(rejected(
                "initialization status accepts at most 64 exact identities",
            ));
        }
        let inner = self.inner();
        if !inner.initialization_enabled {
            return Ok(vec![
                InitializationTransactionStatus::NotObserved;
                ids.len()
            ]);
        }
        if !allocation_active(&inner.conn)? {
            return Err(Error::new(
                ErrorCode::Storage,
                "initialization journal marker was lost",
            ));
        }
        ids.iter().map(|id| {
            let publication:Option<(Option<i64>,i64)> = inner.conn.query_row("SELECT position,terminal FROM _jazz_initialization_publications_v1 WHERE node=?1 AND tx=?2 AND author=?3 AND initialization=1",rusqlite::params![id.0.node.as_bytes().as_slice(),id.0.time.0 as i64,inner.identity.author.canonical()],|row| Ok((row.get(0)?,row.get(1)?))).optional().map_err(storage)?;
            let Some((Some(position),terminal)) = publication else { return Ok(InitializationTransactionStatus::NotObserved); };
            if position<=0 { return Err(Error::new(ErrorCode::Storage,"invalid initialization publication position")); }
            let position = position as u64;
            if terminal==0 {
                if let Err(error) = inner.verify_initialization_pending(position) {
                    if error.code == ErrorCode::NotObserved { return Ok(InitializationTransactionStatus::Incomplete); }
                    return Err(error);
                }
                return Ok(InitializationTransactionStatus::Complete(InitializationWriteState { transaction_id:id.0,publication_position:position,fate:InitializationFate::Pending,durability:DurabilityTier::Local,global_position:None }));
            }
            inner.initialization_terminal_state(id.0,position,terminal)
        }).collect()
    }

    /// Consume a frozen batch under its captured view, governance and actual
    /// identity. The reservation is never obtained from caller-supplied bytes.
    pub fn publish_initialization_transaction(
        &self,
        seal: InitializationSeal,
    ) -> Result<TxReceipt, Error> {
        self.check_initialization_owner(&seal)?;
        let captured = Db {
            inner: self.inner.clone(),
            view: seal.view,
            governance: seal.governance,
        };
        captured.commit_serializable_batch(&seal.batch, seal.open, Some(seal.reservation))
    }

    /// Opt an authenticated physical owner into qualified journaling. Replica
    /// owners enable it before writes; real authorities retain their actual
    /// role and run the same validation. Legacy pending prefixes need genuine
    /// authority settlement first; their absent original proof is not invented.
    pub fn enable_initialization_journal(&self) -> Result<(), Error> {
        self.ensure_initialization_owner_active()?;
        if self.view.is_some() {
            return Err(rejected(
                "initialization journaling requires its physical owner",
            ));
        }
        self.admit_initialization_owner()?;
        let mut inner = self.inner();
        if !matches!(inner.identity.author, AuthorSubject::Authenticated(_)) {
            return Err(rejected(
                "initialization journaling requires its authenticated owner",
            ));
        }
        inner.activate_initialization()
    }

    /// Freeze this exact prepared batch, retaining its governing context. No
    /// row is published and no upload is queued until the seal is published.
    pub fn seal_initialization_transaction(
        &self,
        open: OpenTransactionId,
    ) -> Result<InitializationSeal, Error> {
        self.ensure_initialization_owner_active()?;
        self.admit_initialization_owner()?;
        let mut inner = self.inner();
        if inner.non_durable
            || (inner.authority && (inner.client.is_some() || self.view.is_some()))
            || inner.tx.is_some()
        {
            return Err(unsupported(
                "initialization sealing outside its durable owner's staged serializable transaction",
            ));
        }
        inner.require_cached_catalogue_schema(&inner.schema)?;
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
        Self::check_client_evidence_owner(&inner, batch)?;
        let author = inner.initialization_author(batch)?;
        for (schema, _) in &batch.ops {
            inner.require_cached_catalogue_schema(schema)?;
        }
        let reservation = inner.reserve_initialization(author)?;
        let batch = inner
            .batches
            .remove(&open)
            .expect("checked before reservation");
        inner.retired_initializations.insert(open);
        Ok(InitializationSeal {
            open,
            owner: Rc::downgrade(&self.inner),
            reservation,
            batch,
            view: self.view.clone(),
            governance: self.governance.clone(),
            author,
        })
    }

    fn check_initialization_owner(&self, seal: &InitializationSeal) -> Result<(), Error> {
        self.ensure_initialization_owner_active()?;
        if !Weak::ptr_eq(&seal.owner, &Rc::downgrade(&self.inner)) {
            return Err(rejected(
                "initialization seal belongs to another runtime owner",
            ));
        }
        let inner = self.inner();
        if (inner.authority && (inner.client.is_some() || self.view.is_some()))
            || inner.non_durable
            || (!inner.authority && inner.client.is_none())
            || inner.identity.author != seal.author
        {
            return Err(rejected(
                "initialization seal no longer has its admitted runtime owner",
            ));
        }
        inner.require_mutation_lifetime()?;
        inner.initialization_author(&seal.batch)?;
        Ok(())
    }

    /// Cancelling consumes the capability and releases its staged leases. The
    /// durably issued identity remains spent and can never be recycled here.
    pub fn cancel_initialization_transaction(&self, seal: InitializationSeal) -> Result<(), Error> {
        self.check_initialization_owner(&seal)
    }

    /// This local coordinate check is INSERT eligibility, not accepted absence
    /// or read authority. The ordinary original read set and write policies
    /// still decide the transaction when an authority receives it.
    pub fn prepare_initialization_insert(
        &self,
        open: OpenTransactionId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.ensure_initialization_owner_active()?;
        self.admit_initialization_owner()?;
        {
            let inner = self.inner();
            inner.require_cached_catalogue_schema(&inner.schema)?;
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
            Self::check_client_evidence_owner(&inner, batch)?;
            inner.initialization_author(batch)?;
            let stored = inner.schema.stored_table(table)?;
            if !inner.candidate_coordinate_absent(stored, row)?
                || batch.ops.iter().any(|(schema, op)| {
                    op.row() == Some(row) && schema.stored_table(op.table()).ok() == Some(stored)
                })
            {
                return Err(conflict(
                    "initialization insert coordinate already exists or is staged",
                ));
            }
        }
        self.inner().activate_initialization()?;
        let query = Query::from(table).filter(crate::query::eq(
            crate::query::col("id"),
            crate::query::lit(Value::Uuid(row.0)),
        ));
        let prepared = self.prepare_query(&query)?;
        // This is the actual same-batch read record; it stays ordinary and
        // include-deleted. No fabricated accepted evidence or external warmup.
        self.record_read(open, &prepared, true)
    }
}

impl Inner {
    pub(super) fn captured_initialization_origins(
        &self,
        own: &[u64],
    ) -> Result<Option<Vec<(u64, [u8; 16], u64)>>, Error> {
        if !self.initialization_enabled {
            return Ok(None);
        }
        if !allocation_active(&self.conn)? {
            return Err(Error::new(
                ErrorCode::Storage,
                "initialization allocation marker was lost",
            ));
        }
        if own.len() > sync::MAX_SERIALIZABLE_ORIGINS {
            return Err(unsupported(
                "serializable original dependency prefix exceeds 4096 commits",
            ));
        }
        let mut positions = BTreeSet::new();
        let mut origins = BTreeSet::new();
        let mut captured = Vec::with_capacity(own.len());
        for &position in own {
            let origin = self.local_publication_origin(position)?;
            let node = *origin.node.as_bytes();
            if !positions.insert(position) || !origins.insert((node, origin.time.0)) {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "ambiguous original dependency prefix",
                ));
            }
            captured.push((position, node, origin.time.0));
        }
        Ok(Some(captured))
    }
}

//! This device in its account (`E2EE_PROTOCOL.md` §2.1–§2.7, §2.12, §6.3a;
//! core hooks note H6, H8): the device request, the bootstrap of the
//! account's first device, and the verification of the account at each
//! read: replay of its public rows, the floor that refuses older evidence,
//! the private evidence and the account key.
//!
//! Every step reads the account's rows in one serializable transaction, at
//! one cut (`ledger_read`), so that the authority refuses a publication
//! whose evidence changed before it commits; a conflict reads again. A
//! publication waits until the authority accepts it.

mod devices;
pub(crate) mod rows;

use std::cell::Cell;
use std::collections::BTreeSet;

use jazz::db::{Db, ErrorCode, InsertOptions, SubscriptionStream};
use jazz::ids::RowUuid;
use jazz::query::{Operand, Predicate, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::OpenTransactionId;
use jazz::tx::DurabilityTier;
use jazz_e2ee::protocol::account::build::{
    ResetPolicy, RootAuthority, account_root, check_bootstrap, check_reset,
};
use jazz_e2ee::protocol::account::{
    AccountEpochKeys, AccountKey, AccountKeyEvidence, AccountReplay, AccountState,
    PrivateAccountLedger, PublicAccountLedger, verify_private,
};
use jazz_e2ee::protocol::records::account::{
    AccountRoot, DeviceRemovalRequest, DeviceRequest, PublicAccountSuccessor, PublicDeviceApproval,
    RecoveryRoot,
};
use jazz_e2ee::protocol::{Accepted, Position, ProtocolError};

pub use devices::{ApprovedBy, DeviceInfo, DeviceStatus, PendingApproval};

use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::state::Report;
use rows::{Row, uuid_cell};

/// Reads and publications that keep meeting concurrent writes give up
/// after this many attempts.
const ATTEMPTS: usize = 5;

/// Where this device stands in its account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccountStatus {
    /// The device is active, and holds the account key.
    Active,
    /// The device's request is published; another device of the account
    /// has to approve it (or the account is recovered on it).
    AwaitingApproval,
    /// The account no longer has this device (it was removed, or the
    /// account was reset): its private state was wiped and it enrolled
    /// again as a new device, which awaits approval.
    Reenrolled,
}

/// The account as this device last verified it: its state, the
/// authenticated account key and every account epoch key it reaches, and
/// the public rows the state was replayed from. Keys are cleared on drop
/// and never printed.
pub struct OwnAccount {
    pub state: AccountState,
    pub key: AccountKey,
    pub epoch_keys: AccountEpochKeys,
    pub public: PublicAccountLedger,
}

impl std::fmt::Debug for OwnAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnAccount")
            .field("account", &self.state.account_id)
            .field("epoch", &self.state.epoch_id())
            .finish_non_exhaustive()
    }
}

/// The account's rows of one read.
struct AccountRead {
    public: PublicAccountLedger,
    private: PrivateAccountLedger,
    /// Rows of the account that are not records of their tables: evidence
    /// all the same, so an account with any is not empty.
    skipped: usize,
}

/// Whether a publication failed because the rows it read changed before it
/// committed (§0 item 5): its read is out of date, and it reads again.
pub(crate) fn conflict(error: &jazz::db::Error) -> bool {
    error.code == ErrorCode::TransactionConflict
        || error.reason == Some(jazz::db::Reason::SerializationConflict)
}

/// Runs `step` in a serializable transaction of `db` and abandons its
/// handle afterwards; a conflict reads again, `attempts` times in all, and
/// the last one is the error. Every other outcome is returned as it is.
pub(crate) async fn serializable<T>(
    db: &Db,
    attempts: usize,
    mut step: impl AsyncFnMut(OpenTransactionId) -> Result<T, ClientError>,
) -> Result<T, ClientError> {
    let mut last = None;
    for _ in 0..attempts {
        let tx = OpenTransactionId::new();
        db.begin_serializable(tx).await?;
        let outcome = step(tx).await;
        let _ = db.abandon_transaction_handle(tx);
        match outcome {
            Err(ClientError::Database(error)) if conflict(&error) => {
                last = Some(ClientError::Database(error));
            }
            other => return other,
        }
    }
    Err(last.expect("an attempt was made"))
}

fn by_account(table: &str, account: &Value) -> Query {
    Query::from(table).filter(eq(col("accountId"), lit(account.clone())))
}

/// A device whose state this runtime keeps changing (re-enrolment) is read
/// once per operation.
impl E2eeClient {
    /// Reads `query` in transaction `tx`, as records with their positions;
    /// rows that are not records of their table are left out and reported.
    pub(crate) async fn read_rows<R: Row>(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        skipped: &Cell<usize>,
        query: Query,
    ) -> Result<(Vec<Accepted<R>>, u64), ClientError> {
        let prepared = db.prepare_query(&query)?;
        let read = db.tx_ref(tx).ledger_read(&prepared).await?;
        let mut rows = Vec::with_capacity(read.rows.len());
        for (row, at) in &read.rows {
            match R::decode(row) {
                Some(record) => rows.push(Accepted::new(Position(*at), record)),
                None => {
                    skipped.set(skipped.get() + 1);
                    self.shared.report(Report::SkippedRecord {
                        document: format!("{}/{}", R::TABLE, row.row_uuid().0),
                        reason: "a row that is not a record of its table".to_owned(),
                    });
                }
            }
        }
        Ok((rows, read.through))
    }

    /// The public rows of every account in `account_ids` at the cut of
    /// `tx`: one query per table over all of them (`accountId IN …`, core
    /// hooks note H6), however many accounts, and the device keys their
    /// rows name, besides `extra_devices` (given to every account).
    pub(crate) async fn read_public_many(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        skipped: &Cell<usize>,
        account_ids: &[String],
        extra_devices: BTreeSet<String>,
    ) -> Result<Vec<PublicAccountLedger>, ClientError> {
        let accounts: Vec<Operand> = account_ids
            .iter()
            .map(|id| {
                uuid_cell(id)
                    .map(Operand::Literal)
                    .ok_or(ProtocolError::InvalidRecord("the account ID is not a UUID"))
            })
            .collect::<Result<_, _>>()?;
        let of = |table: &str| {
            Query::from(table).filter(Predicate::In(
                Operand::Column("accountId".to_owned()),
                accounts.clone(),
            ))
        };
        let (roots, through) = self
            .read_rows::<AccountRoot>(db, tx, skipped, of(rows::ACCOUNT_ROOTS))
            .await?;
        let (approvals, _) = self
            .read_rows::<PublicDeviceApproval>(db, tx, skipped, of(rows::PUBLIC_DEVICE_APPROVALS))
            .await?;
        let (successors, _) = self
            .read_rows::<PublicAccountSuccessor>(
                db,
                tx,
                skipped,
                of(rows::PUBLIC_ACCOUNT_SUCCESSORS),
            )
            .await?;
        let (recovery_roots, _) = self
            .read_rows::<RecoveryRoot>(db, tx, skipped, of(rows::RECOVERY_ROOTS))
            .await?;
        let (account_keys, _) = self
            .read_rows::<jazz_e2ee::protocol::records::account::AccountSealingKey>(
                db,
                tx,
                skipped,
                of(rows::ACCOUNT_KEYS),
            )
            .await?;
        let (removal_requests, _) = self
            .read_rows::<DeviceRemovalRequest>(db, tx, skipped, of(rows::DEVICE_REMOVAL_REQUESTS))
            .await?;

        let mut by_account: std::collections::BTreeMap<String, PublicAccountLedger> = account_ids
            .iter()
            .map(|id| (id.clone(), PublicAccountLedger::new(id, Position(through))))
            .collect();
        let mut devices: std::collections::BTreeMap<String, BTreeSet<String>> = account_ids
            .iter()
            .map(|id| (id.clone(), extra_devices.clone()))
            .collect();
        macro_rules! group {
            ($rows:expr, $field:ident, $devices:expr) => {
                for row in $rows {
                    let account = row.row.account_id.clone();
                    if let Some(ledger) = by_account.get_mut(&account) {
                        let named: Vec<String> = $devices(&row);
                        devices.entry(account).or_default().extend(named);
                        ledger.$field.push(row);
                    }
                }
            };
        }
        group!(roots, roots, |r: &Accepted<AccountRoot>| vec![
            r.row.device_id.clone()
        ]);
        group!(approvals, approvals, |a: &Accepted<
            PublicDeviceApproval,
        >| vec![
            a.row.device_id.clone(),
            a.row.signer_id.clone()
        ]);
        group!(successors, successors, |x: &Accepted<
            PublicAccountSuccessor,
        >| {
            let mut named = vec![x.row.signer_id.clone()];
            named.extend(x.row.removed_device_id.clone());
            named
        });
        group!(recovery_roots, recovery_roots, |r: &Accepted<
            RecoveryRoot,
        >| vec![
            r.row.signer_id.clone()
        ]);
        group!(account_keys, account_keys, |_: &Accepted<
            jazz_e2ee::protocol::records::account::AccountSealingKey,
        >| Vec::new());
        group!(removal_requests, removal_requests, |r: &Accepted<
            DeviceRemovalRequest,
        >| vec![
            r.row.device_id.clone()
        ]);

        let wanted: BTreeSet<&String> = devices.values().flatten().collect();
        if !wanted.is_empty() {
            let ids = wanted
                .iter()
                .filter_map(|id| uuid_cell(id))
                .map(Operand::Literal)
                .collect();
            let query = Query::from(rows::DEVICE_KEYS)
                .filter(Predicate::In(Operand::Column("deviceId".to_owned()), ids));
            let (keys, _) = self.read_rows(db, tx, skipped, query).await?;
            for key in keys {
                let key: Accepted<jazz_e2ee::protocol::records::account::DeviceKeysRow> = key;
                for (account, named) in &devices {
                    if named.contains(&key.row.device_id)
                        && let Some(ledger) = by_account.get_mut(account)
                    {
                        ledger.keys.push(key.clone());
                    }
                }
            }
        }
        Ok(account_ids
            .iter()
            .map(|id| by_account.remove(id).expect("every account has a ledger"))
            .collect())
    }

    /// The account's public and private rows at the cut of `tx`.
    async fn read_account(
        &self,
        db: &Db,
        tx: OpenTransactionId,
    ) -> Result<AccountRead, ClientError> {
        let skipped = Cell::new(0);
        let account_id = self.shared.account_id.clone();
        // The private tables hold only the account's own rows for its own
        // sessions (R4–R9).
        let (identities, _) = self
            .read_rows(db, tx, &skipped, Query::from(rows::ACCOUNT_IDENTITIES))
            .await?;
        let (requests, _) = self
            .read_rows::<DeviceRequest>(db, tx, &skipped, Query::from(rows::DEVICE_REQUESTS))
            .await?;
        let (challenges, _) = self
            .read_rows(db, tx, &skipped, Query::from(rows::DEVICE_CHALLENGES))
            .await?;
        let (proofs, _) = self
            .read_rows(db, tx, &skipped, Query::from(rows::DEVICE_PROOFS))
            .await?;
        let (private_approvals, _) = self
            .read_rows(db, tx, &skipped, Query::from(rows::DEVICE_APPROVALS))
            .await?;
        let (deliveries, _) = self
            .read_rows(db, tx, &skipped, Query::from(rows::DEVICE_DELIVERIES))
            .await?;
        let account = uuid_cell(&account_id)
            .ok_or(ProtocolError::InvalidRecord("the account ID is not a UUID"))?;
        let (private_successors, _) = self
            .read_rows(
                db,
                tx,
                &skipped,
                by_account(rows::ACCOUNT_SUCCESSORS, &account),
            )
            .await?;
        let (recovery_deliveries, _) = self
            .read_rows(db, tx, &skipped, Query::from(rows::RECOVERY_DELIVERIES))
            .await?;
        let public = self
            .read_public_many(
                db,
                tx,
                &skipped,
                std::slice::from_ref(&account_id),
                requests.iter().map(|r| r.row.id.clone()).collect(),
            )
            .await?
            .remove(0);
        let through = public.through;
        Ok(AccountRead {
            public,
            private: PrivateAccountLedger {
                through,
                identities,
                requests,
                challenges,
                proofs,
                approvals: private_approvals,
                deliveries,
                successors: private_successors,
                recovery_deliveries,
            },
            skipped: skipped.get(),
        })
    }

    /// Stages `record` in transaction `tx`.
    pub(crate) async fn stage<R: Row>(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        record: &R,
    ) -> Result<(), ClientError> {
        let cells = record.cells().ok_or(ProtocolError::InvalidRecord(
            "a record with an ID that is not a UUID",
        ))?;
        let row = uuid::Uuid::parse_str(record.id())
            .map_err(|_| ProtocolError::InvalidRecord("a record with an ID that is not a UUID"))?;
        db.tx_ref(tx)
            .insert(
                R::TABLE,
                cells,
                InsertOptions {
                    row_id: Some(RowUuid(row)),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Commits `tx` and waits until the authority accepts it.
    pub(crate) async fn publish(
        &self,
        db: &Db,
        tx: OpenTransactionId,
    ) -> Result<(), jazz::db::Error> {
        let receipt = db.commit_serializable_handle(tx).await?;
        db.wait_for_transaction(receipt.0, DurabilityTier::Global)
            .await?;
        Ok(())
    }

    /// Brings this device into its account and verifies the account:
    ///
    /// - without a request of this device, publishes one, with its public
    ///   key projection (§2.2);
    /// - in an account with no evidence at all, bootstraps it with this
    ///   device (§2.3): its identity, public root (signed by the device-account
    ///   identity for a device-account account) and the sealing key record of
    ///   the first epoch, in one transaction;
    /// - otherwise replays the account at one cut and checks it against the
    ///   floor this device stored (§2.4, "Floors"): an older read is
    ///   `stale-state`, a replaced root `root-not-mine` (both errors); a
    ///   device the account no longer has wipes this account's private state
    ///   and enrols again as a new device ([`AccountStatus::Reenrolled`]).
    ///   An active device then verifies the private evidence, acquires the
    ///   account key (§2.6, §2.7) and advances its floor.
    ///
    /// If the protocol refuses the account evidence the server returns,
    /// [`E2eeClient::with_account`] returns `None` until a read verifies
    /// the account again. Refused actions, local cryptographic failures
    /// and database errors preserve the last verified account.
    ///
    /// Needs the authority: every read is a ledger read.
    pub async fn ensure_account(&self, db: &Db) -> Result<AccountStatus, ClientError> {
        let mut reenrolled = false;
        for _ in 0..ATTEMPTS {
            self.ensure_open()?;
            let tx = OpenTransactionId::new();
            db.begin_serializable(tx).await?;
            let outcome = self.step(db, tx).await;
            let _ = db.abandon_transaction_handle(tx);
            match outcome {
                Ok(Step::Again) => continue,
                Ok(Step::Done(status))
                    if reenrolled && status == AccountStatus::AwaitingApproval =>
                {
                    return Ok(AccountStatus::Reenrolled);
                }
                Ok(Step::Done(status)) => return Ok(status),
                Ok(Step::Reenrolled) => {
                    reenrolled = true;
                    continue;
                }
                Err(ClientError::Database(error)) if conflict(&error) => continue,
                Err(error) => return Err(self.forget_account_on(error)),
            }
        }
        Err(ClientError::Database(jazz::db::Error::new(
            ErrorCode::TransactionConflict,
            "the account kept changing while this device read it",
        )))
    }

    async fn step(&self, db: &Db, tx: OpenTransactionId) -> Result<Step, ClientError> {
        let read = self.read_account(db, tx).await?;
        let device = self.shared.device()?.clone();
        let writer = self.writer()?;
        // A device that verified this account before checks every read
        // against its floor before it stages anything: a server that shows
        // it less (an empty account, an older state, no request of its own)
        // gets nothing published.
        if self.check_floor_first(&read, &device.id).await? == FloorCheck::NoLongerActive {
            self.reenrol().await?;
            return Ok(Step::Reenrolled);
        }
        let requested = read
            .private
            .requests
            .iter()
            .any(|request| request.row.id == device.id);
        if !requested {
            self.stage(db, tx, &device.request()).await?;
            let keys = device.keys_row(&mut **self.shared.random.borrow_mut())?;
            self.stage(db, tx, &keys).await?;
            self.publish(db, tx).await?;
            return Ok(Step::Again);
        }
        if read.public.is_empty() && !read.private.has_history() && read.skipped == 0 {
            let permit = check_bootstrap(&writer, &device, &read.public, &read.private)?;
            let authority = match &self.device_account {
                Some(identity) => RootAuthority::DeviceAccount(identity),
                None => RootAuthority::App,
            };
            let records = account_root(
                &writer,
                &device,
                &permit,
                authority,
                &mut **self.shared.random.borrow_mut(),
            )?;
            self.stage(db, tx, &records.identity).await?;
            self.stage(db, tx, &records.root).await?;
            self.stage(db, tx, &records.account_key).await?;
            self.publish(db, tx).await?;
            return Ok(Step::Again);
        }

        let replay = AccountReplay::new(&read.public, &self.shared.domain, &self.shared.registry)?;
        let Some(state) = replay.current()? else {
            // Evidence without a counting root: nothing this device can
            // verify or bootstrap over.
            return Err(ProtocolError::InconsistentEvidence.into());
        };
        let through = read.public.through;
        if let Some(identity) = &self.device_account {
            state.check_device_account(identity)?;
        }
        let floor = state.floor(through, Some(&device.id));
        if !state.is_active(&device.id) {
            self.advance_floor(&self.shared.account_id, &floor).await?;
            return Ok(Step::Done(AccountStatus::AwaitingApproval));
        }
        let evidence = verify_private(&replay, &state, &read.private)?;
        let keys = AccountKeyEvidence::new(&replay, &state, &evidence)?;
        let key = keys.acquire(&device.id, std::slice::from_ref(&device.sealing))?;
        let epoch_keys = keys.epoch_keys(&key)?;
        self.advance_floor(&self.shared.account_id, &floor).await?;
        {
            // A new account epoch (a device removed, wherever that was
            // done) may supersede the epoch a space's current version was
            // sealed to: each space's mark is evaluated again before this
            // runtime's next write to it.
            let mut keys = self.shared.keys.borrow_mut();
            let removals = !state.removed.is_empty() || state.generation > 0;
            if keys.evaluated_epoch.as_deref() != Some(state.epoch_id()) {
                keys.rotation_evaluated.clear();
                keys.evaluated_epoch = Some(state.epoch_id().to_owned());
                // A removal of this account's own makes every space one
                // with a member that removed a device.
                if removals {
                    keys.space_removals.clear();
                }
            }
        }
        *self.account.borrow_mut() = Some(OwnAccount {
            state,
            key,
            epoch_keys,
            public: read.public.clone(),
        });
        Ok(Step::Done(AccountStatus::Active))
    }

    /// Resets the account on this device (§2.11): a new generation whose
    /// only device is this one, for an account of the application's own
    /// sign-in that lost every device and recovery, where the application
    /// allows resets. The account's other devices are no longer active in
    /// it: each wipes its private state and enrolls again when it next
    /// verifies the account. Earlier space keys are not carried over.
    /// Needs the authority.
    pub async fn reset_account(&self, db: &Db) -> Result<AccountStatus, ClientError> {
        self.serializable(db, async |tx| self.reset_step(db, tx).await)
            .await?;
        self.ensure_account(db).await
    }

    async fn reset_step(&self, db: &Db, tx: OpenTransactionId) -> Result<(), ClientError> {
        let read = self.read_account(db, tx).await?;
        let device = self.shared.device()?.clone();
        if self.check_floor_first(&read, &device.id).await? == FloorCheck::NoLongerActive {
            return Err(ProtocolError::DeviceNoLongerActive.into());
        }
        let writer = self.writer()?;
        let replay = AccountReplay::new(&read.public, &self.shared.domain, &self.shared.registry)?;
        let state = replay
            .current()?
            .ok_or(ProtocolError::InconsistentEvidence)?;
        let policy = ResetPolicy {
            device_account: self.device_account.is_some(),
            allow_account_reset: self.allow_account_reset(),
        };
        let permit = check_reset(
            &writer,
            policy,
            &state,
            &device,
            &read.public,
            &read.private,
        )?;
        let records = account_root(
            &writer,
            &device,
            &permit,
            RootAuthority::App,
            &mut **self.shared.random.borrow_mut(),
        )?;
        self.stage(db, tx, &records.identity).await?;
        self.stage(db, tx, &records.root).await?;
        self.stage(db, tx, &records.account_key).await?;
        self.publish(db, tx).await?;
        Ok(())
    }

    /// Checks `read` against this device's floor for its account, if it has
    /// one (§2.4, "Floors"). The floor was stored once the device had
    /// published its request and verified the account, so a read that
    /// shows less (no counting root, an earlier cut or generation, a
    /// chain without the floor's epoch, or no request of this device) is
    /// `stale-state`, and a replaced root `root-not-mine`; nothing is
    /// staged on such a read.
    async fn check_floor_first(
        &self,
        read: &AccountRead,
        device_id: &str,
    ) -> Result<FloorCheck, ClientError> {
        let Some(floor) = self.floor(&self.shared.account_id).await else {
            return Ok(FloorCheck::NoFloor);
        };
        let replay = AccountReplay::new(&read.public, &self.shared.domain, &self.shared.registry)?;
        let Some(state) = replay.current()? else {
            return Err(ProtocolError::StaleState.into());
        };
        match state.check_floor(read.public.through, &floor) {
            Ok(()) => {}
            Err(ProtocolError::DeviceNoLongerActive) => return Ok(FloorCheck::NoLongerActive),
            Err(error) => return Err(error.into()),
        }
        if !read
            .private
            .requests
            .iter()
            .any(|request| request.row.id == device_id)
        {
            return Err(ProtocolError::StaleState.into());
        }
        Ok(FloorCheck::Passed)
    }

    /// Forgets the last verified account when the protocol refuses the
    /// evidence returned by a later read or the read shows this device is
    /// no longer active. All other errors keep the account by default, so
    /// new error kinds must be explicitly identified as reasons to
    /// invalidate verified state.
    pub(crate) fn forget_account_on(&self, error: ClientError) -> ClientError {
        if matches!(
            &error,
            ClientError::Protocol(
                ProtocolError::StaleState
                    | ProtocolError::RootNotMine
                    | ProtocolError::DeviceNoLongerActive
                    | ProtocolError::Intrinsic(_)
                    | ProtocolError::InconsistentEvidence
                    | ProtocolError::InvalidRecord(_)
                    | ProtocolError::AccountKeyUnavailable
                    | ProtocolError::Unsupported(_),
            )
        ) {
            *self.account.borrow_mut() = None;
        }
        error
    }

    /// Calls `f` with the last verified account while this device is active
    /// in it. The verified account is cleared when a later read is refused
    /// or shows this device is no longer active. Refused actions, local
    /// cryptographic failures and database errors preserve it.
    pub fn with_account<T>(&self, f: impl FnOnce(&OwnAccount) -> T) -> Option<T> {
        self.account.borrow().as_ref().map(f)
    }

    /// Subscriptions to this account's public rows that change its state:
    /// roots, approvals, successors, recovery roots, sealing keys and
    /// removal requests (core hooks note H6). When one changes, the caller
    /// verifies the account again ([`E2eeClient::ensure_account`]).
    pub async fn watch_account(&self, db: &Db) -> Result<AccountWatch, ClientError> {
        let account = uuid_cell(&self.shared.account_id)
            .ok_or(ProtocolError::InvalidRecord("the account ID is not a UUID"))?;
        let mut streams = Vec::new();
        for table in [
            rows::ACCOUNT_ROOTS,
            rows::PUBLIC_DEVICE_APPROVALS,
            rows::PUBLIC_ACCOUNT_SUCCESSORS,
            rows::RECOVERY_ROOTS,
            rows::ACCOUNT_KEYS,
            rows::DEVICE_REMOVAL_REQUESTS,
        ] {
            let prepared = db.prepare_query(&by_account(table, &account))?;
            streams.push(db.subscribe(&prepared, Default::default()).await?);
        }
        Ok(AccountWatch { streams })
    }
}

/// How a read compares with this device's floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FloorCheck {
    /// The device has no floor for the account yet.
    NoFloor,
    Passed,
    /// The account no longer has this device.
    NoLongerActive,
}

/// What one pass of [`E2eeClient::ensure_account`] did.
enum Step {
    /// It published something: read again.
    Again,
    /// It wiped the account's state and enrolled a new device.
    Reenrolled,
    Done(AccountStatus),
}

/// Live subscriptions to the rows that change this device's account.
pub struct AccountWatch {
    streams: Vec<SubscriptionStream>,
}

impl AccountWatch {
    /// Whether any of the account's rows changed since the last call; the
    /// first call reports the rows already there.
    pub fn changed(&mut self) -> bool {
        let mut changed = false;
        for stream in &mut self.streams {
            while stream.try_next_event().is_some() {
                changed = true;
            }
        }
        changed
    }
}

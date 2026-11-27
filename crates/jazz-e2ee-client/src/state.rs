//! What the runtime and its hooks share on the database's thread: the
//! device, the held keys, the catalogue identities the cell contexts name,
//! and the reports reads leave for the runtime.

use std::cell::{Ref, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::rc::Rc;

use jazz::db::{Db, ErrorCode};
use jazz::e2ee::{EncryptionEntry, SpaceAddress as DbSpaceAddress, Unreadable, UnreadableCode};
use jazz::ids::RowUuid;
use jazz_e2ee::RandomSource;
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::account::build::LocalDevice;
use jazz_e2ee::protocol::content::HeldKeys;
use jazz_e2ee::protocol::domain::{AppDomain, SharedDomain};
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::protocol::records::space::SpaceRoot;
use jazz_e2ee::registry::Registry;

use crate::error::ClientError;
use crate::spaces::SpaceKeyState;
use crate::values::ColumnCodec;

/// At most this many reports wait for the runtime; older ones are dropped
/// first (protocol §4.3 keeps a bounded set of the same size).
pub const MAX_REPORTS: usize = 10_000;

/// Keeps a queue bounded, returning its oldest item when it was full.
pub(crate) fn push_bounded<T>(queue: &mut VecDeque<T>, item: T, cap: usize) -> Option<T> {
    let dropped = (queue.len() == cap).then(|| queue.pop_front()).flatten();
    queue.push_back(item);
    dropped
}

/// How long a staged member row is waited for before the write is taken as
/// rejected or abandoned: a space such a row would have changed is served at
/// the next natural moment anyway.
pub(crate) const MEMBER_CHANGE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// The first wait before a failed operation is tried again, doubling up to
/// the last: the runtime's verification, and the settle of a space whose
/// acquisition failed.
pub(crate) const RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(250);
pub(crate) const RETRY_LAST: std::time::Duration = std::time::Duration::from_secs(30);

/// A space whose settle failed: how many times in a row, and when it may be
/// tried again (until then its rows are unresolved, not absent). The first
/// failure is retried at the next settle; each later one waits
/// [`RETRY_FIRST`], doubling up to [`RETRY_LAST`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct SettleBackoff {
    pub(crate) failures: u32,
    pub(crate) retry_at: web_time::Instant,
}

impl SettleBackoff {
    /// After one more failure of a space that had failed `previous` times
    /// in a row (none: it had not).
    pub(crate) fn after_failure(previous: Option<&Self>, now: web_time::Instant) -> Self {
        let failures = previous.map_or(0, |backoff| backoff.failures) + 1;
        let wait = match failures {
            1 => std::time::Duration::ZERO,
            n => RETRY_FIRST
                .saturating_mul(1 << (n - 2).min(16))
                .min(RETRY_LAST),
        };
        Self {
            failures,
            retry_at: now + wait,
        }
    }
}

/// Something a read or a load left for the runtime to act on or pass on.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Report {
    /// A cell or row a read left out, or returned with a notice
    /// (protocol §4.3). Listeners ([`crate::E2eeClient::on_report`]) get each
    /// distinct one once.
    Unreadable(Unreadable),
    /// A row a read left out because this device does not hold the version
    /// its cell names: the runtime acquires it (protocol §4.2).
    MissingKey {
        table: String,
        space: DbSpaceAddress,
        epoch_id: String,
    },
    /// A record of the private store that failed its load checks and was
    /// skipped (protocol §6.2, §6.3a).
    SkippedRecord { document: String, reason: String },
    /// A valid sealing key record that did not derive from this account's epoch key.
    ConflictingAccountKey {
        record_id: String,
        signer_id: String,
        epoch_id: String,
    },
    /// A sealing key record whose signature this device cannot verify.
    UnsupportedAccountKey {
        record_id: String,
        signer_id: String,
        capability: jazz_e2ee::protocol::error::UnsupportedCapability,
    },
    /// A delivery to this account that provably failed to open or
    /// authenticate (protocol §3.4): nothing is written; the member stays
    /// `key-pending` until an updater rotates.
    InvalidDelivery {
        space: DbSpaceAddress,
        sender: String,
        recipient: String,
        key_epoch_id: String,
    },
    /// A key that opened and passed its own version's marker, but whose
    /// history fails further back (protocol §3.4): the fault is the named
    /// successor's author's; the remedy is a recovery rotation.
    BrokenHistory {
        space: DbSpaceAddress,
        key_epoch_id: String,
        author: String,
    },
    /// A member account whose current generation is above 0: its encryption
    /// was reset (protocol §2.11), once per space, account and generation.
    AccountReset {
        account: String,
        space: DbSpaceAddress,
        generation: u64,
    },
    /// A space with more member accounts than the design limit
    /// (protocol §3.8), once per space.
    SpaceOverMemberLimit {
        space: DbSpaceAddress,
        members: usize,
    },
    /// The runtime could not verify whether the space needs rotation before a write.
    RotationCheckFailed {
        space: DbSpaceAddress,
        reason: String,
    },
    /// The runtime could not deliver keys to members of the space: the
    /// rest of a rotation (once per attempt), a pass after the space's
    /// members or keys changed, or the startup or reconnect pass over held
    /// spaces. At most once per space per round of these.
    DeliveryFailed {
        space: DbSpaceAddress,
        reason: String,
    },
    /// A rotation the runtime tried before a write to `space`, which failed
    /// or was refused, with its reason. The write is sealed under the
    /// space's current version all the same (protocol §3.5), which a
    /// removed member or device may hold. Told once per failed attempt.
    RotationFailed {
        space: DbSpaceAddress,
        reason: String,
    },
    /// A report ([`Report::Unreadable`], per space or per row) that no
    /// longer holds: the key was obtained, or the space now has a valid
    /// root, or the account is now a member (protocol §4.3). `id` and
    /// `column` name the row of a per-row report.
    Withdrawn {
        table: String,
        id: Option<RowUuid>,
        column: Option<String>,
        space: DbSpaceAddress,
        code: UnreadableCode,
    },
}

/// A read left out a row because the version its cell names is not held:
/// what the runtime needs to acquire it and then settle the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Missing {
    pub(crate) table: String,
    pub(crate) id: RowUuid,
    pub(crate) column: String,
    pub(crate) space: SpaceAddress,
    pub(crate) epoch_id: String,
}

/// Cells to repair: by table and row, the row's space and its columns.
pub(crate) type Repairable = BTreeMap<(String, RowUuid), (SpaceAddress, BTreeSet<String>)>;

/// Where one repair read's notes of cells under a refused creation's key go.
pub(crate) type Collector = std::rc::Rc<RefCell<Repairable>>;

/// What identifies a report for deduplication (protocol §4.3).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReportKey {
    table: String,
    id: Option<RowUuid>,
    column: Option<String>,
    code: UnreadableCode,
    space: Option<SpaceAddress>,
}

impl ReportKey {
    fn of(unreadable: &Unreadable) -> Self {
        Self {
            table: unreadable.table.clone(),
            id: unreadable.id,
            column: unreadable.column.clone(),
            code: unreadable.code,
            space: unreadable.space.as_ref().map(unchecked_address),
        }
    }
}

/// A report listener.
pub(crate) type Listener = Rc<dyn Fn(&Report)>;

/// The reports delivered to listeners: each distinct report once, in a
/// bounded least-recently-used set, which a listener registered later
/// receives first (protocol §4.3).
#[derive(Default)]
pub(crate) struct Published {
    /// Events already delivered, by what makes each one the same event.
    events: std::collections::HashSet<String>,
    event_order: VecDeque<String>,
    tick: u64,
    seen: HashMap<ReportKey, (u64, Unreadable)>,
    order: BTreeMap<u64, ReportKey>,
    listeners: Vec<Listener>,
}

impl Published {
    /// Remembers the last MAX_REPORTS distinct event identities in publication order.
    fn insert_event(&mut self, key: String) -> bool {
        if !self.events.insert(key.clone()) {
            return false;
        }
        if let Some(oldest) = push_bounded(&mut self.event_order, key, MAX_REPORTS) {
            self.events.remove(&oldest);
        }
        true
    }

    /// Takes `unreadable`, returning whether it is new.
    fn insert(&mut self, unreadable: Unreadable) -> bool {
        let key = ReportKey::of(&unreadable);
        self.tick += 1;
        let tick = self.tick;
        let fresh = match self.seen.insert(key.clone(), (tick, unreadable)) {
            Some((old, _)) => {
                self.order.remove(&old);
                false
            }
            None => true,
        };
        self.order.insert(tick, key);
        while self.order.len() > MAX_REPORTS {
            if let Some((_, oldest)) = self.order.pop_first() {
                self.seen.remove(&oldest);
            }
        }
        fresh
    }

    /// The reports still held, oldest first.
    fn held(&self) -> Vec<Report> {
        self.order
            .values()
            .map(|key| Report::Unreadable(self.seen[key].1.clone()))
            .collect()
    }

    /// The spaces with a report waiting for a key or a root.
    pub(crate) fn pending_spaces(&self) -> BTreeSet<SpaceAddress> {
        self.seen
            .keys()
            .filter(|key| {
                key.id.is_none()
                    && matches!(
                        key.code,
                        UnreadableCode::KeyPending | UnreadableCode::SpacePending
                    )
            })
            .filter_map(|key| key.space.clone())
            .collect()
    }

    fn listeners(&self) -> Vec<Listener> {
        self.listeners.clone()
    }

    /// Forgets which missing versions of `space` were told, so a version
    /// still missing after the space's keys changed is told again.
    fn forget_missing_keys(&mut self, space: &SpaceAddress) {
        let of_space = format!(":{}:{}:", space.scope_id, space.identifier);
        self.event_order.retain(|key| {
            if key.starts_with("missing-key:") && key.contains(&of_space) {
                self.events.remove(key);
                false
            } else {
                true
            }
        });
    }

    /// Forgets every report and event, keeping the listeners, and returns
    /// the withdrawals of the held reports of a space.
    fn reset(&mut self) -> Vec<Report> {
        let listeners = std::mem::take(&mut self.listeners);
        let old = std::mem::replace(
            self,
            Self {
                listeners,
                ..Default::default()
            },
        );
        old.order
            .into_values()
            .filter_map(|key| {
                Some(Report::Withdrawn {
                    space: db_address(&key.space?),
                    table: key.table,
                    id: key.id,
                    column: key.column,
                    code: key.code,
                })
            })
            .collect()
    }

    /// Forgets the reports of `space`, per space and per row, with one of
    /// `codes`, keeping the space report for an incomplete equality query.
    fn withdraw(
        &mut self,
        space: &SpaceAddress,
        codes: &[UnreadableCode],
        incomplete: Option<UnreadableCode>,
    ) -> Vec<Report> {
        let gone: Vec<ReportKey> = self
            .seen
            .keys()
            .filter(|key| {
                key.space.as_ref() == Some(space)
                    && codes.contains(&key.code)
                    && (key.id.is_some() || Some(key.code) != incomplete)
            })
            .cloned()
            .collect();
        let mut out = Vec::new();
        for key in gone {
            if let Some((tick, _)) = self.seen.remove(&key) {
                self.order.remove(&tick);
            }
            out.push(Report::Withdrawn {
                table: key.table,
                id: key.id,
                column: key.column,
                space: db_address(space),
                code: key.code,
            });
        }
        out
    }
}

/// One encrypted table: its entry and how each encrypted column is
/// encoded.
#[derive(Debug, Clone)]
pub(crate) struct TablePlan {
    pub(crate) entry: EncryptionEntry,
    pub(crate) columns: BTreeMap<String, ColumnCodec>,
}

impl TablePlan {
    /// Whether rows of this table are their own spaces.
    pub(crate) fn self_space(&self) -> bool {
        self.entry.space == "id"
    }
}

/// Jazz's catalogue identities of the declared tables and encrypted
/// columns, which every cell context names (protocol §4.1).
#[derive(Debug, Clone, Default)]
pub(crate) struct Catalogue {
    tables: BTreeMap<String, String>,
    pub(crate) table_names: BTreeMap<String, String>,
    columns: BTreeMap<(String, String), String>,
    /// Per declared table, the encrypted columns a migration added to rows
    /// stored before them: those rows hold no equality token for them.
    lens_added: BTreeMap<String, BTreeSet<String>>,
}

/// The error of a read or write before the identities are known.
pub(crate) fn not_observed() -> jazz::db::Error {
    jazz::db::Error::new(
        ErrorCode::NotObserved,
        "encrypted tables cannot be read or written before this client has synced their \
         catalogue identities once",
    )
}

impl Catalogue {
    /// The identities of every declared table, space table and encrypted
    /// column, or `None` while the database has not observed them.
    pub(crate) async fn resolve(
        db: &Db,
        plans: &BTreeMap<String, TablePlan>,
    ) -> Result<Option<Self>, ClientError> {
        fn known<T>(
            found: Result<Option<T>, jazz::db::Error>,
            what: &str,
        ) -> Result<Option<T>, ClientError> {
            match found {
                Ok(Some(id)) => Ok(Some(id)),
                Ok(None) => Err(ClientError::Unsupported(format!(
                    "the encryption declarations name `{what}`, which the schema does not have"
                ))),
                Err(error) if error.code == ErrorCode::NotObserved => Ok(None),
                Err(error) => Err(error.into()),
            }
        }
        let Some(names) = db.table_names_by_identity().await? else {
            return Ok(None);
        };
        let mut catalogue = Self::default();
        for (identity, name) in names {
            let id = identity.0.to_string();
            catalogue.tables.insert(name.clone(), id.clone());
            catalogue.table_names.insert(id, name);
        }
        for (table, plan) in plans {
            for name in [table, &plan.entry.scope] {
                if !catalogue.tables.contains_key(name) {
                    return Err(ClientError::Unsupported(format!(
                        "the encryption declarations name `{name}`, which the schema does not have"
                    )));
                }
            }
            let added = db.lens_added_columns(table)?;
            catalogue.lens_added.insert(
                table.clone(),
                plan.columns
                    .keys()
                    .filter(|column| added.contains(*column))
                    .cloned()
                    .collect(),
            );
            for column in plan.columns.keys() {
                let Some(id) = known(
                    db.column_identity(table, column).await,
                    &format!("{table}.{column}"),
                )?
                else {
                    return Ok(None);
                };
                catalogue
                    .columns
                    .insert((table.clone(), column.clone()), id.0.to_string());
            }
        }
        Ok(Some(catalogue))
    }

    pub(crate) fn table(&self, table: &str) -> Result<&str, jazz::db::Error> {
        self.tables
            .get(table)
            .map(String::as_str)
            .ok_or_else(not_observed)
    }

    /// Whether a migration added encrypted `column` of `table` under rows
    /// stored before it.
    pub(crate) fn lens_added(&self, table: &str, column: &str) -> bool {
        self.lens_added
            .get(table)
            .is_some_and(|columns| columns.contains(column))
    }

    pub(crate) fn column(&self, table: &str, column: &str) -> Result<&str, jazz::db::Error> {
        self.columns
            .get(&(table.to_owned(), column.to_owned()))
            .map(String::as_str)
            .ok_or_else(not_observed)
    }
}

/// The keys and what decides how they are used.
#[derive(Debug, Default)]
pub(crate) struct KeyState {
    pub(crate) held: HeldKeys,
    /// The last successful acquisition's state and membership, for reads
    /// that still lack a key.
    pub(crate) space_reads: BTreeMap<SpaceAddress, (SpaceKeyState, bool)>,
    pub(crate) catalogue: Option<Catalogue>,
    /// Whether the client is online: offline, equality queries compute no
    /// tokens (protocol §4.5).
    pub(crate) online: bool,
    /// Spaces whose creation by this device was refused while the client
    /// runs (protocol §3.2).
    pub(crate) creation_refused: BTreeSet<SpaceAddress>,
    /// Spaces the last pass found to need rotation (protocol §3.5): the
    /// next encrypted write rotates them first.
    pub(crate) rotation_pending: BTreeSet<SpaceAddress>,
    /// Spaces a pass of this runtime has evaluated the mark of since it
    /// opened and since this account's current epoch last changed. The mark
    /// lives only in memory: a removal another runtime made, on this device
    /// before a restart or on another device, marks a space here only
    /// through a pass. So the first online write to a space under a key
    /// other than a creation of this run runs one first, in an account that
    /// ever removed a device (protocol §3.5).
    pub(crate) rotation_evaluated: BTreeSet<SpaceAddress>,
    /// The account epoch `rotation_evaluated` was evaluated against: a
    /// verification that finds another one empties it.
    pub(crate) evaluated_epoch: Option<String>,
    /// Per space, whether any of its member accounts removed a device or
    /// was reset, as the last pass over it read them; `true` again when a
    /// member account changed since. A space whose members never did skips
    /// the read before a write: no removed device can hold its versions.
    pub(crate) space_removals: BTreeMap<SpaceAddress, bool>,
    /// Spaces whose repair the authority refused (this device may not update
    /// the rows it wrote): the natural moments stop repairing them while the
    /// client runs; an explicit call always attempts (protocol §3.2).
    pub(crate) repair_refused: BTreeSet<SpaceAddress>,
    /// Rotation attempts made during a host preparation (`Some` of its
    /// number), and grants that its next retry consumes when sealing (`None`).
    pub(crate) rotation_tried: BTreeSet<(Option<u64>, SpaceAddress)>,
    /// Spaces whose rotation by this device was refused while the client
    /// runs: no automatic rotation, and no evaluation of the mark.
    pub(crate) rotation_refused: BTreeSet<SpaceAddress>,
    /// Set once the store is wiped: nothing more is sealed or opened.
    pub(crate) closed: bool,
}

impl KeyState {
    pub(crate) fn mark_rotation(
        &mut self,
        address: &SpaceAddress,
        marked: bool,
        removals: Option<bool>,
    ) {
        if let Some(removals) = removals {
            self.rotation_evaluated.insert(address.clone());
            self.space_removals.insert(address.clone(), removals);
        }
        if marked {
            self.rotation_pending.insert(address.clone());
        } else {
            self.rotation_pending.remove(address);
        }
    }

    pub(crate) fn missing_key_code(&self, space: &SpaceAddress) -> UnreadableCode {
        match self.space_reads.get(space) {
            Some((SpaceKeyState::Incomplete, _)) => UnreadableCode::UnsupportedFormat,
            Some((_, false)) => UnreadableCode::KeyUnavailable,
            Some((SpaceKeyState::NoRoot, true)) => UnreadableCode::SpacePending,
            _ => UnreadableCode::KeyPending,
        }
    }

    pub(crate) fn incomplete_query_code(&self, space: &SpaceAddress) -> UnreadableCode {
        match self.space_reads.get(space) {
            Some((_, false)) => UnreadableCode::KeyUnavailable,
            _ => UnreadableCode::KeyPending,
        }
    }

    /// Missing evidence about this space requires a read, including on a
    /// device that first encountered another member during this run.
    pub(crate) fn needs_evaluation(&self, address: &SpaceAddress) -> bool {
        if self.rotation_refused.contains(address) || self.rotation_evaluated.contains(address) {
            return false;
        }
        self.space_removals.get(address).copied().unwrap_or(true)
    }
}

/// The account rows needed by one space's last authenticated pass.
#[derive(Default, Clone)]
pub(crate) struct WatchedMembers {
    pub version: Option<jazz_e2ee::protocol::space::members::CurrentVersion>,
    pub members: BTreeSet<String>,
    pub holders: BTreeSet<String>,
    pub waiting: BTreeSet<String>,
}

/// A removal delta names only a row ID. Remember the owner of each visible
/// row so changes can be routed to the accounts or spaces they affect.
pub(crate) struct WatchedRows<K> {
    pub stream: jazz::db::SubscriptionStream,
    pub rows: BTreeMap<RowUuid, (K, jazz::db::Pos)>,
    /// The next delta is the stream's opening, kept as the rows already
    /// seen rather than reported as changes.
    baseline: bool,
}

impl<K: Ord + Clone> WatchedRows<K> {
    pub fn new(stream: jazz::db::SubscriptionStream) -> Self {
        Self {
            stream,
            rows: BTreeMap::new(),
            baseline: false,
        }
    }

    /// A stream whose opening is a baseline: the rows it opens with are
    /// not changes, only what later deltas add, update or remove is.
    pub fn from_baseline(stream: jazz::db::SubscriptionStream) -> Self {
        Self {
            baseline: true,
            ..Self::new(stream)
        }
    }

    /// The changed owners, and whether this stream must be opened again.
    pub fn changes(
        &mut self,
        owner: impl Fn(&jazz::db::CurrentRow) -> Option<K>,
    ) -> (BTreeSet<K>, bool) {
        use jazz::db::SubscriptionEvent;
        let mut changed = BTreeSet::new();
        let mut closed = false;
        while let Some(event) = self.stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    added,
                    updated,
                    removed,
                    reset,
                    ..
                } => {
                    if std::mem::take(&mut self.baseline) {
                        self.rows = added
                            .into_iter()
                            .chain(updated)
                            .filter_map(|row| Some((row.row_uuid(), (owner(&row)?, row.seq()))))
                            .collect();
                        continue;
                    }
                    let mut previous = if reset {
                        std::mem::take(&mut self.rows)
                    } else {
                        BTreeMap::new()
                    };
                    for row in added.into_iter().chain(updated) {
                        if let Some(key) = owner(&row) {
                            let entry = (key, row.seq());
                            let before = if reset {
                                previous.remove(&row.row_uuid())
                            } else {
                                self.rows.remove(&row.row_uuid())
                            };
                            if !reset || before.as_ref() != Some(&entry) {
                                changed.insert(entry.0.clone());
                                changed.extend(before.map(|(key, _)| key));
                            }
                            self.rows.insert(row.row_uuid(), entry);
                        }
                    }
                    changed.extend(previous.into_values().map(|(key, _)| key));
                    for row in removed {
                        changed.extend(self.rows.remove(&row.row_uuid).map(|(key, _)| key));
                    }
                }
                SubscriptionEvent::Rejected { .. } | SubscriptionEvent::Closed => {
                    closed = true;
                }
            }
        }
        (changed, closed)
    }
}

pub(crate) fn row_space(row: &jazz::db::CurrentRow) -> Option<SpaceAddress> {
    match (row.cell("scopeId"), row.cell("identifier")) {
        (Some(jazz::records::Value::Uuid(scope)), Some(jazz::records::Value::Uuid(id))) => {
            Some(SpaceAddress {
                scope_id: scope.to_string(),
                identifier: id.to_string(),
            })
        }
        _ => None,
    }
}
pub(crate) struct AccountStream {
    pub accounts: BTreeSet<String>,
    pub rows: WatchedRows<String>,
}

#[derive(Default)]
pub(crate) struct MemberWatch {
    pub space_streams: Vec<(&'static str, WatchedRows<SpaceAddress>)>,
    pub covered: BTreeSet<SpaceAddress>,
    pub subscribed: BTreeSet<SpaceAddress>,
    #[cfg(any(test, feature = "testing"))]
    pub last_check_rows: usize,
    #[cfg(any(test, feature = "testing"))]
    pub delivery_reads: (usize, usize, usize),
    #[cfg(any(test, feature = "testing"))]
    pub public_account_reads: usize,
    #[cfg(any(test, feature = "testing"))]
    pub chain_reads: usize,
    #[cfg(any(test, feature = "testing"))]
    pub delivery_mark_checks: usize,
    #[cfg(any(test, feature = "testing"))]
    pub delivery_chain_replays: usize,
    pub streams: BTreeMap<&'static str, AccountStream>,
    pub spaces: BTreeMap<SpaceAddress, WatchedMembers>,
    pub changed: BTreeSet<SpaceAddress>,
    /// Rotations this device made with deliveries still to send, including
    /// in unwatched spaces (protocol §3.5).
    pub pending_deliveries: BTreeMap<SpaceAddress, PendingRotation>,
    /// Spaces told `delivery-failed` in the current round: one sync and the
    /// startup or reconnect sweep that follows it.
    pub delivery_failures_told: BTreeSet<SpaceAddress>,
    /// While the session's background pass runs, over several turns of the
    /// loop, the round lasts until it ends: a space is told once.
    pub failure_round_held: bool,
    /// Tests: the next sync fails before it serves anything.
    #[cfg(any(test, feature = "testing"))]
    pub fail_next_sync: bool,
    /// Tests: how many of the next listings of the background pass's
    /// spaces fail.
    #[cfg(any(test, feature = "testing"))]
    pub fail_background_lists: u32,
    /// Tests: the background pass's steps wait forever.
    #[cfg(any(test, feature = "testing"))]
    pub hold_background: bool,
    /// Tests: the background pass's step of this space waits forever.
    #[cfg(any(test, feature = "testing"))]
    pub hold_background_space: Option<SpaceAddress>,
    /// Tests: how many of the next full acquisitions fail.
    #[cfg(any(test, feature = "testing"))]
    pub fail_acquisitions: u32,
    /// Tests: the full acquisitions and passes run, in order, each with
    /// its space.
    #[cfg(any(test, feature = "testing"))]
    pub work: Vec<(&'static str, SpaceAddress)>,
    pub by_account: BTreeMap<(&'static str, String), BTreeSet<SpaceAddress>>,
}

/// The rest of a rotation this device made: the members it still delivers
/// the new version to, each whether or not a delivery to it exists, since
/// another member's delivery may not open (protocol §3.5).
#[derive(Debug, Default)]
pub(crate) struct PendingRotation {
    /// Members not yet delivered to, in the rotation's order.
    pub accounts: Vec<String>,
    /// Failed attempts in a row, offline attempts not counted.
    pub failures: u32,
    /// No attempt before this, after a failure.
    pub retry_at: Option<web_time::Instant>,
}

impl MemberWatch {
    /// After a gap, the next write must check the authority even if a
    /// live subscription will eventually catch up.
    pub(crate) fn invalidate_rotation_checks(&mut self, keys: &mut KeyState) {
        for (space, deps) in &mut self.spaces {
            deps.version = None;
            keys.rotation_evaluated.remove(space);
            keys.space_removals.remove(space);
        }
    }

    /// A closed stream must be reopened and its spaces checked again.
    pub(crate) fn invalidate_coverage(&mut self, keys: &mut KeyState) {
        self.covered.clear();
        self.invalidate_rotation_checks(keys);
        self.changed.extend(self.spaces.keys().cloned());
    }
}

/// This device's creations of spaces in this run (protocol §3.2, §5.4).
#[derive(Default)]
pub(crate) struct Creations {
    /// Roots prepared, with their keys held `unpublished`, that the next
    /// write to their space stages.
    pub(crate) prepared: BTreeMap<SpaceAddress, SpaceRoot>,
    /// Roots staged in this run whose transaction may not have committed
    /// yet: a root absent from the store is not yet known to be lost.
    pub(crate) staged: BTreeSet<String>,
    /// Refusals collected by the host before the unpublished key was settled.
    pub(crate) refused: BTreeMap<String, jazz::db::Error>,
    /// Staged roots the database dropped (their write or transaction
    /// failed): the next write abandons them before anything seals under
    /// their keys.
    pub(crate) dropped: BTreeSet<String>,
}

/// Everything the runtime and its hooks share.
pub(crate) struct Shared {
    pub(crate) registry: Registry,
    pub(crate) selected: MechanismConfig,
    pub(crate) domain: AppDomain,
    pub(crate) content: SharedDomain,
    pub(crate) account_id: String,
    pub(crate) plans: BTreeMap<String, TablePlan>,
    /// This device; `None` once the store is wiped, its secrets and
    /// approval nonce dropped (and zeroed) with it (§6.4).
    pub(crate) device: RefCell<Option<LocalDevice>>,
    pub(crate) keys: RefCell<KeyState>,
    pub(crate) random: RefCell<Box<dyn RandomSource>>,
    /// Reports waiting for the runtime to deliver them to listeners: a
    /// hook never calls out while the database is busy.
    pub(crate) outbox: RefCell<VecDeque<Report>>,
    /// Test observation: every report a hook made since the test last
    /// took them, before delivery and de-duplication.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) taken: RefCell<VecDeque<Report>>,
    pub(crate) published: RefCell<Published>,
    pub(crate) creations: RefCell<Creations>,
    /// Versions reads needed and did not hold, since the runtime last
    /// looked.
    pub(crate) missing: RefCell<VecDeque<Missing>>,
    /// Spaces whose settle failed, until one succeeds: a space that keeps
    /// failing is not read again at every settle.
    pub(crate) settle_backoff: RefCell<BTreeMap<SpaceAddress, SettleBackoff>>,
    /// Member rows this client's writes staged, with their spaces and when,
    /// until the next pass after they commit serves the space (protocol
    /// §3.4), or until [`MEMBER_CHANGE_WAIT`] passes without the row: a write
    /// that was rejected or abandoned leaves no row to wait for.
    pub(crate) member_changes: RefCell<VecDeque<(RowUuid, SpaceAddress, web_time::Instant)>>,
    /// Member rows this client's writes deleted, with when, until their
    /// space's pass has run (protocol §3.4, §3.5): a removal's pass sets the
    /// needs-rotation mark, so the next write rotates first. While one is
    /// queued and the client is online, an encrypted write is held back
    /// (`NeedsKey`) until the queue is served. A deleted row the local store
    /// does not show after [`MEMBER_CHANGE_WAIT`] is dropped.
    pub(crate) member_removals: RefCell<VecDeque<(RowUuid, web_time::Instant)>>,
    /// The runtime clock when a test holds it still (`None`: the
    /// real one), so a wait is stepped, not slept.
    pub(crate) held_clock: std::cell::Cell<Option<web_time::Instant>>,
    /// The collectors of the repair reads in flight, each registered for the
    /// duration of its read (protocol §3.2).
    pub(crate) collectors: RefCell<BTreeMap<uuid::Uuid, Collector>>,
    /// The number the next host write preparation takes.
    pub(crate) next_write: std::cell::Cell<u64>,
    /// The watch of the watched spaces' member accounts.
    pub(crate) member_watch: RefCell<MemberWatch>,
    /// Writes keep member watches warm for 60 seconds. Open subscriptions
    /// retain their own interest independently. On the first write after
    /// expiry the runtime checks the authority before reusing a held key.
    pub(crate) written: RefCell<BTreeMap<SpaceAddress, web_time::Instant>>,
}

impl Shared {
    pub(crate) fn new(
        registry: Registry,
        selected: MechanismConfig,
        domain: AppDomain,
        account_id: String,
        plans: BTreeMap<String, TablePlan>,
        device: LocalDevice,
        random: Box<dyn RandomSource>,
    ) -> Self {
        Self {
            content: domain.shared(),
            registry,
            selected,
            domain,
            account_id,
            plans,
            device: Some(device).into(),
            keys: KeyState {
                online: true,
                ..KeyState::default()
            }
            .into(),
            random: RefCell::new(random),
            outbox: Default::default(),
            #[cfg(any(test, feature = "testing"))]
            taken: Default::default(),
            published: Default::default(),
            creations: Default::default(),
            missing: Default::default(),
            settle_backoff: Default::default(),
            member_changes: Default::default(),
            member_removals: Default::default(),
            held_clock: Default::default(),
            collectors: Default::default(),
            next_write: Default::default(),
            member_watch: Default::default(),
            written: Default::default(),
        }
    }

    pub(crate) fn recent_writes(&self) -> BTreeSet<SpaceAddress> {
        let now = self.now();
        let mut recent = self.written.borrow_mut();
        recent.retain(|_, last| now.duration_since(*last) < std::time::Duration::from_secs(60));
        recent.keys().cloned().collect()
    }

    pub(crate) fn note_write(&self, address: &SpaceAddress) {
        let now = self.now();
        let last = self.written.borrow_mut().insert(address.clone(), now);
        let watch = self.member_watch.borrow();
        let subscribed = watch.covered.contains(address) && watch.subscribed.contains(address);
        drop(watch);
        if !subscribed
            && last
                .is_none_or(|time| now.duration_since(time) >= std::time::Duration::from_secs(60))
        {
            self.member_watch
                .borrow_mut()
                .spaces
                .entry(address.clone())
                .or_default()
                .version = None;
            let mut keys = self.keys.borrow_mut();
            keys.rotation_evaluated.remove(address);
            keys.space_removals.insert(address.clone(), true);
        }
    }

    pub(crate) fn notice_member_changes(&self) {
        let mut watch = self.member_watch.borrow_mut();
        let mut changed = BTreeSet::new();
        watch.streams.retain(|&table, sub| {
            let (mut accounts, closed) = sub.rows.changes(crate::account::rows::row_account_id);
            if closed {
                accounts.extend(sub.accounts.iter().cloned());
            }
            changed.extend(accounts.into_iter().map(|id| (table, id)));
            !closed
        });
        let mut affected = BTreeSet::new();
        for account in changed {
            if let Some(spaces) = watch.by_account.get(&account) {
                affected.extend(spaces.iter().cloned());
            }
        }
        let mut chain_changed = BTreeSet::new();
        let mut lost_watch = false;
        for (table, sub) in &mut watch.space_streams {
            let (spaces, closed) = sub.changes(row_space);
            lost_watch |= closed;
            if matches!(
                *table,
                crate::account::rows::SPACE_ROOTS | crate::account::rows::SPACE_SUCCESSORS
            ) {
                chain_changed.extend(spaces.iter().cloned());
            }
            affected.extend(spaces);
        }
        for space in chain_changed {
            if let Some(deps) = watch.spaces.get_mut(&space) {
                deps.version = None;
            }
        }
        let mut keys = self.keys.borrow_mut();
        if lost_watch {
            watch.invalidate_coverage(&mut keys);
        }
        for space in affected {
            keys.rotation_evaluated.remove(&space);
            keys.space_removals.insert(space.clone(), true);
            watch.changed.insert(space);
        }
    }
}

/// Queues `missing`, at most `cap` entries. A full queue drops its oldest
/// entry of a space that is not backed off: a backed-off space's entries
/// are the rows its reads are unresolved for (re-queued ahead of newer
/// ones), so they go last. When every entry is backed off, `missing` itself
/// is dropped. A dropped entry is never dropped silently: it will not be
/// fetched, and a read reports a missing key only once its fetch settles,
/// so the report a row left out for want of a key gets is returned for it
/// (a later read queues it again). That covers an entry the settle loop
/// re-queues, which no read reports again.
#[must_use]
fn enqueue_missing(
    queue: &mut VecDeque<Missing>,
    backoff: &BTreeMap<SpaceAddress, SettleBackoff>,
    missing: Missing,
    cap: usize,
) -> Option<Report> {
    if queue.contains(&missing) {
        return None;
    }
    if queue.len() >= cap {
        match queue
            .iter()
            .position(|entry| !backoff.contains_key(&entry.space))
        {
            Some(oldest) => {
                let evicted = queue.remove(oldest).expect("a queued entry");
                queue.push_back(missing);
                return Some(Report::MissingKey {
                    table: evicted.table,
                    space: db_address(&evicted.space),
                    epoch_id: evicted.epoch_id,
                });
            }
            None => {
                return Some(Report::MissingKey {
                    table: missing.table,
                    space: db_address(&missing.space),
                    epoch_id: missing.epoch_id,
                });
            }
        }
    }
    queue.push_back(missing);
    None
}

impl Shared {
    /// This device, or [`ClientError::Closed`] once the store was wiped.
    pub(crate) fn device(&self) -> Result<Ref<'_, LocalDevice>, ClientError> {
        Ref::filter_map(self.device.borrow(), Option::as_ref).map_err(|_| ClientError::Closed)
    }

    /// Drops the device's secrets and approval nonce (each zeroed on drop)
    /// and every held key: the store was wiped (§6.4).
    pub(crate) fn forget_secrets(&self) {
        let mut keys = self.keys.borrow_mut();
        keys.closed = true;
        keys.held = Default::default();
        self.device.borrow_mut().take();
    }

    pub(crate) fn report(&self, report: Report) {
        #[cfg(any(test, feature = "testing"))]
        push_bounded(&mut self.taken.borrow_mut(), report.clone(), MAX_REPORTS);
        let mut outbox = self.outbox.borrow_mut();
        push_bounded(&mut outbox, report, MAX_REPORTS);
    }

    /// Notes that `column` of the row opened under a refused creation's key
    /// in every repair read now open (none open: nothing is noted). A
    /// repair takes from its own collector only what concerns the table,
    /// space and rows it reads, so another read's notes change nothing.
    pub(crate) fn note_repairable(
        &self,
        table: &str,
        row: RowUuid,
        space: &SpaceAddress,
        column: &str,
    ) {
        for collector in self.collectors.borrow().values() {
            let mut found = collector.borrow_mut();
            if found.len() >= MAX_REPORTS && !found.contains_key(&(table.to_owned(), row)) {
                continue;
            }
            found
                .entry((table.to_owned(), row))
                .or_insert_with(|| (space.clone(), BTreeSet::new()))
                .1
                .insert(column.to_owned());
        }
    }

    pub(crate) fn now(&self) -> web_time::Instant {
        self.held_clock.get().unwrap_or_else(web_time::Instant::now)
    }

    pub(crate) fn missing(&self, missing: Missing) {
        let dropped = enqueue_missing(
            &mut self.missing.borrow_mut(),
            &self.settle_backoff.borrow(),
            missing,
            MAX_REPORTS,
        );
        if let Some(report) = dropped {
            self.report(report);
        }
    }

    /// Delivers what the hooks reported since the last call to the
    /// listeners, each distinct report once, and `extra` too.
    pub(crate) fn flush(&self, extra: Vec<Report>) {
        let mut outbox = std::mem::take(&mut *self.outbox.borrow_mut());
        outbox.extend(extra);
        let mut deliver = Vec::new();
        let listeners = {
            let mut published = self.published.borrow_mut();
            for report in outbox {
                match report {
                    Report::Unreadable(unreadable) => {
                        if published.insert(unreadable.clone()) {
                            deliver.push(Report::Unreadable(unreadable));
                        }
                    }
                    // A read reports the same missing version once per row;
                    // the listeners hear it once.
                    Report::MissingKey {
                        table,
                        space,
                        epoch_id,
                    } => {
                        let key = format!(
                            "missing-key:{table}:{}:{}:{epoch_id}",
                            space.scope_id, space.identifier
                        );
                        if published.insert_event(key) {
                            deliver.push(Report::MissingKey {
                                table,
                                space,
                                epoch_id,
                            });
                        }
                    }
                    other => deliver.push(other),
                }
            }
            published.listeners()
        };
        for report in &deliver {
            for listener in &listeners {
                listener(report);
            }
        }
    }

    /// Forgets which missing versions of `space` the listeners were told:
    /// its keys changed, so one still missing is told again.
    pub(crate) fn forget_missing_keys(&self, space: &SpaceAddress) {
        self.published.borrow_mut().forget_missing_keys(space);
    }

    /// Forgets what was reported for a device and account that are gone
    /// (re-enrolment): reports not yet delivered or polled, the reports and
    /// events already delivered, and with them the spaces watched for
    /// them. The listeners stay; the withdrawals of the reports they hold
    /// reach them with the next delivery.
    pub(crate) fn reset_reports(&self) {
        let withdrawn = self.published.borrow_mut().reset();
        *self.outbox.borrow_mut() = withdrawn.into();
    }

    /// Withdraws the reports of `space` with one of `codes`.
    pub(crate) fn withdraw(
        &self,
        space: &SpaceAddress,
        codes: &[UnreadableCode],
        incomplete: Option<UnreadableCode>,
    ) {
        let withdrawn = self
            .published
            .borrow_mut()
            .withdraw(space, codes, incomplete);
        self.flush(withdrawn);
    }

    /// Delivers `report` once per identity among the last 10,000 distinct events.
    pub(crate) fn event(&self, key: String, report: Report) {
        if self.published.borrow_mut().insert_event(key) {
            self.flush(vec![report]);
        }
    }

    pub(crate) fn conflicting_account_key(
        &self,
        key: &jazz_e2ee::protocol::account::NonDerivedSealingKey,
    ) {
        self.event(
            format!("conflicting-account-key:{}", key.record_id),
            Report::ConflictingAccountKey {
                record_id: key.record_id.clone(),
                signer_id: key.signer_id.clone(),
                epoch_id: key.epoch_id.clone(),
            },
        );
    }

    pub(crate) fn held_reports(&self) -> Vec<Report> {
        self.published.borrow().held()
    }

    pub(crate) fn listen(&self, listener: Listener) {
        let held = {
            let mut published = self.published.borrow_mut();
            published.listeners.push(listener.clone());
            published.held()
        };
        for report in &held {
            listener(report);
        }
    }
}

/// The core's form of a space address.
pub(crate) fn db_address(address: &SpaceAddress) -> DbSpaceAddress {
    DbSpaceAddress {
        scope_id: address.scope_id.clone(),
        identifier: address.identifier.clone(),
    }
}

/// Copies a coordinate without validating it. The watch reports invalid
/// input during sync, and report identities retain whatever was reported.
pub(crate) fn unchecked_address(address: &DbSpaceAddress) -> SpaceAddress {
    SpaceAddress {
        scope_id: address.scope_id.clone(),
        identifier: address.identifier.clone(),
    }
}

#[cfg(test)]
mod watch_tests {
    use super::*;
    use jazz::db::{Db, DbConfig, DbIdentity, block_on};
    use jazz::ids::{AuthorSubject, NodeUuid};
    use jazz::query::Query;
    use jazz::records::Value;
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

    /// Alice replaces her watch after Bob updates a row. The opening
    /// snapshot contains the same row ID and owner at a new position.
    /// Only the watch's routing state exposes whether it noticed the change.
    #[test]
    fn a_reopened_watch_notices_a_rows_new_position() {
        block_on(async {
            let schema = JazzSchema::new(
                &SchemaBuilder::new()
                    .table(TableSchemaBuilder::new("notes").column("body", ColumnType::Text))
                    .build(),
            )
            .unwrap();
            let db = Db::open(DbConfig::new(
                schema,
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ))
            .await
            .unwrap();
            db.set_authority().await.unwrap();
            let cells =
                |text: &str| BTreeMap::from([("body".to_owned(), Value::String(text.into()))]);
            let inserted = db
                .insert("notes", cells("old"), Default::default())
                .await
                .unwrap();
            let prepared = db.prepare_query(&Query::from("notes")).unwrap();
            let watch = || db.subscribe(&prepared, Default::default());
            let owner = |_: &jazz::db::CurrentRow| Some("bob");
            let mut before = WatchedRows::new(watch().await.unwrap());
            assert_eq!(before.changes(owner), (BTreeSet::from(["bob"]), false));
            db.upsert(
                "notes",
                inserted.row_uuid(),
                cells("new"),
                Default::default(),
            )
            .await
            .unwrap();

            let mut reopened = WatchedRows::new(watch().await.unwrap());
            reopened.rows = before.rows;
            assert_eq!(reopened.changes(owner), (BTreeSet::from(["bob"]), false));
        });
    }
}

#[cfg(test)]
mod event_tests {
    use super::*;

    /// Alice sees a repeated row or space report only after eviction. Exercise
    /// the LRU directly to test the 10,000-entry boundary without 10,000 reads.
    #[test]
    fn unreadable_reports_evict_the_least_recently_observed_row_or_space() {
        let mut published = Published::default();
        let row = |i| Unreadable {
            table: "notes".into(),
            id: Some(RowUuid(uuid::Uuid::from_u128(i))),
            column: Some("body".into()),
            code: UnreadableCode::InvalidCiphertext,
            space: None,
        };
        let space = Unreadable {
            id: None,
            column: None,
            space: Some(DbSpaceAddress {
                scope_id: "projects".into(),
                identifier: "plans".into(),
            }),
            code: UnreadableCode::KeyPending,
            ..row(0)
        };
        assert!(published.insert(space.clone()));
        for i in 1..MAX_REPORTS {
            assert!(published.insert(row(i as u128)));
        }
        assert!(!published.insert(space.clone()));
        assert!(!published.insert(row(1)));
        assert!(published.insert(row(MAX_REPORTS as u128)));
        assert!(!published.insert(space));
        assert!(!published.insert(row(1)));
        assert!(published.insert(row(2)));
    }

    /// Alice sees each event once while its identity is retained. Exercise
    /// the cache directly to reach capacity without creating 10,001 accounts.
    #[test]
    fn only_the_oldest_event_identity_is_evicted() {
        let mut published = Published::default();
        for i in 0..MAX_REPORTS {
            assert!(published.insert_event(i.to_string()));
        }
        assert!(!published.insert_event("0".into()));
        assert!(published.insert_event(MAX_REPORTS.to_string()));
        assert!(!published.insert_event("1".into()));
        assert!(published.insert_event("0".into()));
        published.reset();
        assert!(
            published.insert_event(MAX_REPORTS.to_string()),
            "fresh history after re-enrolment"
        );
    }
}

#[cfg(test)]
mod missing_queue_tests {
    use super::*;

    fn entry(space: &str, n: u8) -> Missing {
        Missing {
            table: "notes".to_owned(),
            id: RowUuid::from_bytes([n; 16]),
            column: "body".to_owned(),
            space: SpaceAddress {
                scope_id: "scope".to_owned(),
                identifier: space.to_owned(),
            },
            epoch_id: "e".to_owned(),
        }
    }

    fn backed_off(space: &str) -> (SpaceAddress, SettleBackoff) {
        (
            entry(space, 0).space,
            SettleBackoff::after_failure(None, web_time::Instant::now()),
        )
    }

    /// A full queue evicts the oldest entry of a space that is not backed
    /// off, never a backed-off space's re-queued rows.
    #[test]
    fn a_full_queue_keeps_a_backed_off_spaces_rows() {
        let backoff = BTreeMap::from([backed_off("held")]);
        let mut queue = VecDeque::new();
        for (space, n) in [("held", 1), ("other", 2), ("other", 3), ("other", 4)] {
            // The fourth evicts the second, which is reported (see
            // `an_evicted_entry_is_reported`).
            let _ = enqueue_missing(&mut queue, &backoff, entry(space, n), 3);
        }
        let ids: Vec<u8> = queue.iter().map(|e| e.id.as_bytes()[0]).collect();
        assert_eq!(ids, [1, 3, 4]);
    }

    /// An entry a full queue evicts is never dropped silently: it will not
    /// be fetched, and its read reports a missing key only once a fetch
    /// settles, so the report it would get is returned for it. A unit test:
    /// filling the queue through reads takes 10,000 rows.
    #[test]
    fn an_evicted_entry_is_reported() {
        let backoff = BTreeMap::new();
        let mut queue = VecDeque::new();
        for n in 1..=3 {
            assert_eq!(
                enqueue_missing(&mut queue, &backoff, entry("first", n), 3),
                None
            );
        }
        let evicted = entry("first", 1);
        assert_eq!(
            enqueue_missing(&mut queue, &backoff, entry("second", 4), 3),
            Some(Report::MissingKey {
                table: evicted.table.clone(),
                space: db_address(&evicted.space),
                epoch_id: evicted.epoch_id,
            })
        );
    }

    /// When every queued entry is backed off, the newcomer is dropped.
    #[test]
    fn a_queue_of_only_backed_off_rows_keeps_its_entries_when_full() {
        let backoff = BTreeMap::from([backed_off("held")]);
        let mut queue = VecDeque::new();
        for n in 1..=3 {
            assert_eq!(
                enqueue_missing(&mut queue, &backoff, entry("held", n), 3),
                None
            );
        }
        let _ = enqueue_missing(&mut queue, &backoff, entry("other", 4), 3);
        let ids: Vec<u8> = queue.iter().map(|e| e.id.as_bytes()[0]).collect();
        assert_eq!(ids, [1, 2, 3]);
    }

    /// A newcomer a full queue of backed-off rows drops is not dropped
    /// silently: it gets the report of a row left out for want of a key.
    #[test]
    fn a_dropped_newcomer_is_reported() {
        let backoff = BTreeMap::from([backed_off("held")]);
        let mut queue = VecDeque::new();
        for n in 1..=3 {
            assert_eq!(
                enqueue_missing(&mut queue, &backoff, entry("held", n), 3),
                None
            );
        }
        let dropped = entry("other", 4);
        assert_eq!(
            enqueue_missing(&mut queue, &backoff, dropped.clone(), 3),
            Some(Report::MissingKey {
                table: dropped.table.clone(),
                space: db_address(&dropped.space),
                epoch_id: dropped.epoch_id,
            })
        );
    }
}

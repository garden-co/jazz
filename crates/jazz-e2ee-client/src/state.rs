//! What the runtime and its hooks share on the database's thread: the
//! device, the held keys, the catalogue identities the cell contexts name,
//! and the reports reads leave for the runtime.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::rc::Rc;

use jazz::db::{Db, ErrorCode};
use jazz::e2ee::{EncryptionEntry, SpaceAddress, Unreadable, UnreadableCode};
use jazz::ids::RowUuid;
use jazz_e2ee::RandomSource;
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::account::build::LocalDevice;
use jazz_e2ee::protocol::content::HeldKeys;
use jazz_e2ee::protocol::domain::{AppDomain, SharedDomain};
use jazz_e2ee::protocol::records::space::SpaceRoot;
use jazz_e2ee::registry::Registry;

use crate::error::ClientError;
use crate::values::ColumnCodec;

/// At most this many reports wait for the runtime; older ones are dropped
/// first (protocol §4.3 keeps a bounded set of the same size).
pub const MAX_REPORTS: usize = 10_000;

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
    /// distinct one once; [`crate::E2eeClient::take_reports`] gives every
    /// occurrence the hooks saw.
    Unreadable(Unreadable),
    /// A row a read left out because this device does not hold the version
    /// its cell names: the runtime acquires it (protocol §4.2).
    MissingKey {
        table: String,
        space: SpaceAddress,
        epoch_id: String,
    },
    /// A record of the private store that failed its load checks and was
    /// skipped (protocol §6.2, §6.3a).
    SkippedRecord { document: String, reason: String },
    /// A delivery to this account that provably failed to open or
    /// authenticate (protocol §3.4): nothing is written; the member stays
    /// `key-pending` until an updater rotates.
    InvalidDelivery {
        space: SpaceAddress,
        sender: String,
        recipient: String,
        key_epoch_id: String,
    },
    /// A key that opened and passed its own version's marker, but whose
    /// history fails further back (protocol §3.4): the fault is the named
    /// successor's author's; the remedy is a recovery rotation.
    BrokenHistory {
        space: SpaceAddress,
        key_epoch_id: String,
        author: String,
    },
    /// A member account whose current generation is above 0: its encryption
    /// was reset (protocol §2.11), once per space, account and generation.
    AccountReset {
        account: String,
        space: SpaceAddress,
        generation: u64,
    },
    /// A space with more member accounts than the design limit
    /// (protocol §3.8), once per space.
    SpaceOverMemberLimit { space: SpaceAddress, members: usize },
    /// A report ([`Report::Unreadable`], per space or per row) that no
    /// longer holds: the key was obtained, or the space now has a valid
    /// root, or the account is now a member (protocol §4.3). `id` and
    /// `column` name the row of a per-row report.
    Withdrawn {
        table: String,
        id: Option<RowUuid>,
        column: Option<String>,
        space: SpaceAddress,
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
            space: unreadable.space.clone(),
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
    tick: u64,
    seen: HashMap<ReportKey, (u64, Unreadable)>,
    order: BTreeMap<u64, ReportKey>,
    listeners: Vec<Listener>,
}

impl Published {
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

    /// Forgets the reports of `space`, per space and per row, with one of
    /// `codes`, returning their withdrawals.
    fn withdraw(&mut self, space: &SpaceAddress, codes: &[UnreadableCode]) -> Vec<Report> {
        let gone: Vec<ReportKey> = self
            .seen
            .keys()
            .filter(|key| key.space.as_ref() == Some(space) && codes.contains(&key.code))
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
                space: space.clone(),
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
    pub(crate) fn resolve(
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
        let mut catalogue = Self::default();
        for (table, plan) in plans {
            for name in [table, &plan.entry.scope] {
                let Some(id) = known(db.table_identity(name), name)? else {
                    return Ok(None);
                };
                catalogue.tables.insert(name.clone(), id.0.to_string());
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
                    db.column_identity(table, column),
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
    pub(crate) catalogue: Option<Catalogue>,
    /// Whether the client is online: offline, equality queries compute no
    /// tokens (protocol §4.5).
    pub(crate) online: bool,
    /// Spaces whose creation by this device was refused while the client
    /// runs (protocol §3.2).
    pub(crate) creation_refused: BTreeSet<jazz_e2ee::protocol::records::space::SpaceAddress>,
    /// Spaces the last pass found to need rotation (protocol §3.5): the
    /// next encrypted write rotates them first.
    pub(crate) rotation_pending: BTreeSet<jazz_e2ee::protocol::records::space::SpaceAddress>,
    /// Spaces whose repair the authority refused (this device may not update
    /// the rows it wrote): the natural moments stop repairing them while the
    /// client runs; an explicit call always attempts (protocol §3.2).
    pub(crate) repair_refused: BTreeSet<jazz_e2ee::protocol::records::space::SpaceAddress>,
    /// Spaces whose rotation the current write already tried: it seals under
    /// the current version whatever came of it, and the mark stays unless
    /// the rotation succeeded or was refused (protocol §3.5). A write the
    /// runtime does not wrap (a host's, retried after
    /// [`crate::E2eeClient::prepare_space`]) takes its space's mark when it
    /// seals, so the next write gets its own rotation chance.
    pub(crate) rotation_tried: BTreeSet<jazz_e2ee::protocol::records::space::SpaceAddress>,
    /// Spaces whose rotation by this device was refused while the client
    /// runs: no automatic rotation, and no evaluation of the mark.
    pub(crate) rotation_refused: BTreeSet<jazz_e2ee::protocol::records::space::SpaceAddress>,
    /// Set once the store is wiped: nothing more is sealed or opened.
    pub(crate) closed: bool,
}

/// This device's creations of spaces in this run (protocol §3.2, §5.4).
#[derive(Default)]
pub(crate) struct Creations {
    /// Roots prepared, with their keys held `unpublished`, that the next
    /// write to their space stages.
    pub(crate) prepared: BTreeMap<jazz_e2ee::protocol::records::space::SpaceAddress, SpaceRoot>,
    /// Roots staged by the call in progress, which the runtime settles
    /// when the call fails.
    pub(crate) attempt: Vec<(jazz_e2ee::protocol::records::space::SpaceAddress, String)>,
    /// Roots staged in this run whose transaction may not have committed
    /// yet: a root absent from the store is not yet known to be lost.
    pub(crate) staged: BTreeSet<String>,
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
    pub(crate) device: RefCell<LocalDevice>,
    pub(crate) keys: RefCell<KeyState>,
    pub(crate) random: RefCell<Box<dyn RandomSource>>,
    pub(crate) reports: RefCell<VecDeque<Report>>,
    /// Reports waiting for the runtime to deliver them to listeners: a
    /// hook never calls out while the database is busy.
    pub(crate) outbox: RefCell<Vec<Report>>,
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
    /// The settle back-off's clock when a test holds it still (`None`: the
    /// real one), so a wait is stepped, not slept.
    pub(crate) held_clock: std::cell::Cell<Option<web_time::Instant>>,
    /// How long a staged member row is waited for (tests shorten it).
    pub(crate) member_change_wait: std::cell::Cell<std::time::Duration>,
    /// The collectors of the repair reads in flight, each registered for the
    /// duration of its read (protocol §3.2).
    pub(crate) collectors: RefCell<BTreeMap<uuid::Uuid, Collector>>,
    /// How many [`crate::E2eeClient::write`] calls are running: their
    /// rounds keep the marks of `rotation_tried` until the call returns.
    pub(crate) wrapped_writes: std::cell::Cell<usize>,
}

/// Queues `missing`, at most `cap` entries. A full queue drops its oldest
/// entry of a space that is not backed off: a backed-off space's entries
/// are the rows its reads are unresolved for (re-queued ahead of newer
/// ones), so they go last. When every entry is backed off, `missing` itself
/// is dropped, never silently: the report a row left out for want of a key
/// gets is returned for it (a later read queues it again).
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
                queue.remove(oldest);
            }
            None => {
                return Some(Report::MissingKey {
                    table: missing.table,
                    space: missing.space,
                    epoch_id: missing.epoch_id,
                });
            }
        }
    }
    queue.push_back(missing);
    None
}

impl Shared {
    pub(crate) fn report(&self, report: Report) {
        {
            let mut outbox = self.outbox.borrow_mut();
            if outbox.len() == MAX_REPORTS {
                outbox.remove(0);
            }
            outbox.push(report.clone());
        }
        let mut reports = self.reports.borrow_mut();
        if reports.len() == MAX_REPORTS {
            reports.pop_front();
        }
        reports.push_back(report);
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

    /// The settle back-off's clock.
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
                        if published.events.len() >= MAX_REPORTS {
                            published.events.clear();
                        }
                        if published.events.insert(key) {
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

    /// Withdraws the reports of `space` with one of `codes`.
    pub(crate) fn withdraw(&self, space: &SpaceAddress, codes: &[UnreadableCode]) {
        let withdrawn = self.published.borrow_mut().withdraw(space, codes);
        self.flush(withdrawn);
    }

    /// Delivers `report` once for `key` (an event is told once per
    /// delivery, version or space, protocol §3.4, §3.8).
    pub(crate) fn event(&self, key: String, report: Report) {
        {
            let mut published = self.published.borrow_mut();
            if published.events.len() >= MAX_REPORTS {
                published.events.clear();
            }
            if !published.events.insert(key) {
                return;
            }
        }
        self.flush(vec![report]);
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
pub(crate) fn db_address(
    address: &jazz_e2ee::protocol::records::space::SpaceAddress,
) -> SpaceAddress {
    SpaceAddress {
        scope_id: address.scope_id.clone(),
        identifier: address.identifier.clone(),
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
            assert_eq!(
                enqueue_missing(&mut queue, &backoff, entry(space, n), 3),
                None
            );
        }
        let ids: Vec<u8> = queue.iter().map(|e| e.id.as_bytes()[0]).collect();
        assert_eq!(ids, [1, 3, 4]);
    }

    /// When every queued entry is backed off, the newcomer is dropped.
    #[test]
    fn a_queue_of_only_backed_off_rows_drops_the_newcomer() {
        let backoff = BTreeMap::from([backed_off("held")]);
        let mut queue = VecDeque::new();
        for n in 1..=3 {
            let _ = enqueue_missing(&mut queue, &backoff, entry("held", n), 3);
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
                table: dropped.table,
                space: dropped.space,
                epoch_id: dropped.epoch_id,
            })
        );
    }
}

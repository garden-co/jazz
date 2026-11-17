//! Subscription fan-out: shared execution and keyed invalidation.
//!
//! Subscriptions whose compiled SQL and parameters are identical share one
//! live result (a `Group`); a session-independent shape (a public feed) is
//! one group however many identities watch it. Each group declares
//! `Watch`es: the tables it reads, narrowed where the query or its read
//! policy pins a column to one value (a root filter `room = R`, a policy
//! `EXISTS memberships WHERE member = session.user`). A commit's changed
//! rows (old and new images) wake only the groups whose watches they hit;
//! woken groups re-run and diff. Watches are sound by construction: a key
//! is used only where every way the table reaches the result goes through
//! that equality; everything else watches the whole table.
//!
//! A table the result reads only through optional array relations is
//! watched by correlation instead ([`Correlation`]): each such relation
//! reads, per parent row the result holds, the rows whose `inner` column
//! holds the parent's `outer` value. A changed row of it can move the
//! result only if the result holds it at that level, or its old or new
//! `inner` value is one the held parents have. Anything this cannot decide
//! (a root that reads the table, a required relation, an offset window, a
//! link that is not a reference) keeps the whole-table watch: refusing is
//! always a full re-run, never a skip.
//!
//! A table read policies read as evidence (a membership `EXISTS`, an
//! `INHERITS` of a referenced row) is watched by those reads
//! ([`Occurrence`]): whatever row a policy is checked for, an `EXISTS
//! members WHERE room = row.room AND person = claims.person` can change
//! value only with a row whose `person` is mine, before or after the
//! write, and only when it is inserted, deleted, or changes a column the
//! `EXISTS` reads. This bounds grants as well as revocations, since it
//! does not depend on what the result holds. A table read this way and
//! through relations is watched both ways, and a change either matches
//! wakes the group. A policy form this cannot bound (a relation `EXISTS`,
//! an update or delete stamp, a recursive `INHERITS` chain) watches every
//! table it reads whole, as do branch views.

use super::*;
use crate::schema::history::{history_source_table, is_history_table};
use std::borrow::Cow;
use std::collections::HashSet;

pub(super) type Queue = Rc<RefCell<VecDeque<SubscriptionEvent>>>;

/// What a group reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Watch {
    Table(String),
    /// Rows of `table` whose `column` holds the canonical value.
    Key(String, String, String),
    /// Rows of `table` that reach the result only through its array
    /// relations: a change wakes the group when one of its
    /// [`Correlation`]s on `table` matches it.
    Related(String),
    /// Rows of `table` that read policies read as evidence: a change wakes
    /// the group when one of its [`Occurrence`]s on `table` matches it.
    Evidence(String),
}

impl Watch {
    fn table(&self) -> &String {
        let (Watch::Table(table)
        | Watch::Key(table, ..)
        | Watch::Related(table)
        | Watch::Evidence(table)) = self;
        table
    }
}

/// One array relation level whose table is watched by correlation: its
/// rows are those of `table` whose `inner` column holds a value of their
/// parent row's `outer` column.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Correlation {
    /// The relation's stored table.
    table: String,
    /// The relation columns from the root to this relation: the parents
    /// are the rows reached through all but the last.
    path: Vec<String>,
    /// The stored `inner` column, or `id`.
    inner: String,
    /// `inner` is a list of references (one per element).
    inner_list: bool,
    /// The parent's `outer` column as its rows name it, or `id`.
    outer: String,
}

/// What a group's result holds at one [`Correlation`]'s level.
///
/// Counted, so that one root row's share can be added and taken away
/// (see `Group::by_root`); a key is held while its count is positive.
#[derive(Clone, Debug, Default, PartialEq)]
struct Held {
    /// The parents' `outer` values, canonical, one per list element.
    parents: HashMap<String, u32>,
    /// The rows the relation holds under them.
    rows: HashMap<RowUuid, u32>,
    /// How many times a parent's value or a row's id could not be read:
    /// while any, every change to the table wakes the group.
    incomplete: u32,
}

impl Held {
    fn complete(&self) -> bool {
        self.incomplete == 0
    }

    fn add(&mut self, other: &Held) {
        for (atom, n) in &other.parents {
            *self.parents.entry(atom.clone()).or_default() += n;
        }
        for (row, n) in &other.rows {
            *self.rows.entry(*row).or_default() += n;
        }
        self.incomplete += other.incomplete;
    }

    fn sub(&mut self, other: &Held) {
        for (atom, n) in &other.parents {
            if let Some(count) = self.parents.get_mut(atom) {
                *count -= (*n).min(*count);
                if *count == 0 {
                    self.parents.remove(atom);
                }
            }
        }
        for (row, n) in &other.rows {
            if let Some(count) = self.rows.get_mut(row) {
                *count -= (*n).min(*count);
                if *count == 0 {
                    self.rows.remove(row);
                }
            }
        }
        self.incomplete -= other.incomplete.min(self.incomplete);
    }

    /// The same keys held, and the same completeness.
    #[cfg(any(test, feature = "testing"))]
    fn same_keys(&self, other: &Held) -> bool {
        self.complete() == other.complete()
            && self.parents.len() == other.parents.len()
            && self.rows.len() == other.rows.len()
            && self
                .parents
                .keys()
                .all(|atom| other.parents.contains_key(atom))
            && self.rows.keys().all(|row| other.rows.contains_key(row))
    }
}

/// Where a set of canonical atoms is collected.
trait AtomSink {
    fn put(&mut self, atom: String);
}

impl AtomSink for HashSet<String> {
    fn put(&mut self, atom: String) {
        self.insert(atom);
    }
}

impl AtomSink for HashMap<String, u32> {
    fn put(&mut self, atom: String) {
        *self.entry(atom).or_default() += 1;
    }
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static CORRELATE: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Turn correlation watches off (`false`: every table a relation reads
/// is watched whole, as before them) on this thread, for A/B runs and
/// differential tests against the full re-run path.
#[cfg(any(test, feature = "testing"))]
pub fn set_correlation_filter_for_test(on: bool) {
    CORRELATE.with(|cell| cell.set(on));
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static POLICY_PINS: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Turn policy evidence pins off (`false`: every table a read policy of
/// the root or of a relation's table reads is watched whole, as before
/// them) on this thread, for A/B runs and differential tests.
#[cfg(any(test, feature = "testing"))]
pub fn set_policy_pins_for_test(on: bool) {
    POLICY_PINS.with(|cell| cell.set(on));
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static FAIL_RECHECKS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Make the next `n` re-checks of root rows by id on this thread fail with
/// a query error (not missing chunks), as a failed read would: the
/// subscription is rejected. For tests of what follows a rejected
/// re-check.
#[cfg(any(test, feature = "testing"))]
pub fn fail_rechecks_for_test(n: u32) {
    FAIL_RECHECKS.with(|cell| cell.set(n));
}

fn policy_pins_enabled() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        POLICY_PINS.with(std::cell::Cell::get)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        true
    }
}

fn correlation_enabled() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        CORRELATE.with(std::cell::Cell::get)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupKey {
    /// The renamed tables and columns of the schema naming the rows:
    /// versions on either side of a rename compile to the same SQL but
    /// label rows differently.
    renames: String,
    sql: String,
    params: String,
    /// Set only when the SQL depends on the identity's session (array
    /// relations, read as the identity, always may).
    identity: Option<String>,
    /// The array relations nested in each row: queries that differ only
    /// in them compile to the same root SQL.
    relations: String,
    /// The parts of large values each row is read in: queries that differ
    /// only in them compile to the same SQL too.
    partial: String,
    /// The columns whose changes updated rows carry as splices.
    splices: Vec<String>,
    /// The position the result reads as of: results of different upstream
    /// reads settle apart.
    position: String,
}

struct Member {
    queue: Weak<RefCell<VecDeque<SubscriptionEvent>>>,
    shape: Shape,
    bound: bool,
    /// Where this member's `settled` flag comes from, and the tier it was
    /// last told it settled at (`None`: not told it settled). A result a
    /// relay settled at its own store is told again once its upstream
    /// settles it globally.
    settled: sync::SettledBy,
    told: std::cell::Cell<Option<DurabilityTier>>,
    joined: std::cell::Cell<Joined>,
}

/// Where a member stands with its group's result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Joined {
    /// Told nothing yet: it waits for an opening of a current result. A
    /// group that awaits chunks holds rows that may be stale, so a member
    /// that joins it then waits with it instead of opening on them.
    AwaitingOpening,
    /// Opened: follows the group's deltas.
    Open,
}

/// One shared live result.
pub(super) struct Group {
    key: GroupKey,
    prepared: PreparedQuery,
    /// The root table's stored name, which changed rows name.
    root: String,
    identity: Option<AuthorSubject>,
    read: ReadSpec,
    pub(super) rows: Vec<CurrentRow>,
    members: Vec<Member>,
    watches: Vec<Watch>,
    /// Per table watched as [`Watch::Evidence`], how its read policies
    /// read it.
    evidence: HashMap<String, Vec<Occurrence>>,
    /// A single-table result (unbounded or a first page) whose rows'
    /// membership depends only on the rows themselves: a commit that
    /// changes only root rows is applied by re-checking those ids, not by
    /// re-running the query.
    recheck: bool,
    /// Every order column is projected, so sort keys come from the rows.
    projected: bool,
    /// Sort keys parallel to `rows`. Built from the rows on the first
    /// re-check when `projected`; otherwise read with the rows, before
    /// any later write can move them.
    keys: Vec<SortKey>,
    /// Optional includes whose target table nothing else in the query
    /// reads: (target, root column). A change to a target row cannot move
    /// the result, only refresh the rows that reference it.
    includes: Vec<(String, String)>,
    /// The last run needed large-value chunks this replica does not hold
    /// yet: the run is retried when chunks arrive, and nothing (opening,
    /// delta or settlement) is published meanwhile.
    awaiting: bool,
    /// Whether the opening was published.
    opened: bool,
    /// Under a branch view, per branched stored table, the branch keys it
    /// reads (its head, and its live base): a change on any other key of
    /// that table cannot move the result.
    branches: HashMap<String, Vec<Vec<u8>>>,
    /// Under a branch view over a base (live or frozen), what the head
    /// rows of each branched table it reads mask (see [`ViewMask`]).
    masks: Vec<ViewMask>,
    /// The stored tables the query reads: a cut is taken only while the
    /// store holds others' versions of them past the result's position.
    tables: BTreeSet<String>,
    /// The cut the last run read through (see [`Store::visible_read`]):
    /// its position and the own commits past it, or `None` for the
    /// current state.
    applied: Option<(sync::Pos, usize)>,
    /// The array relation levels whose tables are watched by correlation
    /// ([`Watch::Related`]), and what the result holds at each, parallel.
    correlations: Vec<Correlation>,
    held: Vec<Held>,
    /// Each root row's share of `held`, with the cells it was read from
    /// (kept alive, so their address names them).
    by_root: Shares,
    /// `held` was counted in one pass over the rows and `by_root` is
    /// empty: the shares are built (see [`Store::share_held`]) only when a
    /// re-check or a re-nest is about to change some rows, so a group that
    /// only re-runs pays one pass per run.
    shares_stale: bool,
    /// While set, [`Group::push`] collects events here instead of
    /// delivering them: one commit's re-check and re-nest are sent as one
    /// delta (see [`compose`]).
    deferred: Option<Vec<SubscriptionEvent>>,
    /// The refresh oracle's view of what the members were sent: the rows
    /// before a refresh, and the events pushed since.
    #[cfg(any(test, feature = "testing"))]
    oracle_before: Option<Vec<CurrentRow>>,
    #[cfg(any(test, feature = "testing"))]
    oracle_pushed: Vec<SubscriptionEvent>,
    /// A re-nest follows this re-check: the refresh oracle compares after
    /// that, not here.
    #[cfg(any(test, feature = "testing"))]
    renest_follows: bool,
    /// When the root table is watched by correlation too (a
    /// self-relation): the root rows the result holds, which wake it
    /// whatever their images say.
    root_held: Option<HashSet<RowUuid>>,
    /// Its relation statements, kept across runs
    /// ([`Store::nest_relations_kept`]).
    plans: crate::db::RelationPlans,
    /// Its re-check's root statement, kept per number of ids
    /// ([`Store::root_plan`]).
    root_plans: RootPlans,
}

/// A re-check's root statement (the query narrowed to the changed ids),
/// kept per number of ids. Kept only where the ids bind as one blob
/// parameter each and nothing else of the statement depends on them
/// ([`Store::build_root_plan`]); `None` marks a count that does not, which
/// compiles as before. At most [`ROOT_PLANS_MAX`] counts are kept: past
/// that the map starts over, so a result re-checking many distinct
/// numbers of ids compiles as it would without it.
#[derive(Default)]
struct RootPlans {
    stamp: Option<crate::db::PlanStamp>,
    by_count: HashMap<usize, Option<(Compiled, Vec<usize>)>>,
    /// Statements reused (for [`FanoutStats::kept_root_plans`]).
    hits: u64,
}

const ROOT_PLANS_MAX: usize = 32;

/// `prepared` narrowed to the root rows `ids` (as a root filter, unordered
/// and unbounded): a re-check's query.
fn narrowed_to(prepared: &PreparedQuery, ids: &[Value]) -> PreparedQuery {
    let mut prepared = prepared.clone();
    prepared.query.filters.push(crate::query::Predicate::In(
        crate::query::Operand::Column("id".to_owned()),
        ids.iter()
            .cloned()
            .map(crate::query::Operand::Literal)
            .collect(),
    ));
    prepared.query.order_by.clear();
    prepared.query.limit = None;
    prepared
}

type SortKey = Vec<(Sql, bool)>;

impl Group {
    /// Under a branch view over a base: what its head rows mask.
    pub(super) fn masks(&self) -> &[ViewMask] {
        &self.masks
    }

    /// Whether the last run waits on large-value chunks still to arrive.
    pub(super) fn awaiting(&self) -> bool {
        self.awaiting
    }

    /// The root table's stored name.
    pub(super) fn root(&self) -> &str {
        &self.root
    }

    /// The stored tables the query reads, its policies' included.
    pub(super) fn tables(&self) -> &BTreeSet<String> {
        &self.tables
    }

    /// The query this group answers.
    pub(super) fn query(&self) -> &Query {
        &self.prepared.query
    }

    fn live(&self) -> bool {
        self.members.iter().any(|m| m.queue.strong_count() > 0)
    }
}

/// Counters for the fan-out bench and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FanoutStats {
    pub groups: usize,
    pub shared_joins: u64,
    /// Groups a commit refreshed, by any path: a re-run, a re-check, or a
    /// re-nest alone (a group woken only through correlated relations).
    pub woken: u64,
    /// Groups a commit did not refresh: every group, less the woken ones.
    pub skipped: u64,
    /// Woken groups refreshed without a re-run: their root rows re-checked
    /// by id, or the relations of the roots a correlated write reached
    /// nested again, or both (a re-check that failed and ended the
    /// subscription counts here too).
    pub rechecked: u64,
    /// Of those, refreshes that nested relations again (a re-nest).
    pub renested: u64,
    /// Of those re-nests, the ones that read only the relations on the
    /// matched levels' paths, every other relation keeping its cells.
    pub renested_narrowly: u64,
    /// Relation statements a live result reused from an earlier run
    /// instead of compiling them again (over the results it holds now).
    pub kept_plans: u64,
    /// Re-check root statements a live result reused instead of compiling
    /// them again (over the results it holds now).
    pub kept_root_plans: u64,
    /// Woken groups that re-ran their query and diffed the whole result.
    pub reran: u64,
    /// Changed rows of a correlation-watched table that a watching group
    /// skipped: no relation level it holds matched them.
    pub uncorrelated: u64,
    /// Changed rows of a table read policies read that a watching group
    /// skipped: no occurrence of it in the group's policies could see the
    /// change.
    pub unevidenced: u64,
}

#[derive(Default)]
pub(super) struct Fanout {
    next: u64,
    groups: HashMap<u64, Rc<RefCell<Group>>>,
    by_key: HashMap<GroupKey, u64>,
    tables: HashMap<String, BTreeSet<u64>>,
    keys: HashMap<(String, String, String), BTreeSet<u64>>,
    key_columns: HashMap<String, BTreeSet<String>>,
    related: HashMap<String, BTreeSet<u64>>,
    evidence: HashMap<String, BTreeSet<u64>>,
    stats: Rc<RefCell<FanoutStats>>,
    /// Bumped by every dropped stream, so `collect` scans only when needed.
    pub(super) dropped: Rc<std::cell::Cell<usize>>,
}

/// The canonical form of a stored SQL value, for watch matching.
fn canon(value: &Sql) -> String {
    value.key()
}

fn canon_value(value: &Value) -> Option<String> {
    to_sql(value).ok().flatten().map(|sql| canon(&sql))
}

/// Enum variants a store keeps for an import (`Db::keep_variants_for_import`),
/// by table and column as the importing schemas name them.
type KeptVariants = BTreeMap<(String, String), BTreeSet<String>>;

/// A value's canonical form ([`canon_value`]) as column `column` stores it,
/// so it matches the cells of a changed row's image: an enum variant's name
/// as its tag. `None` for a variant the store keeps for an import (`kept`)
/// that has no tag yet: it gets its tag only when a row first stores it,
/// after this key would have been made. A key on it could never match the
/// rows it stands for, so the caller watches the table whole instead.
///
/// Any other name with no tag (a typo, a session claim outside the enum)
/// keeps its name: no row can ever hold it, so a key that matches nothing
/// is exact. `kept` is matched by the variant's name alone, in any table
/// and column: the importing schemas may name the table and column other
/// than the reader's schema does, and a wider watch is only extra wakes.
fn stored_canon(
    column: Option<&crate::values::Column>,
    value: String,
    kept: &KeptVariants,
) -> Option<String> {
    let Some(tags) = column.and_then(|column| column.tags.as_ref()) else {
        return Some(value);
    };
    let tags = tags.borrow();
    let tagged = tags
        .iter()
        .find(|(_, name)| canon(&Sql::Text(name.as_bytes().to_vec())) == value)
        .map(|(tag, _)| canon(&Sql::Integer(tag)));
    if tagged.is_some() {
        return tagged;
    }
    let kept = kept
        .values()
        .flatten()
        .any(|name| canon(&Sql::Text(name.as_bytes().to_vec())) == value);
    (!kept).then_some(value)
}

impl Fanout {
    fn index(&mut self, gid: u64, watches: &[Watch]) {
        for watch in watches {
            match watch {
                Watch::Table(table) => {
                    self.tables.entry(table.clone()).or_default().insert(gid);
                }
                Watch::Related(table) => {
                    self.related.entry(table.clone()).or_default().insert(gid);
                }
                Watch::Evidence(table) => {
                    self.evidence.entry(table.clone()).or_default().insert(gid);
                }
                Watch::Key(table, column, value) => {
                    self.keys
                        .entry((table.clone(), column.clone(), value.clone()))
                        .or_default()
                        .insert(gid);
                    self.key_columns
                        .entry(table.clone())
                        .or_default()
                        .insert(column.clone());
                }
            }
        }
    }

    fn unindex(&mut self, gid: u64, watches: &[Watch]) {
        for watch in watches {
            match watch {
                Watch::Table(table) => {
                    if let Some(set) = self.tables.get_mut(table) {
                        set.remove(&gid);
                    }
                }
                Watch::Related(table) => {
                    if let Some(set) = self.related.get_mut(table) {
                        set.remove(&gid);
                        if set.is_empty() {
                            self.related.remove(table);
                        }
                    }
                }
                Watch::Evidence(table) => {
                    if let Some(set) = self.evidence.get_mut(table) {
                        set.remove(&gid);
                        if set.is_empty() {
                            self.evidence.remove(table);
                        }
                    }
                }
                Watch::Key(table, column, value) => {
                    let key = (table.clone(), column.clone(), value.clone());
                    if let Some(set) = self.keys.get_mut(&key) {
                        set.remove(&gid);
                        if set.is_empty() {
                            self.keys.remove(&key);
                        }
                    }
                }
            }
        }
    }

    fn drop_group(&mut self, gid: u64) {
        if let Some(group) = self.groups.remove(&gid) {
            let group = group.borrow();
            self.by_key.remove(&group.key);
            self.unindex(gid, &group.watches);
        }
    }

    /// Forget groups no live subscription holds.
    pub(super) fn collect(&mut self) {
        if self.dropped.replace(0) == 0 {
            return;
        }
        let dead: Vec<u64> = self
            .groups
            .iter()
            .filter(|(_, g)| !g.borrow().live())
            .map(|(id, _)| *id)
            .collect();
        for gid in dead {
            self.drop_group(gid);
        }
        self.key_columns.clear();
        for (table, column, _) in self.keys.keys() {
            self.key_columns
                .entry(table.clone())
                .or_default()
                .insert(column.clone());
        }
    }

    pub(super) fn live_members(&self) -> Vec<(Shape, bool)> {
        self.groups
            .values()
            .flat_map(|g| {
                g.borrow()
                    .members
                    .iter()
                    .filter(|m| m.queue.strong_count() > 0)
                    .map(|m| (m.shape, m.bound))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(super) fn snapshot_stats(&self, stats: FanoutStats) -> FanoutStats {
        let (kept_plans, kept_root_plans) =
            self.groups.values().fold((0, 0), |(plans, roots), group| {
                let group = group.borrow();
                (plans + group.plans.hits, roots + group.root_plans.hits)
            });
        FanoutStats {
            groups: self.groups.len(),
            kept_plans,
            kept_root_plans,
            ..stats
        }
    }

    pub(super) fn stats(&self) -> Rc<RefCell<FanoutStats>> {
        self.stats.clone()
    }

    /// Tell members whose remote answered, or settled them at a higher
    /// tier, without changing their rows.
    pub(super) fn flush_settled(&mut self) {
        for group in self.groups.values() {
            let group = group.borrow();
            if group.awaiting {
                continue;
            }
            for member in &group.members {
                let now = member.settled.tier();
                if member.joined.get() == Joined::Open
                    && member.settled.get()
                    && member.told.get().is_none_or(|told| told < now)
                    && let Some(queue) = member.queue.upgrade()
                {
                    member.told.set(Some(now));
                    tracing::debug!(
                        target: "jazz::sync",
                        table = %group.query().table,
                        tier = ?now,
                        "subscription settled",
                    );
                    let mut event = delta(false, Vec::new(), Vec::new(), Vec::new());
                    if let SubscriptionEvent::Delta { settled, tier, .. } = &mut event {
                        *settled = true;
                        *tier = now;
                    }
                    queue.borrow_mut().push_back(event);
                }
            }
        }
    }
}

impl Store {
    /// Open a subscription: join the group with identical SQL, or start one.
    pub(super) async fn subscribe_member(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: ReadSpec,
        settled: sync::SettledBy,
    ) -> Result<(Queue, Weak<RefCell<Group>>), Error> {
        self.check_splices(prepared, &read.splices)?;
        let (compiled, session_used) = self.compile_tracked(prepared, identity, &read)?;
        let key = GroupKey {
            renames: renames(prepared.source_schema(&self.owner.borrow())),
            sql: compiled.sql.clone(),
            params: format!("{:?}", compiled.params),
            identity: (session_used || !prepared.query.array_subqueries.is_empty())
                .then(|| format!("{identity:?}")),
            relations: format!("{:?}", prepared.query.array_subqueries),
            partial: format!("{:?}", compiled.partial),
            splices: read.splices.clone(),
            position: format!("{:?}", read.position),
        };
        let now = settled.get();
        // `opened`: the member's queue starts with an opening.
        let member = |queue: &Queue, opened: bool| Member {
            queue: Rc::downgrade(queue),
            shape: prepared.shape,
            bound: !prepared.binding.is_empty(),
            settled: settled.clone(),
            told: std::cell::Cell::new((opened && now).then(|| settled.tier())),
            joined: std::cell::Cell::new(if opened {
                Joined::Open
            } else {
                Joined::AwaitingOpening
            }),
        };
        let tier = settled.tier();
        let open = |rows: &[CurrentRow]| {
            let mut event = opening(rows);
            if let SubscriptionEvent::Delta {
                settled, tier: t, ..
            } = &mut event
            {
                *settled = now;
                *t = tier;
            }
            event
        };
        if let Some(handle) = self
            .fan
            .borrow()
            .by_key
            .get(&key)
            .and_then(|gid| self.fan.borrow().groups.get(gid).cloned())
        {
            let mut group = handle.borrow_mut();
            // While the group awaits chunks its rows may be stale: the
            // joiner opens with the group's next successful run instead.
            let opened = group.opened && !group.awaiting;
            let queue: Queue = Rc::new(RefCell::new(if opened {
                VecDeque::from([open(&group.rows)])
            } else {
                VecDeque::new()
            }));
            group.members.push(member(&queue, opened));
            self.fan.borrow().stats.borrow_mut().shared_joins += 1;
            return Ok((queue, Rc::downgrade(&handle)));
        }
        // A replica missing chunks the result needs opens it once they
        // arrive. Its cut is decided over its relations' tables too.
        let tables = self.with_relation_tables(prepared, &compiled.tables)?;
        let visible = self.visible_read(&read, &tables);
        let applied = cut_of(&visible);
        let run = match &visible {
            Cow::Borrowed(_) => self.execute(&compiled, &prepared.query.table).await,
            Cow::Owned(visible) => {
                let compiled = self.compile(prepared, identity, visible)?;
                self.execute(&compiled, &prepared.query.table).await
            }
        };
        let run = match run {
            Ok(rows) => {
                self.nest_relations(prepared, identity, &visible, rows)
                    .await
            }
            Err(error) => Err(error),
        };
        let (rows, awaiting) = match run {
            Ok(rows) => (rows, false),
            Err(error) if crate::large::is_missing(&error) => (Vec::new(), true),
            Err(error) => return Err(error),
        };
        let (watches, correlations, evidence) =
            self.watch_plan(prepared, identity, &read, &compiled)?;
        let root = prepared
            .schema
            .as_ref()
            .unwrap_or(&self.owner.borrow())
            .stored_table(
                history_source_table(&prepared.query.table).unwrap_or(&prepared.query.table),
            )?
            .to_owned();
        // With relations, the watches must also allow it: decided at each
        // refresh (`Group::rechecks`), as `refresh_where` rebuilds them.
        let recheck = recheckable(&prepared.query, &root, &read, &compiled);
        let branches = view_branches(
            prepared.source_schema(&self.owner.borrow()),
            &read,
            &watches,
        );
        let masks = view_masks(
            prepared.source_schema(&self.owner.borrow()),
            prepared,
            &read,
            watches.iter().map(Watch::table),
        );
        let projected = order_projected(&prepared.query, &compiled);
        let includes = self.output_only_includes(prepared, identity, &read, &compiled)?;
        let queue: Queue = Rc::new(RefCell::new(if awaiting {
            VecDeque::new()
        } else {
            VecDeque::from([open(&rows)])
        }));
        let gid = self.fan.borrow().next;
        self.fan.borrow_mut().next += 1;
        self.fan.borrow_mut().index(gid, &watches);
        self.fan.borrow_mut().by_key.insert(key.clone(), gid);
        let handle = Rc::new(RefCell::new(Group {
            key,
            prepared: prepared.clone(),
            root,
            identity,
            read,
            rows,
            members: vec![member(&queue, !awaiting)],
            watches,
            evidence,
            recheck,
            projected,
            keys: Vec::new(),
            includes,
            awaiting,
            opened: !awaiting,
            branches,
            masks,
            tables,
            applied,
            correlations,
            held: Vec::new(),
            by_root: HashMap::new(),
            shares_stale: true,
            deferred: None,
            #[cfg(any(test, feature = "testing"))]
            oracle_before: None,
            #[cfg(any(test, feature = "testing"))]
            oracle_pushed: Vec::new(),
            #[cfg(any(test, feature = "testing"))]
            renest_follows: false,
            root_held: None,
            plans: crate::db::RelationPlans::default(),
            root_plans: RootPlans::default(),
        }));
        Self::hold(&mut handle.borrow_mut(), false);
        self.read_keys(&handle).await;
        let weak = Rc::downgrade(&handle);
        self.fan.borrow_mut().groups.insert(gid, handle);
        Ok((queue, weak))
    }

    /// The optional includes that only shape output: their target is read
    /// by nothing else (no policy, join, array or required include), and
    /// their column is projected.
    fn output_only_includes(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        compiled: &Compiled,
    ) -> Result<Vec<(String, String)>, Error> {
        let query = &prepared.query;
        if query.includes.iter().all(|include| include.require) {
            return Ok(Vec::new());
        }
        let owner_state = self.owner.borrow();
        let schema = prepared.source_schema(&owner_state);
        let root = schema.table(&query.table)?;
        let mut bare = prepared.clone();
        bare.query.includes.retain(|include| include.require);
        let others = self.compile_tracked(&bare, identity, read)?.0.tables;
        let mut out = Vec::new();
        for include in query.includes.iter().filter(|include| !include.require) {
            let column = include.path.split('.').next().unwrap_or_default();
            let Some(target) = root.references.get(column) else {
                continue;
            };
            let target = schema.stored_table(target)?;
            if target != root.stored
                && !others.contains(target)
                && compiled.projection.iter().any(|(name, ..)| name == column)
            {
                out.push((target.to_owned(), column.to_owned()));
            }
        }
        Ok(out)
    }

    /// The stored tables a read of `prepared` as `identity` depends on: what
    /// its live subscription would watch, its compiled SQL reads, and what
    /// those tables' read policies read. A write to any other table cannot
    /// change its result.
    pub(super) fn read_dependencies(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<BTreeSet<String>, Error> {
        self.read_dependencies_in(prepared, identity, &ReadSpec::default())
    }

    /// [`Self::read_dependencies`] of a read in `read`'s view.
    pub(super) fn read_dependencies_in(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<BTreeSet<String>, Error> {
        let (compiled, _) = self.compile_tracked(prepared, identity, read)?;
        let mut tables = compiled.tables.clone();
        for watch in self.watch_plan(prepared, identity, read, &compiled)?.0 {
            tables.insert(watch.table().clone());
        }
        if let Some(author) = identity {
            let owner_state = self.owner.borrow();
            let policies = prepared
                .schema
                .as_ref()
                .map_or(&**owner_state, |schema| schema.policy_schema());
            for table in tables.clone() {
                // Internal tables (history, branches) carry no policy.
                if policies.by_stored(&table).is_some() {
                    tables.extend(self.read_policy_tables(policies, &table, author)?);
                }
            }
        }
        Ok(tables)
    }

    fn compile_tracked(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<(Compiled, bool), Error> {
        let (policy, claims) = self.read_sessions(prepared, identity);
        let owner = self.owner.borrow().clone();
        let compiled = self
            .read_ctx(prepared, &owner, policy.as_ref(), &claims, read)
            .compile_query(&prepared.query, read.include_deleted)?;
        self.refuse_latest_only_cut(&compiled, read)?;
        let used = claims.used.get() || policy.as_ref().is_some_and(|p| p.used.get());
        Ok((compiled, used))
    }

    /// The watches of one subscription (see the module docs for
    /// soundness), the correlations its [`Watch::Related`] tables are
    /// matched by, and the occurrences its [`Watch::Evidence`] tables are.
    fn watch_plan(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        compiled: &Compiled,
    ) -> Result<
        (
            Vec<Watch>,
            Vec<Correlation>,
            HashMap<String, Vec<Occurrence>>,
        ),
        Error,
    > {
        let owner_state = self.owner.borrow();
        let schema = prepared.source_schema(&owner_state);
        // Watches name stored tables and columns: changed rows do.
        let stored = |name: &str| schema.stored_table(name).map(str::to_owned);
        // Under a branch view, inherited rows read with the head's branch
        // columns, so a key on a branch column would miss base changes.
        let keyable = |table: &str, column: &str| {
            read.view.is_none()
                || schema
                    .by_stored(table)
                    .is_some_and(|t| !t.branch_by.iter().any(|c| t.stored_name(c) == column))
        };
        let query = &prepared.query;
        // A history read's rows are its source table's versions.
        let root = schema.table(history_source_table(&query.table).unwrap_or(&query.table))?;
        // Tables the query itself reaches (joins, includes, reachability):
        // these always watch the whole table. Array relations' tables do
        // too unless they are watched by correlation.
        let mut structural = BTreeSet::new();
        fn joins(
            out: &mut BTreeSet<String>,
            list: &[crate::query::JoinVia],
            stored: &dyn Fn(&str) -> Result<String, Error>,
        ) -> Result<(), Error> {
            for join in list {
                out.insert(stored(&join.table)?);
                if let Some(lookup) = &join.source_lookup {
                    out.insert(stored(&lookup.table)?);
                }
                joins(out, &join.nested_joins, stored)?;
            }
            Ok(())
        }
        joins(&mut structural, &query.joins, &stored)?;
        let mut related = BTreeSet::new();
        fn arrays(
            out: &mut BTreeSet<String>,
            list: &[crate::query::ArraySubquery],
            stored: &dyn Fn(&str) -> Result<String, Error>,
        ) -> Result<(), Error> {
            for array in list {
                out.insert(stored(&array.table)?);
                arrays(out, &array.nested_arrays, stored)?;
            }
            Ok(())
        }
        arrays(&mut related, &query.array_subqueries, &stored)?;
        for source in query.flat_join.iter().flat_map(|flat| &flat.sources) {
            let table = &source.table;
            structural.insert(stored(history_source_table(table).unwrap_or(table))?);
        }
        for include in &query.includes {
            let column = include.path.split('.').next().unwrap_or_default();
            if let Some(target) = root.references.get(column) {
                structural.insert(stored(target)?);
            }
        }
        for reach in &query.reachable {
            structural.insert(stored(&reach.access_table)?);
            structural.insert(stored(&reach.edge_table)?);
            if let Some(seed) = &reach.seed {
                structural.insert(stored(&seed.table)?);
            }
        }
        // What the query's structure reads besides the root, compiled with
        // no policy and no relations: `Compiled::tables` lists every
        // relation's table (`validate_array` resolves them), read or not.
        let (_, claims) = self.read_sessions(prepared, identity);
        let mut bare = prepared.clone();
        bare.query.array_subqueries.clear();
        let bare = self
            .read_ctx(&bare, &owner_state, None, &claims, read)
            .compile_query(&bare.query, read.include_deleted)?;
        structural.extend(bare.tables.iter().filter(|t| **t != root.stored).cloned());
        let root_reread = bare.sql.matches(&quote(&root.stored)).count() != 1;
        // What the read policies of every table read (the root's, applied
        // by its SQL; the relations', applied by `compile_children`; the
        // joins'), and how each `EXISTS` bounds the rows it can see.
        let readers: BTreeSet<String> = std::iter::once(root.stored.clone())
            .chain(related.iter().cloned())
            .chain(structural.iter().cloned())
            .collect();
        let evidence = self.evidence(prepared, identity, read, &readers)?;
        let unpinned = |table: &String| matches!(evidence.get(table), Some(None));
        let mut refused: BTreeSet<String> = structural.clone();
        refused.extend(evidence.keys().filter(|t| unpinned(t)).cloned());
        if root_reread {
            refused.insert(root.stored.clone());
        }
        let (correlated, correlations) = self.correlations(prepared, read, compiled, &refused)?;
        let root_key = if structural.contains(&root.stored)
            || root_reread
            || unpinned(&root.stored)
            || (related.contains(&root.stored) && !correlated.contains(&root.stored))
        {
            None
        } else {
            // A read policy that requires `column = session value` of the
            // row itself pins the root too: other rows cannot enter the
            // result.
            let policies = schema.policy_schema();
            let policy_root_key = identity
                .and_then(|author| self.read_sessions(prepared, Some(author)).0)
                .filter(|s| !s.system)
                .and_then(|session| {
                    // The policy names columns as its governing schema does.
                    let governing = policies.by_stored(&root.stored).unwrap_or(root);
                    root.policy(Operation::Select)
                        .and_then(|p| row_pin(governing, p, &session))
                })
                .and_then(|(column, value)| {
                    let governing = policies.by_stored(&root.stored)?;
                    // As for a query key: only an application column is in
                    // a changed row's image (`$createdBy.account` is not).
                    if !governing.columns.iter().any(|c| c.name == column) {
                        return None;
                    }
                    let value = stored_canon(
                        governing.column(&column),
                        value,
                        &self.kept_variants.borrow(),
                    )?;
                    Some((governing.stored_name(&column).to_owned(), value))
                });
            root_key(query, &prepared.binding)
                // Only an application column is in a changed row's image;
                // a key on anything else (`$createdBy.identity.subject`,
                // `id`) would never be hit.
                .filter(|(column, _)| root.columns.iter().any(|c| c.name == *column))
                .and_then(|(column, value)| {
                    let value =
                        stored_canon(root.column(&column), value, &self.kept_variants.borrow())?;
                    Some((root.stored_name(&column).to_owned(), value))
                })
                .or(policy_root_key)
        }
        .filter(|(column, _)| keyable(&root.stored, column));
        // Every table, by how it is read: whole (a table nothing narrower
        // covers), by key (the root's filter), by correlation (a
        // relation's table), by evidence (read policies' bounded reads of
        // it), or several of the last three, which then all wake the group.
        let mut tables: BTreeSet<String> = compiled.tables.clone();
        tables.extend(structural.iter().cloned());
        tables.extend(related.iter().cloned());
        tables.extend(evidence.keys().cloned());
        tables.insert(root.stored.clone());
        let mut watches = Vec::new();
        let mut occurrences = HashMap::new();
        for table in tables {
            let mut narrow = Vec::new();
            let mut whole = structural.contains(&table);
            if table == root.stored {
                match &root_key {
                    Some((column, value)) => {
                        narrow.push(Watch::Key(table.clone(), column.clone(), value.clone()))
                    }
                    None => whole = true,
                }
            }
            if related.contains(&table) {
                if correlated.contains(&table) {
                    narrow.push(Watch::Related(table.clone()));
                } else {
                    whole = true;
                }
            }
            match evidence.get(&table) {
                Some(Some(list)) => {
                    narrow.push(Watch::Evidence(table.clone()));
                    occurrences.insert(table.clone(), list.clone());
                }
                Some(None) => whole = true,
                // Read only by the query itself.
                None => {
                    if table != root.stored && !related.contains(&table) {
                        whole = true;
                    }
                }
            }
            if whole {
                occurrences.remove(&table);
                watches.push(Watch::Table(table));
            } else {
                watches.extend(narrow);
            }
        }
        watches.sort();
        watches.dedup();
        Ok((watches, correlations, occurrences))
    }

    /// The evidence (see [`Evidence`]) the read policies of `readers` read
    /// for `identity`'s session, by stored table: every table one of them
    /// reads, with how each reads it, or unbounded. Empty for a read no
    /// policy applies to. Under a branch view, which reads policies through
    /// its branches, every table they read is unbounded.
    fn evidence(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        readers: &BTreeSet<String>,
    ) -> Result<Evidence, Error> {
        let mut evidence = Evidence::new();
        let Some(author) = identity else {
            return Ok(evidence);
        };
        let Some(session) = self.read_sessions(prepared, Some(author)).0 else {
            return Ok(evidence);
        };
        if session.system {
            return Ok(evidence);
        }
        let owner = self.owner.borrow();
        let schema = prepared.source_schema(&owner);
        let policies = schema.policy_schema();
        for reader in readers {
            let Some(protected) = policies.by_stored(reader) else {
                continue;
            };
            let reads = self.policy_reads(policies, reader, author)?;
            let mut walk = EvidenceWalk {
                policies,
                session: &session,
                kept: &self.kept_variants.borrow(),
                out: Evidence::new(),
                through: Vec::new(),
                reader: reader.clone(),
            };
            let understood = policy_pins_enabled()
                && read.view.is_none()
                && protected.policy(Operation::Select).is_none_or(|policy| {
                    walk.walk(
                        policy,
                        protected,
                        &mut BTreeSet::new(),
                        &mut BTreeSet::new(),
                    )
                });
            // Every table the policy's SQL reads is accounted for: one the
            // walk did not bound (or did not see) is unbounded.
            for table in reads {
                match walk.out.remove(&table) {
                    Some(Some(list)) if understood => {
                        for occurrence in list {
                            add_evidence(&mut evidence, &table, Some(occurrence));
                        }
                    }
                    _ => add_evidence(&mut evidence, &table, None),
                }
            }
        }
        Ok(evidence)
    }

    /// The array relation levels a subscription's tables can be watched
    /// by correlation through, and those tables: a table qualifies when
    /// every relation reading it does and nothing in `refused` reads it.
    /// A relation level qualifies when it is optional (a required one
    /// decides its parent's membership), reads no window past an offset
    /// (rows before the window move it without being held), correlates a
    /// reference with a reference, and its parent rows carry `outer`.
    /// `refused` holds what else reads a table in a way correlation cannot
    /// bound: the root's SQL (compiled without its policy and relations)
    /// reading it, or reading the root again; `structural` (joins,
    /// includes, reachability); a read policy `EXISTS` over it that no
    /// pin bounds (one that is pinned is watched by its pins as well).
    fn correlations(
        &self,
        prepared: &PreparedQuery,
        read: &ReadSpec,
        compiled: &Compiled,
        refused: &BTreeSet<String>,
    ) -> Result<(BTreeSet<String>, Vec<Correlation>), Error> {
        let query = &prepared.query;
        if !correlation_enabled()
            || query.array_subqueries.is_empty()
            || is_history_table(&query.table)
            || query.relation.is_some()
            || query.aggregate.is_some()
            || read.view.is_some()
            || read.cut.is_some()
            || read.settled_only
            || compiled.aggregate
        {
            return Ok((BTreeSet::new(), Vec::new()));
        }
        let owner = self.owner.borrow();
        let schema = prepared.source_schema(&owner);
        let root = schema.table(&query.table)?;
        let mut refused = refused.clone();
        let mut levels: Vec<(Correlation, bool)> = Vec::new();
        let root_columns: Vec<String> = compiled
            .projection
            .iter()
            .map(|(name, ..)| name.to_string())
            .collect();
        relation_levels(
            schema,
            root,
            &root_columns,
            &query.array_subqueries,
            &mut Vec::new(),
            &mut levels,
        )?;
        for (level, fit) in &levels {
            if !fit {
                refused.insert(level.table.clone());
            }
        }
        let correlations: Vec<Correlation> = levels
            .into_iter()
            .map(|(level, _)| level)
            .filter(|level| !refused.contains(&level.table))
            .collect();
        let tables = correlations.iter().map(|c| c.table.clone()).collect();
        Ok((tables, correlations))
    }

    /// The stored tables `table`'s read policy reads, `table` itself
    /// included when the policy reads other rows of it.
    fn policy_reads(
        &self,
        policies: &Schema,
        table: &str,
        author: AuthorSubject,
    ) -> Result<BTreeSet<String>, Error> {
        let Some(protected) = policies.by_stored(table) else {
            return Ok(BTreeSet::new());
        };
        let session = self.session(author);
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(policies, None, &claims, &binding);
        let sql = ctx.operation_policy(protected, Operation::Select, "a", &session)?;
        let mut tables = ctx.tables;
        tables.remove(table);
        if sql.contains(&quote(table)) {
            tables.insert(table.to_owned());
        }
        Ok(tables)
    }

    /// Record what `group`'s result holds at each of its correlation
    /// levels. After a run (`incremental` false) the rows are counted in
    /// one pass and no per-root shares are kept. After a re-check or a
    /// re-nest (`incremental`), whose rows had their shares built first
    /// ([`Store::share_held`]), only the shares of the root rows that
    /// came, left or changed move.
    fn hold(group: &mut Group, incremental: bool) {
        if group.correlations.is_empty() {
            group.held.clear();
            group.by_root.clear();
            group.shares_stale = true;
            group.root_held = None;
            return;
        }
        keep_held(
            &group.correlations,
            &group.rows,
            &mut group.held,
            &mut group.by_root,
            &mut group.shares_stale,
            incremental,
        );
        #[cfg(any(test, feature = "testing"))]
        if oracle::on() {
            for (level, sum) in group.correlations.iter().zip(&group.held) {
                let full = held(level, &group.rows);
                assert!(
                    full.same_keys(sum),
                    "[fanout oracle] kept hold of {} at {:?} differs from a full one: kept {sum:?} full {full:?}",
                    group.root,
                    level.path,
                );
            }
        }
        group.root_held = group
            .correlations
            .iter()
            .any(|level| level.table == group.root)
            .then(|| group.rows.iter().map(|row| row.id).collect());
    }

    /// Build each root row's share of `held` from the rows as they are,
    /// before a re-check or a re-nest changes some of them (see
    /// [`share_rows`]).
    fn share_held(group: &mut Group) {
        share_rows(
            &group.correlations,
            &group.rows,
            &mut group.by_root,
            &mut group.shares_stale,
        );
    }

    /// Wake the groups a commit's changed rows hit, re-run and diff them.
    pub(super) async fn dispatch(&self, mut changes: Vec<(String, RowUuid, Vec<u8>)>) {
        changes.sort();
        // Rows written more than once since the last dispatch (a
        // transaction's repeated writes, a replica applying several
        // pushes of one row in one tick): their history's previous version
        // need not be the one the results last saw, so the images of a
        // correlation or evidence match are not read for them, and every
        // group watching their table that way wakes.
        let repeated: BTreeSet<(String, RowUuid, Vec<u8>)> = changes
            .windows(2)
            .filter(|pair| pair[0] == pair[1])
            .map(|pair| pair[0].clone())
            .collect();
        changes.dedup();
        // Per woken group: the root rows that woke it, or `None` once any
        // other table's row did (then it re-runs).
        let mut woken: BTreeMap<u64, Option<BTreeSet<RowUuid>>> = BTreeMap::new();
        // Per woken group: include targets that changed, as (root column, id).
        let mut targets: BTreeMap<u64, BTreeSet<(String, RowUuid)>> = BTreeMap::new();
        let mut wake = |gids: &BTreeSet<u64>, table: &str, row: RowUuid, key: &[u8]| {
            let fan = self.fan.borrow();
            let groups = &fan.groups;
            for gid in gids {
                let Some(group) = groups.get(gid) else {
                    continue;
                };
                let group = group.borrow();
                if group
                    .branches
                    .get(table)
                    .is_some_and(|keys| !keys.iter().any(|k| k == key))
                {
                    continue;
                }
                let root = group.root == table;
                let entry = woken.entry(*gid).or_insert_with(|| Some(BTreeSet::new()));
                let columns: Vec<&String> = group
                    .includes
                    .iter()
                    .filter(|(target, _)| target == table)
                    .map(|(_, column)| column)
                    .collect();
                if !root && !columns.is_empty() {
                    let found = targets.entry(*gid).or_default();
                    found.extend(columns.into_iter().map(|column| (column.clone(), row)));
                    continue;
                }
                match (entry, root) {
                    (Some(ids), true) => {
                        ids.insert(row);
                    }
                    (entry, _) => *entry = None,
                }
            }
        };
        for (table, row, key) in &changes {
            if let Some(groups) = self.fan.borrow().tables.get(table) {
                wake(groups, table, *row, key);
            }
            let columns = self.fan.borrow().key_columns.get(table).cloned();
            let Some(columns) = columns else {
                continue;
            };
            for image in self.images(table, *row, key, &columns).await {
                for (column, value) in image {
                    if let Some(groups) =
                        self.fan.borrow().keys.get(&(table.clone(), column, value))
                    {
                        wake(groups, table, *row, key);
                    }
                }
            }
        }
        let gone = std::mem::take(&mut *self.gone.borrow_mut());
        for image in &gone {
            let row = RowUuid::from_bytes(image.id);
            if let Some(groups) = self.fan.borrow().tables.get(&image.table) {
                wake(groups, &image.table, row, &image.key);
            }
            let fan_state = self.fan.borrow();
            let Some(columns) = fan_state.key_columns.get(&image.table) else {
                continue;
            };
            for (column, cell) in &image.cells {
                if columns.contains(column)
                    && let Some(groups) = self.fan.borrow().keys.get(&(
                        image.table.clone(),
                        column.clone(),
                        canon(&Sql::from(cell)),
                    ))
                {
                    wake(groups, &image.table, row, &image.key);
                }
            }
        }
        // Tables watched by correlation: the groups whose relation levels
        // hold the row, or a parent its old or new link names.
        let mut uncorrelated = 0;
        // Groups woken only by correlation, with the levels and parents
        // that matched: their roots stand, only the relations of the roots
        // holding a match are nested again (see `Store::renest`).
        let mut scoped: BTreeMap<u64, Vec<(usize, Hit)>> = BTreeMap::new();
        // Groups re-checking a held root row that some relation level also
        // holds (as a child or a parent): re-nested where it is held, after
        // the re-check, instead of a full run.
        let mut selfhits: BTreeMap<u64, Vec<(usize, Hit)>> = BTreeMap::new();
        let mut related = |table: &str, row: RowUuid, images: Option<&[Vec<(String, Sql)>]>| {
            let fan = self.fan.borrow();
            for gid in fan.related.get(table).into_iter().flatten() {
                if woken.get(gid).is_some_and(Option::is_none) {
                    continue;
                }
                let Some(group) = fan.groups.get(gid) else {
                    continue;
                };
                let rechecking = woken
                    .get(gid)
                    .is_some_and(|ids| ids.as_ref().is_some_and(|ids| ids.contains(&row)))
                    && group.borrow().root == table;
                if rechecking
                    && group
                        .borrow()
                        .root_held
                        .as_ref()
                        .is_some_and(|held| held.contains(&row))
                {
                    match correlated_hits(&group.borrow(), table, row, images, false) {
                        None => {
                            selfhits.remove(gid);
                            woken.insert(*gid, None);
                        }
                        Some(hits) if !hits.is_empty() => {
                            selfhits.entry(*gid).or_default().extend(hits);
                        }
                        Some(_) => uncorrelated += 1,
                    }
                    continue;
                }
                if !woken.contains_key(gid) {
                    match correlated_hits(&group.borrow(), table, row, images, true) {
                        None => {
                            scoped.remove(gid);
                            woken.insert(*gid, None);
                        }
                        Some(hits) if !hits.is_empty() => {
                            scoped.entry(*gid).or_default().extend(hits);
                        }
                        Some(_) => uncorrelated += 1,
                    }
                    continue;
                }
                if correlated(&group.borrow(), table, row, images) {
                    woken.insert(*gid, None);
                } else {
                    uncorrelated += 1;
                }
            }
        };
        for (table, row, key) in &changes {
            let Some(gids) = self.fan.borrow().related.get(table).cloned() else {
                continue;
            };
            let columns: BTreeSet<String> = gids
                .iter()
                .filter_map(|gid| self.fan.borrow().groups.get(gid).cloned())
                .flat_map(|group| {
                    group
                        .borrow()
                        .correlations
                        .iter()
                        .filter(|level| level.table == *table && level.inner != "id")
                        .map(|level| level.inner.clone())
                        .collect::<Vec<_>>()
                })
                .collect();
            let images = if columns.is_empty() {
                Some(Vec::new())
            } else {
                self.stored_images(table, *row, key, &columns).await
            };
            let images = images.filter(|_| !repeated.contains(&(table.clone(), *row, key.clone())));
            related(table, *row, images.as_deref());
        }
        for image in &gone {
            if self.fan.borrow().related.contains_key(&image.table) {
                let cells: Vec<(String, Sql)> = image
                    .cells
                    .iter()
                    .map(|(column, cell)| (column.clone(), Sql::from(cell)))
                    .collect();
                related(
                    &image.table,
                    RowUuid::from_bytes(image.id),
                    Some(std::slice::from_ref(&cells)),
                );
            }
        }
        // Tables read policies read as evidence: the groups one of whose
        // occurrences on the table the row meets and changes under.
        let mut unread = 0;
        let mut evident = |table: &str, images: Option<&[Vec<(String, Sql)>]>, gone: bool| {
            let fan = self.fan.borrow();
            for gid in fan.evidence.get(table).into_iter().flatten() {
                if woken.get(gid).is_some_and(Option::is_none) {
                    continue;
                }
                let Some(group) = fan.groups.get(gid) else {
                    continue;
                };
                if evidenced(&group.borrow(), table, images, gone) {
                    woken.insert(*gid, None);
                } else {
                    unread += 1;
                }
            }
        };
        for (table, row, key) in &changes {
            let Some(gids) = self.fan.borrow().evidence.get(table).cloned() else {
                continue;
            };
            let mut columns: BTreeSet<String> = gids
                .iter()
                .filter_map(|gid| self.fan.borrow().groups.get(gid).cloned())
                .flat_map(|group| {
                    group
                        .borrow()
                        .evidence
                        .get(table)
                        .into_iter()
                        .flatten()
                        .flat_map(|o| o.columns.iter().chain(o.pin.iter().map(|(c, _)| c)))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .collect();
            columns.insert("_state".to_owned());
            let images = self.stored_images(table, *row, key, &columns).await;
            let images = images.filter(|_| !repeated.contains(&(table.clone(), *row, key.clone())));
            evident(table, images.as_deref(), false);
        }
        for image in &gone {
            if self.fan.borrow().evidence.contains_key(&image.table) {
                let cells: Vec<(String, Sql)> = image
                    .cells
                    .iter()
                    .map(|(column, cell)| (column.clone(), Sql::from(cell)))
                    .collect();
                evident(&image.table, Some(std::slice::from_ref(&cells)), true);
            }
        }
        self.fan.borrow().stats.borrow_mut().unevidenced += unread;
        self.fan.borrow().stats.borrow_mut().uncorrelated += uncorrelated;
        // Woken any other way too (an evidence match after the
        // correlation one): those re-run below. Nothing else can enter
        // `woken` after `related`: root ids and include targets come from
        // `wake`, before it.
        scoped.retain(|gid, _| !woken.contains_key(gid));
        // A group refreshed only by a re-nest is woken too.
        let refreshed = woken.len() + scoped.len();
        self.fan.borrow().stats.borrow_mut().woken += refreshed as u64;
        self.fan.borrow().stats.borrow_mut().skipped +=
            (self.fan.borrow().groups.len() - refreshed) as u64;
        let groups: Vec<_> = woken
            .into_iter()
            .filter_map(|(gid, ids)| Some((gid, self.fan.borrow().groups.get(&gid).cloned()?, ids)))
            .collect();
        for (gid, group, ids) in groups {
            // Only include targets changed: the result's rows are as before.
            if ids.as_ref().is_none_or(|ids| !ids.is_empty()) {
                #[cfg(any(test, feature = "testing"))]
                {
                    group.borrow_mut().renest_follows =
                        selfhits.contains_key(&gid) && ids.is_some();
                    oracle::begin(&mut group.borrow_mut());
                }
                // A re-nest may follow the re-check: both are held back and
                // sent as one delta, so no member sees the re-checked rows
                // beside the relations they are about to change.
                let pre_ids: Option<Vec<RowUuid>> = (selfhits.contains_key(&gid) && ids.is_some())
                    .then(|| group.borrow().rows.iter().map(|row| row.id).collect());
                if pre_ids.is_some() {
                    group.borrow_mut().deferred = Some(Vec::new());
                }
                let refreshed = self.refresh(&group, ids.as_ref()).await;
                #[cfg(any(test, feature = "testing"))]
                {
                    group.borrow_mut().renest_follows = false;
                }
                let mut reran = matches!(refreshed, Refresh::Reran);
                // After a re-check that applied: a rejection ended the
                // subscription, a re-run read the relations already.
                if let Some(hits) = selfhits.remove(&gid)
                    && matches!(refreshed, Refresh::Rechecked)
                {
                    if let Some(narrowly) = self.renest(&group, &hits).await {
                        self.fan.borrow().stats.borrow_mut().renested += 1;
                        self.fan.borrow().stats.borrow_mut().renested_narrowly +=
                            u64::from(narrowly);
                        #[cfg(any(test, feature = "testing"))]
                        oracle::compare(self, &group, "selfhit").await;
                    } else {
                        self.refresh_group(&group, None).await;
                        reran = true;
                    }
                }
                if let Some(pre_ids) = pre_ids {
                    let events = group.borrow_mut().deferred.take().unwrap_or_default();
                    let mut group = group.borrow_mut();
                    for event in compose(&pre_ids, events, &group.rows) {
                        group.push(event);
                    }
                }
                #[cfg(any(test, feature = "testing"))]
                oracle::finish(&mut group.borrow_mut(), !reran);
                if reran {
                    self.fan.borrow().stats.borrow_mut().reran += 1;
                } else {
                    self.fan.borrow().stats.borrow_mut().rechecked += 1;
                }
            }
            if let Some(changed) = targets.remove(&gid) {
                refresh_includes(&mut group.borrow_mut(), &changed);
            }
        }
        for (gid, hits) in scoped {
            let Some(group) = self.fan.borrow().groups.get(&gid).cloned() else {
                continue;
            };
            #[cfg(any(test, feature = "testing"))]
            oracle::begin(&mut group.borrow_mut());
            let renested = self.renest(&group, &hits).await;
            if let Some(narrowly) = renested {
                self.fan.borrow().stats.borrow_mut().rechecked += 1;
                self.fan.borrow().stats.borrow_mut().renested += 1;
                self.fan.borrow().stats.borrow_mut().renested_narrowly += u64::from(narrowly);
                #[cfg(any(test, feature = "testing"))]
                oracle::compare(self, &group, "renest").await;
            } else {
                self.refresh_group(&group, None).await;
                self.fan.borrow().stats.borrow_mut().reran += 1;
            }
            #[cfg(any(test, feature = "testing"))]
            oracle::finish(&mut group.borrow_mut(), renested.is_some());
            // No include targets: those are found by `wake`, which puts
            // the group in `woken`.
        }
    }

    /// Re-run every group (schema changed); watches are rebuilt because
    /// they can depend on session values.
    /// The active schema changed: every subscription re-runs and restates
    /// its rows (a reset), since their shape may have changed.
    pub(super) async fn reset_everything(&self) {
        self.clear_check_plans();
        self.refresh_where(|_| true, true).await;
    }

    /// `author`'s claims changed: re-run only the groups whose policy or
    /// claim operands read them. A group with no policy identity takes its
    /// claims from this node's own author. Only request claims matching
    /// that effective author are fixed; unrelated scopes still use the registry.
    pub(super) async fn refresh_identity(&self, author: AuthorSubject) {
        let own = self.identity.author;
        self.refresh_where(
            |group| {
                let effective = group.identity.unwrap_or(own);
                effective == author && group.prepared.fixed_claims_for(effective).is_none()
            },
            false,
        )
        .await;
    }

    async fn refresh_where(&self, affected: impl Fn(&Group) -> bool, reset: bool) {
        let groups: Vec<(u64, Rc<RefCell<Group>>)> = self
            .fan
            .borrow()
            .groups
            .iter()
            .filter(|(_, g)| affected(&g.borrow()))
            .map(|(id, g)| (*id, g.clone()))
            .collect();
        for (gid, group) in groups {
            {
                let mut group = group.borrow_mut();
                // A sync session's schema is renamed for the new version and
                // keeps whoever governs it: its bundle's policies when it is
                // pinned to one, else whichever schema is current now.
                if reset
                    && let Some(pinned) = &group.prepared.schema
                    && let Some(policies) = &pinned.policies_from
                    && let Some(governed) = self.regoverned(pinned.jazz.version_id(), policies)
                {
                    group.prepared.schema = Some(governed);
                }
                let old = std::mem::take(&mut group.watches);
                self.fan.borrow_mut().unindex(gid, &old);
                // Correlations name stored columns and compare stored values,
                // which a schema change can rename or renumber: they are
                // rebuilt with the watches, and dropped (the tables watched
                // whole) when the watches cannot be.
                let (watches, correlations, evidence) = self
                    .compile(&group.prepared, group.identity, &group.read)
                    .and_then(|compiled| {
                        self.watch_plan(&group.prepared, group.identity, &group.read, &compiled)
                    })
                    .unwrap_or_else(|_| {
                        let watches = old
                            .into_iter()
                            .map(|watch| match watch {
                                Watch::Related(table) | Watch::Evidence(table) => {
                                    Watch::Table(table)
                                }
                                watch => watch,
                            })
                            .collect();
                        (watches, Vec::new(), HashMap::new())
                    });
                self.fan.borrow_mut().index(gid, &watches);
                group.watches = watches;
                group.evidence = evidence;
                group.correlations = correlations;
                group.by_root.clear();
                group.held.clear();
                // Claims or the schema changed: nothing compiled before holds.
                group.plans.clear();
                group.root_plans.stamp = None;
                group.root_plans.by_count.clear();
                Self::hold(&mut group, false);
            }
            if reset {
                self.reset_group(&group).await;
            } else {
                self.refresh_group(&group, None).await;
            }
        }
    }

    /// Old (pre-commit, from history) and new (current) values of the
    /// watched key columns of one changed row. "New" covers the row on every
    /// branch: a copy-on-write head row replaces, in a branch view, the base
    /// row a subscription saw, so the base image must wake it too.
    pub(super) async fn images(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        columns: &BTreeSet<String>,
    ) -> Vec<Vec<(String, String)>> {
        self.stored_images(table, row, key, columns)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|image| {
                image
                    .into_iter()
                    .map(|(column, cell)| (column, canon(&cell)))
                    .collect()
            })
            .collect()
    }

    /// [`Self::images`] as stored cells; `None` when they could not be read.
    async fn stored_images(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        columns: &BTreeSet<String>,
    ) -> Option<Vec<Vec<(String, Sql)>>> {
        let read = async |sql: String,
                          params: &[jazz_sql::Param<'_>]|
               -> Option<Vec<(i64, Vec<u8>, Vec<(String, Sql)>)>> {
            // Names from the first row: a cached `SELECT *` prepared before
            // its table gained a column names the old columns until a step
            // re-prepares it.
            let mut names: Vec<String> = Vec::new();
            let mut rows = Vec::new();
            let result = self
                .conn
                .sql()
                .query(&sql, params, |r| {
                    let decoded = (|| -> Result<_, jazz_sql::CellError> {
                        if names.is_empty() {
                            names = (0..r.len())
                                .map(|i| {
                                    r.column_name(i)
                                        .map(str::to_owned)
                                        .ok_or(jazz_sql::CellError("missing SQL column name"))
                                })
                                .collect::<Result<_, _>>()?;
                        }
                        let mut seq = 0i64;
                        let mut branch = Vec::new();
                        let mut out = Vec::new();
                        for (i, name) in names.iter().enumerate() {
                            if name == "_clock" {
                                seq = r.get(i)?;
                            } else if name == "_branch" {
                                branch = r.get(i)?;
                            } else if columns.contains(name) {
                                out.push((name.clone(), r.get::<Sql>(i)?));
                            }
                        }
                        Ok((seq, branch, out))
                    })();
                    rows.push(decoded?);
                    Ok(std::ops::ControlFlow::Continue(()))
                })
                .await;
            result.ok()?;
            Some(rows)
        };
        let id = row.0.as_bytes().to_vec();
        let current = read(
            format!("SELECT * FROM {} WHERE id = ?1", quote(table)),
            jazz_sql::params![id],
        )
        .await?;
        let seq = current
            .iter()
            .find(|(_, branch, _)| branch.as_slice() == key)
            .map_or(i64::MAX, |(seq, _, _)| *seq);
        let old = read(
            format!(
                "SELECT * FROM {} WHERE id = ?1 AND _branch = ?2 AND _clock < ?3 \
                 AND (_state & {}) = 0 ORDER BY _clock DESC LIMIT 1",
                quote(&format!("{table}__h")),
                crate::values::STATE_AS_SENT
            ),
            jazz_sql::params![id, key, seq],
        )
        .await?;
        Some(
            current
                .into_iter()
                .chain(old)
                .map(|(_, _, image)| image)
                .collect(),
        )
    }

    /// The re-check statement for `ids`, from the kept one for their
    /// number (built on a first use, see [`Self::build_root_plan`]) with
    /// `ids` bound in; `None`: compile it as usual.
    fn root_plan(
        &self,
        plans: &mut RootPlans,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        ids: &BTreeSet<RowUuid>,
        read: &ReadSpec,
    ) -> Option<Compiled> {
        // The sessions a compile would resolve anyway; the kept stamp is
        // compared with them where they stand.
        let sessions = self.read_sessions(prepared, identity);
        if !crate::db::PlanStamp::keep(
            &mut plans.stamp,
            &self.plan_inputs(prepared, &sessions, read),
        ) {
            plans.by_count.clear();
        }
        let n = ids.len();
        let kept = match plans.by_count.get(&n) {
            Some(kept) => {
                plans.hits += u64::from(kept.is_some());
                kept.clone()
            }
            None => {
                let built = self.build_root_plan(prepared, identity, n, read);
                if plans.by_count.len() >= ROOT_PLANS_MAX {
                    plans.by_count.clear();
                }
                plans.by_count.insert(n, built.clone());
                built
            }
        };
        let (mut compiled, slots) = kept?;
        // `id IN (...)` does not care in which order its ids come: the
        // slots are filled in the set's order, and nothing reads a
        // position as naming a particular id.
        for (slot, id) in slots.iter().zip(ids) {
            compiled.params[*slot] = Sql::Blob(id.0.as_bytes().to_vec());
        }
        Some(compiled)
    }

    /// The re-check statement for `n` ids, kept with the parameters that
    /// hold them: compiled for two sets of `n` sentinel ids, and kept only
    /// when the two are one statement whose parameters differ in exactly
    /// `n` places, each holding the matching sentinel as a blob (so the
    /// ids are bound literals and nothing else depends on them).
    fn build_root_plan(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        n: usize,
        read: &ReadSpec,
    ) -> Option<(Compiled, Vec<usize>)> {
        let sentinels = |tag: u8| -> Vec<[u8; 16]> {
            (0..n)
                .map(|k| {
                    let mut id = [tag; 16];
                    id[8..].copy_from_slice(&(k as u64).to_be_bytes());
                    id
                })
                .collect()
        };
        let compile = |ids: &[[u8; 16]]| {
            let ids: Vec<Value> = ids
                .iter()
                .map(|id| Value::Uuid(uuid::Uuid::from_bytes(*id)))
                .collect();
            self.compile(&narrowed_to(prepared, &ids), identity, read)
                .ok()
        };
        let (a, b) = (sentinels(0xa5), sentinels(0x5a));
        let (plan_a, plan_b) = (compile(&a)?, compile(&b)?);
        if plan_a.sql != plan_b.sql || plan_a.params.len() != plan_b.params.len() {
            return None;
        }
        let slots: Vec<usize> = (0..plan_a.params.len())
            .filter(|at| plan_a.params[*at] != plan_b.params[*at])
            .collect();
        let bound = slots.len() == n
            && slots.iter().enumerate().all(|(k, at)| {
                plan_a.params[*at] == Sql::Blob(a[k].to_vec())
                    && plan_b.params[*at] == Sql::Blob(b[k].to_vec())
            });
        bound.then_some((plan_a, slots))
    }

    /// Apply a commit that changed only the root rows `ids` to a result in
    /// place: re-check those ids and splice them in by the query's order
    /// (sort keys are kept per row), emitting a delta for just those rows.
    /// O(changed · log n) comparisons plus memmoves, not a re-run and full
    /// diff. A first page (`limit`, no offset) keeps its first `limit`
    /// rows; one that was full and shrank re-runs, since it never held the
    /// rows after it. `None` when the order cannot be compared from the
    /// rows (then the caller re-runs).
    async fn recheck_rows(
        &self,
        cell: &RefCell<Group>,
        ids: &BTreeSet<RowUuid>,
    ) -> Option<Result<Option<SubscriptionEvent>, Error>> {
        #[cfg(any(test, feature = "testing"))]
        if FAIL_RECHECKS.with(|cell| {
            let left = cell.get();
            cell.set(left.saturating_sub(1));
            left > 0
        }) {
            return Some(Err(Error::new(ErrorCode::Query, "re-check failed (test)")));
        }
        let (order, projected, read, narrowed, prepared, identity, mut plans) = {
            let mut group = cell.borrow_mut();
            let order = group.prepared.query.order_by.clone();
            let projected = group.projected;
            if group.keys.len() != group.rows.len() {
                if !projected {
                    return None;
                }
                group.keys = match self.projected_keys(&group.prepared, &order, &group.rows)? {
                    Ok(keys) => keys,
                    Err(error) => return Some(Err(error)),
                };
            }
            // Under a cut, a row changed past it still reads as it was. The
            // other rows were read through the cut the last run applied: under
            // another, only a full run (which records it) keeps them one state.
            let read = self.visible_read(&group.read, &group.tables).into_owned();
            if cut_of(&read) != group.applied {
                return None;
            }
            // The same query narrowed to the changed ids, as a root filter, so
            // SQLite seeks them by primary key instead of sorting the view.
            let ids_as_values: Vec<Value> = ids.iter().map(|id| Value::Uuid(id.0)).collect();
            let prepared = group.prepared.clone();
            let identity = group.identity;
            let kept = if read.view.is_none() && read.cut.is_none() {
                self.root_plan(&mut group.root_plans, &prepared, identity, ids, &read)
            } else {
                None
            };
            let narrowed = match kept {
                Some(kept) => {
                    if crate::db::plan_oracle() {
                        let fresh = self
                            .compile(
                                &narrowed_to(&group.prepared, &ids_as_values),
                                group.identity,
                                &read,
                            )
                            .ok();
                        assert!(
                            fresh.is_some_and(|fresh| crate::db::same_compiled(&kept, &fresh)),
                            "a kept re-check statement differs from a fresh one on {}",
                            group.root
                        );
                    }
                    kept
                }
                None => match self.compile(
                    &narrowed_to(&group.prepared, &ids_as_values),
                    group.identity,
                    &read,
                ) {
                    Ok(compiled) => compiled,
                    Err(error) => return Some(Err(error)),
                },
            };
            (
                order,
                projected,
                read,
                narrowed,
                group.prepared.clone(),
                group.identity,
                std::mem::take(&mut group.plans),
            )
        };
        let fresh = match self.execute(&narrowed, &prepared.query.table).await {
            Ok(rows) => rows,
            Err(error) => {
                cell.borrow_mut().plans = plans;
                return Some(Err(error));
            }
        };
        let fresh = self
            .nest_relations_kept(&prepared, identity, &read, fresh, &mut plans)
            .await;
        cell.borrow_mut().plans = plans;
        let fresh = match fresh {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        let keys = if projected {
            self.projected_keys(&prepared, &order, &fresh)
        } else {
            self.sort_keys(
                &prepared,
                &order,
                &fresh.iter().map(|row| row.id).collect::<Vec<_>>(),
            )
            .await
        };
        let fresh_keys = match keys? {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
        let mut group = cell.borrow_mut();
        let fresh: Vec<(SortKey, CurrentRow)> = fresh_keys.into_iter().zip(fresh).collect();
        // Before: where each changed id sat, and what it showed.
        let before: HashMap<RowUuid, (usize, CurrentRow, SortKey)> = group
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| ids.contains(&row.id))
            .map(|(i, row)| (row.id, (i, row.clone(), group.keys[i].clone())))
            .collect();
        let limit = group.prepared.query.limit;
        let page_ids_before: Vec<RowUuid> = if limit.is_some() {
            group.rows.iter().map(|row| row.id).collect()
        } else {
            Vec::new()
        };
        let full_before = limit.is_some_and(|limit| group.rows.len() >= limit);
        // A full page only knows the rows up to its last one: a changed
        // row that now sorts after it may have unseen rows before it.
        let boundary = full_before.then(|| group.keys.last().cloned()).flatten();
        let fresh: Vec<(SortKey, CurrentRow)> = fresh
            .into_iter()
            .filter(|(key, _)| {
                boundary
                    .as_ref()
                    .is_none_or(|last| sql_order(key, last) != std::cmp::Ordering::Greater)
            })
            .collect();
        // A full page that would shrink lost rows whose successors it
        // never held: only a re-run can refill it. Decided before any
        // splice, so the caller re-runs against the result as it was.
        if let Some(limit) = limit
            && full_before
            && group.rows.len() - before.len() + fresh.len() < limit
        {
            return None;
        }
        let mut positions: Vec<usize> = before.values().map(|(i, _, _)| *i).collect();
        positions.sort_unstable_by(|a, b| b.cmp(a));
        for at in positions {
            group.rows.remove(at);
            group.keys.remove(at);
        }
        let fresh_ids: BTreeSet<RowUuid> = fresh.iter().map(|(_, row)| row.id).collect();
        for (key, row) in fresh {
            let at = group
                .keys
                .partition_point(|other| sql_order(other, &key) == std::cmp::Ordering::Less);
            group.keys.insert(at, key);
            group.rows.insert(at, row);
        }
        // A first page keeps its first `limit` rows; unchanged rows pushed
        // past the end leave it.
        let mut pushed_off = Vec::new();
        if let Some(limit) = limit
            && group.rows.len() > limit
        {
            group.keys.truncate(limit);
            pushed_off = group.rows.split_off(limit);
        }
        let fresh_ids: BTreeSet<RowUuid> = fresh_ids
            .into_iter()
            .filter(|id| !pushed_off.iter().any(|row| row.id == *id))
            .collect();
        let mut added = Vec::new();
        let mut updated = Vec::new();
        for (index, (row, key)) in group
            .rows
            .iter()
            .zip(&group.keys)
            .enumerate()
            .filter(|(_, (row, _))| fresh_ids.contains(&row.id))
        {
            match before.get(&row.id) {
                None => added.push(SubscriptionOutputRow {
                    row: row.clone(),
                    previous_index: None,
                    index,
                    splices: Vec::new(),
                }),
                // A row whose order key moved, or that a same-value write
                // gave a newer version, is reported even when its shown
                // cells did not change.
                Some((previous, prior, prior_key))
                    if prior.cells != row.cells
                        || prior.deleted != row.deleted
                        || new_version(prior, row)
                        || prior_key != key =>
                {
                    updated.push(SubscriptionOutputRow {
                        row: row.clone(),
                        previous_index: Some(*previous),
                        index,
                        splices: Vec::new(),
                    })
                }
                Some(_) => {}
            }
        }
        // Changed rows that left, and unchanged rows a page pushed off.
        let mut removed: Vec<RemovedRow> = before
            .iter()
            .filter(|(id, _)| !fresh_ids.contains(id))
            .map(|(id, (index, row, _))| RemovedRow {
                table: row.table.to_string(),
                row_uuid: *id,
                joined: Vec::new(),
                version: None,
                union_arm: None,
                index: *index,
            })
            .collect();
        if !pushed_off.is_empty() {
            let index_before: HashMap<RowUuid, usize> = page_ids_before
                .iter()
                .enumerate()
                .map(|(i, id)| (*id, i))
                .collect();
            removed.extend(pushed_off.iter().filter_map(|row| {
                let index = *index_before.get(&row.id)?;
                (!before.contains_key(&row.id)).then(|| RemovedRow {
                    table: row.table.to_string(),
                    row_uuid: row.id,
                    joined: Vec::new(),
                    version: None,
                    union_arm: None,
                    index,
                })
            }));
        }
        removed.sort_by_key(|row| row.index);
        if added.is_empty() && updated.is_empty() && removed.is_empty() {
            return Some(Ok(None));
        }
        Some(Ok(Some(delta(false, added, updated, removed))))
    }

    /// The sort keys of `rows` in the query's order (ties broken by id):
    /// from their cells when every order column is projected, else read
    /// back by id. `None` when a value cannot be compared from SQL.
    fn projected_keys(
        &self,
        prepared: &PreparedQuery,
        order: &[crate::query::OrderBy],
        rows: &[CurrentRow],
    ) -> Option<Result<Vec<SortKey>, Error>> {
        // Same tie-break as the compiled ORDER BY: id in the last key's
        // direction.
        let tie_desc = matches!(
            order.last().map(|by| by.direction),
            Some(crate::query::OrderDirection::Desc)
        );
        let id_key = |row: &CurrentRow| (Sql::Blob(row.id.0.as_bytes().to_vec()), tie_desc);
        let owner_state = self.owner.borrow();
        let schema = prepared.source_schema(&owner_state);
        // Cells sort as the compiled ORDER BY sorts them: an enum
        // variant by its tag, or its rank in a schema whose declaration
        // order is not tag order.
        let root = schema.tables.get(&prepared.query.table)?;
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let mut key = Vec::with_capacity(order.len() + 1);
            for by in order {
                let value = if by.column == "id" {
                    Sql::Blob(row.id.0.as_bytes().to_vec())
                } else {
                    match row
                        .cells
                        .iter()
                        .find(|(name, _)| *name == by.column)?
                        .1
                        .as_ref()
                    {
                        None => Sql::Null,
                        // A value over the inline threshold sorts by its
                        // chunked cell, which the row does not carry.
                        Some(value) => match root
                            .column(&by.column)
                            .map_or_else(|| to_sql(value), |column| column.order_cell(value))
                            .ok()??
                        {
                            Sql::Text(bytes) | Sql::Blob(bytes)
                                if bytes.len()
                                    > root.column(&by.column).map_or(
                                        jazz_types::large_values::INLINE_VALUE_MAX_BYTES,
                                        crate::large::inline_max,
                                    ) =>
                            {
                                return None;
                            }
                            value => value,
                        },
                    }
                };
                key.push((
                    value,
                    matches!(by.direction, crate::query::OrderDirection::Desc),
                ));
            }
            key.push(id_key(row));
            keys.push(key);
        }
        Some(Ok(keys))
    }

    async fn sort_keys(
        &self,
        prepared: &PreparedQuery,
        order: &[crate::query::OrderBy],
        rows: &[RowUuid],
    ) -> Option<Result<Vec<SortKey>, Error>> {
        // Same tie-break as the compiled ORDER BY: id in the last key's
        // direction.
        let tie_desc = matches!(
            order.last().map(|by| by.direction),
            Some(crate::query::OrderDirection::Desc)
        );
        let id_key = |row: &RowUuid| (Sql::Blob(row.0.as_bytes().to_vec()), tie_desc);
        let owner_state = self.owner.borrow().clone();
        let schema = prepared.source_schema(&owner_state);
        if rows.is_empty() {
            return Some(Ok(Vec::new()));
        }
        let table = schema.tables.get(&prepared.query.table)?;
        // By id alone a branched table's row is ambiguous.
        if !table.branch_by.is_empty() {
            return None;
        }
        let mut columns = String::new();
        for by in order {
            match crate::compile::column_sql(table, "t", &by.column) {
                Ok(sql) => {
                    let sql = table
                        .column(&by.column)
                        .and_then(|column| column.rank_sql(&sql))
                        .unwrap_or(sql);
                    columns.push_str(&format!(", {sql}"));
                }
                Err(error) => return Some(Err(error)),
            }
        }
        let sql = format!(
            "SELECT t.id{columns} FROM {} AS t WHERE t.id IN (SELECT unhex(value) FROM json_each(?1))",
            quote(&table.stored)
        );
        let ids = crate::compile::json_ids(rows.iter().map(|row| row.0.as_bytes().as_slice()));
        let found = self
            .conn
            .sql()
            .query_rows(&sql, jazz_sql::params![ids], |r| {
                let id: Vec<u8> = r.get(0)?;
                let values = (1..=order.len())
                    .map(|i| r.get::<Sql>(i))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((id, values))
            })
            .await;
        let mut found: HashMap<_, _> = match found {
            Ok(found) => found.into_iter().collect(),
            Err(error) => return Some(Err(storage(error))),
        };
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let values = found.remove(row.0.as_bytes().as_slice())?;
            // Stored cells order as the compiled ORDER BY orders them: a
            // chunked cell by itself (SPEC 19.5).
            let mut key: SortKey = values
                .into_iter()
                .zip(order)
                .map(|(value, by)| {
                    (
                        value,
                        matches!(by.direction, crate::query::OrderDirection::Desc),
                    )
                })
                .collect();
            key.push(id_key(row));
            keys.push(key);
        }
        Some(Ok(keys))
    }

    /// Re-run every result that waited for large-value chunks.
    pub(super) async fn retry_awaiting(&self) {
        let groups: Vec<_> = self.fan.borrow().groups.values().cloned().collect();
        for group in groups {
            let opened = {
                let group = group.borrow();
                if !group.awaiting {
                    continue;
                }
                group.opened
            };
            if opened {
                self.refresh_group(&group, None).await;
            } else {
                self.reset_group(&group).await;
            }
        }
    }

    /// A group's rows, with array relations nested in their cells.
    async fn group_rows(&self, cell: &RefCell<Group>) -> Result<Vec<CurrentRow>, Error> {
        let (prepared, identity, read, mut plans) = {
            let mut group = cell.borrow_mut();
            let read = self.visible_read(&group.read, &group.tables).into_owned();
            group.applied = cut_of(&read);
            (
                group.prepared.clone(),
                group.identity,
                read,
                std::mem::take(&mut group.plans),
            )
        };
        let result = async {
            let rows = self.run(&prepared, identity, &read).await?;
            self.nest_relations_kept(&prepared, identity, &read, rows, &mut plans)
                .await
        }
        .await;
        cell.borrow_mut().plans = plans;
        result
    }

    /// Re-run the live results whose cut moved: their read's settle rose,
    /// the connection's position did, or an own commit was accepted past
    /// them.
    pub(super) async fn refresh_visibility(&self) -> Result<(), Error> {
        let groups: Vec<Rc<RefCell<Group>>> = self.fan.borrow().groups.values().cloned().collect();
        for group in groups {
            {
                let group = group.borrow();
                if group.read.position == Position::Current
                    || cut_of(&self.visible_read(&group.read, &group.tables)) == group.applied
                {
                    continue;
                }
            }
            self.refresh_group(&group, None).await;
        }
        Ok(())
    }

    async fn reset_group(&self, cell: &RefCell<Group>) {
        let result = self.group_rows(cell).await;
        {
            let mut group = cell.borrow_mut();
            match result {
                Err(error) if crate::large::is_missing(&error) => group.awaiting = true,
                Ok(rows) => {
                    group.awaiting = false;
                    // Rows that left the result go out as removals before the
                    // re-opening: a reset tells local readers to replace their
                    // result, but a replica only drops rows it is told to (a
                    // tighter policy must evict what it can no longer read).
                    if group.opened {
                        let kept: BTreeSet<RowKey> = rows.iter().map(row_key).collect();
                        let removed: Vec<RemovedRow> = group
                            .rows
                            .iter()
                            .enumerate()
                            .filter(|(_, row)| !kept.contains(&row_key(row)))
                            .map(|(index, row)| RemovedRow {
                                table: row.table.to_string(),
                                row_uuid: row.id,
                                joined: row.joined.to_vec(),
                                version: row.version,
                                union_arm: row.union_arm.clone(),
                                index,
                            })
                            .collect();
                        if !removed.is_empty() {
                            group.push(delta(false, Vec::new(), Vec::new(), removed));
                        }
                    }
                    group.opened = true;
                    group.push(opening(&rows));
                    group.rows = rows;
                    Self::hold(&mut group, false);
                    group.keys.clear();
                }
                Err(error) => group.push(SubscriptionEvent::Rejected {
                    code: error.code,
                    message: error.message.clone(),
                    reason: error.to_string(),
                }),
            }
        }
        self.read_keys(cell).await;
    }

    /// Refuse splices of a column that is not a large-capable column of the
    /// query's table, or that it selects by JSON pointer.
    fn check_splices(&self, prepared: &PreparedQuery, columns: &[String]) -> Result<(), Error> {
        if columns.is_empty() {
            return Ok(());
        }
        let query = &prepared.query;
        let table = &query.table;
        let owner_state = self.owner.borrow();
        let schema = prepared.source_schema(&owner_state);
        let t = schema.table(table)?;
        for column in columns {
            if !t.column(column).is_some_and(crate::large::large_capable) {
                return Err(crate::values::query_error(format!(
                    "splices of {column:?} need a text, bytes or JSON column of {table:?}"
                )));
            }
            if query.partial.iter().any(|selected| {
                selected.column == *column
                    && matches!(selected.part, crate::query::Part::JsonPointer(_))
            }) {
                return Err(crate::values::unsupported(
                    "splices of a column selected by JSON pointer",
                ));
            }
        }
        Ok(())
    }

    /// Give each updated row of `event` its splice columns' changes from
    /// the version the result showed (`prior`). A change that cannot be
    /// computed here (a version no longer held, chunks still to arrive) is
    /// left out, and the reader takes the column whole.
    async fn add_splices(
        &self,
        group: &RefCell<Group>,
        prior: &HashMap<RowUuid, CurrentRow>,
        event: &mut SubscriptionEvent,
    ) {
        use crate::query::Part;
        let SubscriptionEvent::Delta { updated, .. } = event else {
            return;
        };
        let (splices, prepared, identity) = {
            let group = group.borrow();
            (
                group.key.splices.clone(),
                group.prepared.clone(),
                group.identity,
            )
        };
        if splices.is_empty() || updated.is_empty() {
            return;
        }
        let owner_state = self.owner.borrow().clone();
        let schema = prepared.source_schema(&owner_state);
        let Ok(table) = schema.table(&prepared.query.table) else {
            return;
        };
        let policies = schema.policy_schema();
        for out in updated {
            let Some(before) = prior.get(&out.row.id) else {
                continue;
            };
            // Two writers' pending versions can share a position.
            let rank = |row: &CurrentRow| {
                (
                    row.seq,
                    row.writer.as_deref().and_then(sync::writer_of_bytes),
                )
            };
            let (from, to) = (rank(before), rank(&out.row));
            if from == to {
                continue;
            }
            for column in &splices {
                let part = prepared
                    .query
                    .partial
                    .iter()
                    .find(|selected| selected.column == *column)
                    .map(|selected| &selected.part);
                if let Some(part) = part {
                    // A page is read in part: its splices turn the page the
                    // result showed into the new one, in page coordinates.
                    if !matches!(part, Part::JsonPointer(_))
                        && let (Some(old), Some(new)) =
                            (cell_of(before, column), cell_of(&out.row, column))
                        && let Some(splices) = page_splices(old, new)
                    {
                        out.splices.push((column.clone(), splices));
                    }
                    continue;
                }
                // Read access is by the group's own policies: a pinned
                // session's bundle, not the current schema.
                if let Ok(splices) = self
                    .value_delta(table, out.row.id, column, (from, to), identity, policies)
                    .await
                {
                    out.splices.push((column.clone(), splices));
                }
            }
        }
    }

    /// Bring a group's result up to date; true when it re-ran the query.
    async fn refresh_group(&self, group: &RefCell<Group>, ids: Option<&BTreeSet<RowUuid>>) -> bool {
        matches!(self.refresh(group, ids).await, Refresh::Reran)
    }

    /// [`Store::refresh_group`], telling a re-check that ended the
    /// subscription (a rejection) from one that applied.
    async fn refresh(&self, group: &RefCell<Group>, ids: Option<&BTreeSet<RowUuid>>) -> Refresh {
        let (prior, rechecks, relations) = {
            let mut group = group.borrow_mut();
            let prior: HashMap<RowUuid, CurrentRow> = if group.key.splices.is_empty() {
                HashMap::new()
            } else {
                group.rows.iter().map(|row| (row.id, row.clone())).collect()
            };
            let relations = !group.prepared.query.array_subqueries.is_empty();
            if relations && ids.is_some() && group.rechecks() {
                Self::share_held(&mut group);
            }
            (prior, group.rechecks(), relations)
        };
        if let Some(ids) = ids.filter(|_| rechecks) {
            match self.recheck_rows(group, ids).await {
                Some(Ok(Some(mut event))) => {
                    self.add_splices(group, &prior, &mut event).await;
                    {
                        let mut group = group.borrow_mut();
                        group.push(event);
                        if relations {
                            Self::hold(&mut group, true);
                        }
                    }
                    #[cfg(any(test, feature = "testing"))]
                    if relations {
                        oracle::compare(self, group, "recheck").await;
                    }
                    return Refresh::Rechecked;
                }
                Some(Ok(None)) => {
                    #[cfg(any(test, feature = "testing"))]
                    if relations {
                        oracle::compare(self, group, "recheck").await;
                    }
                    return Refresh::Rechecked;
                }
                Some(Err(error)) if crate::large::is_missing(&error) => {}
                Some(Err(error)) => {
                    group.borrow_mut().push(SubscriptionEvent::Rejected {
                        code: error.code,
                        message: error.message.clone(),
                        reason: error.to_string(),
                    });
                    return Refresh::Rejected;
                }
                None => {}
            }
        }
        let rows = match self.group_rows(group).await {
            Ok(rows) => rows,
            Err(error) if crate::large::is_missing(&error) => {
                group.borrow_mut().awaiting = true;
                return Refresh::Reran;
            }
            Err(error) => {
                group.borrow_mut().push(SubscriptionEvent::Rejected {
                    code: error.code,
                    message: error.message.clone(),
                    reason: error.to_string(),
                });
                return Refresh::Reran;
            }
        };
        let (was_awaiting, event) = {
            let mut group = group.borrow_mut();
            (
                std::mem::take(&mut group.awaiting),
                diff(&group.rows, &rows),
            )
        };
        if let Some(mut event) = event {
            self.add_splices(group, &prior, &mut event).await;
            group.borrow_mut().push(event);
        } else if was_awaiting {
            group
                .borrow_mut()
                .push(delta(false, Vec::new(), Vec::new(), Vec::new()));
        }
        {
            let mut group = group.borrow_mut();
            group.open_waiting(&rows);
            group.rows = rows;
            Self::hold(&mut group, false);
            group.keys.clear();
        }
        self.read_keys(group).await;
        Refresh::Reran
    }

    /// Refresh a group woken only through correlated relations (or, after
    /// a re-check, through a root row a relation level also holds) by
    /// nesting the relations again for the root rows whose subtree holds a
    /// matched parent or row. A `Related` table
    /// is read only through optional relations, so the roots, their cells
    /// and their order are unchanged. `Some(narrowly)` when done (whether
    /// only the relations on the hits' paths were read); `None`:
    /// undecided, re-run instead.
    async fn renest(&self, cell: &RefCell<Group>, hits: &[(usize, Hit)]) -> Option<bool> {
        let (affected, bare, scope, prepared, identity, read, mut plans) = {
            let mut group = cell.borrow_mut();
            if group.awaiting || group.prepared.query.array_subqueries.is_empty() {
                return None;
            }
            // Before any row changes: their shares are taken away by `hold`.
            Self::share_held(&mut group);
            let read = self.visible_read(&group.read, &group.tables).into_owned();
            if cut_of(&read) != group.applied {
                return None;
            }
            let mut affected = Vec::new();
            for (index, row) in group.rows.iter().enumerate() {
                let mut hit_any = false;
                for (level, hit) in hits {
                    let level = group.correlations.get(*level)?;
                    match root_hit(level, row, hit) {
                        None => return None,
                        Some(true) => {
                            hit_any = true;
                            break;
                        }
                        Some(false) => {}
                    }
                }
                if hit_any {
                    affected.push(index);
                }
            }
            if affected.is_empty() {
                return None;
            }
            let relations: HashSet<&str> = group
                .prepared
                .query
                .array_subqueries
                .iter()
                .map(|array| array.column_name.as_str())
                .collect();
            // The roots without their relation cells: those are appended after
            // the row's own cells, one per relation, and only those go (a
            // relation may share its name with one of the row's columns).
            let mut bare: Vec<CurrentRow> = Vec::with_capacity(affected.len());
            for index in &affected {
                let row = &group.rows[*index];
                let own = row.cells.len().checked_sub(relations.len())?;
                let trailing: HashSet<&str> = row.cells[own..]
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect();
                if trailing != relations || row.types.len() != row.cells.len() {
                    return None;
                }
                bare.push(CurrentRow {
                    cells: Arc::new(row.cells[..own].to_vec()),
                    types: row.types[..own].to_vec().into(),
                    ..row.clone()
                });
            }
            // Only the relations on a hit's path can have changed: nested again
            // down that path, every sibling relation of a level on it keeps its
            // old cells. Nothing narrower than all of them: the plain re-nest.
            let mut tree = BTreeMap::new();
            for (level, _) in hits {
                if let Some(level) = group.correlations.get(*level) {
                    scope_insert(&mut tree, &level.path);
                }
            }
            let scope =
                scope_normalize(&group.prepared.query.array_subqueries, tree).filter(|tree| {
                    tree.len() < relations.len()
                        || tree.values().any(|node| matches!(node, ScopeNode::Part(_)))
                });
            (
                affected,
                bare,
                scope,
                group.prepared.clone(),
                group.identity,
                read,
                std::mem::take(&mut group.plans),
            )
        };
        let nested = match &scope {
            None => {
                self.nest_relations_kept(&prepared, identity, &read, bare, &mut plans)
                    .await
            }
            Some(scope) => {
                let mut scoped = prepared.clone();
                scoped.query.array_subqueries =
                    scope_prune(&prepared.query.array_subqueries, scope);
                self.nest_relations_kept(&scoped, identity, &read, bare, &mut plans)
                    .await
            }
        };
        cell.borrow_mut().plans = plans;
        let (prior, updated) = {
            let mut group = cell.borrow_mut();
            let nested = nested.ok()?;
            // The same roots, in the same order: checked before anything is
            // merged or replaced, so a refusal leaves the result as it was.
            if nested.len() != affected.len()
                || nested
                    .iter()
                    .zip(&affected)
                    .any(|(row, index)| row.id != group.rows[*index].id)
            {
                return None;
            }
            // The old row with the scoped relations' cells replaced in place.
            let nested: Vec<CurrentRow> = match &scope {
                None => nested,
                Some(scope) => {
                    let mut merged = Vec::with_capacity(nested.len());
                    for (index, fresh) in affected.iter().zip(nested) {
                        let old = &group.rows[*index];
                        let mut cells = (*old.cells).clone();
                        let mut types = old.types.to_vec();
                        for (name, node) in scope {
                            let from = fresh.cells.iter().rposition(|(n, _)| n == name)?;
                            let to = cells.iter().rposition(|(n, _)| n == name)?;
                            match node {
                                ScopeNode::Whole => {
                                    cells[to] = fresh.cells[from].clone();
                                    types[to] = fresh.types[from].clone();
                                }
                                ScopeNode::Part(sub) => {
                                    let (Some(old_value), Some(fresh_value)) =
                                        (&cells[to].1, &fresh.cells[from].1)
                                    else {
                                        return None;
                                    };
                                    let (value, ty) = merge_relation(
                                        old_value,
                                        &types[to],
                                        fresh_value,
                                        &fresh.types[from],
                                        sub,
                                    )?;
                                    cells[to].1 = Some(value);
                                    types[to] = ty;
                                }
                            }
                        }
                        merged.push(CurrentRow {
                            cells: Arc::new(cells),
                            types: types.into(),
                            ..fresh
                        });
                    }
                    merged
                }
            };
            let prior: HashMap<RowUuid, CurrentRow> = if group.key.splices.is_empty() {
                HashMap::new()
            } else {
                affected
                    .iter()
                    .map(|index| (group.rows[*index].id, group.rows[*index].clone()))
                    .collect()
            };
            let mut updated = Vec::new();
            for (index, row) in affected.into_iter().zip(nested) {
                if row.cells != group.rows[index].cells {
                    updated.push(SubscriptionOutputRow {
                        row: row.clone(),
                        previous_index: Some(index),
                        index,
                        splices: Vec::new(),
                    });
                }
                group.rows[index] = row;
            }
            (prior, updated)
        };
        if !updated.is_empty() {
            let mut event = delta(false, Vec::new(), updated, Vec::new());
            self.add_splices(cell, &prior, &mut event).await;
            cell.borrow_mut().push(event);
        }
        Self::hold(&mut cell.borrow_mut(), true);
        Some(scope.is_some())
    }

    /// Reads the sort keys of a re-checkable group whose order is not
    /// shown, so they describe its rows as read (not after a later write).
    async fn read_keys(&self, group: &RefCell<Group>) {
        let (prepared, ids) = {
            let group = group.borrow();
            if !group.recheck || group.projected {
                return;
            }
            (
                group.prepared.clone(),
                group.rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )
        };
        if let Some(Ok(keys)) = self
            .sort_keys(&prepared, &prepared.query.order_by, &ids)
            .await
        {
            group.borrow_mut().keys = keys;
        }
    }
}

/// How [`Store::refresh`] brought a group up to date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refresh {
    /// Its changed root rows re-checked by id, in place.
    Rechecked,
    /// The re-check failed and ended the subscription.
    Rejected,
    /// The query ran again (or now waits for chunks).
    Reran,
}

/// `events`, pushed one after another while one commit was applied to a
/// result whose rows were `pre_ids` and are now `rows`, as one delta from
/// the first state to the last: a row both before and after that any of
/// them reported is updated, from its index before to its index after;
/// one only before is removed, one only after is added. A row's splices
/// are kept when a single event reported it (relative to the state before
/// that event, which for that row is the state before the commit); else
/// it is sent whole. Anything else (a reset, a rejection, a root row more
/// than once) is passed through as it was pushed.
fn compose(
    pre_ids: &[RowUuid],
    events: Vec<SubscriptionEvent>,
    rows: &[CurrentRow],
) -> Vec<SubscriptionEvent> {
    if events.len() < 2
        || events
            .iter()
            .any(|event| !matches!(event, SubscriptionEvent::Delta { reset: false, .. }))
    {
        return events;
    }
    let unique = |ids: &mut dyn Iterator<Item = RowUuid>, len: usize| {
        ids.collect::<HashSet<_>>().len() == len
    };
    if !unique(&mut pre_ids.iter().copied(), pre_ids.len())
        || !unique(&mut rows.iter().map(|row| row.id), rows.len())
    {
        return events;
    }
    type Splices = Vec<(String, Vec<LargeValueDeltaSplice>)>;
    let mut reported: HashMap<RowUuid, (usize, Splices)> = HashMap::new();
    let mut gone: HashMap<RowUuid, RemovedRow> = HashMap::new();
    let (mut publishable, mut settled, mut tier) = (true, true, None);
    for event in &events {
        let SubscriptionEvent::Delta {
            publishable: p,
            added,
            updated,
            removed,
            settled: s,
            tier: t,
            ..
        } = event
        else {
            return events;
        };
        publishable &= *p;
        settled = *s;
        tier = Some(*t);
        for row in added.iter().chain(updated) {
            let entry = reported.entry(row.id).or_default();
            entry.0 += 1;
            entry.1 = row.splices.clone();
        }
        for row in removed {
            reported.entry(row.row_uuid).or_default().0 += 1;
            gone.insert(row.row_uuid, row.clone());
        }
    }
    let Some(tier) = tier else {
        return events;
    };
    let before: HashMap<RowUuid, usize> = pre_ids
        .iter()
        .enumerate()
        .filter(|(_, id)| reported.contains_key(id))
        .map(|(index, id)| (*id, index))
        .collect();
    let after: HashMap<RowUuid, usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| reported.contains_key(&row.id))
        .map(|(index, row)| (row.id, index))
        .collect();
    let (mut added, mut updated, mut removed) = (Vec::new(), Vec::new(), Vec::new());
    for (id, (times, splices)) in reported {
        match (before.get(&id), after.get(&id)) {
            (Some(previous), Some(index)) => updated.push(SubscriptionOutputRow {
                row: rows[*index].clone(),
                previous_index: Some(*previous),
                index: *index,
                splices: if times == 1 { splices } else { Vec::new() },
            }),
            (None, Some(index)) => added.push(SubscriptionOutputRow {
                row: rows[*index].clone(),
                previous_index: None,
                index: *index,
                splices: Vec::new(),
            }),
            (Some(previous), None) => {
                let Some(mut row) = gone.remove(&id) else {
                    return events;
                };
                row.index = *previous;
                removed.push(row);
            }
            (None, None) => {}
        }
    }
    added.sort_by_key(|row| row.index);
    updated.sort_by_key(|row| row.index);
    removed.sort_by_key(|row| row.index);
    vec![SubscriptionEvent::Delta {
        reset: false,
        publishable,
        added,
        updated,
        removed,
        settled,
        tier,
    }]
}

/// Report as updated the result rows that reference a changed include
/// target, so their output (built with the target) is read again.
fn refresh_includes(group: &mut Group, changed: &BTreeSet<(String, RowUuid)>) {
    let refers = |row: &CurrentRow| {
        row.cells.iter().any(|(name, value)| {
            // A nullable reference reads back wrapped.
            let value = match value {
                Some(Value::Nullable(Some(inner))) => Some(inner.as_ref()),
                value => value.as_ref(),
            };
            matches!(value, Some(Value::Uuid(id)) if changed.contains(&(name.clone(), RowUuid(*id))))
        })
    };
    let updated: Vec<SubscriptionOutputRow> = group
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| refers(row))
        .map(|(index, row)| SubscriptionOutputRow {
            row: row.clone(),
            previous_index: Some(index),
            index,
            splices: Vec::new(),
        })
        .collect();
    if !updated.is_empty() {
        group.push(delta(false, Vec::new(), updated, Vec::new()));
    }
}

impl Group {
    /// Whether a root-only wake may be applied by re-checking its ids.
    /// With relations, a root row's write
    /// may change another root row's relation cells (a relation over the
    /// root table itself, or a policy of a relation reading it). That is
    /// only seen when every root-table write is filtered by correlation or
    /// evidence; a whole-table root watch reports it as a root-only wake,
    /// which a recheck by id would miss. Read from the CURRENT watches:
    /// `refresh_where` rebuilds them (and may widen the root to the whole
    /// table) for a live group.
    fn rechecks(&self) -> bool {
        self.recheck
            && (self.prepared.query.array_subqueries.is_empty()
                || !self.watches.contains(&Watch::Table(self.root.clone())))
    }

    /// Publish `event` to every member. A delta reaches only opened
    /// members; an opening opens the others too.
    fn push(&mut self, event: SubscriptionEvent) {
        if let Some(deferred) = &mut self.deferred {
            deferred.push(event);
            return;
        }
        #[cfg(any(test, feature = "testing"))]
        if oracle::on() && self.oracle_before.is_some() {
            self.oracle_pushed.push(event.clone());
        }
        self.members.retain(|m| m.queue.strong_count() > 0);
        for member in &self.members {
            Self::deliver(self.query(), member, event.clone());
        }
    }

    /// Open the members still waiting for an opening on `rows`.
    fn open_waiting(&mut self, rows: &[CurrentRow]) {
        if self
            .members
            .iter()
            .all(|member| member.joined.get() == Joined::Open)
        {
            return;
        }
        let event = opening(rows);
        for member in &self.members {
            if member.joined.get() == Joined::AwaitingOpening {
                Self::deliver(self.query(), member, event.clone());
            }
        }
    }

    fn deliver(query: &Query, member: &Member, event: SubscriptionEvent) {
        match &event {
            SubscriptionEvent::Delta { reset: true, .. } => member.joined.set(Joined::Open),
            // A delta is relative to an opening this member has not had.
            SubscriptionEvent::Delta { .. } if member.joined.get() != Joined::Open => return,
            _ => {}
        }
        if let Some(queue) = member.queue.upgrade() {
            let mut event = event;
            if let SubscriptionEvent::Delta {
                reset,
                added,
                settled,
                tier,
                ..
            } = &mut event
            {
                *settled = member.settled.get();
                *tier = member.settled.tier();
                let told = settled.then_some(*tier);
                if *reset || told != member.told.get() {
                    tracing::debug!(
                        target: "jazz::sync",
                        table = %query.table,
                        opening = *reset,
                        rows = added.len(),
                        settled = *settled,
                        tier = ?*tier,
                        "subscription delivered",
                    );
                }
                member.told.set(told);
            }
            queue.borrow_mut().push_back(event);
        }
    }
}

/// What a scoped re-nest reads again under one relation: all of it, or
/// only the named nested relations (the others keep their old cells).
#[derive(Clone, Debug)]
enum ScopeNode {
    Whole,
    Part(BTreeMap<String, ScopeNode>),
}

/// Add the relation path `path` (root first) to `tree`: its last relation
/// is read again whole; the ones above it only in part.
fn scope_insert(tree: &mut BTreeMap<String, ScopeNode>, path: &[String]) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };
    if rest.is_empty() {
        tree.insert(first.clone(), ScopeNode::Whole);
        return;
    }
    match tree
        .entry(first.clone())
        .or_insert_with(|| ScopeNode::Part(BTreeMap::new()))
    {
        ScopeNode::Whole => {}
        ScopeNode::Part(sub) => scope_insert(sub, rest),
    }
}

/// `tree` checked against the query's relations: `None` when it names
/// one the query has not; a level whose dropped siblings could filter its
/// rows (a required relation) is read whole.
fn scope_normalize(
    arrays: &[crate::query::ArraySubquery],
    tree: BTreeMap<String, ScopeNode>,
) -> Option<BTreeMap<String, ScopeNode>> {
    let mut out = BTreeMap::new();
    for (name, node) in tree {
        let array = arrays.iter().find(|array| array.column_name == name)?;
        let node = match node {
            ScopeNode::Whole => ScopeNode::Whole,
            ScopeNode::Part(sub) => {
                let filters = array.nested_arrays.iter().any(|nested| {
                    !sub.contains_key(&nested.column_name)
                        && nested.requirement != crate::query::ArraySubqueryRequirement::Optional
                });
                if filters {
                    ScopeNode::Whole
                } else {
                    ScopeNode::Part(scope_normalize(&array.nested_arrays, sub)?)
                }
            }
        };
        out.insert(name, node);
    }
    Some(out)
}

/// The relations of `arrays` that `tree` reads again, nested ones pruned
/// the same way.
fn scope_prune(
    arrays: &[crate::query::ArraySubquery],
    tree: &BTreeMap<String, ScopeNode>,
) -> Vec<crate::query::ArraySubquery> {
    arrays
        .iter()
        .filter_map(|array| {
            let node = tree.get(&array.column_name)?;
            let mut array = array.clone();
            if let ScopeNode::Part(sub) = node {
                array.nested_arrays = scope_prune(&array.nested_arrays, sub);
            }
            Some(array)
        })
        .collect()
}

/// A relation cell (an array of records) with the relations `tree` names
/// taken from `fresh` (read with only those) and every other field kept
/// from `old`, re-encoded under the descriptor a full run would build;
/// `None` when the two do not line up row for row.
fn merge_relation(
    old: &Value,
    old_ty: &ValueType,
    fresh: &Value,
    fresh_ty: &ValueType,
    tree: &BTreeMap<String, ScopeNode>,
) -> Option<(Value, ValueType)> {
    use crate::records::{DescriptorField, OwnedRecord, RecordDescriptor};
    let (Value::Array(olds), Value::Array(freshs)) = (old, fresh) else {
        return None;
    };
    if olds.len() != freshs.len() {
        return None;
    }
    if olds.is_empty() {
        return Some((old.clone(), old_ty.clone()));
    }
    let (ValueType::Array(old_element), ValueType::Array(fresh_element)) = (old_ty, fresh_ty)
    else {
        return None;
    };
    let (ValueType::Record(old_desc), ValueType::Record(fresh_desc)) =
        (old_element.as_ref(), fresh_element.as_ref())
    else {
        return None;
    };
    let old_fields = old_desc.fields();
    let fresh_fields = fresh_desc.fields();
    // Relations are appended after the row's own cells: the last match.
    let position = |fields: &[DescriptorField], name: &str| {
        fields
            .iter()
            .rposition(|field| field.name.as_deref() == Some(name))
    };
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(olds.len());
    let mut replaced: BTreeMap<usize, Vec<(ValueType, bool)>> = BTreeMap::new();
    for (old, fresh) in olds.iter().zip(freshs) {
        let (Value::Record(old), Value::Record(fresh)) = (old, fresh) else {
            return None;
        };
        let mut values = old.to_values().ok()?;
        let fresh_values = fresh.to_values().ok()?;
        if values.first() != fresh_values.first() {
            return None;
        }
        for (name, node) in tree {
            let at = position(old_fields, name)?;
            let from = position(fresh_fields, name)?;
            let (value, ty) = match node {
                ScopeNode::Whole => (
                    fresh_values.get(from)?.clone(),
                    fresh_fields[from].value_type.clone(),
                ),
                ScopeNode::Part(sub) => merge_relation(
                    values.get(at)?,
                    &old_fields[at].value_type,
                    fresh_values.get(from)?,
                    &fresh_fields[from].value_type,
                    sub,
                )?,
            };
            let filled = matches!(&value, Value::Array(items) if !items.is_empty());
            replaced.entry(at).or_default().push((ty, filled));
            values[at] = value;
        }
        rows.push(values);
    }
    let mut fields: Vec<DescriptorField> = old_fields.to_vec();
    for (at, types) in replaced {
        // A nested relation's type is its non-empty siblings' widest.
        let mut shared: Option<ValueType> = None;
        for (ty, filled) in &types {
            if *filled {
                shared = Some(match shared {
                    None => ty.clone(),
                    Some(other) => crate::values::widen(&other, ty).ok()?,
                });
            }
        }
        fields[at].value_type = shared.unwrap_or_else(|| types[0].0.clone());
    }
    let descriptor = RecordDescriptor::new_with_fields(fields.clone());
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        let record = row
            .iter()
            .zip(&fields)
            .map(|(value, field)| crate::values::coerce(value, &field.value_type))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        let raw = descriptor.create(&record).ok()?;
        values.push(Value::Record(OwnedRecord::new(raw, descriptor)));
    }
    Some((
        Value::Array(values),
        ValueType::Array(Box::new(ValueType::Record(Box::new(descriptor)))),
    ))
}

/// What matched at one correlation level: the held row, and the held
/// parents' `outer` atoms the row's old or new `inner` names.
#[derive(Clone, Debug, Default, PartialEq)]
struct Hit {
    row: Option<RowUuid>,
    atoms: HashSet<String>,
}

/// [`correlated`] with what matched: `None` when only a full run decides
/// (awaiting, a held root row, an incomplete level, no images), else the
/// matched levels (empty: no match).
fn correlated_hits(
    group: &Group,
    table: &str,
    row: RowUuid,
    images: Option<&[Vec<(String, Sql)>]>,
    root_shortcut: bool,
) -> Option<Vec<(usize, Hit)>> {
    if group.awaiting
        || (root_shortcut
            && group.root == table
            && group
                .root_held
                .as_ref()
                .is_some_and(|held| held.contains(&row)))
    {
        return None;
    }
    let mut hits = Vec::new();
    for (index, (level, held)) in group.correlations.iter().zip(&group.held).enumerate() {
        if level.table != table {
            continue;
        }
        if !held.complete() {
            return None;
        }
        let mut hit = Hit {
            row: held.rows.contains_key(&row).then_some(row),
            atoms: HashSet::new(),
        };
        if level.inner == "id" {
            let atom = canon(&Sql::Blob(row.0.as_bytes().to_vec()));
            if held.parents.contains_key(&atom) {
                hit.atoms.insert(atom);
            }
        } else {
            let images = images?;
            for image in images {
                for (_, cell) in image.iter().filter(|(column, _)| *column == level.inner) {
                    for atom in stored_atoms(cell, level.inner_list)? {
                        if held.parents.contains_key(&atom) {
                            hit.atoms.insert(atom);
                        }
                    }
                }
            }
        }
        if hit.row.is_some() || !hit.atoms.is_empty() {
            hits.push((index, hit));
        }
    }
    Some(hits)
}

/// Whether `root`'s subtree holds, at `level`, a parent whose `outer`
/// value is one of `hit`'s atoms or the hit row; `None` when undecided.
fn root_hit(level: &Correlation, root: &CurrentRow, hit: &Hit) -> Option<bool> {
    let (last, through) = level.path.split_last()?;
    let mut parents: Vec<Node> = vec![Node::Row(root)];
    for column in through {
        let mut next = Vec::new();
        for parent in &parents {
            next.extend(parent.children(column)?);
        }
        parents = next;
    }
    for parent in &parents {
        if !hit.atoms.is_empty() {
            let mut atoms = HashSet::new();
            if !value_atoms(&parent.value(&level.outer)?, &mut atoms) {
                return None;
            }
            if atoms.iter().any(|atom| hit.atoms.contains(atom)) {
                return Some(true);
            }
        }
        if let Some(row) = hit.row {
            for child in parent.children(last)? {
                if child.id()? == row {
                    return Some(true);
                }
            }
        }
    }
    Some(false)
}

/// A test-build check of every refresh that does not re-run against one
/// that does, off unless a test turns it on for its thread: after a
/// re-check of a group with relations, or a re-nest, the group's rows
/// (ids, cells, deletion and declared types) must be the rows a full run
/// reads now, and after every [`Store::hold`] the kept counts must hold
/// the keys a full count holds. A mismatch panics. The full run only
/// reads; when it fails (missing chunks) nothing is compared.
#[cfg(any(test, feature = "testing"))]
mod oracle {
    use super::{CurrentRow, Group, RowUuid, Store, SubscriptionEvent, Value};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    thread_local! {
        static ON: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    pub(super) fn on() -> bool {
        ON.with(std::cell::Cell::get)
    }

    pub(super) fn set(on: bool) {
        ON.with(|cell| cell.set(on));
    }

    /// Note the rows before a refresh, to replay what it sends.
    pub(super) fn begin(group: &mut Group) {
        group.oracle_pushed.clear();
        group.oracle_before = on().then(|| group.rows.clone());
    }

    /// After a refresh that did not re-run (`checked`): at most one delta
    /// was sent, and applying it to the rows before, as a member does,
    /// gives the rows now (ids, order and cells).
    pub(super) fn finish(group: &mut Group, checked: bool) {
        let pushed = std::mem::take(&mut group.oracle_pushed);
        let Some(before) = group.oracle_before.take() else {
            return;
        };
        if !checked
            || pushed
                .iter()
                .any(|event| !matches!(event, SubscriptionEvent::Delta { .. }))
        {
            return;
        }
        assert!(
            pushed.len() <= 1,
            "[fanout oracle] one refresh of {} sent {} deltas: {pushed:?}",
            group.root,
            pushed.len(),
        );
        let mut seen: Vec<(RowUuid, Arc<Vec<(String, Option<Value>)>>)> = before
            .iter()
            .map(|row| (row.id, row.cells.clone()))
            .collect();
        for event in &pushed {
            let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            else {
                continue;
            };
            if *reset {
                seen.clear();
            }
            let moved: std::collections::HashSet<RowUuid> = removed
                .iter()
                .map(|gone| gone.row_uuid)
                .chain(updated.iter().map(|row| row.id))
                .collect();
            seen.retain(|(id, _)| !moved.contains(id));
            let mut placed: Vec<_> = added.iter().chain(updated).collect();
            placed.sort_by_key(|row| row.index);
            for row in placed {
                assert!(
                    row.index <= seen.len(),
                    "[fanout oracle] delta of {} places a row past the end: {event:?}",
                    group.root,
                );
                seen.insert(row.index, (row.id, row.cells.clone()));
            }
        }
        let now: Vec<_> = group
            .rows
            .iter()
            .map(|row| (row.id, row.cells.clone()))
            .collect();
        assert!(
            seen == now,
            "[fanout oracle] the deltas sent for {} do not lead to its rows\nsent {pushed:?}\nreplayed {seen:?}\nrows {now:?}",
            group.root,
        );
    }

    pub(super) async fn compare(fanout: &Store, cell: &std::cell::RefCell<Group>, label: &str) {
        let (prepared, identity, read) = {
            let group = cell.borrow();
            if !on() || (group.renest_follows && label == "recheck") {
                return;
            }
            (
                group.prepared.clone(),
                group.identity,
                fanout.visible_read(&group.read, &group.tables).into_owned(),
            )
        };
        let Ok(rows) = fanout.run(&prepared, identity, &read).await else {
            return;
        };
        let Ok(full) = fanout
            .nest_relations(&prepared, identity, &read, rows)
            .await
        else {
            return;
        };
        let group = cell.borrow();
        let same = |a: &CurrentRow, b: &CurrentRow| {
            a.id == b.id && a.cells == b.cells && a.deleted == b.deleted && a.types == b.types
        };
        if full.len() == group.rows.len() && full.iter().zip(&group.rows).all(|(a, b)| same(a, b)) {
            return;
        }
        let ids = |rows: &[CurrentRow]| rows.iter().map(|row| row.id).collect::<BTreeSet<_>>();
        let (kept_ids, full_ids) = (ids(&group.rows), ids(&full));
        let changed: Vec<_> = group
            .rows
            .iter()
            .filter(|row| {
                full.iter()
                    .any(|other| other.id == row.id && !same(row, other))
            })
            .map(|row| row.id)
            .collect();
        panic!(
            "[fanout oracle] {label} of {} left other rows than a full run: missing {:?} extra {:?} changed {:?}\nkept {:?}\nfull {:?}",
            group.root,
            full_ids.difference(&kept_ids).collect::<Vec<_>>(),
            kept_ids.difference(&full_ids).collect::<Vec<_>>(),
            changed,
            group.rows,
            full,
        );
    }
}

/// Turn the refresh oracle (see `oracle`) on or off on this thread.
#[cfg(any(test, feature = "testing"))]
pub fn set_fanout_oracle_for_test(on: bool) {
    oracle::set(on);
}

/// Whether a group may apply root-only commits by re-checking ids: a
/// single-table query (not a union), unbounded or a first page, (no joins,
/// required relations, required includes, aggregates, windows), outside
/// branch views and cuts (a settled-only read is one), whose compiled SQL
/// reads its root table exactly once (a read policy that looks at other
/// rows of the same table would let one row's change move another).
/// Optional relations do not filter roots (they are not in the root SQL,
/// see `Ctx::array_requirements`): a re-check nests the changed roots'
/// relations again, and what a write elsewhere does to the others' is
/// the correlation watches' to find (see [`Group::rechecks`]).
fn recheckable(query: &Query, root: &str, read: &ReadSpec, compiled: &Compiled) -> bool {
    !is_history_table(&query.table)
        && query.relation.is_none()
        && query.offset == 0
        && query.aggregate.is_none()
        && query.joins.is_empty()
        && query.flat_join.is_none()
        && query.reachable.is_empty()
        && query.inherits.is_empty()
        && query
            .array_subqueries
            .iter()
            .all(|array| array.requirement == crate::query::ArraySubqueryRequirement::Optional)
        && query.includes.iter().all(|include| !include.require)
        && read.view.is_none()
        && read.cut.is_none()
        && !read.settled_only
        && !compiled.aggregate
        && compiled.sql.matches(&quote(root)).count() == 1
}

/// The branch keys a branch-view group reads, per branched table it
/// watches: its head's, and its base's unless the base is frozen (a cut
/// state no later change reaches). Tables a selector cannot key are left
/// out, so every change to them wakes the group.
fn view_branches(
    schema: &Schema,
    read: &ReadSpec,
    watches: &[Watch],
) -> HashMap<String, Vec<Vec<u8>>> {
    let mut out = HashMap::new();
    let Some(view) = &read.view else {
        return out;
    };
    for watch in watches {
        let table = watch.table();
        let Some(t) = schema.by_stored(table).filter(|t| !t.branch_by.is_empty()) else {
            continue;
        };
        let mut keys = vec![];
        let live_base = view.base.as_ref().filter(|_| view.base_cut.is_none());
        for selector in std::iter::once(&view.head).chain(live_base) {
            match t.selector_key(selector) {
                Ok(key) => keys.push(key),
                Err(_) => {
                    keys.clear();
                    break;
                }
            }
        }
        if !keys.is_empty() {
            out.insert(table.clone(), keys);
        }
    }
    out
}

/// What a branch view over a base masks on a replica that reads it over
/// the base rows it holds: a base row of a branched table it reads shows
/// there unless the replica holds the head's row of it too (a tombstone, or
/// a version the query leaves out), which the view's own result need not
/// ship (ch. 8 §8.4). Only base rows the reader may see through the view
/// need one: of the root table, those the query matches; of a table it
/// reads besides (relations, joins, policies), those the reader may read.
#[derive(Clone)]
pub(crate) struct ViewMask {
    pub table: String,
    pub head: Vec<u8>,
    pub base: Vec<u8>,
    /// The root table's: the query without its window, read on the base
    /// alone (as of a frozen base's cut).
    pub base_query: Option<PreparedQuery>,
    pub base_read: ReadSpec,
}

/// The [`ViewMask`]s of a read of `prepared` in `read` over the stored
/// `tables` it reads, if it is a branch view over a base.
pub(crate) fn view_masks<'t>(
    schema: &Schema,
    prepared: &PreparedQuery,
    read: &ReadSpec,
    tables: impl IntoIterator<Item = &'t String>,
) -> Vec<ViewMask> {
    let Some(view) = read.view.as_ref() else {
        return Vec::new();
    };
    let Some(base) = view.base.as_ref() else {
        return Vec::new();
    };
    let query = &prepared.query;
    let root: Option<String> = history_source_table(&query.table)
        .is_none()
        .then(|| schema.stored_table(&query.table).ok().map(str::to_owned))
        .flatten();
    let base_read = ReadSpec {
        position: Position::Current,
        include_deleted: read.include_deleted,
        settled_only: read.settled_only,
        view: Some(View {
            head: base.clone(),
            base: None,
            base_cut: None,
        }),
        cut: view.base_cut.clone(),
        splices: Vec::new(),
    };
    let mut out: Vec<ViewMask> = Vec::new();
    let tables: Vec<String> = tables.into_iter().cloned().chain(root.clone()).collect();
    for table in tables.iter().map(String::as_str) {
        if out.iter().any(|mask| mask.table == table) {
            continue;
        }
        let Some(branched) = schema.by_stored(table).filter(|t| !t.branch_by.is_empty()) else {
            continue;
        };
        let (Ok(head), Ok(base)) = (
            branched.selector_key(&view.head),
            branched.selector_key(base),
        ) else {
            continue;
        };
        if head == base {
            continue;
        }
        out.push(ViewMask {
            table: table.to_owned(),
            head,
            base,
            base_query: (root.as_deref() == Some(table)).then(|| prepared.unwindowed()),
            base_read: base_read.clone(),
        });
    }
    out
}

/// Whether every order column of a query is among its shown columns.
fn order_projected(query: &Query, compiled: &Compiled) -> bool {
    query.order_by.iter().all(|key| {
        key.column == "id"
            || compiled
                .projection
                .iter()
                .any(|(name, ..)| *name == key.column)
    })
}

/// SQLite's ORDER BY comparison of two sort keys (NULL < numbers < text <
/// blobs; text and blobs bytewise), each part ascending or descending.
fn sql_order(a: &[(Sql, bool)], b: &[(Sql, bool)]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    fn rank(value: &Sql) -> u8 {
        match value {
            Sql::Null => 0,
            Sql::Integer(_) | Sql::Real(_) => 1,
            Sql::Text(_) => 2,
            Sql::Blob(_) => 3,
        }
    }
    for ((x, desc), (y, _)) in a.iter().zip(b) {
        let ordering = match (x, y) {
            (Sql::Integer(x), Sql::Integer(y)) => x.cmp(y),
            (Sql::Integer(x), Sql::Real(y)) => {
                (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal)
            }
            (Sql::Real(x), Sql::Integer(y)) => {
                x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
            }
            (Sql::Real(x), Sql::Real(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
            (Sql::Text(x), Sql::Text(y)) => x.cmp(y),
            (Sql::Blob(x), Sql::Blob(y)) => x.cmp(y),
            (x, y) => rank(x).cmp(&rank(y)),
        };
        let ordering = if *desc { ordering.reverse() } else { ordering };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(super) fn opening(rows: &[CurrentRow]) -> SubscriptionEvent {
    let added = rows
        .iter()
        .enumerate()
        .map(|(index, row)| SubscriptionOutputRow {
            row: row.clone(),
            previous_index: None,
            index,
            splices: Vec::new(),
        })
        .collect();
    delta(true, added, Vec::new(), Vec::new())
}

/// What identifies a result row across runs: its id, plus its seq for a
/// history read, whose versions share their row's id, a flat join's
/// joined rows, and a union read's arm (the occurrence).
type RowKey = (
    RowUuid,
    crate::db::Pos,
    Arc<[crate::values::JoinedRow]>,
    Option<Arc<str>>,
);

fn row_key(row: &CurrentRow) -> RowKey {
    let version = if is_history_table(&row.table) {
        row.seq
    } else {
        crate::db::Pos::default()
    };
    (row.id, version, row.joined.clone(), row.union_arm.clone())
}

/// The keyed diff of two results, or `None` when nothing changed.
/// A row with the same cells at a newer version (a same-value write: its
/// metadata moved). A pending local version settling as the server's is
/// the same write, not a new one.
/// The value of `row`'s cell `column`, unwrapped from any nullable wrapper.
fn cell_of<'a>(row: &'a CurrentRow, column: &str) -> Option<&'a Value> {
    let mut value = row
        .cells
        .iter()
        .find(|(name, _)| name == column)?
        .1
        .as_ref()?;
    while let Value::Nullable(Some(inner)) = value {
        value = inner;
    }
    Some(value)
}

/// The one splice that turns page `old` into page `new`, trimmed to where
/// they differ (on code point boundaries for text). `None` for a value that
/// is not a page of bytes or text.
fn page_splices(old: &Value, new: &Value) -> Option<Vec<crate::db::LargeValueDeltaSplice>> {
    match (old, new) {
        (Value::Bytes(a), Value::Bytes(b)) => Some(crate::db::byte_splices(a, b, false)),
        (Value::String(a), Value::String(b)) => {
            Some(crate::db::byte_splices(a.as_bytes(), b.as_bytes(), true))
        }
        _ => None,
    }
}

fn new_version(prior: &CurrentRow, row: &CurrentRow) -> bool {
    prior.seq != row.seq && (prior.seq.accepted() || !row.seq.accepted())
}

fn diff(before: &[CurrentRow], after: &[CurrentRow]) -> Option<SubscriptionEvent> {
    let old: HashMap<RowKey, (usize, &CurrentRow)> = before
        .iter()
        .enumerate()
        .map(|(i, row)| (row_key(row), (i, row)))
        .collect();
    let new: BTreeSet<RowKey> = after.iter().map(row_key).collect();
    let moved = moved_rows(before, after, &new);
    let mut added = Vec::new();
    let mut updated = Vec::new();
    for (index, row) in after.iter().enumerate() {
        match old.get(&row_key(row)) {
            None => added.push(SubscriptionOutputRow {
                row: row.clone(),
                previous_index: None,
                index,
                splices: Vec::new(),
            }),
            Some((previous, prior))
                if prior.cells != row.cells
                    || prior.deleted != row.deleted
                    || new_version(prior, row)
                    || moved.contains(&row_key(row)) =>
            {
                updated.push(SubscriptionOutputRow {
                    row: row.clone(),
                    previous_index: Some(*previous),
                    index,
                    splices: Vec::new(),
                })
            }
            Some(_) => {}
        }
    }
    let removed: Vec<RemovedRow> = before
        .iter()
        .enumerate()
        .filter(|(_, row)| !new.contains(&row_key(row)))
        .map(|(index, row)| RemovedRow {
            table: row.table.to_string(),
            row_uuid: row.id,
            joined: row.joined.to_vec(),
            version: row.version,
            union_arm: row.union_arm.clone(),
            index,
        })
        .collect();
    if added.is_empty() && updated.is_empty() && removed.is_empty() {
        return None;
    }
    Some(delta(false, added, updated, removed))
}

/// Rows kept by both results that changed place relative to each other
/// (an order key that is not shown moved them): every kept row outside a
/// longest run that kept its relative order.
fn moved_rows(
    before: &[CurrentRow],
    after: &[CurrentRow],
    kept_after: &BTreeSet<RowKey>,
) -> BTreeSet<RowKey> {
    let rank: HashMap<RowKey, usize> = before
        .iter()
        .filter(|row| kept_after.contains(&row_key(row)))
        .enumerate()
        .map(|(rank, row)| (row_key(row), rank))
        .collect();
    let kept: Vec<(RowKey, usize)> = after
        .iter()
        .filter_map(|row| Some((row_key(row), *rank.get(&row_key(row))?)))
        .collect();
    // Patience sorting: `tails[k]` ends the best increasing run of length
    // k + 1; `back` links each element to its predecessor in its run.
    let mut tails: Vec<usize> = Vec::new();
    let mut back: Vec<Option<usize>> = vec![None; kept.len()];
    for (i, (_, rank)) in kept.iter().enumerate() {
        let at = tails.partition_point(|&t| kept[t].1 < *rank);
        back[i] = at.checked_sub(1).map(|k| tails[k]);
        if at == tails.len() {
            tails.push(i);
        } else {
            tails[at] = i;
        }
    }
    let mut in_order = vec![false; kept.len()];
    let mut cursor = tails.last().copied();
    while let Some(i) = cursor {
        in_order[i] = true;
        cursor = back[i];
    }
    kept.iter()
        .zip(in_order)
        .filter(|(_, in_order)| !in_order)
        .map(|((id, _), _)| id.clone())
        .collect()
}

/// A top-level `column = literal/param` filter on the root table.
fn root_key(query: &Query, binding: &BTreeMap<String, Value>) -> Option<(String, String)> {
    use crate::query::{Operand, Predicate};
    for filter in &query.filters {
        let Predicate::Eq(left, right) = filter else {
            continue;
        };
        let (column, other) = match (left, right) {
            (Operand::Column(c), other) | (other, Operand::Column(c)) => (c, other),
            _ => continue,
        };
        let value = match other {
            Operand::Literal(value) => value.clone(),
            Operand::Param(name) => match binding.get(name) {
                Some(value) => value.clone(),
                None => continue,
            },
            _ => continue,
        };
        if let Some(value) = canon_value(&value) {
            return Some((column.clone(), value));
        }
    }
    None
}

/// Collect `EXISTS table WHERE column = session.path AND ..` pins of a read
/// policy. Returns false when the policy has a form this analysis does not
/// understand (then no table is pinned). A table seen twice with different
/// pins, or nested inside another EXISTS, is not pinned.
pub(super) fn policy_pins(
    schema: &Schema,
    policy: &PolicyExpr,
    session: &Session,
    pins: &mut HashMap<String, Option<(String, String)>>,
) -> bool {
    use crate::tools::PolicyValue;
    match policy {
        PolicyExpr::And(list) | PolicyExpr::Or(list) => {
            list.iter().all(|p| policy_pins(schema, p, session, pins))
        }
        // A pin bounds which rows can change an EXISTS's value, whatever
        // surrounds it, so negation keeps it sound.
        PolicyExpr::Not(inner) => policy_pins(schema, inner, session, pins),
        PolicyExpr::Exists { table, condition } => {
            let conjuncts: Vec<&PolicyExpr> = match condition.as_ref() {
                PolicyExpr::And(list) => list.iter().collect(),
                other => vec![other],
            };
            let mut pin = None;
            for conjunct in &conjuncts {
                match conjunct {
                    PolicyExpr::Cmp {
                        column,
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(path),
                    } if path.first().map(String::as_str) != Some("__jazz_outer_row") => {
                        if pin.is_none() {
                            // The value the compiled comparison holds the
                            // column to (a claim as the column's type).
                            let ty = schema
                                .table(table)
                                .ok()
                                .and_then(|table| table.column_type(column).cloned());
                            pin = session_value_for(path, ty.as_ref(), session)
                                .as_ref()
                                .and_then(canon_value)
                                .map(|v| (column.clone(), v));
                        }
                    }
                    PolicyExpr::Exists { .. }
                    | PolicyExpr::ExistsRel { .. }
                    | PolicyExpr::Inherits { .. }
                    | PolicyExpr::InheritsReferencing { .. }
                    | PolicyExpr::Not(_)
                    | PolicyExpr::Or(_) => {
                        return false;
                    }
                    _ => {}
                }
            }
            match pins.get(table) {
                Some(existing) if *existing != pin => {
                    pins.insert(table.clone(), None);
                }
                _ => {
                    pins.insert(table.clone(), pin);
                }
            }
            true
        }
        PolicyExpr::ExistsRel { .. }
        | PolicyExpr::Inherits { .. }
        | PolicyExpr::InheritsReferencing { .. } => false,
        // Everything else reads only the protected row (and the session).
        _ => true,
    }
}

/// One way a read policy reads rows of a table as evidence about another
/// row: an `EXISTS` over the table, an `INHERITS` through a reference to
/// it, or an `INHERITS_REFERENCING` of rows of it. Whatever row it is
/// checked for, its value can change only with a row of the table that is
/// inserted, deleted, or changes one of `columns`, and (with a `pin`)
/// whose old or new image meets the pin: no other row can meet its
/// condition, before or after the write.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Occurrence {
    /// `column = session value` among its conjuncts: the stored column and
    /// the canonical value.
    pin: Option<(String, String)>,
    /// The stored application columns of the table's rows it reads.
    columns: BTreeSet<String>,
    /// Set for the source of an `INHERITS_REFERENCING` in the reader's own
    /// policy: a row of this table can only change whether the reader rows
    /// its `via` names are visible.
    link: Option<Link>,
}

/// The reader an [`Occurrence`]'s rows grant through `INHERITS_REFERENCING`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Link {
    /// The reader's stored table.
    reader: String,
    /// The source's stored `via` column.
    via: String,
    /// `via` is a list of references (one per element).
    list: bool,
}

/// The stored tables read policies read as evidence, each with every
/// [`Occurrence`] over it, or `None` when one could not be bounded: the
/// table is watched whole.
type Evidence = BTreeMap<String, Option<Vec<Occurrence>>>;

fn add_evidence(evidence: &mut Evidence, table: &str, occurrence: Option<Occurrence>) {
    let entry = evidence
        .entry(table.to_owned())
        .or_insert_with(|| Some(Vec::new()));
    match (entry.as_mut(), occurrence) {
        (Some(list), Some(occurrence)) => {
            if !list.contains(&occurrence) {
                list.push(occurrence);
            }
        }
        _ => *entry = None,
    }
}

fn conjuncts(condition: &PolicyExpr) -> Vec<&PolicyExpr> {
    match condition {
        PolicyExpr::And(list) => list.iter().collect(),
        other => vec![other],
    }
}

/// Reads the [`Evidence`] of read policies, in the names of `policies`
/// (the schema policies name tables and columns by), for `session`.
struct EvidenceWalk<'a> {
    policies: &'a Schema,
    session: &'a Session,
    /// The variants the store keeps for an import ([`stored_canon`]).
    kept: &'a KeptVariants,
    out: Evidence,
    /// The tables (and operations) whose policies an `INHERITS` is being
    /// read through: a chain back to one is watched whole.
    through: Vec<(String, Operation)>,
    /// The stored table whose policy is walked.
    reader: String,
}

impl EvidenceWalk<'_> {
    /// Walk `expr`, evaluated on a row of `table`: the columns of that row
    /// it reads go to `own`, those of the enclosing row (an outer-row
    /// reference) to `outer`. Returns false for a form this does not read
    /// (a relation `EXISTS`, a system column other than `$createdBy` or
    /// `$createdAt`): then everything the policy reads is unbounded.
    fn walk(
        &mut self,
        expr: &PolicyExpr,
        table: &Table,
        own: &mut BTreeSet<String>,
        outer: &mut BTreeSet<String>,
    ) -> bool {
        use crate::tools::PolicyValue;
        let value = |value: &PolicyValue, outer: &mut BTreeSet<String>| match value {
            PolicyValue::SessionRef(path)
                if path.first().map(String::as_str) == Some("__jazz_outer_row") =>
            {
                match path.get(1) {
                    Some(column) => {
                        outer.insert(column.clone());
                        true
                    }
                    None => false,
                }
            }
            _ => true,
        };
        let column = |column: &str, own: &mut BTreeSet<String>| {
            // `id` and the create stamps never change; any other system
            // column (an update or delete stamp) is not in a row's image.
            if column == "id" || column == "$createdAt" || column.starts_with("$createdBy") {
                return true;
            }
            if column.starts_with('$') {
                return false;
            }
            own.insert(column.to_owned());
            true
        };
        match expr {
            PolicyExpr::And(list) | PolicyExpr::Or(list) => {
                list.iter().all(|p| self.walk(p, table, own, outer))
            }
            PolicyExpr::Not(inner) => self.walk(inner, table, own, outer),
            PolicyExpr::Cmp {
                column: c,
                value: v,
                ..
            }
            | PolicyExpr::Contains {
                column: c,
                value: v,
            } => column(c, own) && value(v, outer),
            PolicyExpr::InList { column: c, values } => {
                column(c, own) && values.iter().all(|v| value(v, outer))
            }
            PolicyExpr::IsNull { column: c }
            | PolicyExpr::IsNotNull { column: c }
            | PolicyExpr::In { column: c, .. } => column(c, own),
            PolicyExpr::SessionCmp { .. }
            | PolicyExpr::SessionIsNull { .. }
            | PolicyExpr::SessionIsNotNull { .. }
            | PolicyExpr::SessionContains { .. }
            | PolicyExpr::SessionInList { .. }
            | PolicyExpr::True
            | PolicyExpr::False => true,
            PolicyExpr::Exists {
                table: target,
                condition,
            } => {
                let Ok(target) = self.policies.table(target) else {
                    return false;
                };
                let mut columns = BTreeSet::new();
                if !self.walk(condition, target, &mut columns, own) {
                    return false;
                }
                let pin = conjuncts(condition)
                    .into_iter()
                    .find_map(|c| value_pin(target, c, self.session, self.kept));
                self.occurrence(target, pin, columns);
                true
            }
            PolicyExpr::Inherits {
                operation,
                via_column,
                ..
            } => {
                if !column(via_column, own) {
                    return false;
                }
                let Some(target) = table
                    .references
                    .get(via_column)
                    .and_then(|name| self.policies.table(name).ok())
                else {
                    return false;
                };
                self.inherit(target, *operation, BTreeSet::new(), None)
            }
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                ..
            } => {
                let Ok(source) = self.policies.table(source_table) else {
                    return false;
                };
                let mut columns = BTreeSet::new();
                // Only the reader's own policy: one reached through another
                // table's INHERITS decides other rows. A `via` that holds no
                // row references cannot be matched to held parents.
                let link = (self.through.is_empty() && table.stored == self.reader)
                    .then(|| link_kind(source, via_column))
                    .flatten()
                    .map(|list| Link {
                        reader: table.stored.clone(),
                        via: source.stored_name(via_column).to_owned(),
                        list,
                    });
                column(via_column, &mut columns) && self.inherit(source, *operation, columns, link)
            }
            PolicyExpr::ExistsRel { .. } => false,
        }
    }

    /// Rows of `table` are read through its `operation` policy, besides
    /// `columns` of them: one occurrence, pinned when the policy requires
    /// `column = session value` of the row. A chain back to a table already
    /// being read through (a parent chain) is watched whole.
    fn inherit(
        &mut self,
        table: &Table,
        operation: Operation,
        mut columns: BTreeSet<String>,
        link: Option<Link>,
    ) -> bool {
        let key = (table.stored.clone(), operation);
        if self.through.contains(&key) {
            add_evidence(&mut self.out, &table.stored, None);
            return true;
        }
        let Some(policy) = table.policy(operation) else {
            // No policy: the inherited check is false whatever the rows.
            return true;
        };
        self.through.push(key);
        let read = self.walk(policy, table, &mut columns, &mut BTreeSet::new());
        self.through.pop();
        if !read {
            return false;
        }
        let pin = conjuncts(policy)
            .into_iter()
            .find_map(|c| value_pin(table, c, self.session, self.kept));
        self.linked_occurrence(table, pin, columns, link);
        true
    }

    fn occurrence(&mut self, table: &Table, pin: Option<Pin>, columns: BTreeSet<String>) {
        self.linked_occurrence(table, pin, columns, None)
    }

    fn linked_occurrence(
        &mut self,
        table: &Table,
        pin: Option<Pin>,
        columns: BTreeSet<String>,
        link: Option<Link>,
    ) {
        let columns = columns
            .iter()
            .map(|c| table.stored_name(c).to_owned())
            .collect();
        add_evidence(
            &mut self.out,
            &table.stored,
            Some(Occurrence { pin, columns, link }),
        );
    }
}

/// A stored column and the canonical value it must hold.
type Pin = (String, String);

/// `column = session value` as a [`Pin`], when `column` is an application
/// column (only those are in a changed row's image; not `$createdBy`)
/// whose stored form the value's canonical form matches: a UUID column
/// against a UUID (or a claim spelling one, as the policy compares it),
/// text against text, an integer against an integer, a boolean against a
/// boolean, an enum against a variant name.
fn value_pin(
    table: &Table,
    conjunct: &PolicyExpr,
    session: &Session,
    kept: &KeptVariants,
) -> Option<Pin> {
    use crate::tools::PolicyValue;
    let PolicyExpr::Cmp {
        column,
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(path),
    } = conjunct
    else {
        return None;
    };
    if path.first().map(String::as_str) == Some("__jazz_outer_row") {
        return None;
    }
    let stored = table.columns.iter().find(|c| c.name == *column)?;
    let ty = match &stored.ty {
        ValueType::Nullable(inner) => inner.as_ref(),
        ty => ty,
    };
    let mut value = session.resolve(path)?;
    if crate::compile::is_claim(path) {
        value = crate::values::claim_as(ty, value)?;
    }
    let value = match (ty, value) {
        (ValueType::Uuid, value @ Value::Uuid(_)) => value,
        (ValueType::String, value @ Value::String(_)) => value,
        (_, value @ Value::String(_)) if stored.tags.is_some() => value,
        (
            ValueType::I32 | ValueType::I64 | ValueType::U8 | ValueType::U16 | ValueType::U32,
            value @ (Value::I32(_) | Value::I64(_) | Value::U8(_) | Value::U16(_) | Value::U32(_)),
        ) => value,
        (ValueType::Bool, value @ Value::Bool(_)) => value,
        _ => return None,
    };
    let value = stored_canon(Some(stored), canon_value(&value)?, kept)?;
    Some((stored.stored.clone(), value))
}

/// Policy pins (see [`policy_pins`]) by stored table and column. A table
/// the schema does not know stays unpinned (it is watched whole).
/// `schema`'s tables and columns named other than they are stored, in a
/// canonical text form (empty without renames).
fn renames(schema: &Schema) -> String {
    let mut out = String::new();
    for table in schema.tables.values() {
        let columns = table.columns.iter().filter(|c| c.name != c.stored);
        if table.name == table.stored && columns.clone().next().is_none() {
            continue;
        }
        out.push_str(&format!("{:?}={:?}:", table.name, table.stored));
        for column in columns {
            out.push_str(&format!("{:?}={:?},", column.name, column.stored));
        }
        out.push(';');
    }
    out
}

pub(super) fn stored_pins(
    schema: &Schema,
    pins: HashMap<String, Option<(String, String)>>,
) -> HashMap<String, Option<(String, String)>> {
    pins.into_iter()
        .filter_map(|(table, pin)| {
            let table = schema.table(&table).ok()?;
            let pin = pin.map(|(column, value)| {
                // These pins are made anew for each tick's writes, after
                // their cells are stored: a variant with no tag yet is in
                // none of them, and its name, which no cell holds, rightly
                // matches none. So no variant counts as kept here.
                let value =
                    stored_canon(table.column(&column), value.clone(), &KeptVariants::new())
                        .unwrap_or(value);
                (table.stored_name(&column).to_owned(), value)
            });
            Some((table.stored.clone(), pin))
        })
        .collect()
}

/// `column = session value` required of the protected row itself: the
/// policy is that comparison, or a conjunction containing it.
fn row_pin(table: &Table, policy: &PolicyExpr, session: &Session) -> Option<(String, String)> {
    use crate::tools::PolicyValue;
    match policy {
        PolicyExpr::Cmp {
            column,
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(path),
        } if path.first().map(String::as_str) != Some("__jazz_outer_row") => {
            session_value_for(path, table.column_type(column), session)
                .as_ref()
                .and_then(canon_value)
                .map(|value| (column.clone(), value))
        }
        PolicyExpr::And(list) => list.iter().find_map(|p| row_pin(table, p, session)),
        _ => None,
    }
}

/// `session.<path>` as a policy compares it with a column of type `ty`: a
/// provider claim converted by [`crate::values::claim_as`], as the
/// compiled policy converts it, so a pin holds the value the policy does.
fn session_value_for(path: &[String], ty: Option<&ValueType>, session: &Session) -> Option<Value> {
    let value = session.resolve(path)?;
    match ty {
        Some(ty) if crate::compile::is_claim(path) => crate::values::claim_as(ty, value),
        _ => Some(value),
    }
}

/// Every array relation level under a parent of `parent` (whose rows
/// carry `columns`), depth first, each with whether it qualifies for a
/// correlation watch on its own (see [`Store::correlations`]).
fn relation_levels(
    schema: &Schema,
    parent: &Table,
    columns: &[String],
    arrays: &[crate::query::ArraySubquery],
    path: &mut Vec<String>,
    out: &mut Vec<(Correlation, bool)>,
) -> Result<(), Error> {
    for array in arrays {
        let child = schema.table(&array.table)?;
        let inner = link_kind(child, &array.inner_column);
        let outer = link_kind(parent, &array.outer_column);
        let carried = array.outer_column == "id" || columns.contains(&array.outer_column);
        let fit = array.requirement == crate::query::ArraySubqueryRequirement::Optional
            && array.offset == 0
            && inner.is_some()
            && outer.is_some()
            && carried;
        path.push(array.column_name.clone());
        out.push((
            Correlation {
                table: child.stored.clone(),
                path: path.clone(),
                inner: if array.inner_column == "id" {
                    "id".to_owned()
                } else {
                    child.stored_name(&array.inner_column).to_owned()
                },
                inner_list: inner == Some(true),
                outer: array.outer_column.clone(),
            },
            fit,
        ));
        let child_columns: Vec<String> = match &array.select {
            Some(select) => select.clone(),
            None => child.columns.iter().map(|c| c.name.clone()).collect(),
        };
        relation_levels(
            schema,
            child,
            &child_columns,
            &array.nested_arrays,
            path,
            out,
        )?;
        path.pop();
    }
    Ok(())
}

/// A correlation column's shape when it holds row references: `Some(false)`
/// for `id` or a UUID column, `Some(true)` for a list of UUIDs, `None` for
/// anything else (its stored form need not compare as its value does).
fn link_kind(table: &Table, column: &str) -> Option<bool> {
    if column == "id" {
        return Some(false);
    }
    let column = table.column(column)?;
    if column.tags.is_some() {
        return None;
    }
    let unwrap = |ty: &ValueType| match ty {
        ValueType::Nullable(inner) => inner.as_ref().clone(),
        ty => ty.clone(),
    };
    match unwrap(&column.ty) {
        ValueType::Uuid => Some(false),
        ValueType::Array(element) if unwrap(&element) == ValueType::Uuid => Some(true),
        _ => None,
    }
}

/// A row of a result at some relation depth: a root row, or a related
/// row's record nested in its parent's relation cell.
enum Node<'a> {
    Row(&'a CurrentRow),
    Record(crate::records::OwnedRecord),
}

impl Node<'_> {
    fn id(&self) -> Option<RowUuid> {
        match self {
            Node::Row(row) => Some(row.id),
            Node::Record(record) => match record.get("row_uuid").ok()? {
                Value::Uuid(id) => Some(RowUuid(id)),
                _ => None,
            },
        }
    }

    /// The value of `column` (`id` included); `None` when absent.
    fn value(&self, column: &str) -> Option<Value> {
        if column == "id" {
            return self.id().map(|id| Value::Uuid(id.0));
        }
        match self {
            Node::Row(row) => row
                .cells
                .iter()
                .find(|(name, _)| name == column)
                .and_then(|(_, value)| value.clone()),
            Node::Record(record) => record.get(column).ok(),
        }
    }

    /// The related rows in relation cell `column`; `None` when absent.
    fn children(&self, column: &str) -> Option<Vec<Node<'static>>> {
        let mut value = self.value(column)?;
        while let Value::Nullable(Some(inner)) = value {
            value = *inner;
        }
        match value {
            Value::Array(items) => items
                .into_iter()
                .map(|item| match item {
                    Value::Record(record) => Some(Node::Record(record)),
                    _ => None,
                })
                .collect(),
            Value::Nullable(None) => Some(Vec::new()),
            _ => None,
        }
    }
}

/// Each root row's share of a group's `held`, with the cells it was read
/// from (kept alive, so their address names them).
type Shares = HashMap<RowUuid, (Arc<Vec<(String, Option<Value>)>>, Vec<Held>)>;

/// Bring `held` (one per level of `correlations`) up to `rows`. When
/// `incremental` and the shares are built (`!stale`), only the shares of
/// root rows that came, left or changed (their cells are another `Arc`)
/// move. Otherwise (after a run, or a root row more than once) the rows
/// are counted in one pass and the shares are dropped, stale until
/// [`share_rows`] builds them again: counting shares on top of a full
/// count would hold the keys of rows that have left.
fn keep_held(
    correlations: &[Correlation],
    rows: &[CurrentRow],
    held_now: &mut Vec<Held>,
    by_root: &mut Shares,
    stale: &mut bool,
    incremental: bool,
) {
    if held_now.len() != correlations.len() {
        *held_now = vec![Held::default(); correlations.len()];
        by_root.clear();
        *stale = true;
    }
    let ids: Option<HashSet<RowUuid>> = (incremental && !*stale)
        .then(|| rows.iter().map(|row| row.id).collect())
        .filter(|ids: &HashSet<RowUuid>| ids.len() == rows.len());
    if let Some(ids) = ids {
        let gone: Vec<RowUuid> = by_root
            .keys()
            .filter(|id| !ids.contains(id))
            .copied()
            .collect();
        for id in gone {
            if let Some((_, shares)) = by_root.remove(&id) {
                for (sum, share) in held_now.iter_mut().zip(&shares) {
                    sum.sub(share);
                }
            }
        }
        for row in rows {
            if by_root
                .get(&row.id)
                .is_some_and(|(cells, _)| Arc::ptr_eq(cells, &row.cells))
            {
                continue;
            }
            let shares: Vec<Held> = correlations
                .iter()
                .map(|level| held(level, std::slice::from_ref(row)))
                .collect();
            for (sum, share) in held_now.iter_mut().zip(&shares) {
                sum.add(share);
            }
            if let Some((_, old)) = by_root.insert(row.id, (row.cells.clone(), shares)) {
                for (sum, share) in held_now.iter_mut().zip(&old) {
                    sum.sub(share);
                }
            }
        }
    } else {
        by_root.clear();
        *stale = true;
        *held_now = correlations.iter().map(|level| held(level, rows)).collect();
    }
    // A level with no path is never complete (see `held`).
    for (level, sum) in correlations.iter().zip(held_now.iter_mut()) {
        if level.path.is_empty() {
            sum.incomplete = sum.incomplete.max(1);
        }
    }
}

/// Build each root row's share of a `held` that already counts `rows`,
/// before a re-check or a re-nest changes some of them. Rows sharing an
/// id get no shares: [`keep_held`] then counts in one pass again.
fn share_rows(
    correlations: &[Correlation],
    rows: &[CurrentRow],
    by_root: &mut Shares,
    stale: &mut bool,
) {
    if !*stale || correlations.is_empty() {
        return;
    }
    let mut shares = HashMap::with_capacity(rows.len());
    for row in rows {
        let share: Vec<Held> = correlations
            .iter()
            .map(|level| held(level, std::slice::from_ref(row)))
            .collect();
        if shares.insert(row.id, (row.cells.clone(), share)).is_some() {
            return;
        }
    }
    *by_root = shares;
    *stale = false;
}

/// What `rows` hold at `level`.
fn held(level: &Correlation, rows: &[CurrentRow]) -> Held {
    let mut held = Held::default();
    let Some((last, through)) = level.path.split_last() else {
        held.incomplete += 1;
        return held;
    };
    let mut parents: Vec<Node> = rows.iter().map(Node::Row).collect();
    for column in through {
        let mut next = Vec::new();
        for parent in &parents {
            match parent.children(column) {
                Some(children) => next.extend(children),
                None => held.incomplete += 1,
            }
        }
        parents = next;
    }
    for parent in &parents {
        match parent.value(&level.outer) {
            Some(value) => {
                if !value_atoms(&value, &mut held.parents) {
                    held.incomplete += 1;
                }
            }
            None => held.incomplete += 1,
        }
        match parent.children(last) {
            Some(children) => {
                for child in children {
                    match child.id() {
                        Some(id) => {
                            *held.rows.entry(id).or_default() += 1;
                        }
                        None => held.incomplete += 1,
                    }
                }
            }
            None => held.incomplete += 1,
        }
    }
    held
}

/// Add a reference value's canonical stored forms (one per list element)
/// to `out`; false when one has none.
fn value_atoms(value: &Value, out: &mut impl AtomSink) -> bool {
    match value {
        Value::Nullable(None) => true,
        Value::Nullable(Some(inner)) => value_atoms(inner, out),
        Value::Array(items) => items.iter().all(|item| value_atoms(item, out)),
        value => match canon_value(value) {
            Some(atom) => {
                out.put(atom);
                true
            }
            None => false,
        },
    }
}

/// A stored reference cell's canonical forms (one per element of a list,
/// stored as a JSON array of UUID strings); `None` when it cannot be read
/// as one.
fn stored_atoms(cell: &Sql, list: bool) -> Option<Vec<String>> {
    match cell {
        Sql::Null => Some(Vec::new()),
        cell if !list => Some(vec![canon(cell)]),
        Sql::Text(json) => {
            let items: Vec<serde_json::Value> = serde_json::from_slice(json).ok()?;
            items
                .iter()
                .map(|item| match item {
                    serde_json::Value::Null => Some(None),
                    item => {
                        let id = uuid::Uuid::parse_str(item.as_str()?).ok()?;
                        Some(Some(canon(&Sql::Blob(id.as_bytes().to_vec()))))
                    }
                })
                .collect::<Option<Vec<Option<String>>>>()
                .map(|atoms| atoms.into_iter().flatten().collect())
        }
        _ => None,
    }
}

/// Whether a change to row `row` of `table`, with stored images `images`
/// (`None`: unreadable), can move `group`'s result through its
/// correlation levels on `table`.
/// Whether a changed row of `table`, with `images` (its current image per
/// branch and its previous one; `None` when they could not be read) or
/// the last image of a row that is `gone`, can change the value of an
/// [`Occurrence`] of `table` in `group`'s read policies: the row meets the
/// occurrence's pin in some image, and it was inserted, deleted or
/// removed, or changed a column the occurrence reads.
fn evidenced(
    group: &Group,
    table: &str,
    images: Option<&[Vec<(String, Sql)>]>,
    gone: bool,
) -> bool {
    let Some(images) = images else {
        return true;
    };
    let Some(occurrences) = group.evidence.get(table) else {
        return true;
    };
    let cell = |image: &[(String, Sql)], column: &str| {
        image
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, cell)| cell.clone())
    };
    let deleted = |image: &[(String, Sql)]| match cell(image, "_state") {
        Some(Sql::Integer(state)) => Some(state & STATE_DELETED != 0),
        _ => None,
    };
    occurrences.iter().any(|occurrence| {
        let pinned = occurrence.pin.as_ref().is_none_or(|(column, value)| {
            images
                .iter()
                .any(|image| cell(image, column).is_none_or(|cell| canon(&cell) == *value))
        });
        // One current image and one previous one, both read: a write that
        // kept the row's liveness and every column the occurrence reads
        // cannot change it.
        let changed = gone
            || match images {
                [new, old] => {
                    deleted(new).is_none()
                        || deleted(new) != deleted(old)
                        || occurrence.columns.iter().any(|column| {
                            cell(new, column).is_none() || cell(new, column) != cell(old, column)
                        })
                }
                _ => true,
            };
        pinned && changed && linked(group, occurrence, images)
    })
}

/// Whether a change matching a linked [`Occurrence`] can reach the
/// result. The reader rows its `via` names (in its old or new image) are
/// the only ones it can show or hide; when the result reads the reader
/// only at relation levels keyed by `id` (every one complete), those rows
/// matter only if a held parent names them. Anything else: yes.
fn linked(group: &Group, occurrence: &Occurrence, images: &[Vec<(String, Sql)>]) -> bool {
    let Some(Link { reader, via, list }) = occurrence.link.as_ref() else {
        return true;
    };
    if group.awaiting || group.root == *reader {
        return true;
    }
    let levels: Vec<&Held> = group
        .correlations
        .iter()
        .zip(&group.held)
        .filter(|(level, _)| level.table == *reader)
        .map(|(level, held)| (level.inner == "id" && held.complete()).then_some(held))
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default();
    if levels.is_empty()
        || levels.len()
            != group
                .correlations
                .iter()
                .filter(|l| l.table == *reader)
                .count()
    {
        return true;
    }
    images.iter().any(|image| {
        image
            .iter()
            .filter(|(column, _)| column == via)
            .any(|(_, cell)| match stored_atoms(cell, *list) {
                Some(atoms) => atoms
                    .iter()
                    .any(|atom| levels.iter().any(|held| held.parents.contains_key(atom))),
                None => true,
            })
    }) || !images
        .iter()
        .any(|image| image.iter().any(|(column, _)| column == via))
}

fn correlated(
    group: &Group,
    table: &str,
    row: RowUuid,
    images: Option<&[Vec<(String, Sql)>]>,
) -> bool {
    if group.awaiting
        || (group.root == table
            && group
                .root_held
                .as_ref()
                .is_some_and(|held| held.contains(&row)))
    {
        return true;
    }
    group
        .correlations
        .iter()
        .zip(&group.held)
        .filter(|(level, _)| level.table == table)
        .any(|(level, held)| {
            if !held.complete() || held.rows.contains_key(&row) {
                return true;
            }
            if level.inner == "id" {
                return held
                    .parents
                    .contains_key(&canon(&Sql::Blob(row.0.as_bytes().to_vec())));
            }
            let Some(images) = images else {
                return true;
            };
            images.iter().any(|image| {
                image
                    .iter()
                    .filter(|(column, _)| *column == level.inner)
                    .any(|(_, cell)| match stored_atoms(cell, level.inner_list) {
                        Some(atoms) => atoms.iter().any(|atom| held.parents.contains_key(atom)),
                        None => true,
                    })
            })
        })
}

/// The cut a run read through, as [`Group::applied`] keeps it.
fn cut_of(read: &ReadSpec) -> Option<(sync::Pos, usize)> {
    (read.position != Position::Current)
        .then_some(read.cut.as_ref())
        .flatten()
        .map(|cut| (cut.seq, cut.extra.len()))
}

#[cfg(test)]
mod hold_tests {
    use super::*;

    /// A message by `author`, its `writer` relation read and empty.
    fn message(id: u8, author: u8) -> CurrentRow {
        CurrentRow {
            table: Arc::from("messages"),
            id: RowUuid::from_bytes([id; 16]),
            seq: crate::db::Pos::at(id as u64),
            writer: None,
            deleted: false,
            cells: Arc::new(vec![
                (
                    "author".to_owned(),
                    Some(Value::Uuid(RowUuid::from_bytes([author; 16]).0)),
                ),
                ("writer".to_owned(), Some(Value::Array(Vec::new()))),
            ]),
            types: Arc::from([ValueType::Uuid, ValueType::Array(Box::new(ValueType::Uuid))]),
            joined: Arc::from([]),
            version: None,
            union_arm: None,
        }
    }

    /// The messages' `writer` relation: users by the message's `author`.
    fn writer() -> Vec<Correlation> {
        vec![Correlation {
            table: "users".to_owned(),
            path: vec!["writer".to_owned()],
            inner: "id".to_owned(),
            inner_list: false,
            outer: "author".to_owned(),
        }]
    }

    fn full(rows: &[CurrentRow]) -> Vec<Held> {
        writer().iter().map(|level| held(level, rows)).collect()
    }

    #[test]
    fn a_result_that_held_a_root_row_twice_then_once_holds_only_its_rows() {
        let levels = writer();
        let (mut sums, mut shares, mut stale) = (Vec::new(), Shares::new(), true);
        // A root row more than once: counted in one pass, no shares.
        let twice = vec![message(1, 10), message(1, 11)];
        keep_held(&levels, &twice, &mut sums, &mut shares, &mut stale, false);
        share_rows(&levels, &twice, &mut shares, &mut stale);
        assert!(stale && shares.is_empty());
        // A re-check leaves each root row once: no author of the rows
        // before may stay held.
        let once = vec![message(2, 12)];
        keep_held(&levels, &once, &mut sums, &mut shares, &mut stale, true);
        assert_eq!(sums, full(&once));
        // Its shares built, the next re-check moves only its own.
        share_rows(&levels, &once, &mut shares, &mut stale);
        assert!(!stale);
        let next = vec![once[0].clone(), message(3, 13)];
        keep_held(&levels, &next, &mut sums, &mut shares, &mut stale, true);
        assert_eq!(sums, full(&next));
    }

    #[test]
    fn kept_shares_follow_rows_that_come_leave_and_change() {
        let levels = writer();
        let (mut sums, mut shares, mut stale) = (Vec::new(), Shares::new(), true);
        let rows = vec![message(1, 10), message(2, 10), message(3, 11)];
        keep_held(&levels, &rows, &mut sums, &mut shares, &mut stale, false);
        assert!(stale, "a run keeps no shares");
        share_rows(&levels, &rows, &mut shares, &mut stale);
        // Row 2 leaves, row 3 changes author, row 4 comes; row 1 is the
        // same `Arc` and keeps its share.
        let rows = vec![rows[0].clone(), message(3, 12), message(4, 13)];
        keep_held(&levels, &rows, &mut sums, &mut shares, &mut stale, true);
        assert!(!stale);
        assert_eq!(sums, full(&rows));
        // A run after it counts in one pass again.
        keep_held(&levels, &rows, &mut sums, &mut shares, &mut stale, false);
        assert!(stale && shares.is_empty());
        assert_eq!(sums, full(&rows));
    }
}

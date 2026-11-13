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
#[derive(Clone, Debug, Default)]
struct Held {
    /// The parents' `outer` values, canonical, one per list element.
    parents: HashSet<String>,
    /// The rows the relation holds under them.
    rows: HashSet<RowUuid>,
    /// False when a parent's value or a row's id could not be read: then
    /// every change to the table wakes the group.
    complete: bool,
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
    /// The cut the last run read through (see [`Inner::visible_read`]):
    /// its position and the own commits past it, or `None` for the
    /// current state.
    applied: Option<(sync::Pos, usize)>,
    /// The array relation levels whose tables are watched by correlation
    /// ([`Watch::Related`]), and what the result holds at each, parallel.
    correlations: Vec<Correlation>,
    held: Vec<Held>,
    /// When the root table is watched by correlation too (a
    /// self-relation): the root rows the result holds, which wake it
    /// whatever their images say.
    root_held: Option<HashSet<RowUuid>>,
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
    pub woken: u64,
    pub skipped: u64,
    /// Woken groups that applied their root rows by re-checking ids.
    pub rechecked: u64,
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
    stats: FanoutStats,
    /// Bumped by every dropped stream, so `collect` scans only when needed.
    pub(super) dropped: Rc<std::cell::Cell<usize>>,
}

/// The canonical form of a stored SQL value, for watch matching.
fn canon(value: &Sql) -> String {
    format!("{value:?}")
}

fn canon_value(value: &Value) -> Option<String> {
    to_sql(value).ok().flatten().map(|sql| canon(&sql))
}

/// A value's canonical form ([`canon_value`]) as column `column` stores it:
/// an enum variant's name as its tag where it has one, so it matches the
/// cells of a changed row's image.
fn stored_canon(column: Option<&crate::values::Column>, value: String) -> String {
    let Some(tags) = column.and_then(|column| column.tags.as_ref()) else {
        return value;
    };
    let tags = tags.borrow();
    tags.iter()
        .find(|(_, name)| canon(&Sql::Text(name.as_bytes().to_vec())) == value)
        .map_or(value, |(tag, _)| canon(&Sql::Integer(tag)))
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

    pub(super) fn stats(&self) -> FanoutStats {
        FanoutStats {
            groups: self.groups.len(),
            ..self.stats.clone()
        }
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
                if member.settled.get()
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

impl Inner {
    /// Open a subscription: join the group with identical SQL, or start one.
    pub(super) fn subscribe_member(
        &mut self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: ReadSpec,
        settled: sync::SettledBy,
    ) -> Result<(Queue, Weak<RefCell<Group>>), Error> {
        self.check_splices(&prepared.query, &read.splices)?;
        let (compiled, session_used) = self.compile_tracked(prepared, identity, &read)?;
        let key = GroupKey {
            renames: renames(prepared.schema.as_ref().unwrap_or(&self.owner)),
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
        let member = |queue: &Queue| Member {
            queue: Rc::downgrade(queue),
            shape: prepared.shape,
            bound: !prepared.binding.is_empty(),
            settled: settled.clone(),
            told: std::cell::Cell::new(now.then(|| settled.tier())),
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
            .by_key
            .get(&key)
            .and_then(|gid| self.fan.groups.get(gid))
            .cloned()
        {
            let mut group = handle.borrow_mut();
            let queue: Queue = Rc::new(RefCell::new(if group.opened {
                VecDeque::from([open(&group.rows)])
            } else {
                VecDeque::new()
            }));
            group.members.push(member(&queue));
            self.fan.stats.shared_joins += 1;
            return Ok((queue, Rc::downgrade(&handle)));
        }
        // A replica missing chunks the result needs opens it once they
        // arrive. Its cut is decided over its relations' tables too.
        let tables = self.with_relation_tables(prepared, &compiled.tables)?;
        let visible = self.visible_read(&read, &tables);
        let applied = cut_of(&visible);
        let run = match &visible {
            Cow::Borrowed(_) => self.execute(&compiled, &prepared.query.table),
            Cow::Owned(visible) => self
                .compile(prepared, identity, visible)
                .and_then(|compiled| self.execute(&compiled, &prepared.query.table)),
        };
        let (rows, awaiting) =
            match run.and_then(|rows| self.nest_relations(prepared, identity, &visible, rows)) {
                Ok(rows) => (rows, false),
                Err(error) if crate::large::is_missing(&error) => (Vec::new(), true),
                Err(error) => return Err(error),
            };
        let (watches, correlations, evidence) =
            self.watch_plan(prepared, identity, &read, &compiled)?;
        let root = prepared
            .schema
            .as_ref()
            .unwrap_or(&self.owner)
            .stored_table(
                history_source_table(&prepared.query.table).unwrap_or(&prepared.query.table),
            )?
            .to_owned();
        let recheck = recheckable(&prepared.query, &root, &read, &compiled);
        let branches = view_branches(
            prepared.schema.as_ref().unwrap_or(&self.owner),
            &read,
            &watches,
        );
        let masks = view_masks(
            prepared.schema.as_ref().unwrap_or(&self.owner),
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
        let gid = self.fan.next;
        self.fan.next += 1;
        self.fan.index(gid, &watches);
        self.fan.by_key.insert(key.clone(), gid);
        let handle = Rc::new(RefCell::new(Group {
            key,
            prepared: prepared.clone(),
            root,
            identity,
            read,
            rows,
            members: vec![member(&queue)],
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
            root_held: None,
        }));
        Self::hold(&mut handle.borrow_mut());
        self.read_keys(&mut handle.borrow_mut());
        let weak = Rc::downgrade(&handle);
        self.fan.groups.insert(gid, handle);
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
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
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
            let policies = prepared
                .schema
                .as_ref()
                .map_or(&*self.owner, |schema| schema.policy_schema());
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
        let compiled = self
            .read_ctx(prepared, policy.as_ref(), &claims, read)
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
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
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
            .read_ctx(&bare, None, &claims, read)
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
                    root.policy(Operation::Select)
                        .and_then(|p| row_pin(p, &session))
                })
                .and_then(|(column, value)| {
                    let governing = policies.by_stored(&root.stored)?;
                    // As for a query key: only an application column is in
                    // a changed row's image (`$createdBy.account` is not).
                    governing.columns.iter().any(|c| c.name == column).then(|| {
                        let value = stored_canon(governing.column(&column), value);
                        (governing.stored_name(&column).to_owned(), value)
                    })
                });
            root_key(query, &prepared.binding)
                // Only an application column is in a changed row's image;
                // a key on anything else (`$createdBy.identity.subject`,
                // `id`) would never be hit.
                .filter(|(column, _)| root.columns.iter().any(|c| c.name == *column))
                .map(|(column, value)| {
                    let value = stored_canon(root.column(&column), value);
                    (root.stored_name(&column).to_owned(), value)
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
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
        let policies = schema.policy_schema();
        for reader in readers {
            let Some(protected) = policies.by_stored(reader) else {
                continue;
            };
            let reads = self.policy_reads(policies, reader, author)?;
            let mut walk = EvidenceWalk {
                policies,
                session: &session,
                out: Evidence::new(),
                through: Vec::new(),
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
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
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
    /// levels, after a run replaced its rows.
    fn hold(group: &mut Group) {
        group.held = group
            .correlations
            .iter()
            .map(|level| held(level, &group.rows))
            .collect();
        group.root_held = group
            .correlations
            .iter()
            .any(|level| level.table == group.root)
            .then(|| group.rows.iter().map(|row| row.id).collect());
    }

    /// Wake the groups a commit's changed rows hit, re-run and diff them.
    pub(super) fn dispatch(&mut self, mut changes: Vec<(String, RowUuid, Vec<u8>)>) {
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
        let groups = &self.fan.groups;
        let mut wake = |gids: &BTreeSet<u64>, table: &str, row: RowUuid, key: &[u8]| {
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
            if let Some(groups) = self.fan.tables.get(table) {
                wake(groups, table, *row, key);
            }
            let Some(columns) = self.fan.key_columns.get(table) else {
                continue;
            };
            for image in self.images(table, *row, key, columns) {
                for (column, value) in image {
                    if let Some(groups) = self.fan.keys.get(&(table.clone(), column, value)) {
                        wake(groups, table, *row, key);
                    }
                }
            }
        }
        let gone = std::mem::take(&mut *self.gone.borrow_mut());
        for image in &gone {
            let row = RowUuid::from_bytes(image.id);
            if let Some(groups) = self.fan.tables.get(&image.table) {
                wake(groups, &image.table, row, &image.key);
            }
            let Some(columns) = self.fan.key_columns.get(&image.table) else {
                continue;
            };
            for (column, cell) in &image.cells {
                if columns.contains(column)
                    && let Some(groups) = self.fan.keys.get(&(
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
        let mut related = |table: &str, row: RowUuid, images: Option<&[Vec<(String, Sql)>]>| {
            for gid in self.fan.related.get(table).into_iter().flatten() {
                if woken.get(gid).is_some_and(Option::is_none) {
                    continue;
                }
                let Some(group) = self.fan.groups.get(gid) else {
                    continue;
                };
                if correlated(&group.borrow(), table, row, images) {
                    woken.insert(*gid, None);
                } else {
                    uncorrelated += 1;
                }
            }
        };
        for (table, row, key) in &changes {
            let Some(gids) = self.fan.related.get(table) else {
                continue;
            };
            let columns: BTreeSet<String> = gids
                .iter()
                .filter_map(|gid| self.fan.groups.get(gid))
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
                self.stored_images(table, *row, key, &columns)
            };
            let images = images.filter(|_| !repeated.contains(&(table.clone(), *row, key.clone())));
            related(table, *row, images.as_deref());
        }
        for image in &gone {
            if self.fan.related.contains_key(&image.table) {
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
            for gid in self.fan.evidence.get(table).into_iter().flatten() {
                if woken.get(gid).is_some_and(Option::is_none) {
                    continue;
                }
                let Some(group) = self.fan.groups.get(gid) else {
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
            let Some(gids) = self.fan.evidence.get(table) else {
                continue;
            };
            let mut columns: BTreeSet<String> = gids
                .iter()
                .filter_map(|gid| self.fan.groups.get(gid))
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
            let images = self.stored_images(table, *row, key, &columns);
            let images = images.filter(|_| !repeated.contains(&(table.clone(), *row, key.clone())));
            evident(table, images.as_deref(), false);
        }
        for image in &gone {
            if self.fan.evidence.contains_key(&image.table) {
                let cells: Vec<(String, Sql)> = image
                    .cells
                    .iter()
                    .map(|(column, cell)| (column.clone(), Sql::from(cell)))
                    .collect();
                evident(&image.table, Some(std::slice::from_ref(&cells)), true);
            }
        }
        self.fan.stats.unevidenced += unread;
        self.fan.stats.uncorrelated += uncorrelated;
        self.fan.stats.woken += woken.len() as u64;
        self.fan.stats.skipped += (self.fan.groups.len() - woken.len()) as u64;
        let groups: Vec<_> = woken
            .into_iter()
            .filter_map(|(gid, ids)| Some((gid, self.fan.groups.get(&gid).cloned()?, ids)))
            .collect();
        for (gid, group, ids) in groups {
            let mut group = group.borrow_mut();
            // Only include targets changed: the result's rows are as before.
            if ids.as_ref().is_none_or(|ids| !ids.is_empty()) {
                if self.refresh_group(&mut group, ids.as_ref()) {
                    self.fan.stats.reran += 1;
                } else {
                    self.fan.stats.rechecked += 1;
                }
            }
            if let Some(changed) = targets.remove(&gid) {
                refresh_includes(&mut group, &changed);
            }
        }
    }

    /// Re-run every group (schema changed); watches are rebuilt because
    /// they can depend on session values.
    /// The active schema changed: every subscription re-runs and restates
    /// its rows (a reset), since their shape may have changed.
    pub(super) fn reset_everything(&mut self) {
        self.refresh_where(|_| true, true);
    }

    /// `author`'s claims changed: re-run only the groups whose policy or
    /// claim operands read them. A group with no policy identity takes its
    /// claims from this node's own author; one with request claims carries
    /// its own and is unaffected.
    pub(super) fn refresh_identity(&mut self, author: AuthorSubject) {
        let own = self.identity.author;
        self.refresh_where(
            |group| {
                group.prepared.request_claims.is_none()
                    && (group.identity == Some(author)
                        || (group.identity.is_none() && author == own))
            },
            false,
        );
    }

    fn refresh_where(&mut self, affected: impl Fn(&Group) -> bool, reset: bool) {
        let groups: Vec<(u64, Rc<RefCell<Group>>)> = self
            .fan
            .groups
            .iter()
            .filter(|(_, g)| affected(&g.borrow()))
            .map(|(id, g)| (*id, g.clone()))
            .collect();
        for (gid, group) in groups {
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
            self.fan.unindex(gid, &old);
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
                            Watch::Related(table) | Watch::Evidence(table) => Watch::Table(table),
                            watch => watch,
                        })
                        .collect();
                    (watches, Vec::new(), HashMap::new())
                });
            self.fan.index(gid, &watches);
            group.watches = watches;
            group.evidence = evidence;
            group.correlations = correlations;
            Self::hold(&mut group);
            if reset {
                self.reset_group(&mut group);
            } else {
                self.refresh_group(&mut group, None);
            }
        }
    }

    /// Old (pre-commit, from history) and new (current) values of the
    /// watched key columns of one changed row. "New" covers the row on every
    /// branch: a copy-on-write head row replaces, in a branch view, the base
    /// row a subscription saw, so the base image must wake it too.
    pub(super) fn images(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        columns: &BTreeSet<String>,
    ) -> Vec<Vec<(String, String)>> {
        self.stored_images(table, row, key, columns)
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
    fn stored_images(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        columns: &BTreeSet<String>,
    ) -> Option<Vec<Vec<(String, Sql)>>> {
        let read = |sql: String,
                    params: &[&dyn rusqlite::ToSql]|
         -> Option<Vec<(i64, Vec<u8>, Vec<(String, Sql)>)>> {
            let mut statement = self.conn.prepare_cached(&sql).ok()?;
            // Names from the first row: a cached `SELECT *` prepared before
            // its table gained a column names the old columns until a step
            // re-prepares it.
            let mut names: Vec<String> = Vec::new();
            let rows = statement.query_map(params, |r| {
                if names.is_empty() {
                    let statement = r.as_ref();
                    names = (0..statement.column_count())
                        .map(|i| statement.column_name(i).map(str::to_owned))
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
                        out.push((name.clone(), r.get::<_, Sql>(i)?));
                    }
                }
                Ok((seq, branch, out))
            });
            rows.ok()?.collect::<Result<_, _>>().ok()
        };
        let id = row.0.as_bytes().to_vec();
        let current = read(
            format!("SELECT * FROM {} WHERE id = ?1", quote(table)),
            &[&id],
        )?;
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
            &[&id, &key, &seq],
        )?;
        Some(
            current
                .into_iter()
                .chain(old)
                .map(|(_, _, image)| image)
                .collect(),
        )
    }

    /// Apply a commit that changed only the root rows `ids` to a result in
    /// place: re-check those ids and splice them in by the query's order
    /// (sort keys are kept per row), emitting a delta for just those rows.
    /// O(changed · log n) comparisons plus memmoves, not a re-run and full
    /// diff. A first page (`limit`, no offset) keeps its first `limit`
    /// rows; one that was full and shrank re-runs, since it never held the
    /// rows after it. `None` when the order cannot be compared from the
    /// rows (then the caller re-runs).
    fn recheck_rows(
        &self,
        group: &mut Group,
        ids: &BTreeSet<RowUuid>,
    ) -> Option<Result<Option<SubscriptionEvent>, Error>> {
        let order = group.prepared.query.order_by.clone();
        let projected = group.projected;
        if group.keys.len() != group.rows.len() {
            if !projected {
                return None;
            }
            group.keys = match self.sort_keys(group, &order, projected, &group.rows)? {
                Ok(keys) => keys,
                Err(error) => return Some(Err(error)),
            };
        }
        // The same query narrowed to the changed ids, as a root filter, so
        // SQLite seeks them by primary key instead of sorting the view.
        let mut prepared = group.prepared.clone();
        prepared.query.filters.push(crate::query::Predicate::In(
            crate::query::Operand::Column("id".to_owned()),
            ids.iter()
                .map(|id| crate::query::Operand::Literal(Value::Uuid(id.0)))
                .collect(),
        ));
        prepared.query.order_by.clear();
        prepared.query.limit = None;
        // Under a cut, a row changed past it still reads as it was. The
        // other rows were read through the cut the last run applied: under
        // another, only a full run (which records it) keeps them one state.
        let read = self.visible_read(&group.read, &group.tables);
        if cut_of(&read) != group.applied {
            return None;
        }
        let narrowed = match self.compile(&prepared, group.identity, &read) {
            Ok(compiled) => compiled,
            Err(error) => return Some(Err(error)),
        };
        let fresh = match self.execute(&narrowed, &group.prepared.query.table) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        let fresh_keys = match self.sort_keys(group, &order, projected, &fresh)? {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
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
    fn sort_keys(
        &self,
        group: &Group,
        order: &[crate::query::OrderBy],
        projected: bool,
        rows: &[CurrentRow],
    ) -> Option<Result<Vec<SortKey>, Error>> {
        // Same tie-break as the compiled ORDER BY: id in the last key's
        // direction.
        let tie_desc = matches!(
            order.last().map(|by| by.direction),
            Some(crate::query::OrderDirection::Desc)
        );
        let id_key = |row: &CurrentRow| (Sql::Blob(row.id.0.as_bytes().to_vec()), tie_desc);
        let schema = group.prepared.schema.as_ref().unwrap_or(&self.owner);
        if projected {
            // Cells sort as the compiled ORDER BY sorts them: an enum
            // variant by its tag, or its rank in a schema whose declaration
            // order is not tag order.
            let root = schema.tables.get(&group.prepared.query.table)?;
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
            return Some(Ok(keys));
        }
        if rows.is_empty() {
            return Some(Ok(Vec::new()));
        }
        let table = schema.tables.get(&group.prepared.query.table)?;
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
        let ids = crate::compile::json_ids(rows.iter().map(|row| row.id.0.as_bytes().as_slice()));
        let read = || -> rusqlite::Result<HashMap<Vec<u8>, Vec<Sql>>> {
            let mut statement = self.conn.prepare_cached(&sql)?;
            let found = statement.query_map([&ids], |r| {
                let id: Vec<u8> = r.get(0)?;
                let values = (1..=order.len())
                    .map(|i| r.get::<_, Sql>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((id, values))
            })?;
            found.collect()
        };
        let mut found = match read() {
            Ok(found) => found,
            Err(error) => return Some(Err(storage(error))),
        };
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let values = found.remove(row.id.0.as_bytes().as_slice())?;
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
    pub(super) fn retry_awaiting(&self) {
        for handle in self.fan.groups.values() {
            let mut group = handle.borrow_mut();
            if !group.awaiting {
                continue;
            }
            if group.opened {
                self.refresh_group(&mut group, None);
            } else {
                self.reset_group(&mut group);
            }
        }
    }

    /// A group's rows, with array relations nested in their cells.
    fn group_rows(&self, group: &mut Group) -> Result<Vec<CurrentRow>, Error> {
        let read = self.visible_read(&group.read, &group.tables);
        group.applied = cut_of(&read);
        let rows = self.run(&group.prepared, group.identity, &read)?;
        self.nest_relations(&group.prepared, group.identity, &read, rows)
    }

    /// Re-run the live results whose cut moved: their read's settle rose,
    /// the connection's position did, or an own commit was accepted past
    /// them.
    pub(super) fn refresh_visibility(&mut self) -> Result<(), Error> {
        let groups: Vec<Rc<RefCell<Group>>> = self.fan.groups.values().cloned().collect();
        for group in groups {
            let mut group = group.borrow_mut();
            if group.read.position == Position::Current
                || cut_of(&self.visible_read(&group.read, &group.tables)) == group.applied
            {
                continue;
            }
            self.refresh_group(&mut group, None);
        }
        Ok(())
    }

    fn reset_group(&self, group: &mut Group) {
        match self.group_rows(group) {
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
                Self::hold(group);
                group.keys.clear();
                self.read_keys(group);
            }
            Err(error) => group.push(SubscriptionEvent::Rejected {
                code: error.code,
                message: error.message.clone(),
                reason: error.to_string(),
            }),
        }
    }

    /// Refuse splices of a column that is not a large-capable column of the
    /// query's table, or that it selects by JSON pointer.
    fn check_splices(&self, query: &Query, columns: &[String]) -> Result<(), Error> {
        if columns.is_empty() {
            return Ok(());
        }
        let table = &query.table;
        let t = self.schema.table(table)?;
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
    fn add_splices(
        &self,
        group: &Group,
        prior: &HashMap<RowUuid, CurrentRow>,
        event: &mut SubscriptionEvent,
    ) {
        use crate::query::Part;
        let SubscriptionEvent::Delta { updated, .. } = event else {
            return;
        };
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
            for column in &group.key.splices {
                let part = group
                    .prepared
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
                let policies = group
                    .prepared
                    .schema
                    .as_ref()
                    .map_or(&*self.owner, |schema| schema.policy_schema());
                if let Ok(splices) = self.value_delta(
                    &group.prepared.query.table,
                    out.row.id,
                    column,
                    (from, to),
                    group.identity,
                    policies,
                ) {
                    out.splices.push((column.clone(), splices));
                }
            }
        }
    }

    /// Bring a group's result up to date; true when it re-ran the query.
    fn refresh_group(&self, group: &mut Group, ids: Option<&BTreeSet<RowUuid>>) -> bool {
        // The versions the result showed, which splices start from.
        let prior: HashMap<RowUuid, CurrentRow> = if group.key.splices.is_empty() {
            HashMap::new()
        } else {
            group.rows.iter().map(|row| (row.id, row.clone())).collect()
        };
        if let Some(ids) = ids.filter(|_| group.recheck) {
            match self.recheck_rows(group, ids) {
                Some(Ok(Some(mut event))) => {
                    self.add_splices(group, &prior, &mut event);
                    group.push(event);
                    return false;
                }
                Some(Ok(None)) => return false,
                // Missing chunks: the full run below decides to wait.
                Some(Err(error)) if crate::large::is_missing(&error) => {}
                Some(Err(error)) => {
                    group.push(SubscriptionEvent::Rejected {
                        code: error.code,
                        message: error.message.clone(),
                        reason: error.to_string(),
                    });
                    return false;
                }
                None => {}
            }
        }
        let rows = match self.group_rows(group) {
            Ok(rows) => rows,
            Err(error) if crate::large::is_missing(&error) => {
                group.awaiting = true;
                return true;
            }
            Err(error) => {
                let event = SubscriptionEvent::Rejected {
                    code: error.code,
                    message: error.message.clone(),
                    reason: error.to_string(),
                };
                group.push(event);
                return true;
            }
        };
        let was_awaiting = std::mem::take(&mut group.awaiting);
        if let Some(mut event) = diff(&group.rows, &rows) {
            self.add_splices(group, &prior, &mut event);
            group.push(event);
        } else if was_awaiting {
            // Settlement withheld while waiting is published now.
            group.push(delta(false, Vec::new(), Vec::new(), Vec::new()));
        }
        // Order keys that are not shown may have moved even when no row
        // did: read them again with the rows.
        group.rows = rows;
        Self::hold(group);
        group.keys.clear();
        self.read_keys(group);
        true
    }

    /// Reads the sort keys of a re-checkable group whose order is not
    /// shown, so they describe its rows as read (not after a later write).
    fn read_keys(&self, group: &mut Group) {
        if !group.recheck || group.projected {
            return;
        }
        let order = group.prepared.query.order_by.clone();
        if let Some(Ok(keys)) = self.sort_keys(group, &order, false, &group.rows) {
            group.keys = keys;
        }
    }
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
    fn push(&mut self, event: SubscriptionEvent) {
        self.members.retain(|m| m.queue.strong_count() > 0);
        for member in &self.members {
            if let Some(queue) = member.queue.upgrade() {
                let mut event = event.clone();
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
                            table = %self.query().table,
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
}

/// Whether a group may apply root-only commits by re-checking ids: a
/// single-table query (not a union), unbounded or a first page, (no joins, arrays, required includes,
/// aggregates, windows), outside branch views and cuts (a settled-only
/// read is one), whose compiled SQL
/// reads its root table exactly once (a read policy that looks at other
/// rows of the same table would let one row's change move another).
fn recheckable(query: &Query, root: &str, read: &ReadSpec, compiled: &Compiled) -> bool {
    !is_history_table(&query.table)
        && query.relation.is_none()
        && query.offset == 0
        && query.aggregate.is_none()
        && query.joins.is_empty()
        && query.flat_join.is_none()
        && query.reachable.is_empty()
        && query.inherits.is_empty()
        && query.array_subqueries.is_empty()
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
    policy: &PolicyExpr,
    session: &Session,
    pins: &mut HashMap<String, Option<(String, String)>>,
) -> bool {
    use crate::tools::PolicyValue;
    match policy {
        PolicyExpr::And(list) | PolicyExpr::Or(list) => {
            list.iter().all(|p| policy_pins(p, session, pins))
        }
        // A pin bounds which rows can change an EXISTS's value, whatever
        // surrounds it, so negation keeps it sound.
        PolicyExpr::Not(inner) => policy_pins(inner, session, pins),
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
                            pin = session
                                .resolve(path)
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
    out: Evidence,
    /// The tables (and operations) whose policies an `INHERITS` is being
    /// read through: a chain back to one is watched whole.
    through: Vec<(String, Operation)>,
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
                    .find_map(|c| value_pin(target, c, self.session));
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
                self.inherit(target, *operation, BTreeSet::new())
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
                column(via_column, &mut columns) && self.inherit(source, *operation, columns)
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
            .find_map(|c| value_pin(table, c, self.session));
        self.occurrence(table, pin, columns);
        true
    }

    fn occurrence(&mut self, table: &Table, pin: Option<Pin>, columns: BTreeSet<String>) {
        let columns = columns
            .iter()
            .map(|c| table.stored_name(c).to_owned())
            .collect();
        add_evidence(
            &mut self.out,
            &table.stored,
            Some(Occurrence { pin, columns }),
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
fn value_pin(table: &Table, conjunct: &PolicyExpr, session: &Session) -> Option<Pin> {
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
    let value = match (ty, session.resolve(path)?) {
        (ValueType::Uuid, Value::String(text)) => Value::Uuid(text.parse().ok()?),
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
    let value = stored_canon(Some(stored), canon_value(&value)?);
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
                let value = stored_canon(table.column(&column), value);
                (table.stored_name(&column).to_owned(), value)
            });
            Some((table.stored.clone(), pin))
        })
        .collect()
}

/// `column = session value` required of the protected row itself: the
/// policy is that comparison, or a conjunction containing it.
fn row_pin(policy: &PolicyExpr, session: &Session) -> Option<(String, String)> {
    use crate::tools::PolicyValue;
    match policy {
        PolicyExpr::Cmp {
            column,
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(path),
        } if path.first().map(String::as_str) != Some("__jazz_outer_row") => session
            .resolve(path)
            .as_ref()
            .and_then(canon_value)
            .map(|value| (column.clone(), value)),
        PolicyExpr::And(list) => list.iter().find_map(|p| row_pin(p, session)),
        _ => None,
    }
}

/// Every array relation level under a parent of `parent` (whose rows
/// carry `columns`), depth first, each with whether it qualifies for a
/// correlation watch on its own (see [`Inner::correlations`]).
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

/// What `rows` hold at `level`.
fn held(level: &Correlation, rows: &[CurrentRow]) -> Held {
    let mut held = Held {
        complete: true,
        ..Held::default()
    };
    let Some((last, through)) = level.path.split_last() else {
        held.complete = false;
        return held;
    };
    let mut parents: Vec<Node> = rows.iter().map(Node::Row).collect();
    for column in through {
        let mut next = Vec::new();
        for parent in &parents {
            match parent.children(column) {
                Some(children) => next.extend(children),
                None => held.complete = false,
            }
        }
        parents = next;
    }
    for parent in &parents {
        match parent.value(&level.outer) {
            Some(value) => {
                if !value_atoms(&value, &mut held.parents) {
                    held.complete = false;
                }
            }
            None => held.complete = false,
        }
        match parent.children(last) {
            Some(children) => {
                for child in children {
                    match child.id() {
                        Some(id) => {
                            held.rows.insert(id);
                        }
                        None => held.complete = false,
                    }
                }
            }
            None => held.complete = false,
        }
    }
    held
}

/// Add a reference value's canonical stored forms (one per list element)
/// to `out`; false when one has none.
fn value_atoms(value: &Value, out: &mut HashSet<String>) -> bool {
    match value {
        Value::Nullable(None) => true,
        Value::Nullable(Some(inner)) => value_atoms(inner, out),
        Value::Array(items) => items.iter().all(|item| value_atoms(item, out)),
        value => match canon_value(value) {
            Some(atom) => {
                out.insert(atom);
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
        pinned && changed
    })
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
            if !held.complete || held.rows.contains(&row) {
                return true;
            }
            if level.inner == "id" {
                return held
                    .parents
                    .contains(&canon(&Sql::Blob(row.0.as_bytes().to_vec())));
            }
            let Some(images) = images else {
                return true;
            };
            images.iter().any(|image| {
                image
                    .iter()
                    .filter(|(column, _)| *column == level.inner)
                    .any(|(_, cell)| match stored_atoms(cell, level.inner_list) {
                        Some(atoms) => atoms.iter().any(|atom| held.parents.contains(atom)),
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

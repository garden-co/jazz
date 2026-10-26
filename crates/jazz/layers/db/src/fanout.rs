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

use super::*;
use crate::schema::history::{history_source_table, is_history_table};

pub(super) type Queue = Rc<RefCell<VecDeque<SubscriptionEvent>>>;

/// What a group reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Watch {
    Table(String),
    /// Rows of `table` whose `column` holds the canonical value.
    Key(String, String, String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupKey {
    /// The renamed tables and columns of the schema naming the rows:
    /// versions on either side of a rename compile to the same SQL but
    /// label rows differently.
    renames: String,
    sql: String,
    params: String,
    /// The SQL selects stored columns; output aliases and types also belong
    /// to a shared result's identity.
    projection: String,
    /// Set only when the SQL depends on the identity's session (array
    /// relations, read as the identity, always may).
    identity: Option<String>,
    /// The array relations nested in each row: queries that differ only
    /// in them compile to the same root SQL.
    relations: String,
}

struct Member {
    queue: Weak<RefCell<VecDeque<SubscriptionEvent>>>,
    shape: Shape,
    bound: bool,
    /// Where this member's `settled` flag comes from, and the last value
    /// it was told.
    settled: sync::SettledBy,
    told: std::cell::Cell<Option<DurabilityTier>>,
    accepted_evidence: bool,
    support_inputs: bool,
}

/// One shared live result.
pub(super) struct Group {
    key: GroupKey,
    prepared: PreparedQuery,
    /// The root table's stored name, which changed rows name.
    root: String,
    identity: Option<AuthorSubject>,
    read: ReadSpec,
    /// Pending-state tracking for ordinary UNION previews, independent of
    /// the occurrence/evidence coverage used by scoped UNIONs.
    preview_pending: Option<(BTreeSet<String>, Rc<std::cell::Cell<bool>>)>,
    pub(super) rows: Vec<CurrentRow>,
    members: Vec<Member>,
    watches: Vec<Watch>,
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
}

type SortKey = Vec<(Sql, bool)>;

impl Group {
    /// Remote query and permission support streams also need changed input
    /// witnesses when the visible row result is unchanged.
    pub(super) fn observe_support_inputs(&mut self, queue: &Queue) {
        let queue = Rc::downgrade(queue);
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.queue.ptr_eq(&queue))
        {
            member.support_inputs = true;
        }
    }

    fn notify_support_inputs(&self) {
        if self.awaiting || !self.opened || self.read.frozen() {
            return;
        }
        for member in self.members.iter().filter(|member| member.support_inputs) {
            if let Some(queue) = member.queue.upgrade() {
                let mut event = delta(false, Vec::new(), Vec::new(), Vec::new());
                if let SubscriptionEvent::Delta { settled, tier, .. } = &mut event {
                    *settled = member.settled.get();
                    *tier = member.settled.tier();
                }
                queue.borrow_mut().push_back(event);
            }
        }
    }

    pub(super) fn observe_accepted_evidence(&mut self, queue: &Queue) {
        let queue = Rc::downgrade(queue);
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.queue.ptr_eq(&queue))
        {
            member.accepted_evidence = true;
        }
    }

    /// Whether the last run waits on large-value chunks still to arrive.
    pub(super) fn awaiting(&self) -> bool {
        self.awaiting
    }

    /// The root table's stored name.
    pub(super) fn root(&self) -> &str {
        &self.root
    }

    /// The query this group answers.
    pub(super) fn query(&self) -> &Query {
        &self.prepared.query
    }

    pub(super) fn includes_deleted(&self) -> bool {
        self.read.include_deleted
    }

    pub(super) fn union_cut(&self) -> Option<u64> {
        self.read.union_live.as_ref().and_then(|live| live.cut())
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
}

#[derive(Default)]
pub(super) struct Fanout {
    next: u64,
    groups: HashMap<u64, Rc<RefCell<Group>>>,
    by_key: HashMap<GroupKey, u64>,
    tables: HashMap<String, BTreeSet<u64>>,
    keys: HashMap<(String, String, String), BTreeSet<u64>>,
    key_columns: HashMap<String, BTreeSet<String>>,
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

impl Fanout {
    fn index(&mut self, gid: u64, watches: &[Watch]) {
        for watch in watches {
            match watch {
                Watch::Table(table) => {
                    self.tables.entry(table.clone()).or_default().insert(gid);
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

    /// Tell members whose remote answered without changing their rows.
    pub(super) fn flush_settled(&mut self) {
        for group in self.groups.values() {
            let group = group.borrow();
            if group.awaiting {
                continue;
            }
            for member in &group.members {
                if member.settled.get()
                    && member.told.get() != Some(member.settled.tier())
                    && let Some(queue) = member.queue.upgrade()
                {
                    member.told.set(Some(member.settled.tier()));
                    let mut event = delta(false, Vec::new(), Vec::new(), Vec::new());
                    if let SubscriptionEvent::Delta { settled, tier, .. } = &mut event {
                        *settled = true;
                        *tier = member.settled.tier();
                    }
                    queue.borrow_mut().push_back(event);
                }
            }
        }
    }
}

impl Inner {
    /// Reuse keyed dependency routing for fresh authority evidence. It may
    /// qualify already-visible optimistic contents, so ordinary row diffing
    /// cannot signal it. Only explicitly opted-in members receive an empty delta.
    pub(super) fn notify_accepted_evidence(&self, changes: Vec<(String, RowUuid, Vec<u8>)>) {
        let mut woken = BTreeSet::new();
        for (table, row, key) in changes.into_iter().collect::<BTreeSet<_>>() {
            if let Some(groups) = self.fan.tables.get(&table) {
                woken.extend(groups);
            }
            let Some(columns) = self.fan.key_columns.get(&table) else {
                continue;
            };
            for image in self.images(&table, row, &key, columns) {
                for (column, value) in image {
                    if let Some(groups) = self.fan.keys.get(&(table.clone(), column, value)) {
                        woken.extend(groups);
                    }
                }
            }
        }
        for gid in woken {
            let Some(group) = self.fan.groups.get(gid) else {
                continue;
            };
            let group = group.borrow();
            if group.awaiting || !group.opened || group.read.frozen() {
                continue;
            }
            for member in group
                .members
                .iter()
                .filter(|member| member.accepted_evidence)
            {
                if let Some(queue) = member.queue.upgrade() {
                    let mut event = delta(false, Vec::new(), Vec::new(), Vec::new());
                    if let SubscriptionEvent::Delta { settled, tier, .. } = &mut event {
                        *settled = member.settled.get();
                        *tier = member.settled.tier();
                    }
                    queue.borrow_mut().push_back(event);
                }
            }
        }
    }

    /// Open a subscription: join the group with identical SQL, or start one.
    pub(super) fn subscribe_member(
        &mut self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: ReadSpec,
        settled: sync::SettledBy,
    ) -> Result<(Queue, Weak<RefCell<Group>>), Error> {
        let (compiled, session_used) = self.compile_tracked(prepared, identity, &read)?;
        if let Some(live) = &read.union_live {
            live.tables
                .borrow_mut()
                .extend(compiled.tables.iter().cloned());
        }
        let key = GroupKey {
            renames: renames(prepared.schema.as_ref().unwrap_or(&self.owner)),
            sql: compiled.sql.clone(),
            params: format!("{:?}", compiled.params),
            projection: format!("{:?}", compiled.projection),
            identity: (session_used || !prepared.query.array_subqueries.is_empty())
                .then(|| format!("{identity:?}")),
            relations: format!(
                "{:?}{:?}{:?}",
                prepared.query.array_subqueries,
                read.scoped_live.then_some(&prepared.request_claims),
                read.union_live
                    .as_ref()
                    .map(|live| (Rc::as_ptr(live) as usize, read.union_preview))
            ),
        };
        let existing = self
            .fan
            .by_key
            .get(&key)
            .and_then(|gid| self.fan.groups.get(gid))
            .cloned();
        let preview_pending = if compiled.union
            && read.union_preview
            && !read.settled_only
            && read.union_live.is_none()
            && self.client.is_some()
        {
            let pending = existing
                .as_ref()
                .and_then(|group| group.borrow().preview_pending.clone())
                .unwrap_or_else(|| {
                    (
                        compiled.tables.clone(),
                        Rc::new(std::cell::Cell::new(false)),
                    )
                });
            pending.1.set(self.union_has_pending_inputs(&pending.0)?);
            Some(pending)
        } else {
            None
        };
        let settled = match &preview_pending {
            Some((_, pending)) => sync::SettledBy::Preview {
                upstream: Box::new(settled),
                pending: pending.clone(),
            },
            None => settled,
        };
        let now = settled.get();
        let member = |queue: &Queue| Member {
            queue: Rc::downgrade(queue),
            shape: prepared.shape,
            bound: !prepared.binding.is_empty(),
            settled: settled.clone(),
            told: std::cell::Cell::new(now.then(|| settled.tier())),
            accepted_evidence: false,
            support_inputs: false,
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
        if let Some(handle) = existing {
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
        // A replica missing chunks the result needs opens it once they arrive.
        let initial = if read.union_live.is_some() {
            self.run(prepared, identity, &read)
        } else {
            self.execute(&compiled, &prepared.query.table)
        };
        let (rows, awaiting) =
            match initial.and_then(|rows| self.nest_relations(prepared, identity, &read, rows)) {
                Ok(rows) => (rows, false),
                Err(error) if crate::large::is_missing(&error) || sync::union_waiting(&error) => {
                    (Vec::new(), true)
                }
                Err(error) => return Err(error),
            };
        let watches = self.watches(prepared, identity, &read, &compiled)?;
        let root = prepared
            .schema
            .as_ref()
            .unwrap_or(&self.owner)
            .stored_table(
                history_source_table(&prepared.query.table).unwrap_or(&prepared.query.table),
            )?
            .to_owned();
        let recheck = recheckable(&prepared.query, &root, &read, &compiled);
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
            preview_pending,
            rows,
            members: vec![member(&queue)],
            watches,
            recheck,
            projected,
            keys: Vec::new(),
            includes,
            awaiting,
            opened: !awaiting,
        }));
        self.read_keys(&mut handle.borrow_mut());
        let weak = Rc::downgrade(&handle);
        if let Some(live) = &handle.borrow().read.union_live {
            live.groups.borrow_mut().push(weak.clone());
        }
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
                && compiled.projection.iter().any(|(name, _)| name == column)
            {
                out.push((target.to_owned(), column.to_owned()));
            }
        }
        Ok(out)
    }

    fn compile_tracked(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<(Compiled, bool), Error> {
        let (policy, claims) = self.read_sessions(prepared, identity);
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        self.require_catalogue_schema(&schema)?;
        let compiled = Ctx::new(&schema, policy.as_ref(), &claims, &prepared.binding)
            .with_owner_previews(
                self.owner_previews(identity)
                    && history_source_table(&prepared.query.table).is_none(),
            )
            .with_catalogue(self)
            .with_view(read.view.clone())
            .with_cut(read.cut.clone())
            .with_settled_only(read.settled_only)
            .with_history_origin(self.history_origin())
            .with_members_only(read.members_only)
            .compile_query(&prepared.query, read.include_deleted)?;
        let used = claims.used.get() || policy.as_ref().is_some_and(|p| p.used.get());
        Ok((compiled, used))
    }

    /// The watches of one subscription; see the module docs for soundness.
    fn watches(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        compiled: &Compiled,
    ) -> Result<Vec<Watch>, Error> {
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
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
        // Tables the query itself reaches (joins, arrays, includes,
        // reachability): these always watch the whole table.
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
        if crate::db::relation::is_union(query) {
            // Each arm can reach these tables independently of the root policy.
            structural.extend(compiled.tables.iter().cloned());
        }
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
        arrays(&mut structural, &query.array_subqueries, &stored)?;
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
        // Policy evidence pinned by a session equality.
        let mut pinned: HashMap<String, Option<(String, String)>> = HashMap::new();
        let session = identity.and_then(|author| self.read_sessions(prepared, Some(author)).0);
        if let Some(session) = session.filter(|s| !s.system) {
            match root.policy(Operation::Select) {
                Some(policy) if policy_pins(policy, &session, &mut pinned) => {}
                _ => pinned.clear(),
            }
        }
        // Policies name tables and columns as the schema they come from.
        let policies = schema.policy_schema();
        let mut pinned = stored_pins(policies, pinned);
        if pinned.contains_key(&root.stored) {
            pinned.clear();
        }
        // A read policy that requires `column = session value` of the row
        // itself pins the root too: other rows cannot enter the result.
        let policy_root_key = identity
            .and_then(|author| self.read_sessions(prepared, Some(author)).0)
            .filter(|s| !s.system)
            .and_then(|session| {
                root.policy(Operation::Select)
                    .and_then(|p| row_pin(p, &session))
            })
            .and_then(|(column, value)| {
                let governing = policies.by_stored(&root.stored)?;
                // As for a query key: only an application column is in a
                // changed row's image (`$createdBy.account` is not).
                governing
                    .columns
                    .iter()
                    .any(|c| c.name == column)
                    .then(|| (governing.stored_name(&column).to_owned(), value))
            });
        let mut watches = Vec::new();
        let root_key = if structural.contains(&root.stored) {
            None
        } else {
            root_key(query, &prepared.binding)
                // Only an application column is in a changed row's image;
                // a key on anything else (`$createdBy.identity.subject`,
                // `id`) would never be hit.
                .filter(|(column, _)| root.columns.iter().any(|c| c.name == *column))
                .map(|(column, value)| (root.stored_name(&column).to_owned(), value))
                .or(policy_root_key)
        }
        .filter(|(column, _)| keyable(&root.stored, column));
        watches.push(match root_key {
            Some((column, value)) => Watch::Key(root.stored.clone(), column, value),
            None => Watch::Table(root.stored.clone()),
        });
        let mut reads = compiled.tables.clone();
        reads.extend(structural.iter().cloned());
        for table in reads {
            if table == root.stored {
                continue;
            }
            watches.push(match pinned.get(&table) {
                Some(Some((column, value)))
                    if !structural.contains(&table) && keyable(&table, column) =>
                {
                    Watch::Key(table.clone(), column.clone(), value.clone())
                }
                _ => Watch::Table(table.clone()),
            });
        }
        watches.sort();
        watches.dedup();
        Ok(watches)
    }

    /// Wake the groups a commit's changed rows hit, re-run and diff them.
    pub(super) fn dispatch(&mut self, mut changes: Vec<(String, RowUuid, Vec<u8>)>) {
        changes.sort();
        changes.dedup();
        // Per woken group: the root rows that woke it, or `None` once any
        // other table's row did (then it re-runs).
        let mut woken: BTreeMap<u64, Option<BTreeSet<RowUuid>>> = BTreeMap::new();
        // Per woken group: include targets that changed, as (root column, id).
        let mut targets: BTreeMap<u64, BTreeSet<(String, RowUuid)>> = BTreeMap::new();
        let groups = &self.fan.groups;
        let mut wake = |gids: &BTreeSet<u64>, table: &str, row: RowUuid| {
            for gid in gids {
                let Some(group) = groups.get(gid) else {
                    continue;
                };
                let group = group.borrow();
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
                wake(groups, table, *row);
            }
            let Some(columns) = self.fan.key_columns.get(table) else {
                continue;
            };
            for image in self.images(table, *row, key, columns) {
                for (column, value) in image {
                    if let Some(groups) = self.fan.keys.get(&(table.clone(), column, value)) {
                        wake(groups, table, *row);
                    }
                }
            }
        }
        for image in std::mem::take(&mut *self.gone.borrow_mut()) {
            let row = RowUuid::from_bytes(image.id);
            if let Some(groups) = self.fan.tables.get(&image.table) {
                wake(groups, &image.table, row);
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
                    wake(groups, &image.table, row);
                }
            }
        }
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
            group.notify_support_inputs();
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
            // A schema the current one governs (a sync session's) takes the
            // policies of whichever schema is current now.
            if reset
                && let Some(pinned) = &group.prepared.schema
                && pinned.policies_from.is_some()
            {
                group.prepared.schema = Some(self.effective_schema(pinned));
            }
            let old = std::mem::take(&mut group.watches);
            self.fan.unindex(gid, &old);
            let watches = self
                .compile(&group.prepared, group.identity, &group.read)
                .and_then(|compiled| {
                    self.watches(&group.prepared, group.identity, &group.read, &compiled)
                })
                .unwrap_or(old);
            self.fan.index(gid, &watches);
            group.watches = watches;
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
        let read = |sql: String,
                    params: &[&dyn rusqlite::ToSql]|
         -> Vec<(i64, Vec<u8>, Vec<(String, String)>)> {
            let Ok(mut statement) = self.conn.prepare_cached(&sql) else {
                return Vec::new();
            };
            let names: Vec<String> = statement
                .column_names()
                .iter()
                .map(|s| (*s).to_owned())
                .collect();
            let rows = statement.query_map(params, |r| {
                let mut seq = 0;
                let mut branch = Vec::new();
                let mut out = Vec::new();
                for (i, name) in names.iter().enumerate() {
                    if name == "_seq" {
                        seq = r.get(i)?;
                    } else if name == "_b" {
                        branch = r.get(i)?;
                    } else if columns.contains(name) {
                        out.push((name.clone(), canon(&r.get::<_, Sql>(i)?)));
                    }
                }
                Ok((seq, branch, out))
            });
            rows.map(|rows| rows.filter_map(Result::ok).collect())
                .unwrap_or_default()
        };
        let id = row.0.as_bytes().to_vec();
        let current = read(
            format!("SELECT * FROM {} WHERE id = ?1", quote(table)),
            &[&id],
        );
        let seq = current
            .iter()
            .find(|(_, branch, _)| branch.as_slice() == key)
            .map_or(i64::MAX, |(seq, _, _)| *seq);
        let old = read(
            format!(
                "SELECT * FROM {} WHERE id = ?1 AND _b = ?2 AND _seq < ?3 ORDER BY _seq DESC LIMIT 1",
                quote(&format!("{table}__h"))
            ),
            &[&id, &key, &seq],
        );
        current
            .into_iter()
            .chain(old)
            .map(|(_, _, image)| image)
            .collect()
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
        let narrowed = match self.compile(&prepared, group.identity, &group.read) {
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
        if projected {
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
                            Some(value) => match self.logical_key(to_sql(value).ok()??) {
                                Ok(value) => value,
                                Err(error) => return Some(Err(error)),
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
        let schema = group.prepared.schema.as_ref().unwrap_or(&self.owner);
        let table = schema.tables.get(&group.prepared.query.table)?;
        // By id alone a branched table's row is ambiguous.
        if !table.branch_by.is_empty() {
            return None;
        }
        let mut columns = String::new();
        for by in order {
            match crate::compile::column_sql(table, "t", &by.column) {
                Ok(sql) => columns.push_str(&format!(", {sql}")),
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
        // Reads on past missing chunks, so one round trip asks for them all.
        let mut blocked = false;
        for row in rows {
            let values = found.remove(row.id.0.as_bytes().as_slice())?;
            let mut key: SortKey = Vec::with_capacity(order.len() + 1);
            for (value, by) in values.into_iter().zip(order) {
                let value = match self.logical_key(value) {
                    Ok(value) => value,
                    Err(error) if crate::large::is_missing(&error) => {
                        blocked = true;
                        Sql::Null
                    }
                    Err(error) => return Some(Err(error)),
                };
                key.push((
                    value,
                    matches!(by.direction, crate::query::OrderDirection::Desc),
                ));
            }
            key.push(id_key(row));
            keys.push(key);
        }
        if blocked {
            return Some(Err(crate::large::missing()));
        }
        Some(Ok(keys))
    }

    /// A sort-key cell as its logical bytes, so keys compare as the `jazz`
    /// collation orders them: a chunked or tagged cell is read as its value.
    fn logical_key(&self, value: Sql) -> Result<Sql, Error> {
        Ok(match value {
            Sql::Text(cell) => Sql::Text(self.chunks.materialize(&cell)?),
            value => value,
        })
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
    fn group_rows(&self, group: &Group) -> Result<Vec<CurrentRow>, Error> {
        if let Some((tables, pending)) = &group.preview_pending {
            pending.set(self.union_has_pending_inputs(tables)?);
        }
        let rows = self.run(&group.prepared, group.identity, &group.read)?;
        self.nest_relations(&group.prepared, group.identity, &group.read, rows)
    }

    fn reset_group(&self, group: &mut Group) {
        match self.group_rows(group) {
            Err(error) if crate::large::is_missing(&error) || sync::union_waiting(&error) => {
                group.awaiting = true
            }
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
                group.keys.clear();
                self.read_keys(group);
            }
            Err(error) => group.push(SubscriptionEvent::Rejected {
                reason: error.to_string(),
            }),
        }
    }

    /// Bring a group's result up to date; true when it re-ran the query.
    fn refresh_group(&self, group: &mut Group, ids: Option<&BTreeSet<RowUuid>>) -> bool {
        if let Some(ids) = ids.filter(|_| group.recheck) {
            match self.recheck_rows(group, ids) {
                Some(Ok(Some(event))) => {
                    group.push(event);
                    return false;
                }
                Some(Ok(None)) => return false,
                // Missing chunks: the full run below decides to wait.
                Some(Err(error)) if crate::large::is_missing(&error) => {}
                Some(Err(error)) => {
                    group.push(SubscriptionEvent::Rejected {
                        reason: error.to_string(),
                    });
                    return false;
                }
                None => {}
            }
        }
        let rows = match self.group_rows(group) {
            Ok(rows) => rows,
            Err(error) if crate::large::is_missing(&error) || sync::union_waiting(&error) => {
                group.awaiting = true;
                return true;
            }
            Err(error) => {
                let event = SubscriptionEvent::Rejected {
                    reason: error.to_string(),
                };
                group.push(event);
                return true;
            }
        };
        let was_awaiting = std::mem::take(&mut group.awaiting);
        if let Some(event) = diff(&group.rows, &rows) {
            group.push(event);
        } else if was_awaiting {
            // Settlement withheld while waiting is published now.
            group.push(delta(false, Vec::new(), Vec::new(), Vec::new()));
        }
        // Order keys that are not shown may have moved even when no row
        // did: read them again with the rows.
        group.rows = rows;
        group.keys.clear();
        self.read_keys(group);
        true
    }

    /// An upload recorded after row notifications changes preview settlement.
    pub(super) fn refresh_union_pending(&self, tables: &BTreeSet<String>) {
        // UNION watches include every physical input table. The existing
        // table index keeps a new upload from visiting unrelated groups.
        let groups = tables
            .iter()
            .filter_map(|table| self.fan.tables.get(table))
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        for id in groups {
            let Some(group) = self.fan.groups.get(&id) else {
                continue;
            };
            let mut group = group.borrow_mut();
            if group.read.union_live.is_some() || group.preview_pending.is_some() {
                self.refresh_group(&mut group, None);
                // Standalone writes can enqueue their row delta before the
                // upload journal records pending status. Correct those queued
                // previews before the caller can observe the completed write.
                for member in &group.members {
                    if let Some(queue) = member.queue.upgrade() {
                        for event in queue.borrow_mut().iter_mut() {
                            if let SubscriptionEvent::Delta { settled, tier, .. } = event {
                                *settled = member.settled.get();
                                *tier = member.settled.tier();
                                member.told.set(settled.then_some(*tier));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Coverage can change without row writes, including an empty answer.
    /// Receipt-only changes wake just the attached groups.
    pub(super) fn refresh_union_coverage(&self, slots: &[Rc<sync::UnionLive>]) {
        for live in slots {
            live.groups
                .borrow_mut()
                .retain(|group| group.strong_count() > 0);
            let groups = live
                .groups
                .borrow()
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            for group in groups {
                let mut group = group.borrow_mut();
                if group.opened {
                    self.refresh_group(&mut group, None);
                } else {
                    self.reset_group(&mut group);
                }
            }
        }
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
        row.cells.iter().any(|(name, value)| match value {
            Some(Value::Uuid(id)) => changed.contains(&(name.clone(), RowUuid(*id))),
            _ => false,
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
                if let SubscriptionEvent::Delta { settled, tier, .. } = &mut event {
                    *settled = member.settled.get();
                    *tier = member.settled.tier();
                    member.told.set(settled.then_some(*tier));
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
/// rows of the same table would let one row's change move another). A
/// synced read (`members_only`) qualifies: whether the server lists a row
/// is a fact about that row alone, and listing or unlisting it reports the
/// row as changed.
fn recheckable(query: &Query, root: &str, read: &ReadSpec, compiled: &Compiled) -> bool {
    !crate::db::relation::is_union(query)
        && !is_history_table(&query.table)
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

/// Whether every order column of a query is among its shown columns.
fn order_projected(query: &Query, compiled: &Compiled) -> bool {
    // A shown alias may have the name of a different source sort column.
    // Read physical sort keys through the existing bounded lookup instead.
    if query.relation.is_some() {
        return query.order_by.iter().all(|key| key.column == "id");
    }
    query.order_by.iter().all(|key| {
        key.column == "id"
            || compiled
                .projection
                .iter()
                .any(|(name, _)| *name == key.column)
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
        })
        .collect();
    delta(true, added, Vec::new(), Vec::new())
}

/// What identifies a result row across runs: its id, plus its seq for a
/// history read, whose versions share their row's id, a flat join's
/// joined rows, and a union read's arm (the occurrence).
type RowKey = (
    RowUuid,
    u64,
    Arc<[crate::values::JoinedRow]>,
    Option<Arc<str>>,
);

fn row_key(row: &CurrentRow) -> RowKey {
    let version = if is_history_table(&row.table) {
        row.seq
    } else {
        0
    };
    (row.id, version, row.joined.clone(), row.union_arm.clone())
}

/// The keyed diff of two results, or `None` when nothing changed.
/// A row with the same cells at a newer version (a same-value write: its
/// metadata moved). A pending local version settling as the server's is
/// the same write, not a new one.
fn new_version(prior: &CurrentRow, row: &CurrentRow) -> bool {
    prior.seq != row.seq && !(prior.seq >= crate::db::LOCAL_BASE && row.seq < crate::db::LOCAL_BASE)
}

pub(super) fn diff(before: &[CurrentRow], after: &[CurrentRow]) -> Option<SubscriptionEvent> {
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
            let pin = pin.map(|(column, value)| (table.stored_name(&column).to_owned(), value));
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

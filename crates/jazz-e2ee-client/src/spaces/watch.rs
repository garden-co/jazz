//! What this device watches to acquire keys without waiting for a read
//! (protocol §4.2, §3.8; core hooks note H6): the deliveries addressed to
//! its account, its own member rows, and per space it holds keys of, or
//! has a report waiting for, the space's member rows, roots and
//! successors.
//!
//! Each watch opens before the decision it guards: a space joins the
//! watch, and only then is acquired, so a delivery that syncs in between
//! is seen. The shared watch also tracks member accounts and current
//! version deliveries.

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{Db, ReadOpts, ReadTier};
use jazz::e2ee::SpaceAddress as DbSpaceAddress;
use jazz::query::{Operand, Predicate, Query, col, eq, lit};
use jazz_e2ee::protocol::records::space::SpaceAddress;

use super::{SpaceKeyState, space_query};
use crate::account::rows::{self, account_id_cell, row_account_id, uuid_cell};
use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::state::{MEMBER_CHANGE_WAIT, WatchedRows, db_address, row_space, unchecked_address};

/// Live subscriptions to the rows that bring this device its keys: always
/// its own member rows and the deliveries addressed to it, and, for the
/// spaces in use, one subscription per system table over all of them.
pub struct SpaceWatch {
    own: Vec<(&'static str, WatchedRows<SpaceAddress>)>,
    /// The spaces the shared subscriptions cover now.
    watched: BTreeSet<SpaceAddress>,
    /// Spaces whose subscriptions or pending reports already had an acquisition pass.
    interest: BTreeSet<SpaceAddress>,
    /// Spaces whose latest watch-triggered check did not complete a delivery pass.
    unserved: BTreeSet<SpaceAddress>,
    /// The current versions whose deliveries the shared watch covers.
    epochs: BTreeSet<String>,
    /// How many open app subscriptions use each space.
    uses: std::collections::BTreeMap<SpaceAddress, usize>,
}

impl SpaceWatch {
    pub(super) fn covers_delivery(&self, space: &SpaceAddress) -> bool {
        self.interest.contains(space) && !self.unserved.contains(space)
    }

    /// An app subscription on an encrypted table opened and its rows are of
    /// `space`: the space joins the watched set (counted). The runtime
    /// cannot see subscriptions open and close, so the binding that
    /// opens them calls this and [`SpaceWatch::release_space`].
    pub fn use_space(&mut self, space: &DbSpaceAddress) {
        *self.uses.entry(unchecked_address(space)).or_default() += 1;
    }

    /// The last app subscription using `space` closed: it leaves the set.
    pub fn release_space(&mut self, space: &DbSpaceAddress) {
        let space = unchecked_address(space);
        if let Some(count) = self.uses.get_mut(&space) {
            *count -= 1;
            if *count == 0 {
                self.uses.remove(&space);
            }
        }
    }
}

/// The queries of the shared watch, one per system table, over `spaces`
/// and, for deliveries, their current `versions`. Both columns of the
/// address are filtered: the store serves `scopeId IN (…) AND identifier IN
/// (…)` from its `(scopeId, identifier, …)` indexes, and scans the table
/// for `identifier IN (…)` alone. Public only so that a test can ask the
/// store for the plan of what the runtime really subscribes to.
#[doc(hidden)]
pub fn watch_queries(spaces: &[DbSpaceAddress], versions: &[String]) -> Vec<(&'static str, Query)> {
    let spaces: Vec<_> = spaces.iter().map(unchecked_address).collect();
    space_queries(&spaces, versions)
}

fn space_queries(spaces: &[SpaceAddress], versions: &[String]) -> Vec<(&'static str, Query)> {
    if spaces.is_empty() {
        return Vec::new();
    }
    let operands = |ids: Vec<jazz::records::Value>| -> Vec<Operand> {
        ids.into_iter().map(Operand::Literal).collect()
    };
    let scopes: BTreeSet<&String> = spaces.iter().map(|space| &space.scope_id).collect();
    let scopes = operands(scopes.into_iter().filter_map(|id| uuid_cell(id)).collect());
    let identifiers = operands(
        spaces
            .iter()
            .filter_map(|space| uuid_cell(&space.identifier))
            .collect(),
    );
    let versions = operands(versions.iter().filter_map(|id| uuid_cell(id)).collect());
    let of_spaces = |table: &str| {
        Query::from(table)
            .filter(Predicate::In(
                Operand::Column("scopeId".to_owned()),
                scopes.clone(),
            ))
            .filter(Predicate::In(
                Operand::Column("identifier".to_owned()),
                identifiers.clone(),
            ))
    };
    let mut queries = vec![
        (rows::SPACE_ROOTS, of_spaces(rows::SPACE_ROOTS)),
        (rows::SPACE_SUCCESSORS, of_spaces(rows::SPACE_SUCCESSORS)),
        (rows::SPACE_MEMBERS, of_spaces(rows::SPACE_MEMBERS)),
    ];
    // Deliveries of the current versions only (protocol §3.8).
    if !versions.is_empty() {
        queries.push((
            rows::SPACE_DELIVERIES,
            of_spaces(rows::SPACE_DELIVERIES).filter(Predicate::In(
                Operand::Column("keyEpochId".to_owned()),
                versions,
            )),
        ));
    }
    queries
}

const WATCHED_ACCOUNT_TABLES: [&str; 3] = [
    rows::ACCOUNT_ROOTS,
    rows::PUBLIC_ACCOUNT_SUCCESSORS,
    rows::ACCOUNT_KEYS,
];

/// The queries of the account watch, one per table, over `accounts`:
/// `accountId IN (…)`, which the tables' `accountId` indexes serve. Public
/// only so that a test can ask the store for their plan.
#[doc(hidden)]
pub fn account_watch_queries(accounts: &[String]) -> Vec<(&'static str, Query)> {
    if accounts.is_empty() {
        return Vec::new();
    }
    WATCHED_ACCOUNT_TABLES
        .into_iter()
        .map(|table| (table, account_query(table, accounts)))
        .collect()
}

fn account_query(table: &str, accounts: &[String]) -> Query {
    let ids: Vec<Operand> = accounts
        .iter()
        .filter_map(|id| uuid_cell(id))
        .map(Operand::Literal)
        .collect();
    Query::from(table).filter(Predicate::In(Operand::Column("accountId".to_owned()), ids))
}

async fn local_members(
    db: &Db,
    space: &SpaceAddress,
) -> Result<Vec<jazz::db::CurrentRow>, ClientError> {
    let query = space_query(rows::SPACE_MEMBERS, space)?;
    Ok(db
        .all(
            &db.prepare_query(&query)?,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
        )
        .await?)
}

impl E2eeClient {
    /// The current version of each of `spaces` as this device last read its
    /// chain: the deliveries of earlier versions are not watched, the newest
    /// key opens them (protocol §3.8).
    fn current_epochs(&self, spaces: &BTreeSet<SpaceAddress>) -> BTreeSet<String> {
        let keys = self.shared.keys.borrow();
        spaces
            .iter()
            .filter_map(|space| keys.held.known(space)?.epochs().last().cloned())
            .collect()
    }

    /// Updates the shared subscriptions when their space set changes, or
    /// only the delivery subscription when the current versions change.
    /// New streams open before the ones they replace close.
    pub(super) async fn refresh_shared(
        &self,
        db: &Db,
        watch: &mut SpaceWatch,
        interest: &BTreeSet<SpaceAddress>,
    ) -> Result<(), ClientError> {
        let mut covered = self.shared.recent_writes();
        covered.extend(interest.iter().cloned());
        let epochs = self.current_epochs(&covered);
        let replace_all =
            covered != watch.watched || self.shared.member_watch.borrow().covered != covered;
        if !replace_all && epochs == watch.epochs {
            self.refresh_accounts(db, watch).await?;
            return Ok(());
        }
        let spaces: Vec<SpaceAddress> = covered.iter().cloned().collect();
        let versions: Vec<String> = epochs.iter().cloned().collect();
        let mut streams = Vec::new();
        for (table, query) in space_queries(&spaces, &versions) {
            if !replace_all && table != rows::SPACE_DELIVERIES {
                continue;
            }
            streams.push((
                table,
                WatchedRows::new(
                    db.subscribe(&db.prepare_query(&query)?, Default::default())
                        .await?,
                ),
            ));
        }
        self.shared.notice_member_changes();
        {
            let mut members = self.shared.member_watch.borrow_mut();
            // Compare the new streams' openings to the last observed rows.
            // A row at the same position is not a change in the ledger.
            for (table, stream) in &mut streams {
                if let Some((_, previous)) =
                    members.space_streams.iter_mut().find(|(t, _)| t == table)
                {
                    stream.rows = std::mem::take(&mut previous.rows);
                }
            }
            if replace_all {
                for deps in members.spaces.values_mut() {
                    deps.version = None;
                }
                members.space_streams.clear();
                members.covered = covered.clone();
            } else {
                // A retained stream may have closed while the new stream
                // opened. Leave its coverage invalidated for the next refresh.
                members
                    .space_streams
                    .retain(|(table, _)| *table != rows::SPACE_DELIVERIES);
            }
            members.space_streams.extend(streams);
        }
        watch.watched = covered;
        watch.epochs = epochs;
        self.refresh_accounts(db, watch).await?;
        Ok(())
    }

    /// Opens the shared subscriptions of the member accounts of the watched
    /// spaces (roots, public successors, sealing keys: what who lacks a key
    /// and the needs-rotation mark depend on, protocol §3.8), one per table
    /// over all the accounts, replaced with the new ones opened first when
    /// the set changes. The members come from this device's store, which the
    /// space watches keep current.
    async fn refresh_accounts(&self, db: &Db, watch: &mut SpaceWatch) -> Result<(), ClientError> {
        let mut active = self.shared.recent_writes();
        self.shared.notice_member_changes();
        let mut by_account = BTreeMap::<_, BTreeSet<_>>::new();
        {
            let mut members = self.shared.member_watch.borrow_mut();
            members.subscribed = watch.uses.keys().cloned().collect();
            active.extend(members.subscribed.iter().cloned());
            let mut keys = self.shared.keys.borrow_mut();
            members.spaces.retain(|space, _| {
                let keep = active.contains(space);
                if !keep {
                    keys.rotation_evaluated.remove(space);
                    keys.space_removals.remove(space);
                }
                keep
            });
            for space in active {
                let deps = members.spaces.entry(space.clone()).or_default();
                for (table, accounts) in WATCHED_ACCOUNT_TABLES.into_iter().zip([
                    &deps.members,
                    &deps.holders,
                    &deps.waiting,
                ]) {
                    for account in accounts {
                        by_account
                            .entry((table, account.clone()))
                            .or_default()
                            .insert(space.clone());
                    }
                }
            }
            members.by_account = by_account.clone();
        }
        for table in WATCHED_ACCOUNT_TABLES {
            let accounts: BTreeSet<_> = by_account
                .keys()
                .filter(|(t, _)| *t == table)
                .map(|(_, account)| account.clone())
                .collect();
            if accounts.is_empty() {
                self.shared.member_watch.borrow_mut().streams.remove(table);
                continue;
            }
            if self
                .shared
                .member_watch
                .borrow()
                .streams
                .get(table)
                .is_some_and(|stream| stream.accounts == accounts)
            {
                continue;
            }
            let listed = accounts.iter().cloned().collect::<Vec<_>>();
            let query = db.prepare_query(&account_query(table, &listed))?;
            let stream = db.subscribe(&query, Default::default()).await?;
            self.shared.notice_member_changes();
            let mut members = self.shared.member_watch.borrow_mut();
            let mut rows = WatchedRows::new(stream);
            if let Some(previous) = members.streams.remove(table) {
                rows.rows = previous.rows.rows;
            }
            members
                .streams
                .insert(table, crate::state::AccountStream { accounts, rows });
        }
        Ok(())
    }

    /// Opens the watches of this account's own rows: its member rows and
    /// the deliveries addressed to it. Spaces join as
    /// [`E2eeClient::sync_spaces`] finds them.
    pub async fn watch_spaces(&self, db: &Db) -> Result<SpaceWatch, ClientError> {
        let account = account_id_cell(&self.shared.account_id)?;
        let mut own = Vec::new();
        for (table, column) in [
            (rows::SPACE_MEMBERS, "accountId"),
            (rows::SPACE_DELIVERIES, "recipientAccountId"),
        ] {
            let query = Query::from(table).filter(eq(col(column), lit(account.clone())));
            own.push((
                table,
                WatchedRows::new(
                    db.subscribe(&db.prepare_query(&query)?, Default::default())
                        .await?,
                ),
            ));
        }
        Ok(SpaceWatch {
            own,
            watched: BTreeSet::new(),
            interest: BTreeSet::new(),
            unserved: BTreeSet::new(),
            epochs: BTreeSet::new(),
            uses: Default::default(),
        })
    }

    /// The spaces of the member rows this client's writes staged that have
    /// committed since (a row the local store shows), taken off the queue. A
    /// row still missing after the wait is a write that was rejected or
    /// abandoned: it is dropped too, so it costs no query on later turns.
    async fn committed_member_changes(
        &self,
        db: &Db,
    ) -> Result<BTreeSet<SpaceAddress>, ClientError> {
        let pending: Vec<_> = self
            .shared
            .member_changes
            .borrow()
            .iter()
            .cloned()
            .collect();
        let mut ready = BTreeSet::new();
        let mut done = Vec::new();
        for (row, space, staged) in pending {
            if ready.contains(&space) {
                done.push(row);
                continue;
            }
            let Ok(address) = SpaceAddress::new(&space.scope_id, &space.identifier) else {
                done.push(row);
                continue;
            };
            let read = local_members(db, &address).await?;
            if read.iter().any(|found| found.row_uuid() == row) {
                ready.insert(space);
                done.push(row);
            } else if self.shared.now().duration_since(staged) >= MEMBER_CHANGE_WAIT {
                done.push(row);
            }
        }
        self.shared
            .member_changes
            .borrow_mut()
            .retain(|(row, _, _)| !done.contains(row));
        Ok(ready)
    }

    /// Serves the member removals this client wrote (protocol §3.4, §3.5):
    /// finds each deleted row's space in the local store and runs the pass
    /// over it, which sets the needs-rotation mark when the removed account
    /// held the current version, so the next write rotates first. A removal
    /// is taken off the queue once its space's pass ran, or when the wait
    /// passed without the row or a pass that succeeded. Offline nothing is
    /// served: the queue waits for the client to be online. The spaces
    /// served.
    pub(crate) async fn serve_removals(
        &self,
        db: &Db,
    ) -> Result<BTreeSet<SpaceAddress>, ClientError> {
        let pending: Vec<_> = self
            .shared
            .member_removals
            .borrow()
            .iter()
            .cloned()
            .collect();
        let mut served = BTreeSet::new();
        if pending.is_empty() || !self.shared.keys.borrow().online {
            return Ok(served);
        }
        let mut opts = ReadOpts::for_read_tier(ReadTier::LocalOnly);
        opts.read_view.include_deleted = true;
        let mut done = Vec::new();
        for (row, staged) in pending {
            let query = Query::from(rows::SPACE_MEMBERS)
                .filter(eq(col("id"), lit(jazz::records::Value::Uuid(row.0))));
            let read = db.all(&db.prepare_query(&query)?, opts.clone()).await?;
            let space = read
                .iter()
                .find(|found| found.row_uuid() == row)
                .and_then(row_space);
            let passed = match &space {
                Some(space) if served.contains(space) => true,
                Some(space) => match SpaceAddress::new(&space.scope_id, &space.identifier) {
                    Ok(address) => self.deliver_pass(db, &address).await.is_ok(),
                    Err(_) => false,
                },
                None => false,
            };
            if passed {
                served.extend(space);
                done.push(row);
            } else if self.shared.now().duration_since(staged) >= MEMBER_CHANGE_WAIT {
                done.push(row);
            }
        }
        self.shared
            .member_removals
            .borrow_mut()
            .retain(|(row, _)| !done.contains(row));
        Ok(served)
    }

    /// Acquires the keys of every space of interest (see
    /// [`E2eeClient::watch_spaces`]) when something the device watches
    /// changed, or a space joined the watch: a delivery or a new version
    /// that synced in is acquired without waiting for a read (protocol
    /// §4.2). Also delivers after a rotation this device committed, even
    /// for unwatched spaces. One space's failure does not stop the others.
    /// Each result is returned with its space. The caller can retry a failed
    /// acquisition with backoff.
    pub async fn sync_spaces(
        &self,
        db: &Db,
        watch: &mut SpaceWatch,
    ) -> Result<Vec<(DbSpaceAddress, Result<SpaceKeyState, ClientError>)>, ClientError> {
        // A new round of `delivery-failed` reports starts here, before
        // anything that can fail: a sync that stops early must not leave
        // the last round's spaces muted in this round's sweep.
        self.shared
            .member_watch
            .borrow_mut()
            .delivery_failures_told
            .clear();
        self.ensure_open()?;
        let mut interest = self.shared.published.borrow().pending_spaces();
        interest.extend(watch.uses.keys().cloned());
        self.refresh_shared(db, watch, &interest).await?;
        #[cfg(any(test, feature = "testing"))]
        if std::mem::take(&mut self.shared.member_watch.borrow_mut().fail_next_sync) {
            return Err(crate::StoreError::Failed("sync failed for the test".into()).into());
        }
        // Member rows this client wrote that have committed: their spaces
        // are served now, in use or not (protocol §3.4).
        let committed = self.committed_member_changes(db).await?;
        // Member rows this client deleted: their spaces' passes run now, so
        // a removal marks its space for rotation (protocol §3.5).
        self.serve_removals(db).await?;
        self.shared.notice_member_changes();
        let members_changed = std::mem::take(&mut self.shared.member_watch.borrow_mut().changed);
        let mut served = committed;
        for (table, stream) in &mut watch.own {
            let (changed, _) = stream.changes(row_space);
            if *table == rows::SPACE_DELIVERIES {
                served.extend(changed);
            } else {
                served.extend(changed.intersection(&interest).cloned());
            }
        }
        served.extend(interest.difference(&watch.interest).cloned());
        watch.interest.clone_from(&interest);
        watch.unserved.retain(|space| interest.contains(space));
        served.extend(members_changed.intersection(&interest).cloned());
        // Rotations this device made with members left to deliver to,
        // each due now or after its retry wait (protocol §3.5).
        let pending_deliveries: BTreeSet<SpaceAddress> = if self.shared.keys.borrow().online {
            let now = self.shared.now();
            self.shared
                .member_watch
                .borrow()
                .pending_deliveries
                .iter()
                .filter(|(_, pending)| pending.retry_at.is_none_or(|at| now >= at))
                .map(|(space, _)| space.clone())
                .collect()
        } else {
            BTreeSet::new()
        };
        // A space served only because its rotation's retry came due runs
        // that batch alone, which reads only its accounts' histories: the
        // pass would read every member's (protocol §3.5).
        let retry_only: BTreeSet<SpaceAddress> =
            pending_deliveries.difference(&served).cloned().collect();
        served.extend(pending_deliveries.iter().cloned());
        let mut results = Vec::new();
        for space in served {
            // Interest may include malformed coordinates registered by a
            // caller. Report that space's error and continue with the rest.
            let validated = self.ensure_open().and_then(|()| {
                SpaceAddress::new(&space.scope_id, &space.identifier).map_err(ClientError::from)
            });
            let mut state = match validated {
                Ok(address) => self
                    .acquire_full(db, &address)
                    .await
                    .map(|acquired| acquired.state),
                Err(error) => Err(error),
            };
            // A subscriber waiting for its key still watches the members' roots.
            // The shared membership subscription supplies these rows locally.
            let needs_pass = matches!(state, Ok(SpaceKeyState::Held | SpaceKeyState::NoRoot));
            if state.is_ok()
                && !needs_pass
                && self
                    .shared
                    .member_watch
                    .borrow()
                    .spaces
                    .contains_key(&space)
            {
                match local_members(db, &space).await {
                    Ok(rows) => {
                        if let Some(deps) =
                            self.shared.member_watch.borrow_mut().spaces.get_mut(&space)
                        {
                            deps.members = rows.iter().filter_map(row_account_id).collect();
                            deps.holders.clear();
                            deps.waiting.retain(|id| deps.members.contains(id));
                            deps.version = None;
                        }
                    }
                    Err(error) => state = Err(error),
                }
            }
            // The rest of this device's rotation: every remaining member,
            // whether or not another member's delivery reached it.
            if pending_deliveries.contains(&space)
                && state.is_ok()
                && let Err(error) = self.finish_rotation(db, &space).await
            {
                state = Err(error);
            }
            // The pass: members that lack a version this device holds.
            let pass = needs_pass && !retry_only.contains(&space);
            if pass
                && let Err(error) = self.deliver_pass(db, &space).await
                && state.is_ok()
            {
                state = Err(error);
            }
            if (pass || pending_deliveries.contains(&space))
                && let Err(error) = &state
            {
                self.tell_delivery_failed(&space, error);
            }
            // Rows written under a refused creation's key, now that the real
            // key may be held (protocol §3.2).
            if state.is_ok()
                && self.holds_refused_key(&space)
                && let Err(error) = self.repair_auto(db, &space).await
            {
                state = Err(error);
            }
            if interest.contains(&space) && !retry_only.contains(&space) {
                if !needs_pass || state.is_err() {
                    watch.unserved.insert(space.clone());
                } else {
                    watch.unserved.remove(&space);
                }
            }
            results.push((db_address(&space), state));
        }
        // What was acquired may have moved a space's current version.
        self.refresh_shared(db, watch, &interest).await?;
        Ok(results)
    }
}

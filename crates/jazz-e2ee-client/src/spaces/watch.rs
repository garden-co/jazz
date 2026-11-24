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
use jazz::e2ee::SpaceAddress;
use jazz::query::{Operand, Predicate, Query, col, eq, lit};
use jazz_e2ee::protocol::ProtocolError;

use super::{SpaceKeyState, protocol_address};
use crate::account::rows::{self, uuid_cell};
use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::state::{WatchedRows, row_space};

/// Live subscriptions to the rows that bring this device its keys: always
/// its own member rows and the deliveries addressed to it, and, for the
/// spaces in use, one subscription per system table over all of them.
pub struct SpaceWatch {
    own: Vec<(&'static str, WatchedRows<SpaceAddress>)>,
    /// The spaces the shared subscriptions cover now.
    watched: BTreeSet<SpaceAddress>,
    /// Spaces whose subscriptions or pending reports already had an acquisition pass.
    interest: BTreeSet<SpaceAddress>,
    /// The current versions whose deliveries the shared watch covers.
    epochs: BTreeSet<String>,
    /// How many open app subscriptions use each space.
    uses: std::collections::BTreeMap<SpaceAddress, usize>,
}

impl SpaceWatch {
    /// An app subscription on an encrypted table opened and its rows are of
    /// `space`: the space joins the watched set (counted). The runtime
    /// cannot see subscriptions open and close, so the binding that
    /// opens them calls this and [`SpaceWatch::release_space`].
    pub fn use_space(&mut self, space: &SpaceAddress) {
        *self.uses.entry(space.clone()).or_default() += 1;
    }

    /// The last app subscription using `space` closed: it leaves the set.
    pub fn release_space(&mut self, space: &SpaceAddress) {
        if let Some(count) = self.uses.get_mut(space) {
            *count -= 1;
            if *count == 0 {
                self.uses.remove(space);
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
pub fn watch_queries(spaces: &[SpaceAddress], versions: &[String]) -> Vec<(&'static str, Query)> {
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

/// The queries of the account watch, one per table, over `accounts`:
/// `accountId IN (…)`, which the tables' `accountId` indexes serve. Public
/// only so that a test can ask the store for their plan.
#[doc(hidden)]
pub fn account_watch_queries(accounts: &[String]) -> Vec<(&'static str, Query)> {
    if accounts.is_empty() {
        return Vec::new();
    }
    [
        rows::ACCOUNT_ROOTS,
        rows::PUBLIC_ACCOUNT_SUCCESSORS,
        rows::ACCOUNT_KEYS,
    ]
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
    let id = |value: &str| uuid_cell(value).ok_or(ProtocolError::InconsistentEvidence);
    let query = Query::from(rows::SPACE_MEMBERS)
        .filter(eq(col("scopeId"), lit(id(&space.scope_id)?)))
        .filter(eq(col("identifier"), lit(id(&space.identifier)?)));
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
            .filter_map(|space| protocol_address(space).ok())
            .filter_map(|address| keys.held.known(&address)?.epochs().last().cloned())
            .collect()
    }

    /// Opens the shared subscriptions of the spaces in `interest`, one per
    /// system table, when the set, or a current version, changed since they
    /// were opened; the new ones are open before the old ones close.
    pub(super) async fn refresh_shared(
        &self,
        db: &Db,
        watch: &mut SpaceWatch,
        interest: &BTreeSet<SpaceAddress>,
    ) -> Result<(), ClientError> {
        let mut covered = self.shared.recent_writes();
        covered.extend(interest.iter().cloned());
        let epochs = self.current_epochs(&covered);
        if covered == watch.watched
            && epochs == watch.epochs
            && self.shared.member_watch.borrow().covered == covered
        {
            self.refresh_accounts(db, watch).await?;
            return Ok(());
        }
        let spaces: Vec<SpaceAddress> = covered.iter().cloned().collect();
        let versions: Vec<String> = epochs.iter().cloned().collect();
        let mut streams = Vec::new();
        for (table, query) in watch_queries(&spaces, &versions) {
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
            for deps in members.spaces.values_mut() {
                deps.version = None;
            }
            members.space_streams = streams;
            members.covered = covered.clone();
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
                if !keep && let Ok(address) = protocol_address(space) {
                    keys.rotation_evaluated.remove(&address);
                    keys.space_removals.remove(&address);
                }
                keep
            });
            for space in active {
                let deps = members.spaces.entry(space.clone()).or_default();
                for (table, accounts) in [
                    (rows::ACCOUNT_ROOTS, &deps.members),
                    (rows::PUBLIC_ACCOUNT_SUCCESSORS, &deps.holders),
                    (rows::ACCOUNT_KEYS, &deps.waiting),
                ] {
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
        for table in [
            rows::ACCOUNT_ROOTS,
            rows::PUBLIC_ACCOUNT_SUCCESSORS,
            rows::ACCOUNT_KEYS,
        ] {
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
            self.shared.member_watch.borrow_mut().streams.insert(
                table,
                crate::state::AccountStream {
                    accounts,
                    rows: WatchedRows::new(stream),
                },
            );
        }
        Ok(())
    }

    /// Opens the watches of this account's own rows: its member rows and
    /// the deliveries addressed to it. Spaces join as
    /// [`E2eeClient::sync_spaces`] finds them.
    pub async fn watch_spaces(&self, db: &Db) -> Result<SpaceWatch, ClientError> {
        let account = uuid_cell(&self.shared.account_id)
            .ok_or(ProtocolError::InvalidRecord("the account ID is not a UUID"))?;
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
        let wait = self.shared.member_change_wait.get();
        for (row, space, staged) in pending {
            if ready.contains(&space) {
                done.push(row);
                continue;
            }
            if protocol_address(&space).is_err() {
                done.push(row);
                continue;
            }
            let read = local_members(db, &space).await?;
            if read.iter().any(|found| found.row_uuid() == row) {
                ready.insert(space);
                done.push(row);
            } else if staged.elapsed() >= wait {
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
        let wait = self.shared.member_change_wait.get();
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
                .and_then(
                    |found| match (found.cell("scopeId"), found.cell("identifier")) {
                        (
                            Some(jazz::records::Value::Uuid(scope)),
                            Some(jazz::records::Value::Uuid(id)),
                        ) => Some(SpaceAddress {
                            scope_id: scope.to_string(),
                            identifier: id.to_string(),
                        }),
                        _ => None,
                    },
                );
            let passed = match &space {
                Some(space) if served.contains(space) => true,
                Some(space) => self.deliver_pass(db, space).await.is_ok(),
                None => false,
            };
            if passed {
                served.extend(space);
                done.push(row);
            } else if staged.elapsed() >= wait {
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
    /// §4.2). One space's failure does not stop the others; each result is
    /// returned with its space. Retrying a failed acquisition with backoff
    /// is the caller's.
    pub async fn sync_spaces(
        &self,
        db: &Db,
        watch: &mut SpaceWatch,
    ) -> Result<Vec<(SpaceAddress, Result<SpaceKeyState, ClientError>)>, ClientError> {
        self.ensure_open()?;
        let mut interest = self.shared.published.borrow().pending_spaces();
        interest.extend(watch.uses.keys().cloned());
        self.refresh_shared(db, watch, &interest).await?;
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
        served.extend(members_changed.intersection(&interest).cloned());
        let mut results = Vec::new();
        for space in served {
            // `acquire` refuses an address that is not a pair of UUIDs: that
            // space is reported with its error, the others go on.
            let mut state = self.acquire(db, &space).await;
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
                            deps.members = rows
                                .iter()
                                .filter_map(|row| match row.cell("accountId") {
                                    Some(jazz::records::Value::Uuid(id)) => Some(id.to_string()),
                                    _ => None,
                                })
                                .collect();
                            deps.holders.clear();
                            deps.waiting.retain(|id| deps.members.contains(id));
                            deps.version = None;
                        }
                    }
                    Err(error) => state = Err(error),
                }
            }
            // The pass: members that lack a version this device holds.
            if needs_pass && let Err(error) = self.deliver_pass(db, &space).await {
                state = Err(error);
            }
            // Rows written under a refused creation's key, now that the real
            // key may be held (protocol §3.2).
            if state.is_ok()
                && self.holds_refused_key(&space)
                && let Err(error) = self.repair_auto(db, &space).await
            {
                state = Err(error);
            }
            results.push((space, state));
        }
        // What was acquired may have moved a space's current version.
        self.refresh_shared(db, watch, &interest).await?;
        Ok(results)
    }
}

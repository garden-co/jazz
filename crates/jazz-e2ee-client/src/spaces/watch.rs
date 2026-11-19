//! What this device watches to acquire keys without waiting for a read
//! (protocol §4.2, §3.8; core hooks note H6): the deliveries addressed to
//! its account, its own member rows, and per space it holds keys of, or
//! has a report waiting for, the space's member rows, roots and
//! successors.
//!
//! Each watch opens before the decision it guards: a space joins the
//! watch, and only then is acquired, so a delivery that syncs in between
//! is seen. Watches of the member accounts' own rows and of the
//! deliveries of each held version belong to the delivery and rotation
//! parts of the runtime.

use std::collections::BTreeSet;

use jazz::db::{Db, ReadOpts, ReadTier, SubscriptionStream};
use jazz::e2ee::SpaceAddress;
use jazz::query::{Operand, Predicate, Query, col, eq, lit};
use jazz_e2ee::protocol::ProtocolError;

use super::{SpaceKeyState, protocol_address};
use crate::account::rows::{self, uuid_cell};
use crate::client::E2eeClient;
use crate::error::ClientError;

/// Live subscriptions to the rows that bring this device its keys: always
/// its own member rows and the deliveries addressed to it, and, for the
/// spaces in use, one subscription per system table over all of them.
pub struct SpaceWatch {
    own: Vec<SubscriptionStream>,
    /// The spaces the shared subscriptions cover now.
    watched: BTreeSet<SpaceAddress>,
    /// The current versions whose deliveries the shared watch covers.
    epochs: BTreeSet<String>,
    shared: Vec<SubscriptionStream>,
    /// The member accounts of the watched spaces whose roots, public
    /// successors and sealing keys the account watch covers.
    accounts: BTreeSet<String>,
    account_streams: Vec<SubscriptionStream>,
    /// How many open app subscriptions use each space.
    uses: std::collections::BTreeMap<SpaceAddress, usize>,
}

impl SpaceWatch {
    fn drained(streams: &mut [SubscriptionStream]) -> bool {
        let mut changed = false;
        for stream in streams {
            while stream.try_next_event().is_some() {
                changed = true;
            }
        }
        changed
    }

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
    let ids: Vec<Operand> = accounts
        .iter()
        .filter_map(|id| uuid_cell(id))
        .map(Operand::Literal)
        .collect();
    [
        rows::ACCOUNT_ROOTS,
        rows::PUBLIC_ACCOUNT_SUCCESSORS,
        rows::ACCOUNT_KEYS,
    ]
    .into_iter()
    .map(|table| {
        (
            table,
            Query::from(table).filter(Predicate::In(
                Operand::Column("accountId".to_owned()),
                ids.clone(),
            )),
        )
    })
    .collect()
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
    /// Both columns of the address are filtered: the store serves
    /// `scopeId IN (…) AND identifier IN (…)` from its
    /// `(scopeId, identifier, …)` index, but scans the table for
    /// `identifier IN (…)` alone. Whether it changed anything.
    pub(super) async fn refresh_shared(
        &self,
        db: &Db,
        watch: &mut SpaceWatch,
        interest: &BTreeSet<SpaceAddress>,
    ) -> Result<bool, ClientError> {
        let epochs = self.current_epochs(interest);
        if *interest == watch.watched && epochs == watch.epochs {
            self.refresh_accounts(db, watch, interest).await?;
            return Ok(false);
        }
        let spaces: Vec<SpaceAddress> = interest.iter().cloned().collect();
        let versions: Vec<String> = epochs.iter().cloned().collect();
        let mut streams = Vec::new();
        for (_, query) in watch_queries(&spaces, &versions) {
            streams.push(
                db.subscribe(&db.prepare_query(&query)?, Default::default())
                    .await?,
            );
        }
        watch.shared = streams;
        watch.watched = interest.clone();
        watch.epochs = epochs;
        self.refresh_accounts(db, watch, interest).await?;
        Ok(true)
    }

    /// Opens the shared subscriptions of the member accounts of the watched
    /// spaces (roots, public successors, sealing keys: what who lacks a key
    /// and the needs-rotation mark depend on, protocol §3.8), one per table
    /// over all the accounts, replaced with the new ones opened first when
    /// the set changes. The members come from this device's store, which the
    /// space watches keep current.
    async fn refresh_accounts(
        &self,
        db: &Db,
        watch: &mut SpaceWatch,
        interest: &BTreeSet<SpaceAddress>,
    ) -> Result<(), ClientError> {
        let mut accounts = BTreeSet::new();
        let spaces: Vec<SpaceAddress> = interest.iter().cloned().collect();
        if let Some((_, members)) = watch_queries(&spaces, &[])
            .into_iter()
            .find(|(table, _)| *table == rows::SPACE_MEMBERS)
        {
            let prepared = db.prepare_query(&members)?;
            let rows = db
                .all(
                    &prepared,
                    jazz::db::ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly),
                )
                .await?;
            for row in rows {
                if let Some(jazz::records::Value::Uuid(id)) = row.cell("accountId") {
                    accounts.insert(id.to_string());
                }
            }
        }
        if accounts == watch.accounts {
            return Ok(());
        }
        let listed: Vec<String> = accounts.iter().cloned().collect();
        let mut streams = Vec::new();
        for (_, query) in account_watch_queries(&listed) {
            streams.push(
                db.subscribe(&db.prepare_query(&query)?, Default::default())
                    .await?,
            );
        }
        watch.account_streams = streams;
        watch.accounts = accounts;
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
            own.push(
                db.subscribe(&db.prepare_query(&query)?, Default::default())
                    .await?,
            );
        }
        Ok(SpaceWatch {
            own,
            watched: BTreeSet::new(),
            epochs: BTreeSet::new(),
            shared: Vec::new(),
            accounts: BTreeSet::new(),
            account_streams: Vec::new(),
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
            let (Some(scope), Some(id)) =
                (uuid_cell(&space.scope_id), uuid_cell(&space.identifier))
            else {
                done.push(row);
                continue;
            };
            let query = Query::from(rows::SPACE_MEMBERS)
                .filter(eq(col("scopeId"), lit(scope)))
                .filter(eq(col("identifier"), lit(id)));
            let read = db
                .all(
                    &db.prepare_query(&query)?,
                    ReadOpts::for_read_tier(ReadTier::LocalOnly),
                )
                .await?;
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

    async fn spaces_of_interest(&self, db: &Db) -> Result<BTreeSet<SpaceAddress>, ClientError> {
        let mut spaces: BTreeSet<SpaceAddress> = BTreeSet::new();
        spaces.extend(self.shared.published.borrow().pending_spaces());
        let account = uuid_cell(&self.shared.account_id)
            .ok_or(ProtocolError::InvalidRecord("the account ID is not a UUID"))?;
        let deliveries =
            Query::from(rows::SPACE_DELIVERIES).filter(eq(col("recipientAccountId"), lit(account)));
        let prepared = db.prepare_query(&deliveries)?;
        let read = db
            .all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly))
            .await?;
        for row in read {
            let cell = |name| row.cell(name);
            if let (Some(jazz::records::Value::Uuid(scope)), Some(jazz::records::Value::Uuid(id))) =
                (cell("scopeId"), cell("identifier"))
            {
                spaces.insert(SpaceAddress {
                    scope_id: scope.to_string(),
                    identifier: id.to_string(),
                });
            }
        }
        Ok(spaces)
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
        let mut interest = self.spaces_of_interest(db).await?;
        interest.extend(watch.uses.keys().cloned());
        let joined = self.refresh_shared(db, watch, &interest).await?;
        // Member rows this client wrote that have committed: their spaces
        // are served now, in use or not (protocol §3.4).
        let committed = self.committed_member_changes(db).await?;
        let changed = joined
            | !committed.is_empty()
            | SpaceWatch::drained(&mut watch.own)
            | SpaceWatch::drained(&mut watch.shared)
            | SpaceWatch::drained(&mut watch.account_streams);
        if !changed {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        let mut served = interest.clone();
        served.extend(committed);
        for space in served {
            // `acquire` refuses an address that is not a pair of UUIDs: that
            // space is reported with its error, the others go on.
            let mut state = self.acquire(db, &space).await;
            // The pass: members that lack a version this device holds.
            if matches!(state, Ok(SpaceKeyState::Held))
                && let Err(error) = self.deliver_pass(db, &space).await
            {
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

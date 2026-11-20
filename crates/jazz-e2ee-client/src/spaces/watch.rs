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
use jazz::query::{Query, col, eq, lit};
use jazz_e2ee::protocol::ProtocolError;

use super::{SpaceKeyState, protocol_address};
use crate::account::rows::{self, uuid_cell};
use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::state::db_address;

/// Live subscriptions to the rows that bring this device its keys.
pub struct SpaceWatch {
    own: Vec<SubscriptionStream>,
    spaces: std::collections::BTreeMap<SpaceAddress, Vec<SubscriptionStream>>,
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
}

impl E2eeClient {
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
            spaces: Default::default(),
        })
    }

    /// The spaces this device acquires on a change: those it holds keys of,
    /// those a read reported `key-pending` or `space-pending`, and those
    /// its own deliveries name.
    async fn spaces_of_interest(&self, db: &Db) -> Result<BTreeSet<SpaceAddress>, ClientError> {
        let mut spaces: BTreeSet<SpaceAddress> = self
            .shared
            .keys
            .borrow()
            .held
            .spaces()
            .iter()
            .map(|space| db_address(space.address()))
            .collect();
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
        let interest = self.spaces_of_interest(db).await?;
        let mut joined = false;
        for space in &interest {
            if watch.spaces.contains_key(space) {
                continue;
            }
            let scope = uuid_cell(&space.scope_id).ok_or(ProtocolError::InvalidRecord(
                "the space's scope is not a UUID",
            ))?;
            let identifier = uuid_cell(&space.identifier).ok_or(ProtocolError::InvalidRecord(
                "the space's identifier is not a UUID",
            ))?;
            let mut streams = Vec::new();
            for table in [
                rows::SPACE_ROOTS,
                rows::SPACE_SUCCESSORS,
                rows::SPACE_MEMBERS,
            ] {
                let query = Query::from(table)
                    .filter(eq(col("scopeId"), lit(scope.clone())))
                    .filter(eq(col("identifier"), lit(identifier.clone())));
                streams.push(
                    db.subscribe(&db.prepare_query(&query)?, Default::default())
                        .await?,
                );
            }
            watch.spaces.insert(space.clone(), streams);
            joined = true;
        }
        let mut changed = joined | SpaceWatch::drained(&mut watch.own);
        for streams in watch.spaces.values_mut() {
            changed |= SpaceWatch::drained(streams);
        }
        if !changed {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        for space in interest {
            protocol_address(&space)?;
            let state = self.acquire(db, &space).await;
            results.push((space, state));
        }
        Ok(results)
    }
}

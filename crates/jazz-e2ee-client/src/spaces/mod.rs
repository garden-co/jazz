//! Spaces on this device (`E2EE_PROTOCOL.md` §3, §4.2, §4.3, §5.4; core
//! hooks note H2, H3, H6, H11): reading a space's rows at one cut, acquiring
//! its keys (§3.3), settling what reads could not open into the reports of
//! §4.3, and the runtime's wrappers around reads and writes that acquire
//! and retry once, as the hooks ask.
//!
//! The creation of spaces is in [`creation`], the watches in [`watch`].

mod creation;
mod delivery;
mod repair;
mod watch;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{Db, Error, ErrorCode};
use jazz::e2ee::{SpaceAddress, Unreadable, UnreadableCode};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::account::{AccountFloor, PublicAccountLedger};
use jazz_e2ee::protocol::content::{HeldSpace, KeyMark, MissingKey};
use jazz_e2ee::protocol::error::UnsupportedCapability;
use jazz_e2ee::protocol::records::space::{SpaceAddress as ProtocolAddress, SpaceDelivery};
use jazz_e2ee::protocol::space::members::{CurrentVersion, ReplayedSpaceChain};
use jazz_e2ee::protocol::space::{AccountDirectory, SpaceLedger, acquire};
use jazz_e2ee::protocol::{Position, ProtocolError};

use crate::account::rows::{self, uuid_cell};
use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::hooks::code;
use crate::state::{Missing, Report, SettleBackoff, Shared};

pub use delivery::MEMBER_LIMIT;
pub use watch::{SpaceWatch, account_watch_queries, watch_queries};

/// Reads that keep meeting concurrent writes give up after this many
/// attempts.
const ATTEMPTS: usize = 3;

/// Where this device stands with a space's keys after acquiring them
/// (protocol §3.3, §4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpaceKeyState {
    /// The device holds the current version's key, or its own creation's
    /// key while that creation is not accepted yet.
    Held,
    /// A member that lacks the current version: it waits for a delivery.
    KeyPending,
    /// Not a member, and the current version's key is not held.
    KeyUnavailable,
    /// The space has no valid root.
    NoRoot,
    /// The chain needs a capability this client lacks past its last
    /// resolved version (§3.3, "Unresolved history").
    Unresolved(UnsupportedCapability),
    /// The read held rows of the space this client cannot decode (a newer
    /// client's, or junk), so what it shows missing, a root, a member row,
    /// a successor, is not known to be missing: nothing is created and no
    /// key is declared unavailable (§3.3). The space is reported
    /// `unsupported-format`.
    Incomplete,
}

/// What replaying a space read gave.
pub(crate) struct Replayed {
    pub(crate) chain: Option<ReplayedSpaceChain>,
    pub(crate) held: Option<HeldSpace>,
    pub(crate) member: bool,
    pub(crate) acquisition: Option<jazz_e2ee::protocol::space::Acquisition>,
}

/// What one acquisition found.
pub(crate) struct Acquired {
    pub(crate) state: SpaceKeyState,
    pub(crate) chain: Option<ReplayedSpaceChain>,
    pub(crate) member: bool,
}

/// One space's rows and the public histories of the accounts they name, at
/// one cut.
pub(crate) struct SpaceRead {
    chain: Option<ReplayedSpaceChain>,
    acquisition: Option<jazz_e2ee::protocol::space::Acquisition>,
    pub(crate) ledger: SpaceLedger,
    pub(crate) accounts: Vec<PublicAccountLedger>,
    /// Rows of the space's own tables (roots, successors, members,
    /// deliveries) that are not records of their table (a newer client's,
    /// or junk): what the read shows absent may not be. Rows of the named
    /// accounts' histories are not counted: they make an account invalid,
    /// not the space incomplete.
    pub(crate) skipped: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SpaceReadPurpose {
    Acquire,
    WriteCheck,
    Deliver,
    Rotate,
}

pub(crate) fn protocol_address(space: &SpaceAddress) -> Result<ProtocolAddress, ClientError> {
    Ok(ProtocolAddress::new(&space.scope_id, &space.identifier)?)
}

#[doc(hidden)]
pub fn successor_probe(address: &ProtocolAddress, epoch: &str) -> Result<Query, ClientError> {
    let id = |s| uuid_cell(s).ok_or(ProtocolError::InconsistentEvidence);
    Ok(Query::from(rows::SPACE_SUCCESSORS)
        .filter(eq(col("scopeId"), lit(id(&address.scope_id)?)))
        .filter(eq(col("identifier"), lit(id(&address.identifier)?)))
        .filter(eq(col("predecessor"), lit(id(epoch)?)))
        .limit(1))
}

/// Deliveries for one version, using the space and epoch index for both
/// acquisition by one account and decisions about all recipients.
#[doc(hidden)]
pub fn epoch_deliveries_query(
    address: &ProtocolAddress,
    epoch: &str,
    account: Option<&str>,
) -> Result<Query, ClientError> {
    let id = |s| uuid_cell(s).ok_or(ProtocolError::InconsistentEvidence);
    let mut query = Query::from(rows::SPACE_DELIVERIES)
        .filter(eq(col("scopeId"), lit(id(&address.scope_id)?)))
        .filter(eq(col("identifier"), lit(id(&address.identifier)?)))
        .filter(eq(col("keyEpochId"), lit(id(epoch)?)));
    if let Some(account) = account {
        query = query.filter(eq(col("recipientAccountId"), lit(id(account)?)));
    }
    Ok(query)
}

pub(crate) fn offline() -> ClientError {
    ClientError::Database(Error::new(
        ErrorCode::NotObserved,
        "acquiring a key needs the authority: this client is offline",
    ))
}

impl E2eeClient {
    /// The space at `address`'s rows, filtered for this account (protocol
    /// §3.3): its roots and successors, this account's member rows and the
    /// deliveries addressed to it, and the public histories of this account
    /// and of every account a root or successor names, all at the cut of
    /// `tx`. Fails on rows this device wrote that the authority has not
    /// accepted: a ledger read has no position for them.
    pub(crate) async fn read_space(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
    ) -> Result<SpaceRead, ClientError> {
        self.read_space_of(db, tx, address, SpaceReadPurpose::Acquire)
            .await
    }

    /// Read the chain evidence first, then select deliveries by authenticated
    /// epoch at the same serializable cut. Rotation needs the whole space.
    pub(crate) async fn read_space_of(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
        purpose: SpaceReadPurpose,
    ) -> Result<SpaceRead, ClientError> {
        let skipped = Cell::new(0);
        let own = self.shared.account_id.clone();
        let scope = uuid_cell(&address.scope_id).ok_or(ProtocolError::InvalidRecord(
            "the space's scope is not a UUID",
        ))?;
        let identifier = uuid_cell(&address.identifier).ok_or(ProtocolError::InvalidRecord(
            "the space's identifier is not a UUID",
        ))?;
        let account =
            uuid_cell(&own).ok_or(ProtocolError::InvalidRecord("the account ID is not a UUID"))?;
        let of_space = |table: &str| {
            Query::from(table)
                .filter(eq(col("scopeId"), lit(scope.clone())))
                .filter(eq(col("identifier"), lit(identifier.clone())))
        };
        let (roots, through) = self
            .read_rows(db, tx, &skipped, of_space(rows::SPACE_ROOTS))
            .await?;
        let (successors, _) = self
            .read_rows(db, tx, &skipped, of_space(rows::SPACE_SUCCESSORS))
            .await?;
        let mut members_query = of_space(rows::SPACE_MEMBERS);
        let all_members = matches!(
            purpose,
            SpaceReadPurpose::Deliver | SpaceReadPurpose::Rotate
        );
        if !all_members {
            members_query = members_query.filter(eq(col("accountId"), lit(account.clone())));
        }
        let (members, _) = self.read_rows(db, tx, &skipped, members_query).await?;
        let mut ledger = if all_members {
            SpaceLedger::whole_space(address.clone(), Position(through))
        } else {
            SpaceLedger::one_account(address.clone(), Position(through), &own)
        };
        ledger.roots = roots;
        ledger.successors = successors;
        ledger.members = members;
        let mut named = BTreeSet::from([own]);
        named.extend(
            ledger
                .roots
                .iter()
                .map(|row| row.row.creator_account_id.clone()),
        );
        named.extend(
            ledger
                .successors
                .iter()
                .map(|row| row.row.author_account_id.clone()),
        );
        if all_members {
            named.extend(ledger.member_accounts());
        }
        let named_ids: Vec<_> = named.iter().cloned().collect();
        let account_skipped = Cell::new(0);
        let mut accounts = self
            .read_public_many(db, tx, &account_skipped, &named_ids, BTreeSet::new())
            .await?;
        let directory = AccountDirectory::new(
            &accounts,
            &self.shared.domain,
            &self.shared.registry,
            ledger.through,
        )?;
        self.space_floors(&accounts, &directory).await?;
        let chain = ReplayedSpaceChain::replay(&ledger, &directory)?;
        let mut acquisition = None;
        if purpose == SpaceReadPurpose::Rotate {
            (ledger.deliveries, _) = self
                .read_rows(db, tx, &skipped, of_space(rows::SPACE_DELIVERIES))
                .await?;
        } else {
            let mut epochs = BTreeSet::new();
            let epoch_deliveries = async |epochs: &BTreeSet<String>, own_only: bool| {
                let mut rows = Vec::new();
                for epoch in epochs {
                    let query = epoch_deliveries_query(
                        address,
                        epoch,
                        own_only.then_some(&self.shared.account_id),
                    )?;
                    let (found, _) = self.read_rows(db, tx, &skipped, query).await?;
                    rows.extend(found);
                }
                Ok::<_, ClientError>(rows)
            };
            if let Some(chain) = &chain {
                epochs.insert(chain.last().epoch_id.clone());
                if purpose != SpaceReadPurpose::WriteCheck {
                    epochs.extend(
                        chain
                            .skipped()
                            .into_iter()
                            .map(|depth| chain.versions[depth].epoch_id.clone()),
                    );
                }
                ledger.deliveries = epoch_deliveries(&epochs, true).await?;
                if purpose != SpaceReadPurpose::WriteCheck {
                    let acquired = {
                        let own = self.account.borrow();
                        let own = own.as_ref().ok_or(ClientError::NoAccount)?;
                        acquire(chain, &ledger, &directory, &own.epoch_keys)?
                    };
                    let held: BTreeSet<_> = acquired.keys.epoch_ids().collect();
                    if purpose == SpaceReadPurpose::Deliver {
                        // Only held versions can be delivered or mark a rotation.
                        epochs.retain(|id| held.contains(id.as_str()));
                        ledger.deliveries = epoch_deliveries(&epochs, false).await?;
                        acquisition = Some(acquired);
                    } else {
                        // If a newer key could not open its history, earlier
                        // deliveries may still give this device the missing keys.
                        let missing: BTreeSet<_> = chain
                            .versions
                            .iter()
                            .map(|v| &v.epoch_id)
                            .filter(|id| !epochs.contains(*id) && !held.contains(id.as_str()))
                            .cloned()
                            .collect();
                        if missing.is_empty() {
                            acquisition = Some(acquired);
                        } else {
                            // Epoch order lets the room-and-recipient index serve the whole read.
                            let query = of_space(rows::SPACE_DELIVERIES)
                                .filter(eq(col("recipientAccountId"), lit(account.clone())))
                                .order_by("keyEpochId", OrderDirection::Asc);
                            let (own_deliveries, _) = self
                                .read_rows::<SpaceDelivery>(db, tx, &skipped, query)
                                .await?;
                            ledger.deliveries.extend(
                                own_deliveries.into_iter().filter(|delivery| {
                                    missing.contains(&delivery.row.key_epoch_id)
                                }),
                            );
                            epochs.extend(missing);
                        }
                    }
                }
            }
            ledger = ledger.for_epochs(epochs);
        }
        if all_members {
            let additional: BTreeSet<_> = ledger
                .deliveries
                .iter()
                .flat_map(|d| [&d.row.sender_account_id, &d.row.recipient_account_id])
                .filter(|id| !named.contains(*id))
                .cloned()
                .collect();
            if !additional.is_empty() {
                accounts.extend(
                    self.read_public_many(
                        db,
                        tx,
                        &account_skipped,
                        &additional.into_iter().collect::<Vec<_>>(),
                        BTreeSet::new(),
                    )
                    .await?,
                );
            }
        }
        Ok(SpaceRead {
            chain,
            acquisition,
            ledger,
            accounts,
            skipped: skipped.get(),
        })
    }

    /// Reuse a replay only while the live watch covers the space's roots and
    /// successors. Confirm the signed records and positions still match the
    /// authority and probe the predecessor index for an extension that has
    /// not reached the watch yet. Any discrepancy requires a fresh replay.
    async fn still_current(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
        version: &CurrentVersion,
    ) -> Result<bool, ClientError> {
        let skipped = Cell::new(0);
        let by_id = |table: &str, id: &str| -> Result<Query, ClientError> {
            Ok(Query::from(table).filter(eq(
                col("id"),
                lit(uuid_cell(id).ok_or(ProtocolError::InconsistentEvidence)?),
            )))
        };
        let (roots, _) = self
            .read_rows(
                db,
                tx,
                &skipped,
                by_id(rows::SPACE_ROOTS, &version.root().row.id)?,
            )
            .await?;
        if roots.as_slice() != std::slice::from_ref(version.root()) {
            return Ok(false);
        }
        if let Some(tip) = version.successor() {
            let (successors, _) = self
                .read_rows(
                    db,
                    tx,
                    &skipped,
                    by_id(rows::SPACE_SUCCESSORS, &tip.row.id)?,
                )
                .await?;
            if successors.as_slice() != std::slice::from_ref(tip) {
                return Ok(false);
            }
        }
        let probe = successor_probe(address, version.epoch())?;
        Ok(db
            .tx_ref(tx)
            .ledger_read(&db.prepare_query(&probe)?)
            .await?
            .rows
            .is_empty())
    }

    /// Authenticate the chain using the normal acquisition read. The mark
    /// uses the same serializable cut and only the current version's
    /// envelopes. Former recipients of earlier versions need no account read.
    async fn check_write_version(
        &self,
        db: &Db,
        space: &SpaceAddress,
        address: &ProtocolAddress,
    ) -> Result<bool, ClientError> {
        if self.shared.keys.borrow().rotation_refused.contains(address) {
            return Ok(false);
        }
        crate::account::serializable(db, ATTEMPTS, async |tx| {
            let by_space = |table: &str| {
                Query::from(table)
                    .filter(eq(
                        col("scopeId"),
                        lit(uuid_cell(&address.scope_id).expect("validated address")),
                    ))
                    .filter(eq(
                        col("identifier"),
                        lit(uuid_cell(&address.identifier).expect("validated address")),
                    ))
            };
            let cached = self
                .shared
                .member_watch
                .borrow()
                .spaces
                .get(space)
                .and_then(|deps| deps.version.clone());
            let version = match cached {
                Some(version) if self.still_current(db, tx, address, &version).await? => version,
                _ => {
                    #[cfg(any(test, feature = "testing"))]
                    {
                        self.shared.member_watch.borrow_mut().chain_reads += 1;
                    }
                    let read = self
                        .read_space_of(db, tx, address, SpaceReadPurpose::WriteCheck)
                        .await?;
                    if read.skipped != 0 {
                        return Err(ProtocolError::InconsistentEvidence.into());
                    }
                    let acquired = self.take_space(db, space, address, read).await?;
                    let Some(chain) = acquired.chain else {
                        return Ok(false);
                    };
                    if !acquired.member || acquired.state != SpaceKeyState::Held {
                        return Ok(false);
                    }
                    CurrentVersion::from_chain(&chain)?
                }
            };
            let skipped = Cell::new(0);
            let (members, through) = self
                .read_rows::<jazz_e2ee::protocol::records::space::SpaceMember>(
                    db,
                    tx,
                    &skipped,
                    by_space(rows::SPACE_MEMBERS),
                )
                .await?;
            if !members
                .iter()
                .any(|member| member.row.account_id == self.shared.account_id)
            {
                if let Some(deps) = self.shared.member_watch.borrow_mut().spaces.get_mut(space) {
                    *deps = Default::default();
                }
                return Ok(false);
            }
            let query = epoch_deliveries_query(address, version.epoch(), None)?;
            let (deliveries, _) = self
                .read_rows::<SpaceDelivery>(db, tx, &skipped, query)
                .await?;
            let mut holders = BTreeSet::from([version.writer().to_owned()]);
            holders.extend(
                deliveries
                    .iter()
                    .map(|delivery| delivery.row.recipient_account_id.clone()),
            );
            let mut named = holders.clone();
            named.insert(version.root().row.creator_account_id.clone());
            let named: Vec<_> = named.into_iter().collect();
            let accounts = self
                .read_public_many(db, tx, &skipped, &named, BTreeSet::new())
                .await?;
            if skipped.get() != 0 {
                return Err(ProtocolError::InconsistentEvidence.into());
            }
            #[cfg(any(test, feature = "testing"))]
            {
                let history = accounts
                    .iter()
                    .map(|a| {
                        a.roots.len()
                            + a.successors.len()
                            + a.approvals.len()
                            + a.keys.len()
                            + a.recovery_roots.len()
                            + a.account_keys.len()
                            + a.removal_requests.len()
                    })
                    .sum::<usize>();
                self.shared.member_watch.borrow_mut().last_check_rows =
                    members.len() + deliveries.len() + history;
            }
            let through = Position(through);
            let directory = AccountDirectory::new(
                &accounts,
                &self.shared.domain,
                &self.shared.registry,
                through,
            )?;
            let own = &self.shared.account_id;
            if directory.has_ledger(own)
                && let Some(floor) = self.floor(own).await
            {
                directory
                    .current(own)?
                    .ok_or(ProtocolError::StaleState)?
                    .check_floor(through, &floor)?;
            }
            let floors = self.check_others(&accounts, through, &directory).await?;
            let marked = version
                .rotation_mark(address, through, &members, &deliveries, &directory)?
                .needs_rotation();
            for (id, floor) in floors {
                self.advance_floor(&id, &floor).await?;
            }
            let members: BTreeSet<_> = members
                .iter()
                .map(|row| row.row.account_id.clone())
                .collect();
            let holders: BTreeSet<_> = holders.intersection(&members).cloned().collect();
            let removals = holders
                .iter()
                .any(|account| match directory.current(account) {
                    Ok(Some(state)) => !state.removed.is_empty() || state.generation > 0,
                    Ok(None) => false,
                    Err(_) => true,
                });
            let covered = self.shared.member_watch.borrow().covered.contains(space);
            if let Some(deps) = self.shared.member_watch.borrow_mut().spaces.get_mut(space) {
                deps.version = covered.then_some(version);
                deps.waiting.retain(|account| members.contains(account));
                deps.members = members;
                deps.holders = holders;
            }
            let mut keys = self.shared.keys.borrow_mut();
            keys.rotation_evaluated.insert(address.clone());
            keys.space_removals.insert(address.clone(), removals);
            if marked {
                keys.rotation_pending.insert(address.clone());
            } else {
                keys.rotation_pending.remove(address);
            }
            Ok(true)
        })
        .await
    }

    /// Check the floors before authenticating the chain or opening a key.
    /// Check again when consuming the finished read, which can name extra recipients.
    async fn space_floors(
        &self,
        accounts: &[PublicAccountLedger],
        directory: &AccountDirectory<'_>,
    ) -> Result<Vec<(String, AccountFloor)>, ClientError> {
        let own = &self.shared.account_id;
        let through = directory.through();
        if let Some(floor) = self.floor(own).await {
            directory
                .replay(own)?
                .current()?
                .ok_or(ProtocolError::StaleState)?
                .check_floor(through, &floor)?;
        }
        self.check_others(accounts, through, directory).await
    }

    /// The chain of `read`, with the keys this account obtains from it, and
    /// whether it is a member. Every account history in the read is checked
    /// against this device's floor for that account first (§6.3a): a server
    /// that rolled one back (to show a removed device as active, or a
    /// member's epoch from before a device removal) is `stale-state`, and
    /// nothing of the read is used, so nothing is decrypted, written or
    /// delivered with it.
    ///
    /// Once the read has passed and its chain replayed, the floor of every
    /// other account whose current state the read verified is stored, or
    /// advanced if that state changed: the cut, generation, root and
    /// current epoch of the account's whole public history at this read's
    /// cut. A state equal to the stored floor writes nothing, so a device
    /// writes a floor once per account and then only when that account
    /// changes. The own account's floor is kept by account verification
    /// alone ([`E2eeClient::ensure_account`]).
    pub(crate) async fn replay_space(&self, read: &mut SpaceRead) -> Result<Replayed, ClientError> {
        let own = &self.shared.account_id;
        let directory = AccountDirectory::new(
            &read.accounts,
            &self.shared.domain,
            &self.shared.registry,
            read.ledger.through,
        )?;
        let floors = self.space_floors(&read.accounts, &directory).await?;
        let member = read.ledger.is_member(own);
        let replayed = {
            let account = self.account.borrow();
            let account = account.as_ref().ok_or(ClientError::NoAccount)?;
            match read.chain.take() {
                None => Replayed {
                    chain: None,
                    held: None,
                    member,
                    acquisition: None,
                },
                Some(chain) => {
                    let acquisition = match read.acquisition.take() {
                        Some(acquired) => acquired,
                        None => acquire(&chain, &read.ledger, &directory, &account.epoch_keys)?,
                    };
                    let held = HeldSpace::from_acquisition(&chain, &acquisition.keys)?;
                    Replayed {
                        chain: Some(chain),
                        held,
                        member,
                        acquisition: Some(acquisition),
                    }
                }
            }
        };
        for (account_id, floor) in &floors {
            self.advance_floor(account_id, floor).await?;
        }
        Ok(replayed)
    }

    /// Checks the current state of every account in `read` but this
    /// device's own against its floor, if this device has one (§2.4,
    /// "Floors"): an account with a floor whose read shows no counting
    /// root, an earlier cut or generation, or a chain without the floor's
    /// epoch is `stale-state`; a replaced root is `root-not-mine`. Returns
    /// the floors to store once the read is used: those of the accounts
    /// with a current state that differs from their stored floor, or that
    /// have none yet. An account whose current state cannot be evaluated
    /// is neither checked nor floored: every use of that state (its
    /// sealing keys, its current epoch) fails the same way.
    async fn check_others(
        &self,
        accounts: &[PublicAccountLedger],
        through: Position,
        directory: &AccountDirectory<'_>,
    ) -> Result<Vec<(String, AccountFloor)>, ClientError> {
        let mut advance = Vec::new();
        for ledger in accounts {
            let account_id = &ledger.account_id;
            if *account_id == self.shared.account_id {
                continue;
            }
            let Ok(state) = directory.current(account_id) else {
                continue;
            };
            let stored = self.floor(account_id).await;
            let Some(state) = state else {
                if stored.is_some() {
                    return Err(ProtocolError::StaleState.into());
                }
                continue;
            };
            if let Some(stored) = &stored {
                state.check_floor(through, stored)?;
            }
            let floor = state.floor(through, None);
            let unchanged = stored.is_some_and(|stored| {
                AccountFloor {
                    through: stored.through,
                    ..floor.clone()
                } == stored
            });
            if !unchanged {
                advance.push((account_id.clone(), floor));
            }
        }
        Ok(advance)
    }

    /// Acquires the keys of `space` (protocol §3.3): reads its rows and
    /// the accounts they name at one cut, replays the chain, and keeps
    /// every key this account's own envelope or a delivery to it gives,
    /// earlier versions the history links reach included. Reads that left
    /// rows out for want of those keys are refreshed. Needs the authority,
    /// and the account verified in this run.
    pub async fn acquire(
        &self,
        db: &Db,
        space: &SpaceAddress,
    ) -> Result<SpaceKeyState, ClientError> {
        Ok(self.acquire_full(db, space).await?.state)
    }

    pub(crate) async fn acquire_full(
        &self,
        db: &Db,
        space: &SpaceAddress,
    ) -> Result<Acquired, ClientError> {
        self.ensure_open()?;
        let address = protocol_address(space)?;
        // A creation of this device that is neither accepted nor refused
        // yet has rows no ledger read can show: its own key serves.
        if self.settle_creations(db, &address, None).await? {
            return Ok(Acquired {
                state: SpaceKeyState::Held,
                chain: None,
                member: true,
            });
        }
        if !self.shared.keys.borrow().online {
            return Err(offline());
        }
        let read = crate::account::serializable(db, ATTEMPTS, async |tx| {
            self.read_space(db, tx, &address)
                .await
                .map_err(|error| self.forget_account_on(error))
        })
        .await?;
        self.take_space(db, space, &address, read).await
    }

    /// Takes what `read` shows of a space: the chain this device now knows,
    /// its keys, the creations it settles and the reports it withdraws.
    async fn take_space(
        &self,
        db: &Db,
        space: &SpaceAddress,
        address: &ProtocolAddress,
        mut read: SpaceRead,
    ) -> Result<Acquired, ClientError> {
        let Replayed {
            chain,
            held,
            member,
            acquisition,
        } = match self.replay_space(&mut read).await {
            Ok(replayed) => replayed,
            Err(error) => return Err(self.forget_account_on(error)),
        };
        if let Some(acquisition) = &acquisition {
            for delivery in &acquisition.non_derived_deliveries {
                self.shared.conflicting_account_key(&delivery.sealing_key);
            }
            for bad in &acquisition.invalid_deliveries {
                self.shared.event(
                    format!("delivery:{}", bad.delivery_id),
                    Report::InvalidDelivery {
                        space: space.clone(),
                        sender: bad.sender_account_id.clone(),
                        recipient: bad.recipient_account_id.clone(),
                        key_epoch_id: bad.key_epoch_id.clone(),
                    },
                );
            }
            for broken in &acquisition.broken_history {
                self.shared.event(
                    format!("history:{}", broken.successor_id),
                    Report::BrokenHistory {
                        space: space.clone(),
                        key_epoch_id: broken.key_epoch_id.clone(),
                        author: broken.author_account_id.clone(),
                    },
                );
            }
        }
        let incomplete = read.skipped > 0;
        let Some(chain) = chain else {
            self.settle_creations(db, address, None).await?;
            let state = if incomplete {
                SpaceKeyState::Incomplete
            } else {
                SpaceKeyState::NoRoot
            };
            if self.record_space_read(space, address, state, member) {
                self.update_unreadable_reasons(db, space, address).await;
            }
            return Ok(Acquired {
                state,
                chain: None,
                member,
            });
        };
        let known = |client: &Self| {
            let keys = client.shared.keys.borrow();
            keys.held
                .roots_of(address)
                .iter()
                .map(|space| space.held().count())
                .sum::<usize>()
        };
        let before = known(self);
        self.shared.keys.borrow_mut().held.observe_chain(&chain);
        if let Some(held) = held {
            self.keep(held).await?;
        }
        self.settle_creations(db, address, Some(&chain.root.row.id))
            .await?;
        let state = match chain.current() {
            Err(ProtocolError::Unsupported(capability)) => SpaceKeyState::Unresolved(capability),
            Err(error) => return Err(error.into()),
            Ok(current) => {
                if self
                    .shared
                    .keys
                    .borrow()
                    .held
                    .holds(address, &current.epoch_id)
                {
                    SpaceKeyState::Held
                } else if member {
                    SpaceKeyState::KeyPending
                } else {
                    SpaceKeyState::KeyUnavailable
                }
            }
        };
        let state = match state {
            SpaceKeyState::KeyPending | SpaceKeyState::KeyUnavailable if incomplete => {
                SpaceKeyState::Incomplete
            }
            other => other,
        };
        let reason_changed = self.record_space_read(space, address, state, member);
        if known(self) != before {
            self.shared.forget_missing_keys(space);
            let tables = self.tables_of(space);
            let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
            db.refresh_reads_of(&tables).await?;
        } else if reason_changed {
            self.update_unreadable_reasons(db, space, address).await;
        }
        Ok(Acquired {
            state,
            chain: Some(chain),
            member,
        })
    }

    /// Records the read's reason for missing keys and whether it changed.
    /// Withdraws the waiting reports of `space` that the acquisition's
    /// `state` no longer gives (protocol §4.3): a space with a root is no
    /// longer `space-pending`, one whose key arrived is neither
    /// `key-pending` nor `key-unavailable`, a member is no longer
    /// `key-unavailable`.
    fn record_space_read(
        &self,
        space: &SpaceAddress,
        address: &ProtocolAddress,
        state: SpaceKeyState,
        member: bool,
    ) -> bool {
        let (incomplete, reason_changed) = {
            let mut keys = self.shared.keys.borrow_mut();
            let previous = keys.missing_key_code(space);
            keys.space_reads.insert(space.clone(), (state, member));
            let incomplete = keys
                .held
                .incomplete(address)
                .then(|| keys.incomplete_query_code(space));
            (incomplete, previous != keys.missing_key_code(space))
        };
        let current = match state {
            SpaceKeyState::KeyPending => Some(UnreadableCode::KeyPending),
            SpaceKeyState::KeyUnavailable => Some(UnreadableCode::KeyUnavailable),
            SpaceKeyState::NoRoot if member => Some(UnreadableCode::SpacePending),
            SpaceKeyState::NoRoot => Some(UnreadableCode::KeyUnavailable),
            _ => None,
        };
        let gone: Vec<UnreadableCode> = [
            UnreadableCode::KeyPending,
            UnreadableCode::KeyUnavailable,
            UnreadableCode::SpacePending,
        ]
        .into_iter()
        .filter(|code| Some(*code) != current)
        .collect();
        self.shared.withdraw(space, &gone, incomplete);
        reason_changed
    }

    async fn update_unreadable_reasons(
        &self,
        db: &Db,
        space: &SpaceAddress,
        address: &ProtocolAddress,
    ) {
        db.update_unreadable_reasons(space, |epoch| {
            let keys = self.shared.keys.borrow();
            if keys
                .held
                .known(address)
                .is_some_and(|known| known.is_absent(epoch))
            {
                UnreadableCode::InvalidCiphertext
            } else {
                keys.missing_key_code(space)
            }
        })
        .await;
    }

    /// The encrypted tables whose rows `space` holds the keys of.
    pub(crate) fn tables_of(&self, space: &SpaceAddress) -> Vec<String> {
        let keys = self.shared.keys.borrow();
        let Some(catalogue) = keys.catalogue.as_ref() else {
            return Vec::new();
        };
        self.shared
            .plans
            .iter()
            .filter(|(_, plan)| {
                catalogue
                    .table(&plan.entry.scope)
                    .is_ok_and(|scope| scope == space.scope_id)
            })
            .map(|(table, _)| table.clone())
            .collect()
    }

    /// Settles what a read left out for want of keys (protocol §4.2,
    /// §4.3), after one acquisition per space: `key-pending` and
    /// `space-pending` once per `(table, space)`, `key-unavailable` and
    /// `invalid-ciphertext` per row. Whether any version is now held.
    ///
    /// Every space is settled. One whose acquisition fails reports nothing
    /// (a failure is not a key's absence) and keeps its entries for the
    /// next settle; the first such error is returned once the others are
    /// reported. A space that failed is retried at the next settle, then
    /// after a wait that doubles with each failure in a row (see
    /// [`SettleBackoff`]): until then it is not read and fails nothing, so
    /// the other spaces' rows are still returned; its rows stay unresolved
    /// (left out, reported `MissingKey` by the read, their entries kept).
    /// A success ends the wait.
    async fn settle_missing(&self, db: &Db, missing: Vec<Missing>) -> Result<bool, ClientError> {
        let mut by_space: BTreeMap<SpaceAddress, Vec<Missing>> = BTreeMap::new();
        for entry in missing {
            by_space.entry(entry.space.clone()).or_default().push(entry);
        }
        // A space no read waits on any more, neither this one nor the
        // entries kept for a later settle, has nothing to back off from.
        {
            let queued: BTreeSet<SpaceAddress> = self
                .shared
                .missing
                .borrow()
                .iter()
                .map(|entry| entry.space.clone())
                .collect();
            self.shared
                .settle_backoff
                .borrow_mut()
                .retain(|space, _| by_space.contains_key(space) || queued.contains(space));
        }
        let now = self.shared.now();
        let mut reports = Vec::new();
        let mut held = false;
        let mut failure = None;
        for (space, entries) in by_space {
            let backoff = self.shared.settle_backoff.borrow().get(&space).copied();
            if backoff.is_some_and(|backoff| now < backoff.retry_at) {
                for entry in entries {
                    self.shared.missing(entry);
                }
                continue;
            }
            match self.settle_space(db, &space, &entries).await {
                Ok((settled, any_held)) => {
                    self.shared.settle_backoff.borrow_mut().remove(&space);
                    reports.extend(settled);
                    held |= any_held;
                }
                Err(error) => {
                    let next = SettleBackoff::after_failure(backoff.as_ref(), now);
                    self.shared.settle_backoff.borrow_mut().insert(space, next);
                    for entry in entries {
                        self.shared.missing(entry);
                    }
                    failure.get_or_insert(error);
                }
            }
        }
        self.shared.flush(reports);
        match failure {
            Some(error) => Err(error),
            None => Ok(held),
        }
    }

    /// Settles the `entries` a read left out of `space`: the reports they
    /// get, and whether any version is now held.
    async fn settle_space(
        &self,
        db: &Db,
        space: &SpaceAddress,
        entries: &[Missing],
    ) -> Result<(Vec<Report>, bool), ClientError> {
        let acquired = self.acquire_full(db, space).await?;
        let address = protocol_address(space)?;
        let mut reports = Vec::new();
        let mut held = false;
        if acquired.state == SpaceKeyState::Incomplete {
            // What the read shows missing may not be: neither a key's
            // absence nor a space's is reported.
            for table in entries
                .iter()
                .map(|entry| entry.table.clone())
                .collect::<BTreeSet<_>>()
            {
                reports.push(Report::Unreadable(Unreadable {
                    table,
                    id: None,
                    column: None,
                    code: UnreadableCode::UnsupportedFormat,
                    space: Some(space.clone()),
                }));
            }
            return Ok((reports, held));
        }
        let mut invalid_epoch = false;
        for entry in entries {
            let settled = self.shared.keys.borrow_mut().held.settle_missing(
                &address,
                &entry.epoch_id,
                acquired.chain.as_deref(),
                acquired.member,
            )?;
            let unreadable = |code, row: bool| {
                Report::Unreadable(Unreadable {
                    table: entry.table.clone(),
                    id: row.then_some(entry.id),
                    column: row.then(|| entry.column.clone()),
                    code,
                    space: Some(space.clone()),
                })
            };
            match settled {
                MissingKey::Held => held = true,
                MissingKey::Report(error) => {
                    let code = code(error);
                    invalid_epoch |= code == UnreadableCode::InvalidCiphertext;
                    let per_space = matches!(
                        code,
                        UnreadableCode::KeyPending | UnreadableCode::SpacePending
                    );
                    reports.push(unreadable(code, !per_space));
                }
                // Neither invalid nor known to be pending: no code of
                // its own (protocol §4.2).
                MissingKey::Unresolved(_) => {}
            }
        }
        if invalid_epoch {
            self.update_unreadable_reasons(db, space, &address).await;
        }
        Ok((reports, held))
    }

    /// After host reads that left rows out for want of a key, acquires the
    /// keys of the spaces they named (once per space) and settles the
    /// reports to `key-pending`, `key-unavailable` or `space-pending`.
    /// Whether any key was obtained, so the reads are worth repeating.
    /// Nothing to do (and nothing read) when no read left a row out.
    pub async fn settle_reads(&self, db: &Db) -> Result<bool, ClientError> {
        self.ensure_open()?;
        let missing: Vec<Missing> = self.shared.missing.borrow_mut().drain(..).collect();
        if missing.is_empty() || !self.shared.keys.borrow().online {
            return Ok(false);
        }
        self.settle_missing(db, missing).await
    }

    /// Keep rotation marks private while preparing. Grant the host its
    /// retry only if it is still waiting when preparation finishes.
    pub async fn prepare_host_write(
        &self,
        db: &Db,
        space: &SpaceAddress,
        deadline: Option<web_time::Instant>,
    ) -> Result<bool, ClientError> {
        self.ensure_open()?;
        let expired = || deadline.is_some_and(|end| web_time::Instant::now() >= end);
        if expired() {
            return Ok(false);
        }
        let call = WritePreparation::enter(&self.shared);
        self.reconcile_once(db).await?;
        self.begin_attempt(db).await?;
        if !self.make_ready(db, space, Some(call.id), deadline).await? || expired() {
            return Ok(false);
        }
        let address = protocol_address(space)?;
        let mut keys = self.shared.keys.borrow_mut();
        if keys
            .rotation_tried
            .remove(&(Some(call.id), address.clone()))
        {
            keys.rotation_tried.insert((None, address));
        }
        Ok(true)
    }

    /// Grant a host's next retry the current key if a rotation still needs
    /// attention. The normal seal checks the key again and consumes the mark.
    pub fn retry_with_current_key(
        &self,
        space: &SpaceAddress,
        failure: &str,
    ) -> Result<bool, ClientError> {
        self.ensure_open()?;
        let address = protocol_address(space)?;
        let mut keys = self.shared.keys.borrow_mut();
        let pending = keys.rotation_pending.contains(&address)
            || keys.needs_evaluation(&address)
            || !self.shared.member_removals.borrow().is_empty();
        if !pending
            || crate::hooks::write_key(&keys, &self.shared.creations.borrow().dropped, &address)
                .is_err()
        {
            return Ok(false);
        }
        keys.rotation_tried.insert((None, address));
        drop(keys);
        self.shared.flush(vec![Report::RotationFailed {
            space: space.clone(),
            reason: failure.to_owned(),
        }]);
        Ok(true)
    }

    /// Abandons creations whose staged root the database dropped before
    /// preparing the next host retry.
    async fn begin_attempt(&self, db: &Db) -> Result<(), ClientError> {
        let dropped: Vec<String> = self
            .shared
            .creations
            .borrow()
            .dropped
            .iter()
            .cloned()
            .collect();
        for root_id in &dropped {
            self.abandon_creation(root_id).await?;
        }
        // A creation the authority refused since the last write stops
        // the automatic paths before this one stages anything.
        self.settle_pending(db).await?;
        // A member removal this client wrote is served before anything is
        // sealed: its pass marks the space for rotation, so the write that
        // follows rotates first (protocol §3.5).
        self.serve_removals(db).await?;
        Ok(())
    }

    /// Only an observed tombstone ends administration. A missing row can
    /// simply be outside the local store's subscriptions.
    async fn space_deleted(&self, db: &Db, space: &SpaceAddress) -> Result<bool, ClientError> {
        let table = {
            let keys = self.shared.keys.borrow();
            let catalogue = keys
                .catalogue
                .as_ref()
                .ok_or_else(crate::state::not_observed)?;
            self.shared
                .plans
                .values()
                .map(|plan| &plan.entry.scope)
                .find(|table| catalogue.table(table).is_ok_and(|id| id == space.scope_id))
                .ok_or_else(crate::state::not_observed)?
        };
        let id = uuid_cell(&space.identifier).ok_or(ProtocolError::InconsistentEvidence)?;
        let query = Query::from(table)
            .select(["id"])
            .filter(eq(col("id"), lit(id)));
        let mut opts = jazz::db::ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly);
        opts.read_view.include_deleted = true;
        Ok(db
            .all(&db.prepare_query(&query)?, opts)
            .await?
            .iter()
            .any(|row| row.is_deleted()))
    }

    /// Gets a key ready for the host to retry a write to `space`. Rotation
    /// attempts use the preparation's mark.
    async fn make_ready(
        &self,
        db: &Db,
        space: &SpaceAddress,
        call: Option<u64>,
        deadline: Option<web_time::Instant>,
    ) -> Result<bool, ClientError> {
        let address = protocol_address(space)?;
        let online = self.shared.keys.borrow().online;
        // A space whose mark this runtime has not evaluated, where a removed
        // device may hold its version (see `KeyState::needs_evaluation`), is
        // read first, with the account verified, by one pass over that
        // space: the mark of a removal made elsewhere (another device or
        // account, or this device before the runtime opened) is known only
        // once a pass has read it. A pass that fails marks the space (fail
        // closed), as after a removal.
        self.shared.note_write(&address);
        self.shared.notice_member_changes();
        let unevaluated = |keys: &crate::state::KeyState| keys.needs_evaluation(&address);
        let mut ready = false;
        if online && unevaluated(&self.shared.keys.borrow()) {
            self.shared
                .keys
                .borrow_mut()
                .rotation_tried
                .insert((call, address.clone()));
            let unverified = self.account.borrow().is_none();
            let checked = async {
                if unverified {
                    self.ensure_account(db).await?;
                }
                if unevaluated(&self.shared.keys.borrow()) {
                    ready = self.check_write_version(db, space, &address).await?;
                }
                Ok::<_, ClientError>(())
            }
            .await;
            if let Err(error) = checked {
                self.shared.flush(vec![Report::RotationCheckFailed {
                    space: space.clone(),
                    reason: error.to_string(),
                }]);
                let mut keys = self.shared.keys.borrow_mut();
                if !keys.rotation_refused.contains(&address) {
                    keys.rotation_pending.insert(address.clone());
                }
            }
        }
        // A space that needs rotation is rotated before the write; refused,
        // offline or undecided, the write seals under the current version
        // (protocol §3.5).
        // A removal still queued here was staged by the failed call itself
        // (the attempt served the rest): the write tried again seals under
        // the current version, and the removal is served once it commits,
        // before the next write.
        let removal = !self.shared.member_removals.borrow().is_empty();
        let pending = {
            let mut keys = self.shared.keys.borrow_mut();
            let pending = keys.rotation_pending.contains(&address);
            if pending || removal {
                keys.rotation_tried.insert((call, address.clone()));
            }
            pending
        };
        if pending {
            // A rotation that succeeded or was refused clears the mark; one
            // that failed for another reason leaves it for the next write.
            // Either way the app is told why: the write goes on under the
            // current version.
            if online
                && deadline.is_none_or(|end| web_time::Instant::now() < end)
                && !self.space_deleted(db, space).await?
                && let Err(error) = self.rotate_before(db, space, deadline).await
            {
                self.shared.flush(vec![Report::RotationFailed {
                    space: space.clone(),
                    reason: error.to_string(),
                }]);
            }
            // Only a device that holds the current version can write under
            // it. One that holds no key (a device awaiting approval, or a
            // member waiting for a delivery) gets one as any write does,
            // below: told to try again now, it would fail the same way.
            let holds_current = {
                let keys = self.shared.keys.borrow();
                crate::hooks::write_key(&keys, &self.shared.creations.borrow().dropped, &address)
                    .is_ok()
            };
            if holds_current {
                return Ok(true);
            }
            if !online {
                return Ok(false);
            }
        }
        if ready {
            return Ok(true);
        }
        if online {
            let acquired = self.acquire_full(db, space).await?;
            match acquired.state {
                SpaceKeyState::Held => return Ok(true),
                SpaceKeyState::NoRoot => {}
                _ => return Ok(false),
            }
        }
        if self
            .shared
            .keys
            .borrow()
            .creation_refused
            .contains(&address)
        {
            return Ok(false);
        }
        // A read that shows no root while this device holds a key of the
        // space is a read that hides it: no second root.
        if self.holds_live_key(&address) {
            return Ok(false);
        }
        self.prepare_creation(&address).await?;
        Ok(true)
    }

    /// Marks the key of a root this device staged and then failed to
    /// commit `refused`: the creation did not happen, and nothing stops
    /// the next write from trying again (protocol §3.2, "Transaction
    /// failed otherwise").
    async fn abandon_creation(&self, root_id: &str) -> Result<(), ClientError> {
        self.shared.creations.borrow_mut().staged.remove(root_id);
        let space = self.shared.keys.borrow().held.space(root_id).cloned();
        if let Some(mut space) = space {
            space.settle(
                jazz_e2ee::protocol::content::CreationFate::NotAccepted,
                None,
            );
            self.keep(space).await?;
        }
        // Refused and kept: the hooks need not hold the key back any more.
        self.shared.creations.borrow_mut().dropped.remove(root_id);
        Ok(())
    }
}

/// Whether `space` holds a key that is not `refused`.
pub(crate) fn has_live_key(space: &HeldSpace) -> bool {
    space.held().any(|(_, mark)| *mark != KeyMark::Refused)
}

/// Whether `space` holds a key of an unaccepted creation.
pub(crate) fn has_unpublished(space: &HeldSpace) -> bool {
    space
        .held()
        .any(|(_, mark)| matches!(mark, KeyMark::Unpublished { .. }))
}

/// Keeps rotation marks private to one host write preparation.
struct WritePreparation<'a> {
    shared: &'a Shared,
    id: u64,
}

impl<'a> WritePreparation<'a> {
    fn enter(shared: &'a Shared) -> Self {
        let id = shared.next_write.get();
        shared.next_write.set(id.wrapping_add(1));
        Self { shared, id }
    }
}
impl Drop for WritePreparation<'_> {
    fn drop(&mut self) {
        let id = self.id;
        self.shared
            .keys
            .borrow_mut()
            .rotation_tried
            .retain(|(call, _)| *call != Some(id));
    }
}

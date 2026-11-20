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
use jazz::e2ee::{E2eeRefusal, SpaceAddress, Unreadable, UnreadableCode};
use jazz::query::{Query, col, eq, lit};
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::account::{AccountFloor, PublicAccountLedger};
use jazz_e2ee::protocol::content::{HeldSpace, KeyMark, MissingKey};
use jazz_e2ee::protocol::error::UnsupportedCapability;
use jazz_e2ee::protocol::records::space::SpaceAddress as ProtocolAddress;
use jazz_e2ee::protocol::space::{AccountDirectory, SpaceChain, SpaceLedger, acquire};
use jazz_e2ee::protocol::{Position, ProtocolError};

use crate::account::rows::{self, uuid_cell};
use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::hooks::code;
use crate::state::{Missing, Report, SettleBackoff, Shared};

pub use creation::SpaceCreation;
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
    pub(crate) chain: Option<SpaceChain>,
    pub(crate) held: Option<HeldSpace>,
    pub(crate) member: bool,
    pub(crate) acquisition: Option<jazz_e2ee::protocol::space::Acquisition>,
}

/// What one acquisition found.
pub(crate) struct Acquired {
    pub(crate) state: SpaceKeyState,
    pub(crate) chain: Option<SpaceChain>,
    pub(crate) member: bool,
}

/// One space's rows and the public histories of the accounts they name, at
/// one cut.
pub(crate) struct SpaceRead {
    pub(crate) ledger: SpaceLedger,
    pub(crate) accounts: Vec<PublicAccountLedger>,
    /// Rows of the space's own tables (roots, successors, members,
    /// deliveries) that are not records of their table (a newer client's,
    /// or junk): what the read shows absent may not be. Rows of the named
    /// accounts' histories are not counted: they make an account invalid,
    /// not the space incomplete.
    pub(crate) skipped: usize,
}

pub(crate) fn protocol_address(space: &SpaceAddress) -> Result<ProtocolAddress, ClientError> {
    Ok(ProtocolAddress::new(&space.scope_id, &space.identifier)?)
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
        self.read_space_of(db, tx, address, false).await
    }

    /// [`E2eeClient::read_space`], or with `whole` the whole space: every
    /// member row and every delivery of the address (protocol §3.4), and
    /// the public history of every account they name. Deliveries are
    /// readable to every member (R14), so a delivery another holder writes
    /// meanwhile is a phantom of this read and makes a transaction that
    /// read it conflict.
    pub(crate) async fn read_space_of(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
        whole: bool,
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
        let mut deliveries_query = of_space(rows::SPACE_DELIVERIES);
        if !whole {
            members_query = members_query.filter(eq(col("accountId"), lit(account.clone())));
            deliveries_query = deliveries_query.filter(eq(col("recipientAccountId"), lit(account)));
        }
        let (members, _) = self.read_rows(db, tx, &skipped, members_query).await?;
        let (deliveries, _) = self.read_rows(db, tx, &skipped, deliveries_query).await?;
        let space_skipped = skipped.get();
        let mut ledger = if whole {
            SpaceLedger::whole_space(address.clone(), Position(through))
        } else {
            SpaceLedger::one_account(address.clone(), Position(through), &own)
        };
        ledger.roots = roots;
        ledger.successors = successors;
        ledger.members = members;
        ledger.deliveries = deliveries;
        let mut named = BTreeSet::from([own]);
        named.extend(
            ledger
                .roots
                .iter()
                .map(|root| root.row.creator_account_id.clone()),
        );
        named.extend(
            ledger
                .successors
                .iter()
                .map(|successor| successor.row.author_account_id.clone()),
        );
        if whole {
            named.extend(ledger.members.iter().map(|m| m.row.account_id.clone()));
            for delivery in &ledger.deliveries {
                named.insert(delivery.row.recipient_account_id.clone());
                named.insert(delivery.row.sender_account_id.clone());
            }
        }
        let named: Vec<String> = named.into_iter().collect();
        let accounts = self
            .read_public_many(db, tx, &skipped, &named, BTreeSet::new())
            .await?;
        Ok(SpaceRead {
            ledger,
            accounts,
            skipped: space_skipped,
        })
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
    pub(crate) async fn replay_space(&self, read: &SpaceRead) -> Result<Replayed, ClientError> {
        let own = &self.shared.account_id;
        let floor = self.floor(own).await;
        let directory = AccountDirectory::new(
            &read.accounts,
            &self.shared.domain,
            &self.shared.registry,
            read.ledger.through,
        )?;
        if let Some(floor) = &floor {
            let state = directory
                .replay(own)?
                .current()?
                .ok_or(ProtocolError::StaleState)?;
            state.check_floor(read.ledger.through, floor)?;
        }
        let floors = self.check_others(read, &directory).await?;
        let member = read.ledger.is_member(own);
        let replayed = {
            let account = self.account.borrow();
            let account = account.as_ref().ok_or(ClientError::NoAccount)?;
            match SpaceChain::replay(&read.ledger, &directory)? {
                None => Replayed {
                    chain: None,
                    held: None,
                    member,
                    acquisition: None,
                },
                Some(chain) => {
                    let acquisition =
                        acquire(&chain, &read.ledger, &directory, &account.epoch_keys)?;
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
        read: &SpaceRead,
        directory: &AccountDirectory<'_>,
    ) -> Result<Vec<(String, AccountFloor)>, ClientError> {
        let through = read.ledger.through;
        let mut advance = Vec::new();
        for ledger in &read.accounts {
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
            self.read_space(db, tx, &address).await
        })
        .await?;
        self.take_space(db, space, &address, &read).await
    }

    /// Takes what `read` shows of a space: the chain this device now knows,
    /// its keys, the creations it settles and the reports it withdraws.
    async fn take_space(
        &self,
        db: &Db,
        space: &SpaceAddress,
        address: &ProtocolAddress,
        read: &SpaceRead,
    ) -> Result<Acquired, ClientError> {
        let Replayed {
            chain,
            held,
            member,
            acquisition,
        } = match self.replay_space(read).await {
            Ok(replayed) => replayed,
            Err(error) => return Err(self.forget_account_on(error)),
        };
        if let Some(acquisition) = &acquisition {
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
            self.withdraw_stale(space, state, member);
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
        self.withdraw_stale(space, state, member);
        if known(self) != before {
            let tables = self.tables_of(space);
            let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
            db.refresh_reads_of(&tables)?;
        }
        Ok(Acquired {
            state,
            chain: Some(chain),
            member,
        })
    }

    /// Withdraws the waiting reports of `space` that the acquisition's
    /// `state` no longer gives (protocol §4.3): a space with a root is no
    /// longer `space-pending`, one whose key arrived is neither
    /// `key-pending` nor `key-unavailable`, a member is no longer
    /// `key-unavailable`.
    fn withdraw_stale(&self, space: &SpaceAddress, state: SpaceKeyState, member: bool) {
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
        self.shared.withdraw(space, &gone);
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
    /// [`SettleBackoff`]): until then its entries are kept, it is not read,
    /// and its last error counts as its failure (its rows are unresolved,
    /// not absent). A success ends the wait.
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
        let now = web_time::Instant::now();
        let mut reports = Vec::new();
        let mut held = false;
        let mut failure = None;
        for (space, entries) in by_space {
            let backoff = self.shared.settle_backoff.borrow().get(&space).cloned();
            if let Some(backoff) = backoff.as_ref().filter(|backoff| now < backoff.retry_at) {
                for entry in entries {
                    self.shared.missing(entry);
                }
                failure.get_or_insert_with(|| backoff.error.clone());
                continue;
            }
            match self.settle_space(db, &space, &entries).await {
                Ok((settled, any_held)) => {
                    self.shared.settle_backoff.borrow_mut().remove(&space);
                    reports.extend(settled);
                    held |= any_held;
                }
                Err(error) => {
                    let next = SettleBackoff::after_failure(backoff.as_ref(), error.clone());
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
        for entry in entries {
            let settled = self.shared.keys.borrow_mut().held.settle_missing(
                &address,
                &entry.epoch_id,
                acquired.chain.as_ref(),
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
        Ok((reports, held))
    }

    /// What a binding whose reads it cannot wrap in [`E2eeClient::read`]
    /// does after reads that left rows out for want of a key: acquires the
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

    /// Runs the read `op`, the way a binding wraps it (core hooks note
    /// H3, H4): rows whose key this device lacks are left out and
    /// reported by the hooks; this acquires the keys once, settles the
    /// reports to `key-pending`, `key-unavailable` or `space-pending`, and
    /// runs `op` again if a key was obtained. An encrypted equality
    /// query on a space with no held key (a cold query) is acquired the
    /// same way; when the account is a member that lacks the key, `op`'s
    /// own `key-pending` error is returned and the space is reported. A
    /// client that is offline acquires nothing.
    pub async fn read<T>(
        &self,
        db: &Db,
        mut op: impl AsyncFnMut() -> Result<T, Error>,
    ) -> Result<T, ClientError> {
        self.ensure_open()?;
        self.reconcile_once(db).await?;
        for round in 0..2 {
            // What `op` leaves out is settled here; what earlier reads left
            // (kept for a later settle) stays queued.
            let kept: Vec<Missing> = self.shared.missing.borrow_mut().drain(..).collect();
            let outcome = op().await;
            let missing: Vec<Missing> = self.shared.missing.borrow_mut().drain(..).collect();
            for entry in kept {
                self.shared.missing(entry);
            }
            let online = self.shared.keys.borrow().online;
            match outcome {
                Err(error) => {
                    let Some(E2eeRefusal::NeedsKey { space }) = E2eeRefusal::of(&error) else {
                        self.shared.flush(Vec::new());
                        return Err(error.into());
                    };
                    if round == 1 || !online {
                        self.shared.flush(Vec::new());
                        return Err(error.into());
                    }
                    let acquired = self.acquire_full(db, &space).await?;
                    if acquired.state == SpaceKeyState::Held {
                        continue;
                    }
                    let code = match acquired.state {
                        SpaceKeyState::Incomplete => UnreadableCode::UnsupportedFormat,
                        SpaceKeyState::KeyPending => UnreadableCode::KeyPending,
                        SpaceKeyState::NoRoot if acquired.member => UnreadableCode::SpacePending,
                        _ => UnreadableCode::KeyUnavailable,
                    };
                    let per_space = code != UnreadableCode::KeyUnavailable;
                    let reports = self
                        .tables_of(&space)
                        .into_iter()
                        .filter(|_| per_space)
                        .map(|table| {
                            Report::Unreadable(Unreadable {
                                table,
                                id: None,
                                column: None,
                                code,
                                space: Some(space.clone()),
                            })
                        })
                        .collect();
                    self.shared.flush(reports);
                    return Err(error.into());
                }
                Ok(value) => {
                    if missing.is_empty() || round == 1 || !online {
                        self.shared.flush(Vec::new());
                        return Ok(value);
                    }
                    if !self.settle_missing(db, missing).await? {
                        return Ok(value);
                    }
                }
            }
        }
        unreachable!("the second round returns")
    }

    /// Runs the write or staging call `op`, the way a binding wraps it
    /// (core hooks note H2): when the hooks need a key, this acquires it,
    /// or prepares the space's root (online, after reading the address's
    /// roots; offline without), and runs `op` again, which stages the root
    /// in the same transaction as the write. A space whose creation this
    /// device was refused while the client runs fails `space-pending`
    /// without a root; a member that lacks the key fails with the call's
    /// own `key-pending` error. When the call fails after it staged a
    /// root, that creation did not happen: its key is marked `refused` and
    /// the next write prepares a new root.
    pub async fn write<T>(
        &self,
        db: &Db,
        mut op: impl AsyncFnMut() -> Result<T, Error>,
    ) -> Result<T, ClientError> {
        self.ensure_open()?;
        self.reconcile_once(db).await?;
        let _wrapped = WrappedWrite::enter(&self.shared);
        for round in 0..ATTEMPTS {
            self.begin_attempt(db).await?;
            if round == 0 {
                self.shared.keys.borrow_mut().rotation_tried.clear();
            }
            let outcome = op().await;
            let staged = std::mem::take(&mut self.shared.creations.borrow_mut().attempt);
            let error = match outcome {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            for (_, root_id) in &staged {
                self.abandon_creation(root_id).await?;
            }
            let Some(E2eeRefusal::NeedsKey { space }) = E2eeRefusal::of(&error) else {
                return Err(error.into());
            };
            if round + 1 == ATTEMPTS || !self.make_ready(db, &space).await? {
                return Err(error.into());
            }
        }
        unreachable!("the last round returns")
    }

    /// What a binding whose writes it cannot wrap in [`E2eeClient::write`]
    /// (a synchronous write call of a host language) does after such a
    /// write failed `key-pending` or `space-pending` for `space`: the
    /// steps `write` takes between its rounds. `true` when the write can be
    /// tried again; `false` when it cannot yet (a member waits for a
    /// delivery, or the creation was refused), so the caller waits and
    /// calls again, or gives up. The write tried again after a rotation
    /// this prepared seals under whatever version that left; the write after
    /// it gets its own rotation, as each call of `write` does.
    pub async fn prepare_space(&self, db: &Db, space: &SpaceAddress) -> Result<bool, ClientError> {
        self.ensure_open()?;
        self.reconcile_once(db).await?;
        self.begin_attempt(db).await?;
        self.make_ready(db, space).await
    }

    /// What each attempt of a write starts with, wrapped or not: the
    /// creations whose staged root the database dropped since are
    /// abandoned, as `write` abandons what a failed call staged.
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
        self.shared.creations.borrow_mut().attempt.clear();
        Ok(())
    }

    /// Gets a key ready for a write to `space`: whether the write can be
    /// tried again.
    async fn make_ready(&self, db: &Db, space: &SpaceAddress) -> Result<bool, ClientError> {
        let address = protocol_address(space)?;
        let online = self.shared.keys.borrow().online;
        // A space that needs rotation is rotated before the write; refused,
        // offline or undecided, the write seals under the current version
        // (protocol §3.5).
        let pending = {
            let mut keys = self.shared.keys.borrow_mut();
            let pending = keys.rotation_pending.contains(&address);
            if pending {
                keys.rotation_tried.insert(address.clone());
            }
            pending
        };
        if pending {
            // A rotation that succeeded or was refused clears the mark; one
            // that failed for another reason leaves it for the next write.
            if online {
                let _ = self.rotate(db, space).await;
            }
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

/// A running [`E2eeClient::write`] call: while any runs, the marks of
/// `rotation_tried` last for the call's rounds; when the last returns they
/// are cleared, so a write no call wraps gets its own rotation chance.
struct WrappedWrite<'a>(&'a Shared);

impl<'a> WrappedWrite<'a> {
    fn enter(shared: &'a Shared) -> Self {
        shared.wrapped_writes.set(shared.wrapped_writes.get() + 1);
        Self(shared)
    }
}

impl Drop for WrappedWrite<'_> {
    fn drop(&mut self) {
        let left = self.0.wrapped_writes.get().saturating_sub(1);
        self.0.wrapped_writes.set(left);
        if left == 0 {
            self.0.keys.borrow_mut().rotation_tried.clear();
        }
    }
}

//! Spaces on this device (`E2EE_PROTOCOL.md` §3, §4.2, §4.3, §5.4; core
//! hooks note H2, H3, H6, H11): reading a space's rows at one cut, acquiring
//! its keys (§3.3), settling what reads could not open into the reports of
//! §4.3, and the runtime's wrappers around reads and writes that acquire
//! and retry once, as the hooks ask.
//!
//! The creation of spaces is in [`creation`], the watches in [`watch`].

mod creation;
mod watch;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{Db, Error, ErrorCode};
use jazz::e2ee::{E2eeRefusal, SpaceAddress, Unreadable, UnreadableCode};
use jazz::query::{Query, col, eq, lit};
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::account::PublicAccountLedger;
use jazz_e2ee::protocol::content::{HeldSpace, KeyMark, MissingKey};
use jazz_e2ee::protocol::error::UnsupportedCapability;
use jazz_e2ee::protocol::records::space::SpaceAddress as ProtocolAddress;
use jazz_e2ee::protocol::space::{AccountDirectory, SpaceChain, SpaceLedger, acquire};
use jazz_e2ee::protocol::{Position, ProtocolError};

use crate::account::rows::{self, uuid_cell};
use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::hooks::code;
use crate::state::{Missing, Report};

pub use creation::SpaceCreation;
pub use watch::SpaceWatch;

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
        let (members, _) = self
            .read_rows(
                db,
                tx,
                &skipped,
                of_space(rows::SPACE_MEMBERS).filter(eq(col("accountId"), lit(account.clone()))),
            )
            .await?;
        let (deliveries, _) = self
            .read_rows(
                db,
                tx,
                &skipped,
                of_space(rows::SPACE_DELIVERIES)
                    .filter(eq(col("recipientAccountId"), lit(account))),
            )
            .await?;
        let space_skipped = skipped.get();
        let mut ledger = SpaceLedger::one_account(address.clone(), Position(through), &own);
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
    /// whether it is a member. This account's own history in the read is
    /// checked against the floor first: a server that rolled it back (to
    /// show a removed device as active) is `stale-state`, and nothing of
    /// the read is used.
    pub(crate) async fn replay_space(&self, read: &SpaceRead) -> Result<Replayed, ClientError> {
        let own = &self.shared.account_id;
        let floor = self.floor(own).await;
        let account = self.account.borrow();
        let account = account.as_ref().ok_or(ClientError::NoAccount)?;
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
        let member = read.ledger.is_member(own);
        let Some(chain) = SpaceChain::replay(&read.ledger, &directory)? else {
            return Ok(Replayed {
                chain: None,
                held: None,
                member,
            });
        };
        let acquisition = acquire(&chain, &read.ledger, &directory, &account.epoch_keys)?;
        let held = HeldSpace::from_acquisition(&chain, &acquisition.keys)?;
        Ok(Replayed {
            chain: Some(chain),
            held,
            member,
        })
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
        } = match self.replay_space(read).await {
            Ok(replayed) => replayed,
            Err(error) => return Err(self.forget_account_on(error)),
        };
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
    /// reported.
    async fn settle_missing(&self, db: &Db, missing: Vec<Missing>) -> Result<bool, ClientError> {
        let mut by_space: BTreeMap<SpaceAddress, Vec<Missing>> = BTreeMap::new();
        for entry in missing {
            by_space.entry(entry.space.clone()).or_default().push(entry);
        }
        let mut reports = Vec::new();
        let mut held = false;
        let mut failure = None;
        for (space, entries) in by_space {
            match self.settle_space(db, &space, &entries).await {
                Ok((settled, any_held)) => {
                    reports.extend(settled);
                    held |= any_held;
                }
                Err(error) => {
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
            self.shared.missing.borrow_mut().clear();
            let outcome = op().await;
            let missing: Vec<Missing> = self.shared.missing.borrow_mut().drain(..).collect();
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
        for round in 0..ATTEMPTS {
            // A creation the authority refused since the last write stops
            // the automatic paths before this one stages anything.
            self.settle_pending(db).await?;
            self.shared.creations.borrow_mut().attempt.clear();
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
    /// calls again, or gives up.
    pub async fn prepare_space(&self, db: &Db, space: &SpaceAddress) -> Result<bool, ClientError> {
        self.ensure_open()?;
        self.reconcile_once(db).await?;
        self.settle_pending(db).await?;
        self.shared.creations.borrow_mut().attempt.clear();
        self.make_ready(db, space).await
    }

    /// Gets a key ready for a write to `space`: whether the write can be
    /// tried again.
    async fn make_ready(&self, db: &Db, space: &SpaceAddress) -> Result<bool, ClientError> {
        let address = protocol_address(space)?;
        let online = self.shared.keys.borrow().online;
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

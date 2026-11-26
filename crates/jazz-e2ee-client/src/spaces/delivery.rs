//! Delivering keys to members, rotating a space and removing members
//! (`E2EE_PROTOCOL.md` §3.4, §3.5, §3.8; core hooks note H11).
//!
//! A holder's **pass** over a space finds the members that lack a version it
//! holds and delivers it, in one serializable transaction that read the
//! space's deliveries: the first holder to commit wins, a later one finds
//! its read changed (a phantom), reads again and finds nothing to do. The
//! pass runs at natural moments, never on a timer: when the app asks
//! ([`E2eeClient::deliver_keys`]) and when
//! [`E2eeClient::sync_spaces`] sees a space's rows change. Rotation is by an
//! updater, lazily before an encrypted write to a space that needs it, or
//! at once by [`E2eeClient::rotate`].

use std::collections::BTreeSet;

use jazz::db::{Db, Reason};
use jazz::e2ee::SpaceAddress as DbSpaceAddress;
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::error::RefusalCode;
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::protocol::space::build::{Author, deliver, recover_rotation, rotate};
use jazz_e2ee::protocol::space::members::{
    DELIVERY_BATCH, DeliveryChain, lacking_held, rotation_mark, rotation_recipients,
};

use super::{ATTEMPTS, Replayed, SpaceReadPurpose, SpaceWatch, offline, protocol_address};
use crate::account::has_removed_devices;
use crate::client::{E2eeClient, SharedRandom};
use crate::error::ClientError;
use crate::state::{PendingRotation, Report, db_address};

/// More member accounts than this in a space is beyond the first release's
/// design (protocol §3.8): reported once per space, and the runtime goes on.
pub const MEMBER_LIMIT: usize = 1_000;

/// Batches of deliveries one pass writes at most, a bound that stops a
/// misbehaving server from keeping a pass busy forever.
const MAX_BATCHES: usize = 100;

/// Failed attempts in a row after which the rest of a rotation is left to
/// the pass of any holder (protocol §3.4).
pub(crate) const ROTATION_ATTEMPTS: u32 = 6;

/// The wait before the rest of a rotation is tried again after `failures`
/// failed attempts in a row: 2 s, doubling, at most 60 s.
pub(crate) fn retry_delay(failures: u32) -> std::time::Duration {
    std::time::Duration::from_secs((1u64 << failures.min(6)).min(60))
}

/// Which rotation to make.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Rotation {
    Ordinary,
    Recovery,
}

impl E2eeClient {
    /// The deliveries one transaction writes for `address`: to every member
    /// that lacks a version this device holds, at most
    /// [`DELIVERY_BATCH`], read and committed in `tx`. Also evaluates the
    /// needs-rotation mark from the same read. How many deliveries it wrote.
    async fn deliver_step<'a>(
        &'a self,
        db: &Db,
        tx: OpenTransactionId,
        space: &SpaceAddress,
        previous: &mut Option<DeliveryChain<'a>>,
    ) -> Result<usize, ClientError> {
        let mut read = self
            .read_space_of(db, tx, space, SpaceReadPurpose::Deliver(previous))
            .await?;
        let members = read.ledger.member_accounts().len();
        if members > MEMBER_LIMIT {
            self.shared.event(
                format!("members:{}/{}", space.scope_id, space.identifier),
                Report::SpaceOverMemberLimit {
                    space: db_address(space),
                    members,
                },
            );
        }
        let (
            Replayed {
                chain,
                member,
                acquisition,
                ..
            },
            directory,
        ) = self
            .replay_space(
                &mut read.chain,
                &mut read.acquisition,
                &read.ledger,
                &read.accounts,
            )
            .await?;
        // Only a member delivers (R17). What it delivers is what it holds of
        // this chain, which can be a skipped version without the current one
        // (protocol §3.4).
        if !member {
            if let Some(deps) = self.shared.member_watch.borrow_mut().spaces.get_mut(space) {
                *deps = Default::default();
            }
            return Ok(0);
        }
        // A member whose current generation is above 0 was reset
        // (protocol §2.11): told once per space, account and generation.
        let mut deps = crate::state::WatchedMembers {
            members: read.ledger.member_accounts(),
            ..Default::default()
        };
        for account in &deps.members {
            let state = match directory.current(account) {
                Err(ProtocolError::Unsupported(_) | ProtocolError::Intrinsic(_)) => continue,
                result => result?,
            };
            if let Some(state) = state
                && state.generation > 0
            {
                self.shared.event(
                    format!(
                        "reset:{}/{}:{account}:{}",
                        space.scope_id, space.identifier, state.generation
                    ),
                    Report::AccountReset {
                        account: account.clone(),
                        space: db_address(space),
                        generation: state.generation,
                    },
                );
            }
        }
        let (Some(chain), Some(acquisition)) = (chain, acquisition) else {
            if let Some(watched) = self.shared.member_watch.borrow_mut().spaces.get_mut(space) {
                *watched = deps;
            }
            return Ok(0);
        };
        let eligible = acquisition.has_current(&chain)
            && !self.shared.keys.borrow().rotation_refused.contains(space);
        if eligible {
            if self.shared.member_watch.borrow().covered.contains(space) {
                deps.version =
                    Some(jazz_e2ee::protocol::space::members::CurrentVersion::from_chain(&chain)?);
            }
            #[cfg(any(test, feature = "testing"))]
            {
                self.shared.member_watch.borrow_mut().delivery_mark_checks += 1;
            }
            let marked = rotation_mark(&chain, &read.ledger, &directory)?.needs_rotation();
            deps.holders = jazz_e2ee::protocol::space::members::holders(
                &chain,
                &read.ledger,
                chain.current_depth()?,
            )?
            .intersection(&deps.members)
            .cloned()
            .collect();
            let removals = deps
                .holders
                .iter()
                .any(|account| match directory.current(account) {
                    Ok(Some(state)) => has_removed_devices(&state),
                    Ok(None) => false,
                    Err(_) => true,
                });
            // Only a watched space keeps this answer, since only its watch
            // reports later removals; the first-write and 60 s re-checks in
            // `note_write` also make an unwatched space's write re-read it.
            let watched = self.shared.member_watch.borrow().spaces.contains_key(space);
            self.shared
                .keys
                .borrow_mut()
                .mark_rotation(space, marked, watched.then_some(removals));
        } else {
            self.shared.keys.borrow_mut().rotation_pending.remove(space);
        }
        let lacking = lacking_held(&chain, &read.ledger, &directory, &acquisition.keys)?;
        let suite = self.shared.registry.select(&self.shared.selected)?;
        let writer = self.space_writer(suite);
        let mut written = 0;
        'members: for member in &lacking.lacking {
            if member.account_id == self.shared.account_id {
                continue;
            }
            if !member
                .epoch_ids
                .iter()
                .any(|epoch| acquisition.keys.version(epoch).is_some())
            {
                continue;
            }
            // A member with no sealing key yet waits for a later pass.
            let Some(recipient) =
                directory.recipient(&member.account_id, suite.key_seal.mechanism())?
            else {
                deps.waiting.insert(member.account_id.clone());
                continue;
            };
            for epoch in &member.epoch_ids {
                // Only versions this device holds are its to deliver.
                let Some(version) = acquisition.keys.version(epoch) else {
                    continue;
                };
                if written == DELIVERY_BATCH {
                    break 'members;
                }
                let delivery = deliver(
                    &writer,
                    &chain,
                    &version,
                    &recipient,
                    &mut SharedRandom(&self.shared.random),
                )?;
                self.stage(db, tx, &delivery).await?;
                written += 1;
            }
        }
        if written > 0 {
            self.publish(db, tx).await?;
        }
        if let Some(watched) = self.shared.member_watch.borrow_mut().spaces.get_mut(space) {
            *watched = deps;
        }
        Ok(written)
    }

    /// The pass over `space`.
    pub(crate) async fn deliver_pass(
        &self,
        db: &Db,
        space: &SpaceAddress,
    ) -> Result<usize, ClientError> {
        self.ensure_open()?;
        if !self.shared.keys.borrow().online {
            return Err(offline());
        }
        let mut total = 0;
        let mut previous = None;
        for _ in 0..MAX_BATCHES {
            let outcome = crate::account::serializable(db, ATTEMPTS, async |tx| {
                self.deliver_step(db, tx, space, &mut previous).await
            })
            .await;
            match outcome {
                Ok(0) => return Ok(total),
                Ok(written) => total += written,
                Err(error) => return Err(self.forget_account_on(error)),
            }
        }
        Ok(total)
    }

    /// The pass over every held space when the app starts or asks to serve
    /// new members now. Each pass reads the authority, so it can see members
    /// added to an unwatched space. A failure in one space leaves the others
    /// free to run.
    pub async fn deliver_keys(
        &self,
        db: &Db,
    ) -> Result<Vec<(DbSpaceAddress, Result<usize, ClientError>)>, ClientError> {
        self.deliver_keys_outside_watch(db, None).await
    }

    /// Retained watches catch up on reconnect. Read the authority for held
    /// spaces without a watch-driven pass, including failed passes.
    pub(crate) async fn deliver_keys_outside_watch(
        &self,
        db: &Db,
        watch: Option<&SpaceWatch>,
    ) -> Result<Vec<(DbSpaceAddress, Result<usize, ClientError>)>, ClientError> {
        let held: BTreeSet<SpaceAddress> = self.held_addresses().into_iter().collect();
        let mut results = Vec::new();
        for space in held {
            if watch.is_some_and(|watch| watch.covers_delivery(&space)) {
                continue;
            }
            let mut outcome = self.deliver_pass(db, &space).await;
            if outcome.is_ok()
                && self.holds_refused_key(&space)
                && let Err(error) = self.repair_auto(db, &space).await
            {
                outcome = Err(error);
            }
            results.push((db_address(&space), outcome));
        }
        Ok(results)
    }

    /// After a device removal this device made: the pass over every space
    /// it holds keys of, which marks each one whose current version the
    /// removed device could open (protocol §2.8, §3.5), so that this device's
    /// next write to it rotates first. A space whose pass fails (offline, a
    /// refused or failed read) is marked anyway, unless its rotation was
    /// refused: a needless rotation costs one rotation, but a write sealed
    /// under a version the removed device holds cannot be undone. A later
    /// pass that succeeds clears a mark the space does not need.
    pub(crate) async fn mark_after_device_removal(&self, db: &Db) {
        let held = self.held_addresses();
        for address in held {
            if self.deliver_pass(db, &address).await.is_err() {
                let mut keys = self.shared.keys.borrow_mut();
                if !keys.rotation_refused.contains(&address) {
                    keys.rotation_pending.insert(address);
                }
            }
        }
    }

    /// Whether `space` needs rotation (protocol §3.5): its current version
    /// is held by an account that is no longer a member, or was sealed to an
    /// account epoch a removal superseded. A write to it is then preceded by
    /// a rotation. Evaluated by the pass, so after a removal it is known as
    /// soon as the pass has read the space.
    pub fn needs_rotation(&self, space: &DbSpaceAddress) -> Result<bool, ClientError> {
        let address = protocol_address(space)?;
        Ok(self
            .shared
            .keys
            .borrow()
            .rotation_pending
            .contains(&address))
    }

    /// Rotates `space` now (protocol §3.5): one successor and the first
    /// batch of deliveries in one transaction. The runtime delivers to
    /// the remaining members in the background. Call it right after removing
    /// a member. A refusal by the authority stops automatic rotation of
    /// the space while the client runs; an explicit call always attempts.
    /// Returns the number of deliveries in the first transaction, or zero
    /// if the space has no key yet. Needs the authority.
    pub async fn rotate(&self, db: &Db, space: &DbSpaceAddress) -> Result<usize, ClientError> {
        self.ensure_open()?;
        self.rotate_with(db, &protocol_address(space)?, Rotation::Ordinary, None)
            .await
    }

    /// The recovery rotation (protocol §3.5): a successor whose history
    /// wraps the newest version this device holds, for a space no online
    /// member holds the current version of or whose history is broken. The
    /// application calls it explicitly.
    pub async fn recover_rotation(
        &self,
        db: &Db,
        space: &DbSpaceAddress,
    ) -> Result<usize, ClientError> {
        self.ensure_open()?;
        self.rotate_with(db, &protocol_address(space)?, Rotation::Recovery, None)
            .await
    }

    pub(super) async fn rotate_with(
        &self,
        db: &Db,
        space: &SpaceAddress,
        kind: Rotation,
        deadline: Option<web_time::Instant>,
    ) -> Result<usize, ClientError> {
        self.ensure_open()?;
        if !self.shared.keys.borrow().online {
            return Err(offline());
        }
        let rotated = crate::account::serializable(db, ATTEMPTS, async |tx| {
            self.rotate_step(db, tx, space, kind, deadline).await
        })
        .await;
        let (first, rest) = match rotated {
            Ok(Some(done)) => done,
            Ok(None) => return Ok(0),
            Err(ClientError::Database(error))
                if error.reason == Some(Reason::AuthorizationDenied) =>
            {
                let mut keys = self.shared.keys.borrow_mut();
                if let Some(deps) = self.shared.member_watch.borrow_mut().spaces.get_mut(space) {
                    deps.holders.clear();
                }
                keys.rotation_refused.insert(space.clone());
                keys.rotation_pending.remove(space);
                return Err(ClientError::Database(error));
            }
            Err(error) => return Err(self.forget_account_on(error)),
        };
        if kind == Rotation::Ordinary {
            // The rest is delivered in the background; a newer rotation by
            // this device replaces an older one's rest.
            let mut watch = self.shared.member_watch.borrow_mut();
            if rest.is_empty() {
                watch.pending_deliveries.remove(space);
            } else {
                watch.pending_deliveries.insert(
                    space.clone(),
                    PendingRotation {
                        accounts: rest.clone(),
                        ..Default::default()
                    },
                );
            }
        }
        // Learn and keep the new version before returning. The mark is
        // cleared once the new version is held: until then a write
        // to the space waits for it instead of sealing under the version the
        // removed member holds.
        self.acquire_full(db, space).await?;
        {
            let mut keys = self.shared.keys.borrow_mut();
            keys.rotation_pending.remove(space);
            // The new version is sealed to the members' current account
            // epochs: nothing a removal superseded holds it. As above, only a
            // watched space keeps this answer.
            if self.shared.member_watch.borrow().spaces.contains_key(space) {
                keys.rotation_evaluated.insert(space.clone());
            }
        }
        if kind == Rotation::Ordinary {
            return Ok(first);
        }
        let mut written = first;
        for batch in rest.chunks(DELIVERY_BATCH) {
            let Some(count) = crate::account::serializable(db, ATTEMPTS, async |tx| {
                self.batch_step(db, tx, space, batch).await
            })
            .await?
            else {
                break;
            };
            written += count;
        }
        Ok(written)
    }

    /// The rest of a rotation this device made (protocol §3.5): its
    /// remaining members get the current version from this device in
    /// batches of [`DELIVERY_BATCH`], each whether or not a delivery to it
    /// already exists, since another member's delivery may not open. Each
    /// batch reads only its accounts' histories. A failure keeps the members
    /// not yet served for a retry after [`retry_delay`]; after
    /// [`ROTATION_ATTEMPTS`] failures in a row, or once this device is no
    /// longer a member, they are left to the pass of any holder (§3.4).
    /// How many deliveries it wrote; zero with nothing pending.
    pub(crate) async fn finish_rotation(
        &self,
        db: &Db,
        space: &SpaceAddress,
    ) -> Result<usize, ClientError> {
        self.ensure_open()?;
        let mut written = 0;
        loop {
            let batch: Vec<String> = {
                let mut watch = self.shared.member_watch.borrow_mut();
                let Some(pending) = watch.pending_deliveries.get(space) else {
                    return Ok(written);
                };
                if pending.accounts.is_empty() {
                    watch.pending_deliveries.remove(space);
                    return Ok(written);
                }
                pending
                    .accounts
                    .iter()
                    .take(DELIVERY_BATCH)
                    .cloned()
                    .collect()
            };
            let outcome = if self.shared.keys.borrow().online {
                crate::account::serializable(db, ATTEMPTS, async |tx| {
                    self.batch_step(db, tx, space, &batch).await
                })
                .await
            } else {
                Err(offline())
            };
            let mut watch = self.shared.member_watch.borrow_mut();
            let Some(pending) = watch.pending_deliveries.get_mut(space) else {
                // A rotation in the meantime replaced or ended this one.
                drop(watch);
                return outcome.map(|count| written + count.unwrap_or(0));
            };
            match outcome {
                Ok(Some(count)) => {
                    written += count;
                    // A newer rotation may have replaced the rest meanwhile.
                    if pending.accounts.starts_with(&batch) {
                        pending.accounts.drain(..batch.len());
                    }
                    pending.failures = 0;
                    pending.retry_at = None;
                }
                Ok(None) => {
                    watch.pending_deliveries.remove(space);
                    return Ok(written);
                }
                Err(error) => {
                    if self.shared.keys.borrow().online {
                        pending.failures += 1;
                    }
                    if pending.failures >= ROTATION_ATTEMPTS {
                        watch.pending_deliveries.remove(space);
                    } else {
                        pending.retry_at = Some(self.shared.now() + retry_delay(pending.failures));
                    }
                    drop(watch);
                    return Err(self.forget_account_on(error));
                }
            }
        }
    }

    /// Tells the app `delivery-failed` for `space`, at most once per round:
    /// a sync of the watched spaces and the startup or reconnect sweep that
    /// follows it.
    pub(crate) fn tell_delivery_failed(&self, space: &SpaceAddress, error: &ClientError) {
        if !self
            .shared
            .member_watch
            .borrow_mut()
            .delivery_failures_told
            .insert(space.clone())
        {
            return;
        }
        self.shared.flush(vec![Report::DeliveryFailed {
            space: db_address(space),
            reason: error.to_string(),
        }]);
    }

    /// When the next rest of a rotation is due again after a failure. Only
    /// a retry time still ahead counts: one that passed while offline waits
    /// for the reconnect, and as the runtime's earliest deadline the host
    /// would drop it as past, and with it every later timer.
    pub(crate) fn next_rotation_retry(&self) -> Option<web_time::Instant> {
        let now = self.shared.now();
        self.shared
            .member_watch
            .borrow()
            .pending_deliveries
            .values()
            .filter_map(|pending| pending.retry_at)
            .filter(|at| *at > now)
            .min()
    }

    /// One rotation transaction: the successor and the first batch. Its
    /// first-batch size and the accounts left for the batches that follow,
    /// or `None` when an ordinary rotation found no key to rotate.
    async fn rotate_step(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &SpaceAddress,
        kind: Rotation,
        deadline: Option<web_time::Instant>,
    ) -> Result<Option<(usize, Vec<String>)>, ClientError> {
        let purpose = match kind {
            Rotation::Ordinary => SpaceReadPurpose::Rotate,
            Rotation::Recovery => SpaceReadPurpose::RecoverRotation,
        };
        let mut read = self.read_space_of(db, tx, address, purpose).await?;
        let (
            Replayed {
                chain,
                member,
                acquisition,
                ..
            },
            directory,
        ) = self
            .replay_space(
                &mut read.chain,
                &mut read.acquisition,
                &read.ledger,
                &read.accounts,
            )
            .await?;
        if !member {
            return Err(ProtocolError::Refused(RefusalCode::NotAMember).into());
        }
        // A current-version delivery row that cannot be decoded is skipped:
        // deliveries only order the first batch (holders first) and lead
        // this device to its own key, so a missing one costs at most that
        // order or a `rotation-needs-key`. Refusing instead would let one
        // malformed row stop every rotation of the space, the removal's
        // protection (protocol §3.5).
        if kind == Rotation::Ordinary && chain.is_none() {
            // A skipped row could be the root. A locally held key also
            // contradicts a read that shows membership but no root.
            if read.skipped > 0 || self.holds_live_key(address) {
                return Err(
                    ProtocolError::Refused(RefusalCode::RotationKeyStateUnconfirmed).into(),
                );
            }
            return Ok(None);
        }
        let (Some(chain), Some(acquisition)) = (chain, acquisition) else {
            return Err(ProtocolError::Refused(RefusalCode::RotationNeedsKey).into());
        };
        let suite = self.shared.registry.select(&self.shared.selected)?;
        let recipients = rotation_recipients(
            &chain,
            &read.ledger,
            &directory,
            &self.shared.account_id,
            suite.key_seal.mechanism(),
            &mut SharedRandom(&self.shared.random),
        )?;
        let writer = self.space_writer(suite);
        let records = {
            let account = self.account.borrow();
            let account = account.as_ref().ok_or(ClientError::NoAccount)?;
            let device = self.shared.device()?.clone();
            let author = Author::new(&directory, &self.shared.account_id, &account.key, &device)?;
            let make = match kind {
                Rotation::Ordinary => rotate,
                Rotation::Recovery => recover_rotation,
            };
            make(
                &writer,
                &chain,
                &acquisition.keys,
                &author,
                &recipients.first,
                &mut SharedRandom(&self.shared.random),
            )?
        };
        self.stage(db, tx, &records.successor).await?;
        for delivery in &records.deliveries {
            self.stage(db, tx, delivery).await?;
        }
        if deadline.is_some_and(|end| web_time::Instant::now() >= end) {
            return Ok(None);
        }
        self.publish(db, tx).await?;
        Ok(Some((records.deliveries.len(), recipients.rest)))
    }

    /// One batch of deliveries of the current version to `accounts`, each
    /// whether or not a delivery to it exists (protocol §3.5), read and
    /// committed in a transaction of its own that re-reads on a conflict.
    async fn batch_step(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &SpaceAddress,
        accounts: &[String],
    ) -> Result<Option<usize>, ClientError> {
        // The batch reads only this device's own deliveries, to acquire the
        // key: a row it cannot decode is skipped like any unreadable row.
        let mut read = self
            .read_space_of(db, tx, address, SpaceReadPurpose::RotationBatch(accounts))
            .await?;
        let (
            Replayed {
                chain,
                member,
                acquisition,
                ..
            },
            directory,
        ) = self
            .replay_space(
                &mut read.chain,
                &mut read.acquisition,
                &read.ledger,
                &read.accounts,
            )
            .await?;
        // A device no longer a member, or without the key, leaves the rest
        // to the pass of any holder (§3.4).
        if !member {
            return Ok(None);
        }
        let (Some(chain), Some(acquisition)) = (chain, acquisition) else {
            return Ok(None);
        };
        let suite = self.shared.registry.select(&self.shared.selected)?;
        let writer = self.space_writer(suite);
        let current = chain.current()?.epoch_id.clone();
        let Some(version) = acquisition.keys.version(&current) else {
            return Ok(None);
        };
        let members = read.ledger.member_accounts();
        let mut written = 0;
        for account in accounts {
            // A member removed or without a sealing key since is skipped.
            if !members.contains(account) {
                continue;
            }
            let Some(recipient) = directory.recipient(account, suite.key_seal.mechanism())? else {
                continue;
            };
            let delivery = deliver(
                &writer,
                &chain,
                &version,
                &recipient,
                &mut SharedRandom(&self.shared.random),
            )?;
            self.stage(db, tx, &delivery).await?;
            written += 1;
        }
        if written > 0 {
            self.publish(db, tx).await?;
        }
        Ok(Some(written))
    }
}

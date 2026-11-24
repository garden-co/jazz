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
use jazz::e2ee::SpaceAddress;
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::error::RefusalCode;
use jazz_e2ee::protocol::records::space::SpaceAddress as ProtocolAddress;
use jazz_e2ee::protocol::space::AccountDirectory;
use jazz_e2ee::protocol::space::build::{Author, SpaceWriter, deliver, recover_rotation, rotate};
use jazz_e2ee::protocol::space::members::{
    DELIVERY_BATCH, lacking_held, rotation_mark, rotation_recipients,
};

use super::{ATTEMPTS, Replayed, SpaceReadPurpose, offline, protocol_address};
use crate::account::has_removed_devices;
use crate::client::{E2eeClient, SharedRandom};
use crate::error::ClientError;
use crate::state::{Report, db_address};

/// More member accounts than this in a space is beyond the first release's
/// design (protocol §3.8): reported once per space, and the runtime goes on.
pub const MEMBER_LIMIT: usize = 1_000;

/// Batches of deliveries one pass writes at most, a bound that stops a
/// misbehaving server from keeping a pass busy forever.
const MAX_BATCHES: usize = 100;

/// Which rotation to make.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rotation {
    Ordinary,
    Recovery,
}

impl E2eeClient {
    /// The deliveries one transaction writes for `address`: to every member
    /// that lacks a version this device holds, at most
    /// [`DELIVERY_BATCH`], read and committed in `tx`. Also evaluates the
    /// needs-rotation mark from the same read. How many deliveries it wrote.
    async fn deliver_step(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
        space: &SpaceAddress,
    ) -> Result<usize, ClientError> {
        let mut read = self
            .read_space_of(db, tx, address, SpaceReadPurpose::Deliver)
            .await?;
        let members = read.ledger.member_accounts().len();
        if members > MEMBER_LIMIT {
            self.shared.event(
                format!("members:{}/{}", space.scope_id, space.identifier),
                Report::SpaceOverMemberLimit {
                    space: space.clone(),
                    members,
                },
            );
        }
        let Replayed {
            chain,
            member,
            acquisition,
            ..
        } = self.replay_space(&mut read).await?;
        // Only a member delivers (R17). What it delivers is what it holds of
        // this chain, which can be a skipped version without the current one
        // (protocol §3.4).
        if !member {
            if let Some(deps) = self.shared.member_watch.borrow_mut().spaces.get_mut(space) {
                *deps = Default::default();
            }
            return Ok(0);
        }
        let directory = AccountDirectory::new(
            &read.accounts,
            &self.shared.domain,
            &self.shared.registry,
            read.ledger.through,
        )?;
        // A member whose current generation is above 0 was reset
        // (protocol §2.11): told once per space, account and generation.
        let mut deps = crate::state::WatchedMembers {
            members: read.ledger.member_accounts(),
            ..Default::default()
        };
        for account in &deps.members {
            if let Ok(Some(state)) = directory.current(account)
                && state.generation > 0
            {
                self.shared.event(
                    format!(
                        "reset:{}/{}:{account}:{}",
                        space.scope_id, space.identifier, state.generation
                    ),
                    Report::AccountReset {
                        account: account.clone(),
                        space: space.clone(),
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
            && !self.shared.keys.borrow().rotation_refused.contains(address);
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
            self.shared.keys.borrow_mut().mark_rotation(
                address,
                marked,
                watched.then_some(removals),
            );
        } else {
            self.shared
                .keys
                .borrow_mut()
                .rotation_pending
                .remove(address);
        }
        let lacking = lacking_held(&chain, &read.ledger, &directory, &acquisition.keys)?;
        let suite = self.shared.registry.select(&self.shared.selected)?;
        let writer = SpaceWriter {
            suite,
            registry: &self.shared.registry,
            domain: &self.shared.domain,
            account_id: &self.shared.account_id,
        };
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
        let address = protocol_address(space)?;
        if !self.shared.keys.borrow().online {
            return Err(offline());
        }
        let mut total = 0;
        for _ in 0..MAX_BATCHES {
            let outcome = crate::account::serializable(db, ATTEMPTS, async |tx| {
                self.deliver_step(db, tx, &address, space).await
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

    /// The pass over every space this device holds keys of: what the host
    /// calls at its natural moments (when the app starts or reconnects, and
    /// when it wants new members served now). Each space's pass reads the
    /// authority, so a member added to a space nothing was watching is
    /// seen. One space's failure does not stop the others.
    pub async fn deliver_keys(
        &self,
        db: &Db,
    ) -> Result<Vec<(SpaceAddress, Result<usize, ClientError>)>, ClientError> {
        let held: BTreeSet<SpaceAddress> = self
            .shared
            .keys
            .borrow()
            .held
            .spaces()
            .iter()
            .map(|space| db_address(space.address()))
            .collect();
        let mut results = Vec::new();
        for space in held {
            let mut outcome = self.deliver_pass(db, &space).await;
            if outcome.is_ok()
                && self.holds_refused_key(&space)
                && let Err(error) = self.repair_auto(db, &space).await
            {
                outcome = Err(error);
            }
            results.push((space, outcome));
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
        let held: Vec<_> = self
            .shared
            .keys
            .borrow()
            .held
            .spaces()
            .iter()
            .map(|space| space.address().clone())
            .collect();
        for address in held {
            if self.deliver_pass(db, &db_address(&address)).await.is_err() {
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
    pub fn needs_rotation(&self, space: &SpaceAddress) -> Result<bool, ClientError> {
        let address = protocol_address(space)?;
        Ok(self
            .shared
            .keys
            .borrow()
            .rotation_pending
            .contains(&address))
    }

    /// Rotates `space` now (protocol §3.5): one successor and the first
    /// batch of deliveries in one transaction, the rest of the members in
    /// batches of up to 100 right after, whether or not a delivery to them
    /// exists. Call it right after removing a member. A refusal by the
    /// authority stops the automatic rotation of the space while the client
    /// runs; an explicit call always attempts. How many deliveries were
    /// written, or zero if the space has no key yet. Needs the authority.
    pub async fn rotate(&self, db: &Db, space: &SpaceAddress) -> Result<usize, ClientError> {
        self.rotate_before(db, space, None).await
    }

    pub(super) async fn rotate_before(
        &self,
        db: &Db,
        space: &SpaceAddress,
        deadline: Option<web_time::Instant>,
    ) -> Result<usize, ClientError> {
        self.rotate_with(db, space, Rotation::Ordinary, deadline)
            .await
    }

    /// The recovery rotation (protocol §3.5): a successor whose history
    /// wraps the newest version this device holds, for a space no online
    /// member holds the current version of or whose history is broken. The
    /// application calls it explicitly.
    pub async fn recover_rotation(
        &self,
        db: &Db,
        space: &SpaceAddress,
    ) -> Result<usize, ClientError> {
        self.rotate_with(db, space, Rotation::Recovery, None).await
    }

    async fn rotate_with(
        &self,
        db: &Db,
        space: &SpaceAddress,
        kind: Rotation,
        deadline: Option<web_time::Instant>,
    ) -> Result<usize, ClientError> {
        self.ensure_open()?;
        let address = protocol_address(space)?;
        if !self.shared.keys.borrow().online {
            return Err(offline());
        }
        let rotated = crate::account::serializable(db, ATTEMPTS, async |tx| {
            self.rotate_step(db, tx, &address, kind, deadline).await
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
                keys.rotation_refused.insert(address.clone());
                keys.rotation_pending.remove(&address);
                return Err(ClientError::Database(error));
            }
            Err(error) => return Err(self.forget_account_on(error)),
        };
        // Learn and keep the new version, then deliver it to the rest. The
        // mark is cleared once the new version is held: until then a write
        // to the space waits for it instead of sealing under the version the
        // removed member holds.
        self.acquire_full(db, space).await?;
        {
            let mut keys = self.shared.keys.borrow_mut();
            keys.rotation_pending.remove(&address);
            // The new version is sealed to the members' current account
            // epochs: nothing a removal superseded holds it. As above, only a
            // watched space keeps this answer.
            if self.shared.member_watch.borrow().spaces.contains_key(space) {
                keys.rotation_evaluated.insert(address.clone());
            }
        }
        let mut written = first;
        for batch in rest.chunks(DELIVERY_BATCH) {
            written += self.deliver_batch(db, &address, batch).await?;
        }
        Ok(written)
    }

    /// One rotation transaction: the successor and the first batch. Its
    /// first-batch size and the accounts left for the batches that follow,
    /// or `None` when an ordinary rotation found no key to rotate.
    async fn rotate_step(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
        kind: Rotation,
        deadline: Option<web_time::Instant>,
    ) -> Result<Option<(usize, Vec<String>)>, ClientError> {
        let mut read = self
            .read_space_of(db, tx, address, SpaceReadPurpose::Rotate)
            .await?;
        let Replayed {
            chain,
            member,
            acquisition,
            ..
        } = self.replay_space(&mut read).await?;
        if !member {
            return Err(ProtocolError::Refused(RefusalCode::NotAMember).into());
        }
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
        let directory = AccountDirectory::new(
            &read.accounts,
            &self.shared.domain,
            &self.shared.registry,
            read.ledger.through,
        )?;
        let suite = self.shared.registry.select(&self.shared.selected)?;
        let recipients = rotation_recipients(
            &chain,
            &read.ledger,
            &directory,
            &self.shared.account_id,
            suite.key_seal.mechanism(),
            &mut SharedRandom(&self.shared.random),
        )?;
        let writer = SpaceWriter {
            suite,
            registry: &self.shared.registry,
            domain: &self.shared.domain,
            account_id: &self.shared.account_id,
        };
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
    async fn deliver_batch(
        &self,
        db: &Db,
        address: &ProtocolAddress,
        accounts: &[String],
    ) -> Result<usize, ClientError> {
        crate::account::serializable(db, ATTEMPTS, async |tx| {
            self.batch_step(db, tx, address, accounts).await
        })
        .await
    }

    async fn batch_step(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        address: &ProtocolAddress,
        accounts: &[String],
    ) -> Result<usize, ClientError> {
        let mut read = self
            .read_space_of(db, tx, address, SpaceReadPurpose::Rotate)
            .await?;
        let Replayed {
            chain, acquisition, ..
        } = self.replay_space(&mut read).await?;
        let (Some(chain), Some(acquisition)) = (chain, acquisition) else {
            return Ok(0);
        };
        let directory = AccountDirectory::new(
            &read.accounts,
            &self.shared.domain,
            &self.shared.registry,
            read.ledger.through,
        )?;
        let suite = self.shared.registry.select(&self.shared.selected)?;
        let writer = SpaceWriter {
            suite,
            registry: &self.shared.registry,
            domain: &self.shared.domain,
            account_id: &self.shared.account_id,
        };
        let current = chain.current()?.epoch_id.clone();
        let Some(version) = acquisition.keys.version(&current) else {
            return Ok(0);
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
        Ok(written)
    }
}

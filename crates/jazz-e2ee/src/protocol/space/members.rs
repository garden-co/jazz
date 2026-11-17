//! Who holds and who lacks a version, and the needs-rotation mark
//! (`E2EE_PROTOCOL.md` §1.1 "Key holder", §3.4 "Who lacks a key", §3.5
//! "Needs rotation").
//!
//! Everything here is derived from envelope metadata of accepted records:
//! which account and account epoch each `accountEnvelope` and delivery
//! names. It is conservative evidence, not proof of possession: the server
//! checks no envelope, so an accepted delivery whose envelope does not open
//! still counts.

use std::collections::{BTreeMap, BTreeSet};

use crate::envelope::Mechanism;
use crate::random::RandomSource;

use super::super::account::{AccountReplay, AccountState};
use super::super::error::{ProtocolError, ProtocolResult};
use super::super::position::Position;
use super::super::records::account::AccountAction;
use super::chain::SpaceChain;
use super::ledger::{AccountDirectory, Recipient, SpaceLedger};

/// Which record an envelope of a version belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EnvelopeSource {
    /// The `accountEnvelope` of the space's root.
    Root,
    /// The `accountEnvelope` of a successor.
    Successor,
    /// A delivery.
    Delivery,
}

/// One envelope of a version, as its record names it: the account and the
/// account epoch whose sealing key it was sealed to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VersionEnvelope {
    pub account_id: String,
    pub account_epoch_id: String,
    pub source: EnvelopeSource,
    /// The root, successor or delivery row ID.
    pub record_id: String,
    pub position: Position,
}

/// The envelopes of the version at `depth`: the `accountEnvelope` of its
/// own record, then the deliveries of it, by position then ID. With
/// `after_root`, deliveries at or before the root's position are left out,
/// as acquisition skips them (§3.3).
pub fn envelopes(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    depth: usize,
    after_root: bool,
) -> ProtocolResult<Vec<VersionEnvelope>> {
    if *chain.address() != ledger.address {
        return Err(ProtocolError::InconsistentEvidence);
    }
    let version = chain
        .version(depth)
        .ok_or(ProtocolError::InvalidRecord("depth beyond the chain"))?;
    let (writer, writer_epoch) = version.writer(&chain.root.row);
    let (source, record_id) = match &version.successor {
        None => (EnvelopeSource::Root, chain.root.row.id.clone()),
        Some(successor) => (EnvelopeSource::Successor, successor.row.id.clone()),
    };
    let mut found = vec![VersionEnvelope {
        account_id: writer.to_owned(),
        account_epoch_id: writer_epoch.to_owned(),
        source,
        record_id,
        position: version.position,
    }];
    for delivery in ledger.deliveries_of(&version.epoch_id) {
        if after_root && delivery.position <= chain.root.position {
            continue;
        }
        found.push(VersionEnvelope {
            account_id: delivery.row.recipient_account_id.clone(),
            account_epoch_id: delivery.row.recipient_account_epoch_id.clone(),
            source: EnvelopeSource::Delivery,
            record_id: delivery.row.id.clone(),
            position: delivery.position,
        });
    }
    Ok(found)
}

/// The key holders of the version at `depth` (§1.1): the account that wrote
/// its root or successor, and every account a delivery of it was accepted
/// to.
pub fn holders(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    depth: usize,
) -> ProtocolResult<BTreeSet<String>> {
    Ok(envelopes(chain, ledger, depth, false)?
        .into_iter()
        .map(|envelope| envelope.account_id)
        .collect())
}

/// A member that lacks versions it is concerned with (§3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lacking {
    pub account_id: String,
    /// The member's current generation, from its public roots; `None` when
    /// it has no counting root yet (nobody can seal to it).
    pub generation: Option<u64>,
    /// The versions it lacks, by depth, current last.
    pub epoch_ids: Vec<String>,
}

/// Every member that lacks a version concerned by delivery (§3.4): the
/// current version and the skipped versions. A member **lacks** a version
/// when no accepted envelope of it (a delivery to it, or the
/// `accountEnvelope` of the version's record that it wrote) names an
/// account epoch of its current generation. A reset member therefore lacks
/// every version until someone delivers again; a device-removal account
/// rotation changes nothing here. Needs the public ledger of every member.
pub fn lacking(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
) -> ProtocolResult<Vec<Lacking>> {
    let concerned = chain.concerned()?;
    let mut by_depth = BTreeMap::new();
    for depth in &concerned {
        by_depth.insert(*depth, envelopes(chain, ledger, *depth, true)?);
    }
    let mut out = Vec::new();
    for account in ledger.member_accounts() {
        let state = accounts.current(&account)?;
        let generation_epochs: BTreeSet<&str> = state
            .iter()
            .flat_map(|state| state.epochs.iter().map(|epoch| epoch.epoch_id.as_str()))
            .collect();
        let epoch_ids: Vec<String> = concerned
            .iter()
            .filter(|depth| {
                !by_depth[*depth].iter().any(|envelope| {
                    envelope.account_id == account
                        && generation_epochs.contains(envelope.account_epoch_id.as_str())
                })
            })
            .map(|depth| chain.versions[*depth].epoch_id.clone())
            .collect();
        if !epoch_ids.is_empty() {
            out.push(Lacking {
                account_id: account,
                generation: state.map(|state| state.generation),
                epoch_ids,
            });
        }
    }
    Ok(out)
}

/// An envelope of the current version sealed to an account epoch that a
/// later `remove-device` account successor superseded (§3.5): the removed
/// device can open it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersededEnvelope {
    pub envelope: VersionEnvelope,
    /// The first `remove-device` successor after the envelope's account
    /// epoch in that account's chain.
    pub successor_id: String,
    pub removed_device_id: String,
}

/// The needs-rotation mark of a space (§3.5), with the evidence for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationMark {
    /// The current version's epoch.
    pub epoch_id: String,
    /// Holders of the current version that are no longer members.
    pub former_members: Vec<String>,
    /// Envelopes of the current version a device removal superseded.
    pub superseded: Vec<SupersededEnvelope>,
}

impl RotationMark {
    /// Whether the space needs rotation.
    pub fn needs_rotation(&self) -> bool {
        !self.former_members.is_empty() || !self.superseded.is_empty()
    }
}

/// The needs-rotation mark (§3.5): the current version is held (§1.1) by an
/// account that is no longer a member, or one of its envelopes was sealed
/// to an account epoch that a later `remove-device` successor superseded,
/// anywhere after that epoch in the account's chain, even with other
/// successors in between. Derived from tables every member can read;
/// nobody writes it. Needs the public ledger of every account an envelope
/// of the current version names.
pub fn rotation_mark(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
) -> ProtocolResult<RotationMark> {
    let depth = chain.current_depth()?;
    let members = ledger.member_accounts();
    let found = envelopes(chain, ledger, depth, false)?;
    let former_members: BTreeSet<String> = found
        .iter()
        .filter(|envelope| !members.contains(&envelope.account_id))
        .map(|envelope| envelope.account_id.clone())
        .collect();
    let mut superseded = Vec::new();
    for envelope in found {
        let replay = accounts.replay(&envelope.account_id)?;
        if let Some((successor_id, removed_device_id)) =
            removal_after(replay, &envelope.account_epoch_id)?
        {
            superseded.push(SupersededEnvelope {
                envelope,
                successor_id,
                removed_device_id,
            });
        }
    }
    Ok(RotationMark {
        epoch_id: chain.versions[depth].epoch_id.clone(),
        former_members: former_members.into_iter().collect(),
        superseded,
    })
}

/// The first accepted `remove-device` successor after `epoch_id` in the
/// account's chain that contains it: the current generation's chain, or,
/// for an epoch of an earlier generation, that generation's chain as it
/// stood when the next generation began (§2.11). `None` when no removal
/// follows the epoch, or the epoch is in none of the account's chains.
fn removal_after(
    replay: &AccountReplay<'_>,
    epoch_id: &str,
) -> ProtocolResult<Option<(String, String)>> {
    let through = replay.ledger().through;
    let Some(current) = replay.as_of(Position(through.0.saturating_add(1)))? else {
        return Ok(None);
    };
    if let Some(found) = removal_in(&current, epoch_id) {
        return Ok(found);
    }
    // Each earlier generation's chain, as of the canonical root of the
    // generation after it.
    for next in current.roots.iter().skip(1) {
        let Some(earlier) = replay.as_of(next.position)? else {
            continue;
        };
        if let Some(found) = removal_in(&earlier, epoch_id) {
            return Ok(found);
        }
    }
    Ok(None)
}

/// `Some(removal)` when `state`'s chain contains `epoch_id`, with the first
/// `remove-device` successor after it, if any.
#[allow(clippy::option_option)]
fn removal_in(state: &AccountState, epoch_id: &str) -> Option<Option<(String, String)>> {
    let index = state
        .epochs
        .iter()
        .position(|epoch| epoch.epoch_id == epoch_id)?;
    Some(state.epochs[index + 1..].iter().find_map(|epoch| {
        let successor = &epoch.successor.as_ref()?.row;
        (successor.action == AccountAction::RemoveDevice)
            .then(|| {
                successor
                    .removed_device_id
                    .clone()
                    .map(|device| (successor.id.clone(), device))
            })
            .flatten()
    }))
}

/// At most this many deliveries go in a rotation's own transaction, and in
/// each later delivery transaction (§3.5).
pub const DELIVERY_BATCH: usize = 100;

/// The members a rotation delivers its new version to (§3.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationRecipients {
    /// At most [`DELIVERY_BATCH`] members, delivered to in the rotation's
    /// own transaction: first members that held the version being replaced,
    /// then the others, each group in random order.
    pub first: Vec<Recipient>,
    /// The remaining members with a valid sealing key, in the same order,
    /// delivered to right after the rotation is accepted, in batches of up
    /// to [`DELIVERY_BATCH`].
    pub rest: Vec<Recipient>,
    /// Members without a valid sealing key of the selected mechanism (or
    /// without a counting root): they receive the version later from any
    /// holder.
    pub waiting: Vec<String>,
}

/// Chooses the recipients of a rotation by `rotator` (§3.5): every current
/// member other than the rotator's account that has a valid sealing key of
/// `mechanism` (§2.12), the members that held the current version first.
/// Needs the public ledger of every member.
pub fn rotation_recipients(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
    rotator: &str,
    mechanism: Mechanism<'_>,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RotationRecipients> {
    let depth = chain.current_depth()?;
    let held = holders(chain, ledger, depth)?;
    let mut holding = Vec::new();
    let mut others = Vec::new();
    let mut waiting = Vec::new();
    for account in ledger.member_accounts() {
        if account == rotator {
            continue;
        }
        match accounts.recipient(&account, mechanism)? {
            None => waiting.push(account),
            Some(recipient) if held.contains(&account) => holding.push(recipient),
            Some(recipient) => others.push(recipient),
        }
    }
    shuffle(&mut holding, rng)?;
    shuffle(&mut others, rng)?;
    holding.extend(others);
    let rest = holding.split_off(holding.len().min(DELIVERY_BATCH));
    Ok(RotationRecipients {
        first: holding,
        rest,
        waiting,
    })
}

/// Fisher–Yates over `rng`. The order only spreads load; it protects
/// nothing.
fn shuffle<T>(items: &mut [T], rng: &mut dyn RandomSource) -> ProtocolResult<()> {
    for index in (1..items.len()).rev() {
        let mut bytes = [0u8; 8];
        rng.fill(&mut bytes)?;
        let pick = (u64::from_le_bytes(bytes) % (index as u64 + 1)) as usize;
        items.swap(index, pick);
    }
    Ok(())
}

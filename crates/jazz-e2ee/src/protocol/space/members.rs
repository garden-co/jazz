//! Who holds and who lacks a version, and the needs-rotation mark
//! (`E2EE_PROTOCOL.md` §1.1 "Key holder", §3.4 "Who lacks a key", §3.5
//! "Needs rotation").
//!
//! Everything here is derived from envelope metadata of accepted records:
//! which account and account epoch each `accountEnvelope` and delivery
//! names. It is conservative evidence, not proof of possession: the server
//! checks no envelope, so an accepted delivery whose envelope does not open
//! still counts.
//!
//! Every function here needs a read of the whole space
//! ([`SpaceLedger::whole_space`]) and refuses a one-account read as
//! [`ProtocolError::InconsistentEvidence`]. A member whose account history
//! needs a capability this client lacks is skipped and reported as
//! unsupported, never as invalid, and the rest of the space is served
//! (§3.4, "A member this client cannot evaluate"). A member whose account
//! rows the caller left out of the read is still
//! [`ProtocolError::InconsistentEvidence`], and a provider failure still
//! fails the operation.

use std::collections::{BTreeMap, BTreeSet};

use crate::envelope::Mechanism;
use crate::random::RandomSource;

use super::super::account::{AccountReplay, AccountState};
use super::super::error::{ProtocolError, ProtocolResult, UnsupportedCapability};
use super::super::position::Position;
use super::super::records::account::AccountAction;
use super::chain::SpaceChain;
use super::ledger::{AccountDirectory, Recipient, SpaceLedger};

/// A member, or an account an envelope names, whose public account history
/// needs a capability this client lacks (§3.4, §7): skipped, and reported
/// as unsupported, never as invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedMember {
    pub account_id: String,
    pub capability: UnsupportedCapability,
}

/// Runs `evaluate` for one account; an unsupported capability becomes the
/// account's [`UnsupportedMember`] report, and every other failure fails
/// the operation. An account whose ledger the caller left out fails with
/// [`ProtocolError::InconsistentEvidence`] before anything is evaluated.
fn evaluate<T>(
    accounts: &AccountDirectory<'_>,
    account_id: &str,
    evaluate: impl FnOnce() -> ProtocolResult<T>,
) -> ProtocolResult<Result<T, UnsupportedMember>> {
    if !accounts.has_ledger(account_id) {
        return Err(ProtocolError::InconsistentEvidence);
    }
    match evaluate() {
        Ok(value) => Ok(Ok(value)),
        Err(ProtocolError::Unsupported(capability)) => Ok(Err(UnsupportedMember {
            account_id: account_id.to_owned(),
            capability,
        })),
        Err(error) => Err(error),
    }
}

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
/// `ledger` must be the read of the whole space `chain` was replayed from.
pub fn envelopes(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    depth: usize,
    after_root: bool,
) -> ProtocolResult<Vec<VersionEnvelope>> {
    check_whole_read(chain, ledger, None)?;
    version_envelopes(chain, ledger, depth, after_root)
}

/// The chain's own read check, and a read of the whole space.
fn check_whole_read(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: Option<&AccountDirectory<'_>>,
) -> ProtocolResult<()> {
    chain.check_read(ledger, accounts)?;
    ledger.require_whole_space()
}

/// [`envelopes`], over a read already checked.
fn version_envelopes(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    depth: usize,
    after_root: bool,
) -> ProtocolResult<Vec<VersionEnvelope>> {
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
/// to. A delivery at or before the root's position counts here (§3.4).
/// `ledger` must be the read of the whole space `chain` was replayed from.
pub fn holders(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    depth: usize,
) -> ProtocolResult<BTreeSet<String>> {
    check_whole_read(chain, ledger, None)?;
    version_holders(chain, ledger, depth)
}

/// [`holders`], over a read already checked.
fn version_holders(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    depth: usize,
) -> ProtocolResult<BTreeSet<String>> {
    Ok(version_envelopes(chain, ledger, depth, false)?
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

/// Who lacks a version (§3.4), and the members this client cannot
/// evaluate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LackingMembers {
    /// Members that lack a version concerned, by account ID.
    pub lacking: Vec<Lacking>,
    /// Members whose account history needs a capability this client lacks:
    /// skipped, reported as unsupported.
    pub unsupported: Vec<UnsupportedMember>,
}

/// Every member that lacks a version concerned by delivery (§3.4): the
/// current version and the skipped versions. A member **lacks** a version
/// when no accepted envelope of it (a delivery to it, or the
/// `accountEnvelope` of the version's record that it wrote) names an
/// account epoch of its current generation. A delivery at or before the
/// root's position does not count, as in acquiring (§3.3). A reset member
/// therefore lacks every version until someone delivers again; a
/// device-removal account rotation changes nothing here. A member whose
/// account history needs a capability this client lacks is reported in
/// [`LackingMembers::unsupported`] and the others are still evaluated.
/// Needs the public ledger of every member, from the read of the whole
/// space `chain` was replayed from.
pub fn lacking(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
) -> ProtocolResult<LackingMembers> {
    check_whole_read(chain, ledger, Some(accounts))?;
    let concerned = chain.concerned()?;
    let mut by_depth = BTreeMap::new();
    for depth in &concerned {
        by_depth.insert(*depth, version_envelopes(chain, ledger, *depth, true)?);
    }
    let mut out = Vec::new();
    let mut unsupported = Vec::new();
    for account in ledger.member_accounts() {
        let state = match evaluate(accounts, &account, || accounts.current(&account))? {
            Ok(state) => state,
            Err(member) => {
                unsupported.push(member);
                continue;
            }
        };
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
    Ok(LackingMembers {
        lacking: out,
        unsupported,
    })
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
    /// Accounts an envelope of the current version names whose history
    /// needs a capability this client lacks. Reported, and otherwise
    /// ignored: they never set the mark here, or a client that cannot read
    /// them would rotate again each time newer clients deliver to them.
    /// Clients that can evaluate them still mark the space.
    pub unsupported: Vec<UnsupportedMember>,
}

impl RotationMark {
    /// Whether the space needs rotation. [`RotationMark::unsupported`] does
    /// not count.
    pub fn needs_rotation(&self) -> bool {
        !self.former_members.is_empty() || !self.superseded.is_empty()
    }
}

/// The needs-rotation mark (§3.5): the current version is held (§1.1) by an
/// account that is no longer a member, or one of its envelopes was sealed
/// to an account epoch that a later `remove-device` successor superseded,
/// anywhere after that epoch in the account's chain, even with other
/// successors in between. Derived from tables every member can read;
/// nobody writes it. An account whose history needs a capability this
/// client lacks is reported in [`RotationMark::unsupported`] and does not
/// set the mark. Needs the public ledger of every account an envelope of
/// the current version names, from the read of the whole space `chain` was
/// replayed from.
pub fn rotation_mark(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
) -> ProtocolResult<RotationMark> {
    check_whole_read(chain, ledger, Some(accounts))?;
    let depth = chain.current_depth()?;
    let members = ledger.member_accounts();
    let found = version_envelopes(chain, ledger, depth, false)?;
    let former_members: BTreeSet<String> = found
        .iter()
        .filter(|envelope| !members.contains(&envelope.account_id))
        .map(|envelope| envelope.account_id.clone())
        .collect();
    let mut superseded = Vec::new();
    let mut unsupported: Vec<UnsupportedMember> = Vec::new();
    for envelope in found {
        let account_id = envelope.account_id.clone();
        let removal = evaluate(accounts, &account_id, || {
            removal_after(accounts.replay(&account_id)?, &envelope.account_epoch_id)
        })?;
        let removal = match removal {
            Ok(removal) => removal,
            Err(member) => {
                if !unsupported.contains(&member) {
                    unsupported.push(member);
                }
                continue;
            }
        };
        if let Some((successor_id, removed_device_id)) = removal {
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
        unsupported,
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
    /// to [`DELIVERY_BATCH`]. Each batch is built from the read it is
    /// delivered in, so only the accounts are kept here: the caller takes
    /// each recipient from that read
    /// ([`AccountDirectory::recipient`](super::ledger::AccountDirectory::recipient)).
    pub rest: Vec<String>,
    /// Members without a valid sealing key of the selected mechanism (or
    /// without a counting root): they receive the version later from any
    /// holder.
    pub waiting: Vec<String>,
    /// Members whose account history needs a capability this client lacks:
    /// skipped and reported as unsupported; a holder that can read them
    /// delivers to them later.
    pub unsupported: Vec<UnsupportedMember>,
}

/// Chooses the recipients of a rotation by `rotator` (§3.5): every current
/// member other than the rotator's account that has a valid sealing key of
/// `mechanism` (§2.12), the members that held the current version first.
/// A member whose account history needs a capability this client lacks is
/// reported in [`RotationRecipients::unsupported`]. Needs the public ledger
/// of every member, from the read of the whole space `chain` was replayed
/// from.
pub fn rotation_recipients(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
    rotator: &str,
    mechanism: Mechanism<'_>,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RotationRecipients> {
    check_whole_read(chain, ledger, Some(accounts))?;
    let depth = chain.current_depth()?;
    let held = version_holders(chain, ledger, depth)?;
    let mut holding = Vec::new();
    let mut others = Vec::new();
    let mut waiting = Vec::new();
    let mut unsupported = Vec::new();
    for account in ledger.member_accounts() {
        if account == rotator {
            continue;
        }
        match evaluate(accounts, &account, || {
            accounts.recipient(&account, mechanism)
        })? {
            Err(member) => unsupported.push(member),
            Ok(None) => waiting.push(account),
            Ok(Some(recipient)) if held.contains(&account) => holding.push(recipient),
            Ok(Some(recipient)) => others.push(recipient),
        }
    }
    shuffle(&mut holding, rng)?;
    shuffle(&mut others, rng)?;
    holding.extend(others);
    let rest = holding
        .split_off(holding.len().min(DELIVERY_BATCH))
        .into_iter()
        .map(|recipient| recipient.account_id().to_owned())
        .collect();
    Ok(RotationRecipients {
        first: holding,
        rest,
        waiting,
        unsupported,
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

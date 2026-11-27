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
use crate::registry::Registry;

use super::super::account::{AccountReplay, AccountState, PublicAccountLedger};
use super::super::domain::AppDomain;
use super::super::error::{
    IntrinsicContradiction, ProtocolError, ProtocolResult, UnsupportedCapability,
};
use super::super::position::{Accepted, Position, check_read};
use super::super::records::account::AccountAction;
use super::super::records::space::{
    SpaceAddress, SpaceDelivery, SpaceMember, SpaceRoot, SpaceSuccessor,
};
use super::chain::{SpaceChain, SpaceVersion};
use super::ledger::{AccountDirectory, Recipient, SpaceLedger, collect_member_accounts};

/// A member, or an account an envelope names, whose public account history
/// needs a capability this client lacks (§3.4, §7): skipped, and reported
/// as unsupported, never as invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedMember {
    pub account_id: String,
    pub capability: UnsupportedCapability,
}

/// A member, or an account an envelope names, whose public account history
/// is an intrinsic contradiction (§7), for example a root without its
/// device's keys: skipped and reported, so that one broken account never
/// stops deliveries or rotations for the rest of the space (§3.4). Its
/// envelopes of the current version set the needs-rotation mark (§3.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokenMember {
    pub account_id: String,
    pub contradiction: IntrinsicContradiction,
}

/// Why one account was skipped.
enum Skipped {
    Unsupported(UnsupportedMember),
    Broken(BrokenMember),
}

/// Runs `evaluate` for one account; an unsupported capability or an
/// intrinsic contradiction in the account's own history becomes that
/// account's report, and every other failure fails the operation. An
/// account whose ledger the caller left out fails with
/// [`ProtocolError::InconsistentEvidence`] before anything is evaluated.
fn evaluate<T>(
    accounts: &AccountDirectory<'_>,
    account_id: &str,
    evaluate: impl FnOnce() -> ProtocolResult<T>,
) -> ProtocolResult<Result<T, Skipped>> {
    if !accounts.has_ledger(account_id) {
        return Err(ProtocolError::InconsistentEvidence);
    }
    match evaluate() {
        Ok(value) => Ok(Ok(value)),
        Err(ProtocolError::Unsupported(capability)) => {
            Ok(Err(Skipped::Unsupported(UnsupportedMember {
                account_id: account_id.to_owned(),
                capability,
            })))
        }
        Err(ProtocolError::Intrinsic(contradiction)) => Ok(Err(Skipped::Broken(BrokenMember {
            account_id: account_id.to_owned(),
            contradiction,
        }))),
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

impl VersionEnvelope {
    fn from_version(version: &SpaceVersion, root: &SpaceRoot) -> Self {
        let (account, epoch) = version.writer(root);
        Self {
            account_id: account.to_owned(),
            account_epoch_id: epoch.to_owned(),
            source: if version.successor.is_some() {
                EnvelopeSource::Successor
            } else {
                EnvelopeSource::Root
            },
            record_id: version.record_id(root).to_owned(),
            position: version.position,
        }
    }

    fn from_delivery(delivery: &Accepted<SpaceDelivery>) -> Self {
        let row = &delivery.row;
        Self {
            account_id: row.recipient_account_id.clone(),
            account_epoch_id: row.recipient_account_epoch_id.clone(),
            source: EnvelopeSource::Delivery,
            record_id: row.id.clone(),
            position: delivery.position,
        }
    }
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
    let mut found = vec![VersionEnvelope::from_version(version, &chain.root.row)];
    found.extend(
        ledger
            .deliveries_of(&version.epoch_id)
            .into_iter()
            .filter(|delivery| !after_root || delivery.position > chain.root.position)
            .map(VersionEnvelope::from_delivery),
    );
    Ok(found)
}

/// The key holders of the version at `depth` (§1.1): the account that wrote
/// its root or successor, and every account a delivery of it was accepted
/// to. A delivery at or before the root's position counts here (§3.4).
/// The read must cover every recipient of this epoch.
pub fn holders(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    depth: usize,
) -> ProtocolResult<BTreeSet<String>> {
    chain.check_read(ledger, None)?;
    ledger.require_epoch(
        &chain
            .version(depth)
            .ok_or(ProtocolError::InconsistentEvidence)?
            .epoch_id,
    )?;
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
    /// Members whose account history contradicts itself: skipped, reported.
    pub broken: Vec<BrokenMember>,
}

/// Members lacking a version the sender holds and can deliver. A read of
/// those epochs covers this decision, including after a recovery rotation.
pub fn lacking_held(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
    keys: &super::SpaceKeys,
) -> ProtocolResult<LackingMembers> {
    keys.check_chain(chain)?;
    let mut concerned = chain.concerned()?;
    let held: BTreeSet<_> = keys.depths().collect();
    concerned.retain(|depth| held.contains(depth));
    lacking_at(chain, ledger, accounts, concerned)
}

fn lacking_at(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
    concerned: Vec<usize>,
) -> ProtocolResult<LackingMembers> {
    chain.check_read(ledger, Some(accounts))?;
    if ledger.filtered_for().is_some() {
        return Err(ProtocolError::InconsistentEvidence);
    }
    let mut by_depth = BTreeMap::new();
    for depth in &concerned {
        ledger.require_epoch(&chain.versions[*depth].epoch_id)?;
        by_depth.insert(*depth, version_envelopes(chain, ledger, *depth, true)?);
    }
    let mut out = Vec::new();
    let mut unsupported = Vec::new();
    let mut broken = Vec::new();
    for account in ledger.member_accounts() {
        let state = match evaluate(accounts, &account, || accounts.current(&account))? {
            Ok(state) => state,
            Err(Skipped::Unsupported(member)) => {
                unsupported.push(member);
                continue;
            }
            Err(Skipped::Broken(member)) => {
                broken.push(member);
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
        broken,
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
    /// Envelopes of the current version sealed to an account epoch of a
    /// generation the account has since reset (§2.11): that generation's
    /// devices, lost or not, can still open them.
    pub reset: Vec<VersionEnvelope>,
    /// Accounts an envelope of the current version names whose history
    /// needs a capability this client lacks. Reported, and otherwise
    /// ignored: they never set the mark here, or a client that cannot read
    /// them would rotate again each time newer clients deliver to them.
    /// Clients that can evaluate them still mark the space.
    pub unsupported: Vec<UnsupportedMember>,
    /// Accounts an envelope of the current version names whose history
    /// contradicts itself. Nobody can tell which of their devices can open
    /// it, so they set the mark: the safe direction.
    pub broken: Vec<BrokenMember>,
}

impl RotationMark {
    /// Whether the space needs rotation. [`RotationMark::unsupported`] does
    /// not count; [`RotationMark::broken`] does.
    pub fn needs_rotation(&self) -> bool {
        !self.former_members.is_empty()
            || !self.superseded.is_empty()
            || !self.reset.is_empty()
            || !self.broken.is_empty()
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
    chain.check_read(ledger, Some(accounts))?;
    let depth = chain.current_depth()?;
    ledger.require_epoch(&chain.versions[depth].epoch_id)?;
    let members = ledger.member_accounts();
    let found = version_envelopes(chain, ledger, depth, false)?;
    mark_envelopes(&chain.versions[depth].epoch_id, &members, found, accounts)
}

/// An immutable replay for callers that retain the current version for a
/// later rotation check. Only replay can construct it.
#[derive(Debug)]
pub struct ReplayedSpaceChain(SpaceChain);

impl ReplayedSpaceChain {
    pub fn replay(
        ledger: &SpaceLedger,
        accounts: &AccountDirectory<'_>,
    ) -> ProtocolResult<Option<Self>> {
        Ok(SpaceChain::replay(ledger, accounts)?.map(Self))
    }
}

impl std::ops::Deref for ReplayedSpaceChain {
    type Target = SpaceChain;

    fn deref(&self) -> &SpaceChain {
        &self.0
    }
}

/// Chain verification and its inputs retained for one delivery pass. Each
/// batch still reads the authority and checks memberships and recipients at
/// its own cut.
pub struct DeliveryChain<'a> {
    chain: SpaceChain,
    roots: Vec<Accepted<SpaceRoot>>,
    successors: Vec<Accepted<SpaceSuccessor>>,
    authors: Vec<PublicAccountLedger>,
    domain: AppDomain,
    registry: &'a Registry,
}

impl<'a> DeliveryChain<'a> {
    pub fn replay(
        ledger: &SpaceLedger,
        accounts: &AccountDirectory<'_>,
        registry: &'a Registry,
    ) -> ProtocolResult<Option<Self>> {
        if !std::ptr::eq(registry, accounts.registry()) {
            return Err(ProtocolError::InconsistentEvidence);
        }
        Ok(SpaceChain::replay(ledger, accounts)?.map(|chain| Self {
            chain,
            roots: ledger.roots.clone(),
            successors: ledger.successors.clone(),
            authors: Self::author_history(ledger, accounts),
            domain: accounts.domain().clone(),
            registry,
        }))
    }

    pub fn verified(&self) -> ReplayedSpaceChain {
        ReplayedSpaceChain(self.chain.clone())
    }

    /// Returns a replay for this read only if all chain evidence matches.
    /// Author checks use rows strictly before the signed record's position.
    /// Later account activity is still checked independently for deliveries.
    pub fn reuse(
        &self,
        ledger: &SpaceLedger,
        accounts: &AccountDirectory<'_>,
    ) -> ProtocolResult<Option<ReplayedSpaceChain>> {
        if ledger.address != *self.chain.address()
            || ledger.through < self.chain.through()
            || &self.domain != accounts.domain()
            || !std::ptr::eq(self.registry, accounts.registry())
            || self.roots != ledger.roots
            || self.successors != ledger.successors
            || self.authors != Self::author_history(ledger, accounts)
        {
            return Ok(None);
        }
        let reused = self.chain.at_matching_read(ledger, accounts)?;
        // The invariant `author_history` relies on, checked where it is cheap
        // to: a reused chain is the chain a fresh replay of this read gives.
        debug_assert_eq!(
            SpaceChain::replay(ledger, accounts)?.as_ref(),
            Some(&reused),
            "a reused delivery chain differs from a fresh replay"
        );
        Ok(Some(ReplayedSpaceChain(reused)))
    }

    /// The account evidence the chain's verification depended on, compared
    /// on reuse. Per author of a root or successor it keeps the account's
    /// rows strictly before that author's latest record. This relies on an
    /// invariant of `AccountDirectory::verify_author`: a record's check
    /// depends only on whether the author's whole history replays, and on
    /// its rows strictly before the record's position (`state_at`). Rows
    /// are append-only by position, so the rows before an author's earlier
    /// record are a prefix of those before its latest one, and equal
    /// prefixes at the latest cut give equal checks for every record. An
    /// author whose history does not replay is left out: its records were
    /// skipped, and they stay skipped while it is left out of both reads;
    /// once it replays it appears in only one, and the comparison fails.
    /// Debug builds check the result against a fresh replay in [`Self::reuse`].
    fn author_history(
        ledger: &SpaceLedger,
        accounts: &AccountDirectory<'_>,
    ) -> Vec<PublicAccountLedger> {
        let cut_and_author = ledger
            .roots
            .iter()
            .map(|root| (root.position, &root.row.creator_account_id))
            .chain(
                ledger
                    .successors
                    .iter()
                    .map(|successor| (successor.position, &successor.row.author_account_id)),
            );
        let mut cuts = BTreeMap::new();
        for (position, author) in cut_and_author {
            let cut = cuts.entry(author).or_insert(position);
            *cut = (*cut).max(position);
        }
        cuts.into_iter()
            .filter_map(|(author, cut)| Some(accounts.replay(author).ok()?.ledger().before(cut)))
            .collect()
    }
}

/// The current version's own envelope, copied from the result of chain
/// replay. Only the runtime observing the chain can establish freshness.
/// An observed change of root or successor requires a fresh acquisition.
#[derive(Clone, Debug)]
pub struct CurrentVersion {
    address: SpaceAddress,
    epoch: String,
    through: Position,
    writer: VersionEnvelope,
    root: Accepted<SpaceRoot>,
    successor: Option<Accepted<SpaceSuccessor>>,
}

impl CurrentVersion {
    /// A caller can inspect the replay but cannot certify an edited copy.
    ///
    /// ```compile_fail
    /// use jazz_e2ee::protocol::space::members::{CurrentVersion, ReplayedSpaceChain};
    /// fn edit(replay: &ReplayedSpaceChain) {
    ///     let mut chain = (**replay).clone();
    ///     chain.versions.pop();
    ///     let _ = CurrentVersion::from_chain(&chain);
    /// }
    /// ```
    pub fn from_chain(chain: &ReplayedSpaceChain) -> ProtocolResult<Self> {
        let version = chain.current()?;
        Ok(Self {
            address: chain.address().clone(),
            epoch: version.epoch_id.clone(),
            through: chain.through(),
            root: chain.root.clone(),
            successor: version.successor.clone(),
            writer: VersionEnvelope::from_version(version, &chain.root.row),
        })
    }

    pub fn epoch(&self) -> &str {
        &self.epoch
    }
    pub fn writer(&self) -> &str {
        &self.writer.account_id
    }

    pub fn root(&self) -> &Accepted<SpaceRoot> {
        &self.root
    }
    pub fn successor(&self) -> Option<&Accepted<SpaceSuccessor>> {
        self.successor.as_ref()
    }

    /// All membership rows of this address and all accepted deliveries of
    /// this epoch, read at one cut. Deliveries count even when the envelope
    /// cannot be opened. The version writer counts without a delivery row.
    pub fn rotation_mark(
        &self,
        address: &SpaceAddress,
        through: Position,
        members: &[Accepted<SpaceMember>],
        deliveries: &[Accepted<SpaceDelivery>],
        accounts: &AccountDirectory<'_>,
    ) -> ProtocolResult<RotationMark> {
        if address != &self.address || through < self.through || through != accounts.through() {
            return Err(ProtocolError::InconsistentEvidence);
        }
        let root = &self.root.row;
        let application = accounts.domain().account(&root.creator_account_id)?;
        let verify =
            |account: &str, device: &str, epoch: &str, position, bytes: &[u8], signature: &[u8]| {
                match accounts.verify_author(account, device, epoch, position, bytes, signature)? {
                    super::AuthorCheck::Valid => Ok(()),
                    _ => Err(ProtocolError::InconsistentEvidence),
                }
            };
        verify(
            &root.creator_account_id,
            &root.device_id,
            &root.account_epoch_id,
            self.root.position,
            &root.signed_bytes(&application)?,
            &root.signature,
        )?;
        if let Some(successor) = &self.successor {
            let row = &successor.row;
            verify(
                &row.author_account_id,
                &row.author_device_id,
                &row.author_epoch_id,
                successor.position,
                &row.signed_bytes(&application, &root.scope()?)?,
                &row.signature,
            )?;
        }
        check_read(members, through)?;
        check_read(deliveries, through)?;
        if members.iter().any(|member| {
            member.row.scope_id != address.scope_id || member.row.identifier != address.identifier
        }) || deliveries.iter().any(|delivery| {
            let row = &delivery.row;
            row.scope_id != address.scope_id
                || row.identifier != address.identifier
                || row.key_epoch_id != self.epoch
        }) {
            return Err(ProtocolError::InconsistentEvidence);
        }
        let membership = collect_member_accounts(members.iter());
        let mut found = vec![self.writer.clone()];
        let mut ordered = deliveries.iter().collect::<Vec<_>>();
        super::by_position_then_id(&mut ordered);
        found.extend(ordered.into_iter().map(VersionEnvelope::from_delivery));
        mark_envelopes(&self.epoch, &membership, found, accounts)
    }
}

fn mark_envelopes(
    epoch: &str,
    members: &BTreeSet<String>,
    found: Vec<VersionEnvelope>,
    accounts: &AccountDirectory<'_>,
) -> ProtocolResult<RotationMark> {
    let former_members: BTreeSet<String> = found
        .iter()
        .filter(|envelope| !members.contains(&envelope.account_id))
        .map(|envelope| envelope.account_id.clone())
        .collect();
    let mut superseded = Vec::new();
    let mut reset = Vec::new();
    let mut unsupported: Vec<UnsupportedMember> = Vec::new();
    let mut broken: Vec<BrokenMember> = Vec::new();
    for envelope in found {
        let account_id = envelope.account_id.clone();
        let removal = evaluate(accounts, &account_id, || {
            supersession(accounts.replay(&account_id)?, &envelope.account_epoch_id)
        })?;
        let removal = match removal {
            Ok(removal) => removal,
            Err(Skipped::Unsupported(member)) => {
                if !unsupported.contains(&member) {
                    unsupported.push(member);
                }
                continue;
            }
            Err(Skipped::Broken(member)) => {
                if !broken.contains(&member) {
                    broken.push(member);
                }
                continue;
            }
        };
        match removal {
            Some(Supersession::Removal(successor_id, removed_device_id)) => {
                superseded.push(SupersededEnvelope {
                    envelope,
                    successor_id,
                    removed_device_id,
                });
            }
            Some(Supersession::Reset) => reset.push(envelope),
            None => {}
        }
    }
    Ok(RotationMark {
        epoch_id: epoch.to_owned(),
        former_members: former_members.into_iter().collect(),
        superseded,
        reset,
        unsupported,
        broken,
    })
}

/// Why an envelope's account epoch no longer confines it to the account's
/// active devices.
enum Supersession {
    /// The first accepted `remove-device` successor after the epoch, and the
    /// device it removed.
    Removal(String, String),
    /// The epoch belongs to a generation the account has since reset.
    Reset,
}

/// What superseded `epoch_id` in the account's chains (§3.5): the first
/// accepted `remove-device` successor after it, in the current generation's
/// chain or, for an epoch of an earlier generation, in that generation's
/// chain as it stood at the reset; failing that, for an earlier generation,
/// the reset that ended it (§2.11). `None` when nothing superseded it, or the epoch is in
/// none of the account's chains.
fn supersession(
    replay: &AccountReplay<'_>,
    epoch_id: &str,
) -> ProtocolResult<Option<Supersession>> {
    let through = replay.ledger().through;
    let Some(current) = replay.as_of(Position(through.0.saturating_add(1)))? else {
        return Ok(None);
    };
    if let Some(found) = removal_in(&current, epoch_id) {
        return Ok(found.map(|(successor, device)| Supersession::Removal(successor, device)));
    }
    // Each earlier generation's chain, as of the canonical root of the
    // generation after it: an epoch found there was ended by that reset.
    for next in current.roots.iter().skip(1) {
        let Some(earlier) = replay.as_of(next.position)? else {
            continue;
        };
        match removal_in(&earlier, epoch_id) {
            Some(Some((successor, device))) => {
                return Ok(Some(Supersession::Removal(successor, device)));
            }
            Some(None) => return Ok(Some(Supersession::Reset)),
            None => {}
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
        (successor.columns.action == AccountAction::RemoveDevice)
            .then(|| {
                successor
                    .columns
                    .removed_device_id
                    .clone()
                    .map(|device| (successor.columns.id.clone(), device))
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
    /// Members whose account history contradicts itself: skipped and
    /// reported; nothing can be sealed to them.
    pub broken: Vec<BrokenMember>,
}

/// Chooses the recipients of a rotation by `rotator` (§3.5): every current
/// member other than the rotator's account that has a valid sealing key of
/// `mechanism` (§2.12), the members that held the current version first.
/// A member whose account history needs a capability this client lacks is
/// reported in [`RotationRecipients::unsupported`]. Needs the public ledger
/// of every member and every delivery of the current version at the cut
/// `chain` was replayed from.
pub fn rotation_recipients(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
    rotator: &str,
    mechanism: Mechanism<'_>,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RotationRecipients> {
    chain.check_read(ledger, Some(accounts))?;
    let depth = chain.current_depth()?;
    let held = holders(chain, ledger, depth)?;
    let mut holding = Vec::new();
    let mut others = Vec::new();
    let mut waiting = Vec::new();
    let mut unsupported = Vec::new();
    let mut broken = Vec::new();
    for account in ledger.member_accounts() {
        if account == rotator {
            continue;
        }
        match evaluate(accounts, &account, || {
            accounts.recipient(&account, mechanism)
        })? {
            Err(Skipped::Unsupported(member)) => unsupported.push(member),
            Err(Skipped::Broken(member)) => broken.push(member),
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
        broken,
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

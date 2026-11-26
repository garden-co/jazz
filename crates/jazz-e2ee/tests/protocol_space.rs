//! Space replay (`E2EE_PROTOCOL.md` §3.2–§3.5): the space's root among the
//! roots of its address, the chain rule, acquiring keys, who lacks a
//! version, the needs-rotation mark and the space record builders.
//!
//! Tests build records with the crate's own builders and accept them on
//! one sequence of positions. The server's policies (R12–R17) are not
//! modelled: most writes are ones the authority would accept, but some
//! tests also accept rows the authority or a correct client would never
//! write (tampered signatures, forged signers, damaged envelopes, rows
//! from another space) to check that replay refuses them.

mod common;

use common::TestRandom;
use common::mechanisms::{
    FLAKY_SEAL, FLAKY_SIGN, FlakyWrapper, OTHER_SEAL, OTHER_SIGN, UNROUTED_SEAL, UNROUTED_WRAP,
    custom_registry, flaky_registry, relabelled, sealing_with,
};
use common::protocol::sealing_key_record;
use common::space::*;
use jazz_e2ee::agility::{KeySealer, KeyWrapper, MechanismConfig};
use jazz_e2ee::context::{CryptoContext, frame_record};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::{key_envelope, key_seal};
use jazz_e2ee::protocol::account::PublicAccountLedger;
use jazz_e2ee::protocol::account::build::{self as account_build, LocalDevice, RotationTarget};
use jazz_e2ee::protocol::derive::{RecoveryKeys, RecoverySecret};
use jazz_e2ee::protocol::error::{
    IntrinsicContradiction, ProtocolError, RefusalCode, UnsupportedCapability,
};
use jazz_e2ee::protocol::records::space::{SpaceAddress, SpaceDelivery};
use jazz_e2ee::protocol::space::build::{self, Author};
use jazz_e2ee::protocol::space::chain::select_root;
use jazz_e2ee::protocol::space::members::{
    self, DELIVERY_BATCH, DeliveryChain, EnvelopeSource, ReplayedSpaceChain, lacking_held,
    rotation_mark, rotation_recipients,
};
use jazz_e2ee::protocol::space::{
    AccountDirectory, BrokenHistory, InvalidSpaceRoot, KeyCheck, Recipient, SpaceChain, SpaceKeys,
    SpaceLedger, acquire,
};
use jazz_e2ee::protocol::{Accepted, Position, ProtocolResult, derive, ids, signing};
use jazz_e2ee::registry::Registry;

const BUILTIN: Mechanism<'static> = key_seal::MECHANISM;

fn refused(code: RefusalCode) -> ProtocolError {
    ProtocolError::Refused(code)
}

/// Alice's space with `members` as members (Alice first), created by
/// Alice.
fn space(seed: u64, members: &[&str]) -> World {
    let mut world = World::new(seed);
    for account in members {
        world.account(account);
    }
    for account in members {
        world.add_member(account);
    }
    world.create(ALICE);
    world
}

fn lacking_of(world: &World) -> Vec<(String, Vec<String>)> {
    let read = world.read();
    let chain = read.chain(world).unwrap();
    lacking_held(
        &chain,
        &read.space,
        &read.directory(world),
        &world.acquire(ALICE).keys,
    )
    .unwrap()
    .lacking
    .into_iter()
    .map(|entry| (entry.account_id, entry.epoch_ids))
    .collect()
}

fn mark_of(world: &World) -> members::RotationMark {
    let read = world.read();
    let chain = read.chain(world).unwrap();
    rotation_mark(&chain, &read.space, &read.directory(world)).unwrap()
}

/// Alice checks only the envelopes of the replayed version. Bob's device
/// removal and subsequent membership removal agree with a full pass.
#[test]
fn checking_current_envelopes_agrees_with_the_full_mark() {
    let mut world = space(22, &[ALICE, BOB]);
    let phone = world.approve_device(BOB);
    let epoch = world.current();
    world.deliver(ALICE, BOB, &epoch);
    let check = |world: &World| {
        let read = world.read();
        let replay = ReplayedSpaceChain::replay(&read.space, &read.directory(world))
            .unwrap()
            .unwrap();
        let version = members::CurrentVersion::from_chain(&replay).unwrap();
        let current: Vec<_> = read
            .space
            .deliveries
            .iter()
            .filter(|d| d.row.key_epoch_id == version.epoch())
            .cloned()
            .collect();
        let mark = version
            .rotation_mark(
                &read.space.address,
                read.space.through,
                &read.space.members,
                &current,
                &read.directory(world),
            )
            .unwrap();
        assert_eq!(mark, mark_of(world));
    };
    check(&world);
    world.remove_device(BOB, &phone.id);
    check(&world);
    world.rotate(ALICE);
    check(&world);
    world.remove_member(BOB);
    check(&world);
}

/// The mark check rejects a damaged signature, an absent recipient history,
/// and envelopes from another version of Alice's space.
#[test]
fn a_current_version_check_rejects_inconsistent_evidence() {
    let mut world = space(32, &[ALICE, BOB]);
    let epoch = world.current();
    world.deliver(ALICE, BOB, &epoch);
    let mut read = world.read();
    let chain = ReplayedSpaceChain::replay(&read.space, &read.directory(&world))
        .unwrap()
        .unwrap();
    let valid = members::CurrentVersion::from_chain(&chain).unwrap();
    let mut damaged = read.space.clone();
    damaged.roots[0].row.signature[0] ^= 1;
    assert!(
        ReplayedSpaceChain::replay(&damaged, &read.directory(&world))
            .unwrap()
            .is_none()
    );
    let check = |version: &members::CurrentVersion, read: &Read| {
        version.rotation_mark(
            &read.space.address,
            read.space.through,
            &read.space.members,
            &read.space.deliveries,
            &read.directory(&world),
        )
    };
    read.ledgers.retain(|a| a.account_id != BOB);
    assert_eq!(
        check(&valid, &read),
        Err(ProtocolError::InconsistentEvidence)
    );
    let mut read = world.read();
    read.space.deliveries[0].row.key_epoch_id = read.space.address.identifier.clone();
    assert_eq!(
        check(&valid, &read),
        Err(ProtocolError::InconsistentEvidence)
    );
}

/// A new member receives the current version from a holder (§3.4).
///
/// ```text
/// alice ──root──► space          bob joins: lacks v0
/// alice ──delivery(v0 → bob's account epoch)──► bob acquires v0
/// ```
#[test]
fn a_new_member_receives_the_current_version_from_a_holder() {
    let mut world = space(1, &[ALICE]);
    world.account(BOB);
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    assert_eq!(chain.creator(), ALICE);
    assert_eq!(chain.versions.len(), 1);
    let v0 = chain.current().unwrap().epoch_id.clone();
    let alice = world.acquire(ALICE);
    assert!(alice.has_current(&chain));
    assert!(lacking_of(&world).is_empty());

    world.add_member(BOB);
    assert_eq!(lacking_of(&world), [(BOB.to_owned(), vec![v0.clone()])]);
    let before = world.acquire(BOB);
    assert!(!before.has_current(&chain));
    assert!(before.invalid_deliveries.is_empty());

    let delivery = world.deliver(ALICE, BOB, &v0);
    assert_eq!(
        delivery.recipient_account_epoch_id,
        world.account_epoch(BOB)
    );
    let bob = world.acquire(BOB);
    assert!(bob.has_current(&chain));
    assert_eq!(bob.keys.key(&v0), alice.keys.key(&v0));
    assert!(lacking_of(&world).is_empty());
    assert!(!mark_of(&world).needs_rotation());
}

/// The space's root is the first valid root of its address; every other
/// root is a lost creation (§3.2).
#[test]
fn the_first_valid_root_is_the_space_root() {
    let mut world = World::new(2);
    world.account(ALICE);
    world.account(BOB);
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(BOB);
    world.create(ALICE);
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    assert_eq!(chain.creator(), BOB);
    assert_eq!(chain.lost.len(), 1);
    assert_eq!(chain.lost[0].creator_account_id, ALICE);
    assert_eq!(chain.lost[0].invalid, None);
    // Each creator's domain is its own: the space's records use Bob's.
    assert_eq!(chain.application, world.shared.domain.account(BOB).unwrap());
    // Alice's own envelope of her lost root opens nothing in this chain.
    assert!(!world.acquire(ALICE).has_current(&chain));
    assert!(world.acquire(BOB).has_current(&chain));
}

/// Roots accepted in one transaction are ordered by ID.
#[test]
fn roots_at_one_position_are_ordered_by_id() {
    let mut world = World::new(3);
    world.account(ALICE);
    world.account(BOB);
    let alice_device = world.accounts[ALICE].device.clone();
    let bob_device = world.accounts[BOB].device.clone();
    let alice = world.build_root(ROW, ALICE, &alice_device).unwrap().root;
    let bob = world.build_root(ROW, BOB, &bob_device).unwrap().root;
    let at = world.next();
    world.space.roots.push(Accepted::new(at, alice.clone()));
    world.space.roots.push(Accepted::new(at, bob.clone()));
    let chain = world.read().chain(&world).unwrap();
    let first = if alice.id < bob.id { &alice } else { &bob };
    assert_eq!(chain.root.row.id, first.id);
}

/// A root written by a device that was removed before the root was
/// accepted is invalid, and does not block a valid later root (§3.2).
#[test]
fn a_root_by_a_removed_device_does_not_block_a_later_root() {
    let mut world = World::new(4);
    world.account(ALICE);
    world.account(BOB);
    let phone = world.approve_device(ALICE);
    let early = world.build_root(ROW, ALICE, &phone).unwrap().root;
    world.remove_device(ALICE, &phone.id);
    let at = world.next();
    world.space.roots.push(Accepted::new(at, early.clone()));
    assert!(world.read().chain(&world).is_none(), "no valid root yet");

    world.create(BOB);
    let chain = world.read().chain(&world).unwrap();
    assert_eq!(chain.creator(), BOB);
    assert_eq!(chain.lost.len(), 1);
    assert_eq!(chain.lost[0].root_id, early.id);
    assert_eq!(chain.lost[0].invalid, Some(InvalidSpaceRoot::NotActive));
}

/// Each validity condition of a root (§3.2): a valid signature, a creator
/// with an account root strictly before it, a device active with the named
/// account epoch, a well-formed row. The first valid one is the root.
#[test]
fn invalid_roots_are_skipped_with_their_reasons() {
    let mut world = World::new(5);
    world.account(ALICE);
    let device = world.accounts[ALICE].device.clone();

    let mut tampered = world.build_root(ROW, ALICE, &device).unwrap().root;
    tampered.signature[70] ^= 1;
    let at = world.next();
    world.space.roots.push(Accepted::new(at, tampered.clone()));

    // A root naming the account epoch Alice has since rotated away from.
    let stale = world.build_root(ROW, ALICE, &device).unwrap().root;
    let phone = world.approve_device(ALICE);
    world.remove_device(ALICE, &phone.id);
    let at = world.next();
    world.space.roots.push(Accepted::new(at, stale.clone()));

    let mut unsigned = world.build_root(ROW, ALICE, &device).unwrap().root;
    unsigned.signature.clear();
    let at = world.next();
    world.space.roots.push(Accepted::new(at, unsigned.clone()));

    // Carol has no account root at all.
    let mut foreign = world.build_root(ROW, ALICE, &device).unwrap().root;
    foreign.creator_account_id = CAROL.to_owned();
    let at = world.next();
    world.space.roots.push(Accepted::new(at, foreign.clone()));

    let valid_at = world.create(ALICE);
    let mut read = world.read();
    read.ledgers
        .push(PublicAccountLedger::new(CAROL, read.space.through));
    let chain = read.chain(&world).unwrap();
    assert_eq!(chain.root.position, valid_at);
    let reasons: Vec<_> = chain
        .lost
        .iter()
        .map(|root| (root.root_id.clone(), root.invalid))
        .collect();
    assert_eq!(
        reasons,
        [
            (tampered.id, Some(InvalidSpaceRoot::SignatureInvalid)),
            (stale.id, Some(InvalidSpaceRoot::NotActive)),
            (unsigned.id, Some(InvalidSpaceRoot::Malformed)),
            (foreign.id, Some(InvalidSpaceRoot::NoAccountRoot)),
        ]
    );
}

/// A read that leaves out the public ledger of an account a record names
/// is inconsistent evidence, never "not active".
#[test]
fn a_missing_account_ledger_is_inconsistent_evidence() {
    let world = space(6, &[ALICE]);
    let read = world.read();
    let others: Vec<_> = read
        .ledgers
        .iter()
        .filter(|ledger| ledger.account_id != ALICE)
        .cloned()
        .collect();
    let directory = AccountDirectory::new(
        &others,
        &world.shared.domain,
        &world.shared.registry,
        read.space.through,
    )
    .unwrap();
    assert_eq!(
        SpaceChain::replay(&read.space, &directory),
        Err(ProtocolError::InconsistentEvidence)
    );
}

/// Re-signs a successor with `account`'s default device after a test
/// changed it.
fn resign(
    world: &World,
    account: &str,
    successor: &mut jazz_e2ee::protocol::records::space::SpaceSuccessor,
) {
    let chain = world.read().chain(world).unwrap();
    let device = &world.accounts[account].device;
    let bytes = successor
        .signed_bytes(&chain.application, &chain.scope)
        .unwrap();
    let signer = world
        .shared
        .registry
        .signer(device.signing.public.mechanism.as_mechanism())
        .unwrap();
    successor.signature = signer.sign(&device.signing, &bytes).unwrap();
}

/// The chain rule (§3.3): only the first valid successor of the current
/// version extends the chain; a second successor of the same predecessor,
/// one not strictly later, one reusing an epoch, one whose history names
/// no version of the chain, one with a bad signature and one by a device
/// removed before it was accepted are all skipped.
#[test]
fn the_chain_rule_skips_invalid_successors() {
    let mut world = space(7, &[ALICE, BOB]);
    let v0 = world.current();
    let phone = world.approve_device(ALICE);
    let stale = world.build_rotation(ALICE, false).unwrap();
    let v1 = world.rotate(ALICE);
    let v1_at = world.cut();
    // A second successor of v0, accepted after v1.
    world.accept_rotation(stale);

    // By a device removed before it is accepted.
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let keys = world.acquire(ALICE).keys;
    let own = world.own_with(ALICE, &phone).unwrap();
    let mut rng = TestRandom(77);
    let by_phone = build::rotate(
        &world.writer(ALICE),
        &chain,
        &keys,
        &Author::new(&read.directory(&world), ALICE, &own.key, &phone).unwrap(),
        &[],
        &mut rng,
    )
    .unwrap()
    .successor;
    world.remove_device(ALICE, &phone.id);
    let at = world.next();
    world.space.successors.push(Accepted::new(at, by_phone));

    // Not strictly later than its predecessor.
    let same_position = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    world
        .space
        .successors
        .push(Accepted::new(v1_at, same_position));
    // Reuses the root's epoch.
    let mut reused = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    reused.epoch_id = v0.clone();
    resign(&world, ALICE, &mut reused);
    // History names no version of the chain.
    let mut unknown_history = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    unknown_history.history_epoch_id = ids::random_v4(&mut world.rng).unwrap();
    resign(&world, ALICE, &mut unknown_history);
    // A bad signature.
    let mut tampered = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    tampered.signature[70] ^= 1;
    for successor in [reused, unknown_history, tampered] {
        let at = world.next();
        world.space.successors.push(Accepted::new(at, successor));
    }
    assert_eq!(world.current(), v1);

    let v2 = world.rotate(ALICE);
    let chain = world.read().chain(&world).unwrap();
    let epochs: Vec<_> = chain.versions.iter().map(|v| v.epoch_id.clone()).collect();
    assert_eq!(epochs, [v0.clone(), v1.clone(), v2.clone()]);
    assert_eq!(chain.history_path(2), [2, 1, 0]);
    assert!(chain.skipped().is_empty());
    assert_eq!(chain.concerned().unwrap(), [2]);

    // Bob receives v2 in the rotation and opens every version from it.
    let bob = world.acquire(BOB);
    for epoch in [&v0, &v1, &v2] {
        assert!(bob.keys.holds(epoch), "{epoch}");
    }
    assert_eq!(bob.keys.newest().unwrap().epoch_id(), v2);
    let (root, successors) = chain.lineage(2).unwrap();
    assert_eq!(root.id, chain.root.row.id);
    assert_eq!(successors.len(), 2);
}

/// A recovery rotation (§3.5) wraps the newest version its author holds
/// and skips the versions after it: the chain gives no key for them, so a
/// member obtains them only from its own envelope or a delivery, and
/// members lack them until a holder delivers (§3.4).
#[test]
fn a_recovery_rotation_skips_versions_no_member_online_holds() {
    let mut world = space(8, &[ALICE, BOB, CAROL]);
    world.account(DAVE);
    world.add_member(DAVE);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    // Alice rotates while Bob and Carol are not members, so only she
    // holds v1.
    world.remove_member(BOB);
    world.remove_member(CAROL);
    world.remove_member(DAVE);
    let v1 = world.rotate(ALICE);
    world.add_member(BOB);
    world.add_member(CAROL);
    world.add_member(DAVE);

    assert_eq!(
        world.build_rotation(BOB, false).err(),
        Some(refused(RefusalCode::RotationNeedsKey))
    );
    assert_eq!(
        world.build_rotation(DAVE, true).err(),
        Some(refused(RefusalCode::RotationNeedsKey)),
        "a recovery rotation needs at least one held version"
    );
    let rotation = world.build_rotation(BOB, true).unwrap();
    assert_eq!(rotation.records.successor.predecessor, v1);
    assert_eq!(rotation.records.successor.history_epoch_id, v0);
    let v2 = world.accept_rotation(rotation);

    let read = world.read();
    let chain = read.chain(&world).unwrap();
    assert_eq!(chain.current().unwrap().epoch_id, v2);
    assert_eq!(chain.history_path(2), [2, 0]);
    assert_eq!(chain.skipped(), [1]);
    assert_eq!(chain.concerned().unwrap(), [1, 2]);

    let carol = world.acquire(CAROL);
    assert!(carol.has_current(&chain));
    assert!(carol.keys.holds(&v0));
    assert!(!carol.keys.holds(&v1));
    // Alice keeps v1 from her own account envelope.
    let alice = world.acquire(ALICE);
    for epoch in [&v0, &v1, &v2] {
        assert!(alice.keys.holds(epoch), "{epoch}");
    }
    let mut missing = lacking_of(&world);
    missing.sort();
    assert_eq!(
        missing,
        [
            (BOB.to_owned(), vec![v1.clone()]),
            (CAROL.to_owned(), vec![v1.clone()]),
            (DAVE.to_owned(), vec![v1.clone()]),
        ]
    );
    world.deliver(ALICE, CAROL, &v1);
    assert!(world.acquire(CAROL).keys.holds(&v1));
}

/// A successor whose author's signing mechanism this reader lacks leaves
/// the chain unresolved there (§3.3): the reader does not skip it, and has
/// no current version.
#[test]
fn an_unsupported_signing_mechanism_leaves_the_chain_unresolved() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let mut world = World::with_registry(9, registry);
    world.account(ALICE);
    world.account_with(
        BOB,
        MechanismConfig {
            signature: OTHER_SIGN.into(),
            ..MechanismConfig::default()
        },
    );
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    let v1 = world.rotate(BOB);
    assert_eq!(
        world.current(),
        v1,
        "a reader with the mechanism resolves it"
    );

    let read = world.read();
    let builtin = Registry::builtin();
    let directory = AccountDirectory::new(
        &read.ledgers,
        &world.shared.domain,
        &builtin,
        read.space.through,
    )
    .unwrap();
    let chain = SpaceChain::replay(&read.space, &directory)
        .unwrap()
        .unwrap();
    assert_eq!(chain.last().epoch_id, v0);
    let unresolved = chain.unresolved.as_ref().unwrap();
    assert_eq!(
        unresolved.capability,
        UnsupportedCapability::SigningMechanism
    );
    assert_eq!(
        chain.current(),
        Err(ProtocolError::Unsupported(
            UnsupportedCapability::SigningMechanism
        ))
    );
    let own = world.own(ALICE);
    let alice = acquire(&chain, &read.space, &directory, &own.epoch_keys).unwrap();
    assert!(alice.keys.holds(&v0));
    assert!(!alice.has_current(&chain));
}

/// A member whose account resets lacks every version again, which holders
/// see from its public roots alone, and receives the current version
/// again; the old generation opens nothing (§2.11, §3.4).
#[test]
fn a_reset_member_receives_the_current_version_again() {
    let mut world = space(10, &[ALICE, BOB]);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    assert!(lacking_of(&world).is_empty());

    let space_rows = world.space.clone();
    world.reset(BOB);
    assert_eq!(world.space, space_rows, "no space table changed");
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let missing = lacking_held(
        &chain,
        &read.space,
        &read.directory(&world),
        &world.acquire(ALICE).keys,
    )
    .unwrap()
    .lacking;
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].account_id, BOB);
    assert_eq!(missing[0].generation, Some(1));
    assert_eq!(missing[0].epoch_ids, std::slice::from_ref(&v0));
    let bob = world.acquire(BOB);
    assert!(!bob.has_current(&chain));
    assert!(
        bob.invalid_deliveries.is_empty(),
        "a delivery to another generation is skipped, not reported"
    );

    world.deliver(ALICE, BOB, &v0);
    assert!(world.acquire(BOB).has_current(&chain));
    assert!(lacking_of(&world).is_empty());
}

/// A delivery that does not open is reported as an invalid delivery naming
/// its sender, holders count the member as holding the version, and the
/// member receives a working key with the next rotation (§3.4).
#[test]
fn a_garbage_delivery_is_reported_and_the_next_rotation_heals_it() {
    let mut world = space(11, &[ALICE, BOB, MALLORY]);
    let v0 = world.current();
    world.deliver(ALICE, MALLORY, &v0);
    let mut garbage = world.build_delivery(MALLORY, BOB, &v0).unwrap();
    let last = garbage.envelope.len() - 1;
    garbage.envelope[last] ^= 1;
    let at = world.next();
    world
        .space
        .deliveries
        .push(Accepted::new(at, garbage.clone()));

    let chain = world.read().chain(&world).unwrap();
    let bob = world.acquire(BOB);
    assert!(!bob.has_current(&chain));
    assert_eq!(bob.invalid_deliveries.len(), 1);
    let report = &bob.invalid_deliveries[0];
    assert_eq!(report.delivery_id, garbage.id);
    assert_eq!(report.sender_account_id, MALLORY);
    assert_eq!(report.recipient_account_id, BOB);
    assert_eq!(report.key_epoch_id, v0);
    assert!(
        lacking_of(&world).is_empty(),
        "holders cannot see it failed"
    );

    let v1 = world.rotate(ALICE);
    let chain = world.read().chain(&world).unwrap();
    let bob = world.acquire(BOB);
    assert!(bob.has_current(&chain));
    assert!(bob.keys.holds(&v0));
    assert!(bob.keys.holds(&v1));
    assert!(bob.invalid_deliveries.is_empty(), "v0 now comes from v1");
}

/// A delivery whose opening fails with a provider error is not reported
/// as an invalid delivery: the acquisition fails loudly (§0, §3.4).
#[test]
fn a_delivery_that_fails_with_a_provider_error_is_not_reported() {
    let (registry, fails) = custom_registry(FLAKY_SEAL);
    let mut world = World::with_registry(12, registry);
    world.account_with(ALICE, sealing_with(FLAKY_SEAL));
    world.account_with(BOB, sealing_with(FLAKY_SEAL));
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    assert!(world.acquire(BOB).keys.holds(&v0));

    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let own = world.own(BOB);
    fails.store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        acquire(
            &chain,
            &read.space,
            &read.directory(&world),
            &own.epoch_keys
        )
        .err(),
        Some(ProtocolError::ProviderFailure)
    );
}

/// A member whose account has no sealing key of the selected mechanism
/// waits: sealing to it is "recipient unsupported". Once its account
/// publishes one (a change to its account rows only), it receives the
/// version (§2.12, §3.4).
#[test]
fn a_member_receives_the_version_once_its_sealing_key_appears() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let mut world = World::with_registry(13, registry);
    world.account(ALICE);
    world.account_with(BOB, sealing_with(OTHER_SEAL));
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    let v0 = world.current();
    assert_eq!(
        world.build_delivery(ALICE, BOB, &v0).err(),
        Some(refused(RefusalCode::UnsupportedRecipient))
    );
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let mut rng = TestRandom(1);
    let recipients = rotation_recipients(
        &chain,
        &read.space,
        &read.directory(&world),
        ALICE,
        BUILTIN,
        &mut rng,
    )
    .unwrap();
    assert_eq!(recipients.waiting, [BOB]);

    // Bob's device publishes the built-in record for its current epoch.
    let space_rows = world.space.clone();
    let own = world.own(BOB);
    world.with_account(BOB, |entry, rng| {
        let public_key = derive::account_sealing_key_pair(&key_seal::Builtin, own.key.key())
            .unwrap()
            .public
            .bytes;
        let record = sealing_key_record(
            &entry.env,
            &entry.device,
            own.state.epoch_id(),
            BUILTIN,
            public_key,
            rng,
        );
        let at = entry.ledger.next();
        entry
            .ledger
            .public
            .account_keys
            .push(Accepted::new(at, record));
    });
    assert_eq!(world.space, space_rows);
    world.deliver(ALICE, BOB, &v0);
    assert!(world.acquire(BOB).has_current(&chain));
}

/// An envelope names the account epoch it was sealed to; after the
/// account rotates, its remaining devices still open it through the
/// account's history, and a newly approved device of the account obtains
/// every space key sealed to it (§2.12, §3.6).
#[test]
fn account_devices_open_envelopes_of_earlier_account_epochs() {
    let mut world = space(14, &[ALICE, BOB]);
    let v0 = world.current();
    let bob_e0 = world.account_epoch(BOB);
    let delivery = world.deliver(ALICE, BOB, &v0);
    assert_eq!(delivery.recipient_account_epoch_id, bob_e0);
    let phone = world.approve_device(BOB);
    world.remove_device(BOB, &phone.id);
    assert_ne!(world.account_epoch(BOB), bob_e0);
    let chain = world.read().chain(&world).unwrap();
    assert!(world.acquire(BOB).has_current(&chain));
    // A device removal changes nothing for who lacks a key.
    assert!(lacking_of(&world).is_empty());

    let tablet = world.approve_device(ALICE);
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let own = world.own_with(ALICE, &tablet).unwrap();
    let tablet_keys = acquire(
        &chain,
        &read.space,
        &read.directory(&world),
        &own.epoch_keys,
    )
    .unwrap();
    assert!(tablet_keys.has_current(&chain));
}

/// Deliveries name their own space and version: one built from a space's
/// keys names that space, a delivery row naming a version of another space
/// is not a candidate, and a delivery at or before the root's position is
/// skipped (§3.3, §3.4).
#[test]
fn deliveries_are_bound_to_their_space() {
    let mut world = space(15, &[ALICE, BOB]);
    world.add_member_of(OTHER_ROW, ALICE);
    world.add_member_of(OTHER_ROW, BOB);
    world.create_of(OTHER_ROW, ALICE);
    let other = world.read_of(SpaceAddress::new(SCOPE, OTHER_ROW).unwrap());
    let other_chain = other.chain(&world).unwrap();
    let own = world.own(ALICE);
    let other_keys = acquire(
        &other_chain,
        &other.space,
        &other.directory(&world),
        &own.epoch_keys,
    )
    .unwrap()
    .keys;
    let other_v0 = other_chain.current().unwrap().epoch_id.clone();
    let recipient = other
        .directory(&world)
        .recipient(BOB, BUILTIN)
        .unwrap()
        .unwrap();
    let mut rng = TestRandom(3);
    let to_bob = build::deliver(
        &world.writer(ALICE),
        &other_chain,
        &other_keys.version(&other_v0).unwrap(),
        &recipient,
        &mut rng,
    )
    .unwrap();
    assert_eq!(to_bob.identifier, OTHER_ROW);

    let chain = world.read().chain(&world).unwrap();
    let alice = world.acquire(ALICE);
    assert!(alice.keys.version(&other_v0).is_none());
    // The other space's delivery copied into this space's address.
    let copied = SpaceDelivery {
        identifier: ROW.to_owned(),
        ..to_bob
    };
    let at = world.next();
    world.space.deliveries.push(Accepted::new(at, copied));
    let bob = world.acquire(BOB);
    assert!(bob.keys.depths().next().is_none());
    assert!(bob.invalid_deliveries.is_empty());
    assert!(!bob.has_current(&chain));

    // A real delivery accepted at the root's own position is skipped.
    let v0 = world.current();
    let early = world.build_delivery(ALICE, BOB, &v0).unwrap();
    world
        .space
        .deliveries
        .push(Accepted::new(chain.root.position, early));
    let bob = world.acquire(BOB);
    assert!(!bob.has_current(&chain));
    assert!(bob.invalid_deliveries.is_empty());
    assert_eq!(lacking_of(&world).len(), 1);
}

/// A `remove-device` account successor of a member holding the current
/// version marks the space, with no change to any space table (§3.5).
#[test]
fn removing_a_device_of_a_holder_marks_the_space() {
    let mut world = space(16, &[ALICE, BOB]);
    let v0 = world.current();
    let phone = world.approve_device(BOB);
    world.deliver(ALICE, BOB, &v0);
    assert!(!mark_of(&world).needs_rotation());

    let space_rows = world.space.clone();
    let removal = world.remove_device(BOB, &phone.id);
    assert_eq!(world.space, space_rows);
    let mark = mark_of(&world);
    assert!(mark.needs_rotation());
    assert!(mark.former_members.is_empty());
    assert_eq!(mark.superseded.len(), 1);
    assert_eq!(mark.superseded[0].envelope.account_id, BOB);
    assert_eq!(mark.superseded[0].envelope.source, EnvelopeSource::Delivery);
    assert_eq!(mark.superseded[0].successor_id, removal);
    assert_eq!(mark.superseded[0].removed_device_id, phone.id);

    world.rotate(ALICE);
    assert!(!mark_of(&world).needs_rotation(), "a rotation clears it");
}

/// The removal marks the space also when other account successors
/// separate it from the envelope's account epoch (§3.5).
#[test]
fn a_removal_separated_by_other_successors_marks_the_space() {
    let mut world = space(17, &[ALICE, BOB]);
    let v0 = world.current();
    let phone = world.approve_device(BOB);
    world.deliver(ALICE, BOB, &v0);
    world.with_account(BOB, |entry, rng| {
        let recovery = RecoveryKeys::derive(
            &RecoverySecret::from_bytes([7; 32]),
            &entry.env.application(),
            0,
        )
        .unwrap();
        let device = entry.device.clone();
        let root = entry
            .ledger
            .register_recovery(&entry.env, &device, &recovery, rng);
        entry.ledger.rotate(
            &entry.env,
            &device,
            RotationTarget::RetireRecoveryRoot(root.id),
            rng,
        );
    });
    assert!(
        !mark_of(&world).needs_rotation(),
        "a retirement removes nobody"
    );
    let removal = world.remove_device(BOB, &phone.id);
    let mark = mark_of(&world);
    assert!(mark.needs_rotation());
    assert_eq!(mark.superseded[0].successor_id, removal);
}

/// A raw delivery with an invalid envelope that names the current version
/// and a superseded account epoch of another member marks the space,
/// although no key was transferred (§3.5); the member reports it as an
/// invalid delivery, and a rotation clears the mark.
#[test]
fn a_junk_delivery_naming_a_superseded_epoch_marks_the_space() {
    let mut world = space(18, &[ALICE, BOB, MALLORY]);
    let v0 = world.current();
    let bob_e0 = world.account_epoch(BOB);
    let phone = world.approve_device(BOB);
    world.remove_device(BOB, &phone.id);
    world.deliver(ALICE, BOB, &v0);
    assert!(!mark_of(&world).needs_rotation());

    let junk = SpaceDelivery {
        id: ids::random_v4(&mut world.rng).unwrap(),
        scope_id: SCOPE.into(),
        identifier: ROW.into(),
        key_epoch_id: v0.clone(),
        sender_account_id: MALLORY.into(),
        recipient_account_id: BOB.into(),
        recipient_account_epoch_id: bob_e0.clone(),
        envelope: b"junk".to_vec(),
    };
    let at = world.next();
    world.space.deliveries.push(Accepted::new(at, junk.clone()));
    let mark = mark_of(&world);
    assert!(mark.needs_rotation());
    assert_eq!(mark.superseded.len(), 1);
    assert_eq!(mark.superseded[0].envelope.record_id, junk.id);
    let bob = world.acquire(BOB);
    assert!(
        bob.invalid_deliveries.is_empty(),
        "the real delivery opened first"
    );

    world.rotate(ALICE);
    assert!(!mark_of(&world).needs_rotation());
}

/// An account that holds the current version and is no longer a member
/// marks the space; a rotation clears it and the former member receives
/// nothing of the new version (§3.5).
#[test]
fn a_former_member_holding_the_current_version_marks_the_space() {
    let mut world = space(19, &[ALICE, BOB]);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    world.remove_member(BOB);
    let mark = mark_of(&world);
    assert_eq!(mark.former_members, [BOB]);
    assert!(mark.needs_rotation());

    world.rotate(ALICE);
    assert!(!mark_of(&world).needs_rotation());
    let chain = world.read().chain(&world).unwrap();
    let bob = world.acquire(BOB);
    assert!(bob.keys.holds(&v0));
    assert!(!bob.has_current(&chain));
}

/// Rotation refusals (§3.5, §7): a device without the current version's
/// key is `rotation-needs-key`; a removed device and a device that is not
/// active may not write.
#[test]
fn rotation_preconditions_refuse() {
    let mut world = space(20, &[ALICE, BOB]);
    assert_eq!(
        world.build_rotation(BOB, false).err(),
        Some(refused(RefusalCode::RotationNeedsKey))
    );
    let phone = world.approve_device(ALICE);
    let keys = world.acquire(ALICE).keys;
    let own = world.own_with(ALICE, &phone).unwrap();
    world.remove_device(ALICE, &phone.id);
    let stranger = world.accounts[ALICE].env.device(&mut world.rng);
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let directory = read.directory(&world);
    let after = world.own(ALICE);
    let mut rng = TestRandom(5);
    for (device, code) in [
        (&phone, RefusalCode::DeviceRemoved),
        (&stranger, RefusalCode::DeviceNotActive),
    ] {
        assert_eq!(
            build::rotate(
                &world.writer(ALICE),
                &chain,
                &keys,
                &Author::new(&directory, ALICE, &after.key, device).unwrap(),
                &[],
                &mut rng,
            )
            .err(),
            Some(refused(code))
        );
    }
    // A key of an account epoch that is no longer current.
    assert_eq!(
        build::rotate(
            &world.writer(ALICE),
            &chain,
            &keys,
            &Author::new(&directory, ALICE, &own.key, &world.accounts[ALICE].device).unwrap(),
            &[],
            &mut rng,
        )
        .err(),
        Some(ProtocolError::AccountKeyUnavailable)
    );
    // No key of the current version.
    let empty = SpaceKeys::new(&chain);
    assert_eq!(
        build::rotate(
            &world.writer(ALICE),
            &chain,
            &empty,
            &Author::new(&directory, ALICE, &after.key, &world.accounts[ALICE].device).unwrap(),
            &[],
            &mut rng,
        )
        .err(),
        Some(refused(RefusalCode::RotationNeedsKey))
    );
}

/// A key held from elsewhere (a stored held key, or a creation's own key)
/// authenticates against the chain exactly as an acquired one, and a key
/// of no version of the chain does not.
#[test]
fn held_keys_authenticate_against_the_chain() {
    let mut world = World::new(21);
    world.account(ALICE);
    world.add_member(ALICE);
    let device = world.accounts[ALICE].device.clone();
    let records = world.build_root(ROW, ALICE, &device).unwrap();
    let at = world.next();
    world
        .space
        .roots
        .push(Accepted::new(at, records.root.clone()));
    let v1 = world.rotate(ALICE);
    let chain = world.read().chain(&world).unwrap();
    let mut keys = SpaceKeys::new(&chain);
    let registry = &world.shared.registry;
    assert_eq!(
        keys.authenticate(&chain, registry, &records.root.epoch_id, &records.key)
            .unwrap(),
        KeyCheck::Accepted
    );
    assert!(keys.holds(&records.root.epoch_id));
    assert!(!keys.holds(&v1));
    assert_eq!(
        keys.authenticate(&chain, registry, &v1, &records.key)
            .unwrap(),
        KeyCheck::Invalid
    );
    assert_eq!(
        keys.authenticate(&chain, registry, &records.root.id, &records.key)
            .unwrap(),
        KeyCheck::Invalid
    );
    let rendered = format!("{keys:?} {records:?}");
    assert!(!rendered.contains(&format!("{:?}", *records.key)));
}

/// A rotation delivers first to members that held the version it replaces,
/// then to the others, at most 100 in its own transaction; members without
/// a valid sealing key of the selected mechanism wait (§3.5).
#[test]
fn rotation_recipients_prefer_previous_holders_and_cap_the_first_batch() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let mut world = World::with_registry(22, registry);
    world.account(ALICE);
    world.add_member(ALICE);
    world.create(ALICE);
    let v0 = world.current();
    let others: Vec<String> = (0..DELIVERY_BATCH + 1)
        .map(|index| format!("{index:08x}-0000-5000-8000-000000000001"))
        .collect();
    for account in &others {
        world.account(account);
        world.add_member(account);
    }
    world.account_with(CAROL, sealing_with(OTHER_SEAL));
    world.add_member(CAROL);
    let holders = [&others[40], &others[90]];
    for holder in holders {
        world.deliver(ALICE, holder, &v0);
    }
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let mut rng = TestRandom(9);
    let recipients = rotation_recipients(
        &chain,
        &read.space,
        &read.directory(&world),
        ALICE,
        BUILTIN,
        &mut rng,
    )
    .unwrap();
    assert_eq!(recipients.first.len(), DELIVERY_BATCH);
    assert_eq!(recipients.rest.len(), 1);
    assert_eq!(recipients.waiting, [CAROL]);
    let mut first_two: Vec<_> = recipients.first[..2]
        .iter()
        .map(|recipient| recipient.account_id())
        .collect();
    first_two.sort();
    let mut expected: Vec<_> = holders.iter().map(|holder| holder.as_str()).collect();
    expected.sort();
    assert_eq!(first_two, expected);
    assert!(
        recipients
            .first
            .iter()
            .all(|recipient| recipient.account_id() != ALICE)
    );
    assert!(recipients.rest.iter().all(|account| account != ALICE));
}

/// `X(column, recipient, epoch)` of §3.1, rebuilt here from the formula
/// with the context primitive: the creator's domain, the space policy, the
/// address, `__e2ee_spaces` and the root's row ID.
fn x(creator: &str, root_id: &str, column: &str, recipient: &str, epoch: &str) -> Vec<u8> {
    let application = format!(r#"["{}","test","{creator}"]"#, common::protocol::APP_ID);
    CryptoContext {
        application: &application,
        policy: "jazz.e2ee.space.v1",
        scope: SCOPE,
        identifier: ROW,
        table: "__e2ee_spaces",
        row: root_id,
        column,
        epoch,
        recipient,
    }
    .encode()
    .unwrap()
}

fn unwrap_with(key: &[u8; 32], context: &[u8], envelope: &[u8]) -> [u8; 32] {
    *key_envelope::Builtin
        .unwrap(key, context, envelope)
        .unwrap()
}

fn open_with(account_key: &[u8; 32], context: &[u8], envelope: &[u8]) -> [u8; 32] {
    let pair = derive::account_sealing_key_pair(&key_seal::Builtin, account_key).unwrap();
    *key_seal::Builtin.open(&pair, context, envelope).unwrap()
}

/// The builders write exactly the bytes §3.1 defines: each signature
/// verifies over the signed bytes rebuilt here from the formula, each
/// marker unwraps to `Z32` and each history link to the wrapped key under
/// the formula's context, and each account envelope and delivery opens
/// with the recipient account's sealing key of the named epoch under the
/// formula's context.
#[test]
fn builders_write_the_specified_bytes() {
    let mut world = World::new(23);
    for account in [ALICE, BOB] {
        world.account(account);
        world.add_member(account);
    }
    let device = world.accounts[ALICE].device.clone();
    let alice = world.own(ALICE);
    let alice_epoch = alice.state.epoch_id().to_owned();
    let records = world.build_root(ROW, ALICE, &device).unwrap();
    let at = world.next();
    world
        .space
        .roots
        .push(Accepted::new(at, records.root.clone()));
    let root = &records.root;
    let k0 = *records.key;

    // Root.
    assert_eq!(
        (root.mechanism.as_str(), root.version),
        (key_envelope::MECHANISM.id, key_envelope::MECHANISM.version)
    );
    assert_eq!(root.account_epoch_id, alice_epoch);
    let column = format!(r#"["root","{ALICE}","{}","{alice_epoch}"]"#, device.id);
    let signed = frame_record(&[
        &x(ALICE, &root.id, &column, "", &root.epoch_id),
        &envelope::encode(
            Mechanism::new(&root.mechanism, root.version),
            &root.verification,
        )
        .unwrap(),
        &root.account_envelope,
    ])
    .unwrap();
    let registry = world.shared.registry.clone();
    let registry = &registry;
    assert!(signing::verify(registry, &device.signing.public, &signed, &root.signature).unwrap());
    assert_eq!(
        unwrap_with(
            &k0,
            &x(ALICE, &root.id, "verification", "", &root.epoch_id),
            &root.verification
        ),
        [0; 32]
    );
    let recipient = format!(r#"["{ALICE}","{alice_epoch}"]"#);
    assert_eq!(
        open_with(
            alice.key.key(),
            &x(
                ALICE,
                &root.id,
                "account-envelope",
                &recipient,
                &root.epoch_id
            ),
            &root.account_envelope
        ),
        k0
    );

    // Successor, with its delivery to Bob in the same transaction.
    let rotation = world.build_rotation(ALICE, false).unwrap();
    let successor = rotation.records.successor.clone();
    let k1 = *rotation.records.key;
    assert_eq!(successor.predecessor, root.epoch_id);
    assert_eq!(successor.history_epoch_id, root.epoch_id);
    let author = format!(r#"["{ALICE}","{}","{alice_epoch}"]"#, device.id);
    let role = |name: &str| format!(r#"["successor","{}","{name}"]"#, successor.id);
    let mut signed = x(
        ALICE,
        &root.id,
        &role("signature"),
        &author,
        &successor.epoch_id,
    );
    signed.extend(
        frame_record(&[
            successor.predecessor.as_bytes(),
            successor.history_epoch_id.as_bytes(),
            &successor.verification,
            &successor.history,
            &successor.account_envelope,
        ])
        .unwrap(),
    );
    assert!(
        signing::verify(
            registry,
            &device.signing.public,
            &signed,
            &successor.signature
        )
        .unwrap()
    );
    assert_eq!(
        unwrap_with(
            &k1,
            &x(ALICE, &root.id, "verification", "", &successor.epoch_id),
            &successor.verification
        ),
        [0; 32]
    );
    assert_eq!(
        unwrap_with(
            &k1,
            &x(
                ALICE,
                &root.id,
                &role("history"),
                &successor.history_epoch_id,
                &successor.epoch_id
            ),
            &successor.history
        ),
        k0
    );
    assert_eq!(
        open_with(
            alice.key.key(),
            &x(
                ALICE,
                &root.id,
                &role("account-envelope"),
                &recipient,
                &successor.epoch_id
            ),
            &successor.account_envelope
        ),
        k1
    );
    assert_eq!(rotation.records.deliveries.len(), 1);
    let delivery = &rotation.records.deliveries[0];
    let bob = world.own(BOB);
    let bob_epoch = bob.state.epoch_id();
    assert_eq!(
        (
            delivery.scope_id.as_str(),
            delivery.identifier.as_str(),
            delivery.key_epoch_id.as_str(),
            delivery.sender_account_id.as_str(),
            delivery.recipient_account_id.as_str(),
            delivery.recipient_account_epoch_id.as_str(),
        ),
        (
            SCOPE,
            ROW,
            successor.epoch_id.as_str(),
            ALICE,
            BOB,
            bob_epoch
        )
    );
    assert_eq!(
        open_with(
            bob.key.key(),
            &x(
                ALICE,
                &root.id,
                &format!(r#"["delivery","{}","{ALICE}"]"#, delivery.id),
                &format!(r#"["{BOB}","{bob_epoch}"]"#),
                &successor.epoch_id
            ),
            &delivery.envelope
        ),
        k1
    );
    let v1 = world.accept_rotation(rotation);

    // A recovery rotation by Bob, whose newest held version is v1 while
    // the current one is v2: its history wraps K₁ under a context whose
    // recipient is v1.
    world.remove_member(BOB);
    let v2 = world.rotate(ALICE);
    world.add_member(BOB);
    let recovery = world.build_rotation(BOB, true).unwrap().records;
    let successor = &recovery.successor;
    assert_eq!(successor.predecessor, v2);
    assert_eq!(successor.history_epoch_id, v1);
    let history_column = format!(r#"["successor","{}","history"]"#, successor.id);
    assert_eq!(
        unwrap_with(
            &recovery.key,
            &x(ALICE, &root.id, &history_column, &v1, &successor.epoch_id),
            &successor.history
        ),
        k1
    );
    let bob_device = &world.accounts[BOB].device;
    let bob_author = format!(r#"["{BOB}","{}","{bob_epoch}"]"#, bob_device.id);
    let mut signed = x(
        ALICE,
        &root.id,
        &format!(r#"["successor","{}","signature"]"#, successor.id),
        &bob_author,
        &successor.epoch_id,
    );
    signed.extend(
        frame_record(&[
            successor.predecessor.as_bytes(),
            successor.history_epoch_id.as_bytes(),
            &successor.verification,
            &successor.history,
            &successor.account_envelope,
        ])
        .unwrap(),
    );
    assert!(
        signing::verify(
            registry,
            &bob_device.signing.public,
            &signed,
            &successor.signature
        )
        .unwrap()
    );
}

/// The keys alice holds, checked to fit `chain` and to hold at least one
/// version, so `lacking_held` reaches its per-version checks instead of
/// stopping at the keys.
fn held_keys(world: &World, chain: &SpaceChain) -> SpaceKeys {
    let keys = world.acquire(ALICE).keys;
    assert!(keys.depths().next().is_some(), "alice holds a version");
    assert!(
        chain
            .versions
            .iter()
            .any(|version| keys.epoch_ids().any(|held| held == version.epoch_id)),
        "the held keys belong to this chain"
    );
    keys
}

/// Calls every function that combines a chain with a space ledger (and,
/// where it takes one, an account directory) and expects each to refuse
/// the evidence as not coming from one read.
fn assert_inconsistent(
    world: &World,
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    directory: &AccountDirectory<'_>,
) {
    let inconsistent = Some(ProtocolError::InconsistentEvidence);
    let own = world.own(BOB);
    let mut rng = TestRandom(1);
    assert_eq!(
        members::envelopes(chain, ledger, 0, false).err(),
        inconsistent
    );
    assert_eq!(members::holders(chain, ledger, 0).err(), inconsistent);
    assert_eq!(
        lacking_held(chain, ledger, directory, &held_keys(world, chain)).err(),
        inconsistent
    );
    assert_eq!(rotation_mark(chain, ledger, directory).err(), inconsistent);
    assert_eq!(
        rotation_recipients(chain, ledger, directory, ALICE, BUILTIN, &mut rng).err(),
        inconsistent
    );
    assert_eq!(
        acquire(chain, ledger, directory, &own.epoch_keys).err(),
        inconsistent
    );
}

/// A chain, a space ledger and an account directory from different reads
/// are inconsistent evidence for every function that combines them (§0
/// item 3), never a basis for who lacks a key, the needs-rotation mark or
/// a rotation's recipients. Keys held from an earlier read still serve
/// against a later chain.
#[test]
fn a_chain_ledger_and_directory_of_different_reads_are_inconsistent_evidence() {
    let mut world = space(30, &[ALICE, BOB]);
    let v0 = world.current();
    let phone = world.approve_device(BOB);
    world.deliver(ALICE, BOB, &v0);
    let old = world.read();
    let old_keys = world.acquire(ALICE).keys;
    world.remove_device(BOB, &phone.id);
    let new = world.read();
    let old_chain = old.chain(&world).unwrap();
    let new_chain = new.chain(&world).unwrap();
    let old_directory = old.directory(&world);
    let new_directory = new.directory(&world);
    let registry = &world.shared.registry;
    let mut rng = TestRandom(30);

    // One read: the removal marks the space, and nothing else is amiss.
    assert!(
        rotation_mark(&new_chain, &new.space, &new_directory)
            .unwrap()
            .needs_rotation()
    );
    assert!(
        lacking_held(
            &new_chain,
            &new.space,
            &new_directory,
            &world.acquire(ALICE).keys
        )
        .unwrap()
        .lacking
        .is_empty()
    );
    let recipients = rotation_recipients(
        &new_chain,
        &new.space,
        &new_directory,
        ALICE,
        BUILTIN,
        &mut rng,
    )
    .unwrap();
    assert_eq!(recipients.first.len(), 1);
    assert_eq!(
        members::holders(&new_chain, &new.space, 0).unwrap(),
        [ALICE.to_owned(), BOB.to_owned()].into()
    );
    assert_eq!(
        members::envelopes(&new_chain, &new.space, 0, false)
            .unwrap()
            .len(),
        2
    );
    let own = world.own(BOB);
    assert!(
        acquire(&new_chain, &new.space, &new_directory, &own.epoch_keys)
            .unwrap()
            .has_current(&new_chain)
    );
    assert!(
        !rotation_mark(&old_chain, &old.space, &old_directory)
            .unwrap()
            .needs_rotation(),
        "the earlier read, on its own, shows no removal yet"
    );

    // The older account directory with the newer space ledger and chain,
    // and the reverse.
    let inconsistent = Some(ProtocolError::InconsistentEvidence);
    for (chain, ledger, directory) in [
        (&new_chain, &new.space, &old_directory),
        (&old_chain, &old.space, &new_directory),
    ] {
        assert_eq!(rotation_mark(chain, ledger, directory).err(), inconsistent);
        assert_eq!(
            lacking_held(chain, ledger, directory, &held_keys(&world, chain)).err(),
            inconsistent
        );
        assert_eq!(
            rotation_recipients(chain, ledger, directory, ALICE, BUILTIN, &mut rng).err(),
            inconsistent
        );
    }
    // A chain of one read with the space ledger of the other.
    assert_inconsistent(&world, &old_chain, &new.space, &new_directory);
    assert_inconsistent(&world, &new_chain, &old.space, &old_directory);
    // A ledger of another address at the same cut.
    let mut other = new.space.clone();
    other.address = SpaceAddress::new(SCOPE, OTHER_ROW).unwrap();
    assert_inconsistent(&world, &new_chain, &other, &new_directory);

    // Keys held from the earlier read authenticate against the later
    // chain, and a rotation over it uses them.
    let mut keys = SpaceKeys::new(&old_chain);
    assert_eq!(
        keys.authenticate(&new_chain, registry, &v0, old_keys.key(&v0).unwrap())
            .unwrap(),
        KeyCheck::Accepted
    );
    let alice = world.own(ALICE);
    let rotation = build::rotate(
        &world.writer(ALICE),
        &new_chain,
        &old_keys,
        &Author::new(
            &new_directory,
            ALICE,
            &alice.key,
            &world.accounts[ALICE].device,
        )
        .unwrap(),
        &recipients.first,
        &mut rng,
    )
    .unwrap();
    assert_eq!(rotation.successor.predecessor, v0);
}

/// A row beyond the read's cut is inconsistent evidence for every
/// function that combines a chain with that ledger, as for the replay: a
/// delivery the read does not contain does not stop the member lacking
/// the version.
#[test]
fn a_row_beyond_the_cut_is_inconsistent_evidence() {
    let mut world = space(31, &[ALICE, BOB]);
    let v0 = world.current();
    let delivery = world.build_delivery(ALICE, BOB, &v0).unwrap();
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let directory = read.directory(&world);
    assert_eq!(
        lacking_held(&chain, &read.space, &directory, &world.acquire(ALICE).keys)
            .unwrap()
            .lacking
            .into_iter()
            .map(|entry| (entry.account_id, entry.epoch_ids))
            .collect::<Vec<_>>(),
        [(BOB.to_owned(), vec![v0.clone()])]
    );

    let mut beyond = read.space.clone();
    beyond
        .deliveries
        .push(Accepted::new(Position(beyond.through.0 + 1), delivery));
    assert_eq!(
        SpaceChain::replay(&beyond, &directory),
        Err(ProtocolError::InconsistentEvidence)
    );
    assert_inconsistent(&world, &chain, &beyond, &directory);
}

/// A table that repeats a row ID is not one read: every function that
/// combines a chain with the ledger refuses it.
#[test]
fn duplicate_rows_are_inconsistent_evidence_for_the_member_helpers() {
    let mut world = space(32, &[ALICE, BOB]);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    let directory = read.directory(&world);
    assert!(
        lacking_held(&chain, &read.space, &directory, &world.acquire(ALICE).keys)
            .unwrap()
            .lacking
            .is_empty()
    );

    let mut twice_delivered = read.space.clone();
    let repeated = twice_delivered.deliveries[0].clone();
    twice_delivered.deliveries.push(repeated);
    let mut twice_a_member = read.space.clone();
    let repeated = twice_a_member.members[1].clone();
    twice_a_member.members.push(repeated);
    for ledger in [&twice_delivered, &twice_a_member] {
        assert_inconsistent(&world, &chain, ledger, &directory);
    }
}

/// Alice's space with `DELIVERY_BATCH + 1` other members, each with a valid
/// sealing key.
fn crowded(seed: u64) -> (World, Vec<String>) {
    let mut world = space(seed, &[ALICE]);
    let others: Vec<String> = (0..=DELIVERY_BATCH)
        .map(|index| format!("{index:08x}-0000-5000-8000-0000000000c0"))
        .collect();
    for account in &others {
        world.account(account);
        world.add_member(account);
    }
    (world, others)
}

/// A rotation puts at most 100 deliveries in its own transaction (§3.5):
/// the builder refuses a larger first batch before generating anything,
/// the successor with the first batch is accepted, and the rotator then
/// delivers to the remaining members in later transactions, also to a
/// member that already has a delivery of the new version that does not
/// open. Every member then holds the new version. For an ordinary and a
/// recovery rotation.
#[test]
fn a_rotation_delivers_to_the_first_batch_then_to_the_rest() {
    for recovery in [false, true] {
        let (mut world, others) = crowded(33 + u64::from(recovery));
        let rotator = if recovery {
            // Bob holds only v0; Alice rotates to v1 while he is no
            // member, so his newest version is behind.
            world.account(BOB);
            world.add_member(BOB);
            let v0 = world.current();
            world.deliver(ALICE, BOB, &v0);
            world.remove_member(BOB);
            let staged = world.build_rotation(ALICE, false).unwrap();
            assert_eq!(staged.records.deliveries.len(), DELIVERY_BATCH);
            assert_eq!(staged.rest.len(), 1);
            world.accept_rotation(staged);
            assert!(
                lacking_of(&world).is_empty(),
                "the rest was delivered after"
            );
            world.add_member(BOB);
            BOB
        } else {
            ALICE
        };
        let previous = world.current();

        let mut rng = TestRandom(33);
        let (records, rest) = {
            let read = world.read();
            let chain = read.chain(&world).unwrap();
            let directory = read.directory(&world);
            let keys = world.acquire(rotator).keys;
            let own = world.own(rotator);
            let writer = world.writer(rotator);
            let author = Author::new(
                &directory,
                rotator,
                &own.key,
                &world.accounts[rotator].device,
            )
            .unwrap();
            let recipients =
                rotation_recipients(&chain, &read.space, &directory, rotator, BUILTIN, &mut rng)
                    .unwrap();
            assert_eq!(recipients.first.len(), DELIVERY_BATCH);
            assert!(!recipients.rest.is_empty());
            let build = |recipients: &[Recipient], rng: &mut TestRandom| {
                if recovery {
                    build::recover_rotation(&writer, &chain, &keys, &author, recipients, rng)
                } else {
                    build::rotate(&writer, &chain, &keys, &author, recipients, rng)
                }
            };
            let everyone: Vec<Recipient> = recipients
                .first
                .iter()
                .cloned()
                .chain(
                    recipients
                        .rest
                        .iter()
                        .map(|account| directory.recipient(account, BUILTIN).unwrap().unwrap()),
                )
                .collect();
            let before = rng.0;
            assert!(matches!(
                build(&everyone, &mut rng),
                Err(ProtocolError::InvalidRecord(_))
            ));
            assert_eq!(rng.0, before, "nothing was generated");
            let records = build(&recipients.first, &mut rng).unwrap();
            assert_eq!(records.deliveries.len(), DELIVERY_BATCH);
            assert_eq!(records.successor.predecessor, previous);
            (records, recipients.rest)
        };

        let new = records.successor.epoch_id.clone();
        let at = world.next();
        world
            .space
            .successors
            .push(Accepted::new(at, records.successor));
        for delivery in records.deliveries {
            world.space.deliveries.push(Accepted::new(at, delivery));
        }
        assert_eq!(world.current(), new);
        let lacking_new = |world: &World| {
            let mut accounts: Vec<String> = lacking_of(world)
                .into_iter()
                .filter(|(_, epochs)| epochs.contains(&new))
                .map(|(account, _)| account)
                .collect();
            accounts.sort();
            accounts
        };
        let mut rest_accounts = rest.clone();
        rest_accounts.sort();
        assert_eq!(lacking_new(&world), rest_accounts);

        // A member of the first batch delivers the new version to one of
        // the rest, and that delivery does not open: holders cannot see it.
        let garbled = rest[0].clone();
        let holder = others
            .iter()
            .find(|account| !rest_accounts.contains(account))
            .unwrap()
            .clone();
        let mut garbage = world.build_delivery(&holder, &garbled, &new).unwrap();
        let last = garbage.envelope.len() - 1;
        garbage.envelope[last] ^= 1;
        let at = world.next();
        world.space.deliveries.push(Accepted::new(at, garbage));
        assert!(!lacking_new(&world).contains(&garbled));

        // The rotator delivers to every remaining member regardless.
        for batch in rest.chunks(DELIVERY_BATCH) {
            let deliveries = world
                .build_deliveries(rotator, &new, batch)
                .unwrap()
                .expect("the rotator holds the new version");
            assert_eq!(deliveries.len(), batch.len());
            let at = world.next();
            for delivery in deliveries {
                world.space.deliveries.push(Accepted::new(at, delivery));
            }
        }
        assert!(lacking_new(&world).is_empty());

        let read = world.read();
        let chain = read.chain(&world).unwrap();
        let mut accounts = others.clone();
        accounts.push(ALICE.to_owned());
        if recovery {
            accounts.push(BOB.to_owned());
        }
        for account in &accounts {
            let own = world.own(account);
            let acquisition = acquire(
                &chain,
                &read.space,
                &read.directory(&world),
                &own.epoch_keys,
            )
            .unwrap();
            assert!(acquisition.has_current(&chain), "{account}");
            let reported = usize::from(*account == garbled);
            assert_eq!(acquisition.invalid_deliveries.len(), reported, "{account}");
        }
    }
}

/// A root whose check needs a signing mechanism this reader lacks, before
/// any valid root, leaves the choice unresolved: the reader does not take
/// the later valid root (§3.2). Malformed and inactive earlier roots need
/// no capability and are discarded.
#[test]
fn an_unsupported_root_before_a_valid_root_leaves_the_choice_unresolved() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let mut world = World::with_registry(40, registry);
    world.account(ALICE);
    world.account_with(
        BOB,
        MechanismConfig {
            signature: OTHER_SIGN.into(),
            ..MechanismConfig::default()
        },
    );
    let device = world.accounts[ALICE].device.clone();
    let mut unsigned = world.build_root(ROW, ALICE, &device).unwrap().root;
    unsigned.signature.clear();
    let at = world.next();
    world.space.roots.push(Accepted::new(at, unsigned.clone()));
    // By a device removed before the root is accepted.
    let phone = world.approve_device(ALICE);
    let by_phone = world.build_root(ROW, ALICE, &phone).unwrap().root;
    world.remove_device(ALICE, &phone.id);
    let at = world.next();
    world.space.roots.push(Accepted::new(at, by_phone.clone()));
    let bob_device = world.accounts[BOB].device.clone();
    let bobs = world.build_root(ROW, BOB, &bob_device).unwrap().root;
    let at = world.next();
    world.space.roots.push(Accepted::new(at, bobs.clone()));
    let alices_at = world.create(ALICE);
    let alices = world.space.roots.last().unwrap().row.id.clone();

    // A reader with the mechanism: Bob's root is the space's root.
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    assert_eq!(chain.root.row.id, bobs.id);
    let reasons: Vec<_> = chain
        .lost
        .iter()
        .map(|root| (root.root_id.clone(), root.invalid))
        .collect();
    assert_eq!(
        reasons,
        [
            (unsigned.id.clone(), Some(InvalidSpaceRoot::Malformed)),
            (by_phone.id.clone(), Some(InvalidSpaceRoot::NotActive)),
            (alices.clone(), None),
        ]
    );

    // A reader without it does not skip Bob's root.
    let builtin = Registry::builtin();
    let directory = AccountDirectory::new(
        &read.ledgers,
        &world.shared.domain,
        &builtin,
        read.space.through,
    )
    .unwrap();
    let unresolved = Some(ProtocolError::Unsupported(
        UnsupportedCapability::SigningMechanism,
    ));
    assert_eq!(select_root(&read.space, &directory).err(), unresolved);
    assert_eq!(
        SpaceChain::replay(&read.space, &directory).err(),
        unresolved
    );

    // Without Bob's root, the same reader discards the malformed and
    // inactive roots and chooses Alice's.
    let mut without = read.space.clone();
    without.roots.retain(|root| root.row.id != bobs.id);
    let chain = SpaceChain::replay(&without, &directory).unwrap().unwrap();
    assert_eq!(chain.root.row.id, alices);
    assert_eq!(chain.root.position, alices_at);
    let reasons: Vec<_> = chain.lost.iter().map(|root| root.invalid).collect();
    assert_eq!(
        reasons,
        [
            Some(InvalidSpaceRoot::Malformed),
            Some(InvalidSpaceRoot::NotActive)
        ]
    );
}

#[derive(Debug, Clone, Copy)]
enum Damage {
    /// The history ciphertext does not open.
    HistoryCiphertext,
    /// The history opens, under the right context, to a key that is not
    /// the predecessor's.
    HistoryOpensAnotherKey,
    /// The marker opens, under the right context, to something other than
    /// `Z32`.
    MarkerNotZero,
}

/// A successor whose signature verifies but whose history link or marker
/// is wrong extends the chain (the signature covers what it is given), yet
/// no key of it is accepted: not from the author's own envelope, not from
/// a delivery, not as a held key (§3.3). A wrong marker makes the delivery
/// invalid; a wrong history link is the successor's fault, reported as a
/// broken history naming it and its author, never as an invalid delivery
/// (§3.4). A held key re-checked against the chain gives the same broken
/// history, not a plain invalid key.
#[test]
fn a_signed_successor_with_a_wrong_history_or_marker_gives_no_key() {
    for (seed, damage) in [
        (41, Damage::HistoryCiphertext),
        (42, Damage::HistoryOpensAnotherKey),
        (43, Damage::MarkerNotZero),
    ] {
        let mut world = space(seed, &[ALICE, BOB]);
        let v0 = world.current();
        world.deliver(ALICE, BOB, &v0);
        let chain = world.read().chain(&world).unwrap();
        let mut rotation = world.build_rotation(ALICE, false).unwrap();
        let k1 = *rotation.records.key;
        let successor = &mut rotation.records.successor;
        let mut rng = TestRandom(seed);
        match damage {
            Damage::HistoryCiphertext => {
                let last = successor.history.len() - 1;
                successor.history[last] ^= 1;
            }
            Damage::HistoryOpensAnotherKey => {
                let context = successor
                    .history_context(&chain.application, &chain.scope)
                    .unwrap();
                successor.history = key_envelope::Builtin
                    .wrap(&k1, &context, &[7; 32], &mut rng)
                    .unwrap();
            }
            Damage::MarkerNotZero => {
                let context = successor
                    .marker_context(&chain.application, &chain.scope)
                    .unwrap();
                successor.verification = key_envelope::Builtin
                    .wrap(&k1, &context, &[1; 32], &mut rng)
                    .unwrap();
            }
        }
        resign(&world, ALICE, successor);
        let successor_id = successor.id.clone();
        let v1 = world.accept_rotation(rotation);
        let chain = world.read().chain(&world).unwrap();
        assert_eq!(chain.current().unwrap().epoch_id, v1, "{damage:?}");

        let bob = world.acquire(BOB);
        assert!(bob.keys.holds(&v0), "{damage:?}");
        assert!(!bob.keys.holds(&v1), "{damage:?}");
        let alice = world.acquire(ALICE);
        assert!(!alice.keys.holds(&v1), "{damage:?}");
        let held_check = if matches!(damage, Damage::MarkerNotZero) {
            assert_eq!(bob.invalid_deliveries.len(), 1, "{damage:?}");
            assert_eq!(bob.invalid_deliveries[0].key_epoch_id, v1);
            assert!(bob.broken_history.is_empty(), "{damage:?}");
            assert!(alice.broken_history.is_empty(), "{damage:?}");
            KeyCheck::Invalid
        } else {
            assert!(bob.invalid_deliveries.is_empty(), "{damage:?}");
            let broken = BrokenHistory {
                space: address(),
                key_epoch_id: v1.clone(),
                successor_id: successor_id.clone(),
                author_account_id: ALICE.to_owned(),
            };
            assert_eq!(
                bob.broken_history,
                std::slice::from_ref(&broken),
                "{damage:?}"
            );
            assert_eq!(
                alice.broken_history,
                std::slice::from_ref(&broken),
                "{damage:?}"
            );
            KeyCheck::BrokenHistory(broken)
        };
        let mut keys = SpaceKeys::new(&chain);
        assert_eq!(
            keys.authenticate(&chain, &world.shared.registry, &v1, &k1)
                .unwrap(),
            held_check,
            "{damage:?}"
        );
        assert!(keys.depths().next().is_none(), "{damage:?}");
    }
}

/// A delivery sealed with a mechanism this reader does not route is
/// skipped as unsupported, not reported, and a later delivery of the same
/// version that it can open gives the key (§3.3, §3.4).
#[test]
fn an_unrouted_delivery_is_skipped_for_a_later_supported_one() {
    let mut world = space(44, &[ALICE, BOB]);
    let v0 = world.current();
    let mut unrouted = world.build_delivery(ALICE, BOB, &v0).unwrap();
    unrouted.envelope = relabelled(key_seal::MECHANISM, UNROUTED_SEAL, &unrouted.envelope);
    let at = world.next();
    world
        .space
        .deliveries
        .push(Accepted::new(at, unrouted.clone()));
    let chain = world.read().chain(&world).unwrap();
    let bob = world.acquire(BOB);
    assert!(!bob.has_current(&chain));
    assert!(bob.invalid_deliveries.is_empty());
    assert_eq!(bob.unsupported, std::slice::from_ref(&unrouted.id));

    world.deliver(ALICE, BOB, &v0);
    let bob = world.acquire(BOB);
    assert!(bob.has_current(&chain));
    assert!(bob.invalid_deliveries.is_empty());
    assert_eq!(bob.unsupported, std::slice::from_ref(&unrouted.id));
}

/// A successor whose marker or history link is wrapped with a mechanism
/// this reader does not route gives no key and is reported as
/// unsupported, never as an invalid delivery (§3.3).
#[test]
fn an_unrouted_marker_or_history_is_unsupported_not_invalid() {
    for (seed, history) in [(45, false), (46, true)] {
        let mut world = space(seed, &[ALICE, BOB]);
        let v0 = world.current();
        world.deliver(ALICE, BOB, &v0);
        let mut rotation = world.build_rotation(ALICE, false).unwrap();
        let k1 = *rotation.records.key;
        let successor = &mut rotation.records.successor;
        if history {
            successor.history =
                relabelled(key_envelope::MECHANISM, UNROUTED_WRAP, &successor.history);
        } else {
            successor.verification = relabelled(
                key_envelope::MECHANISM,
                UNROUTED_WRAP,
                &successor.verification,
            );
        }
        resign(&world, ALICE, successor);
        let successor_id = successor.id.clone();
        let to_bob = rotation.records.deliveries[0].id.clone();
        let v1 = world.accept_rotation(rotation);
        let chain = world.read().chain(&world).unwrap();
        assert_eq!(chain.current().unwrap().epoch_id, v1);

        let bob = world.acquire(BOB);
        assert!(bob.keys.holds(&v0));
        assert!(!bob.keys.holds(&v1));
        assert!(bob.invalid_deliveries.is_empty());
        assert_eq!(bob.unsupported, [to_bob]);
        let alice = world.acquire(ALICE);
        assert!(!alice.keys.holds(&v1));
        assert_eq!(alice.unsupported, [successor_id]);
        let mut keys = SpaceKeys::new(&chain);
        assert_eq!(
            keys.authenticate(&chain, &world.shared.registry, &v1, &k1)
                .unwrap(),
            KeyCheck::Unsupported
        );
    }
}

/// A provider failure while checking a marker, unwrapping a history link
/// or verifying a space record's signature fails the operation as a
/// provider failure: never an invalid key, delivery or record (§0, §3.3).
#[test]
fn provider_failures_at_markers_history_and_signatures_fail_the_operation() {
    for (seed, history) in [(47, false), (48, true)] {
        let (registry, wrap_fails, _) = flaky_registry();
        let mut world = World::with_registry(seed, registry);
        for account in [ALICE, BOB] {
            world.account(account);
            world.add_member(account);
        }
        world.create(ALICE);
        let v0 = world.current();
        world.deliver(ALICE, BOB, &v0);
        let k0 = *world.acquire(ALICE).keys.key(&v0).unwrap();
        let chain = world.read().chain(&world).unwrap();
        let mut rotation = world.build_rotation(ALICE, false).unwrap();
        let k1 = *rotation.records.key;
        let successor = &mut rotation.records.successor;
        let flaky = FlakyWrapper {
            fails: wrap_fails.clone(),
        };
        let mut rng = TestRandom(seed);
        if history {
            let context = successor
                .history_context(&chain.application, &chain.scope)
                .unwrap();
            successor.history = flaky.wrap(&k1, &context, &k0, &mut rng).unwrap();
        } else {
            let context = successor
                .marker_context(&chain.application, &chain.scope)
                .unwrap();
            successor.verification = flaky.wrap(&k1, &context, &[0; 32], &mut rng).unwrap();
        }
        resign(&world, ALICE, successor);
        let v1 = world.accept_rotation(rotation);

        let read = world.read();
        let chain = read.chain(&world).unwrap();
        let own = world.own(BOB);
        let registry = &world.shared.registry;
        assert!(
            acquire(
                &chain,
                &read.space,
                &read.directory(&world),
                &own.epoch_keys
            )
            .unwrap()
            .keys
            .holds(&v1),
            "supported and valid while the provider works"
        );
        wrap_fails.store(true, std::sync::atomic::Ordering::SeqCst);
        let failure = Some(ProtocolError::ProviderFailure);
        assert_eq!(
            acquire(
                &chain,
                &read.space,
                &read.directory(&world),
                &own.epoch_keys
            )
            .err(),
            failure
        );
        let mut keys = SpaceKeys::new(&chain);
        assert_eq!(keys.authenticate(&chain, registry, &v1, &k1).err(), failure);
        assert_eq!(
            world.build_rotation(BOB, false).err(),
            failure,
            "never mistaken for a missing key"
        );
    }

    // Bob signs with the flaky mechanism: a successor of the space, and the
    // root of another space.
    let (registry, _, sign_fails) = flaky_registry();
    let mut world = World::with_registry(49, registry);
    world.account(ALICE);
    world.account_with(
        BOB,
        MechanismConfig {
            signature: FLAKY_SIGN.into(),
            ..MechanismConfig::default()
        },
    );
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    let v1 = world.rotate(BOB);
    world.add_member_of(OTHER_ROW, BOB);
    world.create_of(OTHER_ROW, BOB);
    let read = world.read();
    let other = world.read_of(SpaceAddress::new(SCOPE, OTHER_ROW).unwrap());
    assert_eq!(read.chain(&world).unwrap().current().unwrap().epoch_id, v1);
    assert_eq!(other.chain(&world).unwrap().creator(), BOB);

    sign_fails.store(true, std::sync::atomic::Ordering::SeqCst);
    for read in [&read, &other] {
        let directory = read.directory(&world);
        assert_eq!(
            SpaceChain::replay(&read.space, &directory).err(),
            Some(ProtocolError::ProviderFailure)
        );
    }
}

/// A device removal in an account generation that a reset has since
/// replaced still marks the space: the removal is looked for in that
/// generation's chain as it stood at the reset (§3.5). A reset alone marks
/// it too: the version was sealed to the reset generation, whose devices
/// can still open it, and a reset is what an account does when its devices
/// are lost.
#[test]
fn a_removal_before_a_reset_keeps_the_mark() {
    let mut world = space(60, &[ALICE, BOB]);
    let v0 = world.current();
    let phone = world.approve_device(BOB);
    world.deliver(ALICE, BOB, &v0);
    let removal = world.remove_device(BOB, &phone.id);
    assert!(mark_of(&world).needs_rotation());
    world.reset(BOB);
    let mark = mark_of(&world);
    assert!(mark.needs_rotation());
    assert!(mark.former_members.is_empty());
    assert_eq!(mark.superseded.len(), 1);
    assert_eq!(mark.superseded[0].envelope.account_id, BOB);
    assert_eq!(mark.superseded[0].successor_id, removal);
    assert_eq!(mark.superseded[0].removed_device_id, phone.id);

    let mut world = space(61, &[ALICE, BOB]);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    world.reset(BOB);
    let mark = mark_of(&world);
    assert!(mark.needs_rotation());
    assert!(mark.superseded.is_empty());
    assert_eq!(mark.reset.len(), 1);
    assert_eq!(mark.reset[0].account_id, BOB);

    // Rotating answers it: the new version goes to Bob's new generation.
    world.rotate(ALICE);
    assert!(!mark_of(&world).needs_rotation());
}

/// Two valid successors of the current version at one position: the one
/// with the lower ID is the next version (§3.3).
#[test]
fn successors_at_one_position_are_ordered_by_id() {
    let mut world = space(62, &[ALICE, BOB]);
    let v0 = world.current();
    let one = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    let two = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    let (low, high) = if one.id < two.id {
        (one, two)
    } else {
        (two, one)
    };
    let at = world.next();
    world.space.successors.push(Accepted::new(at, high.clone()));
    world.space.successors.push(Accepted::new(at, low.clone()));
    let chain = world.read().chain(&world).unwrap();
    let epochs: Vec<_> = chain.versions.iter().map(|v| v.epoch_id.clone()).collect();
    assert_eq!(epochs, [v0, low.epoch_id.clone()]);
    assert_eq!(chain.versions[1].successor.as_ref().unwrap().row.id, low.id);
}

/// Two valid sealing key records of one epoch and mechanism at one
/// position: a sender seals to the one with the lower ID (§2.12).
#[test]
fn sealing_key_records_at_one_position_are_ordered_by_id() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let mut world = World::with_registry(63, registry);
    world.account(ALICE);
    world.account_with(BOB, sealing_with(OTHER_SEAL));
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    let v0 = world.current();
    let own = world.own(BOB);
    let ids = world.with_account(BOB, |entry, rng| {
        let public_key = derive::account_sealing_key_pair(&key_seal::Builtin, own.key.key())
            .unwrap()
            .public
            .bytes;
        let mut records: Vec<_> = (0..2)
            .map(|_| {
                sealing_key_record(
                    &entry.env,
                    &entry.device,
                    own.state.epoch_id(),
                    BUILTIN,
                    public_key.clone(),
                    rng,
                )
            })
            .collect();
        records.sort_by(|a, b| a.id.cmp(&b.id));
        let at = entry.ledger.next();
        for record in records.iter().rev() {
            entry
                .ledger
                .public
                .account_keys
                .push(Accepted::new(at, record.clone()));
        }
        records
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>()
    });
    let read = world.read();
    let directory = read.directory(&world);
    let recipient = directory.recipient(BOB, BUILTIN).unwrap().unwrap();
    assert_eq!(recipient.sealing_key().row.id, ids[0]);
    let valid: Vec<_> = directory
        .sealing_keys(BOB)
        .unwrap()
        .unwrap()
        .valid()
        .iter()
        .filter(|record| record.row.mechanism == BUILTIN.id)
        .map(|record| record.row.id.clone())
        .collect();
    assert_eq!(valid, ids);
    world.deliver(ALICE, BOB, &v0);
    let chain = world.read().chain(&world).unwrap();
    assert!(world.acquire(BOB).has_current(&chain));
}

/// The member helpers over a read made with `registry`, at the world's
/// current cut, with every account's ledger except `left_out`.
struct Helpers {
    lacking: ProtocolResult<members::LackingMembers>,
    mark: ProtocolResult<members::RotationMark>,
    recipients: ProtocolResult<members::RotationRecipients>,
}

fn helpers(world: &World, registry: &Registry, left_out: Option<&str>) -> Helpers {
    let read = world.read();
    let ledgers: Vec<_> = read
        .ledgers
        .iter()
        .filter(|ledger| Some(ledger.account_id.as_str()) != left_out)
        .cloned()
        .collect();
    let directory =
        AccountDirectory::new(&ledgers, &world.shared.domain, registry, read.space.through)
            .unwrap();
    let chain = SpaceChain::replay(&read.space, &directory)
        .unwrap()
        .unwrap();
    let mut rng = TestRandom(1);
    Helpers {
        lacking: lacking_held(&chain, &read.space, &directory, &world.acquire(ALICE).keys),
        mark: rotation_mark(&chain, &read.space, &directory),
        recipients: rotation_recipients(&chain, &read.space, &directory, ALICE, BUILTIN, &mut rng),
    }
}

/// A member whose account history needs a capability this client lacks
/// (here, Bob's devices sign with a mechanism the reader does not have) is
/// skipped and reported as unsupported, never as invalid; the others are
/// still served: Carol lacks the version, is a rotation recipient and
/// receives it. Bob holds the current version under an account epoch a
/// device removal superseded: a client that can read Bob's history marks
/// the space, and one that cannot reports Bob and does not set the mark, so
/// it does not rotate on every write while newer clients keep delivering
/// to him. A member whose account rows the caller left out of the read is
/// still inconsistent evidence (§3.4, "A member this client cannot
/// evaluate").
///
/// ```text
/// alice ──root──► space            members: alice, bob, carol
/// alice ──delivery(v0)──► bob      bob removes a device (other-sign)
/// reader (built-ins) ── lacking: carol; unsupported: bob; mark: unset
/// alice ──delivery(v0)──► carol
/// ```
#[test]
fn a_member_whose_history_this_client_cannot_read_is_skipped_and_reported() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let mut world = World::with_registry(64, registry);
    world.account(ALICE);
    world.account_with(
        BOB,
        MechanismConfig {
            signature: OTHER_SIGN.into(),
            ..MechanismConfig::default()
        },
    );
    world.account(CAROL);
    for account in [ALICE, BOB, CAROL] {
        world.add_member(account);
    }
    world.create(ALICE);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    let phone = world.approve_device(BOB);
    world.remove_device(BOB, &phone.id);

    // A reader with Bob's signing mechanism evaluates him: the removal
    // marks the space.
    let full = helpers(&world, &world.shared.registry, None);
    assert!(full.mark.unwrap().needs_rotation());
    let lacks = full.lacking.unwrap();
    assert!(lacks.unsupported.is_empty());
    assert_eq!(lacks.lacking.len(), 1);
    assert_eq!(lacks.lacking[0].account_id, CAROL);

    // A reader without it skips Bob and serves Carol.
    let builtin = Registry::builtin();
    let unsupported = vec![members::UnsupportedMember {
        account_id: BOB.to_owned(),
        capability: UnsupportedCapability::SigningMechanism,
    }];
    let partial = helpers(&world, &builtin, None);
    let lacks = partial.lacking.unwrap();
    assert_eq!(
        lacks
            .lacking
            .iter()
            .map(|entry| (entry.account_id.as_str(), entry.epoch_ids.clone()))
            .collect::<Vec<_>>(),
        [(CAROL, vec![v0.clone()])]
    );
    assert_eq!(lacks.unsupported, unsupported);
    let recipients = partial.recipients.unwrap();
    assert_eq!(
        recipients
            .first
            .iter()
            .map(|recipient| recipient.account_id())
            .collect::<Vec<_>>(),
        [CAROL]
    );
    assert!(recipients.waiting.is_empty());
    assert_eq!(recipients.unsupported, unsupported);
    let mark = partial.mark.unwrap();
    assert!(!mark.needs_rotation());
    assert_eq!(mark.unsupported, unsupported);

    world.deliver(ALICE, CAROL, &v0);
    let chain = world.read().chain(&world).unwrap();
    assert!(world.acquire(CAROL).has_current(&chain));
    let lacks = helpers(&world, &builtin, None).lacking.unwrap();
    assert!(lacks.lacking.is_empty());
    assert_eq!(lacks.unsupported, unsupported);

    // Carol's account rows left out of the read: inconsistent evidence,
    // not a member to skip.
    let missing = helpers(&world, &builtin, Some(CAROL));
    let inconsistent = Some(ProtocolError::InconsistentEvidence);
    assert_eq!(missing.lacking.err(), inconsistent);
    assert_eq!(missing.mark.err(), inconsistent);
    assert_eq!(missing.recipients.err(), inconsistent);
}

/// If the authority returns a damaged successor in the next delivery
/// batch, Alice cannot reuse the chain she verified in the first batch.
#[test]
fn delivery_chain_reuse_refuses_a_changed_successor() {
    let mut world = space(68, &[ALICE, BOB]);
    let root = world.current();
    let epoch = world.rotate(ALICE);
    let mut read = world.read();
    let cached =
        DeliveryChain::replay(&read.space, &read.directory(&world), &world.shared.registry)
            .unwrap()
            .unwrap();
    assert_eq!(cached.verified().last().epoch_id, epoch);

    // Keep the author and position fixed to isolate the successor evidence.
    read.space.successors[0].row.signature[40] ^= 1;
    assert_eq!(read.chain(&world).unwrap().last().epoch_id, root);
    assert!(
        cached
            .reuse(&read.space, &read.directory(&world))
            .unwrap()
            .is_none(),
        "the cached successor would bypass the failed signature",
    );
}

/// Alice's deliveries may reuse a chain only while its author's account
/// history at the signed records' positions still matches.
#[test]
fn delivery_chain_reuse_refuses_changed_author_history() {
    let mut world = space(69, &[ALICE, BOB]);
    world.approve_device(ALICE);
    world.rotate(ALICE);
    let mut read = world.read();
    let cached =
        DeliveryChain::replay(&read.space, &read.directory(&world), &world.shared.registry)
            .unwrap()
            .unwrap();
    assert!(
        cached
            .reuse(&read.space, &read.directory(&world))
            .unwrap()
            .is_some()
    );

    let alice = read
        .ledgers
        .iter_mut()
        .find(|ledger| ledger.account_id == ALICE)
        .unwrap();
    assert_eq!(alice.approvals.len(), 1);
    alice.approvals.clear();
    assert!(
        cached
            .reuse(&read.space, &read.directory(&world))
            .unwrap()
            .is_none()
    );
}

/// Alice's current-epoch read covers its holders, mark and rotation
/// recipients. A different epoch cannot cover those recipients, and the
/// bounded read cannot stand in for the whole history.
#[test]
fn an_epoch_read_cannot_claim_whole_space_coverage() {
    let mut world = space(66, &[ALICE, BOB]);
    let root = world.current();
    let epoch = world.rotate(ALICE);
    let whole = world.read();
    let directory = whole.directory(&world);
    let chain = whole.chain(&world).unwrap();
    let current = whole.space.clone().for_epochs([epoch].into());
    let previous = whole.space.clone().for_epochs([root].into());
    let inconsistent = Some(ProtocolError::InconsistentEvidence);
    assert!(members::holders(&chain, &current, 1).is_ok());
    assert_eq!(
        rotation_mark(&chain, &current, &directory),
        rotation_mark(&chain, &whole.space, &directory)
    );
    assert_eq!(members::holders(&chain, &current, 0).err(), inconsistent);
    assert_eq!(
        members::envelopes(&chain, &current, 1, false).err(),
        inconsistent
    );
    let recipients = |read| {
        rotation_recipients(
            &chain,
            read,
            &directory,
            ALICE,
            BUILTIN,
            &mut TestRandom(66),
        )
    };
    assert_eq!(recipients(&current), recipients(&whole.space));
    assert_eq!(recipients(&previous).err(), inconsistent);
}

/// Alice and Bob produce identical recipients and successor records when
/// recipient selection reads only the current epoch, across membership and
/// device changes and during Bob's recovery from an older key.
#[test]
fn rotation_records_agree_with_current_epoch_recipient_evidence() {
    let mut world = space(67, &[ALICE, BOB]);
    world.rotate(ALICE);
    let compare = |world: &World, rotator, history: Option<&str>| {
        let whole = world.read();
        let mut bounded = world.read();
        let chain = whole.chain(world).unwrap();
        let directory = whole.directory(world);
        bounded.space = bounded
            .space
            .for_epochs([chain.last().epoch_id.clone()].into());
        let own = world.own(rotator);
        let full_keys = acquire(&chain, &whole.space, &directory, &own.epoch_keys)
            .unwrap()
            .keys;
        let mut own_deliveries =
            SpaceLedger::one_account(whole.space.address.clone(), whole.space.through, rotator);
        own_deliveries.deliveries.extend(
            whole
                .space
                .deliveries
                .iter()
                .filter(|delivery| {
                    let row = &delivery.row;
                    row.recipient_account_id == rotator
                        && (row.key_epoch_id == chain.last().epoch_id
                            || history == Some(&row.key_epoch_id))
                })
                .cloned(),
        );
        let scoped_keys = acquire(&chain, &own_deliveries, &directory, &own.epoch_keys)
            .unwrap()
            .keys;
        let candidates = |read| {
            rotation_recipients(
                &chain,
                read,
                &directory,
                rotator,
                BUILTIN,
                &mut TestRandom(67),
            )
            .unwrap()
        };
        assert_eq!(candidates(&bounded.space), candidates(&whole.space));
        let rotate = |read, keys| {
            world
                .build_rotation_with(
                    rotator,
                    read,
                    &chain,
                    keys,
                    &directory,
                    history.is_some(),
                    &mut TestRandom(67),
                )
                .unwrap()
        };
        let full = rotate(&whole, &full_keys);
        let scoped = rotate(&bounded, &scoped_keys);
        assert_eq!(scoped.rest, full.rest);
        assert_eq!(scoped.records.successor, full.records.successor);
        assert_eq!(scoped.records.deliveries, full.records.deliveries);
    };

    world.account(CAROL);
    world.add_member(CAROL);
    compare(&world, ALICE, None);

    let phone = world.approve_device(BOB);
    world.remove_device(BOB, &phone.id);
    compare(&world, ALICE, None);

    world.remove_member(BOB);
    compare(&world, ALICE, None);

    let last_held = world.current();
    world.rotate(ALICE);
    world.add_member(BOB);
    assert_eq!(
        world.acquire(BOB).keys.newest().unwrap().epoch_id(),
        last_held
    );
    compare(&world, BOB, Some(&last_held));
}

/// A read filtered for one account (the space's roots and successors, that
/// account's member rows and the deliveries addressed to it) serves that
/// account's acquisition and the chain replay, and nothing else: another
/// account's acquisition and every helper that needs the whole space
/// refuse it as inconsistent evidence (§3.3, §3.4).
#[test]
fn a_read_for_one_account_serves_only_that_accounts_acquisition() {
    let mut world = space(65, &[ALICE, BOB]);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    let whole = world.read();
    assert_eq!(whole.space.filtered_for(), None);
    let mut bobs = SpaceLedger::one_account(address(), whole.space.through, BOB);
    assert_eq!(bobs.filtered_for(), Some(BOB));
    bobs.roots = whole.space.roots.clone();
    bobs.successors = whole.space.successors.clone();
    bobs.members = whole
        .space
        .members
        .iter()
        .filter(|member| member.row.account_id == BOB)
        .cloned()
        .collect();
    bobs.deliveries = whole
        .space
        .deliveries
        .iter()
        .filter(|delivery| delivery.row.recipient_account_id == BOB)
        .cloned()
        .collect();
    let directory = whole.directory(&world);
    let chain = SpaceChain::replay(&bobs, &directory).unwrap().unwrap();
    assert_eq!(chain, whole.chain(&world).unwrap());

    let bob = world.own(BOB);
    assert!(
        acquire(&chain, &bobs, &directory, &bob.epoch_keys)
            .unwrap()
            .has_current(&chain)
    );
    let alice = world.own(ALICE);
    let inconsistent = Some(ProtocolError::InconsistentEvidence);
    assert_eq!(
        acquire(&chain, &bobs, &directory, &alice.epoch_keys).err(),
        inconsistent
    );
    let mut rng = TestRandom(65);
    assert_eq!(members::holders(&chain, &bobs, 0).err(), inconsistent);
    assert_eq!(
        members::envelopes(&chain, &bobs, 0, false).err(),
        inconsistent
    );
    assert_eq!(
        lacking_held(&chain, &bobs, &directory, &held_keys(&world, &chain)).err(),
        inconsistent
    );
    assert_eq!(rotation_mark(&chain, &bobs, &directory).err(), inconsistent);
    assert_eq!(
        rotation_recipients(&chain, &bobs, &directory, ALICE, BUILTIN, &mut rng).err(),
        inconsistent
    );
    // The whole space's read serves every helper.
    assert!(lacking_held(&chain, &whole.space, &directory, &world.acquire(ALICE).keys).is_ok());
    assert!(
        acquire(&chain, &whole.space, &directory, &alice.epoch_keys)
            .unwrap()
            .has_current(&chain)
    );
}

/// A key that opens and passes its own version's marker but whose history
/// fails further back is not accepted, and is not reported against the
/// member that delivered it: the fault is the earlier successor's, reported
/// as a broken history naming that successor, its version and its author
/// (§3.3, §3.4).
///
/// ```text
/// alice ──successor v1 (history to v0 later broken)──► space
/// carol ──successor v2 + delivery(v2)──► bob
/// bob opens v2 ✓, v2's marker ✓, v2 → v1 ✓, v1 → v0 ✗
///     no invalid delivery naming carol; broken history: v1 by alice
/// ```
#[test]
fn a_broken_history_further_back_is_not_reported_against_the_sender() {
    let mut world = space(66, &[ALICE, BOB, CAROL]);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    world.deliver(ALICE, CAROL, &v0);
    let v1 = world.rotate(ALICE);
    let v2 = world.rotate(CAROL);
    let chain = world.read().chain(&world).unwrap();
    assert_eq!(chain.current().unwrap().epoch_id, v2);
    assert!(world.acquire(BOB).has_current(&chain));

    // Alice's successor's history link no longer opens; she signs it again.
    let index = world
        .space
        .successors
        .iter()
        .position(|successor| successor.row.epoch_id == v1)
        .unwrap();
    let mut damaged = world.space.successors[index].row.clone();
    let last = damaged.history.len() - 1;
    damaged.history[last] ^= 1;
    resign(&world, ALICE, &mut damaged);
    world.space.successors[index].row = damaged.clone();
    let chain = world.read().chain(&world).unwrap();
    assert_eq!(chain.current().unwrap().epoch_id, v2);

    let bob = world.acquire(BOB);
    assert!(bob.keys.holds(&v0));
    assert!(!bob.keys.holds(&v1));
    assert!(!bob.keys.holds(&v2));
    assert!(
        bob.invalid_deliveries.is_empty(),
        "neither carol's delivery of v2 nor alice's of v1 is at fault"
    );
    assert_eq!(
        bob.broken_history,
        [BrokenHistory {
            space: address(),
            key_epoch_id: v1,
            successor_id: damaged.id,
            author_account_id: ALICE.to_owned(),
        }]
    );
}

/// An invalid root with a lower ID, accepted at the same position as a
/// valid root, does not displace it: ties are broken by ID among valid
/// roots only (§3.2).
#[test]
fn an_invalid_root_with_a_lower_id_at_the_same_position_does_not_displace_a_valid_one() {
    let mut world = World::new(67);
    world.account(ALICE);
    world.add_member(ALICE);
    let device = world.accounts[ALICE].device.clone();
    let valid = world.build_root(ROW, ALICE, &device).unwrap().root;
    let mut invalid = world.build_root(ROW, ALICE, &device).unwrap().root;
    // The lowest well-formed ID; the signature no longer covers the row.
    invalid.id = "00000000-0000-4000-8000-000000000000".to_owned();
    assert!(invalid.id < valid.id);
    let at = world.next();
    world.space.roots.push(Accepted::new(at, invalid.clone()));
    world.space.roots.push(Accepted::new(at, valid.clone()));
    let chain = world.read().chain(&world).unwrap();
    assert_eq!(chain.root.row.id, valid.id);
    assert_eq!(chain.lost.len(), 1);
    assert_eq!(chain.lost[0].root_id, invalid.id);
    assert_eq!(
        chain.lost[0].invalid,
        Some(InvalidSpaceRoot::SignatureInvalid)
    );
    assert!(world.acquire(ALICE).has_current(&chain));
}

/// An invalid successor with a lower ID, accepted at the same position as
/// a valid successor of the same version, does not displace it: the next
/// version is the first valid successor (§3.3).
#[test]
fn an_invalid_successor_with_a_lower_id_at_the_same_position_does_not_displace_a_valid_one() {
    let mut world = space(68, &[ALICE, BOB]);
    let v0 = world.current();
    let valid = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    let mut invalid = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    invalid.id = "00000000-0000-4000-8000-000000000000".to_owned();
    assert!(invalid.id < valid.id);
    let at = world.next();
    world
        .space
        .successors
        .push(Accepted::new(at, invalid.clone()));
    world
        .space
        .successors
        .push(Accepted::new(at, valid.clone()));
    let chain = world.read().chain(&world).unwrap();
    let epochs: Vec<_> = chain.versions.iter().map(|v| v.epoch_id.clone()).collect();
    assert_eq!(epochs, [v0, valid.epoch_id.clone()]);
    assert_eq!(
        chain.versions[1].successor.as_ref().unwrap().row.id,
        valid.id
    );
}

/// A delivery accepted at exactly the root's position is skipped for
/// acquiring and for who lacks a version, but counts for key holders and
/// for the needs-rotation mark, the safe direction (§3.3, §3.4).
#[test]
fn a_delivery_at_the_roots_position_counts_for_holders_but_not_for_acquiring() {
    let mut world = space(69, &[ALICE, BOB]);
    let v0 = world.current();
    let chain = world.read().chain(&world).unwrap();
    let early = world.build_delivery(ALICE, BOB, &v0).unwrap();
    world
        .space
        .deliveries
        .push(Accepted::new(chain.root.position, early.clone()));

    let bob = world.acquire(BOB);
    assert!(!bob.has_current(&chain));
    assert!(bob.invalid_deliveries.is_empty());
    assert_eq!(lacking_of(&world), [(BOB.to_owned(), vec![v0.clone()])]);
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    assert_eq!(
        members::holders(&chain, &read.space, 0).unwrap(),
        [ALICE.to_owned(), BOB.to_owned()].into()
    );
    let ids = |after_root| {
        members::envelopes(&chain, &read.space, 0, after_root)
            .unwrap()
            .into_iter()
            .map(|envelope| envelope.record_id)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(false), [chain.root.row.id.clone(), early.id.clone()]);
    assert_eq!(ids(true), std::slice::from_ref(&chain.root.row.id));

    world.remove_member(BOB);
    let mark = mark_of(&world);
    assert!(mark.needs_rotation());
    assert_eq!(mark.former_members, [BOB]);
}

/// `account`'s devices `phone` and `tablet`, approved; `phone` then removes
/// `tablet` and co-publishes the new account epoch's sealing key record
/// with a key that does not derive from the new epoch key. Returns the
/// record and `phone`.
fn non_derived_account_epoch(
    world: &mut World,
    account: &str,
) -> (
    jazz_e2ee::protocol::records::account::AccountSealingKey,
    LocalDevice,
) {
    world.with_account(account, |entry, rng| {
        let mut phone = entry.env.device(rng);
        let mut tablet = entry.env.device(rng);
        let approver = entry.device.clone();
        for device in [&mut phone, &mut tablet] {
            entry.ledger.request(device, rng);
            entry.ledger.approve(&entry.env, &approver, device, rng);
        }
        let key = entry.ledger.acquire(&entry.env, &phone).unwrap();
        let state = entry.ledger.state(&entry.env);
        let mut rotation = account_build::rotate(
            &entry.env.writer(),
            &state,
            &phone,
            &key,
            &RotationTarget::RemoveDevice(tablet.id.clone()),
            rng,
        )
        .unwrap();
        let wrong = derive::account_sealing_key_pair(&key_seal::Builtin, &[9; 32])
            .unwrap()
            .public
            .bytes;
        let record = &mut rotation.account_key;
        record.public_key = wrong;
        let bytes = record.signed_bytes(&entry.env.application()).unwrap();
        let signer = entry
            .env
            .registry
            .signer(phone.signing.public.mechanism.as_mechanism())
            .unwrap();
        record.signature = signer.sign(&phone.signing, &bytes).unwrap();
        let at = entry.ledger.next();
        entry.ledger.accept_rotation(at, &rotation);
        (rotation.account_key.clone(), phone)
    })
}

/// A sender delivers to a member whose current sealing key record does not
/// derive from its account key. The sender cannot know and seals to it;
/// the member's devices know, so the delivery that does not open is
/// reported against that record and the device that signed it, and no
/// invalid delivery names the sender (§2.12, §3.4). Once the member removes
/// that device, a delivery sealed to the new epoch's record opens.
#[test]
fn a_delivery_sealed_to_a_non_derived_record_is_reported_against_the_record() {
    let mut world = space(50, &[ALICE, BOB]);
    let v0 = world.current();
    let (record, phone) = non_derived_account_epoch(&mut world, BOB);
    let read = world.read();
    let recipient = read
        .directory(&world)
        .recipient(BOB, BUILTIN)
        .unwrap()
        .unwrap();
    assert_eq!(recipient.sealing_key().row.id, record.id);

    let delivery = world.deliver(ALICE, BOB, &v0);
    let bob = world.acquire(BOB);
    assert!(!bob.keys.holds(&v0));
    assert!(bob.invalid_deliveries.is_empty());
    assert_eq!(bob.non_derived_deliveries.len(), 1);
    let reported = &bob.non_derived_deliveries[0];
    assert_eq!(reported.delivery_id, delivery.id);
    assert_eq!(reported.key_epoch_id, v0);
    assert_eq!(reported.sealing_key.record_id, record.id);
    assert_eq!(reported.sealing_key.epoch_id, record.epoch_id);
    assert_eq!(reported.sealing_key.signer_id, phone.id);
    assert!(reported.sealing_key.selected);
    let own = world.own(BOB);
    assert_eq!(
        own.epoch_keys.non_derived_sealing_keys(),
        std::slice::from_ref(&reported.sealing_key)
    );

    // A delivery damaged in transit to the same record is not the
    // sender's either; to a record that derives, it would be.
    let mut damaged = world.build_delivery(ALICE, BOB, &v0).unwrap();
    let last = damaged.envelope.len() - 1;
    damaged.envelope[last] ^= 1;
    let at = world.next();
    world.space.deliveries.push(Accepted::new(at, damaged));

    world.remove_device(BOB, &phone.id);
    let new_epoch = world.account_epoch(BOB);
    assert_ne!(new_epoch, record.epoch_id);
    world.deliver(ALICE, BOB, &v0);
    let bob = world.acquire(BOB);
    assert!(bob.keys.holds(&v0));
    assert!(bob.invalid_deliveries.is_empty());
    assert_eq!(bob.non_derived_deliveries.len(), 2);
    assert!(
        bob.non_derived_deliveries
            .iter()
            .all(|reported| reported.sealing_key.record_id == record.id)
    );
}

/// Alice seals her copy of a new space root with the derived key even when
/// the first valid public record names a different key signed by her device.
#[test]
fn a_root_self_envelope_skips_an_earlier_non_derived_sealing_key() {
    let mut world = World::new(51);
    world.account(ALICE);
    world.add_member(ALICE);
    let wrong = derive::account_sealing_key_pair(&key_seal::Builtin, &[9; 32]).unwrap();
    let conflicting_id = world.with_account(ALICE, |entry, rng| {
        let later = entry.ledger.next();
        let genuine = &mut entry.ledger.public.account_keys[0];
        let earlier = std::mem::replace(&mut genuine.position, later);
        let mut conflict = genuine.row.clone();
        conflict.id = ids::random_v4(rng).unwrap();
        assert_ne!(conflict.public_key, wrong.public.bytes);
        conflict.public_key = wrong.public.bytes.clone();
        let bytes = conflict.signed_bytes(&entry.env.application()).unwrap();
        let signer = entry
            .env
            .registry
            .signer(entry.device.signing.public.mechanism.as_mechanism())
            .unwrap();
        conflict.signature = signer.sign(&entry.device.signing, &bytes).unwrap();
        let id = conflict.id.clone();
        entry
            .ledger
            .public
            .account_keys
            .insert(0, Accepted::new(earlier, conflict));
        id
    });
    let read = world.read();
    let directory = read.directory(&world);
    assert_eq!(
        directory
            .recipient(ALICE, BUILTIN)
            .unwrap()
            .unwrap()
            .sealing_key()
            .row
            .id,
        conflicting_id
    );

    let own = world.own(ALICE);
    let device = world.accounts[ALICE].device.clone();
    let records = world.build_root(ROW, ALICE, &device).unwrap();
    let account = &world.accounts[ALICE];
    let context = records
        .root
        .account_envelope_context(&account.env.application())
        .unwrap();
    let envelope = &records.root.account_envelope;
    let derived = derive::account_sealing_key_pair(&key_seal::Builtin, own.key.key()).unwrap();
    assert_eq!(
        *account
            .env
            .registry
            .open_sealed(&[derived], &context, envelope)
            .unwrap(),
        *records.key
    );
    assert_eq!(
        account
            .env
            .registry
            .open_sealed(&[wrong], &context, envelope)
            .unwrap_err(),
        jazz_e2ee::Error::InvalidCiphertext
    );
}

/// Attribution is decided against the read being replayed, not against a
/// report the account's keys carry from an earlier read: a non-derived
/// record published after the device last took its keys still takes the
/// blame for a delivery sealed to it, never the sender (§2.12, §3.4).
#[test]
fn a_record_published_after_the_keys_were_taken_still_takes_the_blame() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let mut world = World::with_registry(52, registry);
    world.account(ALICE);
    world.account_with(BOB, sealing_with(OTHER_SEAL));
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    let v0 = world.current();

    // Bob's keys, taken before any built-in record of his exists.
    let stale = world.own(BOB);
    assert!(stale.epoch_keys.non_derived_sealing_keys().is_empty());

    // A device of Bob's publishes a later built-in record that does not
    // derive from the account key; senders of the built-in mechanism
    // select it.
    let own = world.own(BOB);
    let record = world.with_account(BOB, |entry, rng| {
        let public_key = derive::account_sealing_key_pair(&key_seal::Builtin, &[9; 32])
            .unwrap()
            .public
            .bytes;
        let record = sealing_key_record(
            &entry.env,
            &entry.device,
            own.state.epoch_id(),
            BUILTIN,
            public_key,
            rng,
        );
        let at = entry.ledger.next();
        entry
            .ledger
            .public
            .account_keys
            .push(Accepted::new(at, record.clone()));
        record
    });
    let delivery = world.deliver(ALICE, BOB, &v0);

    // With the stale keys and today's read, the delivery is blamed on the
    // record, as it is with fresh keys.
    let read = world.read();
    let chain = read.chain(&world).unwrap();
    for keys in [&stale.epoch_keys, &world.own(BOB).epoch_keys] {
        let bob = acquire(&chain, &read.space, &read.directory(&world), keys).unwrap();
        assert!(!bob.keys.holds(&v0));
        assert!(bob.invalid_deliveries.is_empty());
        assert_eq!(bob.non_derived_deliveries.len(), 1);
        assert_eq!(bob.non_derived_deliveries[0].delivery_id, delivery.id);
        assert_eq!(
            bob.non_derived_deliveries[0].sealing_key.record_id,
            record.id
        );
    }
}

/// A recipient or an author state from another read than the chain's is
/// inconsistent evidence for a delivery and for both kinds of rotation;
/// from the chain's own read they are accepted. Membership is not checked
/// by the builder: the server's policy enforces it.
#[test]
fn a_recipient_or_author_from_another_read_is_inconsistent_evidence() {
    let mut world = space(51, &[ALICE, BOB]);
    let v0 = world.current();
    let old = world.read();
    let old_recipient = old
        .directory(&world)
        .recipient(BOB, BUILTIN)
        .unwrap()
        .unwrap();
    assert_eq!(old_recipient.through(), old.space.through);
    let alice = world.own(ALICE);
    let device = world.accounts[ALICE].device.clone();
    let old_directory = old.directory(&world);
    let old_author = Author::new(&old_directory, ALICE, &alice.key, &device).unwrap();
    assert_eq!(old_author.through(), old.space.through);

    // Any later row moves the cut: Carol's account, with no member row.
    world.account(CAROL);
    let new = world.read();
    let chain = new.chain(&world).unwrap();
    let directory = new.directory(&world);
    let keys = world.acquire(ALICE).keys;
    let version = keys.version(&v0).unwrap();
    let recipient = directory.recipient(BOB, BUILTIN).unwrap().unwrap();
    let author = Author::new(&directory, ALICE, &alice.key, &device).unwrap();
    let writer = world.writer(ALICE);
    let mut rng = TestRandom(51);
    let inconsistent = Some(ProtocolError::InconsistentEvidence);

    assert_eq!(
        build::deliver(&writer, &chain, &version, &old_recipient, &mut rng).err(),
        inconsistent
    );
    for recovery in [false, true] {
        let rotate = |author: &Author<'_>, recipients: &[Recipient], rng: &mut TestRandom| {
            if recovery {
                build::recover_rotation(&writer, &chain, &keys, author, recipients, rng)
            } else {
                build::rotate(&writer, &chain, &keys, author, recipients, rng)
            }
        };
        let before = rng.0;
        assert_eq!(
            rotate(&old_author, std::slice::from_ref(&recipient), &mut rng).err(),
            inconsistent,
            "recovery: {recovery}"
        );
        assert_eq!(
            rotate(&author, std::slice::from_ref(&old_recipient), &mut rng).err(),
            inconsistent,
            "recovery: {recovery}"
        );
        assert_eq!(rng.0, before, "nothing was generated");
        // The same read: accepted.
        let records = rotate(&author, std::slice::from_ref(&recipient), &mut rng).unwrap();
        assert_eq!(records.deliveries.len(), 1);
    }
    let delivery = build::deliver(&writer, &chain, &version, &recipient, &mut rng).unwrap();
    assert_eq!(delivery.recipient_account_id, BOB);
    // Carol is no member: the server's policy refuses that delivery, not
    // the builder.
    assert!(!new.space.is_member(CAROL));
    let to_carol = directory.recipient(CAROL, BUILTIN).unwrap().unwrap();
    assert!(build::deliver(&writer, &chain, &version, &to_carol, &mut rng).is_ok());
}

/// Empties `account`'s device-keys rows: its root then names a device with no
/// keys, an intrinsic contradiction of that account's own history
/// (`missing-root-key`, §7).
fn break_account(world: &mut World, account: &str) {
    world
        .accounts
        .get_mut(account)
        .unwrap()
        .ledger
        .public
        .keys
        .clear();
}

/// One member whose own account history contradicts itself (here, its root
/// has no device keys) never stops the space (§3.4, "A member this client
/// cannot evaluate"): it is skipped and reported as broken, the other
/// members are still served, and because nobody can tell which of its
/// devices can open what it holds, it sets the needs-rotation mark. A
/// successor it signed, even one built correctly before its history broke,
/// is not accepted, and the chain still replays.
#[test]
fn a_member_with_a_broken_account_history_is_skipped_and_marks_the_space() {
    let mut world = World::new(66);
    for account in [ALICE, BOB, CAROL] {
        world.account(account);
        world.add_member(account);
    }
    world.create(ALICE);
    let v0 = world.current();
    world.deliver(ALICE, BOB, &v0);
    let junk = world.build_rotation(BOB, false).unwrap();

    break_account(&mut world, BOB);
    let registry = world.shared.registry.clone();
    let broken = vec![members::BrokenMember {
        account_id: BOB.to_owned(),
        contradiction: IntrinsicContradiction::MissingRootKey,
    }];
    let found = helpers(&world, &registry, None);
    let lacks = found.lacking.unwrap();
    assert_eq!(
        lacks
            .lacking
            .iter()
            .map(|entry| entry.account_id.as_str())
            .collect::<Vec<_>>(),
        [CAROL]
    );
    assert_eq!(lacks.broken, broken);
    let recipients = found.recipients.unwrap();
    assert_eq!(
        recipients
            .first
            .iter()
            .map(|recipient| recipient.account_id())
            .collect::<Vec<_>>(),
        [CAROL]
    );
    assert_eq!(recipients.broken, broken);
    let mark = found.mark.unwrap();
    assert!(mark.needs_rotation());
    assert_eq!(mark.broken, broken);

    world.accept_rotation(junk);
    let chain = world.read().chain(&world).unwrap();
    assert_eq!(chain.versions.len(), 1);
    assert_eq!(world.current(), v0);
    world.deliver(ALICE, CAROL, &v0);
    assert!(world.acquire(CAROL).has_current(&chain));
}

/// A successor accepted at its predecessor's own position (here, in the
/// root's transaction) is not strictly after it and is not a version; the
/// same row one position later is (§3.3).
#[test]
fn a_successor_at_its_predecessors_position_is_not_a_version() {
    let mut world = space(67, &[ALICE, BOB]);
    let v0 = world.current();
    let successor = world
        .build_rotation(ALICE, false)
        .unwrap()
        .records
        .successor;
    let root_at = world.read().chain(&world).unwrap().root.position;

    let mut same = world.read();
    same.space
        .successors
        .push(Accepted::new(root_at, successor.clone()));
    let chain = same.chain(&world).unwrap();
    assert_eq!(chain.versions.len(), 1);
    assert_eq!(chain.versions[0].epoch_id, v0);

    let at = world.next();
    world.space.successors.push(Accepted::new(at, successor));
    assert_eq!(world.read().chain(&world).unwrap().versions.len(), 2);
}

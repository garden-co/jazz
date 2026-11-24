//! Account sealing keys (`E2EE_PROTOCOL.md` §2.12): which
//! `__e2ee_account_keys` rows are valid, the record a sender seals to, the
//! derivation check a device of the account makes, and the later
//! publication of a migration's new mechanism.

mod common;

use common::TestRandom;
use common::mechanisms::{OTHER_SEAL, OTHER_SIGN, UNROUTED_SEAL, custom_registry, sealing_with};
use common::protocol::{ALICE, Env, Ledger, sealing_key_record};
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::envelope::Mechanism;
use jazz_e2ee::mechanisms::key_seal;
use jazz_e2ee::protocol::account::build::{self, LocalDevice, RotationTarget};
use jazz_e2ee::protocol::account::{
    AccountEpochKeys, AccountKeyEvidence, NonDerivedSealingKey, UnsupportedSealingKey,
    verify_private,
};
use jazz_e2ee::protocol::error::UnsupportedCapability;
use jazz_e2ee::protocol::records::account::AccountSealingKey;
use jazz_e2ee::protocol::{Accepted, derive};

const BUILTIN: Mechanism<'static> = key_seal::MECHANISM;

/// A sealing key record of `epoch_id` signed by `signer` with
/// `public_key`, outside the builders' rules.
fn crafted(
    env: &Env,
    signer: &LocalDevice,
    epoch_id: &str,
    public_key: Vec<u8>,
    rng: &mut TestRandom,
) -> AccountSealingKey {
    sealing_key_record(env, signer, epoch_id, BUILTIN, public_key, rng)
}

fn derived(key: &[u8; 32]) -> Vec<u8> {
    derive::account_sealing_key_pair(&key_seal::Builtin, key)
        .unwrap()
        .public
        .bytes
}

fn valid_ids(env: &Env, ledger: &Ledger) -> Vec<String> {
    ledger
        .replay(env)
        .sealing_keys()
        .unwrap()
        .expect("a root")
        .valid()
        .iter()
        .map(|record| record.row.id.clone())
        .collect()
}

fn record_id(ledger: &Ledger, index: usize) -> String {
    ledger.public.account_keys[index].row.id.clone()
}

/// Alice's account with `laptop` as its root device, bootstrapped.
fn bootstrapped(seed: u64) -> (Env, Ledger, LocalDevice, TestRandom) {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(seed);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    (env, ledger, laptop, rng)
}

fn epoch_keys(env: &Env, ledger: &Ledger, device: &LocalDevice) -> AccountEpochKeys {
    let replay = ledger.replay(env);
    let state = replay.current().unwrap().unwrap();
    let private = verify_private(&replay, &state, &ledger.private).unwrap();
    let evidence = AccountKeyEvidence::new(&replay, &state, &private).unwrap();
    let key = evidence
        .acquire(&device.id, std::slice::from_ref(&device.sealing))
        .unwrap();
    evidence.epoch_keys(&key).unwrap()
}

/// The record a bootstrap writes with the root is valid, and it is the
/// record a sender seals to.
#[test]
fn record_co_published_by_a_bootstrap_is_valid() {
    let (env, ledger, _, _) = bootstrapped(1);
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert_eq!(valid_ids(&env, &ledger), [record_id(&ledger, 0)]);
    assert_eq!(
        sealing.current(BUILTIN).map(|record| &record.row.id),
        Some(&record_id(&ledger, 0))
    );
    assert!(sealing.current(OTHER_SEAL).is_none());
}

/// The record a reset writes with its new root is valid; the earlier
/// generation's record is not part of the current chain.
#[test]
fn record_co_published_by_a_reset_is_valid() {
    let (env, mut ledger, _, mut rng) = bootstrapped(2);
    let tablet = env.device(&mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.root(&env, &tablet, 1, &mut rng);
    assert_eq!(valid_ids(&env, &ledger), [record_id(&ledger, 1)]);
}

/// The record a rotation writes is valid when its author signs it.
#[test]
fn record_co_published_by_a_rotation_is_valid() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(3);
    let mut phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    assert_eq!(
        valid_ids(&env, &ledger),
        [record_id(&ledger, 0), record_id(&ledger, 1)]
    );
}

/// A rotation's author signs the new epoch's record; the device it removes
/// does not. (A device never removes itself, §2.8.)
#[test]
fn record_co_published_by_a_removal_is_signed_by_its_author() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(4);
    let mut phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    assert_eq!(ledger.public.account_keys[1].row.signer_id, laptop.id);
    assert!(valid_ids(&env, &ledger).contains(&record_id(&ledger, 1)));
}

/// At a bootstrap's position, a record signed by a device of no standing is
/// not valid, and no later approval makes it valid.
#[test]
fn record_at_a_bootstrap_signed_by_a_device_without_standing_is_not_valid() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(5);
    let laptop = env.device(&mut rng);
    let mut stranger = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.request(&stranger, &mut rng);
    let (root_at, key) = ledger.root(&env, &laptop, 0, &mut rng);
    let epoch = ledger.state(&env).epoch_id().to_owned();
    let forged = crafted(&env, &stranger, &epoch, derived(&key), &mut rng);
    ledger
        .public
        .account_keys
        .push(Accepted::new(root_at, forged.clone()));
    assert_eq!(valid_ids(&env, &ledger), [record_id(&ledger, 0)]);

    ledger.approve(&env, &laptop, &mut stranger, &mut rng);
    assert!(ledger.state(&env).is_active(&stranger.id));
    assert!(!valid_ids(&env, &ledger).contains(&forged.id));
}

/// At a rotation's position, only its author's record is valid: not one by
/// a device active in the predecessor epoch (here the removed one), nor one
/// the new membership admits, nor one of no standing, even after that one
/// is approved.
#[test]
fn record_at_a_rotation_signed_by_another_device_is_not_valid() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(6);
    let mut phone = env.device(&mut rng);
    let mut tablet = env.device(&mut rng);
    let mut stranger = env.device(&mut rng);
    for device in [&phone, &tablet, &stranger] {
        ledger.request(device, &mut rng);
    }
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger.approve(&env, &laptop, &mut tablet, &mut rng);
    let (at, rotation) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let epoch = rotation.public.epoch_id.clone();
    let public_key = derived(&rotation.key);
    let mut forged = Vec::new();
    for signer in [&phone, &tablet, &stranger] {
        let record = crafted(&env, signer, &epoch, public_key.clone(), &mut rng);
        forged.push(record.id.clone());
        ledger.public.account_keys.push(Accepted::new(at, record));
    }
    let valid = valid_ids(&env, &ledger);
    assert!(valid.contains(&rotation.account_key.id));
    for id in &forged {
        assert!(!valid.contains(id), "{id}");
    }

    ledger.approve(&env, &laptop, &mut stranger, &mut rng);
    assert!(ledger.state(&env).is_active(&stranger.id));
    let valid = valid_ids(&env, &ledger);
    for id in &forged {
        assert!(!valid.contains(id), "{id}");
    }
}

/// A migration: once the application selects a new sealing mechanism, an
/// active device publishes the current epoch's record of it, which is
/// valid and is what senders of that mechanism seal to. The publication
/// writes nothing when a valid record of the selected mechanism exists.
#[test]
fn later_publication_by_an_active_device_is_valid() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let env = Env::with(registry.clone(), Default::default(), ALICE);
    let migrated = Env::with(registry, sealing_with(OTHER_SEAL), ALICE);
    let mut rng = TestRandom(7);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);

    let state = ledger.state(&env);
    let key = ledger.acquire(&env, &phone).unwrap();
    let record = sealing_key_record(
        &migrated,
        &phone,
        state.epoch_id(),
        OTHER_SEAL,
        derive::account_sealing_key_pair(migrated.writer().suite.key_seal, key.key())
            .unwrap()
            .public
            .bytes,
        &mut rng,
    );
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, record.clone()));

    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert!(sealing.is_valid(&record.id));
    assert_eq!(
        sealing.current(OTHER_SEAL).map(|found| &found.row.id),
        Some(&record.id)
    );
    assert_eq!(
        sealing.current(BUILTIN).map(|found| &found.row.id),
        Some(&record_id(&ledger, 0))
    );
}

/// A later publication by a device that is not active when it is accepted
/// is not valid: a removed device, and a device never approved.
#[test]
fn later_publication_by_a_device_that_was_not_active_is_not_valid() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(8);
    let mut phone = env.device(&mut rng);
    let stranger = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.request(&stranger, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let (_, rotation) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let epoch = rotation.public.epoch_id.clone();
    let mut forged = Vec::new();
    for signer in [&phone, &stranger] {
        let record = crafted(&env, signer, &epoch, derived(&rotation.key), &mut rng);
        forged.push(record.id.clone());
        let at = ledger.next();
        ledger.public.account_keys.push(Accepted::new(at, record));
    }
    let valid = valid_ids(&env, &ledger);
    for id in &forged {
        assert!(!valid.contains(id), "{id}");
    }
}

/// A later publication for an epoch that is no longer current when it is
/// accepted is not valid, even by an active device; nor is a record
/// accepted before its epoch's creation.
#[test]
fn later_publication_for_an_epoch_no_longer_current_is_not_valid() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(9);
    let root_epoch = ledger.state(&env).epoch_id().to_owned();
    let root_key = *ledger.acquire(&env, &laptop).unwrap().key();
    let mut phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);

    // A record of the next epoch, accepted before the rotation creating it.
    let key = ledger.acquire(&env, &laptop).unwrap();
    let state = ledger.state(&env);
    let rotation = build::rotate(
        &env.writer(),
        &state,
        &laptop,
        &key,
        &RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    )
    .unwrap();
    let early = crafted(
        &env,
        &laptop,
        &rotation.public.epoch_id,
        derived(&rotation.key),
        &mut rng,
    );
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, early.clone()));
    let at = ledger.next();
    ledger.accept_rotation(at, &rotation);
    assert_eq!(ledger.state(&env).epoch_id(), rotation.public.epoch_id);

    // A record of the root epoch, after the rotation superseded it.
    let stale = crafted(&env, &laptop, &root_epoch, derived(&root_key), &mut rng);
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, stale.clone()));

    let valid = valid_ids(&env, &ledger);
    assert!(!valid.contains(&early.id));
    assert!(!valid.contains(&stale.id));
    assert!(valid.contains(&rotation.account_key.id));
}

/// Of two valid records of one epoch and mechanism, a sender seals to the
/// first by position.
#[test]
fn senders_seal_to_the_first_valid_record() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(10);
    let key = *ledger.acquire(&env, &laptop).unwrap().key();
    let epoch = ledger.state(&env).epoch_id().to_owned();
    let second = crafted(&env, &laptop, &epoch, derived(&key), &mut rng);
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, second.clone()));
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert!(sealing.is_valid(&second.id));
    assert_eq!(
        sealing.current(BUILTIN).map(|record| &record.row.id),
        Some(&record_id(&ledger, 0))
    );
}

/// A device of the account checks that every valid record of an epoch it
/// holds derives from that epoch's key; a valid record that does not is
/// reported (`conflicting-account-key`) with its epoch, mechanism and
/// signing device, and the check itself succeeds.
#[test]
fn a_valid_record_that_does_not_derive_from_the_key_is_reported() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(11);
    let mut phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let keys = epoch_keys(&env, &ledger, &laptop);
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert_eq!(sealing.valid().len(), 2);
    assert!(
        sealing
            .check_derived(&env.registry, &keys)
            .unwrap()
            .is_empty()
    );
    assert!(keys.non_derived_sealing_keys().is_empty());

    // The root device signs a second record at the root's position with a
    // key that is not the derived one.
    let root_at = ledger.public.roots[0].position;
    let root_epoch = ledger.public.roots[0].row.epoch_id.clone();
    let wrong = crafted(&env, &laptop, &root_epoch, derived(&[9; 32]), &mut rng);
    ledger
        .public
        .account_keys
        .push(Accepted::new(root_at, wrong.clone()));
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert!(sealing.is_valid(&wrong.id));
    let selected = sealing
        .first(&root_epoch, BUILTIN)
        .is_some_and(|first| first.row.id == wrong.id);
    let expected = NonDerivedSealingKey {
        record_id: wrong.id.clone(),
        epoch_id: root_epoch.clone(),
        mechanism: BUILTIN.id.to_owned(),
        version: BUILTIN.version,
        signer_id: laptop.id.clone(),
        position: root_at,
        selected,
    };
    assert_eq!(
        sealing.check_derived(&env.registry, &keys).unwrap(),
        std::slice::from_ref(&expected)
    );
    let keys = epoch_keys(&env, &ledger, &laptop);
    assert_eq!(keys.non_derived_sealing_keys(), [expected]);
}

/// A device reaches every account epoch key of its generation from the
/// current one, through the successors' `history`, root epoch first.
#[test]
fn epoch_keys_reach_every_epoch_of_the_generation() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(12);
    let root_key = *ledger.acquire(&env, &laptop).unwrap().key();
    let mut expected = vec![(ledger.state(&env).epoch_id().to_owned(), root_key)];
    for _ in 0..2 {
        let mut phone = env.device(&mut rng);
        ledger.request(&phone, &mut rng);
        ledger.approve(&env, &laptop, &mut phone, &mut rng);
        let (_, rotation) = ledger.rotate(
            &env,
            &laptop,
            RotationTarget::RemoveDevice(phone.id.clone()),
            &mut rng,
        );
        expected.push((rotation.public.epoch_id.clone(), *rotation.key));
    }
    let keys = epoch_keys(&env, &ledger, &laptop);
    assert_eq!(
        keys.epoch_ids().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(epoch, _)| epoch.as_str())
            .collect::<Vec<_>>()
    );
    for (epoch, key) in &expected {
        assert_eq!(keys.get(epoch), Some(key));
    }
    assert_eq!(keys.generation(), 0);
    let rendered = format!("{keys:?}");
    assert!(!rendered.contains(&format!("{:?}", root_key)));
}

/// A device of the account rotates it and co-publishes the new epoch's
/// record with a key that does not derive from the new epoch key. The
/// record is valid (the rotation's author signed it), so it is the record
/// senders seal to. Every device of the account still acquires its key and
/// reports the record and its signer, and its own devices never select it.
/// Removing the signing device starts a new epoch whose co-published
/// record derives, is current, and opens what senders seal to it (§2.12).
///
/// ```text
/// laptop ──root──► ledger; phone, tablet ──approved──► ledger
/// phone  ──remove tablet + record not derived from K₁──► ledger
/// laptop, phone ──acquire K₁──► ok, record reported
/// laptop ──remove phone + record derived from K₂──► ledger
/// sender ──seal to the current record──► laptop opens
/// ```
#[test]
fn a_record_that_does_not_derive_is_reported_until_its_signer_is_removed() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(13);
    let mut phone = env.device(&mut rng);
    let mut tablet = env.device(&mut rng);
    for device in [&mut phone, &mut tablet] {
        ledger.request(device, &mut rng);
        ledger.approve(&env, &laptop, device, &mut rng);
    }
    let key = ledger.acquire(&env, &phone).unwrap();
    let state = ledger.state(&env);
    let mut rotation = build::rotate(
        &env.writer(),
        &state,
        &phone,
        &key,
        &RotationTarget::RemoveDevice(tablet.id.clone()),
        &mut rng,
    )
    .unwrap();
    let epoch = rotation.public.epoch_id.clone();
    rotation.account_key = crafted(&env, &phone, &epoch, derived(&[9; 32]), &mut rng);
    let at = ledger.next();
    ledger.accept_rotation(at, &rotation);
    let record = rotation.account_key.clone();
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert!(sealing.is_valid(&record.id), "co-published by its author");
    assert_eq!(
        sealing.current(BUILTIN).map(|found| &found.row.id),
        Some(&record.id),
        "senders cannot check derivation"
    );

    let expected = NonDerivedSealingKey {
        record_id: record.id.clone(),
        epoch_id: epoch.clone(),
        mechanism: BUILTIN.id.to_owned(),
        version: BUILTIN.version,
        signer_id: phone.id.clone(),
        position: at,
        selected: true,
    };
    for device in [&laptop, &phone] {
        let key = ledger.acquire(&env, device).unwrap();
        assert_eq!(key.epoch_id(), epoch);
        assert_eq!(
            key.authenticated().non_derived_sealing_keys,
            std::slice::from_ref(&expected)
        );
        let keys = epoch_keys(&env, &ledger, device);
        assert_eq!(
            keys.non_derived_sealing_keys(),
            std::slice::from_ref(&expected)
        );
    }

    // Laptop removes phone: the new epoch's co-published record derives.
    let (_, removal) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    let current = sealing.current(BUILTIN).expect("the co-published record");
    assert_eq!(current.row.id, removal.account_key.id);
    let pair = derive::account_sealing_key_pair(&key_seal::Builtin, &removal.key).unwrap();
    assert_eq!(current.row.public_key, pair.public.bytes);
    let keys = epoch_keys(&env, &ledger, &laptop);
    // The earlier epoch's record is still reported, but no longer current.
    assert_eq!(keys.non_derived_sealing_keys(), [expected]);

    // A sender seals to the current record; the account's device opens it
    // with the key pair it derives.
    let context = b"a test context";
    let secret = [5u8; 32];
    let envelope = env
        .registry
        .select(&env.config)
        .unwrap()
        .seal_to(
            std::slice::from_ref(&current.row.public()),
            context,
            &secret,
            &mut rng,
        )
        .unwrap();
    let opened = env
        .registry
        .open_sealed(std::slice::from_ref(&pair), context, &envelope)
        .unwrap();
    assert_eq!(*opened, secret);
}

/// A record signed by a device whose signing mechanism this client does
/// not support is skipped and reported as unsupported: the account's
/// replay, its valid records and acquiring its key all succeed (§2.12).
#[test]
fn a_record_signed_with_an_unsupported_signing_mechanism_is_skipped() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let env = Env::with(registry, Default::default(), ALICE);
    let other_signing = Env::with(
        env.registry.clone(),
        MechanismConfig {
            signature: OTHER_SIGN.into(),
            ..MechanismConfig::default()
        },
        ALICE,
    );
    let reader = Env::new(ALICE);
    let mut rng = TestRandom(17);
    let laptop = env.device(&mut rng);
    let mut phone = other_signing.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let key = *ledger.acquire(&env, &laptop).unwrap().key();
    let epoch = ledger.state(&env).epoch_id().to_owned();
    let record = crafted(&env, &phone, &epoch, derived(&key), &mut rng);
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, record.clone()));
    assert!(
        ledger
            .replay(&env)
            .sealing_keys()
            .unwrap()
            .unwrap()
            .is_valid(&record.id),
        "a reader with the mechanism"
    );

    // The reader replays the account: the record is skipped and reported.
    let sealing = ledger.replay(&reader).sealing_keys().unwrap().unwrap();
    assert_eq!(valid_ids(&reader, &ledger), [record_id(&ledger, 0)]);
    assert_eq!(
        sealing.unsupported(),
        [UnsupportedSealingKey {
            record_id: record.id.clone(),
            signer_id: phone.id.clone(),
            capability: UnsupportedCapability::SigningMechanism,
        }]
    );
    assert_eq!(
        sealing.current(BUILTIN).map(|found| &found.row.id),
        Some(&record_id(&ledger, 0))
    );

    // Laptop, as such a reader, acquires the account key: the record
    // neither fails nor enters the derivation check. Private verification
    // needs the mechanism for phone's own proof, a separate rule, so that
    // row is left out of this read.
    ledger
        .private
        .proofs
        .retain(|proof| proof.row.id != ledger.private.challenges[0].row.id);
    let key = ledger.acquire(&reader, &laptop).unwrap();
    assert!(key.authenticated().non_derived_sealing_keys.is_empty());
    let keys = epoch_keys(&reader, &ledger, &laptop);
    assert!(keys.non_derived_sealing_keys().is_empty());
}

/// A record whose signer had no signing key published strictly before it
/// is not valid (§2.12), even when it is otherwise the co-published
/// record of a bootstrap whose device's keys were accepted with the root.
#[test]
fn a_record_whose_signer_had_no_signing_key_before_it_is_not_valid() {
    let (env, mut ledger, _, _) = bootstrapped(14);
    let root_at = ledger.public.roots[0].position;
    assert_eq!(valid_ids(&env, &ledger), [record_id(&ledger, 0)]);
    // The device's keys accepted in the root's own transaction: the root
    // still counts, but the record has no signing key before it.
    ledger.public.keys[0].position = root_at;
    assert!(ledger.replay(&env).current().unwrap().is_some());
    assert!(valid_ids(&env, &ledger).is_empty());
}

/// A record of a sealing mechanism this client does not have is skipped:
/// the derivation check cannot derive it and leaves it unchecked,
/// acquiring the account key still succeeds, and a sender of the built-in
/// mechanism still seals to the built-in record (§2.12).
#[test]
fn a_record_of_an_unsupported_sealing_mechanism_is_skipped() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(15);
    let epoch = ledger.state(&env).epoch_id().to_owned();
    let unknown = sealing_key_record(&env, &laptop, &epoch, UNROUTED_SEAL, vec![3; 32], &mut rng);
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, unknown.clone()));
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert!(sealing.is_valid(&unknown.id));
    assert_eq!(
        sealing.current(BUILTIN).map(|record| &record.row.id),
        Some(&record_id(&ledger, 0))
    );
    assert!(sealing.current(UNROUTED_SEAL).is_some());
    let keys = epoch_keys(&env, &ledger, &laptop);
    assert!(
        sealing
            .check_derived(&env.registry, &keys)
            .unwrap()
            .is_empty()
    );
}

/// At a reset's position, only the new root's device signs the new
/// generation's record: not the device of the earlier generation, and not
/// a device of no standing, even after it is approved (§2.12).
#[test]
fn record_at_a_reset_signed_by_another_device_is_not_valid() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(16);
    let tablet = env.device(&mut rng);
    let mut stranger = env.device(&mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.request(&stranger, &mut rng);
    let (reset_at, key) = ledger.root(&env, &tablet, 1, &mut rng);
    let epoch = ledger.state(&env).epoch_id().to_owned();
    let mut forged = Vec::new();
    for signer in [&laptop, &stranger] {
        let record = crafted(&env, signer, &epoch, derived(&key), &mut rng);
        forged.push(record.id.clone());
        ledger
            .public
            .account_keys
            .push(Accepted::new(reset_at, record));
    }
    assert_eq!(valid_ids(&env, &ledger), [record_id(&ledger, 1)]);

    ledger.approve(&env, &tablet, &mut stranger, &mut rng);
    assert!(ledger.state(&env).is_active(&stranger.id));
    let valid = valid_ids(&env, &ledger);
    for id in &forged {
        assert!(!valid.contains(id), "{id}");
    }
}

/// Alice reports the first non-derived record even with cached epoch keys.
#[test]
fn cached_keys_can_report_conflicts_against_the_current_read() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let env = Env::with(registry, Default::default(), ALICE);
    let mut rng = TestRandom(41);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let epoch = ledger.state(&env).epoch_id().to_owned();

    // Keys taken before anything of the new mechanism is published.
    let cached = epoch_keys(&env, &ledger, &laptop);
    assert!(cached.non_derived_sealing_keys().is_empty());

    // Phone publishes a later record of the new mechanism that does not
    // derive; senders select it, the cached keys know nothing of it.
    let poison = sealing_key_record(
        &env,
        &phone,
        &epoch,
        OTHER_SEAL,
        derived(&[9; 32]),
        &mut rng,
    );
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, poison.clone()));
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert_eq!(
        sealing.current(OTHER_SEAL).map(|r| &r.row.id),
        Some(&poison.id)
    );
    let fresh = epoch_keys(&env, &ledger, &laptop);

    // A later record that derives is valid and adds no conflict.
    let key = *ledger.acquire(&env, &laptop).unwrap().key();
    let sealer = env.registry.key_sealer(OTHER_SEAL).unwrap();
    let good_public = derive::account_sealing_key_pair(sealer, &key)
        .unwrap()
        .public
        .bytes;
    let good = sealing_key_record(&env, &laptop, &epoch, OTHER_SEAL, good_public, &mut rng);
    let at = ledger.next();
    ledger
        .public
        .account_keys
        .push(Accepted::new(at, good.clone()));
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert_eq!(
        sealing.current(OTHER_SEAL).map(|r| &r.row.id),
        Some(&poison.id)
    );
    assert!(sealing.is_valid(&good.id));
    for keys in [&cached, &fresh] {
        let conflicts = sealing.check_derived(&env.registry, keys).unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].record_id, poison.id);
        assert!(conflicts[0].selected);
    }
}

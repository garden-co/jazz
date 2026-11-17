//! Account sealing keys (`E2EE_PROTOCOL.md` §2.12): which
//! `__e2ee_account_keys` rows are valid, the record a sender seals to, the
//! derivation check a device of the account makes, and the later
//! publication of a migration's new mechanism.

mod common;

use common::TestRandom;
use common::mechanisms::{OTHER_SEAL, custom_registry, sealing_with};
use common::protocol::{ALICE, Env, Ledger};
use jazz_e2ee::envelope::Mechanism;
use jazz_e2ee::mechanisms::key_seal;
use jazz_e2ee::protocol::account::build::{self, LocalDevice, RotationTarget};
use jazz_e2ee::protocol::account::{AccountEpochKeys, AccountKeyEvidence, verify_private};
use jazz_e2ee::protocol::error::{IntrinsicContradiction, ProtocolError};
use jazz_e2ee::protocol::records::account::AccountSealingKey;
use jazz_e2ee::protocol::{Accepted, derive, ids, signing};

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
    let mut record = AccountSealingKey {
        id: ids::random_v4(rng).unwrap(),
        account_id: env.account.clone(),
        epoch_id: epoch_id.to_owned(),
        signer_id: signer.id.clone(),
        mechanism: BUILTIN.id.to_owned(),
        version: BUILTIN.version,
        public_key,
        signature: Vec::new(),
    };
    let bytes = record.signed_bytes(&env.application()).unwrap();
    let signer_impl = env
        .registry
        .signer(signer.signing.public.mechanism.as_mechanism())
        .unwrap();
    record.signature = signing::sign(signer_impl, &signer.signing, &bytes).unwrap();
    record
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
    let phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &phone, &mut rng);
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

/// A rotation whose author removes itself still signs the new epoch's
/// record: the creation authorises it, not the new membership.
#[test]
fn record_co_published_by_a_rotation_whose_signer_removes_itself_is_valid() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(4);
    let phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &phone, &mut rng);
    ledger.rotate(
        &env,
        &phone,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    assert_eq!(ledger.public.account_keys[1].row.signer_id, phone.id);
    assert!(valid_ids(&env, &ledger).contains(&record_id(&ledger, 1)));
}

/// At a bootstrap's position, a record signed by a device of no standing is
/// not valid, and no later approval makes it valid.
#[test]
fn record_at_a_bootstrap_signed_by_a_device_without_standing_is_not_valid() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(5);
    let laptop = env.device(&mut rng);
    let stranger = env.device(&mut rng);
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

    ledger.approve(&env, &laptop, &stranger, &mut rng);
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
    let phone = env.device(&mut rng);
    let tablet = env.device(&mut rng);
    let stranger = env.device(&mut rng);
    for device in [&phone, &tablet, &stranger] {
        ledger.request(device, &mut rng);
    }
    ledger.approve(&env, &laptop, &phone, &mut rng);
    ledger.approve(&env, &laptop, &tablet, &mut rng);
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

    ledger.approve(&env, &laptop, &stranger, &mut rng);
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
    let phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &phone, &mut rng);

    let state = ledger.state(&env);
    let key = ledger.acquire(&env, &phone).unwrap();
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert_eq!(
        build::later_account_sealing_key(&env.writer(), &sealing, &state, &phone, &key, &mut rng)
            .unwrap(),
        None,
        "the epoch already has a valid record of the built-in mechanism"
    );
    let record = build::later_account_sealing_key(
        &migrated.writer(),
        &sealing,
        &state,
        &phone,
        &key,
        &mut rng,
    )
    .unwrap()
    .expect("no record of the new mechanism yet");
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
    let state = ledger.state(&env);
    assert_eq!(
        build::later_account_sealing_key(
            &migrated.writer(),
            &sealing,
            &state,
            &phone,
            &key,
            &mut rng
        )
        .unwrap(),
        None
    );
}

/// A later publication by a device that is not active when it is accepted
/// is not valid: a removed device, and a device never approved.
#[test]
fn later_publication_by_a_device_that_was_not_active_is_not_valid() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(8);
    let phone = env.device(&mut rng);
    let stranger = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.request(&stranger, &mut rng);
    ledger.approve(&env, &laptop, &phone, &mut rng);
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
    let phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &phone, &mut rng);

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
/// `conflicting-account-key`, since only the account's own devices can
/// write one.
#[test]
fn a_valid_record_that_does_not_derive_from_the_key_is_a_contradiction() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(11);
    let phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &phone, &mut rng);
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let keys = epoch_keys(&env, &ledger, &laptop);
    let sealing = ledger.replay(&env).sealing_keys().unwrap().unwrap();
    assert_eq!(sealing.valid().len(), 2);
    sealing.check_derived(&env.registry, &keys).unwrap();

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
    assert_eq!(
        sealing.check_derived(&env.registry, &keys),
        Err(ProtocolError::Intrinsic(
            IntrinsicContradiction::ConflictingAccountKey
        ))
    );
}

/// A device reaches every account epoch key of its generation from the
/// current one, through the successors' `history`, root epoch first.
#[test]
fn epoch_keys_reach_every_epoch_of_the_generation() {
    let (env, mut ledger, laptop, mut rng) = bootstrapped(12);
    let root_key = *ledger.acquire(&env, &laptop).unwrap().key();
    let mut expected = vec![(ledger.state(&env).epoch_id().to_owned(), root_key)];
    for _ in 0..2 {
        let phone = env.device(&mut rng);
        ledger.request(&phone, &mut rng);
        ledger.approve(&env, &laptop, &phone, &mut rng);
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

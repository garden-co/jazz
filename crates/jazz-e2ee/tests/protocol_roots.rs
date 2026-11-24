//! Account roots (`E2EE_PROTOCOL.md` §2.3, §2.4 step 1): device-account roots
//! signed by the device-account identity, app roots and their trust-on-first-use
//! limit, rejected roots, floors and `root-not-mine`; and the
//! same-transaction rule of §2.4 step 3, which only valid rows take part in.

mod common;

use common::TestRandom;
use common::protocol::{ALICE, Env, Ledger};
use jazz_e2ee::mechanisms::signature;
use jazz_e2ee::protocol::account::build::{
    self, LocalDevice, ResetPolicy, RootAuthority, RotationTarget,
};
use jazz_e2ee::protocol::account::{AccountReplay, verify_private};
use jazz_e2ee::protocol::derive::{RecoveryKeys, RecoverySecret};
use jazz_e2ee::protocol::device_account::{self, DeviceAccountIdentity};
use jazz_e2ee::protocol::domain::AppDomain;
use jazz_e2ee::protocol::error::{
    IntrinsicContradiction, ProtocolError, RefusalCode, RootRejection,
};
use jazz_e2ee::protocol::records::account::{AccountKind, AccountRoot, PublicAccountSuccessor};
use jazz_e2ee::protocol::{Accepted, Position, ids, signing};

const SECRET: [u8; 32] = [7; 32];

fn identity(secret: [u8; 32]) -> DeviceAccountIdentity {
    DeviceAccountIdentity::from_secret(&RecoverySecret::from_bytes(secret)).unwrap()
}

/// The device-account derivations match `local-first-auth` and Jazz's account
/// registry. Public keys, subjects and the founder IDs for application
/// `6b0c3f8e-…` are the cases of `local-first-auth/test-vectors.json`, which
/// the TypeScript and Rust suites both check; the founder ID for the
/// protocol tests' application was computed with the TypeScript
/// `identityOf` and `localFirstAccountId`.
#[test]
fn device_account_derivation_matches_typescript() {
    let vectors =
        include_str!("../../../packages/jazz-tools/src/local-first-auth/test-vectors.json");
    let app = AppDomain::new("6b0c3f8e-2d9a-4f61-9a57-1c2e3d4f5a6b", None).unwrap();
    // The fixture's cases are flat objects whose fields contain plain ASCII
    // strings. Read all three values from the same object.
    let (_, cases) = vectors.split_once("\"cases\"").unwrap();
    let (cases, _) = cases.split_once(']').unwrap();
    let cases: Vec<_> = cases.split('{').skip(1).collect();
    let public_keys = [
        "f9f532c83b83c214b9e88f66ff159067f63d23bb405b8c82be54dd2a06b806d4",
        "b80682ae077693de867d390b4762319d13ebbd00a792a5f4aadc1b5f9171e893",
        "ff3d97bfb89227b2ae43697296031fe63bcfb9628f17df15e0e28013021451d9",
        "8152f422c30f383f5b263a4c1832b59f2203795cfb7b7efe2fa3f32a4174fff3",
    ];
    assert_eq!(cases.len(), public_keys.len());
    for (case, public_key) in cases.into_iter().zip(public_keys) {
        let field = |name: &str| {
            let (_, tail) = case.split_once(&format!("\"{name}\"")).unwrap();
            let quoted = tail.trim_start().strip_prefix(':').unwrap().trim_start();
            quoted.strip_prefix('"').unwrap().split_once('"').unwrap().0
        };
        let secret = RecoverySecret::parse(field("secret")).unwrap();
        let subject = field("subject");
        let founder = field("founderAccountV1");
        let identity = DeviceAccountIdentity::from_secret(&secret).unwrap();
        assert_eq!(common::to_hex(identity.public_key()), public_key);
        assert_eq!(identity.subject(), subject);
        assert_eq!(identity.account_id(&app).unwrap(), founder);
        assert!(device_account::derives_account(
            &app,
            identity.public_key(),
            founder
        ));
        assert!(!device_account::derives_account(
            &app,
            identity.public_key(),
            subject
        ));
    }
    let tests = AppDomain::new(common::protocol::APP_ID, None).unwrap();
    assert_eq!(
        identity(std::array::from_fn(|i| i as u8))
            .account_id(&tests)
            .unwrap(),
        "c5c24ec7-dbbe-5955-892c-f1869b943bd3"
    );
    assert!(!format!("{:?}", identity(SECRET)).contains("signing: KeyPair"));
}

/// A device account's root is signed by its identity; the account
/// replays and its device obtains the key.
#[test]
fn device_account_root_is_signed_and_counts() {
    let env = Env::device_account(SECRET);
    let mut rng = TestRandom(101);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(&env.account);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let state = ledger.state(&env);
    assert_eq!(state.root.row.kind, AccountKind::DeviceAccount);
    assert!(state.rejected_roots.is_empty());
    state
        .check_device_account(env.identity.as_ref().unwrap())
        .unwrap();
    assert!(ledger.acquire(&env, &laptop).is_ok());

    // Another identity's key does not found this account.
    let writer = env.writer();
    let mut scratch = Ledger::new(&env.account);
    scratch.request(&laptop, &mut rng);
    let permit =
        build::check_bootstrap(&writer, &laptop, &scratch.public, &scratch.private).unwrap();
    let other = identity([8; 32]);
    assert_eq!(
        build::account_root(
            &writer,
            &laptop,
            &permit,
            RootAuthority::DeviceAccount(&other),
            &mut rng
        )
        .unwrap_err(),
        ProtocolError::Refused(RefusalCode::DeviceAccountIdentityMismatch)
    );
}

fn device_account_ledger(
    seed: u64,
) -> (
    Env,
    Ledger,
    TestRandom,
    jazz_e2ee::protocol::account::build::LocalDevice,
) {
    let env = Env::device_account(SECRET);
    let mut rng = TestRandom(seed);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(&env.account);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    (env, ledger, rng, laptop)
}

fn rejection(ledger: &Ledger, env: &Env, root_id: &str) -> Option<RootRejection> {
    AccountReplay::new(&ledger.public, &env.domain, &env.registry)
        .unwrap()
        .rejected_roots()
        .unwrap()
        .into_iter()
        .find(|rejected| rejected.root_id == root_id)
        .map(|rejected| rejected.reason)
}

/// Nobody without the secret can write or substitute a device-account root:
/// an unsigned app root, a tampered signature, a key that does not derive
/// the account and substituted root device keys are all rejected and
/// reported, never counted.
#[test]
fn forged_device_account_roots_do_not_count() {
    let (env, ledger, mut rng, laptop) = device_account_ledger(102);
    let genuine = ledger.public.roots[0].clone();
    let mallory = env.device(&mut rng);

    // An app root written before the genuine one, for another device.
    let mut forged = ledger.public.clone();
    let app_root = AccountRoot {
        id: ids::random_v4(&mut rng).unwrap(),
        device_id: mallory.id.clone(),
        epoch_id: ids::random_v4(&mut rng).unwrap(),
        kind: AccountKind::App,
        device_account_public_key: None,
        device_account_signature: None,
        ..genuine.row.clone()
    };
    forged
        .roots
        .insert(0, Accepted::new(Position(0), app_root.clone()));
    forged.keys.push(Accepted::new(
        Position(0),
        mallory.keys_row(&mut rng).unwrap(),
    ));
    let mut with_forgery = Ledger::new(&env.account);
    with_forgery.public = forged;
    with_forgery.private = ledger.private.clone();
    let state = with_forgery.state(&env);
    assert_eq!(state.root, genuine);
    assert_eq!(
        rejection(&with_forgery, &env, &app_root.id),
        Some(RootRejection::KindMismatch)
    );

    // A tampered signature: no counting root at all.
    let mut tampered = Ledger::new(&env.account);
    tampered.public = ledger.public.clone();
    let signature = tampered.public.roots[0]
        .row
        .device_account_signature
        .as_mut()
        .unwrap();
    let last = signature.len() - 1;
    signature[last] ^= 1;
    assert!(tampered.replay(&env).current().unwrap().is_none());
    assert_eq!(
        rejection(&tampered, &env, &genuine.row.id),
        Some(RootRejection::SignatureInvalid)
    );

    // A root signed by a key that does not derive the account ID.
    let other_env = Env::device_account([9; 32]);
    let mut other = Ledger::new(&other_env.account);
    other.request(&laptop, &mut rng);
    other.root(&other_env, &laptop, 0, &mut rng);
    let mut foreign = Ledger::new(&env.account);
    foreign.public.keys = ledger.public.keys.clone();
    let mut moved = other.public.roots[0].clone();
    moved.row.account_id = env.account.clone();
    foreign.public.roots.push(moved.clone());
    foreign.public.through = ledger.public.through;
    assert!(foreign.replay(&env).current().unwrap().is_none());
    assert_eq!(
        rejection(&foreign, &env, &moved.row.id),
        Some(RootRejection::KeyMismatch)
    );

    // The root device's published keys replaced: the signature binds them.
    let mut swapped = Ledger::new(&env.account);
    swapped.public = ledger.public.clone();
    swapped.public.keys[0].row.keys = mallory.keys();
    assert!(swapped.replay(&env).current().unwrap().is_none());
    assert_eq!(
        rejection(&swapped, &env, &genuine.row.id),
        Some(RootRejection::SignatureInvalid)
    );

    // A malformed root is reported too.
    let mut malformed = Ledger::new(&env.account);
    malformed.public = ledger.public.clone();
    malformed.public.roots[0].row.device_account_public_key = None;
    assert_eq!(
        rejection(&malformed, &env, &genuine.row.id),
        Some(RootRejection::Malformed)
    );
}

/// A row that copies the account's public key and signature but names a
/// device without published keys cannot verify: it is rejected and
/// reported, and replay of the genuine root proceeds. So is a root whose
/// device's keys were accepted only after it.
#[test]
fn device_account_root_without_device_keys_is_rejected() {
    let (env, ledger, mut rng, laptop) = device_account_ledger(108);
    let genuine = ledger.public.roots[0].clone();
    let ghost = ids::random_v4(&mut rng).unwrap();

    let mut forged = Ledger::new(&env.account);
    forged.public = ledger.public.clone();
    forged.private = ledger.private.clone();
    let copy = AccountRoot {
        id: ids::random_v4(&mut rng).unwrap(),
        device_id: ghost.clone(),
        ..genuine.row.clone()
    };
    forged
        .public
        .roots
        .insert(0, Accepted::new(Position(0), copy.clone()));
    let state = forged.state(&env);
    assert_eq!(state.root, genuine);
    assert_eq!(
        rejection(&forged, &env, &copy.id),
        Some(RootRejection::DeviceKeysMissing)
    );
    assert_eq!(
        RootRejection::DeviceKeysMissing.code(),
        "root-device-keys-missing"
    );
    assert!(forged.acquire(&env, &laptop).is_ok());

    // The device's keys accepted only after the root do not count for it.
    let mut late = Ledger::new(&env.account);
    late.public = ledger.public.clone();
    let root_at = late.public.roots[0].position;
    late.public.keys[0].position = Position(root_at.0 + 1);
    late.public.through = Position(root_at.0 + 1);
    assert!(late.replay(&env).current().unwrap().is_none());
    assert_eq!(
        rejection(&late, &env, &genuine.row.id),
        Some(RootRejection::DeviceKeysMissing)
    );
}

/// A later generation of a device account counts only if it is signed
/// by the same identity: an app reset root is rejected, a signed one counts.
/// The builders never reset a device account.
#[test]
fn device_account_generations_need_the_same_identity() {
    let (env, mut ledger, mut rng, laptop) = device_account_ledger(103);
    let phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    let state = ledger.state(&env);
    let external = ResetPolicy {
        device_account: false,
        allow_account_reset: true,
    };
    assert_eq!(
        build::check_reset(
            &env.writer(),
            external,
            &state,
            &phone,
            &ledger.public,
            &ledger.private
        ),
        Err(ProtocolError::Refused(RefusalCode::AccountResetNotAllowed))
    );

    // A generation-1 permit from an app account of the same ID, to build
    // raw reset roots for this account.
    let app_env = Env::new(&env.account);
    let mut scratch = Ledger::new(&env.account);
    scratch.request(&laptop, &mut rng);
    scratch.root(&app_env, &laptop, 0, &mut rng);
    scratch.request(&phone, &mut rng);
    let permit = build::check_reset(
        &app_env.writer(),
        external,
        &scratch.state(&app_env),
        &phone,
        &scratch.public,
        &scratch.private,
    )
    .unwrap();

    let unsigned =
        build::account_root(&env.writer(), &phone, &permit, RootAuthority::App, &mut rng).unwrap();
    let mut app_reset = Ledger::new(&env.account);
    app_reset.public = ledger.public.clone();
    let at = Position(ledger.public.through.0 + 1);
    app_reset.public.through = at;
    app_reset
        .public
        .roots
        .push(Accepted::new(at, unsigned.root.clone()));
    assert_eq!(app_reset.state(&env).generation, 0);
    assert_eq!(
        rejection(&app_reset, &env, &unsigned.root.id),
        Some(RootRejection::KindMismatch)
    );

    let signed =
        build::account_root(&env.writer(), &phone, &permit, env.authority(), &mut rng).unwrap();
    let mut signed_reset = Ledger::new(&env.account);
    signed_reset.public = ledger.public.clone();
    signed_reset.public.through = at;
    signed_reset
        .public
        .roots
        .push(Accepted::new(at, signed.root.clone()));
    let state = signed_reset.state(&env);
    assert_eq!(state.generation, 1);
    assert_eq!(state.roots.len(), 2);
    assert!(state.is_active(&phone.id));
}

/// App roots are trusted on first use. A device that keeps a floor of the
/// state it verified detects a substituted root (`root-not-mine`) and a
/// rolled-back read (`stale-state`); its own identity naming another root
/// is `root-not-mine` too.
#[test]
fn substituted_app_root_is_not_mine() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(104);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let state = ledger.state(&env);
    let floor = state.floor(ledger.public.through, Some(&laptop.id));
    state.check_floor(ledger.public.through, &floor).unwrap();

    // The server replaces the root with one of its own device.
    let mallory = env.device(&mut rng);
    let mut substituted = Ledger::new(ALICE);
    substituted.public = ledger.public.clone();
    substituted.private = ledger.private.clone();
    let root = &mut substituted.public.roots[0].row;
    root.id = ids::random_v4(&mut rng).unwrap();
    root.device_id = mallory.id.clone();
    root.epoch_id = ids::random_v4(&mut rng).unwrap();
    let at = substituted.public.roots[0].position;
    substituted
        .public
        .keys
        .push(Accepted::new(at, mallory.keys_row(&mut rng).unwrap()));
    let forged = substituted.state(&env);
    assert_eq!(
        forged.check_floor(substituted.public.through, &floor),
        Err(ProtocolError::RootNotMine)
    );
    // Without a floor, the account's own identity row still names the
    // root it wrote.
    let replay = substituted.replay(&env);
    assert_eq!(
        verify_private(&replay, &forged, &substituted.private).unwrap_err(),
        ProtocolError::RootNotMine
    );
    // A device of a device account checks the root against its own identity.
    assert_eq!(
        forged.check_device_account(&identity(SECRET)),
        Err(ProtocolError::RootNotMine)
    );

    // An older read than the floor is stale.
    let earlier = ledger.public.before(ledger.public.through);
    let old = AccountReplay::new(&earlier, &env.domain, &env.registry)
        .unwrap()
        .current()
        .unwrap()
        .unwrap();
    assert_eq!(
        old.check_floor(earlier.through, &floor),
        Err(ProtocolError::StaleState)
    );
}

/// Conflicting roots of generation 0 fail current replay even after a
/// reset, as they fail every state as of a position after both.
#[test]
fn conflicting_roots_fail_at_every_generation() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(105);
    let laptop = env.device(&mut rng);
    let phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.root(&env, &phone, 1, &mut rng);
    assert_eq!(ledger.state(&env).generation, 1);

    let mut conflicting = ledger.public.clone();
    let mut other = conflicting.roots[0].clone();
    other.row.id = ids::random_v4(&mut rng).unwrap();
    other.row.device_id = phone.id.clone();
    conflicting.roots.push(other);
    let replay = AccountReplay::new(&conflicting, &env.domain, &env.registry).unwrap();
    let conflict = ProtocolError::Intrinsic(IntrinsicContradiction::ConflictingRoots);
    assert_eq!(replay.current().unwrap_err(), conflict);
    assert_eq!(
        replay
            .as_of(Position(conflicting.through.0 + 1))
            .unwrap_err(),
        conflict
    );
}

/// Two different valid generation-0 roots fail closed for good: a reset
/// accepted after both does not escape the conflict. Only valid roots count
/// toward it: a malformed or foreign generation-0 row naming another device
/// is rejected and reported instead.
#[test]
fn generation_zero_conflict_survives_a_later_reset() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(109);
    let laptop = env.device(&mut rng);
    let phone = env.device(&mut rng);
    let tablet = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let phone_at = ledger.request(&phone, &mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.root(&env, &tablet, 1, &mut rng);
    assert_eq!(ledger.state(&env).generation, 1);

    let mut second = ledger.public.roots[0].clone();
    second.row.id = ids::random_v4(&mut rng).unwrap();
    second.row.device_id = phone.id.clone();
    second.row.epoch_id = ids::random_v4(&mut rng).unwrap();
    second.position = phone_at;
    let conflict = ProtocolError::Intrinsic(IntrinsicContradiction::ConflictingRoots);

    let mut conflicting = ledger.public.clone();
    conflicting.roots.push(second.clone());
    let replay = AccountReplay::new(&conflicting, &env.domain, &env.registry).unwrap();
    assert_eq!(replay.current().unwrap_err(), conflict);
    assert_eq!(replay.as_of(conflicting.through).unwrap_err(), conflict);
    assert_eq!(conflict.code(), Some("conflicting-roots"));
    // Before the second root, the account was fine.
    assert_eq!(replay.as_of(phone_at).unwrap().unwrap().generation, 0);

    // Invalid rows do not count toward the conflict.
    let mut malformed = second.clone();
    malformed.row.epoch_id = "not-an-id".into();
    let mut foreign = second.clone();
    foreign.row.account_id = "3a110000-0000-5000-8000-0000000003a1".into();
    // Account IDs are UUIDs: any other text is malformed.
    let mut not_uuid = second.clone();
    not_uuid.row.account_id = "mallory".into();
    for (row, reason) in [
        (malformed, RootRejection::Malformed),
        (foreign, RootRejection::ForeignAccount),
        (not_uuid, RootRejection::Malformed),
    ] {
        let mut view = Ledger::new(ALICE);
        view.public = ledger.public.clone();
        view.public.roots.push(row.clone());
        assert_eq!(view.state(&env).generation, 1);
        assert_eq!(rejection(&view, &env, &row.row.id), Some(reason));
    }
}

/// Successors of one epoch at one position were written in one
/// transaction. Rows that fail any check take no part in the rule: an
/// invalid successor at the same position (bad signature, a signer that is
/// not active, malformed, a wrong revision) does not block a valid one, nor
/// does an invalid or foreign recovery root.
#[test]
fn invalid_rows_in_the_same_transaction_do_not_block_a_successor() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(106);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let stranger = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger.request(&stranger, &mut rng);
    let recovery =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([5; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &recovery, &mut rng);
    let (at, records) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    assert!(ledger.state(&env).is_removed(&phone.id));

    let sign = |row: &mut PublicAccountSuccessor, author: &LocalDevice| {
        let bytes = row.signed_bytes(&env.application()).unwrap();
        row.signature = signing::sign(&signature::Builtin, &author.signing, &bytes).unwrap();
    };
    let mut control = records.public.clone();
    control.id = ids::random_v4(&mut rng).unwrap();
    control.epoch_id = ids::random_v4(&mut rng).unwrap();
    sign(&mut control, &laptop);
    let replay = |view| {
        AccountReplay::new(&view, &env.domain, &env.registry)
            .unwrap()
            .current()
            .unwrap()
            .unwrap()
    };
    let mut standalone = ledger.public.clone();
    standalone.successors.clear();
    standalone
        .successors
        .push(Accepted::new(at, control.clone()));
    assert_eq!(replay(standalone.clone()).epoch_id(), control.epoch_id);

    let mut tie = ledger.public.clone();
    tie.successors.push(Accepted::new(at, control.clone()));
    assert!(
        !replay(tie).is_removed(&phone.id),
        "two valid successors block each other"
    );

    let mut bad_signature = control.clone();
    *bad_signature.signature.last_mut().unwrap() ^= 1;
    let mut inactive_signer = control.clone();
    inactive_signer.signer_id = stranger.id.clone();
    sign(&mut inactive_signer, &stranger);
    let mut malformed = control.clone();
    malformed.removed_device_id = None;
    // A removal with no target has no valid signed representation.
    assert!(malformed.signed_bytes(&env.application()).is_err());
    let mut wrong_revision = control;
    assert_ne!(wrong_revision.revision, b"[]");
    wrong_revision.revision = b"[]".to_vec();
    sign(&mut wrong_revision, &laptop);
    for (name, invalid) in [
        ("bad signature", bad_signature),
        ("inactive signer", inactive_signer),
        ("malformed", malformed),
        ("wrong revision", wrong_revision),
    ] {
        let mut alone = standalone.clone();
        alone.successors[0].row = invalid.clone();
        assert!(!replay(alone).is_removed(&phone.id), "{name} was accepted");

        let mut tie = ledger.public.clone();
        tie.successors.push(Accepted::new(at, invalid));
        assert!(
            replay(tie).is_removed(&phone.id),
            "{name} blocked the valid successor"
        );
    }

    let mut bad_root = ledger.public.recovery_roots[0].clone();
    bad_root.row.id = ids::random_v4(&mut rng).unwrap();
    bad_root.position = at;
    let last = bad_root.row.signature.len() - 1;
    bad_root.row.signature[last] ^= 1;
    let mut foreign_root = ledger.public.recovery_roots[0].clone();
    foreign_root.row.id = ids::random_v4(&mut rng).unwrap();
    foreign_root.row.account_id = "3a110000-0000-5000-8000-0000000003a1".into();
    foreign_root.position = at;
    for root in [bad_root, foreign_root] {
        let mut view = ledger.public.clone();
        view.recovery_roots.push(root);
        let state = AccountReplay::new(&view, &env.domain, &env.registry)
            .unwrap()
            .current()
            .unwrap()
            .unwrap();
        assert!(state.is_removed(&phone.id));
    }

    // A valid recovery root in the same transaction still blocks it.
    let mut view = ledger.public.clone();
    view.recovery_roots[0].position = at;
    let state = AccountReplay::new(&view, &env.domain, &env.registry)
        .unwrap()
        .current()
        .unwrap()
        .unwrap();
    assert!(!state.is_removed(&phone.id));
}

/// A device whose floor records it active and that is not active in a newer
/// state re-enrols (`device-no-longer-active`) instead of raising an alert:
/// it was removed while offline and the account was then reset, or the
/// account moved to a generation it is not part of. Another account's floor
/// follows the new generation, and a device approved in the new generation
/// passes.
#[test]
fn device_no_longer_active_after_removal_and_reset() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(107);
    let mut laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let before = ledger.state(&env);
    let phone_floor = before.floor(ledger.public.through, Some(&phone.id));
    let laptop_floor = before.floor(ledger.public.through, Some(&laptop.id));
    let other = before.floor(ledger.public.through, None);
    assert_eq!(
        phone_floor.active_device.as_deref(),
        Some(phone.id.as_str())
    );

    // The phone is offline while it is removed: same generation.
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let removed = ledger.state(&env);
    assert_eq!(
        removed.check_floor(ledger.public.through, &phone_floor),
        Err(ProtocolError::DeviceNoLongerActive)
    );
    removed
        .check_floor(ledger.public.through, &laptop_floor)
        .unwrap();

    // Then the account is reset by a new device.
    let tablet = env.device(&mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.root(&env, &tablet, 1, &mut rng);
    let after = ledger.state(&env);
    for floor in [&phone_floor, &laptop_floor] {
        let outcome = after.check_floor(ledger.public.through, floor);
        assert_eq!(outcome, Err(ProtocolError::DeviceNoLongerActive));
        assert_eq!(outcome.unwrap_err().code(), Some("device-no-longer-active"));
    }
    after.check_floor(ledger.public.through, &other).unwrap();

    // A device approved in the new generation passes its old floor.
    ledger.approve(&env, &tablet, &mut laptop, &mut rng);
    ledger
        .state(&env)
        .check_floor(ledger.public.through, &laptop_floor)
        .unwrap();
}

/// The root this device verified, contradicted while it is still supposed
/// to be active, stays `root-not-mine`, also when a later generation follows
/// the replaced root.
#[test]
fn replaced_root_before_a_reset_is_not_mine() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(110);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let floor = ledger
        .state(&env)
        .floor(ledger.public.through, Some(&laptop.id));

    let mallory = env.device(&mut rng);
    let tablet = env.device(&mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.root(&env, &tablet, 1, &mut rng);
    let mut replaced = Ledger::new(ALICE);
    replaced.public = ledger.public.clone();
    replaced.private = ledger.private.clone();
    let root = &mut replaced.public.roots[0].row;
    root.id = ids::random_v4(&mut rng).unwrap();
    root.device_id = mallory.id.clone();
    root.epoch_id = ids::random_v4(&mut rng).unwrap();
    let at = replaced.public.roots[0].position;
    replaced
        .public
        .keys
        .push(Accepted::new(at, mallory.keys_row(&mut rng).unwrap()));
    let state = replaced.state(&env);
    assert_eq!(state.generation, 1);
    assert_eq!(
        state.check_floor(replaced.public.through, &floor),
        Err(ProtocolError::RootNotMine)
    );
}

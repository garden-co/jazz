//! Account and device lifecycle (`E2EE_PROTOCOL.md` §2): records built by
//! the crate's builders, accepted at positions, and replayed. These tests
//! drive the public replay, private verification and key APIs end to end.

mod common;

use common::TestRandom;
use common::mechanisms::{OTHER_SEAL, UNROUTED_SEAL, custom_registry, relabelled};
use common::protocol::{ALICE, Env, Ledger};
use jazz_e2ee::agility::MechanismId;
use jazz_e2ee::agility::Signer as _;
use jazz_e2ee::mechanisms::key_seal;
use jazz_e2ee::mechanisms::signature;
use jazz_e2ee::protocol::account::build::{self, LocalDevice, ResetPolicy, RotationTarget};
use jazz_e2ee::protocol::account::{AccountKeyEvidence, PrivateAccountLedger, verify_private};
use jazz_e2ee::protocol::derive::{self, RecoveryKeys, RecoverySecret};
use jazz_e2ee::protocol::error::{
    IntrinsicContradiction, ProtocolError, RecoveryFailure, RefusalCode, UnsupportedCapability,
};
use jazz_e2ee::protocol::ids::IdForm;
use jazz_e2ee::protocol::records::account::{
    AccountKind, AccountRoot, approval_code, challenge_signed_bytes, nonce_commitment,
    proof_signed_bytes, recovery_delivery_context,
};
use jazz_e2ee::protocol::{Accepted, Position, ids, signing};

fn refused(code: RefusalCode) -> ProtocolError {
    ProtocolError::Refused(code)
}

/// The first device bootstraps the account: replay makes it the only active
/// device of the root epoch, and it obtains the root key from its identity
/// envelope.
///
/// ```text
/// alice-laptop ──request──► ledger ──bootstrap──► root(gen 0, epoch e0)
/// ```
#[test]
fn bootstrap_makes_the_first_device_active() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(1);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    build::check_bootstrap(&env.writer(), &laptop, &ledger.public, &ledger.private).unwrap();
    let (at, key) = ledger.root(&env, &laptop, 0, &mut rng);

    let state = ledger.state(&env);
    assert_eq!(state.generation, 0);
    assert_eq!(state.root.position, at);
    assert!(state.is_active(&laptop.id));
    assert_eq!(state.active.len(), 1);
    assert_eq!(state.epochs.len(), 1);

    let acquired = ledger.acquire(&env, &laptop).unwrap();
    assert_eq!(*acquired.key(), key);
    assert_eq!(acquired.authenticated().epoch_id, state.epoch_id());
}

/// Bootstrap refuses a device without a request, a device whose keys differ
/// from its request, and an account that already has history.
#[test]
fn bootstrap_preconditions_refuse() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(2);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    assert_eq!(
        build::check_bootstrap(&env.writer(), &laptop, &ledger.public, &ledger.private),
        Err(refused(RefusalCode::NoDeviceRequest))
    );
    let impostor = env.device(&mut rng);
    let mut other = Ledger::new(ALICE);
    other.request(&laptop, &mut rng);
    other.private.requests[0].row.keys = impostor.keys();
    assert_eq!(
        build::check_bootstrap(&env.writer(), &laptop, &other.public, &other.private),
        Err(refused(RefusalCode::DeviceKeysMismatch))
    );
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    assert_eq!(
        build::check_bootstrap(&env.writer(), &laptop, &ledger.public, &ledger.private),
        Err(refused(RefusalCode::AccountHasHistory))
    );
}

/// Challenge and response approve a second device; private verification
/// accepts the approval, and the new device obtains the account key from
/// its approval delivery.
///
/// ```text
/// laptop ──challenge──► phone ──proof──► laptop ──approval+delivery──► ledger
/// ```
#[test]
fn approval_activates_a_device_that_then_obtains_the_key() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(3);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    let (_, key) = ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let (at, records) = ledger.approve(&env, &laptop, &mut phone, &mut rng);
    assert!(
        phone.approval_nonce.is_none(),
        "enrolment consumes the nonce"
    );

    let (state, evidence) = ledger.evidence(&env);
    assert!(state.is_active(&phone.id));
    assert_eq!(state.approvals.len(), 1);
    assert_eq!(state.approvals[0].position, at);
    assert_eq!(evidence.approvals().len(), 1);
    assert!(evidence.approvals()[0].delivery.is_some());
    assert!(evidence.identity().is_some());

    let acquired = ledger.acquire(&env, &phone).unwrap();
    assert_eq!(*acquired.key(), key);
    assert!(
        acquired
            .authenticated()
            .approval_ids
            .contains(&records.approval.id)
    );

    // Approval is complete before its position, not at it.
    let replay = ledger.replay(&env);
    assert!(!replay.as_of(at).unwrap().unwrap().is_active(&phone.id));
    let then = replay.as_of(Position(at.0 + 1)).unwrap().unwrap();
    assert!(then.is_active(&phone.id));
    assert_eq!(then.epoch_id(), state.epoch_id());
}

fn two_approval_deliveries(env: &Env) -> (Ledger, LocalDevice, [u8; 32]) {
    let mut rng = TestRandom(3);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    let (_, key) = ledger.root(env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    // Stage the second historical delivery from a snapshot of the requester
    // taken before it answered.
    let mut historical_phone = phone.clone();
    ledger.approve(env, &laptop, &mut phone, &mut rng);
    ledger.approve(env, &laptop, &mut historical_phone, &mut rng);
    (ledger, phone, key)
}

/// Alice's phone skips an approval delivery with an unknown sealing
/// mechanism and acquires the account key from the next delivery.
///
/// ```text
/// Alice's phone ── first approval delivery ──✗ unsupported
///               ── second approval delivery ──► account key
/// ```
#[test]
fn account_acquisition_skips_an_unsupported_delivery() {
    let env = Env::new(ALICE);
    let (mut ledger, phone, key) = two_approval_deliveries(&env);
    let first = &mut ledger.private.deliveries[0].row.envelope;
    *first = relabelled(key_seal::MECHANISM, UNROUTED_SEAL, first);
    assert_eq!(ledger.evidence(&env).1.approvals().len(), 2);
    assert_eq!(*ledger.acquire(&env, &phone).unwrap().key(), key);
}

/// Alice's phone reports unsupported after exhausting its approval
/// deliveries when both use an unknown sealing mechanism.
#[test]
fn account_acquisition_reports_only_unsupported_deliveries() {
    let env = Env::new(ALICE);
    let (mut ledger, phone, _) = two_approval_deliveries(&env);
    for delivery in &mut ledger.private.deliveries {
        let envelope = &mut delivery.row.envelope;
        *envelope = relabelled(key_seal::MECHANISM, UNROUTED_SEAL, envelope);
    }
    assert_eq!(ledger.evidence(&env).1.approvals().len(), 2);
    assert_eq!(
        ledger.acquire(&env, &phone).unwrap_err(),
        ProtocolError::Unsupported(UnsupportedCapability::AccountKeyEnvelope)
    );
}

/// A tampered public approval signature, or a non-recovery approval
/// carrying a recovery signature, is skipped rather than failing replay.
#[test]
fn tampered_approvals_are_skipped() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(4);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);

    let mut flipped = ledger.public.clone();
    let last = flipped.approvals[0].row.signature.len() - 1;
    flipped.approvals[0].row.signature[last] ^= 1;
    let mut forged = ledger.public.clone();
    forged.approvals[0].row.recovery_signature = Some(vec![1, 2, 3]);
    for public in [flipped, forged] {
        let state =
            jazz_e2ee::protocol::account::AccountReplay::new(&public, &env.domain, &env.registry)
                .unwrap()
                .current()
                .unwrap()
                .unwrap();
        assert!(!state.is_active(&phone.id));
        assert!(state.approvals.is_empty());
    }
}

/// Approvals at one position are judged against the active set before the
/// batch: a device approved in a batch cannot approve another in the same
/// batch.
///
/// ```text
/// position p: laptop approves phone, phone approves tablet → tablet skipped
/// ```
#[test]
fn approvals_in_one_batch_do_not_authorise_each_other() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(5);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut tablet = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.request(&tablet, &mut rng);
    let (_, first) = ledger.approve(&env, &laptop, &mut phone, &mut rng);
    // Pretend phone's approval of tablet was accepted with laptop's.
    let batch = ledger.public.approvals[0].position;
    let (_, second) = ledger.approve(&env, &phone, &mut tablet, &mut rng);
    ledger
        .public
        .approvals
        .retain(|row| row.row.id != second.public.id);
    ledger
        .public
        .approvals
        .push(Accepted::new(batch, second.public));
    let state = ledger.state(&env);
    assert!(state.is_active(&phone.id));
    assert!(!state.is_active(&tablet.id));
    assert_eq!(state.approvals[0].row.id, first.public.id);
}

/// Removing a device rotates the account: the survivors obtain the new key
/// through the successor's deliveries and its history back to the root; the
/// removed device has no candidate and is never reactivated.
///
/// ```text
/// e0 {laptop, phone} ──successor(remove phone)──► e1 {laptop}
///                                    phone re-approved in e1 ──✗── skipped
/// ```
#[test]
fn removal_rotates_and_excludes_the_device() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(6);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let (at, rotation) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );

    let state = ledger.state(&env);
    assert_eq!(state.epoch_id(), rotation.public.columns.epoch_id);
    assert_eq!(state.current_epoch().position, at);
    assert!(state.is_removed(&phone.id));
    assert!(!state.is_active(&phone.id));

    let acquired = ledger.acquire(&env, &laptop).unwrap();
    assert_eq!(*acquired.key(), *rotation.key);
    assert_eq!(
        ledger.acquire(&env, &phone).unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );

    // A removed device cannot be challenged, and an approval of it in the
    // new epoch is skipped.
    let writer = env.writer();
    assert_eq!(
        build::challenge(&writer, &state, &laptop, &phone.request(), &mut rng).unwrap_err(),
        refused(RefusalCode::DeviceRemoved)
    );
    assert_eq!(
        build::rotate(
            &writer,
            &state,
            &laptop,
            &acquired,
            &RotationTarget::RemoveDevice(phone.id.clone()),
            &mut rng,
        )
        .unwrap_err(),
        refused(RefusalCode::TargetNotActive)
    );
}

/// Two valid successors of one predecessor at one position were written in
/// one transaction and are unordered: neither is accepted, and the epoch
/// does not move.
#[test]
fn concurrent_successors_at_one_position_both_lose() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(7);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let before = ledger.state(&env);
    let (at, _) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let mut other = ledger.public.clone();
    other.successors.pop();
    let mut staged = Ledger::new(ALICE);
    staged.public = other;
    staged.private = ledger.private.clone();
    staged.private.successors.pop();
    let (_, second) = staged.rotate(
        &env,
        &phone,
        RotationTarget::RemoveDevice(laptop.id.clone()),
        &mut rng,
    );
    ledger
        .public
        .successors
        .push(Accepted::new(at, second.public));
    let state = ledger.state(&env);
    assert_eq!(state.epoch_id(), before.epoch_id());
    assert!(state.removed.is_empty());
}

/// A successor accepted at the same position as a valid recovery root is
/// skipped; so is one whose public revision differs from replay's.
#[test]
fn successor_tie_with_recovery_root_and_wrong_revision_are_skipped() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(8);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let secret = RecoverySecret::from_bytes([9; 32]);
    let recovery = RecoveryKeys::derive(&secret, &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &recovery, &mut rng);
    let (at, _) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    assert!(ledger.state(&env).is_removed(&phone.id));

    let mut tied = ledger.public.clone();
    tied.recovery_roots[0].position = at;
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&tied, &env.domain, &env.registry)
            .unwrap();
    assert!(replay.current().unwrap().unwrap().removed.is_empty());

    let mut wrong = ledger.public.clone();
    wrong.successors[0].row.revision = b"[]".to_vec();
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&wrong, &env.domain, &env.registry)
            .unwrap();
    assert!(replay.current().unwrap().unwrap().removed.is_empty());
}

/// Recovery: a root derived from the recovery secret is registered and
/// receives the account key; a new device restores with the secret alone,
/// approves itself with the recovery key, and becomes active although no
/// active device signed.
///
/// ```text
/// laptop ──register root(s)──► ledger ──recovery delivery──► root
/// (laptop lost) new-laptop ──derive(s)──► open delivery ──self-approve──► active
/// ```
#[test]
fn recovery_restores_a_new_device() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(9);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    let (_, key) = ledger.root(&env, &laptop, 0, &mut rng);
    let secret = RecoverySecret::from_bytes([7; 32]);
    let recovery = RecoveryKeys::derive(&secret, &env.application(), 0).unwrap();
    let root = ledger.register_recovery(&env, &laptop, &recovery, &mut rng);
    assert_eq!(root.id, recovery.root_id);
    assert_eq!(ids::uuid_version(&root.id), Some(8));
    assert_eq!(ledger.state(&env).recovery_roots.len(), 1);

    let replacement = env.device(&mut rng);
    ledger.request(&replacement, &mut rng);
    let restored = RecoveryKeys::derive(&secret, &env.application(), 0).unwrap();
    let (state, evidence) = ledger.evidence(&env);
    let replay = ledger.replay(&env);
    let account_key = AccountKeyEvidence::new(&replay, &state, &evidence)
        .unwrap()
        .acquire_with_recovery(&restored, &ledger.private.recovery_deliveries)
        .unwrap();
    assert_eq!(*account_key.key(), key);

    let records = build::recovery_approve(
        &env.writer(),
        &state,
        &mut replacement.clone(),
        &restored,
        &account_key,
        &mut rng,
    )
    .unwrap()
    .expect("not yet active");
    let at = ledger.next();
    ledger
        .private
        .challenges
        .push(Accepted::new(at, records.challenge));
    ledger.private.proofs.push(Accepted::new(at, records.proof));
    ledger.accept_approval(at, &records.records);
    let state = ledger.state(&env);
    assert!(state.is_active(&replacement.id));
    let acquired = ledger.acquire(&env, &replacement).unwrap();
    assert_eq!(*acquired.key(), key);

    // Already active: nothing to write.
    assert!(
        build::recovery_approve(
            &env.writer(),
            &state,
            &mut replacement.clone(),
            &restored,
            &acquired,
            &mut rng
        )
        .unwrap()
        .is_none()
    );

    // Removing the device that recovered retires the root it recovered
    // with in the same rotation (an app account), so the recovery secret it
    // may still hold opens nothing new.
    let before_removal = ledger.public.clone();
    let (at, removal) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(replacement.id.clone()),
        &mut rng,
    );
    assert_eq!(
        removal.public.columns.targets().unwrap().1,
        vec![root.id.clone()]
    );
    assert!(removal.recovery_deliveries.is_empty());
    let state = ledger.state(&env);
    assert!(state.is_removed(&replacement.id));
    assert!(state.recovery_roots.is_empty());
    assert!(state.retired_recovery_roots.contains_key(&root.id));

    // A removal that leaves the root active is not accepted.
    let mut keeps_root = removal.public.clone();
    keeps_root.columns.retired_recovery_root_ids = b"[]".to_vec();
    let bytes = keeps_root.signed_bytes(&env.application()).unwrap();
    keeps_root.signature = signature::Builtin.sign(&laptop.signing, &bytes).unwrap();
    let mut view = before_removal;
    view.through = at;
    view.successors.push(Accepted::new(at, keeps_root));
    let state = jazz_e2ee::protocol::account::AccountReplay::new(&view, &env.domain, &env.registry)
        .unwrap()
        .current()
        .unwrap()
        .unwrap();
    assert!(!state.is_removed(&replacement.id));
}

/// Alice's recovery skips a delivery sealed to a registered mechanism for
/// which she has no key. A later delivery sealed to her recovery key opens.
/// Without a matching key for any candidate, the delivery is unusable in
/// recovery and the account key is unavailable in device acquisition.
///
/// ```text
/// Alice ── first recovery delivery ──✗ no matching key
///       ── second recovery delivery ──► account key
/// ```
#[test]
fn recovery_acquisition_skips_a_delivery_without_a_matching_key() {
    let (registry, _) = custom_registry(OTHER_SEAL);
    let env = Env::with(registry, Default::default(), ALICE);
    let mut rng = TestRandom(10);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    let (_, key) = ledger.root(&env, &laptop, 0, &mut rng);
    let recovery =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([7; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &recovery, &mut rng);
    let valid = &ledger.private.recovery_deliveries[0];
    let mut other = valid.row.clone();
    other.id = ids::random_v4(&mut rng).unwrap();
    let context = recovery_delivery_context(&env.application(), ALICE, &other).unwrap();
    let sealer = env.registry.key_sealer(OTHER_SEAL).unwrap();
    let recipient = sealer.generate_key_pair(&mut rng).unwrap();
    other.envelope = sealer
        .seal(&recipient.public, &context, &key, &mut rng)
        .unwrap();
    assert_eq!(
        env.registry.open_sealed(
            std::slice::from_ref(&recovery.sealing),
            &context,
            &other.envelope,
        ),
        Err(jazz_e2ee::Error::NoKeyForMechanism)
    );
    let (state, evidence) = ledger.evidence(&env);
    let replay = ledger.replay(&env);
    let keys = AccountKeyEvidence::new(&replay, &state, &evidence).unwrap();
    let deliveries = [Accepted::new(valid.position, other), valid.clone()];
    assert_eq!(
        *keys
            .acquire_with_recovery(&recovery, &deliveries)
            .unwrap()
            .key(),
        key
    );
    assert_eq!(
        keys.acquire_with_recovery(&recovery, &deliveries[..1])
            .unwrap_err(),
        ProtocolError::Recovery(RecoveryFailure::DeliveryUnusable)
    );
    assert_eq!(
        keys.acquire(&laptop.id, &[]).unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );
}

/// Recovery fails with its own codes: keys of another secret do not match
/// any root, and a root without a delivery of the current epoch has none.
#[test]
fn recovery_failures_keep_their_codes() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(10);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let recovery =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([7; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &recovery, &mut rng);
    let (state, evidence) = ledger.evidence(&env);
    let replay = ledger.replay(&env);
    let keys = AccountKeyEvidence::new(&replay, &state, &evidence).unwrap();
    let stranger =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([8; 32]), &env.application(), 0).unwrap();
    assert_eq!(
        keys.acquire_with_recovery(&stranger, &ledger.private.recovery_deliveries)
            .unwrap_err(),
        ProtocolError::Recovery(RecoveryFailure::RootMismatch)
    );
    assert_eq!(
        keys.acquire_with_recovery(&recovery, &[]).unwrap_err(),
        ProtocolError::Recovery(RecoveryFailure::DeliveryMissing)
    );
    let mut broken = ledger.private.recovery_deliveries.clone();
    let last = broken[0].row.envelope.len() - 1;
    broken[0].row.envelope[last] ^= 1;
    assert_eq!(
        keys.acquire_with_recovery(&recovery, &broken).unwrap_err(),
        ProtocolError::Recovery(RecoveryFailure::DeliveryUnusable)
    );
}

/// Retiring a recovery root rotates the account; the retired root receives
/// no delivery of the new key, and a recovery approval with it afterwards is
/// ignored.
#[test]
fn retired_recovery_root_no_longer_approves() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(11);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let recovery =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([3; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &recovery, &mut rng);
    let replacement = env.device(&mut rng);
    ledger.request(&replacement, &mut rng);
    let before = ledger.state(&env);
    let key = ledger.acquire(&env, &laptop).unwrap();
    // Prepared while the root was still active, accepted after retirement.
    let stale = build::recovery_approve(
        &env.writer(),
        &before,
        &mut replacement.clone(),
        &recovery,
        &key,
        &mut rng,
    )
    .unwrap()
    .unwrap();
    let (_, rotation) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RetireRecoveryRoot(recovery.root_id.clone()),
        &mut rng,
    );
    assert!(rotation.recovery_deliveries.is_empty());
    let state = ledger.state(&env);
    assert!(state.recovery_roots.is_empty());
    assert!(state.retired_recovery_roots.contains_key(&recovery.root_id));
    let new_key = ledger.acquire(&env, &laptop).unwrap();
    assert_eq!(*new_key.key(), *rotation.key);
    assert_eq!(
        build::recovery_approve(
            &env.writer(),
            &state,
            &mut replacement.clone(),
            &recovery,
            &new_key,
            &mut rng
        )
        .unwrap_err(),
        ProtocolError::Recovery(RecoveryFailure::RootMismatch)
    );

    // The same approval re-signed for the new epoch is still ignored.
    let mut current = stale.records.public.clone();
    current.id = ids::random_v4(&mut rng).unwrap();
    current.epoch_id = state.epoch_id().to_owned();
    let bytes = current
        .signed_bytes(&env.application(), &replacement.keys())
        .unwrap();
    let signer = env.registry.signer(signature::MECHANISM).unwrap();
    current.signature = signer.sign(&replacement.signing, &bytes).unwrap();
    current.recovery_signature = Some(signer.sign(&recovery.signing, &bytes).unwrap());
    let at = ledger.next();
    ledger
        .public
        .approvals
        .push(Accepted::new(at, stale.records.public));
    ledger.public.approvals.push(Accepted::new(at, current));
    assert!(!ledger.state(&env).is_active(&replacement.id));
}

/// Alice's replacement device must have its own matching request and a
/// representable next generation. Check overflow at the protocol boundary
/// because reaching the maximum through successive resets is impractical.
#[test]
fn reset_requires_matching_device_evidence_and_a_next_generation() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(12);
    let laptop = env.device(&mut rng);
    let phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let mut state = ledger.state(&env);
    let check = |state: &_, ledger: &Ledger| {
        build::check_reset(
            &env.writer(),
            ResetPolicy {
                device_account: false,
                allow_account_reset: true,
            },
            state,
            &phone,
            &ledger.public,
            &ledger.private,
        )
    };
    assert_eq!(
        check(&state, &ledger),
        Err(refused(RefusalCode::NoDeviceRequest))
    );
    ledger.request(&phone, &mut rng);
    assert_eq!(check(&state, &ledger).unwrap().generation(), 1);
    let mut mismatched = ledger.clone();
    mismatched.private.requests.last_mut().unwrap().row.keys = laptop.request().keys;
    assert_eq!(
        check(&state, &mismatched),
        Err(refused(RefusalCode::DeviceKeysMismatch))
    );
    state.generation = u64::MAX;
    assert_eq!(
        check(&state, &ledger),
        Err(ProtocolError::InvalidRecord("generation overflow"))
    );
}

/// An account reset starts a new generation: the new root's device is the
/// only active one, earlier devices and recovery roots are inactive, and the
/// state as of a position before the reset is still generation 0.
///
/// ```text
/// gen 0: laptop ──(all devices lost)──► gen 1: new-phone
/// as_of(before reset) → gen 0, laptop active
/// ```
#[test]
fn reset_starts_a_new_generation() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(12);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let recovery =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([4; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &recovery, &mut rng);
    let gen0 = ledger.state(&env);

    let phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    let policy = ResetPolicy {
        device_account: false,
        allow_account_reset: true,
    };
    // A still-active device needs no reset.
    assert_eq!(
        build::check_reset(
            &env.writer(),
            policy,
            &gen0,
            &laptop,
            &ledger.public,
            &ledger.private
        ),
        Err(refused(RefusalCode::ResetDeviceActive))
    );
    build::check_reset(
        &env.writer(),
        policy,
        &gen0,
        &phone,
        &ledger.public,
        &ledger.private,
    )
    .unwrap();
    let (reset_at, reset_key) = ledger.root(&env, &phone, 1, &mut rng);
    // A reset identity's ID is random, not derived: nobody can predict and
    // occupy it in advance.
    let reset_identity = &ledger.private.identities[1].row.id;
    assert!(ids::is_id(reset_identity, IdForm::Random));
    assert_ne!(reset_identity, ALICE);

    let state = ledger.state(&env);
    assert_eq!(state.generation, 1);
    assert!(state.is_active(&phone.id));
    assert!(!state.is_active(&laptop.id));
    assert!(state.recovery_roots.is_empty());
    assert_eq!(*ledger.acquire(&env, &phone).unwrap().key(), reset_key);

    let replay = ledger.replay(&env);
    let before = replay.as_of(reset_at).unwrap().unwrap();
    assert_eq!(before.generation, 0);
    assert!(before.is_active(&laptop.id));
    assert_eq!(before.epoch_id(), gen0.epoch_id());
    let after = replay.as_of(Position(reset_at.0 + 1)).unwrap().unwrap();
    assert_eq!(after.generation, 1);

    for refused_policy in [
        ResetPolicy {
            device_account: true,
            allow_account_reset: true,
        },
        ResetPolicy {
            device_account: false,
            allow_account_reset: false,
        },
    ] {
        assert_eq!(
            build::check_reset(
                &env.writer(),
                refused_policy,
                &gen0,
                &phone,
                &ledger.public,
                &ledger.private
            ),
            Err(refused(RefusalCode::AccountResetNotAllowed))
        );
    }
}

/// A root of generation 1 counts only after a root of generation 0;
/// otherwise it is ignored.
#[test]
fn a_generation_counts_only_after_the_previous_one() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(13);
    let laptop = env.device(&mut rng);
    let phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.request(&phone, &mut rng);
    // A raw generation-1 root with no generation-0 root before it.
    let early = AccountRoot {
        id: ids::random_v4(&mut rng).unwrap(),
        ledger_version: 1,
        generation: 1,
        account_id: ALICE.into(),
        device_id: phone.id.clone(),
        epoch_id: ids::random_v4(&mut rng).unwrap(),
        kind: AccountKind::App,
        device_account_public_key: None,
        device_account_signature: None,
    };
    let at = ledger.next();
    ledger.public.roots.push(Accepted::new(at, early));
    assert!(ledger.replay(&env).current().unwrap().is_none());
    // Bootstrap refuses an account with any root, so laptop's generation-0
    // root is built on its own and then accepted here.
    let mut scratch = Ledger::new(ALICE);
    scratch.request(&laptop, &mut rng);
    scratch.root(&env, &laptop, 0, &mut rng);
    let at = ledger.next();
    ledger
        .public
        .roots
        .push(Accepted::new(at, scratch.public.roots[0].row.clone()));
    let state = ledger.state(&env);
    assert_eq!(state.generation, 0);
    assert!(state.is_active(&laptop.id));
}

fn bootstrapped(seed: u64) -> (Env, Ledger, TestRandom) {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(seed);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    (env, ledger, rng)
}

/// Contradictory accepted rows fail current replay with their codes:
/// conflicting roots, conflicting device keys, a root device without keys.
#[test]
fn intrinsic_contradictions_fail_replay() {
    let (env, ledger, mut rng) = bootstrapped(14);
    let replay_error = |ledger: &Ledger| {
        jazz_e2ee::protocol::account::AccountReplay::new(&ledger.public, &env.domain, &env.registry)
            .unwrap()
            .current()
            .unwrap_err()
    };

    let mut conflicting = Ledger::new(ALICE);
    conflicting.public = ledger.public.clone();
    let mut other: AccountRoot = conflicting.public.roots[0].row.clone();
    other.id = ids::random_v4(&mut rng).unwrap();
    other.device_id = ids::random_v4(&mut rng).unwrap();
    let at = conflicting.public.roots[0].position;
    conflicting.public.roots.push(Accepted::new(at, other));
    assert_eq!(
        replay_error(&conflicting),
        ProtocolError::Intrinsic(IntrinsicContradiction::ConflictingRoots)
    );

    let mut keys = Ledger::new(ALICE);
    keys.public = ledger.public.clone();
    let mut second = keys.public.keys[0].clone();
    second.row.id = ids::random_v4(&mut rng).unwrap();
    second.row.keys.public_key[0] ^= 1;
    keys.public.keys.push(second);
    assert_eq!(
        replay_error(&keys),
        ProtocolError::Intrinsic(IntrinsicContradiction::ConflictingDeviceKeys)
    );

    let mut missing = Ledger::new(ALICE);
    missing.public = ledger.public.clone();
    missing.public.keys.clear();
    assert_eq!(
        replay_error(&missing),
        ProtocolError::Intrinsic(IntrinsicContradiction::MissingRootKey)
    );
}

/// An approval signs its device's keys (§2.5), so one whose device has no
/// published keys cannot be verified: it is skipped, and replay goes on.
#[test]
fn approval_without_recipient_keys_is_skipped() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(15);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger
        .public
        .keys
        .retain(|row| row.row.device_id != phone.id);
    let state = ledger.replay(&env).current().unwrap().unwrap();
    assert!(!state.is_active(&phone.id));
    assert!(state.is_active(&laptop.id));
}

/// A server that shows a client another keys row for an approved device
/// (its own keys under the device's ID) gains nothing: the approval signed
/// the device's real keys, so it no longer verifies and the device is not
/// active in that client's replay. Nothing is sealed to the substituted
/// keys, and nothing signed with them is accepted.
#[test]
fn substituted_device_keys_do_not_activate_the_approved_device() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(16);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mallory = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    assert!(ledger.state(&env).is_active(&phone.id));

    let mut substituted = ledger.public.clone();
    for row in &mut substituted.keys {
        if row.row.device_id == phone.id {
            row.row.keys = mallory.keys();
        }
    }
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&substituted, &env.domain, &env.registry)
            .unwrap();
    let state = replay.current().unwrap().unwrap();
    assert!(!state.is_active(&phone.id));
    assert!(state.is_active(&laptop.id));
    assert_eq!(state.active.len(), 1);
}

/// An `app` root is unsigned, so before first contact a client cannot tell
/// its device's keys from substituted ones (§8). After it, the floor holds
/// the root device's keys, and a later read showing other keys for that
/// device is refused as `root-not-mine`.
#[test]
fn a_floor_refuses_substituted_root_device_keys() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(17);
    let laptop = env.device(&mut rng);
    let mallory = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let through = ledger.public.through;
    let state = ledger.state(&env);
    let floor = state.floor(through, None);
    assert_eq!(floor.root_keys, laptop.keys());
    state.check_floor(through, &floor).unwrap();

    let mut substituted = ledger.public.clone();
    for row in &mut substituted.keys {
        if row.row.device_id == laptop.id {
            row.row.keys = mallory.keys();
        }
    }
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&substituted, &env.domain, &env.registry)
            .unwrap();
    let state = replay.current().unwrap().unwrap();
    assert_eq!(
        state.check_floor(through, &floor).unwrap_err(),
        ProtocolError::RootNotMine
    );
}

/// Unsupported capabilities stop replay and are never read as invalid: a
/// root of another ledger version, and a signer of an unregistered signing
/// mechanism or version.
#[test]
fn unsupported_capabilities_are_not_invalid() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(16);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);

    let mut version = ledger.public.clone();
    version.roots[0].row.ledger_version = 2;
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&version, &env.domain, &env.registry)
            .unwrap();
    assert_eq!(
        replay.current().unwrap_err(),
        ProtocolError::Unsupported(UnsupportedCapability::LedgerVersion)
    );

    for (mechanism, version, expected) in [
        (
            "com.example.sign",
            1,
            UnsupportedCapability::SigningMechanism,
        ),
        ("jazz.sodium.sign", 2, UnsupportedCapability::SigningVersion),
    ] {
        let mut public = ledger.public.clone();
        for row in &mut public.keys {
            if row.row.device_id == laptop.id {
                row.row.keys.signing_mechanism = mechanism.to_owned();
                row.row.keys.signing_version = version;
            }
        }
        let replay =
            jazz_e2ee::protocol::account::AccountReplay::new(&public, &env.domain, &env.registry)
                .unwrap();
        assert_eq!(
            replay.current().unwrap_err(),
            ProtocolError::Unsupported(expected)
        );
    }
}

/// Private verification leaves out what does not verify: an identity at
/// another position, a private approval with a bad signature. Key
/// acquisition then reports the key unavailable, not an error.
#[test]
fn private_verification_drops_unverified_records() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(17);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);

    let mut tampered = ledger.private.clone();
    tampered.approvals[0].row.signature[10] ^= 1;
    tampered.identities[0].position = Position(tampered.identities[0].position.0 + 1);
    let replay = ledger.replay(&env);
    let state = replay.current().unwrap().unwrap();
    let evidence = verify_private(&replay, &state, &tampered).unwrap();
    assert!(evidence.identity().is_none());
    assert!(evidence.approvals().is_empty());
    let keys = AccountKeyEvidence::new(&replay, &state, &evidence).unwrap();
    assert_eq!(
        keys.acquire(&phone.id, std::slice::from_ref(&phone.sealing))
            .unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );

    // Evidence from another cut is refused outright.
    let mut other_cut = PrivateAccountLedger::new(Position(ledger.public.through.0 + 1));
    other_cut.requests = ledger.private.requests.clone();
    assert_eq!(
        verify_private(&replay, &state, &other_cut).unwrap_err(),
        ProtocolError::InconsistentEvidence
    );
}

/// An invalid delivery marker makes that candidate unusable; the key is
/// unavailable through it.
#[test]
fn invalid_delivery_marker_makes_the_key_unavailable() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(18);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let last = ledger.private.deliveries[0].row.verification.len() - 1;
    ledger.private.deliveries[0].row.verification[last] ^= 1;
    assert_eq!(
        ledger.acquire(&env, &phone).unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );
    // The approval itself still counts publicly.
    assert!(ledger.state(&env).is_active(&phone.id));
}

/// The challenge and response refuse with their codes: an inactive
/// approver, a challenge for another device, and a tampered proof.
#[test]
fn approval_handshake_refusals() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(19);
    let laptop = env.device(&mut rng);
    let phone = env.device(&mut rng);
    let tablet = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.request(&tablet, &mut rng);
    let writer = env.writer();
    let state = ledger.state(&env);
    assert_eq!(
        build::challenge(&writer, &state, &tablet, &phone.request(), &mut rng).unwrap_err(),
        refused(RefusalCode::DeviceNotActive)
    );
    let challenge = build::challenge(&writer, &state, &laptop, &phone.request(), &mut rng).unwrap();
    assert_eq!(
        build::respond(
            &writer,
            &state,
            &mut tablet.clone(),
            &challenge.challenge,
            &mut rng
        )
        .unwrap_err(),
        refused(RefusalCode::StaleChallenge)
    );
    let mut proof = build::respond(
        &writer,
        &state,
        &mut phone.clone(),
        &challenge.challenge,
        &mut rng,
    )
    .unwrap();
    let last = proof.proof.len() - 1;
    proof.proof[last] ^= 1;
    assert_eq!(
        build::approve(
            &writer,
            &state,
            &laptop,
            &challenge,
            &proof,
            &phone.request(),
            &ledger.acquire(&env, &laptop).unwrap(),
            &mut rng
        )
        .unwrap_err(),
        refused(RefusalCode::InvalidPossessionProof)
    );
}

fn debug_free_of(rendered: &str, secret: &[u8]) {
    let compact = |text: &str| {
        text.chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>()
            .replace(",]", "]")
    };
    let compact_rendered = compact(rendered);
    for needle in [compact(&format!("{secret:?}")), common::to_hex(secret)] {
        assert!(
            !compact_rendered.contains(&needle),
            "secret bytes in {rendered}"
        );
    }
}

/// No secret-bearing result prints its secret, in ordinary or alternate
/// Debug: root records, challenges, rotations, acquired account keys, local
/// devices and recovery keys and secrets.
#[test]
fn debug_output_never_contains_secrets() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(40);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.request(&phone, &mut rng);
    let writer = env.writer();
    let permit = build::check_bootstrap(&writer, &laptop, &ledger.public, &ledger.private).unwrap();
    let root = build::account_root(
        &writer,
        &laptop,
        &permit,
        build::RootAuthority::App,
        &mut rng,
    )
    .unwrap();
    for rendered in [format!("{root:?}"), format!("{root:#?}")] {
        debug_free_of(&rendered, &root.key[..]);
    }
    let at = ledger.next();
    ledger
        .private
        .identities
        .push(Accepted::new(at, root.identity.clone()));
    ledger
        .public
        .roots
        .push(Accepted::new(at, root.root.clone()));
    let state = ledger.state(&env);

    let challenge = build::challenge(&writer, &state, &laptop, &phone.request(), &mut rng).unwrap();
    for rendered in [format!("{challenge:?}"), format!("{challenge:#?}")] {
        debug_free_of(&rendered, &challenge.secret[..]);
    }
    let key = ledger.acquire(&env, &laptop).unwrap();
    for rendered in [format!("{key:?}"), format!("{key:#?}")] {
        debug_free_of(&rendered, key.key());
    }
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let (_, rotation) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    for rendered in [format!("{rotation:?}"), format!("{rotation:#?}")] {
        debug_free_of(&rendered, &rotation.key[..]);
    }

    let secret = RecoverySecret::from_bytes([0x5a; 32]);
    let mut recovery = RecoveryKeys::derive(&secret, &env.application(), 0).unwrap();
    recovery.sealing.secret.0.fill(0x6b);
    recovery.signing.secret.0.fill(0x7c);
    let mut laptop = laptop;
    laptop.sealing.secret.0.fill(0x8d);
    laptop.signing.secret.0.fill(0x9e);
    let nonce = laptop.approval_nonce.as_mut().unwrap();
    nonce.0.fill(0xaf);

    for rendered in [format!("{secret:?}"), format!("{secret:#?}")] {
        debug_free_of(&rendered, secret.as_bytes());
    }
    for rendered in [format!("{recovery:?}"), format!("{recovery:#?}")] {
        debug_free_of(&rendered, recovery.sealing.secret.as_bytes());
        debug_free_of(&rendered, recovery.signing.secret.as_bytes());
    }
    for rendered in [format!("{laptop:?}"), format!("{laptop:#?}")] {
        debug_free_of(&rendered, laptop.sealing.secret.as_bytes());
        debug_free_of(&rendered, laptop.signing.secret.as_bytes());
        debug_free_of(&rendered, &laptop.approval_nonce.as_ref().unwrap().0[..]);
    }
}

/// A state as of a position needs every row before it: a position beyond
/// the read's cut is inconsistent evidence, never answered from the cut.
#[test]
fn state_beyond_the_cut_is_refused() {
    let (env, ledger, _) = bootstrapped(41);
    let replay = ledger.replay(&env);
    let through = ledger.public.through;
    assert!(replay.as_of(Position(through.0 + 1)).unwrap().is_some());
    assert_eq!(
        replay.as_of(Position(through.0 + 2)).unwrap_err(),
        ProtocolError::InconsistentEvidence
    );
}

/// After a reset, the same recovery secret derives a fresh root for the new
/// generation, which registers and restores; the generation-0 root stays
/// invalid.
#[test]
fn recovery_secret_survives_a_reset() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(42);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let secret = RecoverySecret::from_bytes([6; 32]);
    let gen0 = RecoveryKeys::derive(&secret, &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &gen0, &mut rng);

    let phone = env.device(&mut rng);
    ledger.request(&phone, &mut rng);
    ledger.root(&env, &phone, 1, &mut rng);
    let state = ledger.state(&env);
    assert!(state.recovery_root(&gen0.root_id).is_none());

    let gen1 = RecoveryKeys::derive(&secret, &env.application(), state.generation).unwrap();
    assert_ne!(gen1.root_id, gen0.root_id);
    ledger.register_recovery(&env, &phone, &gen1, &mut rng);
    let (state, evidence) = ledger.evidence(&env);
    assert!(state.recovery_root(&gen1.root_id).is_some());
    let replay = ledger.replay(&env);
    let restored = AccountKeyEvidence::new(&replay, &state, &evidence)
        .unwrap()
        .acquire_with_recovery(&gen1, &ledger.private.recovery_deliveries)
        .unwrap();
    assert_eq!(restored.key(), ledger.acquire(&env, &phone).unwrap().key());
}

/// A recovery root whose sealing key is of a mechanism the configuration
/// does not select receives no delivery from a rotation, and does not block
/// it.
#[test]
fn rotation_skips_recovery_roots_of_other_mechanisms() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(43);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let mut foreign =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([1; 32]), &env.application(), 0).unwrap();
    foreign.sealing.public.mechanism = MechanismId::new("com.example.kem", 1);
    let state = ledger.state(&env);
    let root = build::recovery_root(&env.writer(), &state, &laptop, &foreign).unwrap();
    let at = ledger.next();
    ledger.public.recovery_roots.push(Accepted::new(at, root));
    let native =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([2; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &native, &mut rng);
    assert_eq!(ledger.state(&env).recovery_roots.len(), 2);

    let (_, rotation) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    assert_eq!(rotation.recovery_deliveries.len(), 1);
    assert_eq!(rotation.recovery_deliveries[0].root_id, native.root_id);
}

/// Private evidence is bound to the exact current state it was verified
/// against: a state as of a position cannot be verified or mixed in, and a
/// private approval pairs with the public approval of its own ID only.
#[test]
fn private_evidence_is_bound_to_its_state() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(31);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let (at, records) = ledger.approve(&env, &laptop, &mut phone, &mut rng);
    assert_eq!(records.public.id, records.approval.id);

    let replay = ledger.replay(&env);
    let state = replay.current().unwrap().unwrap();
    let evidence = verify_private(&replay, &state, &ledger.private).unwrap();
    assert_eq!(evidence.approvals().len(), 1);
    let earlier = replay.as_of(at).unwrap().unwrap();
    assert_eq!(
        verify_private(&replay, &earlier, &ledger.private).unwrap_err(),
        ProtocolError::InconsistentEvidence
    );
    assert_eq!(
        AccountKeyEvidence::new(&replay, &earlier, &evidence)
            .err()
            .unwrap(),
        ProtocolError::InconsistentEvidence
    );

    // The same approval published under another ID no longer pairs.
    let mut public = ledger.public.clone();
    let row = &mut public.approvals[0].row;
    row.id = ids::random_v4(&mut rng).unwrap();
    let bytes = row.signed_bytes(&env.application(), &phone.keys()).unwrap();
    row.signature = signature::Builtin.sign(&laptop.signing, &bytes).unwrap();
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&public, &env.domain, &env.registry)
            .unwrap();
    let state = replay.current().unwrap().unwrap();
    assert!(state.is_active(&phone.id));
    let evidence = verify_private(&replay, &state, &ledger.private).unwrap();
    assert!(evidence.approvals().is_empty());
}

/// Accepts a recovery approval of `device` with `recovery` in the current
/// epoch, as replay would see it even if the device is already active.
fn recover_with(
    env: &Env,
    ledger: &mut Ledger,
    holder: &build::LocalDevice,
    device: &build::LocalDevice,
    recovery: &RecoveryKeys,
    rng: &mut TestRandom,
) {
    let key = ledger.acquire(env, holder).unwrap();
    let mut state = ledger.state(env);
    state.active.remove(&device.id);
    let records = build::recovery_approve(
        &env.writer(),
        &state,
        &mut device.clone(),
        recovery,
        &key,
        rng,
    )
    .unwrap()
    .expect("not active in the given state");
    let at = ledger.next();
    ledger
        .private
        .challenges
        .push(Accepted::new(at, records.challenge));
    ledger.private.proofs.push(Accepted::new(at, records.proof));
    ledger.accept_approval(at, &records.records);
}

/// Removing the device that registered a recovery root retires that root
/// with it, as it held the root's key pair; a removal that leaves it active
/// is not accepted. A device that both registered a root and recovered with
/// another retires both in one rotation.
#[test]
fn removing_a_registering_device_retires_its_roots() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(30);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let mut historical_phone = phone.clone();
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let first =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([1; 32]), &env.application(), 0).unwrap();
    let second =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([2; 32]), &env.application(), 0).unwrap();
    // The laptop registers the first root and the phone registers the second.
    // Stage the phone's additional recovery from its unanswered snapshot.
    ledger.register_recovery(&env, &laptop, &first, &mut rng);
    ledger.register_recovery(&env, &phone, &second, &mut rng);
    recover_with(
        &env,
        &mut ledger,
        &laptop,
        &historical_phone,
        &first,
        &mut rng,
    );

    let before = ledger.public.clone();
    let (at, removal) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let mut both = vec![first.root_id.clone(), second.root_id.clone()];
    both.sort();
    assert_eq!(removal.public.columns.targets().unwrap().1, both);
    assert!(removal.recovery_deliveries.is_empty());
    let state = ledger.state(&env);
    assert!(state.is_removed(&phone.id));
    assert!(state.recovery_roots.is_empty());

    // Naming only one of them is not accepted.
    let mut partial = removal.public.clone();
    partial.columns.retired_recovery_root_ids =
        jazz_e2ee::protocol::json::encode_root_id_list([&second.root_id]).unwrap();
    let bytes = partial.signed_bytes(&env.application()).unwrap();
    partial.signature = signature::Builtin.sign(&laptop.signing, &bytes).unwrap();
    let mut view = before;
    view.through = at;
    view.successors.push(Accepted::new(at, partial));
    let state = jazz_e2ee::protocol::account::AccountReplay::new(&view, &env.domain, &env.registry)
        .unwrap()
        .current()
        .unwrap()
        .unwrap();
    assert!(!state.is_removed(&phone.id));

    // Removing the laptop, which registered only the first root.
    let mut ledger2 = Ledger::new(ALICE);
    ledger2.request(&laptop, &mut rng);
    ledger2.root(&env, &laptop, 0, &mut rng);
    ledger2.request(&historical_phone, &mut rng);
    ledger2.approve(&env, &laptop, &mut historical_phone, &mut rng);
    ledger2.register_recovery(&env, &laptop, &first, &mut rng);
    let (_, removal) = ledger2.rotate(
        &env,
        &phone,
        RotationTarget::RemoveDevice(laptop.id.clone()),
        &mut rng,
    );
    assert_eq!(
        removal.public.columns.targets().unwrap().1,
        vec![first.root_id.clone()]
    );
    assert!(ledger2.state(&env).recovery_roots.is_empty());
}

/// Every recovery approval of a device counts, not only its first: a device
/// whose first recovery root is already retired and that recovered again
/// with another root retires that other root when removed.
#[test]
fn removal_retires_every_root_a_device_recovered_with() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(31);
    let laptop = env.device(&mut rng);
    let phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let first =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([1; 32]), &env.application(), 0).unwrap();
    let second =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([2; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &first, &mut rng);
    recover_with(&env, &mut ledger, &laptop, &phone, &first, &mut rng);
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RetireRecoveryRoot(first.root_id.clone()),
        &mut rng,
    );
    ledger.register_recovery(&env, &laptop, &second, &mut rng);
    recover_with(&env, &mut ledger, &laptop, &phone, &second, &mut rng);
    assert!(ledger.state(&env).is_active(&phone.id));

    let (_, removal) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    assert_eq!(
        removal.public.columns.targets().unwrap().1,
        vec![second.root_id.clone()]
    );
    let state = ledger.state(&env);
    assert!(state.is_removed(&phone.id));
    assert!(state.retired_recovery_roots.contains_key(&second.root_id));
    assert!(state.recovery_roots.is_empty());
}

/// Retiring a mechanism is breaking by design: once the signing mechanism
/// is removed from the allow-list, an account chain signed with it is
/// unverifiable (an unsupported capability), with no grace path.
#[test]
fn retired_signing_mechanism_leaves_the_chain_unverifiable() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(32);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    assert!(ledger.state(&env).is_active(&phone.id));

    let retired = env
        .registry
        .allowing(&jazz_e2ee::agility::MechanismAllowList {
            signature: Vec::new(),
            ..Default::default()
        })
        .unwrap();
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&ledger.public, &env.domain, &retired)
            .unwrap();
    assert_eq!(
        replay.current().unwrap_err(),
        ProtocolError::Unsupported(UnsupportedCapability::SigningMechanism)
    );
}

/// Asserts that the `index`th sealing key record was accepted at `at`, for
/// `epoch_id`, signed by `signer` over its signed bytes, with the public key
/// derived from the epoch key `key` (§2.12).
fn assert_sealing_key(
    env: &Env,
    ledger: &Ledger,
    index: usize,
    at: Position,
    signer: &LocalDevice,
    epoch_id: &str,
    key: &[u8; 32],
) {
    let record = &ledger.public.account_keys[index];
    assert_eq!(record.position, at);
    record.row.validate().unwrap();
    assert_eq!(record.row.account_id, ALICE);
    assert_eq!(record.row.epoch_id, epoch_id);
    assert_eq!(record.row.signer_id, signer.id);
    assert_eq!(
        record.row.public(),
        derive::account_sealing_key_pair(&key_seal::Builtin, key)
            .unwrap()
            .public
    );
    let bytes = record.row.signed_bytes(&env.application()).unwrap();
    assert!(
        signing::verify(
            &env.registry,
            &signer.signing.public,
            &bytes,
            &record.row.signature
        )
        .unwrap()
    );
}

/// Every account epoch's creation writes its sealing key record in the same
/// transaction (§2.12): a bootstrap and a reset signed by the root's
/// device, a rotation by its author.
/// A later publication for the current epoch is by an active device that
/// holds the current key; a removed device is refused.
#[test]
fn every_epoch_publishes_its_sealing_key() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(41);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    let (root_at, root_key) = ledger.root(&env, &laptop, 0, &mut rng);
    let root_epoch = ledger.state(&env).epoch_id().to_owned();
    assert_sealing_key(&env, &ledger, 0, root_at, &laptop, &root_epoch, &root_key);

    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let (rotation_at, rotation) = ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let state = ledger.state(&env);
    assert_eq!(state.epoch_id(), rotation.public.columns.epoch_id);
    assert!(state.is_removed(&phone.id));
    assert_sealing_key(
        &env,
        &ledger,
        1,
        rotation_at,
        &laptop,
        &rotation.public.columns.epoch_id,
        &rotation.key,
    );

    let tablet = env.device(&mut rng);
    ledger.request(&tablet, &mut rng);
    let (reset_at, reset_key) = ledger.root(&env, &tablet, 1, &mut rng);
    let reset_epoch = ledger.state(&env).epoch_id().to_owned();
    assert_sealing_key(
        &env,
        &ledger,
        2,
        reset_at,
        &tablet,
        &reset_epoch,
        &reset_key,
    );
}

/// A rotation from an account key authenticated before a later approval of
/// the same epoch is refused: its private revision would miss that approval,
/// and no device could verify the new epoch. Authenticated again, the key
/// rotates, and every remaining device acquires the new key.
#[test]
fn a_key_from_before_a_later_approval_cannot_rotate() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(18);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut tablet = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let stale = ledger.acquire(&env, &laptop).unwrap();

    ledger.request(&tablet, &mut rng);
    ledger.approve(&env, &laptop, &mut tablet, &mut rng);
    let state = ledger.state(&env);
    assert!(state.is_active(&tablet.id));
    let target = RotationTarget::RemoveDevice(phone.id.clone());
    assert_eq!(
        build::rotate(&env.writer(), &state, &laptop, &stale, &target, &mut rng).unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );

    ledger.rotate(&env, &laptop, target, &mut rng);
    let state = ledger.state(&env);
    assert!(!state.is_active(&phone.id));
    let laptop_key = ledger.acquire(&env, &laptop).unwrap();
    let tablet_key = ledger.acquire(&env, &tablet).unwrap();
    assert_eq!(laptop_key.epoch_id(), state.epoch_id());
    assert_eq!(laptop_key.key(), tablet_key.key());
}

/// laptop (root), phone and tablet approved; laptop then removes phone and
/// approves a watch in the new epoch. The
/// returned successor is one laptop built, from that state, to remove tablet
/// (not yet accepted): every check passes for it as built.
fn removed_phone_and_a_pending_successor(
    seed: u64,
) -> (
    Env,
    Ledger,
    LocalDevice,
    LocalDevice,
    LocalDevice,
    jazz_e2ee::protocol::records::account::PublicAccountSuccessor,
) {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(seed);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut tablet = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.approve(&env, &laptop, &mut tablet, &mut rng);
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    // An approval in the new epoch, so the successor's revision is not empty.
    let mut watch = env.device(&mut rng);
    ledger.request(&watch, &mut rng);
    ledger.approve(&env, &laptop, &mut watch, &mut rng);
    let key = ledger.acquire(&env, &laptop).unwrap();
    let state = ledger.state(&env);
    let records = build::rotate(
        &env.writer(),
        &state,
        &laptop,
        &key,
        &RotationTarget::RemoveDevice(tablet.id.clone()),
        &mut rng,
    )
    .unwrap();
    (env, ledger, laptop, phone, tablet, records.public)
}

/// Re-signs `successor` with `device`'s signing key, as `device`.
fn re_sign(
    env: &Env,
    successor: &mut jazz_e2ee::protocol::records::account::PublicAccountSuccessor,
    device: &LocalDevice,
) {
    successor.columns.signer_id = device.id.clone();
    let bytes = successor.signed_bytes(&env.application()).unwrap();
    successor.signature = signature::Builtin.sign(&device.signing, &bytes).unwrap();
}

/// Accepts `successor` alone at the next position and replays.
fn with_successor(
    env: &Env,
    ledger: &Ledger,
    successor: jazz_e2ee::protocol::records::account::PublicAccountSuccessor,
) -> jazz_e2ee::protocol::account::AccountState {
    let mut ledger = ledger.clone();
    let at = ledger.next();
    ledger.public.successors.push(Accepted::new(at, successor));
    ledger.state(env)
}

/// A successor correctly signed by a removed device, or by a device that was
/// never approved, is not accepted: its signer is not active before it. The
/// same successor signed by an active device is accepted, so only the signer
/// differs.
#[test]
fn a_correctly_signed_successor_by_an_inactive_device_is_rejected() {
    let (env, ledger, laptop, phone, tablet, successor) = removed_phone_and_a_pending_successor(19);
    let epoch = ledger.state(&env).epoch_id().to_owned();

    let accepted = with_successor(&env, &ledger, successor.clone());
    assert!(!accepted.is_active(&tablet.id));
    assert_ne!(accepted.epoch_id(), epoch);

    let mut by_removed = successor.clone();
    re_sign(&env, &mut by_removed, &phone);
    let state = with_successor(&env, &ledger, by_removed);
    assert!(state.is_active(&tablet.id));
    assert_eq!(state.epoch_id(), epoch);

    let mut rng = TestRandom(20);
    let stranger = env.device(&mut rng);
    let mut ledger_with_stranger = ledger.clone();
    ledger_with_stranger.request(&stranger, &mut rng);
    let mut by_stranger = successor.clone();
    re_sign(&env, &mut by_stranger, &stranger);
    let state = with_successor(&env, &ledger_with_stranger, by_stranger);
    assert!(state.is_active(&tablet.id));
    assert_eq!(state.epoch_id(), epoch);

    let mut by_laptop = successor;
    re_sign(&env, &mut by_laptop, &laptop);
    assert!(!with_successor(&env, &ledger, by_laptop).is_active(&tablet.id));
}

/// A successor whose `membership` is not exactly the active set minus its
/// target, or whose public `revision` is not the recomputed one, is not
/// accepted, even correctly signed by an active device.
#[test]
fn a_successor_with_other_membership_or_revision_is_rejected() {
    let (env, ledger, laptop, phone, tablet, successor) = removed_phone_and_a_pending_successor(21);
    let epoch = ledger.state(&env).epoch_id().to_owned();

    let mut wider = successor.clone();
    let mut members = jazz_e2ee::protocol::json::decode_id_list(&wider.columns.membership).unwrap();
    members.push(phone.id.clone());
    members.sort();
    wider.columns.membership = jazz_e2ee::protocol::json::encode_id_list(&members).unwrap();
    re_sign(&env, &mut wider, &laptop);
    let state = with_successor(&env, &ledger, wider);
    assert!(state.is_active(&tablet.id));
    assert_eq!(state.epoch_id(), epoch);

    let mut narrower = successor.clone();
    narrower.columns.membership =
        jazz_e2ee::protocol::json::encode_id_list(Vec::<String>::new()).unwrap();
    re_sign(&env, &mut narrower, &laptop);
    assert_eq!(with_successor(&env, &ledger, narrower).epoch_id(), epoch);

    let mut revision = successor;
    assert_ne!(
        revision.revision,
        jazz_e2ee::protocol::json::encode_revision(Vec::<&String>::new()).unwrap()
    );
    revision.revision = jazz_e2ee::protocol::json::encode_revision(Vec::<&String>::new()).unwrap();
    re_sign(&env, &mut revision, &laptop);
    assert_eq!(with_successor(&env, &ledger, revision).epoch_id(), epoch);
}

/// A successor accepted at the current epoch's own position is not strictly
/// after it, and is not accepted; the same row one position later is. The
/// epoch has no approvals, so the successor's revision is right at either
/// position and only the position differs.
#[test]
fn a_successor_at_the_epochs_own_position_is_rejected() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(22);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut tablet = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.approve(&env, &laptop, &mut tablet, &mut rng);
    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let key = ledger.acquire(&env, &laptop).unwrap();
    let state = ledger.state(&env);
    let successor = build::rotate(
        &env.writer(),
        &state,
        &laptop,
        &key,
        &RotationTarget::RemoveDevice(tablet.id.clone()),
        &mut rng,
    )
    .unwrap()
    .public;
    let mut same = ledger.clone();
    same.public.successors.push(Accepted::new(
        state.current_epoch().position,
        successor.clone(),
    ));
    let replayed = same.state(&env);
    assert!(replayed.is_active(&tablet.id));
    assert_eq!(replayed.epoch_id(), state.epoch_id());
    assert!(!with_successor(&env, &ledger, successor).is_active(&tablet.id));
}

/// An identity marker must unwrap to 32 zero bytes: one that authenticates
/// under the root key but wraps other bytes makes the key unavailable.
#[test]
fn an_identity_marker_of_other_bytes_makes_the_key_unavailable() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(23);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let key = ledger.acquire(&env, &laptop).unwrap();
    let identity = ledger.private.identities[0].row.clone();
    let context = jazz_e2ee::protocol::records::account::identity_context(
        &env.application(),
        ALICE,
        &identity.id,
        &identity.epoch_id,
        "verification",
        "",
    )
    .unwrap();
    use jazz_e2ee::agility::KeyWrapper;
    ledger.private.identities[0].row.verification = jazz_e2ee::mechanisms::key_envelope::Builtin
        .wrap(key.key(), &context, &[1; 32], &mut rng)
        .unwrap();
    assert_eq!(
        ledger.acquire(&env, &laptop).unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );
}

/// A device never removes itself (§2.8): it generated the new epoch's key and
/// would keep it. The builder refuses (`self-removal`), and a self-removal
/// successor correctly signed by the removed device is not accepted.
/// Another device removes it, and the removed device holds no key of the
/// new epoch.
#[test]
fn a_device_cannot_remove_itself() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(25);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let state = ledger.state(&env);
    let phone_key = ledger.acquire(&env, &phone).unwrap();
    let laptop_key = ledger.acquire(&env, &laptop).unwrap();
    let itself = RotationTarget::RemoveDevice(phone.id.clone());
    assert_eq!(
        build::rotate(&env.writer(), &state, &phone, &phone_key, &itself, &mut rng).unwrap_err(),
        refused(RefusalCode::SelfRemoval)
    );

    // The same removal built by laptop, then signed by phone as its author.
    let mut by_itself = build::rotate(
        &env.writer(),
        &state,
        &laptop,
        &laptop_key,
        &itself,
        &mut rng,
    )
    .unwrap()
    .public;
    re_sign(&env, &mut by_itself, &phone);
    let replayed = with_successor(&env, &ledger, by_itself);
    assert!(replayed.is_active(&phone.id));
    assert_eq!(replayed.epoch_id(), state.epoch_id());

    ledger.rotate(&env, &laptop, itself, &mut rng);
    assert!(ledger.state(&env).is_removed(&phone.id));
    assert_eq!(
        ledger.acquire(&env, &phone).unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );
}

/// Accepts `request` at the next position.
fn accept_request(
    ledger: &mut Ledger,
    request: jazz_e2ee::protocol::records::account::DeviceRemovalRequest,
) {
    let at = ledger.next();
    ledger
        .public
        .removal_requests
        .push(Accepted::new(at, request));
}

/// A device leaves by requesting its removal (§2.8): it stays an active
/// member until another device completes the request with an ordinary
/// removal, which it then sees as `device-no-longer-active`, and it never
/// holds the new epoch's key.
#[test]
fn a_removal_request_is_completed_by_another_device() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(26);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let before = ledger.state(&env);
    let floor = before.floor(ledger.public.through, Some(&phone.id));

    let request = build::removal_request(&env.writer(), &before, &phone, &mut rng).unwrap();
    accept_request(&mut ledger, request);
    let state = ledger.state(&env);
    assert_eq!(state.removal_requested, [phone.id.clone()].into());
    assert!(state.is_active(&phone.id), "a full member until removed");
    assert_eq!(state.epoch_id(), before.epoch_id());
    ledger.acquire(&env, &phone).unwrap();

    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let state = ledger.state(&env);
    assert!(state.is_removed(&phone.id));
    assert!(state.removal_requested.is_empty(), "complete once removed");
    assert_eq!(
        ledger.acquire(&env, &phone).unwrap_err(),
        ProtocolError::AccountKeyUnavailable
    );
    assert_eq!(
        state
            .check_floor(ledger.public.through, &floor)
            .unwrap_err(),
        ProtocolError::DeviceNoLongerActive
    );
}

/// An account's only device cannot request its removal: nobody could
/// complete it (`no-other-device`).
#[test]
fn the_only_device_cannot_request_its_removal() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(27);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let state = ledger.state(&env);
    assert_eq!(
        build::removal_request(&env.writer(), &state, &laptop, &mut rng).unwrap_err(),
        refused(RefusalCode::NoOtherDevice)
    );
}

/// Only a request the device itself signed while active is pending: one
/// with a bad signature, one signed by another device, or one by a device
/// that was not active with its epoch is not. Such rows never stop a valid
/// request from being pending.
#[test]
fn only_a_valid_removal_request_is_pending() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(28);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let state = ledger.state(&env);
    let valid = build::removal_request(&env.writer(), &state, &phone, &mut rng).unwrap();

    let mut tampered = valid.clone();
    tampered.id = ids::random_v4(&mut rng).unwrap();
    let last = tampered.signature.len() - 1;
    tampered.signature[last] ^= 1;
    let mut by_laptop = valid.clone();
    by_laptop.id = ids::random_v4(&mut rng).unwrap();
    let bytes = by_laptop.signed_bytes(&env.application()).unwrap();
    by_laptop.signature = signature::Builtin.sign(&laptop.signing, &bytes).unwrap();
    let mut other_epoch = valid.clone();
    other_epoch.id = ids::random_v4(&mut rng).unwrap();
    other_epoch.epoch_id = ids::random_v4(&mut rng).unwrap();
    let bytes = other_epoch.signed_bytes(&env.application()).unwrap();
    other_epoch.signature = signature::Builtin.sign(&phone.signing, &bytes).unwrap();
    for invalid in [tampered, by_laptop, other_epoch] {
        let mut with = ledger.clone();
        accept_request(&mut with, invalid.clone());
        assert!(with.state(&env).removal_requested.is_empty());
        accept_request(&mut with, valid.clone());
        assert_eq!(
            with.state(&env).removal_requested,
            [phone.id.clone()].into()
        );
    }
}

/// A removal request is scoped to its generation (§2.8): after a reset, the
/// same device approved again in the new generation is not removed by its
/// request from before the reset.
#[test]
fn a_removal_request_from_before_a_reset_is_not_pending() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(29);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let tablet = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    // Stage two generations of history for the same device ID and request.
    let mut reset_history_phone = phone.clone();
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let state = ledger.state(&env);
    let request = build::removal_request(&env.writer(), &state, &phone, &mut rng).unwrap();
    accept_request(&mut ledger, request);
    assert_eq!(
        ledger.state(&env).removal_requested,
        [phone.id.clone()].into()
    );

    ledger.request(&tablet, &mut rng);
    ledger.root(&env, &tablet, 1, &mut rng);
    ledger.approve(&env, &tablet, &mut reset_history_phone, &mut rng);
    let state = ledger.state(&env);
    assert_eq!(state.generation, 1);
    assert!(state.is_active(&phone.id));
    assert!(state.removal_requested.is_empty());
}

/// Both devices of an account request their removal: both are pending, the
/// first completion removes the other device, and the remaining device's
/// request stays pending with nobody to complete it. A device never removes
/// itself, so the account never reaches zero devices.
#[test]
fn when_both_devices_request_removal_one_remains() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(30);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let state = ledger.state(&env);
    let by_laptop = build::removal_request(&env.writer(), &state, &laptop, &mut rng).unwrap();
    let by_phone = build::removal_request(&env.writer(), &state, &phone, &mut rng).unwrap();
    accept_request(&mut ledger, by_laptop);
    accept_request(&mut ledger, by_phone);
    assert_eq!(
        ledger.state(&env).removal_requested,
        [laptop.id.clone(), phone.id.clone()].into()
    );

    ledger.rotate(
        &env,
        &laptop,
        RotationTarget::RemoveDevice(phone.id.clone()),
        &mut rng,
    );
    let state = ledger.state(&env);
    assert_eq!(state.active, [laptop.id.clone()].into());
    assert_eq!(state.removal_requested, [laptop.id.clone()].into());
    let key = ledger.acquire(&env, &laptop).unwrap();
    assert_eq!(
        build::rotate(
            &env.writer(),
            &state,
            &laptop,
            &key,
            &RotationTarget::RemoveDevice(laptop.id.clone()),
            &mut rng,
        )
        .unwrap_err(),
        refused(RefusalCode::SelfRemoval)
    );
}

/// A server that shows the approver other keys for a pending device (its
/// own, under the device's ID, with its own nonce commitment) gets a
/// challenge signed over those. The real device refuses it
/// (`invalid-challenge`) and shows no code (§2.5, §8).
#[test]
fn a_challenge_over_substituted_requester_keys_is_refused() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(31);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mallory = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let real = ledger.state(&env);

    // The approver's view: phone's keys row and request carry mallory's.
    let mut shown = ledger.public.clone();
    for row in &mut shown.keys {
        if row.row.device_id == phone.id {
            row.row.keys = mallory.keys();
        }
    }
    let replay =
        jazz_e2ee::protocol::account::AccountReplay::new(&shown, &env.domain, &env.registry)
            .unwrap();
    let shown_state = replay.current().unwrap().unwrap();
    let fake_request = jazz_e2ee::protocol::records::account::DeviceRequest {
        id: phone.id.clone(),
        keys: mallory.keys(),
        nonce_commitment: nonce_commitment(
            &phone.id,
            &mallory.approval_nonce.as_ref().unwrap().0[..],
        )
        .unwrap()
        .to_vec(),
    };
    let writer = env.writer();
    let challenge =
        build::challenge(&writer, &shown_state, &laptop, &fake_request, &mut rng).unwrap();
    assert_eq!(
        build::respond(&writer, &real, &mut phone, &challenge.challenge, &mut rng).unwrap_err(),
        refused(RefusalCode::InvalidChallenge)
    );
}

/// The approval code depends on both nonces, and each side commits to its
/// own before it learns the other's (§2.5): the requester's in its request,
/// the approver's in its signed challenge. In an honest approval both
/// devices show the same code: the approver once the proof revealed `n_R`,
/// the requester once it accepted the challenge. A challenge whose nonce or
/// signature was altered is refused, and a proof whose nonce does not open
/// the request's commitment is refused, so neither nonce can be changed
/// after the fact to search for a matching code.
#[test]
fn approval_codes_match_and_neither_nonce_can_be_changed() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(32);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let stranger = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.request(&stranger, &mut rng);
    let state = ledger.state(&env);
    let writer = env.writer();
    let request = phone.request();
    let challenge = build::challenge(&writer, &state, &laptop, &request, &mut rng).unwrap();
    let own_nonce = phone.approval_nonce.as_ref().unwrap().0.to_vec();
    let unanswered_phone = phone.clone();
    let proof =
        build::respond(&writer, &state, &mut phone, &challenge.challenge, &mut rng).unwrap();
    assert_eq!(proof.nonce, own_nonce);
    build::verify_possession(
        &writer,
        &challenge.challenge,
        &proof,
        &request,
        &challenge.secret,
    )
    .unwrap();
    assert_eq!(
        build::respond(&writer, &state, &mut phone, &challenge.challenge, &mut rng).unwrap_err(),
        refused(RefusalCode::InvalidChallenge)
    );
    let coordinates = challenge.challenge.coordinates();
    let code = |keys: &jazz_e2ee::protocol::records::KeyColumns, n_a: &[u8], n_r: &[u8]| {
        approval_code(&env.application(), ALICE, coordinates, keys, n_a, n_r).unwrap()
    };
    let on_approver = code(&request.keys, &challenge.challenge.nonce, &proof.nonce);
    let on_phone = code(&phone.keys(), &challenge.challenge.nonce, &own_nonce);
    assert_eq!(on_approver, on_phone);
    assert_eq!(on_approver.len(), 6);
    assert!(on_approver.bytes().all(|byte| byte.is_ascii_digit()));
    assert_ne!(on_phone, code(&phone.keys(), &[0; 32], &own_nonce));
    assert_ne!(
        on_phone,
        code(&phone.keys(), &challenge.challenge.nonce, &[0; 32])
    );

    let mut other_nonce = challenge.challenge.clone();
    other_nonce.nonce = vec![7; 32];
    let mut tampered = challenge.challenge.clone();
    let last = tampered.signature.len() - 1;
    tampered.signature[last] ^= 1;
    let mut by_stranger = challenge.challenge.clone();
    by_stranger.signer_id = stranger.id.clone();
    let bytes = challenge_signed_bytes(
        &env.application(),
        ALICE,
        by_stranger.coordinates(),
        &stranger.id,
        &request,
        &by_stranger.envelope,
        &by_stranger.nonce,
    )
    .unwrap();
    by_stranger.signature = signature::Builtin.sign(&stranger.signing, &bytes).unwrap();
    for (name, bad) in [
        ("altered nonce", other_nonce),
        ("altered signature", tampered),
        ("inactive signer", by_stranger),
    ] {
        assert_eq!(
            build::respond(
                &writer,
                &state,
                &mut unanswered_phone.clone(),
                &bad,
                &mut rng
            )
            .expect_err(name),
            refused(RefusalCode::InvalidChallenge),
            "{name}"
        );
    }

    let mut swapped = proof.clone();
    swapped.nonce = vec![9; 32];
    let bytes = proof_signed_bytes(
        &env.application(),
        ALICE,
        coordinates,
        &swapped.proof,
        &swapped.nonce,
    )
    .unwrap();
    swapped.signature = signature::Builtin.sign(&phone.signing, &bytes).unwrap();
    assert_eq!(
        build::verify_possession(
            &writer,
            &challenge.challenge,
            &swapped,
            &request,
            &challenge.secret,
        )
        .unwrap_err(),
        refused(RefusalCode::InvalidPossessionProof)
    );
}

/// The possession check compares the proof's nonce commitment against the
/// request's in constant time over every byte: a request whose commitment
/// differs from the opened one in its first or last byte, or is truncated,
/// extended or empty, refuses phone's otherwise valid proof, and only the
/// exact commitment accepts it.
#[test]
fn possession_check_needs_the_exact_nonce_commitment() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(34);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let state = ledger.state(&env);
    let writer = env.writer();
    let request = phone.request();
    let challenge = build::challenge(&writer, &state, &laptop, &request, &mut rng).unwrap();
    let proof =
        build::respond(&writer, &state, &mut phone, &challenge.challenge, &mut rng).unwrap();
    let verify = |request: &jazz_e2ee::protocol::records::account::DeviceRequest| {
        build::verify_possession(
            &writer,
            &challenge.challenge,
            &proof,
            request,
            &challenge.secret,
        )
    };
    verify(&request).unwrap();

    let commitment = request.nonce_commitment.clone();
    assert_eq!(commitment.len(), 32);
    let mut first = commitment.clone();
    first[0] ^= 1;
    let mut last = commitment.clone();
    last[31] ^= 0x80;
    let truncated = commitment[..31].to_vec();
    let mut extended = commitment.clone();
    extended.push(0);
    for bad in [first, last, truncated, extended, Vec::new()] {
        let mut altered = request.clone();
        altered.nonce_commitment = bad;
        assert_eq!(
            verify(&altered).unwrap_err(),
            refused(RefusalCode::InvalidPossessionProof)
        );
    }
}

/// Private verification requires the challenge to be signed by the
/// approval's signer over the requester's keys and nonce commitment, and the
/// proof's nonce to open that commitment.
#[test]
fn private_verification_requires_the_signed_challenge_and_the_nonce() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(33);
    let laptop = env.device(&mut rng);
    let mut tablet = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&tablet, &mut rng);
    ledger.approve(&env, &laptop, &mut tablet, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(&env, &laptop, &mut phone, &mut rng);
    let count = |ledger: &Ledger| {
        let replay = ledger.replay(&env);
        let state = replay.current().unwrap().unwrap();
        verify_private(&replay, &state, &ledger.private)
            .unwrap()
            .approvals()
            .len()
    };
    assert_eq!(count(&ledger), 2, "the tablet's and the phone's approvals");

    let mut resigned = ledger.clone();
    let challenge = &mut resigned.private.challenges.last_mut().unwrap().row;
    challenge.signer_id = tablet.id.clone();
    let bytes = challenge_signed_bytes(
        &env.application(),
        ALICE,
        challenge.coordinates(),
        &tablet.id,
        &phone.request(),
        &challenge.envelope,
        &challenge.nonce,
    )
    .unwrap();
    challenge.signature = signature::Builtin.sign(&tablet.signing, &bytes).unwrap();
    assert_eq!(count(&resigned), 1, "only the tablet's approval verifies");

    let mut tampered = ledger.clone();
    let signature = &mut tampered
        .private
        .challenges
        .last_mut()
        .unwrap()
        .row
        .signature;
    let last = signature.len() - 1;
    signature[last] ^= 1;
    assert_eq!(count(&tampered), 1);

    // A proof re-signed with another nonce: its signature verifies, but the
    // nonce does not open the request's commitment.
    let mut renonced = ledger.clone();
    let proof = &mut renonced.private.proofs.last_mut().unwrap().row;
    proof.nonce = vec![9; 32];
    let coordinates = renonced
        .private
        .challenges
        .last()
        .unwrap()
        .row
        .coordinates();
    let bytes = proof_signed_bytes(
        &env.application(),
        ALICE,
        coordinates,
        &proof.proof,
        &proof.nonce,
    )
    .unwrap();
    proof.signature = signature::Builtin.sign(&phone.signing, &bytes).unwrap();
    assert_eq!(count(&renonced), 1);
}

/// A device answers one challenge with its approval nonce (§2.2, §2.5): a
/// second, even correctly signed, is refused (`invalid-challenge`), so
/// whoever saw `n_R` revealed cannot search for a challenge nonce matching
/// another code. A bad challenge does not spend the nonce.
#[test]
fn the_approval_nonce_answers_one_challenge() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(34);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let state = ledger.state(&env);
    let writer = env.writer();
    let first = build::challenge(&writer, &state, &laptop, &phone.request(), &mut rng).unwrap();
    let mut tampered = first.challenge.clone();
    tampered.nonce = vec![7; 32];
    assert_eq!(
        build::respond(&writer, &state, &mut phone, &tampered, &mut rng).unwrap_err(),
        refused(RefusalCode::InvalidChallenge)
    );
    build::respond(&writer, &state, &mut phone, &first.challenge, &mut rng).unwrap();
    assert!(phone.approval_nonce.is_none());
    let second = build::challenge(&writer, &state, &laptop, &phone.request(), &mut rng).unwrap();
    assert_eq!(
        build::respond(&writer, &state, &mut phone, &second.challenge, &mut rng).unwrap_err(),
        refused(RefusalCode::InvalidChallenge)
    );
}

/// `approve` completes a challenge for the request it was signed over only:
/// a request swapped in at completion (another nonce commitment, with a
/// proof re-signed to open it) is refused as `stale-challenge` before any
/// key is delivered.
#[test]
fn approve_refuses_a_request_other_than_the_challenged_one() {
    let env = Env::new(ALICE);
    let mut rng = TestRandom(35);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let key = ledger.acquire(&env, &laptop).unwrap();
    let state = ledger.state(&env);
    let writer = env.writer();
    let request = phone.request();
    let challenge = build::challenge(&writer, &state, &laptop, &request, &mut rng).unwrap();
    let proof =
        build::respond(&writer, &state, &mut phone, &challenge.challenge, &mut rng).unwrap();

    let other_nonce = [5u8; 32];
    let mut swapped = request.clone();
    swapped.nonce_commitment = nonce_commitment(&phone.id, &other_nonce).unwrap().to_vec();
    let mut reproof = proof.clone();
    reproof.nonce = other_nonce.to_vec();
    let bytes = proof_signed_bytes(
        &env.application(),
        ALICE,
        challenge.challenge.coordinates(),
        &reproof.proof,
        &reproof.nonce,
    )
    .unwrap();
    reproof.signature = signature::Builtin.sign(&phone.signing, &bytes).unwrap();
    assert_eq!(
        build::approve(
            &writer, &state, &laptop, &challenge, &reproof, &swapped, &key, &mut rng
        )
        .unwrap_err(),
        refused(RefusalCode::StaleChallenge)
    );
    build::approve(
        &writer, &state, &laptop, &challenge, &proof, &request, &key, &mut rng,
    )
    .unwrap();
}

/// Alice's linked device has no account secret. It can retire an independent
/// root, but needs the secret to distinguish a derived root from her own.
#[test]
fn retiring_a_derived_root_on_a_linked_device_requires_the_account_secret() {
    let env = Env::device_account([1; 32]);
    let mut rng = TestRandom(13);
    let owner = env.device(&mut rng);
    let mut linked = env.device(&mut rng);
    let mut ledger = Ledger::new(&env.account);
    ledger.request(&owner, &mut rng);
    ledger.root(&env, &owner, 0, &mut rng);
    ledger.request(&linked, &mut rng);
    ledger.approve(&env, &owner, &mut linked, &mut rng);
    let derived =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([2; 32]), &env.application(), 0).unwrap();
    let mut independent =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([3; 32]), &env.application(), 0).unwrap();
    independent.root_id = jazz_e2ee::protocol::ids::random_v4(&mut rng).unwrap();
    ledger.register_recovery(&env, &linked, &derived, &mut rng);
    ledger.register_recovery(&env, &linked, &independent, &mut rng);
    ledger.rotate(
        &env,
        &linked,
        RotationTarget::RetireRecoveryRoot(independent.root_id.clone()),
        &mut rng,
    );
    let state = ledger.state(&env);
    assert!(state.recovery_root(&independent.root_id).is_none());
    let key = ledger.acquire(&env, &linked).unwrap();
    let result = build::rotate(
        &env.writer(),
        &state,
        &linked,
        &key,
        &RotationTarget::RetireRecoveryRoot(derived.root_id),
        &mut rng,
    );
    assert_eq!(
        result.unwrap_err(),
        refused(RefusalCode::DeviceAccountSecretRequired)
    );
}

/// A successor that retires a recovery root retires one of its own account:
/// the server does not check it (the target is in an ID list no policy
/// reads), so replay does. A root of another account is not accepted as the
/// target, whether the ledger lacks it or the server supplied it among the
/// account's roots.
#[test]
fn a_successor_retiring_another_accounts_recovery_root_is_rejected() {
    const BOB: &str = "b0b00000-0000-5000-8000-00000000b0b0";
    let mut rng = TestRandom(23);
    let env = Env::new(ALICE);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(&env, &laptop, 0, &mut rng);
    let own =
        RecoveryKeys::derive(&RecoverySecret::from_bytes([3; 32]), &env.application(), 0).unwrap();
    ledger.register_recovery(&env, &laptop, &own, &mut rng);

    // Bob's recovery root, registered in his own ledger.
    let bobs_env = Env::new(BOB);
    let bobs_laptop = bobs_env.device(&mut rng);
    let mut bobs_ledger = Ledger::new(BOB);
    bobs_ledger.request(&bobs_laptop, &mut rng);
    bobs_ledger.root(&bobs_env, &bobs_laptop, 0, &mut rng);
    let bobs = RecoveryKeys::derive(
        &RecoverySecret::from_bytes([4; 32]),
        &bobs_env.application(),
        0,
    )
    .unwrap();
    let bobs_root = bobs_ledger.register_recovery(&bobs_env, &bobs_laptop, &bobs, &mut rng);

    // The successor as laptop builds it, retiring its own root; the
    // same successor with the target swapped for bob's root.
    let key = ledger.acquire(&env, &laptop).unwrap();
    let state = ledger.state(&env);
    let epoch = state.epoch_id().to_owned();
    let legitimate = build::rotate(
        &env.writer(),
        &state,
        &laptop,
        &key,
        &RotationTarget::RetireRecoveryRoot(own.root_id.clone()),
        &mut rng,
    )
    .unwrap()
    .public;
    let mut foreign = legitimate.clone();
    foreign.columns.retired_recovery_root_ids =
        jazz_e2ee::protocol::json::encode_root_id_list([bobs.root_id.as_str()]).unwrap();
    re_sign(&env, &mut foreign, &laptop);

    assert_ne!(
        with_successor(&env, &ledger, legitimate).epoch_id(),
        epoch,
        "the account's own root is retired"
    );
    assert_eq!(
        with_successor(&env, &ledger, foreign.clone()).epoch_id(),
        epoch,
        "a root the account's ledger lacks"
    );

    let mut supplied = ledger.clone();
    let at = supplied.next();
    supplied
        .public
        .recovery_roots
        .push(Accepted::new(at, bobs_root));
    let state = with_successor(&env, &supplied, foreign);
    assert_eq!(state.epoch_id(), epoch, "a root of another account");
    assert!(state.retired_recovery_roots.is_empty());
}

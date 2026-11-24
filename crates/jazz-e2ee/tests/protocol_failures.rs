//! Provider failures propagate through the protocol (`E2EE_PROTOCOL.md` §0,
//! §2.6, §2.7, §7): a failing signer or key wrapper stops replay, key
//! authentication and the possession check with `CryptoFailure`; it is never
//! reported as an invalid signature, an unusable candidate or a refusal.
//!
//! The custom mechanisms below wrap the built-ins under their own IDs and
//! can be switched to fail, as a host-provided mechanism might.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::TestRandom;
use common::mechanisms::{FLAKY_SIGN, FLAKY_WRAP, flaky_registry_with_all_record_signer};
use common::protocol::{ALICE, Env, Ledger};
use jazz_e2ee::Error;
use jazz_e2ee::agility::{KeyWrapper, MechanismConfig, MechanismImpl};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::protocol::account::build;
use jazz_e2ee::protocol::error::ProtocolError;
use jazz_e2ee::registry::Registry;
use jazz_e2ee::{RandomSource, Result, Zeroizing};

struct Flaky {
    env: Env,
    signer_fails: Arc<AtomicBool>,
    wrapper_fails: Arc<AtomicBool>,
}

fn flaky() -> Flaky {
    let (registry, wrapper_fails, signer_fails) = flaky_registry_with_all_record_signer();
    let config = MechanismConfig {
        signature: FLAKY_SIGN.into(),
        key_wrap: FLAKY_WRAP.into(),
        ..MechanismConfig::default()
    };
    Flaky {
        env: Env::with(registry, config, ALICE),
        signer_fails,
        wrapper_fails,
    }
}

/// While the signer works, the custom mechanisms behave like the built-ins;
/// when its verification fails, replay stops with `CryptoFailure` instead
/// of skipping the approval.
///
/// ```text
/// laptop ──approves──► phone       verify ──✗ provider── replay fails loudly
/// ```
#[test]
fn signer_failure_stops_replay() {
    let flaky = flaky();
    let env = &flaky.env;
    let mut rng = TestRandom(31);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    ledger.approve(env, &laptop, &mut phone, &mut rng);
    assert!(ledger.state(env).is_active(&phone.id));

    flaky.signer_fails.store(true, Ordering::SeqCst);
    assert_eq!(
        ledger.replay(env).current().unwrap_err(),
        ProtocolError::ProviderFailure
    );
}

/// A key wrapper failure while checking the account key's required marker
/// stops acquisition with `CryptoFailure`; it is not "key unavailable".
#[test]
fn wrapper_failure_stops_key_authentication() {
    let flaky = flaky();
    let env = &flaky.env;
    let mut rng = TestRandom(32);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    let (_, key) = ledger.root(env, &laptop, 0, &mut rng);
    assert_eq!(*ledger.acquire(env, &laptop).unwrap().key(), key);

    flaky.wrapper_fails.store(true, Ordering::SeqCst);
    assert_eq!(
        ledger.acquire(env, &laptop).unwrap_err(),
        ProtocolError::ProviderFailure
    );
}

/// A provider failure during the possession check is reported as itself,
/// not as an invalid possession proof.
#[test]
fn wrapper_failure_is_not_an_invalid_possession_proof() {
    let flaky = flaky();
    let env = &flaky.env;
    let mut rng = TestRandom(33);
    let laptop = env.device(&mut rng);
    let mut phone = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    ledger.root(env, &laptop, 0, &mut rng);
    ledger.request(&phone, &mut rng);
    let writer = env.writer();
    let state = ledger.state(env);
    let challenge = build::challenge(&writer, &state, &laptop, &phone.request(), &mut rng).unwrap();
    let proof =
        build::respond(&writer, &state, &mut phone, &challenge.challenge, &mut rng).unwrap();

    flaky.wrapper_fails.store(true, Ordering::SeqCst);
    assert_eq!(
        build::verify_possession(
            &writer,
            &challenge.challenge,
            &proof,
            &phone.request(),
            &challenge.secret
        ),
        Err(ProtocolError::ProviderFailure)
    );
}

/// A wrapper that returns a correctly labelled envelope with an unusable
/// payload: the provider's output, not anyone's input, is wrong.
struct GarbleWrapper;

const GARBLE: Mechanism<'static> = Mechanism::new("com.example.garble-wrap", 1);

impl MechanismImpl for GarbleWrapper {
    fn mechanism(&self) -> Mechanism<'static> {
        GARBLE
    }
}

impl KeyWrapper for GarbleWrapper {
    fn wrap(
        &self,
        _wrapping_key: &[u8; 32],
        _context: &[u8],
        _key: &[u8; 32],
        _rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        envelope::encode(GARBLE, &[0xee; 7])
    }

    fn unwrap(
        &self,
        _wrapping_key: &[u8; 32],
        _context: &[u8],
        _envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        Err(Error::InvalidCiphertext)
    }
}

/// A writer that cannot read back the marker it just wrapped reports a
/// provider failure, not invalid ciphertext.
#[test]
fn unreadable_fresh_output_is_a_provider_failure() {
    let mut registry = Registry::builtin();
    registry
        .register_key_wrapper(Arc::new(GarbleWrapper))
        .unwrap();
    let config = MechanismConfig {
        key_wrap: GARBLE.into(),
        ..MechanismConfig::default()
    };
    let env = Env::with(registry, config, ALICE);
    let mut rng = TestRandom(34);
    let laptop = env.device(&mut rng);
    let mut ledger = Ledger::new(ALICE);
    ledger.request(&laptop, &mut rng);
    let writer = env.writer();
    let permit = build::check_bootstrap(&writer, &laptop, &ledger.public, &ledger.private).unwrap();
    let error = build::account_root(
        &writer,
        &laptop,
        &permit,
        build::RootAuthority::App,
        &mut rng,
    )
    .unwrap_err();
    assert_eq!(error, ProtocolError::ProviderFailure);
    assert!(error.is_provider_failure());
}

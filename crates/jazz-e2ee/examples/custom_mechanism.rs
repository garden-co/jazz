//! Adding a custom key-sealing mechanism, end to end:
//!
//! 1. implement the role's trait ([`KeySealer`]),
//! 2. register it under a reverse-DNS ID,
//! 3. run the conformance kit,
//! 4. select it in the configuration and seal to recipients that advertise it.
//!
//! The mechanism here is a toy: it delegates to the built-in HPKE sealer and
//! only relabels the envelope and keys with its own ID. A real replacement,
//! such as a post-quantum or hybrid KEM, has the same shape.
//!
//! Run with `cargo run -p jazz-e2ee --example custom_mechanism --features conformance`.

use std::sync::Arc;

use jazz_e2ee::agility::{
    KeyPair, KeySealer, MechanismConfig, MechanismId, MechanismImpl, PublicKey,
};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::key_seal;
use jazz_e2ee::registry::Registry;
use jazz_e2ee::{Error, OsRandom, RandomSource, Result, Zeroizing, conformance};

const TOY: Mechanism<'static> = Mechanism::new("com.example.toy-kem", 1);

/// Seals with the built-in HPKE mechanism, under its own envelope and keys.
struct ToyKem;

impl ToyKem {
    /// The same key, labelled as the built-in mechanism's.
    fn inner(public: &PublicKey) -> Result<PublicKey> {
        if public.mechanism != TOY {
            return Err(Error::RecipientUnsupported);
        }
        Ok(PublicKey {
            mechanism: key_seal::MECHANISM.into(),
            bytes: public.bytes.clone(),
        })
    }
}

impl MechanismImpl for ToyKem {
    fn mechanism(&self) -> Mechanism<'static> {
        TOY
    }
}

impl KeySealer for ToyKem {
    fn derive_key_pair(&self, ikm: &[u8]) -> Result<KeyPair> {
        let mut pair = key_seal::Builtin.derive_key_pair(ikm)?;
        pair.public.mechanism = TOY.into();
        Ok(pair)
    }

    fn seal(
        &self,
        recipient: &PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        let sealed = key_seal::Builtin.seal(&Self::inner(recipient)?, context, key, rng)?;
        envelope::encode(TOY, &sealed)
    }

    fn open(
        &self,
        recipient: &KeyPair,
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        let inner = envelope::decode(TOY, envelope)?;
        let pair = KeyPair {
            public: Self::inner(&recipient.public)?,
            secret: recipient.secret.clone(),
        };
        key_seal::Builtin.open(&pair, context, inner)
    }
}

fn run() -> core::result::Result<(), Box<dyn std::error::Error>> {
    // 1–2. Register beside the built-ins. The reserved `jazz.` namespace and
    // IDs without a domain are refused.
    let mut registry = Registry::builtin();
    registry.register_key_sealer(Arc::new(ToyKem))?;

    // 3. The conformance kit. A real mechanism also declares vectors.
    conformance::check_key_sealer(&ToyKem, &[])?;

    // 4. Select it for sealing; every other role keeps its built-in.
    let config = MechanismConfig {
        key_seal: MechanismId::new("com.example.toy-kem", 1),
        ..MechanismConfig::default()
    };
    let suite = registry.select(&config)?;

    // A device advertises key pairs for the mechanisms it supports.
    let mut rng = OsRandom;
    let upgraded = ToyKem.generate_key_pair(&mut rng)?;
    let classic = key_seal::Builtin.generate_key_pair(&mut rng)?;
    let advertised = [classic.public.clone(), upgraded.public.clone()];

    let context = b"example context";
    let key = [7u8; 32];
    let sealed = suite.seal_to(&advertised, context, &key, &mut rng)?;
    assert!(envelope::decode(TOY, &sealed).is_ok());

    // Readers route by the envelope's mechanism, with the matching key pair.
    let opened = registry.open_sealed(&[classic.clone(), upgraded], context, &sealed)?;
    assert_eq!(*opened, key);

    // A recipient that advertises only the built-in key is refused, not
    // silently sealed to with another mechanism.
    let refused = suite.seal_to(&[classic.public], context, &key, &mut rng);
    assert_eq!(refused, Err(Error::RecipientUnsupported));

    // Without the mechanism registered, its envelopes are an unsupported
    // format rather than corrupt data.
    let builtin_only = Registry::builtin();
    assert_eq!(
        builtin_only.key_sealer_for(&sealed).map(|_| ()),
        Err(Error::UnsupportedFormat)
    );
    println!("com.example.toy-kem/1 registered, conformant and selected");
    Ok(())
}

#[cfg_attr(test, allow(dead_code))]
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[test]
fn example_runs() {
    run().unwrap();
}

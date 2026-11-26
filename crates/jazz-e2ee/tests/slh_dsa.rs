//! FIPS 205 external, pure SHA2-128s verification and interoperability.

use fips205::{
    slh_dsa_sha2_128s::KG,
    traits::{KeyGen, SerDes, Signer, Verifier},
};
use jazz_e2ee::{
    Error, OsRandom, RandomSource, Result, Zeroizing,
    conformance::FixedRandom,
    primitives::slh_dsa::{CommitmentKey, verify},
};

macro_rules! vector {
    ($name:literal) => {
        include_bytes!(concat!("fixtures/slh_dsa/", $name, ".bin"))
    };
}

const CONTEXT: &[u8] = b"jazz.e2ee.endorsement.v1";
const MESSAGE: &[u8] = b"endorsement fixture";

/// Bob accepts NIST's valid signature at maximum context length and rejects
/// its separately generated negative case with a modified randomizer.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn acvp_verify() {
    assert!(
        verify(
            vector!("sigver-266-pk"),
            vector!("sigver-266-message"),
            vector!("sigver-266-context"),
            vector!("sigver-266-signature"),
        )
        .unwrap()
    );
    assert!(
        !verify(
            vector!("sigver-253-pk"),
            vector!("sigver-253-message"),
            vector!("sigver-253-context"),
            vector!("sigver-253-signature"),
        )
        .unwrap()
    );
}

/// Alice and Bob use independent providers in both directions under a nonempty
/// context. The seed 00..1f also pins the public key and deterministic signature.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn independent_signers() {
    let seed = Zeroizing::new(std::array::from_fn(|i| i as u8));
    let (alice, public) = CommitmentKey::from_seed(&seed);
    let parts = vector!("jazz-seed-parts");
    let (bob_pk, bob_sk) = KG::keygen_from_seed::<16>(
        parts[..16].try_into().unwrap(),
        parts[16..32].try_into().unwrap(),
        parts[32..].try_into().unwrap(),
    );
    assert_eq!(&public, vector!("jazz-pk"));
    assert_eq!(bob_pk.clone().into_bytes(), public);

    // FIPS 205's deterministic variant uses PK.seed in place of addrnd.
    let deterministic = alice
        .sign(
            &public,
            MESSAGE,
            CONTEXT,
            &mut FixedRandom(public[..16].to_vec()),
        )
        .unwrap();
    assert_eq!(deterministic.as_slice(), vector!("jazz-signature"));

    let signature = alice
        .sign(&public, MESSAGE, CONTEXT, &mut OsRandom)
        .unwrap();
    assert!(bob_pk.verify(MESSAGE, signature.as_slice().try_into().unwrap(), CONTEXT));
    assert_ne!(signature, deterministic);
    let theirs = bob_sk.try_sign(MESSAGE, CONTEXT, true).unwrap();
    assert!(verify(&public, MESSAGE, CONTEXT, &theirs).unwrap());
    assert!(!verify(&public, MESSAGE, b"another context", &theirs).unwrap());
    assert!(!verify(&public, b"another message", CONTEXT, &theirs).unwrap());
}

/// Alice refuses a mismatched commitment and propagates a provider failure.
/// Bob distinguishes malformed signatures from an unusable caller context.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn signing_and_verification_errors() {
    struct Broken;
    impl RandomSource for Broken {
        fn fill(&mut self, _: &mut [u8]) -> Result<()> {
            Err(Error::CryptoFailure)
        }
    }

    let (alice, pk) = CommitmentKey::from_seed(&Zeroizing::new([0; 32]));
    assert_eq!(
        alice.sign(&pk, MESSAGE, CONTEXT, &mut Broken),
        Err(Error::CryptoFailure)
    );
    assert_eq!(
        alice.sign(&[0; 32], MESSAGE, CONTEXT, &mut Broken),
        Err(Error::InvalidInput)
    );
    assert_eq!(
        alice.sign(&pk, MESSAGE, &[0; 256], &mut Broken),
        Err(Error::InvalidInput)
    );
    let sig = vector!("jazz-signature");
    assert_eq!(
        verify(&pk, MESSAGE, &[0; 256], sig),
        Err(Error::InvalidInput)
    );
    assert_eq!(verify(&pk[..31], MESSAGE, CONTEXT, sig), Ok(false));
    assert_eq!(verify(&pk, MESSAGE, CONTEXT, &sig[1..]), Ok(false));
}

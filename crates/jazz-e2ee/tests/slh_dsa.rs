//! FIPS 205 SHA2-128s key generation, checked against an independent provider.

use fips205::{
    slh_dsa_sha2_128s::KG,
    traits::{KeyGen, SerDes},
};
use jazz_e2ee::{Zeroizing, primitives::slh_dsa::CommitmentKey};

macro_rules! vector {
    ($name:literal) => {
        include_bytes!(concat!("fixtures/slh_dsa/", $name, ".bin"))
    };
}

/// Alice and Bob derive the same public key with independent providers. The
/// seed 00..1f also pins the public key.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn independent_key_generation() {
    let seed = Zeroizing::new(std::array::from_fn(|i| i as u8));
    let (_, public) = CommitmentKey::from_seed(&seed);
    let parts = vector!("jazz-seed-parts");
    let (bob_pk, _) = KG::keygen_from_seed::<16>(
        parts[..16].try_into().unwrap(),
        parts[16..32].try_into().unwrap(),
        parts[32..].try_into().unwrap(),
    );
    assert_eq!(&public, vector!("jazz-pk"));
    assert_eq!(bob_pk.into_bytes(), public);
}

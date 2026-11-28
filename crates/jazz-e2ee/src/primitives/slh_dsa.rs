//! SLH-DSA-SHA2-128s (FIPS 205) key derivation for commitments.

use slh_dsa::{Sha2_128s, SigningKey, signature::Keypair};
use zeroize::Zeroizing;

use super::hash::keyed_hash;

pub const PUBLIC_KEY_BYTES: usize = 32;

/// A commitment key pair. The provider clears its secret seeds on drop.
// The provider derives Debug over its secret fields. Keep the wrapper opaque.
pub struct CommitmentKey(SigningKey<Sha2_128s>);

fn derive(seed: &[u8; 32], label: &[u8]) -> Zeroizing<[u8; 16]> {
    let hash = keyed_hash(seed, label);
    let mut part = Zeroizing::new([0; 16]);
    part.copy_from_slice(&hash[..16]);
    part
}

impl CommitmentKey {
    /// Derive the signing key and public commitment from a protected seed.
    pub fn from_seed(seed: &Zeroizing<[u8; 32]>) -> (Self, [u8; PUBLIC_KEY_BYTES]) {
        let key = Self::from_parts(
            &derive(seed, b"jazz.e2ee.slh-dsa-sha2-128s.sk-seed.v1\0"),
            &derive(seed, b"jazz.e2ee.slh-dsa-sha2-128s.sk-prf.v1\0"),
            &derive(seed, b"jazz.e2ee.slh-dsa-sha2-128s.pk-seed.v1\0"),
        );
        let public = key.public_key();
        (key, public)
    }

    fn from_parts(sk_seed: &[u8; 16], sk_prf: &[u8; 16], pk_seed: &[u8; 16]) -> Self {
        Self(SigningKey::slh_keygen_internal(sk_seed, sk_prf, pk_seed))
    }

    /// The FIPS 205 public key, PK.seed || PK.root.
    fn public_key(&self) -> [u8; PUBLIC_KEY_BYTES] {
        self.0.verifying_key().to_bytes().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! vector {
        ($name:literal) => {
            include_bytes!(concat!("../../tests/fixtures/slh_dsa/", $name, ".bin"))
        };
    }

    /// Alice's three derived seeds must match the byte contract. Only the
    /// primitive can inspect them without exposing the signing key.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn commitment_seed_parts() {
        let seed = Zeroizing::new(std::array::from_fn(|i| i as u8));
        let (key, _) = CommitmentKey::from_seed(&seed);
        assert_eq!(&key.0.to_bytes()[..48], vector!("jazz-seed-parts"));
    }

    /// Alice's NIST key-generation vector starts at the FIPS seed triple,
    /// below Jazz's commitment-seed derivation.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn acvp_keygen() {
        let sk = vector!("keygen-1-sk");
        let key = CommitmentKey::from_parts(
            sk[..16].try_into().unwrap(),
            sk[16..32].try_into().unwrap(),
            sk[32..48].try_into().unwrap(),
        );
        assert_eq!(key.0.to_bytes().as_slice(), sk);
        assert_eq!(key.public_key(), sk[32..]);
    }
}

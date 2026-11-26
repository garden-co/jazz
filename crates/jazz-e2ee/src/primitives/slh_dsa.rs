//! Pure SLH-DSA-SHA2-128s (FIPS 205) for endorsements.

use slh_dsa::{Sha2_128s, Signature, SigningKey, VerifyingKey, signature::Keypair};
use zeroize::Zeroizing;

use super::hash::keyed_hash;
use crate::{Error, RandomSource, Result};

pub const PUBLIC_KEY_BYTES: usize = 32;
pub const SIGNATURE_BYTES: usize = 7856;

/// An endorsement signing key. The provider clears its secret seeds on drop.
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

    /// Sign with fresh randomness, then verify against the stored commitment.
    pub fn sign(
        &self,
        commitment: &[u8],
        message: &[u8],
        context: &[u8],
        rng: &mut (impl RandomSource + ?Sized),
    ) -> Result<Vec<u8>> {
        if self.public_key() != commitment || context.len() > 255 {
            return Err(Error::InvalidInput);
        }
        let mut randomness = Zeroizing::new([0; 16]);
        rng.fill(&mut randomness[..])?;
        let signature = self
            .0
            .try_sign_with_context(message, context, Some(&randomness[..]))
            .map_err(|_| Error::CryptoFailure)?
            .to_bytes()
            .to_vec();
        if !verify(commitment, message, context, &signature)? {
            return Err(Error::CryptoFailure);
        }
        Ok(signature)
    }
}

/// Malformed or unauthentic signatures are invalid. A provider failure is an error.
pub fn verify(commitment: &[u8], message: &[u8], context: &[u8], signature: &[u8]) -> Result<bool> {
    if context.len() > 255 {
        return Err(Error::InvalidInput);
    }
    if commitment.len() != PUBLIC_KEY_BYTES || signature.len() != SIGNATURE_BYTES {
        return Ok(false);
    }
    // For this parameter set every byte string of the fixed length decodes.
    let public =
        VerifyingKey::<Sha2_128s>::try_from(commitment).map_err(|_| Error::CryptoFailure)?;
    let signature =
        Signature::<Sha2_128s>::try_from(signature).map_err(|_| Error::CryptoFailure)?;
    // In rc.5, after the context length check the only error here is a root mismatch.
    Ok(public
        .try_verify_with_context(message, context, &signature)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::FixedRandom;

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

    /// Alice's NIST key-generation and hedged-signature vectors start at
    /// the FIPS seed triple, below Jazz's commitment-seed derivation.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn acvp_keygen_and_sign() {
        fn key(sk: &[u8; 64]) -> CommitmentKey {
            CommitmentKey::from_parts(
                sk[..16].try_into().unwrap(),
                sk[16..32].try_into().unwrap(),
                sk[32..48].try_into().unwrap(),
            )
        }

        let sk = vector!("keygen-1-sk");
        assert_eq!(key(sk).0.to_bytes().as_slice(), sk);
        let sk = vector!("siggen-475-sk");
        let signature = key(sk)
            .sign(
                &sk[32..],
                vector!("siggen-475-message"),
                vector!("siggen-475-context"),
                &mut FixedRandom(vector!("siggen-475-additionalRandomness").to_vec()),
            )
            .unwrap();
        assert_eq!(signature.as_slice(), vector!("siggen-475-signature"));
    }

    /// Alice holds a corrupt private key with the expected public commitment.
    /// Only this test can construct it to exercise the signer's self-check.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn rejects_provider_signature_that_fails_verification() {
        let mut corrupt = *vector!("keygen-1-sk");
        corrupt[0] ^= 1;
        let key = CommitmentKey(SigningKey::try_from(corrupt.as_slice()).unwrap());
        assert!(matches!(
            key.sign(
                &corrupt[32..],
                b"message",
                b"context",
                &mut FixedRandom(vec![0; 16])
            ),
            Err(Error::CryptoFailure)
        ));
    }
}

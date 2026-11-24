//! HPKE (RFC 9180) in base mode, single-shot, with the suite
//! DHKEM(X25519, HKDF-SHA256), HKDF-SHA256 and ChaCha20Poly1305
//! (KEM `0x0020`, KDF `0x0001`, AEAD `0x0003`), from the `hpke` crate.

use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, HpkeError, Kem, OpModeR, OpModeS, Serializable};
use zeroize::Zeroizing;

use crate::error::{Error, Result};
use crate::random::RandomSource;

pub const PUBLIC_KEY_BYTES: usize = 32;
pub const SECRET_KEY_BYTES: usize = 32;
/// The encapsulated key: the sender's ephemeral X25519 public key.
pub const ENCAPSULATED_BYTES: usize = 32;
pub const TAG_BYTES: usize = 16;

type Suite = X25519HkdfSha256;

/// An HPKE recipient keypair.
pub struct HpkeKeyPair {
    pub public_key: [u8; PUBLIC_KEY_BYTES],
    pub secret_key: Zeroizing<[u8; SECRET_KEY_BYTES]>,
}

impl HpkeKeyPair {
    /// `DeriveKeyPair` from caller-supplied input keying material.
    pub fn derive(ikm: &[u8]) -> Self {
        let (secret, public) = Suite::derive_keypair(ikm);
        Self {
            public_key: public.to_bytes().into(),
            secret_key: Zeroizing::new(secret.to_bytes().into()),
        }
    }
}

/// The public key of an HPKE secret key.
pub fn public_key(secret_key: &[u8; SECRET_KEY_BYTES]) -> Result<[u8; PUBLIC_KEY_BYTES]> {
    let secret =
        <Suite as Kem>::PrivateKey::from_bytes(secret_key).map_err(|_| Error::InvalidInput)?;
    Ok(Suite::sk_to_pk(&secret).to_bytes().into())
}

/// Adapts a fallible [`RandomSource`] to the infallible RNG `hpke` expects:
/// a failure is recorded, and the caller discards the output.
struct Rng<'a, R: RandomSource + ?Sized> {
    source: &'a mut R,
    failed: bool,
}

impl<R: RandomSource + ?Sized> rand_core::TryRng for Rng<'_, R> {
    type Error = core::convert::Infallible;

    fn try_next_u32(&mut self) -> core::result::Result<u32, Self::Error> {
        let mut bytes = [0u8; 4];
        self.try_fill_bytes(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn try_next_u64(&mut self) -> core::result::Result<u64, Self::Error> {
        let mut bytes = [0u8; 8];
        self.try_fill_bytes(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> core::result::Result<(), Self::Error> {
        if self.source.fill(dst).is_err() {
            self.failed = true;
            dst.fill(0);
        }
        Ok(())
    }
}

impl<R: RandomSource + ?Sized> rand_core::TryCryptoRng for Rng<'_, R> {}

/// Single-shot base-mode seal. Returns the encapsulated key and the
/// ciphertext with its 16-byte tag.
pub fn seal(
    public_key: &[u8; PUBLIC_KEY_BYTES],
    info: &[u8],
    aad: &[u8],
    plaintext: &[u8],
    rng: &mut (impl RandomSource + ?Sized),
) -> Result<([u8; ENCAPSULATED_BYTES], Vec<u8>)> {
    let recipient =
        <Suite as Kem>::PublicKey::from_bytes(public_key).map_err(|_| Error::InvalidInput)?;
    let mut rng = Rng {
        source: rng,
        failed: false,
    };
    let sealed = hpke::single_shot_seal_with_rng::<ChaCha20Poly1305, HkdfSha256, Suite>(
        &OpModeS::Base,
        &recipient,
        info,
        plaintext,
        aad,
        &mut rng,
    );
    if rng.failed {
        return Err(Error::RandomUnavailable);
    }
    // A small-order recipient key yields an all-zero shared secret, which
    // the KEM refuses: invalid input. Anything else is a provider failure.
    let (encapsulated, ciphertext) = sealed.map_err(|error| match error {
        HpkeError::EncapError | HpkeError::ValidationError => Error::InvalidInput,
        _ => Error::CryptoFailure,
    })?;
    Ok((encapsulated.to_bytes().into(), ciphertext))
}

/// Single-shot base-mode open. Authentication failure and malformed input
/// are [`Error::InvalidCiphertext`]; an internal HPKE failure is
/// [`Error::CryptoFailure`]. No plaintext is returned unless authentic.
pub fn open(
    secret_key: &[u8; SECRET_KEY_BYTES],
    encapsulated: &[u8; ENCAPSULATED_BYTES],
    info: &[u8],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let secret =
        <Suite as Kem>::PrivateKey::from_bytes(secret_key).map_err(|_| Error::InvalidInput)?;
    let encapsulated = <Suite as Kem>::EncappedKey::from_bytes(encapsulated)
        .map_err(|_| Error::InvalidCiphertext)?;
    hpke::single_shot_open::<ChaCha20Poly1305, HkdfSha256, Suite>(
        &OpModeR::Base,
        &secret,
        &encapsulated,
        info,
        ciphertext,
        aad,
    )
    .map(Zeroizing::new)
    .map_err(|error| match error {
        HpkeError::OpenError
        | HpkeError::DecapError
        | HpkeError::ValidationError
        | HpkeError::IncorrectInputLength(..) => Error::InvalidCiphertext,
        _ => Error::CryptoFailure,
    })
}

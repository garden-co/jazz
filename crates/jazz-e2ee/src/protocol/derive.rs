//! Derivations from secrets and keys (`E2EE_PROTOCOL.md` §2.1, §2.9,
//! §2.12).

use zeroize::{Zeroize, Zeroizing};

use crate::agility::{KeyPair, KeySealer, Signer as _};
use crate::mechanisms::{key_seal, signature};
use crate::primitives::hash;

use super::domain::AccountDomain;
use super::error::{ProtocolError, ProtocolResult};
use super::ids;

// Every keyed derivation hashes `UTF8(label) ‖ 0x00 ‖ input` (the input may
// be empty), the same style as the mechanisms' key labels.
const RECOVERY_SEED_LABEL: &[u8] = b"jazz.e2ee.recovery-seed.v1\0";
const RECOVERY_GENERATION_SEED_LABEL: &[u8] = b"jazz.e2ee.recovery-generation-seed.v1\0";
const RECOVERY_KEM_LABEL: &[u8] = b"jazz.e2ee.recovery-kem-key.v1\0";
const RECOVERY_SIGN_LABEL: &[u8] = b"jazz.e2ee.recovery-sign-key.v1\0";
const RECOVERY_ROOT_ID_LABEL: &[u8] = b"jazz.e2ee.recovery-root-id.v1\0";
const ACCOUNT_KEM_LABEL: &[u8] = b"jazz.e2ee.account-kem-key.v1\0";
const LOCAL_DEVICE_CHECK_LABEL: &str = "jazz.e2ee.local-device-check.v1\0";
const RECOVERY_MATERIAL_CHECK_LABEL: &str = "jazz.e2ee.recovery-material-check.v1\0";

/// The prefix of a formatted account or recovery secret.
pub const SECRET_PREFIX: &str = "jazz-auth-v1:";

/// A 32-byte recovery secret `s` (§2.9): a local-first account's account
/// secret, or an externally authenticated account's separate secret. Cleared
/// on drop.
#[derive(Clone, PartialEq, Eq)]
pub struct RecoverySecret(Zeroizing<[u8; 32]>);

impl core::fmt::Debug for RecoverySecret {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("RecoverySecret(..)")
    }
}

const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

impl RecoverySecret {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// `jazz-auth-v1:` followed by 43 unpadded base64url characters.
    pub fn format(&self) -> Zeroizing<String> {
        let mut out = Zeroizing::new(String::with_capacity(SECRET_PREFIX.len() + 43));
        out.push_str(SECRET_PREFIX);
        let mut bits = 0u32;
        let mut count = 0u32;
        for byte in self.0.iter() {
            bits = (bits << 8) | u32::from(*byte);
            count += 8;
            while count >= 6 {
                count -= 6;
                out.push(char::from(BASE64URL[((bits >> count) & 63) as usize]));
            }
        }
        if count > 0 {
            out.push(char::from(BASE64URL[((bits << (6 - count)) & 63) as usize]));
        }
        bits.zeroize();
        out
    }

    /// Parses the formatted secret. Anything but exactly the canonical form
    /// is [`ProtocolError::InvalidRecord`].
    pub fn parse(text: &str) -> ProtocolResult<Self> {
        let invalid = ProtocolError::InvalidRecord("invalid recovery secret");
        let encoded = text.strip_prefix(SECRET_PREFIX).ok_or(invalid)?;
        if encoded.len() != 43 {
            return Err(invalid);
        }
        let mut out = Zeroizing::new([0u8; 32]);
        let mut bits = 0u32;
        let mut count = 0u32;
        let mut written = 0usize;
        for symbol in encoded.bytes() {
            let value = BASE64URL
                .iter()
                .position(|candidate| *candidate == symbol)
                .ok_or(invalid)? as u32;
            bits = (bits << 6) | value;
            count += 6;
            if count >= 8 {
                count -= 8;
                out[written] = (bits >> count) as u8;
                written += 1;
            }
        }
        // 43 symbols carry 258 bits: the last two must be zero.
        let canonical = written == 32 && bits & ((1 << count) - 1) == 0;
        bits.zeroize();
        if canonical {
            Ok(Self(out))
        } else {
            Err(invalid)
        }
    }
}

/// The recovery root derived from a recovery secret for one account domain:
/// its version 8 root ID and its sealing and signing key pairs, of the
/// built-in mechanisms `jazz.wrap.hpke-x25519` v1 and `jazz.sodium.sign` v1
/// the labels are bound to.
#[derive(Debug, Clone)]
pub struct RecoveryKeys {
    pub root_id: String,
    pub sealing: KeyPair,
    pub signing: KeyPair,
}

impl RecoveryKeys {
    /// The root of `secret` for the account domain at account `generation`.
    ///
    /// `seed = BLAKE2b-256(key = s, "jazz.e2ee.recovery-seed.v1" ‖ 00 ‖
    /// UTF8(Dₐ))` at generation 0, and `BLAKE2b-256(key = s,
    /// "jazz.e2ee.recovery-generation-seed.v1" ‖ 00 ‖ UTF8(Dₐ) ‖ 00 ‖
    /// UTF8(decimal generation))` above it, so a reset account can register
    /// a fresh root from the same secret. From the seed, the sealing pair is
    /// HPKE `DeriveKeyPair` of `BLAKE2b-256(key = seed,
    /// "jazz.e2ee.recovery-kem-key.v1" ‖ 00)`, the signing seed
    /// `BLAKE2b-256(key = seed, "jazz.e2ee.recovery-sign-key.v1" ‖ 00)`, and the
    /// root ID the first 16 bytes of `BLAKE2b-256(key = seed,
    /// "jazz.e2ee.recovery-root-id.v1" ‖ 00)` as a version 8 UUID.
    pub fn derive(
        secret: &RecoverySecret,
        account_domain: &AccountDomain,
        generation: u64,
    ) -> ProtocolResult<Self> {
        let domain = account_domain.as_str().as_bytes();
        let seed = if generation == 0 {
            hash::keyed_hash_parts(secret.as_bytes(), &[RECOVERY_SEED_LABEL, domain])
        } else {
            hash::keyed_hash_parts(
                secret.as_bytes(),
                &[
                    RECOVERY_GENERATION_SEED_LABEL,
                    domain,
                    &[0],
                    generation.to_string().as_bytes(),
                ],
            )
        };
        let kem_ikm = hash::keyed_hash(&seed, RECOVERY_KEM_LABEL);
        let sign_seed = hash::keyed_hash(&seed, RECOVERY_SIGN_LABEL);
        let id_bytes = hash::keyed_hash(&seed, RECOVERY_ROOT_ID_LABEL);
        let mut root = [0u8; 16];
        root.copy_from_slice(&id_bytes[..16]);
        Ok(Self {
            root_id: ids::uuid_v8(root),
            sealing: key_seal::Builtin.derive_key_pair(&kem_ikm[..])?,
            signing: signature::Builtin.derive_key_pair(&sign_seed[..])?,
        })
    }
}

/// The input keying material of an account epoch's sealing key pair for
/// `sealer`'s mechanism (§2.12): `BLAKE2b-256(key = K,
/// "jazz.e2ee.account-kem-key.v1" ‖ 00 ‖ H(m))`, where `H(m)` is the
/// mechanism's common envelope header (its ID and version, no payload), so
/// one epoch key never gives two mechanisms the same input.
pub fn account_sealing_ikm(
    sealer: &dyn KeySealer,
    epoch_key: &[u8; 32],
) -> ProtocolResult<Zeroizing<[u8; 32]>> {
    Ok(hash::keyed_hash_parts(
        epoch_key,
        &[ACCOUNT_KEM_LABEL, &sealer.mechanism().header()?],
    ))
}

/// An account epoch's sealing key pair for `sealer`'s mechanism (§2.12):
/// HPKE `DeriveKeyPair` (the mechanism's deterministic derivation) of
/// [`account_sealing_ikm`]. Every device holding the epoch key derives it;
/// nothing private is stored.
pub fn account_sealing_key_pair(
    sealer: &dyn KeySealer,
    epoch_key: &[u8; 32],
) -> ProtocolResult<KeyPair> {
    let ikm = account_sealing_ikm(sealer, epoch_key)?;
    Ok(sealer.derive_key_pair(&ikm[..])?)
}

/// The bytes a new local device seals and signs to check its key pairs
/// (§2.1): `UTF8("jazz.e2ee.local-device-check.v1\0" ‖ scope)`.
pub fn local_device_check(scope: &str) -> Vec<u8> {
    [LOCAL_DEVICE_CHECK_LABEL, scope].concat().into_bytes()
}

/// The bytes recovery material is checked over (§2.9):
/// `UTF8("jazz.e2ee.recovery-material-check.v1\0" ‖ scope ‖ "\0" ‖ rootId)`.
pub fn recovery_material_check(scope: &str, root_id: &str) -> Vec<u8> {
    [RECOVERY_MATERIAL_CHECK_LABEL, scope, "\0", root_id]
        .concat()
        .into_bytes()
}

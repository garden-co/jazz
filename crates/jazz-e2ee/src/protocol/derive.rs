//! Derivations from secrets and keys (`E2EE_PROTOCOL.md` §2.1, §2.9,
//! §2.12).

use zeroize::Zeroizing;

use crate::agility::{KeyPair, KeySealer, Signer as _};
use crate::mechanisms::{key_seal, signature};
use crate::primitives::hash;

use super::domain::AccountDomain;
use super::error::ProtocolResult;
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

pub use jazz_types::identity::DeviceAccountSecret as RecoverySecret;

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

fn recovery_seed(
    secret: &RecoverySecret,
    account_domain: &AccountDomain,
    generation: u64,
) -> Zeroizing<[u8; 32]> {
    let domain = account_domain.as_str().as_bytes();
    if generation == 0 {
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
    }
}

/// Commitment seed for the recovery root of this account and generation.
pub fn recovery_commitment_seed(
    secret: &RecoverySecret,
    account_domain: &AccountDomain,
    generation: u64,
) -> Zeroizing<[u8; 32]> {
    let seed = recovery_seed(secret, account_domain, generation);
    hash::keyed_hash(&seed, b"jazz.e2ee.recovery-commitment-seed.v1\0")
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
        let seed = recovery_seed(secret, account_domain, generation);
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

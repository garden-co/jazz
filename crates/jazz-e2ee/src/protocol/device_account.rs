//! The device-account identity (`E2EE_PROTOCOL.md` §2.3), as
//! `packages/jazz-tools/src/local-first-auth` and Jazz's account registry
//! derive it from the 32-byte account secret:
//!
//! - signing key: the Ed25519 seed `SHA-512(UTF8("jazz-auth-sign-v1") ‖
//!   secret)[0..32]`;
//! - subject: UUIDv5 of the 32-byte public key in the namespace whose 16
//!   bytes are `UTF8("jazz-auth-key-v1")`;
//! - account ID: UUIDv5 of `UTF8("jazz-account-founder-v1") ‖ 0x00 ‖
//!   UTF8(subject)` in the namespace of the application's UUID.
//!
//! A device account's root is signed with this key, and replay checks
//! that the key derives the account ID, so nobody without the secret, the
//! server included, can write or substitute it.

use sha1::Digest as _;
use sha2::Digest as _;

use crate::agility::{KeyPair, PublicKey, Signer as _};
use crate::mechanisms::signature;
use crate::registry::Registry;

use super::derive::RecoverySecret;
use super::domain::AppDomain;
use super::error::ProtocolResult;
use super::signing;

const SIGN_DOMAIN: &[u8] = b"jazz-auth-sign-v1";
/// The subject namespace: the 16 ASCII bytes of `jazz-auth-key-v1`.
const SUBJECT_NAMESPACE: &[u8; 16] = b"jazz-auth-key-v1";
const FOUNDER_LABEL: &[u8] = b"jazz-account-founder-v1\0";

/// Length of a device-account public key: a raw Ed25519 key.
pub const PUBLIC_KEY_BYTES: usize = 32;

fn uuid_v5(namespace: &[u8; 16], name: &[&[u8]]) -> String {
    let mut hasher = sha1::Sha1::new();
    hasher.update(namespace);
    for part in name {
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    super::ids::format_uuid(&bytes)
}

fn parse_uuid(value: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = value.bytes().filter(|byte| *byte != b'-').collect();
    if value.len() != 36 || hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (index, pair) in hex.chunks(2).enumerate() {
        let text = core::str::from_utf8(pair).ok()?;
        out[index] = u8::from_str_radix(text, 16).ok()?;
    }
    Some(out)
}

/// The subject of a device-account public key, or `None` unless it is 32
/// bytes.
pub fn subject(public_key: &[u8]) -> Option<String> {
    (public_key.len() == PUBLIC_KEY_BYTES).then(|| uuid_v5(SUBJECT_NAMESPACE, &[public_key]))
}

/// The Jazz account ID a device-account identity with `subject` founds in the
/// application `app_id` (a UUID), or `None` if `app_id` is not one.
pub fn founder_account_id(app_id: &str, subject: &str) -> Option<String> {
    let namespace = parse_uuid(app_id)?;
    Some(uuid_v5(&namespace, &[FOUNDER_LABEL, subject.as_bytes()]))
}

/// Whether `public_key` is the device-account key of `account_id` in `domain`'s
/// application.
pub fn derives_account(domain: &AppDomain, public_key: &[u8], account_id: &str) -> bool {
    subject(public_key)
        .and_then(|subject| founder_account_id(domain.app_id(), &subject))
        .is_some_and(|derived| derived == account_id)
}

/// The signature key of a device-account public key, under the built-in
/// Ed25519 mechanism its signatures use.
pub fn verifying_key(public_key: &[u8]) -> PublicKey {
    PublicKey {
        mechanism: signature::MECHANISM.into(),
        bytes: public_key.to_vec(),
    }
}

/// Verifies a device-account signature (a `jazz.sodium.sign` v1 envelope).
pub fn verify(
    registry: &Registry,
    public_key: &[u8],
    bytes: &[u8],
    signature: &[u8],
) -> ProtocolResult<bool> {
    signing::verify(registry, &verifying_key(public_key), bytes, signature)
}

/// A device-account identity's signing key, derived from the account secret.
/// The secret is not kept; the key pair is cleared on drop and never
/// printed.
#[derive(Clone)]
pub struct DeviceAccountIdentity {
    signing: KeyPair,
}

impl core::fmt::Debug for DeviceAccountIdentity {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DeviceAccountIdentity")
            .field("public_key", &self.signing.public.bytes)
            .field("signing", &"..")
            .finish()
    }
}

impl DeviceAccountIdentity {
    /// The identity of an account secret (`jazz-auth-v1:` …, parsed as a
    /// [`RecoverySecret`], which is the same 32 bytes for a device
    /// account, §2.9).
    pub fn from_secret(secret: &RecoverySecret) -> ProtocolResult<Self> {
        let mut hasher = sha2::Sha512::new();
        hasher.update(SIGN_DOMAIN);
        hasher.update(secret.as_bytes());
        let mut seed = zeroize::Zeroizing::new([0u8; 64]);
        // Written in place, so no copy of the seed outlives this call.
        hasher.finalize_into((&mut *seed).into());
        Ok(Self {
            signing: signature::Builtin.derive_key_pair(&seed[..32])?,
        })
    }

    /// The raw 32-byte Ed25519 public key.
    pub fn public_key(&self) -> &[u8] {
        &self.signing.public.bytes
    }

    pub fn subject(&self) -> String {
        subject(self.public_key()).expect("an Ed25519 public key is 32 bytes")
    }

    /// The account ID this identity founds in `domain`'s application.
    pub fn account_id(&self, domain: &AppDomain) -> Option<String> {
        founder_account_id(domain.app_id(), &self.subject())
    }

    /// Signs `bytes` with the built-in Ed25519 mechanism.
    pub(crate) fn sign(&self, bytes: &[u8]) -> ProtocolResult<Vec<u8>> {
        signing::sign(&signature::Builtin, &self.signing, bytes)
    }
}

//! Signing and verifying record bytes over the mechanism traits.
//!
//! Verification has three outcomes: valid, invalid (`Ok(false)`: malformed
//! or unauthentic input), or a failure. A key of an unregistered signing
//! mechanism is [`UnsupportedCapability`], never invalid; a provider failure
//! is [`crate::Error::CryptoFailure`], never `Ok(false)` (§0, §7).

use crate::agility::{KeyPair, PublicKey, Signer};
use crate::envelope::Mechanism;
use crate::registry::Registry;

use super::error::{ProtocolError, ProtocolResult, UnsupportedCapability};

/// Signs `bytes` with `signer` and the device's (or epoch's, or recovery
/// root's) key pair.
pub fn sign(signer: &dyn Signer, key: &KeyPair, bytes: &[u8]) -> ProtocolResult<Vec<u8>> {
    Ok(signer.sign(key, bytes)?)
}

/// The capability a signing key of `mechanism` needs, if this registry lacks
/// it.
pub fn signing_support(registry: &Registry, mechanism: Mechanism<'_>) -> ProtocolResult<()> {
    if registry.signer(mechanism).is_ok() {
        Ok(())
    } else if registry.has_signer_id(mechanism.id) {
        Err(ProtocolError::Unsupported(
            UnsupportedCapability::SigningVersion,
        ))
    } else {
        Err(ProtocolError::Unsupported(
            UnsupportedCapability::SigningMechanism,
        ))
    }
}

/// Whether `signature` is a valid signature of `bytes` by `key`. A
/// signature envelope of another mechanism than the key's is invalid.
pub fn verify(
    registry: &Registry,
    key: &PublicKey,
    bytes: &[u8],
    signature: &[u8],
) -> ProtocolResult<bool> {
    signing_support(registry, key.mechanism.as_mechanism())?;
    Ok(registry.verify(key, bytes, signature)?)
}

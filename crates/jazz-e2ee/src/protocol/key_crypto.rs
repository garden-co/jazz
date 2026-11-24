use zeroize::Zeroizing;

use crate::agility::{KeyPair, PublicKey};
use crate::error::Error;
use crate::random::RandomSource;
use crate::registry::{Registry, Suite};

use super::error::{ProtocolError, ProtocolResult, RefusalCode};
use super::signing;

/// A 32-byte secret that is cleared on drop and redacted in debug output.
#[derive(Clone)]
pub struct Key32(Zeroizing<[u8; 32]>);

impl From<Zeroizing<[u8; 32]>> for Key32 {
    fn from(key: Zeroizing<[u8; 32]>) -> Self {
        Self(key)
    }
}

impl core::ops::Deref for Key32 {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl core::fmt::Debug for Key32 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt("..", f)
    }
}

pub(super) const Z32: [u8; 32] = [0; 32];

pub(super) fn refuse<T>(code: RefusalCode) -> ProtocolResult<T> {
    Err(ProtocolError::Refused(code))
}

pub(super) fn provider_check(ok: bool) -> ProtocolResult<()> {
    if ok {
        Ok(())
    } else {
        Err(ProtocolError::ProviderFailure)
    }
}

/// Reading back fresh output must succeed, regardless of what went wrong.
pub(super) fn read_back<T, E>(result: Result<T, E>) -> ProtocolResult<T> {
    result.map_err(|_| ProtocolError::ProviderFailure)
}

pub(super) fn random_key(rng: &mut dyn RandomSource) -> ProtocolResult<Zeroizing<[u8; 32]>> {
    let mut key = Zeroizing::new(Z32);
    rng.fill(&mut key[..])?;
    Ok(key)
}

fn unwrap_key(
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> crate::Result<Zeroizing<[u8; 32]>> {
    registry
        .key_wrapper_for(envelope)?
        .unwrap(key, context, envelope)
        .map_err(Error::after_routing)
}

/// The result of reading a candidate envelope or authenticating its marker.
pub(crate) enum Outcome<T> {
    Ok(T),
    Invalid,
    Unsupported,
}

pub(super) fn classify<T>(result: crate::Result<T>) -> ProtocolResult<Outcome<T>> {
    match result {
        Ok(value) => Ok(Outcome::Ok(value)),
        Err(Error::InvalidCiphertext) => Ok(Outcome::Invalid),
        Err(Error::UnsupportedFormat) => Ok(Outcome::Unsupported),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn unwrap(
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<Outcome<Zeroizing<[u8; 32]>>> {
    classify(unwrap_key(registry, key, context, envelope))
}

pub(super) fn marker(
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<Outcome<()>> {
    Ok(match unwrap(registry, key, context, envelope)? {
        Outcome::Ok(value) if *value == Z32 => Outcome::Ok(()),
        Outcome::Ok(_) | Outcome::Invalid => Outcome::Invalid,
        Outcome::Unsupported => Outcome::Unsupported,
    })
}

pub(super) fn wrap(
    suite: &Suite<'_>,
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    payload: &[u8; 32],
    rng: &mut dyn RandomSource,
    read_back_error: impl FnOnce(Error) -> ProtocolError,
) -> ProtocolResult<Vec<u8>> {
    let wrapped = suite.key_wrap.wrap(key, context, payload, rng)?;
    let unwrapped = unwrap_key(registry, key, context, &wrapped).map_err(read_back_error)?;
    provider_check(*unwrapped == *payload)?;
    Ok(wrapped)
}

pub(super) fn seal(
    suite: &Suite<'_>,
    recipient: &PublicKey,
    context: &[u8],
    key: &[u8; 32],
    rng: &mut dyn RandomSource,
) -> ProtocolResult<Vec<u8>> {
    match suite.seal_to(core::slice::from_ref(recipient), context, key, rng) {
        Err(Error::RecipientUnsupported) => refuse(RefusalCode::UnsupportedRecipient),
        other => Ok(other?),
    }
}

pub(super) fn check_sealed(
    registry: &Registry,
    recipient: &KeyPair,
    context: &[u8],
    envelope: &[u8],
    key: &[u8; 32],
) -> ProtocolResult<()> {
    let opened =
        read_back(registry.open_sealed(core::slice::from_ref(recipient), context, envelope))?;
    provider_check(*opened == *key)
}

pub(super) fn sign(registry: &Registry, key: &KeyPair, bytes: &[u8]) -> ProtocolResult<Vec<u8>> {
    let signer = registry.signer(key.public.mechanism.as_mechanism())?;
    let signature = signing::sign(signer, key, bytes)?;
    provider_check(read_back(signing::verify(
        registry,
        &key.public,
        bytes,
        &signature,
    ))?)?;
    Ok(signature)
}

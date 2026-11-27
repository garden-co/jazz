//! Randomness is always passed in, never taken from global state.
//!
//! Operations that need fresh bytes (nonces, stream headers, ephemeral and new
//! keys) take a [`RandomSource`]. Tests can supply a deterministic source; the
//! `os-random` feature (on by default) provides [`OsRandom`], backed by
//! `getrandom`, which on `wasm32-unknown-unknown` uses the browser's
//! `crypto.getRandomValues`.

use crate::error::Result;

/// A cryptographically secure source of random bytes.
pub trait RandomSource {
    /// Fills `dest` entirely, or fails without partial use of its contents.
    fn fill(&mut self, dest: &mut [u8]) -> Result<()>;
}

impl<R: RandomSource + ?Sized> RandomSource for &mut R {
    fn fill(&mut self, dest: &mut [u8]) -> Result<()> {
        (**self).fill(dest)
    }
}

/// The operating system's secure random source.
#[cfg(feature = "os-random")]
#[derive(Debug, Clone, Copy, Default)]
pub struct OsRandom;

#[cfg(feature = "os-random")]
impl RandomSource for OsRandom {
    fn fill(&mut self, dest: &mut [u8]) -> Result<()> {
        getrandom::fill(dest).map_err(|_| crate::error::Error::RandomUnavailable)
    }
}

pub(crate) fn random_array<const N: usize>(
    rng: &mut (impl RandomSource + ?Sized),
) -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    rng.fill(&mut bytes)?;
    Ok(bytes)
}

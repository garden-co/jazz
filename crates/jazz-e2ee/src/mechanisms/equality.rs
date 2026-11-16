//! Equality token mechanism `jazz.sodium.equality`, version 1
//! (`E2EE_EQUALITY_FORMAT.md`).
//!
//! Deterministic, with no nonce:
//! `D = BLAKE2b-256(key = K, len(H) || H || len(C) || C)`,
//! `T = BLAKE2b-256(key = D, V)`, token `H || T`. Tokens disclose equality
//! and frequency; a match identifies a candidate, not trusted plaintext.

use crate::agility::{EqualityTagger, MechanismImpl};
use crate::context::frame_record;
use crate::envelope::{self, Mechanism};
use crate::error::Result;
use crate::primitives::hash;

pub const MECHANISM: Mechanism<'static> = Mechanism::new("jazz.sodium.equality", 1);

/// The token for `value` under `key` and `context`.
pub fn token(key: &[u8; 32], context: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    let derived = hash::keyed_hash(key, &frame_record(&[&MECHANISM.header()?, context])?);
    envelope::encode(MECHANISM, &*hash::keyed_hash(&derived, value))
}

/// The built-in equality mechanism as an [`EqualityTagger`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Builtin;

impl MechanismImpl for Builtin {
    fn mechanism(&self) -> Mechanism<'static> {
        MECHANISM
    }
}

impl EqualityTagger for Builtin {
    fn token(&self, key: &[u8; 32], context: &[u8], value: &[u8]) -> Result<Vec<u8>> {
        token(key, context, value)
    }
}

//! Pure-Rust primitives with fixed byte-level contracts.
//!
//! XChaCha20-Poly1305, keyed BLAKE2b and Ed25519 match their libsodium 1.0.22
//! counterparts byte for byte; HPKE follows RFC 9180. Formats above this
//! module depend only on these contracts, not on how a primitive is
//! implemented.

pub mod aead;
pub mod hash;
pub mod hpke;
pub mod sign;
pub mod slh_dsa;

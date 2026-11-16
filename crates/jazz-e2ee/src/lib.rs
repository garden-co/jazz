//! Jazz end-to-end encryption: wire formats and pure-Rust primitives.
//!
//! This crate does no I/O and holds no global state. Every operation that
//! needs randomness takes a [`RandomSource`]. It implements the formats
//! specified in `crates/jazz/SPEC/E2EE_*_FORMAT.md` byte for byte with
//! maintained pure-Rust crates (RustCrypto, dalek, `hpke`), so it builds
//! unchanged for native targets and `wasm32-unknown-unknown`.
//!
//! - [`envelope`] and [`context`]: the common envelope and canonical context.
//! - [`primitives`]: XChaCha20-Poly1305, keyed BLAKE2b and Ed25519 (each
//!   matching libsodium) and HPKE (RFC 9180).
//! - [`mechanisms`]: the built-in cell, key wrapping, HPKE key sealing,
//!   signature, equality and stream mechanisms; the stream mechanism is
//!   RustCrypto's STREAM construction over XChaCha20-Poly1305.
//! - [`records`]: stored cell and stream records and their contexts.
//! - [`agility`] and [`registry`]: one trait per role, keys that name their
//!   mechanism, routing by envelope header and per-role selection, so a
//!   custom mechanism plugs in beside the built-ins.
//! - [`protocol`]: the protocol's records, signed bytes, derived IDs and
//!   account replay, as pure functions over ledger rows.
//! - `conformance` (feature `conformance`): the test kit every mechanism
//!   implementation runs.
//!
//! Secrets this crate returns or derives are wrapped in [`Zeroizing`] and
//! cleared on drop.

pub mod agility;
#[cfg(feature = "conformance")]
pub mod conformance;
pub mod context;
pub mod envelope;
mod error;
pub mod mechanisms;
pub mod primitives;
pub mod protocol;
mod random;
pub mod records;
pub mod registry;

pub use error::{Error, Result};
#[cfg(feature = "os-random")]
pub use random::OsRandom;
pub use random::RandomSource;
pub use zeroize::Zeroizing;

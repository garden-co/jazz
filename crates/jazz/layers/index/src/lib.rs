//! Specialised index kinds (SPEC 20).
//!
//! Everything here is a pure function of its inputs, with no storage and no
//! SQL, so every node (native, browser WASM, React Native, Durable Objects)
//! computes the same terms, impacts, cells and scores (`INV-IX-1`). The
//! SQLite side (shadow tables, triggers, draining, compilation) lives in
//! `jazz-db`.

#[cfg(feature = "geo")]
pub mod geo;
#[cfg(feature = "text")]
pub mod text;
#[cfg(feature = "vector")]
pub mod vector;

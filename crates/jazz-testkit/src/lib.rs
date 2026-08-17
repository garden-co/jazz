//! Reusable public Jazz test infrastructure.
//!
//! The featureless base intentionally contains only transport helpers that can
//! support Jazz's direct engine contracts without enabling server, storage, or
//! compression capabilities. Public client/server scenarios use the default
//! `scenarios` feature.

pub mod duplex_transport;

#[cfg(feature = "scenarios")]
mod permissions;
#[cfg(feature = "scenarios")]
mod scenarios;
#[cfg(feature = "scenarios")]
pub use scenarios::*;

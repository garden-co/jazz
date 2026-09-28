#![warn(missing_docs)]
#![allow(
    clippy::clone_on_copy,
    clippy::collapsible_if,
    clippy::enum_variant_names,
    clippy::for_kv_map,
    clippy::large_enum_variant,
    clippy::manual_unwrap_or,
    clippy::manual_unwrap_or_default,
    clippy::needless_borrow,
    clippy::too_many_arguments,
    clippy::type_complexity,
    async_fn_in_trait
)]

//! The bottom layer of Jazz: wire-stable identifiers, logical time, object and
//! identity types, and a few process-wide helpers. It depends on nothing else
//! in Jazz. The `jazz` crate re-exports every module here under its old path,
//! so `jazz::ids::RowUuid` and `crate::ids::RowUuid` inside Jazz keep working.

/// Re-export of the underlying groove crate.
pub use groove;

/// Shared, fail-closed state for authority-issued authorization-scope receipts.
pub mod account_registry;
/// Application identifiers.
#[allow(missing_docs)]
pub mod app_id;
/// Diagnostic environment switches, read once per process.
#[doc(hidden)]
pub mod debug_env;
/// Authenticated principal identity helpers.
#[allow(missing_docs)]
pub mod identity;
/// Wire-stable identifiers.
pub mod ids;
/// Driver for ready-immediate thread-affine futures.
pub mod local_executor;
/// Object, branch and query-result identifiers.
#[allow(missing_docs)]
pub mod object;
/// Canonical, whole-input postcard decoding.
pub mod postcard_exact;
/// Logical time and sequence counters.
pub mod time;

/// Bounded metadata-only delivery diagnostics for native acceptance failures.
#[doc(hidden)]
#[cfg(any(test, feature = "testing"))]
pub mod delivery_diagnostics;
#[doc(hidden)]
#[cfg(any(test, feature = "testing"))]
pub mod legacy_test_future;

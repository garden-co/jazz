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

//! Jazz's node layer: the storage-backed node state machine and the per-peer
//! sync state that drives it. It sits directly above `jazz-engine`. The
//! `jazz` crate re-exports it as `jazz::node` and `jazz::peer`, their old
//! paths.

/// Re-export of the underlying groove crate.
pub use groove;

// The layers below, under the paths this crate's code already uses.
#[cfg(test)]
use jazz_model::test_public_schema;
use jazz_model::{model, query, schema, tx};
#[cfg(test)]
use jazz_protocol::wire;
use jazz_protocol::{authorization_scope, protocol, protocol_limits, storage_codec_profile};
#[cfg(test)]
use jazz_types::account_registry;
#[cfg(any(test, feature = "testing"))]
use jazz_types::delivery_diagnostics;
use jazz_types::{debug_env, ids, local_executor, object, time};

/// Storage-backed node implementation and local API.
pub mod node;
/// Per-peer sync state and metrics.
pub mod peer;

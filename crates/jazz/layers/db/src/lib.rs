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

//! Jazz's database layer: the thread-affine `Db` facade over the node, the
//! structured result tree, and the binary row codec the bindings share. It
//! sits directly above `jazz-node`. The `jazz` crate re-exports its modules
//! under their old paths (`jazz::db`, `jazz::binding_codec`, ...).

/// Re-export of the underlying groove crate.
pub use groove;

// The layers below, under the paths this crate's code already uses.
#[cfg(test)]
use jazz_model::row_input;
use jazz_model::{model, query, schema, tx};
use jazz_node::{node, peer};
use jazz_protocol::{authorization_scope, protocol, protocol_limits, wire};
#[cfg(test)]
use jazz_types::account_registry;
#[cfg(any(test, feature = "testing"))]
use jazz_types::delivery_diagnostics;
use jazz_types::{debug_env, ids, local_executor, object, time};

/// Shared binary row payload contract for the NAPI and WASM bindings.
pub mod binding_codec;
/// Disabled-by-default counters used by the native cold-settle attribution bench.
#[cfg(feature = "cold-settle-attribution")]
pub mod cold_settle_attribution;
/// High-level thread-affine database facade.
pub mod db;
/// Host-facing exclusive lifecycle for foreground transaction-node identities.
pub mod foreground_node_lease;
pub(crate) mod positional_order;
/// Canonical recursive structured query-result boundary types.
pub mod result_tree;

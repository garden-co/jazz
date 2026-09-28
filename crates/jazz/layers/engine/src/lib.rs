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

//! Jazz's query-engine layer: lowers queries, relations and policies into
//! groove IVM programs. It sits directly above `jazz-protocol`. The node
//! layer uses it internally as `node::query_engine`, its old path, and
//! re-exports the items the public API needs from `jazz::node`.

/// Re-export of the underlying groove crate.
pub use groove;

// The layers below, under the paths this crate's code already uses.
use jazz_model::{model, query, schema, tx};
use jazz_protocol::protocol;
#[cfg(test)]
use jazz_types::legacy_test_future;
use jazz_types::{debug_env, ids, time};

/// Unified query-engine vocabulary and lowering.
pub mod query_engine;

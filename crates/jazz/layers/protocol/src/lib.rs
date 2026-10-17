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

//! Jazz's protocol layer: sync messages, versioned wire frames, protocol
//! admission limits, authorization scopes and the persistent-codec inventory.
//! It sits directly above `jazz-model`. The `jazz` crate re-exports every
//! module here under its old path, so `jazz::protocol::SyncMessage` and
//! `crate::protocol::SyncMessage` inside Jazz keep working.

/// Re-export of the underlying groove crate.
pub use groove;

// The layers below, under the paths this crate's code already uses.
use jazz_model::{model, query, schema, tx};
#[cfg(test)]
use jazz_types::account_registry;
use jazz_types::{debug_env, ids, object, postcard_exact, time};

pub mod authorization_scope;
/// Simulation-first sync and local event messages.
pub mod protocol;
/// Protocol admission and semantic size limits.
pub mod protocol_limits;
pub mod storage_codec_profile;
/// Versioned transport frames around the semantic sync protocol.
pub mod wire;

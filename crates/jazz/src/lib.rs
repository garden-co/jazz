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

//! Jazz is the local-first database. The public reading order is the
//! [`db`] facade (SQLite storage, query execution, policies and sync) ->
//! [`schema`] / [`query`] (Jazz's schema, query and policy model) ->
//! [`protocol`] (schema versions, lenses and branch views) -> [`records`]
//! (logical values). Start with `jazz/API.md` for the facade.

/// Poll ready-immediate database futures without an async runtime.
pub use db::block_on;
pub use jazz_db::binding_args;
pub use jazz_db::binding_codec;
pub use jazz_db::binding_host;
pub use jazz_db::db;
pub use jazz_db::foreground_node_lease;
pub use jazz_db::format;
#[cfg(feature = "testing")]
pub use jazz_db::full_materializations_for_test;
pub use jazz_db::result_tree;
pub use jazz_db::sync_codec;
pub use jazz_model::model;
pub use jazz_model::query;
pub use jazz_model::row_input;
pub use jazz_model::schema;
pub use jazz_protocol::authorization_scope;
pub use jazz_protocol::protocol;
pub use jazz_protocol::protocol_limits;
pub use jazz_types::account_registry;
pub use jazz_types::app_id;
pub use jazz_types::column_schema;
pub use jazz_types::identity;
pub use jazz_types::ids;
pub use jazz_types::large_values;
pub use jazz_types::local_executor;
pub use jazz_types::object;
pub use jazz_types::postcard_exact;
pub use jazz_types::records;
pub use jazz_types::time;
/// Public runtime and data-model support APIs formerly provided by jazz-tools.
// The tools API was a separate crate before consolidation and intentionally
// retains its existing documentation policy.
#[allow(missing_docs)]
pub mod tools;
pub use jazz_model::tx;
pub use jazz_protocol::wire;

/// Bounded metadata-only delivery diagnostics for native acceptance failures.
#[doc(hidden)]
#[cfg(any(test, feature = "testing"))]
pub use jazz_types::delivery_diagnostics;

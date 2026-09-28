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

//! Jazz's data-model layer: the internal schema, the query AST, transaction
//! vocabulary and the public schema model. It sits directly above
//! `jazz-types`. The `jazz` crate re-exports every module here under its old
//! path, so `jazz::schema::JazzSchema` and `crate::schema::JazzSchema` inside
//! Jazz keep working.

/// Re-export of the underlying groove crate.
pub use groove;

// The layer below, under the paths this crate's code already uses.
use jazz_types::{account_registry, ids, object, postcard_exact, time};

/// Public data model: schema builders, values, policies and lenses.
#[allow(missing_docs)]
pub mod model;
/// Pure query AST, validation, canonicalization, and ids.
pub mod query;
/// Jazz schema and storage lowering.
pub mod schema;
/// Transaction, fate, and history vocabulary.
pub mod tx;

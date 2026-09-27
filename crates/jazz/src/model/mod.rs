//! The public data model: schema builders, values, policies, lenses and the
//! canonical conversion between the public schema and Jazz's internal schema.
//!
//! These modules are re-exported at their historical `crate::tools::` paths.
//! They live here, below `tools`, so that `schema`, `protocol` and the engine
//! can use them without depending on the client-facing API module.

#[doc(hidden)]
pub mod admin_catalogue_row_format;
pub mod branch;
pub mod metadata;
pub mod policy_claims;
pub(crate) mod policy_directory;
pub(crate) mod public_api;
pub mod public_schema;
#[doc(hidden)]
pub mod public_schema_convert;
pub mod schema_lens;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;
pub mod transaction;

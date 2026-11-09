//! Shared server state and optional native hosting.
mod core;
mod thin;

#[cfg(feature = "native")]
pub(crate) use core::SESSION_QUEUE_BUDGET;
pub use core::THIN_INGRESS_SERVER_BUDGET;
use core::*;
pub use core::{
    AbiBytes, CatalogueApplyError, Owner as ServerCore, ServerSession, SessionInput, ThinIngress,
    WireIngress, catalogue_schema,
};

#[cfg(feature = "native")]
pub mod alpha59_alpha60;
#[cfg(feature = "native")]
pub mod auth_admission;
#[cfg(feature = "native")]
mod host;
#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use host::*;

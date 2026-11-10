//! Shared server state and optional native hosting.
pub mod accounts;
pub mod catalogue;
mod core;
pub mod deployment;
mod permissions_validation;
pub mod session_admission;
mod thin;
pub(crate) mod thin_input;
pub mod websocket;

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

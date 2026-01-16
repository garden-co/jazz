//! Groove sync server with hyper HTTP framework.
//!
//! This crate provides a hyper-based implementation of the sync server,
//! running single-threaded for simplicity and Rc compatibility.
//!
//! - HTTP handlers for sync endpoints
//! - Server configuration and JWT authentication
//! - User provisioning
//! - Native sync driver for client-side sync

pub mod config;
mod handlers;
mod hyper_env;
pub mod native_driver;
pub mod user_provisioning;

pub use config::*;
pub use handlers::*;
pub use native_driver::NativeSyncDriver;
pub use user_provisioning::*;

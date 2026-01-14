//! Groove sync server with axum HTTP framework.
//!
//! This crate provides an axum-based implementation of the sync server,
//! including:
//! - `AxumServerEnv`: Implementation of `ServerEnv` for axum
//! - HTTP handlers for sync endpoints
//! - Router configuration
//! - Server configuration and JWT authentication

mod axum_env;
pub mod config;
mod handlers;
pub mod user_provisioning;

pub use axum_env::*;
pub use config::*;
pub use handlers::*;
pub use user_provisioning::*;

//! Groove sync server with axum HTTP framework.
//!
//! This crate provides an axum-based implementation of the sync server,
//! including:
//! - `AxumServerEnv`: Implementation of `ServerEnv` for axum
//! - HTTP handlers for sync endpoints
//! - Router configuration

mod axum_env;
mod handlers;

pub use axum_env::*;
pub use handlers::*;

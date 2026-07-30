//! Authentication middleware for the Jazz server.
//!
//! Provides Axum extractors for:
//! - JWT authentication (frontend clients)
//! - Backend session impersonation (backend clients with secret)
//! - Admin authentication (for schema/policy sync)

pub mod auth;

pub use auth::AuthConfig;

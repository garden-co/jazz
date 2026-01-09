//! Sync module for client-server synchronization.
//!
//! This module implements a query-based sync protocol where:
//! - Clients subscribe to SQL queries
//! - Server executes queries with ReBAC permissions to determine which objects to sync
//! - Both peers track assumed known state to send only deltas
//! - Transport: HTTP POST (client→server) + SSE (server→client)
//!
//! # Crate Organization
//!
//! - **groove** (this crate): Core sync logic and traits
//!   - `SyncClient<E: ClientEnv>`: Generic sync client
//!   - `SyncServer<E: Environment>`: Server-side sync logic (with `sync-server` feature)
//!   - `ClientEnv`, `ServerEnv`: Transport abstraction traits
//!
//! - **groove-server**: Axum-based HTTP server implementation
//!   - `AxumServerEnv`: ServerEnv implementation for axum
//!   - HTTP handlers and router

mod client;
mod env;
mod negotiation;
mod protocol;

#[cfg(feature = "sync-server")]
mod server;

#[cfg(all(test, feature = "sync-server"))]
pub mod test_harness;

pub use client::*;
pub use env::*;
pub use negotiation::*;
pub use protocol::*;

#[cfg(feature = "sync-server")]
pub use server::*;

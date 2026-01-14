//! Sync module for client-server synchronization.
//!
//! This module implements a query-based sync protocol where:
//! - Clients subscribe to SQL queries
//! - Server executes queries with ReBAC permissions to determine which objects to sync
//! - Both peers track assumed known state to send only deltas
//! - Transport: HTTP POST (client→server) + SSE (server→client)
//!
//! # Architecture
//!
//! The sync system is built around `SyncedNode<R, E>` which wraps a `LocalNode`:
//!
//! - **LocalNode**: Pure storage, no async concerns
//! - **SyncedNode**: Adds sync capabilities with:
//!   - `UpstreamServers`: Connections to servers we sync TO
//!   - `ConnectedClients`: Sessions from clients that sync FROM us
//!   - `WriteBuffer`: Batching/debouncing for upstream pushes
//!
//! # Crate Organization
//!
//! - **groove** (this crate): Core sync logic and traits
//!   - `SyncedNode<R, E>`: LocalNode with sync capabilities
//!   - `Runtime`: Trait for async task spawning (TokioRuntime, WasmRuntime)
//!   - `ClientEnv`: Transport abstraction for upstream connections
//!
//! - **groove-server**: Axum-based HTTP server implementation
//!   - HTTP handlers and router

mod client;
mod env;
mod negotiation;
mod protocol;
mod runtime;
mod synced_node;

#[cfg(all(feature = "sync-server", not(target_arch = "wasm32")))]
mod server;

#[cfg(all(feature = "sync-server", not(target_arch = "wasm32")))]
pub mod test_harness;

pub use client::*;
pub use env::*;
pub use negotiation::*;
pub use protocol::*;
pub use runtime::*;
pub use synced_node::*;

#[cfg(all(feature = "sync-server", not(target_arch = "wasm32")))]
pub use server::*;

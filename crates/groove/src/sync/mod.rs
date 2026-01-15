//! Sync module for client-server synchronization.
//!
//! This module implements an object-based sync protocol where:
//! - Clients connect to upstream servers and subscribe to queries
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
//! **Important**: The sync layer knows NOTHING about databases, SQL, schemas, or tables.
//! It operates purely at the object/commit level:
//! - Objects marked as `node_private` in their metadata are never synced
//! - Object metadata is passed through opaquely (BTreeMap<String, String>)
//! - Higher layers (e.g., Database) observe incoming objects via callbacks
//!
//! # Layer Separation
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  Database Layer (SQL, schema, queries)                  │
//! │  - Marks objects with metadata (type, priority, etc.)   │
//! │  - Observes synced objects via on_objects_received      │
//! └─────────────────────────────────────────────────────────┘
//!                           │ uses
//!                           ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │  Sync Layer (this module)                               │
//! │  - Wraps LocalNode for sync capabilities                │
//! │  - Respects node_private flag                           │
//! │  - Passes metadata through opaquely                     │
//! └─────────────────────────────────────────────────────────┘
//!                           │ uses
//!                           ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │  ObjectStore (LocalNode)                                │
//! │  - Pure storage with object metadata                    │
//! └─────────────────────────────────────────────────────────┘
//! ```
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

mod env;
mod negotiation;
mod protocol;
mod runtime;
mod shared;
mod synced_node;

#[cfg(not(target_arch = "wasm32"))]
mod server;

#[cfg(not(target_arch = "wasm32"))]
pub mod test_harness;

pub use env::*;
pub use negotiation::*;
pub use protocol::*;
pub use runtime::*;
pub use shared::*;
pub use synced_node::*;

#[cfg(not(target_arch = "wasm32"))]
pub use server::*;

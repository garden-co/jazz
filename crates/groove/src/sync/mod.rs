//! Sync module for client-server synchronization.
//!
//! This module implements a runtime-less sync architecture using explicit
//! inboxes/outboxes instead of async event loops.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        DRIVER                                │
//! │  (Platform-specific: WasmSyncDriver, NativeSyncDriver)      │
//! │                                                              │
//! │  • Receives I/O events (SSE, HTTP responses, timers)        │
//! │  • Puts events into INBOXES                                  │
//! │  • Calls pass() on SyncEngine                                │
//! │  • Takes actions from OUTBOXES                               │
//! └──────────────────────────┬──────────────────────────────────┘
//!                            │
//!                     pass() │ (synchronous)
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SYNC ENGINE                               │
//! │  (Pure synchronous state machine)                           │
//! │                                                              │
//! │  LocalNode + UpstreamState                                  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Types
//!
//! - `SyncEngine`: The runtime-less state machine
//! - `Inboxes`: Events flowing into the engine
//! - `Outboxes`: Actions for the driver to execute
//! - `SyncServer`: Server-side sync handling

mod engine;
mod env;
mod memory_storage;
mod negotiation;
mod protocol;

#[cfg(test)]
pub mod test_driver;

// Re-export MemoryStorage for drivers
pub use memory_storage::MemoryStorage;

#[cfg(not(target_arch = "wasm32"))]
mod server;

#[cfg(all(
    feature = "jwt-auth",
    feature = "sync-server",
    not(target_arch = "wasm32")
))]
pub mod jwt;

// Engine types (runtime-less state machine)
// Note: QueryId not exported to avoid conflict with server::QueryId
pub use engine::{
    ConnectionEvent, ConnectionEventKind, ConnectionState, Inboxes, LocalWriteEvent, Notification,
    OutboundRequest, Outboxes, PendingWrite, PushResponseEvent, SseInboxEvent, StorageRequest,
    StorageResponse, StreamAction, SubscribeRequestEvent, SubscriptionChunkRequest,
    SubscriptionState, SyncConfig, SyncEngine, TickEvent, TimerId, TimerPurpose, TimerRequest,
    UpstreamId, UpstreamState,
};

// Protocol types
pub use negotiation::*;
pub use protocol::*;

// Server environment (non-wasm only)
#[cfg(not(target_arch = "wasm32"))]
pub use env::*;

#[cfg(not(target_arch = "wasm32"))]
pub use server::*;

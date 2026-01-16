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
//! - `SyncServer`: Server-side sync handling (unchanged)

mod engine;
mod env;
mod negotiation;
mod protocol;

// Legacy async modules (non-wasm only, used by test_harness)
#[cfg(not(target_arch = "wasm32"))]
mod runtime;
#[cfg(not(target_arch = "wasm32"))]
mod shared;
#[cfg(not(target_arch = "wasm32"))]
mod synced_node;

#[cfg(not(target_arch = "wasm32"))]
mod server;

#[cfg(not(target_arch = "wasm32"))]
pub mod test_harness;

#[cfg(all(
    feature = "jwt-auth",
    feature = "sync-server",
    not(target_arch = "wasm32")
))]
pub mod jwt;

// New engine types (runtime-less state machine)
pub use engine::{
    ConnectionEvent, ConnectionEventKind, ConnectionState, Inboxes, LocalWriteEvent, Notification,
    OutboundRequest, Outboxes, PendingWrite, PushResponseEvent, SseInboxEvent, StreamAction,
    SubscribeRequestEvent, SubscriptionState, SyncEngine, TickEvent, TimerId, TimerPurpose,
    TimerRequest,
};

// On WASM, export engine types directly (no conflicts since synced_node isn't compiled)
#[cfg(target_arch = "wasm32")]
pub use engine::{SyncConfig, UpstreamId, UpstreamState};

// On non-WASM, use aliases to avoid conflicts with synced_node types
#[cfg(not(target_arch = "wasm32"))]
pub use engine::SyncConfig as EngineSyncConfig;
#[cfg(not(target_arch = "wasm32"))]
pub use engine::UpstreamState as EngineUpstreamState;
#[cfg(not(target_arch = "wasm32"))]
pub use engine::UpstreamId as EngineUpstreamId;

// Environment and protocol
pub use env::*;
pub use negotiation::*;
pub use protocol::*;

// Legacy async types (non-wasm only, used by test_harness)
// TODO: Remove once test_harness is migrated to driver-based testing
#[cfg(not(target_arch = "wasm32"))]
pub use runtime::*;
#[cfg(not(target_arch = "wasm32"))]
pub use shared::*;
#[cfg(not(target_arch = "wasm32"))]
pub use synced_node::{
    OnObjectsReceivedCallback, PendingWrites, SyncConfig, SyncedNode, UpstreamId, UpstreamServer,
    UpstreamServers, UpstreamState, UpstreamSubscription, WriteBuffer, push_object_standalone,
    run_upstream_event_loop,
};

#[cfg(not(target_arch = "wasm32"))]
pub use server::*;

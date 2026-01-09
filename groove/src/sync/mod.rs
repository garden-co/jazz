//! Sync module for client-server synchronization.
//!
//! This module implements a query-based sync protocol where:
//! - Clients subscribe to SQL queries
//! - Server executes queries with ReBAC permissions to determine which objects to sync
//! - Both peers track assumed known state to send only deltas
//! - Transport: HTTP POST (client→server) + SSE (server→client)

mod client;
mod negotiation;
mod protocol;

#[cfg(feature = "sync-server")]
mod server;

pub use client::*;
pub use negotiation::*;
pub use protocol::*;

#[cfg(feature = "sync-server")]
pub use server::*;

//! Native HTTP/WebSocket server adapter for Jazz's semantic runtime.
//!
//! Without the default `native` feature only the host-independent engine
//! side builds (`runtime`'s engine and `portable`): what a Cloudflare
//! Durable Object host (jazz-wasm) runs on wasm32.

#[cfg(feature = "native")]
mod kv;
#[cfg(feature = "native")]
pub mod middleware;
// Shared with the Durable Object host (jazz-wasm); not a stable API.
#[doc(hidden)]
pub mod portable;
#[cfg(feature = "native")]
pub mod profiling;
#[cfg(feature = "native")]
pub mod resources;
pub mod runtime;
#[cfg(feature = "native")]
pub mod s3;
#[cfg(feature = "native")]
pub mod server;
#[cfg(feature = "native")]
pub mod storage_cache;
#[cfg(feature = "native")]
mod tcp;
#[cfg(feature = "native")]
pub mod vercel_blob;

#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use native::*;

//! Shared server runtime with an optional native HTTP/WebSocket host.

#[cfg(feature = "native")]
mod kv;
#[cfg(feature = "native")]
pub mod middleware;
#[cfg(feature = "native")]
mod native;
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
pub use native::*;

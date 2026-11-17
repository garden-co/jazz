//! Code every host of a Jazz core runs, whatever carries its sockets and
//! store: the native server (`crate::server`) and a Cloudflare Durable
//! Object (jazz-wasm). Nothing here depends on threads, a tokio runtime,
//! the network, the file system or the clock, so it compiles for wasm32.
//! Each host keeps its own I/O around it.

pub mod admission;
pub mod session;
pub mod websocket_codec;

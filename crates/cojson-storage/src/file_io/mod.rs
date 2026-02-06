//! Platform-agnostic file I/O abstractions.
//!
//! This module provides traits and implementations for file I/O that work
//! across different platforms:
//!
//! - **std_fs**: Standard filesystem (Node.js, React Native)
//! - **memory**: In-memory filesystem (Cloudflare Workers, testing)
//! - **opfs**: Origin Private File System (browsers, requires WASM)

mod traits;
pub use traits::*;

mod memory;
pub use memory::InMemoryFileIO;

#[cfg(not(target_arch = "wasm32"))]
mod std_fs;
#[cfg(not(target_arch = "wasm32"))]
pub use std_fs::StdFileIO;

// OPFS implementation would go here for WASM targets
// #[cfg(target_arch = "wasm32")]
// mod opfs;
// #[cfg(target_arch = "wasm32")]
// pub use opfs::OpfsFileIO;

//! Transitional shim for the shared binary row codec.
//!
//! The codec now lives in `crate::row_format`, but a wide surface still imports
//! it through `query_manager::encoding`. Re-export it here while the rest of the
//! codebase migrates to the neutral module path.

pub use crate::row_format::*;

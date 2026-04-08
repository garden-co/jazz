//! Transitional alias for the row-history subsystem.
//!
//! The long-term shape is for this module to own the row-history semantic
//! types and reducer logic directly. For now it re-exports the existing
//! implementation so callers can start moving to the clearer name.

pub use crate::row_regions::*;

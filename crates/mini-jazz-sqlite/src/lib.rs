//! Experimental SQLite-backed Jazz core.
//!
//! This crate contains a small executable prototype of the SQLite-backed design.

pub mod harness;
pub mod model;
pub mod storage;

pub fn crate_marker() -> &'static str {
    "mini-jazz-sqlite"
}

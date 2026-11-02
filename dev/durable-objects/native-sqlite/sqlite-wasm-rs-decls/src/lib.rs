//! The `sqlite3_*` declarations rusqlite calls, with no SQLite behind them.
//! See this crate's Cargo.toml.
#![no_std]
#![allow(clippy::missing_safety_doc)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[rustfmt::skip]
#[allow(clippy::type_complexity)]
mod bindings;

pub use bindings::*;

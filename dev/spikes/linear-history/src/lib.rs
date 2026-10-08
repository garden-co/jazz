//! Measurement spike: Core-sequenced linear row history.
//!
//! This crate is **not** a Jazz subsystem and its byte layouts are **not**
//! durable contracts. It exists to measure what Jazz's storage/ingest path
//! would cost if row history were a linear, authority-sequenced log instead of
//! a per-row version DAG with parents, merge versions, and pending edges.
//!
//! Model (see `README.md` for the full rationale):
//!
//! - Core assigns one strictly increasing [`Seq`] per accepted transaction.
//!   Every row written by that transaction gets a full post-image in history
//!   at `(table, row, seq)`; a separate current row is overwritten in place.
//! - Writes carry no parents. Ordinary columns merge by per-column LWW
//!   [`Stamp`]. Columns with a [`Strategy::ThreeWay`] merge function receive
//!   `(base, ours = current at Core, theirs = authored value)`, where `base`
//!   is either a history snapshot ([`BaseRef::AtSeq`]) or shipped inline
//!   ([`BaseRef::Inline`]) when the author's base was its own unconfirmed
//!   write.
//! - Snapshot reads at a cut `S` use the current row when `row.seq <= S`, and
//!   otherwise one reverse seek into history.
//! - Exclusive transactions validate optimistically against the per-table
//!   change log in `(base, now]`; no query is evaluated at the base cut.
//! - Clients keep confirmed rows plus a pending transaction log that is
//!   replayed over confirmed state (no cascade protocol).

pub mod authority;
pub mod client;
pub mod codec;
pub mod merges;
pub mod model;
pub mod storage;

pub use authority::{Authority, AuthorityOptions, RowRewind, SnapshotStrategy};
pub use client::{Client, TxBuilder};
pub use model::*;

//! Incremental-view-maintenance engine over ordered storage.
//!
//! `groove` is the row-store and IVM layer that Jazz lowers onto. At the
//! bottom, [`storage`] defines the ordered key/value seam and the RocksDB
//! adapter. [`records`] owns compact row descriptors and binary encoding.
//! [`schema`] describes SQL-ish tables, columns, primary keys, and indices.
//! [`queries`] defines the query AST accepted by the public facade. [`ivm`]
//! plans those queries into a shared graph and runs synchronous ticks over base
//! table deltas. [`db`] ties the pieces together as the schema-aware facade.
//!
//! Start reading at [`db::Database`] for the external API, then follow
//! [`ivm::planner`] for query lowering and [`ivm::runtime`] for the tick loop.

pub mod chunks;
/// Disabled-by-default counters used by the cold-settle attribution bench.
#[cfg(feature = "cold-settle-attribution")]
pub mod cold_settle_attribution;
pub mod db;
pub mod ivm;
pub mod large_values;
pub mod queries;
pub mod records;
pub mod schema;
pub mod storage;

pub use internment::Intern;

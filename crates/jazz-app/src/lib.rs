//! A typed Rust client for a Jazz app, opened from its `jazz.app.json`.
//!
//! Bindings generated from `jazz.app.json` (`jazz-tools generate`, or
//! `jazz_codegen` in a `build.rs`) give each table a row struct, an insert
//! struct, an update struct and a module of typed columns, relations and
//! queries, and an `open` that embeds the app's schema. This crate is what
//! they build on: the [`Jazz`] handle and the traits they implement.
//!
//! ```ignore
//! let jazz = jazz_app::open(config).await?;            // generated
//! let open = jazz.all(todos::query().filter(todos::done().eq(false))).await?;
//! let todo = jazz.insert(NewTodo { title: "Write docs".into(), ..Default::default() }).await?;
//! let mut changes = jazz.subscribe(todos::query()).await?; // a futures::Stream
//! ```
//!
//! An end user's client signs in through the app's own backend
//! ([`EndUser`]), with the app's bearer.
//!
//! Not supported yet: connecting to a core directly as an end user (with a
//! JWT the core verifies), local-first (device) accounts, includes (nested related rows in one result), reads inside
//! transactions.

mod app;
mod backend;
mod config;
mod edit;
mod end_user;
mod error;
mod handle;
mod large;
mod query;
mod subscription;
mod table;
mod transaction;
mod value;
mod writes;

pub use app::AppFiles;
pub use config::{Backend, Config, Storage};
pub use edit::TextColumn;
pub use end_user::{EndUser, TokenFuture};
pub use error::{Error, Result};
pub use handle::Jazz;
pub use large::{JsonColumn, LargeColumn, Page, Splice};
pub use query::{Column, Condition, Many, One, Order, Query, created_at, id, updated_at};
pub use subscription::{Added, Delta, Removed, Rows, Subscription, Updated};
pub use table::{Fields, Implementations, Insert, Table, Update};
pub use transaction::Transaction;
pub use value::{ColumnValue, Json, Timestamp};

pub use jazz::account_registry::AccountId;
pub use jazz::tools::{AppId, DurabilityTier, ObjectId, ReadTier};

/// The `jazz` crate, for generated code and [`Jazz::with_client`].
#[doc(hidden)]
pub use jazz;

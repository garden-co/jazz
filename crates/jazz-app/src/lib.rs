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
//! Related rows come with a query's rows through `include` (a tuple per
//! row: `(Todo, Option<Project>)`), and `select` reads only some columns
//! (a [`Partial`] row); see [`Query::include`] and [`Query::select`].
//!
//! [`Query::hop_to`] reads the rows a relation reaches, [`Query::gather`]
//! rows recursively and [`Query::union`] several queries' rows; a table's
//! `history()` reads its rows' accepted versions ([`Version`]) and
//! [`Query::include_deleted`] reads deleted rows too.
//!
//! Not supported yet: connecting to a sync server as an end user (with a
//! JWT), reads inside transactions, subscribing to history, and partial
//! reads of large values.

mod app;
mod backend;
mod config;
mod error;
mod handle;
mod history;
mod include;
mod query;
mod relation;
mod subscription;
mod table;
pub mod untyped;
mod value;

pub use app::AppFiles;
pub use config::{Backend, Config, Storage};
pub use error::{Error, Result};
pub use handle::{Jazz, Transaction};
pub use history::Version;
pub use include::{Include, IntoInclude, Nullability, OneInclude, Partial, Row, Selection, With};
pub use query::{
    Column, Condition, Many, NotNull, Nullable, One, Order, Query, created_at, deleted_at, id,
    updated_at,
};
pub use relation::{Hop, Step};
pub use subscription::{Added, Delta, Removed, Rows, Subscription, Updated};
pub use table::{Fields, Implementations, Insert, Table, Update};
pub use value::{ColumnValue, Json, Timestamp};

pub use jazz::account_registry::AccountId;
pub use jazz::tools::{AppId, ObjectId, ReadTier, TransactionId};

/// The `jazz` crate, for generated code and [`Jazz::with_client`].
#[doc(hidden)]
pub use jazz;

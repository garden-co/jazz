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
//! An end user's client ([`Config::user`]) signs in with a device account
//! ([`Auth::Device`]) or the app's own bearer ([`Auth::bearer`]), and
//! [`Jazz::session`] follows it; [`Session::link`] carries a device account
//! over to the user's provider sign-in.
//!
//! A backend serves its users' clients with an [`Endpoint`]: the app's
//! `/jazz` handler, as TS `createJazz` serves it, with the app's own
//! sign-in deciding who each request's user is.
//!
//! Not supported yet: includes (nested related rows in one result), reads
//! inside transactions. A bearer session is renewed by reconnecting with a
//! fresh bearer, as TypeScript's bearer clients do (in-place renewal is for
//! cookie sessions).

mod advice;
mod app;
mod auth;
mod backend;
mod backend_client;
mod config;
mod edit;
pub mod endpoint;
mod error;
mod handle;
mod large;
mod lifecycle;
mod query;
mod session;
mod subscription;
mod table;
mod transaction;
mod value;
mod writes;

pub use advice::Advice;
pub use app::AppFiles;
pub use auth::{Auth, Bearer, DeviceKey, TokenFuture, TokenProvider, TokenReason};
pub use backend_client::{BackendClientKind, CLOSE_TIMEOUT};
pub use config::{Backend, Config, Storage};
pub use edit::TextColumn;
pub use endpoint::Endpoint;
pub use error::{Error, Result};
pub use handle::Jazz;
pub use jazz::tools::MutationError;
pub use large::{JsonColumn, LargeColumn, Page, Splice};
pub use lifecycle::MutationErrors;
pub use query::{Column, Condition, Many, One, Order, Query, created_at, id, updated_at};
pub use session::{
    AuthFailure, Link, Offline, Session, SessionState, SyncError, SyncStatus, User, UserKind,
};
pub use subscription::{Added, Delta, Removed, Rows, Subscription, Updated};
pub use table::{Fields, Implementations, Insert, Table, Update};
pub use transaction::{OpenTransaction, Transaction};
pub use value::{ColumnValue, Json, Timestamp};

pub use jazz::account_registry::AccountId;
pub use jazz::tools::{AppId, DurabilityTier, ObjectId, ReadTier};

/// Stateless anchors and ranges into text values (cursors, selections,
/// comments), captured from one value and found again in later ones, as
/// TS's `db.textAnchors()`.
pub use jazz_text as text;

/// The `jazz` crate, for generated code and [`Jazz::with_client`].
#[doc(hidden)]
pub use jazz;

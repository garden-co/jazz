//! A typed Rust client for a Jazz app, opened from its `jazz.app.json`.
//!
//! **Experimental: client only; backends are TypeScript.** See
//! [Language Support](https://jazz.tools/docs/reference/language-support#jazz-environment-rust).
//!
//! The app is authored in TypeScript: `schema.ts` beside `Cargo.toml`
//! declares its tables and permissions, and `npx jazz-tools generate` writes
//! `jazz.app.json`, `jazz.permissions.json` and `src/jazz_app.rs` from it.
//! At runtime this crate reads only those generated files.
//!
//! The generated bindings (`src/jazz_app.rs`, or `jazz_codegen::rust::build`
//! in a `build.rs`) give each table a row struct, an insert
//! struct, an update struct and a module of typed columns, relations and
//! queries, and an `open` that embeds the app's schema. This crate is what
//! they build on: the [`Jazz`] handle and the traits they implement.
//!
//! The bindings re-export this crate (`pub use ::jazz_app_support::*`), so
//! app code names everything through one module, `jazz_app`. A generated
//! item wins where a name is the same (a table `errors` is `jazz_app::Error`;
//! this crate's stays `jazz_app_support::Error`).
//!
//! ```ignore
//! mod jazz_app;
//! use jazz_app::{Config, Jazz, NewTodo, Todo, todos};
//!
//! async fn open_todos(jazz: &Jazz) -> jazz_app::Result<Vec<Todo>> {
//!     jazz.all(todos::query().filter(todos::done().eq(false))).await
//! }
//!
//! let jazz = jazz_app::open(config).await?;                    // generated
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
//! An end user's client ([`Config::user`]) signs in with a device account
//! ([`Auth::Device`]) or the app's own bearer ([`Auth::bearer`]), and
//! [`Jazz::session`] follows it; [`Session::link`] carries a device account
//! over to the user's provider sign-in.
//!
//! [`Query::part`] reads part of a large value ([`ValuePart`]): a byte
//! range, a text range or a JSON subtree.
//!
//! Text offsets are UTF-8 bytes (`str` indices) throughout: splices
//! ([`Page::text`], [`Splice::text`]), partial reads ([`ValuePart::text`]),
//! subscription splices ([`ValueSplice::at`]) and anchors
//! ([`text::Anchor::capture_utf8`], [`text::Anchor::resolve_utf8`]). An
//! offset inside a code point is refused, not rounded. The UTF-16 forms
//! (`Page::TextUtf16`, `ValuePart::TextUtf16`, `ValueSplice::at_utf16`, the
//! anchors' own `position`) are for interop with hosts that count UTF-16.
//!
//! A bearer session is renewed by reconnecting with a fresh bearer, as
//! TypeScript's bearer clients do (in-place renewal is for cookie sessions).

mod advice;
mod app;
mod auth;
mod backend;
mod backend_client;
mod branch;
mod config;
mod edit;
#[cfg(feature = "unstable-endpoint")]
pub mod endpoint;
mod error;
mod handle;
mod history;
mod include;
mod jazz_session;
mod large;
mod lifecycle;
mod query;
mod read;
mod relation;
mod remembered;
mod session;
mod subscription;
mod table;
mod transaction;
pub mod untyped;
mod value;
mod writes;

pub use advice::Advice;
pub use app::AppFiles;
pub use auth::{Auth, Bearer, DeviceKey, TokenFuture, TokenProvider, TokenReason};
pub use backend_client::{BackendClientKind, CLOSE_TIMEOUT};
pub use branch::{Branch, BranchView, OnBranch, Snapshot};
pub use config::{Backend, Config, Storage};
pub use edit::{Edits, TextColumn};
#[cfg(feature = "unstable-endpoint")]
pub use endpoint::Endpoint;
pub use error::{Error, Result};
pub use handle::{DEFAULT_FIRST_LOAD_REMOTE_WAIT, DefaultRead, Jazz};
pub use history::Version;
pub use include::{
    Include, IntoInclude, Nullability, OneInclude, Partial, Pick, RefsInclude, Row, Selection, With,
};
pub use jazz::tools::MutationError;
pub use jazz_session::{
    Initial, JazzSession, Recovery, SessionConfig, SessionError, SessionSnapshot, SessionStatus,
};
pub use large::{JsonColumn, LargeColumn, Page, Splice};
pub use lifecycle::MutationErrors;
pub use query::{
    Column, Condition, Many, NotNull, Nullable, One, Order, Query, Refs, ValuePart, created_at,
    deleted_at, id, updated_at,
};
pub use read::ReadOptions;
pub use relation::{Hop, Step};
pub use session::{
    AuthFailure, Link, Offline, Session, SessionState, SyncError, SyncStatus, User, UserKind,
};
pub use subscription::{Added, Delta, Removed, Rows, Subscription, Updated, ValueSplice};
pub use table::{Fields, Implementations, Insert, Table, Update};
pub use transaction::{OpenTransaction, Transaction};
pub use value::{ColumnValue, Json, Timestamp};

pub use jazz::account_registry::AccountId;
pub use jazz::tools::{AppId, DurabilityTier, ObjectId, ReadTier, ResultKey, TransactionId};

/// Stateless anchors and ranges into text values (cursors, selections,
/// comments), captured from one value and found again in later ones, as
/// TS's `db.textAnchors()`. In Rust, capture and resolve them in UTF-8
/// byte offsets (`str` indices): [`text::Anchor::capture_utf8`],
/// [`text::Anchor::resolve_utf8`], [`text::Range::capture_utf8`],
/// [`text::Range::resolve_utf8`]. The plain `capture`/`resolve` and the
/// anchors' stored `position` hints count UTF-16 code units, as JavaScript
/// does, so anchors are shared across hosts.
pub use jazz_text as text;

/// The `jazz` crate, for generated code and [`Jazz::with_client`].
#[doc(hidden)]
pub use jazz;

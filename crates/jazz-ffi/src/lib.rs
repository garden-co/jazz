//! JSON-in/JSON-out bindings of [`jazz_app`] for other languages.
//!
//! Two surfaces over one dynamic handle:
//! - UniFFI (proc-macros) for Swift and Kotlin: [`JazzHandle`],
//!   [`Subscription`], [`SubscriptionListener`], [`JazzError`];
//! - a C ABI for Go via cgo, declared in `include/jazz_ffi.h` ([`c_abi`]).
//!
//! Rows, queries and deltas are plain JSON typed by the app's schema; see
//! the crate's README for the shapes. Not supported yet (refused with
//! `JazzError::Refused`): includes, connecting to a sync server as an end
//! user with a JWT, writing enums with payloads, reads inside transactions
//! (and transactions altogether), computed columns and transforms (the
//! language's bindings implement them over the stored values).

pub mod c_abi;
mod convert;
mod core;
mod error;

use std::sync::Arc;

pub use error::JazzError;

uniffi::setup_scaffolding!("jazz_ffi");

type Result<T> = std::result::Result<T, JazzError>;

/// An open Jazz app. Every method blocks until done and is safe to call
/// from any thread; wrap them in the language's async.
#[derive(uniffi::Object)]
pub struct JazzHandle {
    core: core::Core,
}

#[uniffi::export]
impl JazzHandle {
    /// Opens an app from its config JSON: `{ "app", "permissions"?,
    /// "appId"?, "serverUrl"?, "dataDir"?, "storage"?, "backendSecret"?,
    /// "adminSecret"?, "session"? }`.
    #[uniffi::constructor]
    pub fn open(config_json: String) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            core: core::Core::open(&config_json)?,
        }))
    }

    /// The account id of the user a backend acts as (`session.user`);
    /// `None` for a client.
    pub fn account(&self) -> Result<Option<String>> {
        Ok(self.core.account()?)
    }

    /// Every row the query matches, as a JSON array.
    pub fn all(&self, query_json: String) -> Result<String> {
        Ok(self.core.all(&query_json)?)
    }

    /// The first row the query matches.
    pub fn one(&self, query_json: String) -> Result<Option<String>> {
        Ok(self.core.one(&query_json)?)
    }

    /// Inserts a row; returns it as stored, with its `id`.
    pub fn insert(&self, table: String, row_json: String) -> Result<String> {
        Ok(self.core.insert(&table, &row_json)?)
    }

    /// Sets the columns `patch_json` names on the row with this id.
    pub fn update(&self, table: String, id: String, patch_json: String) -> Result<()> {
        Ok(self.core.update(&table, &id, &patch_json)?)
    }

    pub fn delete(&self, table: String, id: String) -> Result<()> {
        Ok(self.core.delete(&table, &id)?)
    }

    /// Subscribes to the query. The listener is called on a thread of the
    /// subscription's own: first with a `reset` delta holding the full
    /// result, then with each change.
    pub fn subscribe(
        &self,
        query_json: String,
        listener: Arc<dyn SubscriptionListener>,
    ) -> Result<Arc<Subscription>> {
        let inner = self.core.subscribe(
            &query_json,
            Box::new(move |event| match event {
                core::Event::Delta(json) => listener.on_delta(json),
                core::Event::Error(error) => listener.on_error(error.to_string()),
            }),
        )?;
        Ok(Arc::new(Subscription { inner }))
    }

    /// Closes the app; later calls fail with `Closed`. Named `shutdown`, not
    /// `close`, which would clash with Kotlin's `AutoCloseable.close`.
    pub fn shutdown(&self) -> Result<()> {
        Ok(self.core.close()?)
    }
}

impl std::fmt::Debug for JazzHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JazzHandle")
    }
}

/// What a subscription calls with its deltas.
#[uniffi::export(with_foreign)]
pub trait SubscriptionListener: Send + Sync {
    fn on_delta(&self, delta_json: String);
    /// The subscription failed and ends.
    fn on_error(&self, message: String);
}

/// A live subscription; it ends when cancelled or dropped.
#[derive(uniffi::Object)]
pub struct Subscription {
    inner: core::SubscriptionCore,
}

#[uniffi::export]
impl Subscription {
    /// No callback starts after this returns. Cancelling again does nothing.
    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

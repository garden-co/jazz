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
        Ok(self.core.all(&query_json, &Default::default())?)
    }

    /// [`Self::all`] read at `tier`: `"local"`, `"localFirst"` (the
    /// default) or `"remote"`.
    pub fn all_at(&self, query_json: String, tier: String) -> Result<String> {
        Ok(self.core.all(&query_json, &read_opts(&tier)?)?)
    }

    /// The first row the query matches.
    pub fn one(&self, query_json: String) -> Result<Option<String>> {
        Ok(self.core.one(&query_json, &Default::default())?)
    }

    /// [`Self::one`] read at `tier`.
    pub fn one_at(&self, query_json: String, tier: String) -> Result<Option<String>> {
        Ok(self.core.one(&query_json, &read_opts(&tier)?)?)
    }

    /// Inserts a row; returns it as stored, with its `id`.
    pub fn insert(&self, table: String, row_json: String) -> Result<String> {
        Ok(self.core.insert(&table, &row_json, &Default::default())?)
    }

    /// [`Self::insert`], returning once the write reaches `tier`:
    /// `"local"` or `"global"`.
    pub fn insert_at(&self, table: String, row_json: String, tier: String) -> Result<String> {
        Ok(self.core.insert(&table, &row_json, &write_opts(&tier)?)?)
    }

    /// Sets the columns `patch_json` names on the row with this id.
    pub fn update(&self, table: String, id: String, patch_json: String) -> Result<()> {
        Ok(self
            .core
            .update(&table, &id, &patch_json, &Default::default())?)
    }

    /// [`Self::update`], returning once the write reaches `tier`.
    pub fn update_at(
        &self,
        table: String,
        id: String,
        patch_json: String,
        tier: String,
    ) -> Result<()> {
        Ok(self
            .core
            .update(&table, &id, &patch_json, &write_opts(&tier)?)?)
    }

    pub fn delete(&self, table: String, id: String) -> Result<()> {
        Ok(self.core.delete(&table, &id, &Default::default())?)
    }

    /// [`Self::delete`], returning once the write reaches `tier`.
    pub fn delete_at(&self, table: String, id: String, tier: String) -> Result<()> {
        Ok(self.core.delete(&table, &id, &write_opts(&tier)?)?)
    }

    /// Begins a serializable transaction. Its reads see a snapshot and its
    /// own writes; dropping it without `commit` rolls it back.
    pub fn begin(self: Arc<Self>) -> Result<Arc<Transaction>> {
        let tx = core::parse_tx(&self.core.tx_begin(true)?)?;
        Ok(Arc::new(Transaction {
            handle: self,
            tx,
            open: std::sync::atomic::AtomicBool::new(true),
        }))
    }

    /// Subscribes to the query. The listener is called on a thread of the
    /// subscription's own: first with a `reset` delta holding the full
    /// result, then with each change.
    pub fn subscribe(
        &self,
        query_json: String,
        listener: Arc<dyn SubscriptionListener>,
    ) -> Result<Arc<Subscription>> {
        self.subscribe_with(query_json, Default::default(), listener)
    }

    /// [`Self::subscribe`] read at `tier`.
    pub fn subscribe_at(
        &self,
        query_json: String,
        tier: String,
        listener: Arc<dyn SubscriptionListener>,
    ) -> Result<Arc<Subscription>> {
        self.subscribe_with(query_json, read_opts(&tier)?, listener)
    }

    /// Closes the app; later calls fail with `Closed`. Named `shutdown`, not
    /// `close`, which would clash with Kotlin's `AutoCloseable.close`.
    pub fn shutdown(&self) -> Result<()> {
        Ok(self.core.close()?)
    }
}

impl JazzHandle {
    fn subscribe_with(
        &self,
        query_json: String,
        opts: core::Opts,
        listener: Arc<dyn SubscriptionListener>,
    ) -> Result<Arc<Subscription>> {
        let inner = self.core.subscribe(
            &query_json,
            &opts,
            Box::new(move |event| match event {
                core::Event::Delta(json) => listener.on_delta(json),
                core::Event::Error(error) => listener.on_error(error.to_string()),
            }),
        )?;
        Ok(Arc::new(Subscription { inner }))
    }
}

fn read_opts(tier: &str) -> Result<core::Opts> {
    Ok(core::Opts {
        read_tier: Some(core::read_tier(tier)?),
        ..Default::default()
    })
}

fn write_opts(tier: &str) -> Result<core::Opts> {
    Ok(core::Opts {
        write_tier: Some(core::write_tier(tier)?),
        ..Default::default()
    })
}

/// An open transaction: reads and writes inside it, then `commit` or
/// `rollback`. Use it from one thread at a time.
#[derive(uniffi::Object)]
pub struct Transaction {
    handle: Arc<JazzHandle>,
    tx: u64,
    open: std::sync::atomic::AtomicBool,
}

impl Transaction {
    fn opts(&self) -> core::Opts {
        core::Opts {
            tx: Some(self.tx),
            ..Default::default()
        }
    }

    fn end(&self) -> Result<()> {
        if self.open.swap(false, std::sync::atomic::Ordering::AcqRel) {
            Ok(())
        } else {
            Err(JazzError::Refused {
                reason: "the transaction has already ended".into(),
            })
        }
    }
}

#[uniffi::export]
impl Transaction {
    pub fn all(&self, query_json: String) -> Result<String> {
        Ok(self.handle.core.all(&query_json, &self.opts())?)
    }

    pub fn one(&self, query_json: String) -> Result<Option<String>> {
        Ok(self.handle.core.one(&query_json, &self.opts())?)
    }

    pub fn insert(&self, table: String, row_json: String) -> Result<String> {
        Ok(self.handle.core.insert(&table, &row_json, &self.opts())?)
    }

    pub fn update(&self, table: String, id: String, patch_json: String) -> Result<()> {
        Ok(self
            .handle
            .core
            .update(&table, &id, &patch_json, &self.opts())?)
    }

    pub fn delete(&self, table: String, id: String) -> Result<()> {
        Ok(self.handle.core.delete(&table, &id, &self.opts())?)
    }

    /// Commits its writes together, returning once they reach `tier`
    /// (`"local"` or `"global"`) if given.
    pub fn commit(&self, tier: Option<String>) -> Result<()> {
        self.end()?;
        let tier = tier.as_deref().map(core::write_tier).transpose()?;
        Ok(self.handle.core.tx_commit(self.tx, tier)?)
    }

    pub fn rollback(&self) -> Result<()> {
        self.end()?;
        Ok(self.handle.core.tx_rollback(self.tx)?)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.open.swap(false, std::sync::atomic::Ordering::AcqRel) {
            let _ = self.handle.core.tx_rollback(self.tx);
        }
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

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
//! user with a JWT, reads inside transactions
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
        let contract = serde_json::from_str::<serde_json::Value>(&config_json)
            .ok()
            .and_then(|c| c.get("contract").and_then(serde_json::Value::as_u64))
            .unwrap_or(1);
        let core = core::Core::open(&config_json)
            .map_err(|error| JazzError::from(error).for_contract(contract))?;
        Ok(Arc::new(Self { core }))
    }

    /// The account id of the user a backend acts as (`session.user`);
    /// `None` for a client.
    pub fn account(&self) -> Result<Option<String>> {
        self.core.surface(self.core.account())
    }

    /// Every row the query matches, as a JSON array.
    pub fn all(&self, query_json: String) -> Result<String> {
        self.core
            .surface(self.core.all(&query_json, &Default::default()))
    }

    /// [`Self::all`] read at `tier`: `"local"`, `"localFirst"` (the
    /// default) or `"remote"`.
    pub fn all_at(&self, query_json: String, tier: String) -> Result<String> {
        self.core.surface(
            self.core
                .all(&query_json, &self.core.surface(read_opts(&tier))?),
        )
    }

    /// The first row the query matches.
    pub fn one(&self, query_json: String) -> Result<Option<String>> {
        self.core
            .surface(self.core.one(&query_json, &Default::default()))
    }

    /// [`Self::one`] read at `tier`.
    pub fn one_at(&self, query_json: String, tier: String) -> Result<Option<String>> {
        self.core.surface(
            self.core
                .one(&query_json, &self.core.surface(read_opts(&tier))?),
        )
    }

    /// Inserts a row; returns it as stored, with its `id`.
    pub fn insert(&self, table: String, row_json: String) -> Result<String> {
        self.core
            .surface(self.core.insert(&table, &row_json, &Default::default()))
    }

    /// [`Self::insert`], returning once the write reaches `tier`:
    /// `"local"` or `"global"`.
    pub fn insert_at(&self, table: String, row_json: String, tier: String) -> Result<String> {
        self.core.surface(self.core.insert(
            &table,
            &row_json,
            &self.core.surface(write_opts(&tier))?,
        ))
    }

    /// Sets the columns `patch_json` names on the row with this id.
    pub fn update(&self, table: String, id: String, patch_json: String) -> Result<()> {
        self.core.surface(
            self.core
                .update(&table, &id, &patch_json, &Default::default()),
        )
    }

    /// [`Self::update`], returning once the write reaches `tier`.
    pub fn update_at(
        &self,
        table: String,
        id: String,
        patch_json: String,
        tier: String,
    ) -> Result<()> {
        self.core.surface(self.core.update(
            &table,
            &id,
            &patch_json,
            &self.core.surface(write_opts(&tier))?,
        ))
    }

    pub fn delete(&self, table: String, id: String) -> Result<()> {
        self.core
            .surface(self.core.delete(&table, &id, &Default::default()))
    }

    /// [`Self::delete`], returning once the write reaches `tier`.
    pub fn delete_at(&self, table: String, id: String, tier: String) -> Result<()> {
        self.core.surface(
            self.core
                .delete(&table, &id, &self.core.surface(write_opts(&tier))?),
        )
    }

    /// Returns once every write committed so far reaches `tier`
    /// (`"local"` or `"global"`).
    pub fn wait_for_sync(&self, tier: String) -> Result<()> {
        self.core
            .surface(core::write_tier(&tier).and_then(|tier| self.core.wait_for_sync(tier)))
    }

    /// Begins a serializable transaction. Its reads see a snapshot and its
    /// own writes; dropping it without `commit` rolls it back.
    pub fn begin(self: Arc<Self>) -> Result<Arc<Transaction>> {
        let tx = self
            .core
            .surface(self.core.tx_begin(true).and_then(|tx| core::parse_tx(&tx)))?;
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
        self.subscribe_with(query_json, self.core.surface(read_opts(&tier))?, listener)
    }

    /// Closes the app; later calls fail with `Closed`. Named `shutdown`, not
    /// `close`, which would clash with Kotlin's `AutoCloseable.close`.
    pub fn shutdown(&self) -> Result<()> {
        self.core.surface(self.core.close())
    }
}

impl JazzHandle {
    fn subscribe_with(
        &self,
        query_json: String,
        opts: core::Opts,
        listener: Arc<dyn SubscriptionListener>,
    ) -> Result<Arc<Subscription>> {
        let contract = self.core.contract;
        let inner = self.core.surface(self.core.subscribe(
            &query_json,
            &opts,
            Box::new(move |event| match event {
                core::Event::Delta(json) => listener.on_delta(json),
                core::Event::Error(error) => {
                    let error = error.for_contract(contract);
                    listener.on_error(error.kind().to_owned(), error.to_string())
                }
            }),
        ))?;
        Ok(Arc::new(Subscription { inner }))
    }
}

fn read_opts(tier: &str) -> error::Result<core::Opts> {
    Ok(core::Opts {
        read_tier: Some(core::read_tier(tier)?),
        ..Default::default()
    })
}

fn write_opts(tier: &str) -> error::Result<core::Opts> {
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
        self.handle
            .core
            .surface(self.handle.core.all(&query_json, &self.opts()))
    }

    pub fn one(&self, query_json: String) -> Result<Option<String>> {
        self.handle
            .core
            .surface(self.handle.core.one(&query_json, &self.opts()))
    }

    pub fn insert(&self, table: String, row_json: String) -> Result<String> {
        self.handle
            .core
            .surface(self.handle.core.insert(&table, &row_json, &self.opts()))
    }

    pub fn update(&self, table: String, id: String, patch_json: String) -> Result<()> {
        self.handle.core.surface(
            self.handle
                .core
                .update(&table, &id, &patch_json, &self.opts()),
        )
    }

    pub fn delete(&self, table: String, id: String) -> Result<()> {
        self.handle
            .core
            .surface(self.handle.core.delete(&table, &id, &self.opts()))
    }

    /// Commits its writes together, returning once they reach `tier`
    /// (`"local"` or `"global"`) if given.
    pub fn commit(&self, tier: Option<String>) -> Result<()> {
        self.end()?;
        let tier = self
            .handle
            .core
            .surface(tier.as_deref().map(core::write_tier).transpose())?;
        self.handle
            .core
            .surface(self.handle.core.tx_commit(self.tx, tier))
    }

    pub fn rollback(&self) -> Result<()> {
        self.end()?;
        self.handle
            .core
            .surface(self.handle.core.tx_rollback(self.tx))
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
    /// The subscription ended: its last call. `kind` is the error JSON's
    /// kind (`"closed"` when its app closed or its stream ended; `"refused"`;
    /// with contract 2 also `"permission"`, …), so hosts need not match on
    /// `message`.
    fn on_error(&self, kind: String, message: String);
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

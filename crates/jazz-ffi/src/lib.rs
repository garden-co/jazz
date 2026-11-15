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
        let contract = serde_json::from_str::<serde_json::Value>(&config_json)
            .ok()
            .and_then(|c| c.get("contract").and_then(serde_json::Value::as_u64))
            .unwrap_or(1);
        let core = core::Core::open(&config_json)
            .map_err(|error| JazzError::from(error).for_contract(contract))?;
        Ok(Arc::new(Self { core }))
    }

    /// Opens an app as an end user who signs in with the app's bearer
    /// (`"auth": { "bearer": true }` in the config): `tokens` gives it.
    #[uniffi::constructor]
    pub fn open_with_tokens(
        config_json: String,
        tokens: Arc<dyn TokenProvider>,
    ) -> Result<Arc<Self>> {
        let contract = serde_json::from_str::<serde_json::Value>(&config_json)
            .ok()
            .and_then(|c| c.get("contract").and_then(serde_json::Value::as_u64))
            .unwrap_or(1);
        let host = core::HostTokens(Arc::new(move |reason: &str| {
            tokens.token(reason.to_owned())
        }));
        let core = core::Core::open_with_tokens(&config_json, Some(Arc::new(host)))
            .map_err(|error| JazzError::from(error).for_contract(contract))?;
        Ok(Arc::new(Self { core }))
    }

    /// The end user's session as JSON: `{"status": "signedIn", "user":
    /// {...}}`, `{"status": "expired", ...}` or `{"status": "signedOut"}`;
    /// `None` for a local app or a backend.
    pub fn session_state(&self) -> Result<Option<String>> {
        self.core.surface(self.core.session_state())
    }

    /// The connection's status as JSON: `{"link": ...}`.
    pub fn sync_status(&self) -> Result<String> {
        self.core.surface(self.core.sync_status())
    }

    /// Asks the token provider for a fresh token (reason `refresh`).
    pub fn refresh(&self) -> Result<()> {
        self.core.surface(self.core.refresh())
    }

    /// Signs the end user out and closes the app.
    pub fn sign_out(&self) -> Result<()> {
        self.core.surface(self.core.sign_out())
    }

    /// Calls `listener` with each session state, the current one first.
    pub fn watch_session(&self, listener: Arc<dyn StateListener>) -> Result<Arc<Watch>> {
        self.watch("session", listener)
    }

    /// Calls `listener` with each sync status, the current one first.
    pub fn watch_sync(&self, listener: Arc<dyn StateListener>) -> Result<Arc<Watch>> {
        self.watch("sync", listener)
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

    /// Sets the text-merge column `column` to `text`, an edit of
    /// `edited_from` (the value its writer last saw): changes made since by
    /// others are kept, as the server merges them.
    pub fn edit_text(
        &self,
        table: String,
        id: String,
        column: String,
        edited_from: String,
        text: String,
    ) -> Result<()> {
        let edits = text_edit(column, edited_from, text);
        self.core.surface(
            self.core
                .edit(&table, &id, None, &edits, &Default::default()),
        )
    }

    /// Partial edits of a row's text, bytes and JSON values (`edits_json`:
    /// `text`, `splice` and `jsonSet` edits), with an optional whole-column
    /// `patch_json`, in one write.
    pub fn edit(
        &self,
        table: String,
        id: String,
        patch_json: Option<String>,
        edits_json: String,
    ) -> Result<()> {
        self.core.surface(self.core.edit(
            &table,
            &id,
            patch_json.as_deref(),
            &edits_json,
            &Default::default(),
        ))
    }

    /// The server's advice on whether this user may do `op` (`"read"`,
    /// `"insert"`, `"update"`, `"delete"`) without doing it: `true`,
    /// `false`, or `None` when no server can say. `args_json`: `{"id"}` for
    /// read and delete, `{"row"}` for insert, `{"id", "row"?}` for update.
    pub fn can(&self, table: String, op: String, args_json: String) -> Result<Option<bool>> {
        self.core.surface((|| {
            let args = convert_args(&args_json)?;
            let row = args
                .get("row")
                .filter(|r| !r.is_null())
                .map(|r| r.to_string());
            let id = args.get("id").and_then(serde_json::Value::as_str);
            self.core.can(&table, &op, id, row.as_deref())
        })())
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
    fn watch(&self, what: &str, listener: Arc<dyn StateListener>) -> Result<Arc<Watch>> {
        let inner = self.core.surface(
            self.core
                .watch(what, Box::new(move |state| listener.on_change(state))),
        )?;
        Ok(Arc::new(Watch { inner }))
    }

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

fn convert_args(args_json: &str) -> error::Result<serde_json::Value> {
    if args_json.trim().is_empty() {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_str(args_json)
        .map_err(|error| error::refused(format!("args_json is not valid JSON: {error}")))
}

fn text_edit(column: String, edited_from: String, text: String) -> String {
    serde_json::json!([{
        "kind": "text",
        "column": column,
        "text": text,
        "editedFrom": edited_from,
    }])
    .to_string()
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

    pub fn edit_text(
        &self,
        table: String,
        id: String,
        column: String,
        edited_from: String,
        text: String,
    ) -> Result<()> {
        let edits = text_edit(column, edited_from, text);
        self.handle.core.surface(
            self.handle
                .core
                .edit(&table, &id, None, &edits, &self.opts()),
        )
    }

    pub fn edit(
        &self,
        table: String,
        id: String,
        patch_json: Option<String>,
        edits_json: String,
    ) -> Result<()> {
        self.handle.core.surface(self.handle.core.edit(
            &table,
            &id,
            patch_json.as_deref(),
            &edits_json,
            &self.opts(),
        ))
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

/// Where an end user's bearer tokens come from (the host's own sign-in).
/// Called on a thread of its own, so it may block; `reason` is `connect`,
/// `renew`, `expired` or `refresh` (the last two want a fresh token, not a
/// cached one). `None` means no one is signed in.
#[uniffi::export(with_foreign)]
pub trait TokenProvider: Send + Sync {
    fn token(&self, reason: String) -> Option<String>;
}

/// What a session or sync watch calls with each state (JSON).
#[uniffi::export(with_foreign)]
pub trait StateListener: Send + Sync {
    fn on_change(&self, state_json: String);
}

/// A live watch of session or sync states; it ends when cancelled or
/// dropped.
#[derive(uniffi::Object)]
pub struct Watch {
    inner: core::Watch,
}

#[uniffi::export]
impl Watch {
    /// When it returns, no callback is running (unless called from one).
    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

/// A new device account's key as `jazz-auth-v1:…` text, for `"auth":
/// {"deviceKey": …}`: keep it as a password, in the platform's keychain.
#[uniffi::export]
pub fn generate_device_key() -> Result<String> {
    core::generate_device_key().map_err(JazzError::from)
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

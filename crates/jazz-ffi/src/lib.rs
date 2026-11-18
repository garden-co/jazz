//! JSON-in/JSON-out bindings of [`jazz_app_support`] for other languages.
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
mod gate;
mod relational;
mod session;

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
    /// Opens an app from its config JSON: `{ "app", "appId"?, "serverUrl"?,
    /// "auth"?, "dataDir"?, "storage"?, "contract"? }` (see the README): a
    /// local client, or an end user with a device key. Bearer users and
    /// backend clients open with [`Self::open_with_tokens`].
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
            tokens
                .token(reason.to_owned())
                .map_err(|error| error.to_string())
        }));
        let core = core::Core::open_with_tokens(&config_json, Some(Arc::new(host)))
            .map_err(|error| JazzError::from(error).for_contract(contract))?;
        Ok(Arc::new(Self { core }))
    }

    /// The end user's session as JSON: `{"status": "signedIn", "user":
    /// {...}}`, `{"status": "expired", ...}` or `{"status": "signedOut"}`;
    /// `None` for a local app.
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

    /// Links this device account to the user's account with the app's own
    /// sign-in (`bearer`): the session becomes that user's. Returns the user
    /// as JSON.
    pub fn link(&self, bearer: String) -> Result<String> {
        self.core.surface(self.core.link(&bearer))
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

    /// Not part of the documented surface (the backend publish mode); `None`
    /// for every client.
    pub fn bundle(&self) -> Result<Option<String>> {
        self.core.surface(self.core.bundle())
    }

    /// The account id of the user this handle acts as (an end user, a
    /// scoped backend client, a `for_user` handle); `None` for a local app
    /// or a trusted backend client (the system).
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

    /// Writes the row with this id: a new id inserts (`row_json` then holds
    /// every required column), an existing one sets the columns `row_json`
    /// names. Returns the row.
    pub fn upsert(&self, table: String, id: String, row_json: String) -> Result<String> {
        self.core.surface(
            self.core
                .upsert(&table, &id, &row_json, &Default::default()),
        )
    }

    /// Brings back a deleted row, as it was or with `patch_json` (a whole
    /// row) as its new content. Returns the row.
    pub fn restore(&self, table: String, id: String, patch_json: Option<String>) -> Result<String> {
        self.core.surface(self.core.restore(
            &table,
            &id,
            patch_json.as_deref(),
            &Default::default(),
        ))
    }

    /// Begins uploading a large value of `column` piece by piece: `kind`
    /// `"insert"` (a new row, `id` none, its other columns `row_json`),
    /// `"update"` or `"upsert"` (the row with `id`).
    pub fn begin_upload(
        self: Arc<Self>,
        table: String,
        column: String,
        kind: String,
        id: Option<String>,
        row_json: Option<String>,
    ) -> Result<Arc<Upload>> {
        let upload = self.core.surface(
            self.core
                .upload_begin(&table, &column, &kind, id.as_deref(), row_json.as_deref())
                .and_then(|id| core::parse_tx(&id)),
        )?;
        Ok(Arc::new(Upload {
            handle: self,
            upload,
            open: std::sync::atomic::AtomicBool::new(true),
        }))
    }

    /// Begins a serializable transaction. Its reads see a snapshot and its
    /// own writes; dropping it without `commit` rolls it back.
    pub fn begin(self: Arc<Self>) -> Result<Arc<Transaction>> {
        self.begin_with(true)
    }

    /// Begins a mergeable transaction (TS's default `transaction`): its
    /// reads see a snapshot and its own writes, its writes commit together
    /// and merge with concurrent writes, so its commit is never refused as
    /// a conflict.
    pub fn begin_mergeable(self: Arc<Self>) -> Result<Arc<Transaction>> {
        self.begin_with(false)
    }

    /// Leaves the server: local reads and writes go on, and sync resumes
    /// with `reconnect` (TS's `disconnect`). Without a server it does
    /// nothing.
    pub fn disconnect(&self) -> Result<()> {
        self.core.surface(self.core.disconnect())
    }

    /// Resumes sync after `disconnect` (TS's `reconnect`).
    pub fn reconnect(&self) -> Result<()> {
        self.core.surface(self.core.reconnect())
    }

    /// Closes the app and deletes its local store (TS's
    /// `deleteClientStorage`): the next open starts empty and syncs again.
    /// Writes the server hasn't accepted yet are lost. Refused while another
    /// open app holds the same `dataDir`.
    pub fn delete_client_storage(&self) -> Result<()> {
        self.core.surface(self.core.delete_client_storage())
    }

    /// Calls `listener` with each write the server refused that no durable
    /// wait (a `global` tier, `wait_for_sync`) reported, as TS's
    /// `onMutationError`: `{"transaction", "message", "code", "reason",
    /// "denied", "serializable"}`, `code` and `reason` as TS's
    /// `MutationErrorEvent`. Refusals from before the first watch come
    /// first.
    pub fn watch_mutation_errors(&self, listener: Arc<dyn StateListener>) -> Result<Arc<Watch>> {
        self.watch("mutationErrors", listener)
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

    /// [`Self::shutdown`], first waiting up to `timeout_ms` for the writes
    /// the client holds to reach the server (TS's `close({ timeoutMs })`,
    /// 10 000 by default there). With memory storage, writes still unsent
    /// fail it with `Unsynced { pending }` (contract 2; `Io` otherwise), and
    /// the app is closed all the same. `0` closes at once.
    pub fn shutdown_within(&self, timeout_ms: u64) -> Result<()> {
        self.core.surface(
            self.core
                .close_within(Some(std::time::Duration::from_millis(timeout_ms))),
        )
    }

    /// A backend client's kind (`"auth": { "credential": true }`), as the
    /// main backend decided it: `"trusted"` or `"scoped"`; `None` for any
    /// other app.
    pub fn backend_client(&self) -> Result<Option<String>> {
        self.core
            .surface(self.core.backend_client())
            .map(|kind| kind.map(str::to_owned))
    }

    /// On a trusted backend client, a handle acting as `user_id` (the id the
    /// main backend's `authenticate` names them by), with `claims_json` (a
    /// JSON object; empty for `{}`) as the claims its permissions see. It
    /// never creates an account: `NoAccount` (contract 2) when the user has
    /// none yet; `Permission` when the main backend refuses the identity;
    /// `Refused` on a scoped client, a `for_user` handle or any other app.
    /// Its `shutdown` leaves this handle open; after this handle closes, its
    /// calls fail with `Closed`.
    pub fn for_user(&self, user_id: String, claims_json: String) -> Result<Arc<Self>> {
        let core = self
            .core
            .surface(self.core.for_user(&user_id, &claims_json))?;
        Ok(Arc::new(Self { core }))
    }
}

impl JazzHandle {
    fn begin_with(self: Arc<Self>, serializable: bool) -> Result<Arc<Transaction>> {
        let tx = self.core.surface(
            self.core
                .tx_begin(serializable)
                .and_then(|tx| core::parse_tx(&tx)),
        )?;
        Ok(Arc::new(Transaction {
            handle: self,
            tx,
            open: std::sync::atomic::AtomicBool::new(true),
        }))
    }

    fn watch(&self, what: &str, listener: Arc<dyn StateListener>) -> Result<Arc<Watch>> {
        let inner = self.core.surface(self.core.watch(
            what,
            Box::new(move |state| {
                listener.on_change(state.unwrap_or_else(|| CLOSED_STATE.to_owned()))
            }),
        ))?;
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

    pub fn upsert(&self, table: String, id: String, row_json: String) -> Result<String> {
        self.handle.core.surface(
            self.handle
                .core
                .upsert(&table, &id, &row_json, &self.opts()),
        )
    }

    pub fn restore(&self, table: String, id: String, patch_json: Option<String>) -> Result<String> {
        self.handle.core.surface(self.handle.core.restore(
            &table,
            &id,
            patch_json.as_deref(),
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
/// cached one). `None` means no one is signed in (the app stops); an error
/// means the fetch failed (a network blip): the session stays signed in and
/// asks again.
#[uniffi::export(with_foreign)]
pub trait TokenProvider: Send + Sync {
    fn token(&self, reason: String) -> Result<Option<String>>;
}

/// A watch's last state when the app closes (signed out, closed, its store
/// deleted): `{"kind": "closed"}`, after which no call comes.
const CLOSED_STATE: &str = r#"{"kind":"closed","message":"the app is closed"}"#;

/// What a watch calls with each state (JSON); when the app closes, a last
/// `{"kind": "closed", "message"}`, after which no call comes.
#[uniffi::export(with_foreign)]
pub trait StateListener: Send + Sync {
    fn on_change(&self, state_json: String);
}

/// A user-following session (TS's `JazzSession`): who is signed in (the
/// app's bearer, a device account, or no one) and a client for that user,
/// replaced when the user changes. Config: the app's, with `"initial"`:
/// `"backend"` (follow the bearer, the default; needs `"auth": {"bearer":
/// true}` and a token provider), `"device"` or `"signed-out"`.
#[derive(uniffi::Object)]
pub struct Session {
    core: session::SessionCore,
}

impl Session {
    fn surface<T>(&self, result: error::Result<T>) -> Result<T> {
        result.map_err(|error| JazzError::from(error).for_contract(self.core.contract))
    }

    fn call(&self, method: &str) -> Result<()> {
        self.surface(self.core.call(method, &serde_json::Value::Null))
            .map(drop)
    }
}

#[uniffi::export]
impl Session {
    #[uniffi::constructor]
    pub fn open(config_json: String, tokens: Option<Arc<dyn TokenProvider>>) -> Result<Arc<Self>> {
        let contract = serde_json::from_str::<serde_json::Value>(&config_json)
            .ok()
            .and_then(|c| c.get("contract").and_then(serde_json::Value::as_u64))
            .unwrap_or(1);
        let tokens = tokens.map(|tokens| {
            let host = core::HostTokens(Arc::new(move |reason: &str| {
                tokens
                    .token(reason.to_owned())
                    .map_err(|error| error.to_string())
            }));
            let tokens: Arc<dyn jazz_app_support::TokenProvider> = Arc::new(host);
            tokens
        });
        let core = session::SessionCore::open(&config_json, tokens)
            .map_err(|error| JazzError::from(error).for_contract(contract))?;
        Ok(Arc::new(Self { core }))
    }

    /// `{"status": "starting"|"ready"|"signed-out"|"transitioning"|"error"|
    /// "closed", "user"?, "clientEpoch", "error"?: {"kind", "message"},
    /// "recovery"?: "action"|"session"}`.
    pub fn snapshot(&self) -> Result<String> {
        self.surface(self.core.snapshot())
    }

    /// A handle acting for the user signed in now: `NoUser` (with the
    /// status) while there is none. Fetch it again when `clientEpoch`
    /// changes: one from before fails with `Replaced`. Its `shutdown`
    /// leaves the session open.
    pub fn jazz(&self) -> Result<Arc<JazzHandle>> {
        let core = self.surface(self.core.jazz())?;
        Ok(Arc::new(JazzHandle { core }))
    }

    /// The app's auth changed: asks the bearer and the backend who is
    /// signed in now, and follows.
    pub fn refresh(&self) -> Result<()> {
        self.call("refresh")
    }

    pub fn create_device_account(&self) -> Result<()> {
        self.call("createDeviceAccount")
    }

    /// Signs in as the device account `device_key` (`jazz-auth-v1:…`).
    pub fn restore_device_account(&self, device_key: String) -> Result<()> {
        self.surface(self.core.call(
            "restoreDeviceAccount",
            &serde_json::json!({ "deviceKey": device_key }),
        ))
        .map(drop)
    }

    /// Signs in again as the device account this device keeps.
    pub fn reopen_device_account(&self) -> Result<()> {
        self.call("reopenDeviceAccount")
    }

    /// Links the signed-in device account to the bearer's user.
    pub fn link_device_account(&self) -> Result<()> {
        self.call("linkDeviceAccount")
    }

    /// The device account's key (`jazz-auth-v1:…`): keep it as a password.
    pub fn export_device_key(&self) -> Result<String> {
        let key = self.surface(self.core.call("exportDeviceKey", &serde_json::Value::Null))?;
        Ok(key.as_str().unwrap_or_default().to_owned())
    }

    /// Signs out; the session stays, `signed-out`.
    pub fn sign_out(&self) -> Result<()> {
        self.call("signOut")
    }

    /// Repeats what failed, as the snapshot's `recovery` says.
    pub fn retry(&self) -> Result<()> {
        self.call("retry")
    }

    /// Calls `listener` with each snapshot, the current one first; when the
    /// session closes, a last `{"kind": "closed", "message"}`.
    pub fn watch(&self, listener: Arc<dyn StateListener>) -> Result<Arc<Watch>> {
        let inner = self.surface(self.core.watch(Box::new(move |state| {
            listener.on_change(state.unwrap_or_else(|| CLOSED_STATE.to_owned()))
        })))?;
        Ok(Arc::new(Watch { inner }))
    }

    /// Closes the session and its client: handles it gave fail with
    /// `Closed`, watches end.
    pub fn shutdown(&self) -> Result<()> {
        self.surface(self.core.close())
    }
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

/// A large value being uploaded: `push` its pieces, then `finish` (or
/// `abort`). Use it from one thread at a time.
#[derive(uniffi::Object)]
pub struct Upload {
    handle: Arc<JazzHandle>,
    upload: u64,
    /// Not yet finished or aborted: dropped, it aborts.
    open: std::sync::atomic::AtomicBool,
}

impl Drop for Upload {
    fn drop(&mut self) {
        if self.open.swap(false, std::sync::atomic::Ordering::AcqRel) {
            let _ = self.handle.core.upload_abort(self.upload);
        }
    }
}

#[uniffi::export]
impl Upload {
    pub fn push(&self, bytes: Vec<u8>) -> Result<()> {
        self.handle
            .core
            .surface(self.handle.core.upload_push(self.upload, bytes))
    }

    /// Commits the row with its value; returns the row.
    pub fn finish(&self) -> Result<String> {
        self.open.store(false, std::sync::atomic::Ordering::Release);
        self.handle.core.surface(
            self.handle
                .core
                .upload_finish(self.upload, &Default::default()),
        )
    }

    pub fn abort(&self) -> Result<()> {
        self.open.store(false, std::sync::atomic::Ordering::Release);
        self.handle
            .core
            .surface(self.handle.core.upload_abort(self.upload))
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

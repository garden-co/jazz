//! A user-following session (jazz-app-support's `JazzSession`, TS's `JazzSession`):
//! who is signed in (the app's bearer, a device account, or no one), and
//! the client for that user, replaced when the user changes.
//!
//! Its config is the app's config with `"auth": {"bearer": true}` optional
//! and `"initial"`: `"backend"` (follow the bearer; the default),
//! `"device"` (the device account) or `"signed-out"`. A handle from
//! [`SessionCore::jazz`] acts for the user signed in when it was fetched;
//! once the user changes it fails with `Replaced`, and the snapshot's
//! `clientEpoch` says to fetch the new one.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use futures_executor::block_on;
use jazz_app_support::jazz::tools::Schema;
use jazz_app_support::{AppFiles, AppId, Initial, JazzSession, SessionConfig, Storage};
use serde_json::Value as Json;

use crate::convert;
use crate::core::{Core, Watch, spawn_watch};
use crate::error::{FfiError, Result, refused};

pub(crate) struct SessionCore {
    session: JazzSession,
    schema: OnceLock<Arc<Schema>>,
    relations: convert::Relations,
    pub(crate) contract: u64,
    closed: Arc<AtomicBool>,
    /// Shared by every handle the session hands out: their transaction and
    /// upload ids never collide.
    next_tx: Arc<AtomicU64>,
    watches: Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    /// Run when the session closes: a C host's token callback stops here.
    close_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl SessionCore {
    pub fn open(
        config_json: &str,
        tokens: Option<Arc<dyn jazz_app_support::TokenProvider>>,
    ) -> Result<Self> {
        let (files, config, app_text, contract) = session_config(config_json, tokens)?;
        let session = block_on(JazzSession::open(files, config))?;
        Ok(Self {
            session,
            schema: OnceLock::new(),
            relations: convert::Relations::read(&app_text),
            contract,
            closed: Arc::new(AtomicBool::new(false)),
            next_tx: Arc::new(AtomicU64::new(
                AppId::random().uuid().as_u64_pair().1 >> 12 | 1,
            )),
            watches: Mutex::new(Vec::new()),
            close_hook: Mutex::new(None),
        })
    }

    fn open_session(&self) -> Result<&JazzSession> {
        if self.closed.load(Ordering::Acquire) {
            Err(FfiError::Closed)
        } else {
            Ok(&self.session)
        }
    }

    /// The snapshot as JSON: `{"status", "user"?, "clientEpoch", "error"?:
    /// {"kind", "message"}, "recovery"?: "action"|"session"}`.
    pub fn snapshot(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.open_session()?.snapshot()).unwrap_or_default())
    }

    /// A handle acting for the user signed in now. `NoUser` (with the
    /// session's status) while there is none.
    pub fn jazz(&self) -> Result<Core> {
        let session = self.open_session()?;
        // The client and the epoch it belongs to, read together: a change
        // of user between the two reads is retried.
        let (jazz, epoch) = loop {
            let epoch = session.snapshot().client_epoch;
            let jazz = session.jazz()?;
            if session.snapshot().client_epoch == epoch {
                break (jazz, epoch);
            }
        };
        let schema = match self.schema.get() {
            Some(schema) => schema.clone(),
            None => {
                let schema =
                    block_on(jazz.with_client(|client| async move { Ok(client.schema()?) }))?;
                self.schema.get_or_init(|| Arc::new(schema)).clone()
            }
        };
        Ok(Core::for_session(
            jazz,
            schema,
            self.relations.clone(),
            self.contract,
            self.closed.clone(),
            self.next_tx.clone(),
            {
                let session = self.session.clone();
                Box::new(move || session.snapshot().client_epoch != epoch)
            },
        ))
    }

    /// Runs one of the session's operations by name.
    pub fn call(&self, method: &str, args: &Json) -> Result<Json> {
        let session = self.open_session()?;
        match method {
            "refresh" => block_on(session.refresh())?,
            "createDeviceAccount" => block_on(session.create_device_account())?,
            "restoreDeviceAccount" => {
                let key = args
                    .get("deviceKey")
                    .and_then(Json::as_str)
                    .ok_or_else(|| refused("restoreDeviceAccount needs \"deviceKey\""))?;
                block_on(session.restore_device_account(key))?
            }
            "reopenDeviceAccount" => block_on(session.reopen_device_account())?,
            "linkDeviceAccount" => block_on(session.link_device_account())?,
            "exportDeviceKey" => return Ok(Json::String(session.export_device_key()?)),
            "signOut" => block_on(session.sign_out())?,
            "retry" => block_on(session.retry())?,
            "snapshot" => return Ok(serde_json::to_value(session.snapshot()).unwrap_or_default()),
            other => return Err(refused(format!("unknown session method {other:?}"))),
        }
        Ok(Json::Null)
    }

    /// Calls `listener` with each snapshot's JSON, the current one first,
    /// and once with `None` when the session closes.
    pub fn watch(&self, listener: Box<dyn Fn(Option<String>) + Send>) -> Result<Watch> {
        use futures_util::StreamExt as _;
        let stream = Box::pin(
            self.open_session()?
                .changes()
                .map(|snapshot| serde_json::to_string(&snapshot).unwrap_or_default()),
        );
        spawn_watch(stream, listener, &self.watches, &self.closed)
    }

    pub(crate) fn on_close(&self, hook: Box<dyn FnOnce() + Send>) {
        *self.close_hook.lock().unwrap_or_else(|e| e.into_inner()) = Some(hook);
    }

    /// Closes the session and its client: every handle it gave fails with
    /// `Closed`, and every watch ends. Closing again does nothing.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = block_on(self.session.close());
        let hook = self
            .close_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
        for close in std::mem::take(&mut *self.watches.lock().unwrap_or_else(|e| e.into_inner())) {
            let _ = close.send(());
        }
        result?;
        Ok(())
    }
}

fn session_config(
    config_json: &str,
    tokens: Option<Arc<dyn jazz_app_support::TokenProvider>>,
) -> Result<(AppFiles, SessionConfig, String, u64)> {
    let Json::Object(config) = convert::parse(config_json, "the config")? else {
        return Err(refused("the config must be a JSON object"));
    };
    let text = |key: &str| -> Result<Option<String>> {
        match config.get(key) {
            None | Some(Json::Null) => Ok(None),
            Some(Json::String(text)) => Ok(Some(text.clone())),
            Some(_) => Err(refused(format!("config.{key} must be a string"))),
        }
    };
    for key in config.keys() {
        if ![
            "app",
            "appId",
            "serverUrl",
            "storage",
            "dataDir",
            "auth",
            "initial",
            "allowInsecureHttp",
            "linkGuest",
            "contract",
        ]
        .contains(&key.as_str())
        {
            return Err(refused(format!(
                "a session's config has an unknown key {key:?}"
            )));
        }
    }
    let app = text("app")?.ok_or_else(|| refused("the config needs \"app\""))?;
    let app_id = text("appId")?.ok_or_else(|| refused("a session needs an \"appId\""))?;
    let app_id = AppId::from_string(&app_id)
        .map_err(|_| refused(format!("config.appId must be a UUID, not {app_id:?}")))?;
    let server = text("serverUrl")?
        .ok_or_else(|| refused("a session needs \"serverUrl\": the app's Jazz endpoint"))?;
    let storage = match text("storage")?.as_deref() {
        None | Some("memory") => Storage::Memory,
        Some("persistent") => Storage::Dir(
            text("dataDir")?
                .ok_or_else(|| refused("\"storage\": \"persistent\" needs a \"dataDir\""))?
                .into(),
        ),
        Some(other) => {
            return Err(refused(format!(
                "config.storage must be \"memory\" or \"persistent\", not {other:?}"
            )));
        }
    };
    let initial = match text("initial")?.as_deref() {
        None | Some("backend") => Initial::Backend,
        Some("device") => Initial::Device,
        // TS's spelling, and the camelCase this layer's other values use.
        Some("signed-out" | "signedOut") => Initial::SignedOut,
        Some(other) => {
            return Err(refused(format!(
                "config.initial is \"backend\", \"device\" or \"signed-out\", not {other:?}"
            )));
        }
    };
    let contract = match config.get("contract") {
        None | Some(Json::Null) => 1,
        Some(value) => match value.as_u64() {
            Some(version @ (1 | 2)) => version,
            _ => {
                return Err(refused(format!(
                    "config.contract must be 1 or 2, not {value}"
                )));
            }
        },
    };
    let mut session = SessionConfig::new(app_id)
        .server(server)
        .storage(storage)
        .initial(initial);
    match config.get("auth") {
        None | Some(Json::Null) => {}
        Some(auth) if auth == &serde_json::json!({ "bearer": true }) => {
            let tokens = tokens.ok_or_else(|| {
                refused("\"auth\": { \"bearer\": true } needs a token provider: open with one")
            })?;
            session = session.bearer(crate::core::Tokens(tokens));
        }
        Some(_) => {
            return Err(refused(
                "a session's config.auth is { \"bearer\": true }, or none (device accounts come from \"initial\" and the session's calls)",
            ));
        }
    }
    // Whether a signed-in user with no account yet links the device guest
    // (TS's default) or the app links it with linkDeviceAccount.
    match config.get("linkGuest") {
        None | Some(Json::Null) => {}
        Some(Json::Bool(link)) => session = session.link_guest(*link),
        Some(_) => return Err(refused("config.linkGuest must be true or false")),
    }
    match config.get("allowInsecureHttp") {
        None | Some(Json::Null) | Some(Json::Bool(false)) => {}
        Some(Json::Bool(true)) => session = session.allow_insecure_http(),
        Some(_) => return Err(refused("config.allowInsecureHttp must be true or false")),
    }
    if matches!(initial, Initial::Backend) && session.bearer.is_none() {
        return Err(refused(
            "\"initial\": \"backend\" follows the app's bearer: give \"auth\": { \"bearer\": true }, or open with \"initial\": \"device\" or \"signed-out\"",
        ));
    }
    let files = AppFiles::new(app.clone().into_bytes(), None);
    Ok((files, session, app, contract))
}

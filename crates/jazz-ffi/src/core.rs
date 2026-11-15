//! The dynamic handle both surfaces wrap: blocking, JSON in and out.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use futures_executor::block_on;
use jazz_app::jazz::tools::{ReadTier, Schema, SubscriptionStreamItem, TableSchema, Value};
use jazz_app::{AppFiles, AppId, Backend, Config, Jazz, Storage};
use serde_json::Value as Json;

use crate::convert;
use crate::error::{FfiError, Result, refused, unsupported};

/// The issuer jazz-app names a backend's users by.
const BACKEND_ISSUER: &str = "jazz:backend";

pub(crate) struct Core {
    jazz: Jazz,
    schema: Arc<Schema>,
    closed: AtomicBool,
}

/// What a subscription tells its listener.
pub(crate) enum Event {
    Delta(String),
    Error(String),
}

impl Core {
    pub fn open(config_json: &str) -> Result<Self> {
        let (files, config) = config(config_json)?;
        let jazz = block_on(Jazz::open(files, config))?;
        let schema = block_on(jazz.with_client(|client| async move { Ok(client.schema()?) }))?;
        Ok(Self {
            jazz,
            schema: Arc::new(schema),
            closed: AtomicBool::new(false),
        })
    }

    fn open_jazz(&self) -> Result<&Jazz> {
        if self.closed.load(Ordering::Acquire) {
            Err(FfiError::Closed)
        } else {
            Ok(&self.jazz)
        }
    }

    fn table(&self, name: &str) -> Result<&TableSchema> {
        convert::table(&self.schema, name)
    }

    /// The account of the user a backend acts as (`session.user`).
    pub fn account(&self) -> Result<Option<String>> {
        Ok(self
            .open_jazz()?
            .account()
            .map(|account| account.0.to_string()))
    }

    pub fn all(&self, query_json: &str) -> Result<String> {
        let rows = self.rows(&convert::parse(query_json, "the query")?)?;
        Ok(Json::Array(rows).to_string())
    }

    pub fn one(&self, query_json: &str) -> Result<Option<String>> {
        let mut query = convert::parse(query_json, "the query")?;
        if let Json::Object(object) = &mut query {
            object.insert("limit".into(), Json::from(1));
        }
        Ok(self
            .rows(&query)?
            .into_iter()
            .next()
            .map(|row| row.to_string()))
    }

    fn rows(&self, query: &Json) -> Result<Vec<Json>> {
        let (table_name, query) = convert::query(&self.schema, query)?;
        let jazz = self.open_jazz()?;
        let results = block_on(jazz.with_client(move |client| async move {
            Ok(client.query(query, ReadTier::LocalFirst).await?)
        }))?;
        let table = self.table(&table_name)?;
        results
            .into_iter()
            .map(|row| {
                let id = row
                    .key
                    .row_id()
                    .ok_or_else(|| FfiError::Io("a joined result is not a row".into()))?;
                Ok(convert::row_json(table, id, row.fields))
            })
            .collect()
    }

    pub fn insert(&self, table_name: &str, row_json: &str) -> Result<String> {
        let table = self.table(table_name)?.clone();
        let (id, cells) = convert::cells(&table, table_name, row_json, "the row")?;
        let cells: HashMap<String, Value> = cells.into_iter().collect();
        let name = table_name.to_owned();
        let jazz = self.open_jazz()?;
        let row = block_on(jazz.with_client(move |client| async move {
            let (id, values, _) = client.insert_with_id(&name, id.map(|id| *id.uuid()), cells)?;
            let fields = table
                .columns
                .columns
                .iter()
                .zip(values)
                .map(|(column, value)| jazz_app::jazz::tools::QueryResultField {
                    name: column.name.as_str().to_owned(),
                    value,
                })
                .collect();
            Ok(convert::row_json(&table, id, fields))
        }))?;
        Ok(row.to_string())
    }

    pub fn update(&self, table_name: &str, id: &str, patch_json: &str) -> Result<()> {
        let id = convert::parse_id(id, "the id")?;
        let table = self.table(table_name)?;
        let (patched_id, cells) = convert::cells(table, table_name, patch_json, "the patch")?;
        if patched_id.is_some_and(|patched| patched != id) {
            return Err(refused("a patch cannot change a row's id"));
        }
        if cells.is_empty() {
            return Ok(());
        }
        let name = table_name.to_owned();
        block_on(self.open_jazz()?.with_client(move |client| async move {
            client.update(&name, id, cells)?;
            Ok(())
        }))?;
        Ok(())
    }

    pub fn delete(&self, table_name: &str, id: &str) -> Result<()> {
        let id = convert::parse_id(id, "the id")?;
        self.table(table_name)?;
        let name = table_name.to_owned();
        block_on(self.open_jazz()?.with_client(move |client| async move {
            client.delete(&name, id)?;
            Ok(())
        }))?;
        Ok(())
    }

    /// Subscribes to `query_json`; `listener` is called on a thread of the
    /// subscription's own, so it may call back into the handle.
    pub fn subscribe(
        &self,
        query_json: &str,
        listener: Box<dyn Fn(Event) + Send>,
    ) -> Result<SubscriptionCore> {
        let (table_name, query) =
            convert::query(&self.schema, &convert::parse(query_json, "the query")?)?;
        let table = self.table(&table_name)?.clone();
        let (events, received) = mpsc::channel::<Option<Event>>();
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
        let forward = events.clone();
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let mut stream = client
                .subscribe_with_read_tier(query, ReadTier::LocalFirst)
                .await?;
            // Forwards on the client's thread until cancelled, the stream
            // ends, or the delivery thread is gone.
            tokio::task::spawn_local(async move {
                let mut cancelled = cancelled;
                let mut first = true;
                loop {
                    let item = tokio::select! {
                        item = stream.next() => item,
                        _ = &mut cancelled => return,
                    };
                    let event = match item {
                        None => return,
                        Some(SubscriptionStreamItem::Delta(delta)) => {
                            match convert::delta_json(&table, delta, first) {
                                Ok(json) => Event::Delta(json),
                                Err(error) => {
                                    Event::Error(crate::JazzError::from(error).to_string())
                                }
                            }
                        }
                        Some(SubscriptionStreamItem::Rejected { reason }) => {
                            Event::Error(format!("subscription rejected: {reason:?}"))
                        }
                    };
                    first = false;
                    let failed = matches!(event, Event::Error(_));
                    if forward.send(Some(event)).is_err() || failed {
                        return;
                    }
                }
            });
            Ok(())
        }))?;
        let active = Arc::new(AtomicBool::new(true));
        let delivering = active.clone();
        std::thread::Builder::new()
            .name("jazz-subscription".into())
            .spawn(move || {
                while let Ok(Some(event)) = received.recv() {
                    if !delivering.load(Ordering::Acquire) {
                        break;
                    }
                    listener(event);
                }
            })
            .map_err(|error| {
                FfiError::Io(format!("cannot start the subscription's thread: {error}"))
            })?;
        Ok(SubscriptionCore {
            active,
            cancel: Mutex::new(Some(cancel)),
            events,
            _jazz: self.jazz.clone(),
        })
    }

    /// Closes the app: the calls already made finish, subscriptions end,
    /// and every later call fails with `Closed`. Closing again does nothing.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        block_on(self.jazz.shutdown())?;
        Ok(())
    }
}

pub(crate) struct SubscriptionCore {
    active: Arc<AtomicBool>,
    cancel: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    events: mpsc::Sender<Option<Event>>,
    /// Holds the app open while the subscription lives.
    _jazz: Jazz,
}

impl SubscriptionCore {
    /// Ends the subscription: no callback starts after this returns (one
    /// already running finishes). Cancelling again does nothing.
    pub fn cancel(&self) {
        self.active.store(false, Ordering::Release);
        if let Some(cancel) = self.cancel.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = cancel.send(());
            let _ = self.events.send(None);
        }
    }
}

impl Drop for SubscriptionCore {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn config(config_json: &str) -> Result<(AppFiles, Config)> {
    let Json::Object(config) = convert::parse(config_json, "the config")? else {
        return Err(refused("the config must be a JSON object"));
    };
    let mut app = None;
    let mut permissions = None;
    let mut app_id = None;
    let mut server_url = None;
    let mut data_dir = None;
    let mut storage = None;
    let mut backend_secret = None;
    let mut admin_secret = None;
    let mut session = None;
    for (key, value) in &config {
        if value.is_null() {
            continue;
        }
        let text = || {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| refused(format!("config.{key} must be a string")))
        };
        match key.as_str() {
            "app" => app = Some(text()?),
            "permissions" => permissions = Some(text()?),
            "appId" => app_id = Some(text()?),
            "serverUrl" => server_url = Some(text()?),
            "dataDir" => data_dir = Some(text()?),
            "storage" => storage = Some(text()?),
            "backendSecret" => backend_secret = Some(text()?),
            "adminSecret" => admin_secret = Some(text()?),
            "session" => session = Some(value.clone()),
            "jwt" | "jwtToken" | "token" => {
                return Err(unsupported(
                    "connecting to a sync server as an end user with a JWT; open the app as a backend (permissions + backendSecret + session) or locally",
                ));
            }
            other => return Err(refused(format!("the config has an unknown key {other:?}"))),
        }
    }
    let app = app.ok_or_else(|| refused("the config needs \"app\": the text of jazz.app.json"))?;
    let storage = match storage.as_deref() {
        None | Some("memory") => Storage::Memory,
        Some("persistent") => Storage::Dir(
            data_dir
                .ok_or_else(|| refused("\"storage\": \"persistent\" needs a \"dataDir\""))?
                .into(),
        ),
        Some(other) => {
            return Err(refused(format!(
                "config.storage must be \"memory\" or \"persistent\", not {other:?}"
            )));
        }
    };
    let backend = permissions.is_some();
    let app_id = match app_id {
        Some(text) => AppId::from_string(&text)
            .map_err(|_| refused(format!("config.appId must be a UUID, not {text:?}")))?,
        None if backend || storage != Storage::Memory => {
            return Err(refused(
                "the config needs an \"appId\" for a backend or a persistent store",
            ));
        }
        None => AppId::random(),
    };
    let mut result = Config::new(app_id).storage(storage);
    if let Some(url) = server_url {
        result = result.server(url);
    }
    if backend {
        let subject = match &session {
            Some(Json::Object(session)) => {
                if let Some(issuer) = session.get("issuer").filter(|i| !i.is_null())
                    && issuer.as_str() != Some(BACKEND_ISSUER)
                {
                    return Err(unsupported(format!(
                        "a session issuer other than \"{BACKEND_ISSUER}\" ({issuer})"
                    )));
                }
                session
                    .get("subject")
                    .and_then(Json::as_str)
                    .ok_or_else(|| refused("config.session needs a \"subject\""))?
                    .to_owned()
            }
            _ => {
                return Err(refused(
                    "a backend (with \"permissions\") needs a \"session\": { \"subject\" }",
                ));
            }
        };
        let backend_secret =
            backend_secret.ok_or_else(|| refused("a backend needs \"backendSecret\""))?;
        let admin_secret = admin_secret.ok_or_else(|| {
            refused("a backend needs \"adminSecret\": it publishes the app's permissions bundle")
        })?;
        result = result.backend(Backend::new(backend_secret, admin_secret, subject));
    } else if backend_secret.is_some() || admin_secret.is_some() || session.is_some() {
        return Err(refused(
            "backendSecret, adminSecret and session are for a backend, which needs \"permissions\"",
        ));
    }
    let files = AppFiles::new(app.into_bytes(), permissions.map(String::into_bytes));
    Ok((files, result))
}

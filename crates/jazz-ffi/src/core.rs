//! The dynamic handle both surfaces wrap: blocking, JSON in and out.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread::{JoinHandle, ThreadId};

use futures_executor::block_on;
use jazz_app::jazz::tools::{
    DurabilityTier, JazzClient, JazzTransaction, ReadTier, Schema, SubscriptionStreamItem,
    TableSchema, Value,
};
use jazz_app::{AppFiles, AppId, Backend, Config, Jazz, Storage};
use serde_json::Value as Json;

use crate::JazzError;
use crate::convert;
use crate::error::{FfiError, Result, refused, unsupported};

/// The issuer jazz-app names a backend's users by.
const BACKEND_ISSUER: &str = "jazz:backend";

pub(crate) struct Core {
    jazz: Jazz,
    schema: Arc<Schema>,
    relations: convert::Relations,
    closed: AtomicBool,
    /// The open subscriptions, told when the app closes.
    subscriptions: Mutex<Vec<Weak<Delivery>>>,
    next_tx: AtomicU64,
    /// The contract version the host speaks (`"contract"` in the config):
    /// 1 unless it says 2.
    pub(crate) contract: u64,
}

/// How a call runs: inside a transaction, and/or at a durability or read
/// tier.
#[derive(Debug, Clone, Default)]
pub(crate) struct Opts {
    pub tx: Option<u64>,
    pub write_tier: Option<DurabilityTier>,
    pub read_tier: Option<ReadTier>,
}

impl Opts {
    /// From the optional `"tx"` and `"tier"` of a call's arguments; `read`
    /// says whether the call reads or writes.
    pub(crate) fn parse(tx: Option<&Json>, tier: Option<&Json>, read: bool) -> Result<Self> {
        let mut opts = Self::default();
        if let Some(tx) = tx.filter(|t| !t.is_null()) {
            opts.tx = Some(parse_tx(tx.as_str().unwrap_or_default())?);
        }
        if let Some(tier) = tier.filter(|t| !t.is_null()) {
            let tier = tier
                .as_str()
                .ok_or_else(|| refused("\"tier\" must be a string"))?;
            if read {
                opts.read_tier = Some(read_tier(tier)?);
            } else {
                opts.write_tier = Some(write_tier(tier)?);
            }
        }
        if opts.tx.is_some() && opts.write_tier.is_some() {
            return Err(refused(
                "a write inside a transaction has no tier of its own: give it to txCommit",
            ));
        }
        Ok(opts)
    }
}

pub(crate) fn parse_tx(text: &str) -> Result<u64> {
    text.parse().map_err(|_| {
        refused(format!(
            "\"tx\" must be a transaction from txBegin, not {text:?}"
        ))
    })
}

/// Durability a write (or commit) waits for.
pub(crate) fn write_tier(tier: &str) -> Result<DurabilityTier> {
    match tier {
        "local" => Ok(DurabilityTier::Local),
        "global" => Ok(DurabilityTier::GlobalServer),
        "edge" => Err(unsupported(
            "waiting for edge durability: this client knows local and global",
        )),
        other => Err(refused(format!(
            "a write tier is \"local\" or \"global\", not {other:?}"
        ))),
    }
}

/// What a read (or subscription) reads.
pub(crate) fn read_tier(tier: &str) -> Result<ReadTier> {
    match tier {
        "local" => Ok(ReadTier::LocalOnly),
        "localFirst" => Ok(ReadTier::LocalFirst),
        "remote" => Ok(ReadTier::Remote),
        other => Err(refused(format!(
            "a read tier is \"local\", \"localFirst\" or \"remote\", not {other:?}"
        ))),
    }
}

thread_local! {
    /// The open transactions of the client running on this thread (each
    /// app runs its client on a thread of its own), and whether each wrote.
    static TRANSACTIONS: RefCell<Transactions> = RefCell::new(Transactions::default());
}

/// The open transactions. Close rolls them back; any left when the client's
/// thread exits are forgotten, not dropped: the client they belong to is
/// already gone, and dropping them then would abort the process.
#[derive(Default)]
struct Transactions(HashMap<u64, (JazzTransaction, bool)>);

impl Drop for Transactions {
    fn drop(&mut self) {
        for entry in std::mem::take(&mut self.0).into_values() {
            std::mem::forget(entry);
        }
    }
}

/// The client a call runs with: the transaction's, or the app's.
fn client_for(client: JazzClient, tx: Option<u64>, writes: bool) -> jazz_app::Result<JazzClient> {
    let Some(tx) = tx else {
        return Ok(client);
    };
    TRANSACTIONS.with(|transactions| {
        let mut transactions = transactions.borrow_mut();
        let (transaction, wrote) = transactions
            .0
            .get_mut(&tx)
            .ok_or_else(|| jazz_app::Error::Config(format!("transaction {tx} is not open")))?;
        *wrote |= writes;
        Ok(transaction.client().clone())
    })
}

/// What a subscription tells its listener. An error is the last event.
pub(crate) enum Event {
    Delta(String),
    Error(JazzError),
}

/// The channel to a subscription's delivery thread.
struct Delivery {
    active: AtomicBool,
    events: Mutex<mpsc::Sender<Option<Event>>>,
}

impl Delivery {
    fn send(&self, event: Option<Event>) {
        let _ = self
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send(event);
    }
}

impl Core {
    pub fn open(config_json: &str) -> Result<Self> {
        let (files, config, app_text) = config(config_json)?;
        let contract = convert::parse(config_json, "the config")?
            .get("contract")
            .and_then(Json::as_u64)
            .unwrap_or(1);
        let jazz = block_on(Jazz::open(files, config))?;
        let schema = block_on(jazz.with_client(|client| async move { Ok(client.schema()?) }))?;
        Ok(Self {
            jazz,
            schema: Arc::new(schema),
            relations: convert::Relations::read(&app_text),
            closed: AtomicBool::new(false),
            subscriptions: Mutex::new(Vec::new()),
            next_tx: AtomicU64::new(1),
            contract,
        })
    }

    /// An error as this host's contract version names it.
    pub(crate) fn surface<T>(&self, result: Result<T>) -> std::result::Result<T, JazzError> {
        result.map_err(|error| JazzError::from(error).for_contract(self.contract))
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

    pub fn all(&self, query_json: &str, opts: &Opts) -> Result<String> {
        let rows = self.rows(&convert::parse(query_json, "the query")?, opts)?;
        Ok(Json::Array(rows).to_string())
    }

    pub fn one(&self, query_json: &str, opts: &Opts) -> Result<Option<String>> {
        // Validated as given (a bad `limit` is refused), then limited to 1.
        let (_, mut query, shape) = convert::query(
            &self.schema,
            &self.relations,
            &convert::parse(query_json, "the query")?,
        )?;
        query.limit = Some(query.limit.map_or(1, |limit| limit.min(1)));
        Ok(self
            .run_query(shape, query, opts)?
            .into_iter()
            .next()
            .map(|row| row.to_string()))
    }

    fn rows(&self, query: &Json, opts: &Opts) -> Result<Vec<Json>> {
        let (_, query, shape) = convert::query(&self.schema, &self.relations, query)?;
        self.run_query(shape, query, opts)
    }

    fn run_query(
        &self,
        shape: convert::Shape,
        query: jazz_app::jazz::query::Query,
        opts: &Opts,
    ) -> Result<Vec<Json>> {
        let jazz = self.open_jazz()?;
        let (tx, tier) = (opts.tx, opts.read_tier.unwrap_or(ReadTier::LocalFirst));
        let results = block_on(jazz.with_client(move |client| async move {
            let client = client_for(client, tx, false)?;
            Ok(client.query(query, tier).await?)
        }))?;
        results
            .into_iter()
            .map(|row| {
                let id = row
                    .key
                    .row_id()
                    .ok_or_else(|| FfiError::Io("a joined result is not a row".into()))?;
                Ok(convert::row_json(&shape, id, row.fields))
            })
            .collect()
    }

    pub fn insert(&self, table_name: &str, row_json: &str, opts: &Opts) -> Result<String> {
        let table = self.table(table_name)?.clone();
        let (id, cells) = convert::cells(&table, table_name, row_json, "the row")?;
        let cells: HashMap<String, Value> = cells.into_iter().collect();
        let name = table_name.to_owned();
        let jazz = self.open_jazz()?;
        let (tx, tier) = (opts.tx, opts.write_tier);
        let row = block_on(jazz.with_client(move |client| async move {
            let client = client_for(client, tx, true)?;
            let (id, values, committed) =
                client.insert_with_id(&name, id.map(|id| *id.uuid()), cells)?;
            settle(&client, committed, tier).await?;
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
            Ok(convert::row_json(&convert::Shape::of(table), id, fields))
        }))?;
        Ok(row.to_string())
    }

    pub fn update(&self, table_name: &str, id: &str, patch_json: &str, opts: &Opts) -> Result<()> {
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
        let (tx, tier) = (opts.tx, opts.write_tier);
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx, true)?;
            let committed = client.update(&name, id, cells)?;
            settle(&client, committed, tier).await
        }))?;
        Ok(())
    }

    pub fn delete(&self, table_name: &str, id: &str, opts: &Opts) -> Result<()> {
        let id = convert::parse_id(id, "the id")?;
        self.table(table_name)?;
        let name = table_name.to_owned();
        let (tx, tier) = (opts.tx, opts.write_tier);
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx, true)?;
            let committed = client.delete(&name, id)?;
            settle(&client, committed, tier).await
        }))?;
        Ok(())
    }

    /// Subscribes to `query_json`; `listener` is called on a thread of the
    /// subscription's own, so it may call back into the handle.
    pub fn subscribe(
        &self,
        query_json: &str,
        opts: &Opts,
        listener: Box<dyn Fn(Event) + Send>,
    ) -> Result<SubscriptionCore> {
        if opts.tx.is_some() {
            return Err(unsupported("subscribing inside a transaction"));
        }
        let tier = opts.read_tier.unwrap_or(ReadTier::LocalFirst);
        let (_, query, table) = convert::query(
            &self.schema,
            &self.relations,
            &convert::parse(query_json, "the query")?,
        )?;
        let (events, received) = mpsc::channel::<Option<Event>>();
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
        let forward = events.clone();
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let mut stream = client.subscribe_with_read_tier(query, tier).await?;
            // Forwards on the client's thread until cancelled, the stream
            // ends, or the delivery thread is gone.
            tokio::task::spawn_local(async move {
                // If the client drops this task without it reaching an end
                // (its thread winding down), the guard sends `Closed`.
                let mut forward = EndGuard(Some(forward));
                let mut cancelled = cancelled;
                let mut first = true;
                loop {
                    let item = tokio::select! {
                        item = stream.next() => item,
                        _ = &mut cancelled => {
                            forward.0 = None;
                            return;
                        }
                    };
                    let event = match item {
                        // The stream ended without a rejection (the client
                        // stopped, or the server ended the query): say so,
                        // so no host waits for a delta that never comes.
                        None => Event::Error(JazzError::Closed),
                        Some(SubscriptionStreamItem::Delta(delta)) => {
                            match convert::delta_json(&table, delta, first) {
                                Ok(json) => Event::Delta(json),
                                Err(error) => Event::Error(JazzError::from(error)),
                            }
                        }
                        Some(SubscriptionStreamItem::Rejected { reason }) => {
                            Event::Error(JazzError::Refused {
                                reason: format!("subscription rejected: {reason:?}"),
                            })
                        }
                    };
                    first = false;
                    let failed = matches!(event, Event::Error(_));
                    if forward.send(event).is_err() || failed {
                        forward.0 = None;
                        return;
                    }
                }
            });
            Ok(())
        }))?;
        let delivery = Arc::new(Delivery {
            active: AtomicBool::new(true),
            events: Mutex::new(events),
        });
        let delivering = delivery.clone();
        let thread = std::thread::Builder::new()
            .name("jazz-subscription".into())
            .spawn(move || {
                while let Ok(Some(event)) = received.recv() {
                    if !delivering.active.load(Ordering::Acquire) {
                        break;
                    }
                    let last = matches!(event, Event::Error(_));
                    listener(event);
                    if last {
                        break;
                    }
                }
            })
            .map_err(|error| {
                FfiError::Io(format!("cannot start the subscription's thread: {error}"))
            })?;
        let mut subscriptions = self.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        subscriptions.retain(|subscription| subscription.strong_count() > 0);
        subscriptions.push(Arc::downgrade(&delivery));
        drop(subscriptions);
        // A close racing this subscribe may have taken the list before the
        // push: tell this subscription itself. A second `Closed` is never
        // delivered (an error is the last event).
        if self.closed.load(Ordering::Acquire) {
            delivery.send(Some(Event::Error(JazzError::Closed)));
        }
        Ok(SubscriptionCore {
            delivery,
            delivery_thread: thread.thread().id(),
            thread: Mutex::new(Some(thread)),
            cancel: Mutex::new(Some(cancel)),
            _jazz: self.jazz.clone(),
        })
    }

    /// Begins a serializable transaction: its reads see a snapshot and its
    /// own writes, and its writes commit together or not at all.
    pub fn tx_begin(&self, serializable: bool) -> Result<String> {
        if !serializable {
            return Err(unsupported(
                "non-serializable transactions; every transaction is serializable",
            ));
        }
        let tx = self.next_tx.fetch_add(1, Ordering::Relaxed);
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let transaction = client.begin_transaction()?;
            TRANSACTIONS.with(|t| t.borrow_mut().0.insert(tx, (transaction, false)));
            Ok(())
        }))?;
        Ok(tx.to_string())
    }

    /// Commits a transaction, waiting for `tier` if given. A transaction
    /// that wrote nothing just ends.
    pub fn tx_commit(&self, tx: u64, tier: Option<DurabilityTier>) -> Result<()> {
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let (transaction, wrote) = take_tx(tx)?;
            if !wrote {
                transaction.rollback()?;
                return Ok(());
            }
            let committed = transaction.commit()?;
            settle(&client, Some(committed), tier).await
        }))?;
        Ok(())
    }

    /// Waits until every write committed so far reaches `tier`: the client
    /// settles its writes in order, so waiting for the last one is enough.
    pub fn wait_for_sync(&self, tier: DurabilityTier) -> Result<()> {
        block_on(self.open_jazz()?.with_client(move |client| async move {
            match LAST_WRITE.with(|last| last.get()) {
                Some(last) => Ok(client.wait_for_transaction(last, tier).await?),
                None => Ok(()),
            }
        }))?;
        Ok(())
    }

    pub fn tx_rollback(&self, tx: u64) -> Result<()> {
        block_on(self.open_jazz()?.with_client(move |_| async move {
            take_tx(tx)?.0.rollback()?;
            Ok(())
        }))?;
        Ok(())
    }

    /// Closes the app: the calls already made finish, subscriptions end,
    /// and every later call fails with `Closed`. Closing again does nothing.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        // Open transactions roll back while their client still runs.
        let _ = block_on(self.jazz.with_client(|_| async {
            let open = TRANSACTIONS.with(|t| std::mem::take(&mut t.borrow_mut().0));
            for (transaction, _) in open.into_values() {
                let _ = transaction.rollback();
            }
            Ok(())
        }));
        let result = block_on(self.jazz.shutdown());
        // Each open subscription's last event: the app closed.
        let subscriptions =
            std::mem::take(&mut *self.subscriptions.lock().unwrap_or_else(|e| e.into_inner()));
        for delivery in subscriptions.iter().filter_map(Weak::upgrade) {
            delivery.send(Some(Event::Error(JazzError::Closed)));
        }
        result?;
        Ok(())
    }
}

/// A forwarder's sender that, dropped before the forwarder ended on its
/// own, tells the delivery thread the subscription is over.
struct EndGuard(Option<mpsc::Sender<Option<Event>>>);

impl EndGuard {
    fn send(&self, event: Event) -> std::result::Result<(), ()> {
        match &self.0 {
            Some(sender) => sender.send(Some(event)).map_err(drop),
            None => Err(()),
        }
    }
}

impl Drop for EndGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(Some(Event::Error(JazzError::Closed)));
        }
    }
}

fn take_tx(tx: u64) -> jazz_app::Result<(JazzTransaction, bool)> {
    TRANSACTIONS
        .with(|t| t.borrow_mut().0.remove(&tx))
        .ok_or_else(|| jazz_app::Error::Config(format!("transaction {tx} is not open")))
}

/// Waits for a committed write to reach `tier`, when asked to.
async fn settle(
    client: &JazzClient,
    committed: Option<jazz_app::jazz::tools::TransactionId>,
    tier: Option<DurabilityTier>,
) -> jazz_app::Result<()> {
    if let Some(committed) = committed {
        LAST_WRITE.with(|last| last.set(Some(committed)));
        if let Some(tier) = tier {
            client.wait_for_transaction(committed, tier).await?;
        }
    }
    Ok(())
}

thread_local! {
    /// The last write this app's client committed (on its own thread).
    static LAST_WRITE: std::cell::Cell<Option<jazz_app::jazz::tools::TransactionId>> =
        const { std::cell::Cell::new(None) };
}

pub(crate) struct SubscriptionCore {
    delivery: Arc<Delivery>,
    delivery_thread: ThreadId,
    thread: Mutex<Option<JoinHandle<()>>>,
    cancel: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Holds the app open while the subscription lives.
    _jazz: Jazz,
}

impl SubscriptionCore {
    /// Ends the subscription: no callback starts after this returns (one
    /// already running finishes). Cancelling again does nothing.
    pub fn cancel(&self) {
        self.delivery.active.store(false, Ordering::Release);
        if let Some(cancel) = self.cancel.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = cancel.send(());
            self.delivery.send(None);
        }
    }

    /// [`Self::cancel`], then waits for a callback still running to return,
    /// so the caller may free what the callback uses. From the callback
    /// itself (on the delivery thread) it cannot wait, and doesn't.
    pub fn cancel_and_wait(&self) {
        self.cancel();
        if std::thread::current().id() == self.delivery_thread {
            return;
        }
        if let Some(thread) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = thread.join();
        }
    }
}

impl Drop for SubscriptionCore {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn config(config_json: &str) -> Result<(AppFiles, Config, String)> {
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
            // Read by `Core::open`; checked here.
            "contract" => match value.as_u64() {
                Some(1 | 2) => {}
                _ => {
                    return Err(refused(format!(
                        "config.contract must be 1 or 2, not {value}"
                    )));
                }
            },
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
        if result.server_url.is_none() {
            return Err(refused(
                "a backend (with \"permissions\") needs \"serverUrl\": the sync server it publishes to",
            ));
        }
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
    let files = AppFiles::new(
        app.clone().into_bytes(),
        permissions.map(String::into_bytes),
    );
    Ok((files, result, app))
}

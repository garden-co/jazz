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
    /// Whether the app syncs with a server: global durability needs one.
    has_server: bool,
    /// The contract version the host speaks (`"contract"` in the config):
    /// 1 unless it says 2.
    pub(crate) contract: u64,
    /// Run first when the app closes: a C host's callbacks stop here.
    close_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
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
        // TS spells it "local-first".
        "localFirst" | "local-first" => Ok(ReadTier::LocalFirst),
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

/// The streaming uploads in progress on this thread's client.
#[derive(Default)]
struct Uploads(HashMap<u64, Upload>);

struct Upload {
    upload: jazz_app::jazz::tools::StreamingUpload,
    kind: jazz_app::jazz::db::StreamingMutationKind,
    id: Option<jazz_app::ObjectId>,
    table: TableSchema,
    name: String,
}

impl Drop for Uploads {
    fn drop(&mut self) {
        // As for transactions: the client is gone when the thread exits.
        for entry in std::mem::take(&mut self.0).into_values() {
            std::mem::forget(entry);
        }
    }
}

thread_local! {
    static UPLOADS: RefCell<Uploads> = RefCell::new(Uploads::default());
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
fn client_for(client: JazzClient, tx: Option<u64>) -> jazz_app::Result<JazzClient> {
    let Some(tx) = tx else {
        return Ok(client);
    };
    TRANSACTIONS.with(|transactions| {
        let transactions = transactions.borrow();
        let (transaction, _) = transactions
            .0
            .get(&tx)
            .ok_or_else(|| jazz_app::Error::Config(format!("transaction {tx} is not open")))?;
        Ok(transaction.client().clone())
    })
}

/// Records that a write inside `tx` succeeded, so its commit commits.
fn mark_wrote(tx: Option<u64>) {
    if let Some(tx) = tx {
        TRANSACTIONS.with(|transactions| {
            if let Some((_, wrote)) = transactions.borrow_mut().0.get_mut(&tx) {
                *wrote = true;
            }
        });
    }
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
        Self::open_with_tokens(config_json, None)
    }

    /// [`Self::open`] for an end user signing in with the app's bearer
    /// (`"auth": { "bearer": true }`): `tokens` gives it.
    pub fn open_with_tokens(
        config_json: &str,
        tokens: Option<Arc<dyn jazz_app::TokenProvider>>,
    ) -> Result<Self> {
        let (files, config, app_text) = config(config_json, tokens)?;
        let contract = convert::parse(config_json, "the config")?
            .get("contract")
            .and_then(Json::as_u64)
            .unwrap_or(1);
        let has_server = config.server_url.is_some();
        let jazz = block_on(Jazz::open(files, config))?;
        let schema = block_on(jazz.with_client(|client| async move { Ok(client.schema()?) }))?;
        Ok(Self {
            jazz,
            schema: Arc::new(schema),
            relations: convert::Relations::read(&app_text),
            closed: AtomicBool::new(false),
            subscriptions: Mutex::new(Vec::new()),
            // Ids start at a random point, so a stale id from another
            // handle (or an earlier run) fails cleanly instead of naming
            // another transaction.
            next_tx: AtomicU64::new(AppId::random().uuid().as_u64_pair().0 >> 12 | 1),
            has_server,
            contract,
            close_hook: Mutex::new(None),
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

    /// The permissions bundle a backend published and is pinned to.
    pub fn bundle(&self) -> Result<Option<String>> {
        Ok(self.open_jazz()?.bundle().map(str::to_owned))
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
        let parsed = convert::parse(query_json, "the query")?;
        if crate::relational::is_relational(&parsed) {
            return Ok(self
                .relational_rows(&parsed, opts, Some(1))?
                .into_iter()
                .next()
                .map(|row| row.to_string()));
        }
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
        if crate::relational::is_relational(query) {
            return self.relational_rows(query, opts, None);
        }
        let (_, query, shape) = convert::query(&self.schema, &self.relations, query)?;
        self.run_query(shape, query, opts)
    }

    /// hopTo / gather / union / history / includeDeleted queries.
    fn relational_rows(
        &self,
        query: &Json,
        opts: &Opts,
        limit: Option<usize>,
    ) -> Result<Vec<Json>> {
        // A limit goes in before lowering: a relation query carries its
        // window inside the relation, not as an ordinary clause.
        let limited;
        let query = match limit {
            Some(limit) => {
                limited = crate::relational::with_limit(query, limit);
                &limited
            }
            None => query,
        };
        let lowered = crate::relational::lower(&self.schema, &self.relations, query)?;
        let (tx, tier) = (opts.tx, opts.read_tier.unwrap_or(ReadTier::LocalFirst));
        let core_query = lowered.query.clone();
        let reads_deleted = lowered.reads_deleted;
        let results = block_on(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let mut read = jazz_app::jazz::db::ReadOpts::for_read_tier(tier);
            read.read_view.include_deleted = reads_deleted;
            Ok(client.query_with_opts(core_query, read).await?)
        }))?;
        results
            .into_iter()
            .map(|row| crate::relational::row(&lowered, row))
            .collect()
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
            let client = client_for(client, tx)?;
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

    /// Global durability needs a server: refused before anything is written.
    fn reachable(&self, tier: Option<DurabilityTier>) -> Result<()> {
        if tier == Some(DurabilityTier::GlobalServer) && !self.has_server {
            return Err(refused(
                "global durability is unreachable: this app has no server (\"serverUrl\")",
            ));
        }
        Ok(())
    }

    pub fn insert(&self, table_name: &str, row_json: &str, opts: &Opts) -> Result<String> {
        self.reachable(opts.write_tier)?;
        let table = self.table(table_name)?.clone();
        let (id, cells) = convert::cells(&table, table_name, row_json, "the row")?;
        let cells: HashMap<String, Value> = cells.into_iter().collect();
        let name = table_name.to_owned();
        let jazz = self.open_jazz()?;
        let (tx, tier) = (opts.tx, opts.write_tier);
        let row = block_on(jazz.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let (id, values, committed) =
                client.insert_with_id(&name, id.map(|id| *id.uuid()), cells)?;
            mark_wrote(tx);
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

    /// Writes the row with this id: inserts it if there is none (then
    /// `row_json` is a whole row, as for an insert), otherwise sets the
    /// columns `row_json` names (a partial row updates). Returns the row.
    pub fn upsert(
        &self,
        table_name: &str,
        id: &str,
        row_json: &str,
        opts: &Opts,
    ) -> Result<String> {
        self.reachable(opts.write_tier)?;
        let id = convert::parse_id(id, "the id")?;
        let table = self.table(table_name)?.clone();
        let (given, cells) = convert::cells(&table, table_name, row_json, "the row")?;
        if given.is_some_and(|given| given != id) {
            return Err(refused("the row's id must be the upserted id"));
        }
        let name = table_name.to_owned();
        let (tx, tier) = (opts.tx, opts.write_tier);
        let row = block_on(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let committed = client.upsert(&name, *id.uuid(), cells.into_iter().collect())?;
            mark_wrote(tx);
            settle(&client, committed, tier).await?;
            read_back(&client, &table, &name, id).await
        }))?;
        Ok(row.to_string())
    }

    /// Brings back a deleted row, as it was or with `row_json` as its new
    /// content (a whole row: the engine replaces the content atomically).
    /// Returns the row.
    pub fn restore(
        &self,
        table_name: &str,
        id: &str,
        patch_json: Option<&str>,
        opts: &Opts,
    ) -> Result<String> {
        self.reachable(opts.write_tier)?;
        let id = convert::parse_id(id, "the id")?;
        let table = self.table(table_name)?.clone();
        let cells = match patch_json {
            Some(patch) => {
                let (named, cells) = convert::cells(&table, table_name, patch, "the patch")?;
                if named.is_some_and(|named| named != id) {
                    return Err(refused("a restore's patch cannot name another row's id"));
                }
                Some(cells)
            }
            None => None,
        };
        let name = table_name.to_owned();
        let (tx, tier) = (opts.tx, opts.write_tier);
        let row = block_on(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let committed = client.restore(&name, id, cells)?;
            mark_wrote(tx);
            settle(&client, committed, tier).await?;
            read_back(&client, &table, &name, id).await
        }))?;
        Ok(row.to_string())
    }

    pub fn update(&self, table_name: &str, id: &str, patch_json: &str, opts: &Opts) -> Result<()> {
        self.reachable(opts.write_tier)?;
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
            let client = client_for(client, tx)?;
            let committed = client.update(&name, id, cells)?;
            mark_wrote(tx);
            settle(&client, committed, tier).await
        }))?;
        Ok(())
    }

    /// Updates a row in place: `patch_json` sets whole columns (may be
    /// `None`), `edits_json` edits text, bytes and JSON values partially
    /// (see [`convert::edits`]); all in one write.
    pub fn edit(
        &self,
        table_name: &str,
        id: &str,
        patch_json: Option<&str>,
        edits_json: &str,
        opts: &Opts,
    ) -> Result<()> {
        self.reachable(opts.write_tier)?;
        let id = convert::parse_id(id, "the id")?;
        let table = self.table(table_name)?;
        let cells = match patch_json {
            Some(patch) => {
                let (patched_id, cells) = convert::cells(table, table_name, patch, "the patch")?;
                if patched_id.is_some_and(|patched| patched != id) {
                    return Err(refused("a patch cannot change a row's id"));
                }
                cells
            }
            None => Vec::new(),
        };
        let edits = convert::edits(table, table_name, &convert::parse(edits_json, "the edits")?)?;
        let name = table_name.to_owned();
        let (tx, tier) = (opts.tx, opts.write_tier);
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let edits = whole_pages(&client, &name, id, edits).await?;
            let committed = client
                .update_with_edits(&name, id, cells, edits)
                .map_err(|error| match error {
                    // As jazz-app reports an edit of a row this client
                    // cannot see.
                    jazz_app::jazz::tools::JazzError::Write(message)
                        if message.contains("NotObserved") =>
                    {
                        jazz_app::Error::Config(format!("no {name} row {id}"))
                    }
                    other => other.into(),
                })?;
            mark_wrote(tx);
            settle(&client, committed, tier).await
        }))?;
        Ok(())
    }

    pub fn delete(&self, table_name: &str, id: &str, opts: &Opts) -> Result<()> {
        self.reachable(opts.write_tier)?;
        let id = convert::parse_id(id, "the id")?;
        self.table(table_name)?;
        let name = table_name.to_owned();
        let (tx, tier) = (opts.tx, opts.write_tier);
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let committed = client.delete(&name, id)?;
            mark_wrote(tx);
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
        let parsed = convert::parse(query_json, "the query")?;
        let (query, table, reads_deleted) = if crate::relational::is_relational(&parsed) {
            let lowered = crate::relational::lower(&self.schema, &self.relations, &parsed)?;
            if lowered.history {
                return Err(unsupported(
                    "subscribing to history: all versions of a row share its id",
                ));
            }
            (lowered.query, lowered.shape, lowered.reads_deleted)
        } else {
            let (_, query, shape) = convert::query(&self.schema, &self.relations, &parsed)?;
            (query, shape, false)
        };
        let (events, received) = mpsc::channel::<Option<Event>>();
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
        let forward = events.clone();
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let mut read = jazz_app::jazz::db::ReadOpts::for_read_tier(tier);
            read.read_view.include_deleted = reads_deleted;
            let mut stream = client.subscribe_with_opts(query, read).await?;
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
                            let reason = format!("{reason:?}");
                            Event::Error(JazzError::from(crate::error::rejection(
                                &reason,
                                format!("subscription rejected: {reason}"),
                            )))
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

    /// The server's advice on whether this user may do `op` (`read`,
    /// `insert`, `update`, `delete`) without doing it: `Some(true)`,
    /// `Some(false)`, or `None` when no server can say (TS's `canRead` …).
    /// Advisory: `true` doesn't promise the write will succeed (the data or
    /// the permissions may change before it runs). Not in a transaction.
    /// `insert` takes `row_json`; `read` and `delete` take `id`; `update`
    /// takes `id` and an optional `row_json` patch.
    pub fn can(
        &self,
        table_name: &str,
        op: &str,
        id: Option<&str>,
        row_json: Option<&str>,
    ) -> Result<Option<bool>> {
        use jazz_app::jazz::tools::PermissionAdviceRequest as Request;
        let table = self.table(table_name)?;
        let id = || -> Result<jazz_app::ObjectId> {
            convert::parse_id(
                id.ok_or_else(|| refused(format!("can {op} needs an \"id\"")))?,
                "the id",
            )
        };
        let cells = |what: &str| -> Result<Vec<(String, Value)>> {
            match row_json {
                Some(row) => {
                    let (given, cells) = convert::cells(table, table_name, row, what)?;
                    if given.is_some() {
                        return Err(refused(format!(
                            "can {op}: {what} takes no \"id\" (advice is about the row's columns)"
                        )));
                    }
                    Ok(cells)
                }
                None => Ok(Vec::new()),
            }
        };
        let table = table_name.to_owned();
        let request = match op {
            "read" => Request::Read { table, row: id()? },
            "delete" => Request::Delete { table, row: id()? },
            "insert" => {
                if row_json.is_none() {
                    return Err(refused("can insert needs a \"row\""));
                }
                Request::Insert {
                    table,
                    values: cells("the row")?.into_iter().collect(),
                }
            }
            "update" => Request::Update {
                table,
                row: id()?,
                values: cells("the patch")?,
            },
            other => {
                return Err(refused(format!(
                    "op must be \"read\", \"insert\", \"update\" or \"delete\", not {other:?}"
                )));
            }
        };
        let advice = block_on(self.open_jazz()?.with_client(move |client| async move {
            Ok(client.permission_advice(request).await?)
        }))?;
        use jazz_app::jazz::protocol::PermissionAdvice as Advice;
        Ok(match advice {
            Advice::Allowed => Some(true),
            Advice::Denied => Some(false),
            Advice::Unknown => None,
        })
    }

    /// Begins uploading a large value of `column` piece by piece: of a new
    /// row (`id` none: an insert, its other columns `row_json`), or of the
    /// row with `id` (`kind` `update` or `upsert`). Returns the upload's id.
    pub fn upload_begin(
        &self,
        table_name: &str,
        column_name: &str,
        kind: &str,
        id: Option<&str>,
        row_json: Option<&str>,
    ) -> Result<String> {
        use jazz_app::jazz::db::StreamingMutationKind as Kind;
        let table = self.table(table_name)?.clone();
        convert::column(&table, table_name, column_name)?;
        let kind = match kind {
            "insert" => Kind::Insert,
            "update" => Kind::Update,
            "upsert" => Kind::Upsert,
            other => {
                return Err(refused(format!(
                    "an upload's kind is \"insert\", \"update\" or \"upsert\", not {other:?}"
                )));
            }
        };
        let id = id.map(|id| convert::parse_id(id, "the id")).transpose()?;
        if id.is_none() != matches!(kind, Kind::Insert) {
            return Err(refused(
                "an insert upload takes no \"id\"; an update or upsert needs one",
            ));
        }
        let cells = match row_json {
            Some(row) => {
                let (named, cells) = convert::cells(&table, table_name, row, "the row")?;
                if named.is_some() {
                    return Err(refused(
                        "an upload's row takes no \"id\": give it as the upload's id (update/upsert)",
                    ));
                }
                cells
            }
            None => Vec::new(),
        };
        let upload_id = self.next_tx.fetch_add(1, Ordering::Relaxed);
        let (name, column) = (table_name.to_owned(), column_name.to_owned());
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let upload =
                client.begin_streaming_upload(&name, cells.into_iter().collect(), &column)?;
            UPLOADS.with(|u| {
                u.borrow_mut().0.insert(
                    upload_id,
                    Upload {
                        upload,
                        kind,
                        id,
                        table,
                        name,
                    },
                )
            });
            Ok(())
        }))?;
        Ok(upload_id.to_string())
    }

    /// Feeds the next piece of an upload.
    pub fn upload_push(&self, upload: u64, bytes: Vec<u8>) -> Result<()> {
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let mut entry = UPLOADS
                .with(|u| u.borrow_mut().0.remove(&upload))
                .ok_or_else(|| jazz_app::Error::Config(format!("upload {upload} is not open")))?;
            let pushed = client
                .push_streaming_upload(&mut entry.upload, &bytes)
                .await;
            UPLOADS.with(|u| u.borrow_mut().0.insert(upload, entry));
            Ok(pushed?)
        }))?;
        Ok(())
    }

    /// Finishes an upload, committing the row with its value; returns the
    /// row.
    pub fn upload_finish(&self, upload: u64, opts: &Opts) -> Result<String> {
        self.reachable(opts.write_tier)?;
        let tier = opts.write_tier;
        let row = block_on(self.open_jazz()?.with_client(move |client| async move {
            let entry = UPLOADS
                .with(|u| u.borrow_mut().0.remove(&upload))
                .ok_or_else(|| jazz_app::Error::Config(format!("upload {upload} is not open")))?;
            let (id, committed) = client
                .finish_streaming_upload(entry.upload, entry.kind, entry.id)
                .await?;
            settle(&client, Some(committed), tier).await?;
            read_back(&client, &entry.table, &entry.name, id).await
        }))?;
        Ok(row.to_string())
    }

    /// Abandons an upload: nothing is written.
    pub fn upload_abort(&self, upload: u64) -> Result<()> {
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let entry = UPLOADS
                .with(|u| u.borrow_mut().0.remove(&upload))
                .ok_or_else(|| jazz_app::Error::Config(format!("upload {upload} is not open")))?;
            Ok(client.abort_streaming_upload(entry.upload).await?)
        }))?;
        Ok(())
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
        if let Err(error) = self.reachable(tier) {
            // Refused before committing: the transaction ends, as on any
            // failed commit.
            let _ = self.tx_rollback(tx);
            return Err(error);
        }
        block_on(self.open_jazz()?.with_client(move |client| async move {
            let (transaction, wrote) = take_tx(tx)?;
            if !wrote {
                transaction.rollback()?;
                return Ok(());
            }
            let id = transaction.transaction_id();
            let committed = match transaction.commit() {
                Ok(committed) => committed,
                Err(error) => {
                    // A failed commit must not leave the core transaction
                    // open where nothing can reach it.
                    let _ = client.rollback_transaction(id);
                    return Err(error.into());
                }
            };
            settle(&client, Some(committed), tier).await
        }))?;
        Ok(())
    }

    /// Waits until every write committed so far reaches `tier`. Local
    /// writes are durable locally once they return, so `Local` has nothing
    /// to wait for; `GlobalServer` waits for the server to have them all
    /// and fails if it rejected one (the core's `wait_for_sync`, as TS's
    /// `waitForSync`). Without a server there is nothing to wait for. It
    /// checks up to the last 4096 writes individually; older ones rely on
    /// the core wait, which (until jazz#5728) can miss a refusal that is
    /// not among the writes still pending.
    pub fn wait_for_sync(&self, tier: DurabilityTier) -> Result<()> {
        if tier == DurabilityTier::Local {
            self.open_jazz()?;
            return Ok(());
        }
        block_on(self.open_jazz()?.with_client(|client| async move {
            // Taken first, so a refusal is reported by one call only.
            let writes = UNSYNCED.with(|unsynced| std::mem::take(&mut *unsynced.borrow_mut()));
            // Every pending write reaches the server (or is refused)...
            client.wait_for_sync().await?;
            // ...and each write since the last sync is checked on its own:
            // a refusal followed by an accepted write still fails here.
            for write in writes {
                client
                    .wait_for_transaction(write, DurabilityTier::GlobalServer)
                    .await?;
            }
            Ok(())
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

    /// The end user's session as JSON (`{ "status": "signedIn", "user": … }`
    /// and so on); `None` for a local app or a backend.
    pub fn session_state(&self) -> Result<Option<String>> {
        Ok(self
            .open_jazz()?
            .session()
            .map(|session| serde_json::to_string(&session.state()).unwrap_or_default()))
    }

    /// The connection's status as JSON (`{ "link": … }`).
    pub fn sync_status(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.open_jazz()?.sync_status()).unwrap_or_default())
    }

    /// Asks the token provider for a fresh token and reconnects with it.
    pub fn refresh(&self) -> Result<()> {
        let session = self
            .open_jazz()?
            .session()
            .cloned()
            .ok_or_else(|| refused("refresh is for an end user's app (\"auth\")"))?;
        block_on(session.refresh())?;
        Ok(())
    }

    /// Links this device account to the user's account with the app's
    /// own sign-in (`bearer`, a token from it): the session becomes that
    /// user's, on the device's account. Returns the user as JSON.
    pub fn link(&self, bearer: &str) -> Result<String> {
        let session = self
            .open_jazz()?
            .session()
            .cloned()
            .ok_or_else(|| refused("link is for an end user's app (\"auth\")"))?;
        let bearer = bearer.to_owned();
        // On the client's runtime: linking talks to the app's endpoint
        // (tokio I/O), and the caller's thread may have no runtime at all.
        let user = block_on(
            self.open_jazz()?
                .with_client(move |_| async move { session.link(&bearer).await }),
        )?;
        Ok(serde_json::to_string(&user).unwrap_or_default())
    }

    /// Signs the end user out and closes the app.
    pub fn sign_out(&self) -> Result<()> {
        let jazz = self.open_jazz()?;
        if jazz.session().is_none() {
            return Err(refused("sign out is for an end user's app (\"auth\")"));
        }
        block_on(jazz.sign_out())?;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    /// Calls `listener` with each state of the session (`session`) or the
    /// connection (`sync`) as JSON, the current one first, on a thread of
    /// its own, until cancelled.
    pub fn watch(&self, what: &str, listener: Box<dyn Fn(String) + Send>) -> Result<Watch> {
        use futures_util::StreamExt as _;
        let jazz = self.open_jazz()?;
        let mut stream: std::pin::Pin<Box<dyn futures_util::Stream<Item = String> + Send>> =
            match what {
                "session" => {
                    let session = jazz
                        .session()
                        .ok_or_else(|| refused("watching the session is for an end user's app"))?;
                    Box::pin(
                        session
                            .changes()
                            .map(|state| serde_json::to_string(&state).unwrap_or_default()),
                    )
                }
                "sync" => Box::pin(
                    jazz.sync_changes()
                        .map(|status| serde_json::to_string(&status).unwrap_or_default()),
                ),
                other => {
                    return Err(refused(format!(
                        "watch \"session\" or \"sync\", not {other:?}"
                    )));
                }
            };
        let (cancel, mut cancelled) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("jazz-watch".into())
            .spawn(move || {
                block_on(async move {
                    loop {
                        let next =
                            futures_util::future::select(stream.next(), &mut cancelled).await;
                        match next {
                            futures_util::future::Either::Left((Some(state), _)) => listener(state),
                            _ => return,
                        }
                    }
                })
            })
            .map_err(|error| FfiError::Io(format!("cannot start the watch's thread: {error}")))?;
        Ok(Watch {
            thread_id: thread.thread().id(),
            thread: Mutex::new(Some(thread)),
            cancel: Mutex::new(Some(cancel)),
        })
    }

    /// Closes the app: the calls already made finish, subscriptions end,
    /// and every later call fails with `Closed`. Closing again does nothing.
    /// Runs `hook` first when the app closes (once).
    pub(crate) fn on_close(&self, hook: Box<dyn FnOnce() + Send>) {
        *self.close_hook.lock().unwrap_or_else(|e| e.into_inner()) = Some(hook);
    }

    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Some(hook) = self
            .close_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            hook();
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

/// The row with this id as the writer now sees it.
async fn read_back(
    client: &JazzClient,
    table: &TableSchema,
    name: &str,
    id: jazz_app::ObjectId,
) -> jazz_app::Result<Json> {
    use jazz_app::jazz::query as q;
    let mut query = q::Query::from(name);
    query.filters.push(q::eq(
        q::col("id"),
        q::lit(jazz_app::jazz::records::Value::Uuid(*id.uuid())),
    ));
    let row = client
        .query(query, ReadTier::LocalFirst)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| jazz_app::Error::Config(format!("no {name} row {id}")))?;
    Ok(convert::row_json(
        &convert::Shape::of(table.clone()),
        id,
        row.fields,
    ))
}

/// Resolves splice pages given as the whole value to the value's current
/// length, read as the edit's writer sees it.
async fn whole_pages(
    client: &JazzClient,
    table: &str,
    id: jazz_app::ObjectId,
    edits: Vec<jazz_app::jazz::db::LargeValueUpdate>,
) -> jazz_app::Result<Vec<jazz_app::jazz::db::LargeValueUpdate>> {
    use jazz_app::jazz::db::{LargeValueUpdate, LargeValueUpdatePage as Page};
    use jazz_app::jazz::query as q;
    let mut resolved = Vec::with_capacity(edits.len());
    for edit in edits {
        let LargeValueUpdate::Splice {
            column,
            within,
            splices,
        } = edit
        else {
            resolved.push(edit);
            continue;
        };
        let to = match &within {
            Page::Bytes { to, .. } | Page::TextUtf8 { to, .. } | Page::TextUtf16 { to, .. } => *to,
        };
        if to != convert::WHOLE_VALUE {
            resolved.push(LargeValueUpdate::Splice {
                column,
                within,
                splices,
            });
            continue;
        }
        let mut query = q::Query::from(table);
        query.filters.push(q::eq(
            q::col("id"),
            q::lit(jazz_app::jazz::records::Value::Uuid(*id.uuid())),
        ));
        query.select = Some(vec![column.clone()]);
        let rows = client.query(query, ReadTier::LocalOnly).await?;
        let value = rows
            .into_iter()
            .next()
            .and_then(|row| row.fields.into_iter().find(|f| f.name == column))
            .map(|field| field.value)
            .ok_or_else(|| jazz_app::Error::Config(format!("no {table} row {id}")))?;
        let len = |page: &Page| -> u64 {
            match (&value, page) {
                (Value::Text(text), Page::TextUtf16 { .. }) => text.encode_utf16().count() as u64,
                (Value::Text(text), _) => text.len() as u64,
                (Value::Bytea(bytes), _) => bytes.len() as u64,
                _ => 0,
            }
        };
        let within = match within {
            page @ Page::Bytes { .. } => Page::Bytes {
                from: 0,
                to: len(&page),
            },
            page @ Page::TextUtf8 { .. } => Page::TextUtf8 {
                from: 0,
                to: len(&page),
            },
            page @ Page::TextUtf16 { .. } => Page::TextUtf16 {
                from: 0,
                to: len(&page),
            },
        };
        resolved.push(LargeValueUpdate::Splice {
            column,
            within,
            splices,
        });
    }
    Ok(resolved)
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
        match tier {
            // Confirmed by the server: nothing for waitForSync to check.
            Some(DurabilityTier::GlobalServer) => {
                client
                    .wait_for_transaction(committed, DurabilityTier::GlobalServer)
                    .await?;
            }
            tier => {
                UNSYNCED.with(|unsynced| {
                    let mut unsynced = unsynced.borrow_mut();
                    // Bounded for hosts that never call waitForSync: the
                    // oldest unchecked writes are left to the core's wait.
                    if unsynced.len() >= MAX_UNSYNCED {
                        unsynced.pop_front();
                    }
                    unsynced.push_back(committed);
                });
                if let Some(tier) = tier {
                    client.wait_for_transaction(committed, tier).await?;
                }
            }
        }
    }
    Ok(())
}

/// How many unchecked writes waitForSync remembers.
const MAX_UNSYNCED: usize = 4096;

thread_local! {
    /// The writes this app's client committed since its last waitForSync.
    static UNSYNCED: RefCell<std::collections::VecDeque<jazz_app::jazz::tools::TransactionId>> =
        const { RefCell::new(std::collections::VecDeque::new()) };
}

/// A watch of session or sync states.
pub(crate) struct Watch {
    thread_id: ThreadId,
    thread: Mutex<Option<JoinHandle<()>>>,
    cancel: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl Watch {
    /// Ends the watch; when it returns no callback is running (unless it is
    /// called from the callback itself).
    pub fn cancel(&self) {
        if let Some(cancel) = self.cancel.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = cancel.send(());
        }
        if std::thread::current().id() == self.thread_id {
            return;
        }
        if let Some(thread) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = cancel.send(());
        }
    }
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

/// A host's token source as jazz-app's.
struct Tokens(Arc<dyn jazz_app::TokenProvider>);

impl jazz_app::TokenProvider for Tokens {
    fn token(&self, reason: jazz_app::TokenReason) -> jazz_app::TokenFuture {
        self.0.token(reason)
    }
}

/// A host callback that gives tokens, as a jazz-app [`TokenProvider`]: it is
/// called on a thread of its own (it may block, and must not be called on
/// the client's thread), with the reason as `connect`, `renew`, `expired`
/// or `refresh`.
pub(crate) struct HostTokens(pub Arc<HostTokenFn>);

/// A host's token callback: the reason in, the token (or none: signed out)
/// out, or an error (the fetch failed: the session stays, and asks again).
pub(crate) type HostTokenFn =
    dyn Fn(&str) -> std::result::Result<Option<String>, String> + Send + Sync;

impl jazz_app::TokenProvider for HostTokens {
    fn token(&self, reason: jazz_app::TokenReason) -> jazz_app::TokenFuture {
        let callback = self.0.clone();
        let reason = serde_json::to_value(reason)
            .ok()
            .and_then(|r| r.as_str().map(str::to_owned))
            .unwrap_or_else(|| "connect".into());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("jazz-token".into())
            .spawn(move || {
                // A panicking callback is a failed fetch, not a sign-out.
                let token =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(&reason)))
                        .unwrap_or_else(|_| Err("the token provider panicked".into()));
                let _ = sender.send(token);
            });
        Box::pin(async move {
            spawned.map_err(|error| {
                jazz_app::Error::Config(format!("cannot ask for a token: {error}"))
            })?;
            receiver
                .await
                .map_err(|_| jazz_app::Error::Stopped)?
                .map_err(|reason| {
                    jazz_app::Error::Server(format!("the token provider failed: {reason}"))
                })
        })
    }
}

/// A new device account's key, as `jazz-auth-v1:…` text (TS's
/// `exportDeviceKey`): keep it as a password, in the platform's keychain.
pub(crate) fn generate_device_key() -> Result<String> {
    Ok(jazz_app::DeviceKey::generate()?.to_string())
}

fn config(
    config_json: &str,
    tokens: Option<Arc<dyn jazz_app::TokenProvider>>,
) -> Result<(AppFiles, Config, String)> {
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
    let mut auth = None;
    let mut insecure = false;
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
            "auth" => auth = Some(value.clone()),
            // An end user over plain http:// to a host other than
            // localhost, for development only.
            "allowInsecureHttp" => {
                insecure = value
                    .as_bool()
                    .ok_or_else(|| refused("config.allowInsecureHttp must be true or false"))?;
            }
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
                return Err(refused(format!(
                    "config.{key}: an end user's token comes from a token provider: \"auth\": {{ \"bearer\": true }}"
                )));
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
        if auth.is_some() {
            return Err(refused(
                "\"auth\" is for an end user; a backend has a \"session\"",
            ));
        }
    } else if let Some(auth) = auth {
        // An end user, syncing through the app's Jazz endpoint.
        if result.server_url.is_none() {
            return Err(refused(
                "an end user (\"auth\") needs \"serverUrl\": the app's Jazz endpoint",
            ));
        }
        let auth = match (auth.get("deviceKey"), auth.get("bearer")) {
            (Some(Json::String(key)), None) => {
                jazz_app::Auth::Device(key.parse().map_err(|_| {
                    refused("config.auth.deviceKey is not a device key (jazz-auth-v1:…)")
                })?)
            }
            (None, Some(Json::Bool(true))) => {
                let tokens = tokens.ok_or_else(|| {
                    refused("\"auth\": { \"bearer\": true } needs a token provider: open with one")
                })?;
                jazz_app::Auth::bearer(Tokens(tokens))
            }
            _ => {
                return Err(refused(
                    "config.auth is { \"deviceKey\": \"jazz-auth-v1:…\" } or { \"bearer\": true }",
                ));
            }
        };
        result = result.user(auth);
        if insecure {
            result = result.allow_insecure_http();
        }
    } else if result.server_url.is_some() {
        return Err(refused(
            "a \"serverUrl\" needs \"permissions\" (a backend) or \"auth\" (an end user)",
        ));
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

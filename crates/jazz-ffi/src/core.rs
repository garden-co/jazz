//! The dynamic handle both surfaces wrap: blocking, JSON in and out.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread::{JoinHandle, ThreadId};

use futures_executor::block_on;
use jazz_app_support::jazz::tools::{
    DurabilityTier, JazzClient, JazzTransaction, ReadTier, Schema, SubscriptionStreamItem,
    TableSchema, Value,
};
use jazz_app_support::{AppFiles, AppId, Backend, Config, Jazz, Storage};
use serde_json::Value as Json;

use crate::JazzError;
use crate::closing::{Closed, closed_if};
use crate::convert;
use crate::error::{FfiError, Result, refused, unsupported};
use crate::gate::HostGate;

/// The issuer jazz-app-support names a backend's users by.
const BACKEND_ISSUER: &str = "jazz:backend";

pub(crate) struct Core {
    jazz: Jazz,
    schema: Arc<Schema>,
    relations: convert::Relations,
    /// What a job needs of this handle on the client's thread.
    open: Open,
    /// A session's handle: whether its user has changed since it was
    /// fetched (every call then fails `Replaced`, even one answered without
    /// the client).
    replaced: Option<Box<dyn Fn() -> bool + Send + Sync>>,
    /// The open subscriptions, told when the app closes.
    subscriptions: Mutex<Vec<Weak<Delivery>>>,
    /// The open watches, told when the app closes: each gets a last
    /// `None` (closed).
    watches: Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    /// Transaction and upload ids: shared with a `for_user` handle's
    /// parent, so the ids in the client's tables never collide.
    next_tx: Arc<AtomicU64>,
    /// Whether the app syncs with a server: global durability needs one.
    has_server: bool,
    /// The contract version the host speaks (`"contract"` in the config):
    /// 1 unless it says 2.
    pub(crate) contract: u64,
    /// A C host's callbacks: shut as the app closes, waited for before
    /// close returns and again before the handle is freed.
    gates: Mutex<Vec<Arc<dyn HostGate>>>,
    /// A trusted backend client's handle acting for a user (`for_user`): it
    /// shares its parent's client, so closing it leaves that client running.
    derived: bool,
}

/// A handle's state that its jobs take with them to the client's thread.
#[derive(Clone)]
struct Open {
    closed: Closed,
    /// The `closed` of a derived handle's parent (the trusted client, or
    /// the session): closing the parent closes it too.
    parent: Option<Closed>,
    /// The transactions and uploads this handle began: they share the
    /// client's thread-local tables with every handle on that client (a
    /// trusted client's `for_user` handles), so an id only works on the
    /// handle that made it. Jobs change it on the client's thread, so a
    /// close's clean-up there sees all that was begun before it.
    owned: Arc<Mutex<std::collections::HashSet<u64>>>,
}

impl Open {
    fn is_closed(&self) -> bool {
        self.closed.is_closed() || self.parent.as_ref().is_some_and(Closed::is_closed)
    }

    /// In a job that registers something: closed meanwhile, it mustn't.
    fn check(&self) -> jazz_app_support::Result<()> {
        if self.is_closed() {
            Err(jazz_app_support::Error::Stopped)
        } else {
            Ok(())
        }
    }

    fn owned(&self) -> std::sync::MutexGuard<'_, std::collections::HashSet<u64>> {
        self.owned.lock().unwrap_or_else(|e| e.into_inner())
    }
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
    upload: jazz_app_support::jazz::tools::StreamingUpload,
    kind: jazz_app_support::jazz::db::StreamingMutationKind,
    id: jazz_app_support::ObjectId,
    table: TableSchema,
    name: String,
}

impl Drop for Uploads {
    fn drop(&mut self) {
        #[cfg(test)]
        forgetting(self.0.len());
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
        #[cfg(test)]
        forgetting(self.0.len());
        for entry in std::mem::take(&mut self.0).into_values() {
            std::mem::forget(entry);
        }
    }
}

#[cfg(test)]
thread_local! {
    /// What a test runs on this thread after a call's open check and
    /// before its job is sent.
    static BEFORE_SENDING: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

#[cfg(test)]
fn before_sending() {
    let run = BEFORE_SENDING.with(|before| before.borrow_mut().take());
    if let Some(run) = run {
        run();
    }
}

#[cfg(test)]
thread_local! {
    /// Where a test counts what this client thread's exit forgets. No
    /// destructor of its own: it outlasts the tables'.
    static FORGOTTEN: std::cell::Cell<Option<&'static Forgotten>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
#[derive(Default)]
struct Forgotten {
    tables: AtomicU64,
    entries: AtomicU64,
}

#[cfg(test)]
fn forgetting(count: usize) {
    if let Some(forgotten) = FORGOTTEN.get() {
        forgotten.entries.fetch_add(count as u64, Ordering::SeqCst);
        forgotten.tables.fetch_add(1, Ordering::SeqCst);
    }
}

/// The client a call runs with: the transaction's, or the app's.
fn client_for(client: JazzClient, tx: Option<u64>) -> jazz_app_support::Result<JazzClient> {
    let Some(tx) = tx else {
        return Ok(client);
    };
    TRANSACTIONS.with(|transactions| {
        let transactions = transactions.borrow();
        let (transaction, _) = transactions.0.get(&tx).ok_or_else(|| {
            jazz_app_support::Error::Config(format!("transaction {tx} is not open"))
        })?;
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

/// Chooses the error contract even if opening fails while parsing the config.
pub(crate) fn open_contract(config: &str) -> u64 {
    serde_json::from_str::<Json>(config)
        .ok()
        .and_then(|c| c.get("contract").and_then(Json::as_u64))
        .unwrap_or(1)
}

impl Core {
    #[cfg(test)]
    pub fn open(config_json: &str) -> Result<Self> {
        Self::open_with(config_json, None, None)
    }

    #[cfg(test)]
    pub fn open_with_tokens(
        config_json: &str,
        tokens: Option<Arc<dyn jazz_app_support::TokenProvider>>,
    ) -> Result<Self> {
        Self::open_with(config_json, tokens, None)
    }

    /// Opens an app that may declare end-to-end
    /// encryption: `store` is the device's private store (`"environment"` in
    /// the config names the deployment's environment, default `dev`).
    pub fn open_with(
        config_json: &str,
        tokens: Option<Arc<dyn jazz_app_support::TokenProvider>>,
        store: Option<jazz_app_support::EncryptionConfig>,
    ) -> Result<Self> {
        let (files, config, app_text) = config(config_json, tokens, store)?;
        let contract = convert::parse(config_json, "the config")?
            .get("contract")
            .and_then(Json::as_u64)
            .unwrap_or(1);
        let has_server = config.server_url.is_some();
        // Before opening, so an app whose relations don't resolve opens
        // nothing.
        let relations = convert::Relations::read(&app_text)?;
        let jazz = block_on(Jazz::open(files, config))?;
        let schema = block_on(jazz.with_client(|client| async move { Ok(client.schema()?) }))?;
        // A device's schema as the app sees it: encrypted columns have their
        // plaintext types.
        let schema = jazz.plain_schema(schema);
        Ok(Self::with(
            jazz,
            Arc::new(schema),
            relations,
            has_server,
            contract,
            None,
        ))
    }

    /// A handle on a session's client for its signed-in user: closed with
    /// the session, its ids from the session's counter. Closing it leaves
    /// the session (and its client) open, as a `for_user` handle's close
    /// leaves its parent.
    pub(crate) fn for_session(
        jazz: Jazz,
        schema: Arc<Schema>,
        relations: convert::Relations,
        contract: u64,
        closed: Closed,
        next_tx: Arc<AtomicU64>,
        replaced: Box<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        let mut core = Self::with(
            jazz,
            schema,
            relations,
            true,
            contract,
            Some((closed, next_tx)),
        );
        core.replaced = Some(replaced);
        core
    }

    fn with(
        jazz: Jazz,
        schema: Arc<Schema>,
        relations: convert::Relations,
        has_server: bool,
        contract: u64,
        parent: Option<(Closed, Arc<AtomicU64>)>,
    ) -> Self {
        let derived = parent.is_some();
        let (parent, next_tx) = match parent {
            Some((closed, next_tx)) => (Some(closed), next_tx),
            None => (
                None,
                // Ids start at a random point, so a stale id from another
                // app (or an earlier run) fails cleanly instead of naming
                // another transaction.
                Arc::new(AtomicU64::new(
                    AppId::random().uuid().as_u64_pair().1 >> 12 | 1,
                )),
            ),
        };
        let closed = Closed::new();
        // A derived handle's client lives on, so its close also ends its
        // calls' waits on the client's thread.
        let jazz = if derived {
            jazz.ending_waits_with(closed.signal())
        } else {
            jazz
        };
        Self {
            jazz,
            schema,
            relations,
            open: Open {
                closed,
                parent,
                owned: Arc::default(),
            },
            replaced: None,
            subscriptions: Mutex::new(Vec::new()),
            watches: Mutex::new(Vec::new()),
            next_tx,
            has_server,
            contract,
            gates: Mutex::new(Vec::new()),
            derived,
        }
    }

    /// A backend client's kind, as the main backend decided it: `"trusted"`
    /// or `"scoped"`; `None` for any other app.
    pub fn backend_client(&self) -> Result<Option<&'static str>> {
        Ok(self.open_jazz()?.backend_client().map(|kind| match kind {
            jazz_app_support::BackendClientKind::Trusted => "trusted",
            jazz_app_support::BackendClientKind::Scoped => "scoped",
        }))
    }

    /// Refuses an id this handle didn't begin, as if it weren't open.
    fn own(&self, id: u64, what: &str) -> Result<()> {
        self.open_jazz()?;
        if self.open.owned().contains(&id) {
            Ok(())
        } else if self.open.is_closed() {
            // Closed meanwhile, which ended it.
            Err(FfiError::Closed)
        } else {
            Err(refused(format!("{what} {id} is not open")))
        }
    }

    /// The transaction a call runs in, if this handle began it.
    fn tx_of(&self, opts: &Opts) -> Result<Option<u64>> {
        if let Some(tx) = opts.tx {
            self.own(tx, "transaction")?;
        }
        Ok(opts.tx)
    }

    /// On a trusted backend client, a handle acting as `user_id` (the id the
    /// main backend's `authenticate` names them by), with `claims_json` (an
    /// object, or empty for `{}`) as the claims permissions see. It never
    /// creates an account: `NoAccount` when the user has none yet, and
    /// `Permission` when the main backend refuses the identity (its 403).
    pub fn for_user(&self, user_id: &str, claims_json: &str) -> Result<Self> {
        let claims = if claims_json.trim().is_empty() {
            Json::Object(Default::default())
        } else {
            convert::parse(claims_json, "the claims")?
        };
        if !claims.is_object() {
            return Err(refused("the claims must be a JSON object"));
        }
        if user_id.is_empty() {
            return Err(refused(
                "for_user needs the user's id, as the main backend's authenticate names it",
            ));
        }
        let jazz = block_on(self.open_jazz()?.for_user(user_id, claims)).map_err(|error| {
            if self.open.is_closed() {
                FfiError::Closed
            } else {
                for_user_error(user_id, error)
            }
        })?;
        Ok(Self::with(
            jazz,
            self.schema.clone(),
            self.relations.clone(),
            self.has_server,
            self.contract,
            Some((self.open.closed.clone(), self.next_tx.clone())),
        ))
    }

    /// An error as this host's contract version names it.
    pub(crate) fn surface<T>(&self, result: Result<T>) -> std::result::Result<T, JazzError> {
        result.map_err(|error| JazzError::from(error).for_contract(self.contract))
    }

    /// Fails as any call on this handle now would, before its arguments are
    /// looked at: `Closed` once it (or its parent) closed.
    pub(crate) fn ensure_open(&self) -> Result<()> {
        self.open_jazz().map(drop)
    }

    fn open_jazz(&self) -> Result<&Jazz> {
        let replaced = self.replaced.as_ref().is_some_and(|replaced| replaced());
        // Looked at second: a closing session replaces its handles' client,
        // and they read closed.
        if self.open.is_closed() {
            Err(FfiError::Closed)
        } else if replaced {
            Err(FfiError::Replaced(
                "this client was replaced: the session's user signed out or changed".into(),
            ))
        } else {
            Ok(&self.jazz)
        }
    }

    /// Blocks on a call's answer. A root handle's calls end with its
    /// client, which closing stops. A derived handle's client lives on, so
    /// its close ends the waiting itself, at once (what the call sent may
    /// still apply).
    fn wait<T>(&self, call: impl Future<Output = jazz_app_support::Result<T>>) -> Result<T> {
        #[cfg(test)]
        before_sending();
        let ended = block_on(async {
            tokio::select! {
                // The call first: it is sent, and an answer it has wins.
                biased;
                ended = call => Some(ended),
                () = self.open.closed.wait(), if self.derived => None,
            }
        });
        match ended {
            None => Err(FfiError::Closed),
            Some(ended) => self.answered(ended),
        }
    }

    /// [`Self::wait`] for a call whose answer mustn't be dropped (a write's
    /// confirmation or refusal). Its waits on the server end on the
    /// client's thread as this handle closes, so it is as prompt as the
    /// close, which waits for that thread too.
    ///
    /// The invariant: whatever `call` awaits is local work, or ends with
    /// the client's waits (`waits_ended`). Anything else (a wait for a key,
    /// say) hangs a derived handle's close again.
    fn wait_answer<T>(&self, call: impl Future<Output = jazz_app_support::Result<T>>) -> Result<T> {
        #[cfg(test)]
        before_sending();
        self.answered(block_on(call))
    }

    fn answered<T>(&self, ended: jazz_app_support::Result<T>) -> Result<T> {
        use jazz_app_support::Error;
        match ended {
            // How closing shows in a call in flight: the client stopped, a
            // closing session replaced it, or the clean-up took what it used.
            Err(Error::Stopped | Error::Replaced | Error::Config(_)) if self.open.is_closed() => {
                Err(FfiError::Closed)
            }
            ended => Ok(ended?),
        }
    }

    fn table(&self, name: &str) -> Result<&TableSchema> {
        self.ensure_open()?;
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
        self.ensure_open()?;
        let rows = self.rows(&convert::parse(query_json, "the query")?, opts)?;
        Ok(Json::Array(rows).to_string())
    }

    pub fn one(&self, query_json: &str, opts: &Opts) -> Result<Option<String>> {
        self.ensure_open()?;
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
        let (tx, tier) = (
            self.tx_of(opts)?,
            opts.read_tier.unwrap_or(ReadTier::LocalFirst),
        );
        let core_query = lowered.query.clone();
        let reads_deleted = lowered.reads_deleted;
        let results = self.wait(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let mut read = jazz_app_support::jazz::db::ReadOpts::for_read_tier(tier);
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
        query: jazz_app_support::jazz::query::Query,
        opts: &Opts,
    ) -> Result<Vec<Json>> {
        let jazz = self.open_jazz()?;
        let (tx, tier) = (
            self.tx_of(opts)?,
            opts.read_tier.unwrap_or(ReadTier::LocalFirst),
        );
        let results = self.wait(jazz.with_client(move |client| async move {
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
        self.ensure_open()?;
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
        let (tx, tier) = (self.tx_of(opts)?, opts.write_tier);
        let row = self.wait_answer(jazz.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let (id, values, committed) = jazz_app_support::with_key_wait(|| {
                client.insert_with_id(&name, id.map(|id| *id.uuid()), cells.clone())
            })
            .await?;
            mark_wrote(tx);
            settle(&client, committed, tier).await?;
            let fields = jazz_app_support::inserted_fields(&client, &name, values)?;
            Ok(convert::row_json(&convert::Shape::of(table), id, fields))
        }))?;
        Ok(row.to_string())
    }

    /// Writes the row with this id: inserts it if there is none (then
    /// `row_json` is a whole row, as for an insert), otherwise sets the
    /// columns `row_json` names (a partial row updates). Returns the row as
    /// written here, once it reached the tier.
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
        let (tx, tier) = (self.tx_of(opts)?, opts.write_tier);
        let row = self.wait_answer(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let cells: std::collections::HashMap<_, _> = cells.into_iter().collect();
            let committed =
                jazz_app_support::with_key_wait(|| client.upsert(&name, *id.uuid(), cells.clone()))
                    .await?;
            mark_wrote(tx);
            // Read before the wait, as insert answers with what it wrote: by
            // the time the server accepts it, someone may have deleted or
            // changed the row. A refusal still wins over a failed read.
            let written = read_back(&client, &table, &name, id).await;
            settle(&client, committed, tier).await?;
            written
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
        let (tx, tier) = (self.tx_of(opts)?, opts.write_tier);
        let row = self.wait_answer(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let committed =
                jazz_app_support::with_key_wait(|| client.restore(&name, id, cells.clone()))
                    .await?;
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
        let (tx, tier) = (self.tx_of(opts)?, opts.write_tier);
        self.wait_answer(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let committed =
                jazz_app_support::with_key_wait(|| client.update(&name, id, cells.clone())).await?;
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
        let (tx, tier) = (self.tx_of(opts)?, opts.write_tier);
        self.wait_answer(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let edits = whole_pages(&client, &name, id, edits).await?;
            let committed = client
                .update_with_edits(&name, id, cells, edits)
                .map_err(|error| match error.into_original() {
                    // As jazz-app-support reports an edit of a row this client
                    // cannot see.
                    jazz_app_support::jazz::tools::JazzError::Write(message)
                        if message.contains("NotObserved") =>
                    {
                        jazz_app_support::Error::Config(format!("no {name} row {id}"))
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
        let (tx, tier) = (self.tx_of(opts)?, opts.write_tier);
        self.wait_answer(self.open_jazz()?.with_client(move |client| async move {
            let client = client_for(client, tx)?;
            let committed = jazz_app_support::with_key_wait(|| client.delete(&name, id)).await?;
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
        self.ensure_open()?;
        if opts.tx.is_some() {
            return Err(unsupported("subscribing inside a transaction"));
        }
        let tier = opts.read_tier.unwrap_or(ReadTier::LocalFirst);
        let parsed = convert::parse(query_json, "the query")?;
        let (query, table, reads_deleted, history) = if crate::relational::is_relational(&parsed) {
            // A history subscription's versions share their row's id: a
            // host applies them by `$key`.
            let lowered = crate::relational::lower(&self.schema, &self.relations, &parsed)?;
            (
                lowered.query,
                lowered.shape,
                lowered.reads_deleted,
                lowered.history,
            )
        } else {
            let (_, query, shape) = convert::query(&self.schema, &self.relations, &parsed)?;
            (query, shape, false, false)
        };
        let (events, received) = mpsc::channel::<Option<Event>>();
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
        let forward = events.clone();
        // A root's streams end with its client; a derived handle's client
        // lives on, so closing the handle ends them.
        let handle_closed = self.derived.then(|| self.open.closed.clone());
        self.wait(self.open_jazz()?.with_client(move |client| async move {
            let mut read = jazz_app_support::jazz::db::ReadOpts::for_read_tier(tier);
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
                        // The guard sends the last `Closed`.
                        () = closed_if(&handle_closed) => return,
                    };
                    let event = match item {
                        // The stream ended without a rejection (the client
                        // stopped, or the server ended the query): say so,
                        // so no host waits for a delta that never comes.
                        None => Event::Error(JazzError::Closed),
                        Some(SubscriptionStreamItem::Delta(delta)) => {
                            match convert::delta_json(&table, history, delta, first) {
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
        if self.open.is_closed() {
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
        use jazz_app_support::jazz::tools::PermissionAdviceRequest as Request;
        let table = self.table(table_name)?;
        let id = || -> Result<jazz_app_support::ObjectId> {
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
        let advice = self.wait(self.open_jazz()?.with_client(move |client| async move {
            Ok(client.permission_advice(request).await?)
        }))?;
        use jazz_app_support::jazz::protocol::PermissionAdvice as Advice;
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
        use jazz_app_support::jazz::db::StreamingMutationKind as Kind;
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
        let open = self.open.clone();
        self.wait(self.open_jazz()?.with_client(move |client| async move {
            open.check()?;
            let (id, upload) = client
                .begin_streaming_upload_for(kind, &name, id, cells.into_iter().collect(), &column)
                .await?;
            open.check()?;
            clean_up_before_stop();
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
            open.owned().insert(upload_id);
            Ok(())
        }))?;
        Ok(upload_id.to_string())
    }

    /// Feeds the next piece of an upload.
    pub fn upload_push(&self, upload: u64, bytes: Vec<u8>) -> Result<()> {
        self.own(upload, "upload")?;
        let open = self.open.clone();
        self.wait(self.open_jazz()?.with_client(move |client| async move {
            let mut entry = UPLOADS
                .with(|u| u.borrow_mut().0.remove(&upload))
                .ok_or_else(|| {
                    jazz_app_support::Error::Config(format!("upload {upload} is not open"))
                })?;
            let pushed = client
                .push_streaming_upload(&mut entry.upload, &bytes)
                .await;
            if open.is_closed() {
                // The close's clean-up couldn't see it while it was out.
                let _ = client.abort_streaming_upload(entry.upload).await;
                return Err(jazz_app_support::Error::Stopped);
            }
            UPLOADS.with(|u| u.borrow_mut().0.insert(upload, entry));
            Ok(pushed?)
        }))?;
        Ok(())
    }

    /// Finishes an upload, committing the row with its value; returns the
    /// row.
    pub fn upload_finish(&self, upload: u64, opts: &Opts) -> Result<String> {
        self.own(upload, "upload")?;
        self.reachable(opts.write_tier)?;
        let tier = opts.write_tier;
        let open = self.open.clone();
        let row = self.wait_answer(self.open_jazz()?.with_client(move |client| async move {
            open.owned().remove(&upload);
            let entry = UPLOADS
                .with(|u| u.borrow_mut().0.remove(&upload))
                .ok_or_else(|| {
                    jazz_app_support::Error::Config(format!("upload {upload} is not open"))
                })?;
            let (id, committed) = client
                .finish_streaming_upload(entry.upload, entry.kind, Some(entry.id))
                .await?;
            settle(&client, committed, tier).await?;
            read_back(&client, &entry.table, &entry.name, id).await
        }))?;
        Ok(row.to_string())
    }

    /// Abandons an upload: nothing is written.
    pub fn upload_abort(&self, upload: u64) -> Result<()> {
        self.own(upload, "upload")?;
        let open = self.open.clone();
        self.wait(self.open_jazz()?.with_client(move |client| async move {
            open.owned().remove(&upload);
            let entry = UPLOADS
                .with(|u| u.borrow_mut().0.remove(&upload))
                .ok_or_else(|| {
                    jazz_app_support::Error::Config(format!("upload {upload} is not open"))
                })?;
            Ok(client.abort_streaming_upload(entry.upload).await?)
        }))?;
        Ok(())
    }

    /// Begins a transaction: its reads see a snapshot and its own writes,
    /// and its writes commit together or not at all. Serializable, its
    /// commit is refused (`Conflict`) if what it read changed meanwhile;
    /// mergeable (TS's default `transaction`), its writes merge with
    /// concurrent ones and the commit is never refused as a conflict.
    pub fn tx_begin(&self, serializable: bool) -> Result<String> {
        let tx = self.next_tx.fetch_add(1, Ordering::Relaxed);
        let open = self.open.clone();
        self.wait(self.open_jazz()?.with_client(move |client| async move {
            open.check()?;
            let transaction = if serializable {
                client.begin_transaction()?
            } else {
                client.begin_mergeable_transaction()?
            };
            clean_up_before_stop();
            TRANSACTIONS.with(|t| t.borrow_mut().0.insert(tx, (transaction, false)));
            open.owned().insert(tx);
            Ok(())
        }))?;
        Ok(tx.to_string())
    }

    /// Commits a transaction, waiting for `tier` if given. A transaction
    /// that wrote nothing just ends.
    pub fn tx_commit(&self, tx: u64, tier: Option<DurabilityTier>) -> Result<()> {
        self.own(tx, "transaction")?;
        if let Err(error) = self.reachable(tier) {
            // Refused before committing: the transaction ends, as on any
            // failed commit.
            let _ = self.tx_rollback(tx);
            return Err(error);
        }
        let open = self.open.clone();
        self.wait_answer(self.open_jazz()?.with_client(move |client| async move {
            // Committed or failed, the transaction has ended.
            open.owned().remove(&tx);
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
        self.wait_answer(self.open_jazz()?.with_client(|client| async move {
            // Taken first, so a refusal is reported by one call only.
            let writes = UNSYNCED.with(|unsynced| std::mem::take(&mut *unsynced.borrow_mut()));
            let checked = async {
                // Every pending write reaches the server (or is refused)...
                client.wait_for_sync().await?;
                // ...and each write since the last sync is checked on its own:
                // a refusal followed by an accepted write still fails here.
                for &write in &writes {
                    client
                        .wait_for_transaction(write, DurabilityTier::GlobalServer)
                        .await?;
                }
                Ok(())
            }
            .await;
            if checked.is_err() && client.waits_are_over() {
                // Failed as it closes, cut short or with a refusal its host
                // may not look at: the next waitForSync (the parent's, after
                // a derived handle's close) checks these again. Reporting
                // twice beats never.
                unchecked(writes);
            }
            checked
        }))?;
        Ok(())
    }

    pub fn tx_rollback(&self, tx: u64) -> Result<()> {
        self.own(tx, "transaction")?;
        let open = self.open.clone();
        self.wait(self.open_jazz()?.with_client(move |_| async move {
            open.owned().remove(&tx);
            take_tx(tx)?.0.rollback()?;
            Ok(())
        }))?;
        Ok(())
    }

    /// The end user's session as JSON (`{ "status": "signedIn", "user": … }`
    /// and so on); `None` for a local app or a backend.
    pub fn session_state(&self) -> Result<Option<String>> {
        let jazz = self.open_jazz()?;
        jazz.refuse_for_user("session")?;
        Ok(jazz
            .session()
            .map(|session| serde_json::to_string(&session.state()).unwrap_or_default()))
    }

    /// One end-to-end encryption operation by name, with JSON arguments and result.
    pub fn encryption_call(&self, op: &str, args: &str) -> Result<String> {
        let args = if args.trim().is_empty() {
            Json::Object(Default::default())
        } else {
            convert::parse(args, "the arguments")?
        };
        let encryption = self.open_jazz()?.encryption();
        let result = match op {
            "waitUntilReady" => {
                let timeout = match args.get("timeoutMs") {
                    None => 30_000,
                    Some(given) => given.as_u64().ok_or_else(|| {
                        refused("timeoutMs must be a non-negative whole number of milliseconds")
                    })?,
                };
                Json::from(block_on(
                    encryption.ready(std::time::Duration::from_millis(timeout)),
                )?)
            }
            "state" => block_on(encryption.state())?,
            "reports" => {
                let reports = block_on(encryption.take_reports())?;
                let reports = reports
                    .iter()
                    .map(|report| convert::parse(report, "the encryption report"))
                    .collect::<Result<Vec<_>>>()?;
                Json::from(reports)
            }
            _ => block_on(encryption.call(op, args))?,
        };
        Ok(result.to_string())
    }

    /// The connection's status as JSON (`{ "link": … }`).
    pub fn sync_status(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.open_jazz()?.sync_status()).unwrap_or_default())
    }

    /// Asks the token provider for a fresh token and reconnects with it.
    pub fn refresh(&self) -> Result<()> {
        let jazz = self.open_jazz()?;
        jazz.refuse_for_user("refresh")?;
        let session = jazz
            .session()
            .cloned()
            .ok_or_else(|| refused("refresh is for an end user's app (\"auth\")"))?;
        match self.wait(session.refresh()) {
            // Whatever a fetch cut short by closing reports.
            Err(_) if self.open.is_closed() => Err(FfiError::Closed),
            refreshed => refreshed,
        }
    }

    /// Links this device account to the user's account with the app's
    /// own sign-in (`bearer`, a token from it): the session becomes that
    /// user's, on the device's account. Returns the user as JSON.
    pub fn link(&self, bearer: &str) -> Result<String> {
        let jazz = self.open_jazz()?;
        jazz.refuse_for_user("link")?;
        let session = jazz
            .session()
            .cloned()
            .ok_or_else(|| refused("link is for an end user's app (\"auth\")"))?;
        let bearer = bearer.to_owned();
        // On the client's runtime: linking talks to the app's endpoint
        // (tokio I/O), and the caller's thread may have no runtime at all.
        let user = self.wait(self.open_jazz()?.with_client(move |client| async move {
            tokio::select! {
                biased;
                linked = session.link(&bearer) => linked,
                // Closing: this handle's or the client's.
                () = client.waits_ended() => Err(jazz_app_support::Error::Stopped),
            }
        }))?;
        Ok(serde_json::to_string(&user).unwrap_or_default())
    }

    /// Signs the end user out and closes the app; a session's handle
    /// refuses and stays open.
    pub fn sign_out(&self) -> Result<()> {
        let jazz = self.open_jazz()?;
        jazz.refuse_for_user("signOut")?;
        if jazz.session().is_none() {
            return Err(refused("sign out is for an end user's app (\"auth\")"));
        }
        // Derived handles never get this far: a for_user handle is refused
        // above, and a session's handle by jazz-app (the session signs out).
        self.wait(jazz.sign_out())?;
        // Signed out, the app has ended: no token callback after this. A
        // close racing this does the waiting if it was first: two callbacks
        // ending the app must not wait for each other.
        if self.open.closed.close() {
            self.close_gates();
            self.end_watches();
        }
        Ok(())
    }

    /// Leaves the server; local reads and writes go on (TS's
    /// `disconnect`).
    pub fn disconnect(&self) -> Result<()> {
        self.wait(self.open_jazz()?.disconnect())?;
        Ok(())
    }

    /// Resumes sync after [`Self::disconnect`] (TS's `reconnect`).
    pub fn reconnect(&self) -> Result<()> {
        self.wait(self.open_jazz()?.reconnect())?;
        Ok(())
    }

    /// Closes the app and deletes its local store (TS's
    /// `deleteClientStorage`). It holds the store's lock until the files
    /// are gone.
    pub fn delete_client_storage(&self) -> Result<()> {
        let jazz = self.open_jazz()?;
        if self.derived {
            return Err(refused(
                "delete_client_storage on a for_user or session handle: delete through the app that owns the store (not supported yet for a session's account)",
            ));
        }
        // What is open rolls back and aborts as the client stops; a delete
        // refused with the app running on leaves it be.
        let deleted = block_on(jazz.delete_client_storage());
        // Refused as held, only an app that had stopped already is stopped:
        // while it runs nothing else can hold its store.
        let stopped = || block_on(jazz.with_client(|_| async { Ok(()) })).is_err();
        if self.open.closed.is_closed() {
            // A close stopped the client meanwhile: nothing to report but
            // that, unless the store is gone.
            return deleted.map_err(|_| FfiError::Closed);
        }
        if (!matches!(deleted, Err(jazz_app_support::Error::StoreHeld(..))) || stopped())
            && self.open.closed.close()
        {
            self.close_gates();
            self.end_subscriptions();
        }
        deleted?;
        Ok(())
    }

    /// Calls `listener` with each state's JSON, the current one first, and
    /// once with `None` when the app closes (after which nothing comes).
    pub fn watch(&self, what: &str, listener: Box<dyn Fn(Option<String>) + Send>) -> Result<Watch> {
        use futures_util::StreamExt as _;
        let jazz = self.open_jazz()?;
        let stream: std::pin::Pin<Box<dyn futures_util::Stream<Item = String> + Send>> = match what
        {
            "mutationErrors" => Box::pin(
                self.wait(jazz.mutation_errors())?
                    .map(|error| serde_json::to_string(&error).unwrap_or_default()),
            ),
            "session" => {
                jazz.refuse_for_user("watching the session")?;
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
        spawn_watch(stream, listener, &self.watches, &self.open.closed)
    }

    /// A C host's callback, behind its gate: closing stops it.
    pub(crate) fn on_close(&self, gate: Arc<dyn HostGate>) {
        self.gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(gate);
    }

    fn gates(&self) -> Vec<Arc<dyn HostGate>> {
        self.gates.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Returns once no host callback runs, but the caller's own; none
    /// starts after.
    fn close_gates(&self) {
        for gate in self.gates() {
            gate.close();
        }
    }

    /// Before the handle is freed: waits for the host's callbacks again, as
    /// the one that closed it from inside may still be running.
    pub(crate) fn before_free(&self) {
        self.close_gates();
    }

    /// [`Self::close_within`] without waiting for writes.
    pub fn close(&self) -> Result<()> {
        self.close_within(None)
    }

    /// Closes the handle, which stays allocated; safe beside any other call
    /// on it, and every later call fails with `Closed`. Closing again (even
    /// while a close runs) does nothing and returns at once: waiting for
    /// the first would deadlock a callback that it waits for.
    ///
    /// On a root handle it first waits up to `timeout` for writes to reach
    /// the server (with memory storage, those still unsent fail it with
    /// `Unsynced`, and the app closes all the same). Then, at once with
    /// `None` or zero, the calls waiting on the server or the network end
    /// `Closed`; local work already started finishes (jazz-app-support's
    /// 30 s drain is its backstop), and stream sessions get up to 2 s to
    /// end: not instant, and no hard limit (synchronous work isn't
    /// interrupted; the store closes, and dropping the runtime waits for a
    /// name lookup in progress). A derived handle's client lives on: its
    /// waiting calls end `Closed` at once, and what it began is cleaned up.
    ///
    /// Returns once no host callback runs (but the caller's own).
    pub fn close_within(&self, timeout: Option<std::time::Duration>) -> Result<()> {
        // Before `closed` is set: an open of the store right after this
        // close must wait for it, not be refused.
        if !self.derived {
            self.jazz.begin_stopping();
        }
        if !self.open.closed.close() {
            return Ok(());
        }
        if self.derived {
            // The parent's client goes on: only this handle's own
            // transactions roll back and its uploads abort. Its ids are read
            // on the client's thread, after any begin that came before.
            let open = self.open.clone();
            let _ = block_on(self.jazz.with_client(move |client| async move {
                let owned: Vec<u64> = open.owned().drain().collect();
                for id in owned {
                    if let Some((transaction, _)) =
                        TRANSACTIONS.with(|t| t.borrow_mut().0.remove(&id))
                    {
                        let _ = transaction.rollback();
                    }
                    let upload = UPLOADS.with(|u| u.borrow_mut().0.remove(&id));
                    if let Some(entry) = upload {
                        let _ = client.abort_streaming_upload(entry.upload).await;
                    }
                }
                Ok(())
            }));
            self.end_subscriptions();
            self.close_gates();
            return Ok(());
        }
        let drain = timeout.filter(|timeout| !timeout.is_zero());
        // No new callback from here, except for a drain, which may need a
        // fresh token. The running ones are waited for last: one waiting for
        // a call on this handle ends once the client stops.
        if drain.is_none() {
            for gate in self.gates() {
                gate.shut();
            }
        }
        clean_up(&self.jazz);
        let result = match drain {
            Some(timeout) => block_on(self.jazz.close(timeout)),
            None => block_on(self.jazz.shutdown()),
        };
        self.end_subscriptions();
        self.close_gates();
        result?;
        Ok(())
    }

    /// Each open watch's last call: `None`, the app closed.
    fn end_watches(&self) {
        for close in std::mem::take(&mut *self.watches.lock().unwrap_or_else(|e| e.into_inner())) {
            let _ = close.send(());
        }
    }

    /// Each open subscription's last event: the app closed.
    fn end_subscriptions(&self) {
        self.end_watches();
        let subscriptions =
            std::mem::take(&mut *self.subscriptions.lock().unwrap_or_else(|e| e.into_inner()));
        for delivery in subscriptions.iter().filter_map(Weak::upgrade) {
            delivery.send(Some(Event::Error(JazzError::Closed)));
        }
    }
}

/// Rolls back and aborts what is open on `jazz`'s client while it still
/// runs (its tables only forget at the thread's exit).
pub(crate) fn clean_up(jazz: &Jazz) {
    let _ = block_on(jazz.with_client(|client| async move {
        clean_up_open(client).await;
        Ok(())
    }));
}

/// [`clean_up`], on the client's thread.
async fn clean_up_open(client: JazzClient) {
    let open = TRANSACTIONS.with(|t| std::mem::take(&mut t.borrow_mut().0));
    for (transaction, _) in open.into_values() {
        let _ = transaction.rollback();
    }
    let uploads = UPLOADS.with(|u| std::mem::take(&mut u.borrow_mut().0));
    for entry in uploads.into_values() {
        let _ = client.abort_streaming_upload(entry.upload).await;
    }
}

/// Called as a transaction or upload begins: whatever stops the client (a
/// sign-out, a change of user, deleteClientStorage, the session itself)
/// cleans it up first.
fn clean_up_before_stop() {
    jazz_app_support::before_client_stops(|client| Box::pin(clean_up_open(client)));
}

/// `for_user`'s errors: the main backend refusing the identity (its 403,
/// `identity_not_authorized`) is a permission denial, not a malformed call.
fn for_user_error(user_id: &str, error: jazz_app_support::Error) -> FfiError {
    match error {
        jazz_app_support::Error::Refused(reason) => FfiError::Permission(format!(
            "the main backend refused to act for {user_id}: {reason}"
        )),
        error => error.into(),
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
    id: jazz_app_support::ObjectId,
) -> jazz_app_support::Result<Json> {
    use jazz_app_support::jazz::query as q;
    let mut query = q::Query::from(name);
    query.filters.push(q::eq(
        q::col("id"),
        q::lit(jazz_app_support::jazz::records::Value::Uuid(*id.uuid())),
    ));
    let row = client
        .query(query, ReadTier::LocalFirst)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| jazz_app_support::Error::Config(format!("no {name} row {id}")))?;
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
    id: jazz_app_support::ObjectId,
    edits: Vec<jazz_app_support::jazz::db::LargeValueUpdate>,
) -> jazz_app_support::Result<Vec<jazz_app_support::jazz::db::LargeValueUpdate>> {
    use jazz_app_support::jazz::db::{LargeValueUpdate, LargeValueUpdatePage as Page};
    use jazz_app_support::jazz::query as q;
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
            q::lit(jazz_app_support::jazz::records::Value::Uuid(*id.uuid())),
        ));
        query.select = Some(vec![column.clone()]);
        let rows = client.query(query, ReadTier::LocalOnly).await?;
        let value = rows
            .into_iter()
            .next()
            .and_then(|row| row.fields.into_iter().find(|f| f.name == column))
            .map(|field| field.value)
            .ok_or_else(|| jazz_app_support::Error::Config(format!("no {table} row {id}")))?;
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

fn take_tx(tx: u64) -> jazz_app_support::Result<(JazzTransaction, bool)> {
    TRANSACTIONS
        .with(|t| t.borrow_mut().0.remove(&tx))
        .ok_or_else(|| jazz_app_support::Error::Config(format!("transaction {tx} is not open")))
}

/// Waits for a committed write to reach `tier`, when asked to.
async fn settle(
    client: &JazzClient,
    committed: Option<jazz_app_support::jazz::tools::TransactionId>,
    tier: Option<DurabilityTier>,
) -> jazz_app_support::Result<()> {
    if let Some(committed) = committed {
        match tier {
            // Confirmed by the server: nothing for waitForSync to check,
            // unless closing cut the confirmation short.
            Some(DurabilityTier::GlobalServer) => {
                let confirmed = client
                    .wait_for_transaction(committed, DurabilityTier::GlobalServer)
                    .await;
                if confirmed.is_err() && client.waits_are_over() {
                    unchecked([committed].into());
                }
                confirmed?;
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

/// Puts writes nobody heard back for waitForSync to check, oldest first.
fn unchecked(mut writes: std::collections::VecDeque<jazz_app_support::jazz::tools::TransactionId>) {
    UNSYNCED.with(|unsynced| {
        let mut unsynced = unsynced.borrow_mut();
        writes.append(&mut unsynced);
        while writes.len() > MAX_UNSYNCED {
            writes.pop_front();
        }
        *unsynced = writes;
    });
}

/// How many unchecked writes waitForSync remembers.
const MAX_UNSYNCED: usize = 4096;

thread_local! {
    /// The writes this app's client committed since its last waitForSync.
    static UNSYNCED: RefCell<std::collections::VecDeque<jazz_app_support::jazz::tools::TransactionId>> =
        const { RefCell::new(std::collections::VecDeque::new()) };
}

/// A watch of session or sync states.
/// Delivers `stream`'s states to `listener` on a thread of its own until
/// cancelled, then once with `None` when the app (or session) closes:
/// `closers` gets the sender its close fires, `closed` is checked for a
/// close that raced the registration.
pub(crate) fn spawn_watch(
    mut stream: std::pin::Pin<Box<dyn futures_util::Stream<Item = String> + Send>>,
    listener: Box<dyn Fn(Option<String>) + Send>,
    closers: &Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    closed_flag: &Closed,
) -> Result<Watch> {
    use futures_util::StreamExt as _;
    let (cancel, mut cancelled) = tokio::sync::oneshot::channel::<()>();
    let (close, mut closed) = tokio::sync::oneshot::channel::<()>();
    closers
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(close);
    let thread = std::thread::Builder::new()
        .name("jazz-watch".into())
        .spawn(move || {
            block_on(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut cancelled => return,
                        result = &mut closed => {
                            // Sent by the close (an error: the watch was
                            // dropped first, nothing to say).
                            if result.is_ok() {
                                // The states published before the close
                                // (a sign-out's `signedOut`) come first: the
                                // close is only the last word.
                                use futures_util::FutureExt as _;
                                while let Some(Some(state)) = stream.next().now_or_never() {
                                    listener(Some(state));
                                }
                                listener(None);
                            }
                            return;
                        }
                        state = stream.next() => match state {
                            Some(state) => listener(Some(state)),
                            None => {
                                // The stream ended: the app closed.
                                listener(None);
                                return;
                            }
                        },
                    }
                }
            })
        })
        .map_err(|error| FfiError::Io(format!("cannot start the watch's thread: {error}")))?;
    // A close racing this watch may have taken the list before the push.
    if closed_flag.is_closed() {
        for close in std::mem::take(&mut *closers.lock().unwrap_or_else(|e| e.into_inner())) {
            let _ = close.send(());
        }
    }
    Ok(Watch {
        thread_id: thread.thread().id(),
        thread: Mutex::new(Some(thread)),
        cancel: Mutex::new(Some(cancel)),
    })
}

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

/// A host's token source as jazz-app-support's.
pub(crate) struct Tokens(pub(crate) Arc<dyn jazz_app_support::TokenProvider>);

impl jazz_app_support::TokenProvider for Tokens {
    fn token(&self, reason: jazz_app_support::TokenReason) -> jazz_app_support::TokenFuture {
        self.0.token(reason)
    }
}

/// A host callback that gives tokens, as a jazz-app-support [`TokenProvider`]: it is
/// called on a thread of its own (it may block, and must not be called on
/// the client's thread), with the reason as `connect`, `renew`, `expired`
/// or `refresh`.
pub(crate) struct HostTokens(pub Arc<HostTokenFn>);

/// A host's token callback: the reason in, the token (or none: signed out)
/// out, or an error (the fetch failed: the session stays, and asks again).
pub(crate) type HostTokenFn =
    dyn Fn(&str) -> std::result::Result<Option<String>, String> + Send + Sync;

impl jazz_app_support::TokenProvider for HostTokens {
    fn token(&self, reason: jazz_app_support::TokenReason) -> jazz_app_support::TokenFuture {
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
                jazz_app_support::Error::Config(format!("cannot ask for a token: {error}"))
            })?;
            receiver
                .await
                .map_err(|_| jazz_app_support::Error::Stopped)?
                .map_err(|reason| {
                    jazz_app_support::Error::Server(format!("the token provider failed: {reason}"))
                })
        })
    }
}

/// A new device account's key, as `jazz-auth-v1:…` text (TS's
/// `exportDeviceKey`): keep it as a password, in the platform's keychain.
pub(crate) fn generate_device_key() -> Result<String> {
    Ok(jazz_app_support::DeviceKey::generate()?.to_string())
}

fn config(
    config_json: &str,
    tokens: Option<Arc<dyn jazz_app_support::TokenProvider>>,
    store: Option<jazz_app_support::EncryptionConfig>,
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
    let mut environment = None;
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
            "environment" => environment = Some(text()?),
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
    // A backend client: a machine syncing through the app's main backend
    // with its own credential. Its `permissions` are the app's bundle (as
    // TS's createJazzClient takes them), not published.
    let credential = matches!(
        auth.as_ref().and_then(|a| a.get("credential")),
        Some(Json::Bool(true))
    );
    let backend = permissions.is_some() && !credential;
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
    } else if credential {
        if result.server_url.is_none() {
            return Err(refused(
                "a backend client (\"auth\": { \"credential\": true }) needs \"serverUrl\": the main backend's /jazz",
            ));
        }
        if auth
            .as_ref()
            .and_then(Json::as_object)
            .is_some_and(|a| a.len() > 1)
        {
            return Err(refused(
                "config.auth is { \"credential\": true } alone for a backend client",
            ));
        }
        if backend_secret.is_some() || admin_secret.is_some() || session.is_some() {
            return Err(refused(
                "a backend client's credential decides who it is: leave out backendSecret, adminSecret and session",
            ));
        }
        let tokens = tokens.ok_or_else(|| {
            refused("\"auth\": { \"credential\": true } needs a token provider: open with one")
        })?;
        result = result.credential(Tokens(tokens));
        if insecure {
            result = result.allow_insecure_http();
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
                jazz_app_support::Auth::Device(key.parse().map_err(|_| {
                    refused("config.auth.deviceKey is not a device key (jazz-auth-v1:…)")
                })?)
            }
            (None, Some(Json::Bool(true))) => {
                let tokens = tokens.ok_or_else(|| {
                    refused("\"auth\": { \"bearer\": true } needs a token provider: open with one")
                })?;
                jazz_app_support::Auth::bearer(Tokens(tokens))
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
    if let Some(mut encryption) = store {
        if let Some(environment) = environment {
            encryption = encryption.environment(environment);
        }
        result = result.encryption(encryption);
    }
    let files = AppFiles::new(
        app.clone().into_bytes(),
        permissions.map(String::into_bytes),
    );
    Ok((files, result, app))
}

#[cfg(test)]
#[path = "core_close_tests.rs"]
mod close_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// A state published just before the app closes (a sign-out's
    /// `signedOut`, then `end_watches`) reaches the listener before the last
    /// `closed`, even when the watch's thread sees the close first.
    #[test]
    fn a_watch_delivers_the_states_published_before_its_close() {
        let closers: Arc<Mutex<Vec<tokio::sync::oneshot::Sender<()>>>> = Arc::default();
        let closed_flag = Closed::new();
        // First poll: the app closes (as end_watches does), and the state
        // is ready only on the next poll: the close is seen first.
        let mut polled = false;
        let close_all = closers.clone();
        let mut item = Some("signedOut".to_owned());
        let stream = futures_util::stream::poll_fn(move |cx| {
            if !polled {
                polled = true;
                for close in std::mem::take(&mut *close_all.lock().unwrap()) {
                    let _ = close.send(());
                }
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            match item.take() {
                Some(state) => std::task::Poll::Ready(Some(state)),
                None => std::task::Poll::Pending,
            }
        });
        let (sender, states) = mpsc::channel();
        let listener = Box::new(move |state: Option<String>| {
            let _ = sender.send(state);
        });
        let _watch = spawn_watch(Box::pin(stream), listener, &closers, &closed_flag).unwrap();
        let seen: Vec<_> =
            std::iter::from_fn(|| states.recv_timeout(std::time::Duration::from_secs(5)).ok())
                .take(2)
                .collect();
        assert_eq!(seen, [Some("signedOut".to_owned()), None]);
    }

    fn open_transactions(core: &Core) -> usize {
        block_on(
            core.jazz
                .with_client(|_| async { Ok(TRANSACTIONS.with(|t| t.borrow().0.len())) }),
        )
        .unwrap()
    }

    /// A handle sharing another's client, as `for_user` makes one, rolls
    /// back its own open transactions when it closes, and only those (not
    /// observable from outside: an open transaction writes nothing).
    #[test]
    fn closing_a_derived_handle_rolls_back_its_own_transactions() {
        let app = r#"{"format": 1, "tables": {"todos": {"columns": {"title": "string"}}}}"#;
        let parent = Core::open(&serde_json::json!({ "app": app }).to_string()).unwrap();
        let derived = Core::with(
            parent.jazz.clone(),
            parent.schema.clone(),
            parent.relations.clone(),
            false,
            parent.contract,
            Some((parent.open.closed.clone(), parent.next_tx.clone())),
        );
        let parents = parent.tx_begin(true).unwrap();
        derived.tx_begin(true).unwrap();
        assert_eq!(open_transactions(&parent), 2);
        derived.close().unwrap();
        assert_eq!(open_transactions(&parent), 1);
        parent.tx_rollback(parse_tx(&parents).unwrap()).unwrap();
        parent.close().unwrap();
    }

    fn todos() -> Core {
        let app = r#"{"format": 1, "tables": {"todos": {"columns": {"title": "string"}}}}"#;
        Core::open(&serde_json::json!({ "app": app }).to_string()).unwrap()
    }

    fn open_uploads(core: &Core) -> usize {
        block_on(
            core.jazz
                .with_client(|_| async { Ok(UPLOADS.with(|u| u.borrow().0.len())) }),
        )
        .unwrap()
    }

    /// Alice's backend must refuse a plain-column insert upload into an
    /// encrypted table when its required siblings are missing, before she
    /// can send bytes.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_encrypted_table_refuses_an_incomplete_upload_at_begin() {
        let server = jazz_server::JazzServer::start().await.unwrap();
        let config = serde_json::json!({
            "app": include_str!("../../jazz-app/tests/fixtures/encrypted/jazz.app.json"),
            "permissions": include_str!("../../jazz-app/tests/fixtures/encrypted/jazz.permissions.json"),
            "appId": server.app_id().to_string(),
            "serverUrl": server.base_url(),
            "backendSecret": server.backend_secret(),
            "adminSecret": server.admin_secret(),
            "session": { "subject": "alice" },
        });
        let alice = Core::open(&config.to_string()).expect("open the encrypted backend");
        let error = alice
            .upload_begin("tasks", "label", "insert", None, None)
            .expect_err("refuse before the upload can accept bytes");
        assert!(
            matches!(error, FfiError::Refused(ref reason) if reason.contains("missing required field")),
            "{error:?}"
        );
        assert_eq!(open_uploads(&alice), 0);
        alice.close().unwrap();
    }

    /// Holds the client's thread until the returned sender is used.
    fn hold_client(core: &Core) -> (mpsc::Sender<()>, JoinHandle<()>) {
        let (release, released) = mpsc::channel::<()>();
        let (holding, held) = mpsc::channel::<()>();
        let jazz = core.jazz.clone();
        let thread = std::thread::spawn(move || {
            let _ = block_on(jazz.with_client(move |_| async move {
                let _ = holding.send(());
                let _ = released.recv_timeout(std::time::Duration::from_secs(10));
                Ok(())
            }));
        });
        held.recv().unwrap();
        (release, thread)
    }

    /// A transaction and an upload whose begin is on its way to the client
    /// when their handle closes are cleaned up, not left on the parent.
    #[test]
    fn begins_racing_a_derived_close_are_cleaned_up() {
        let parent = todos();
        let derived = Arc::new(Core::with(
            parent.jazz.clone(),
            parent.schema.clone(),
            parent.relations.clone(),
            false,
            parent.contract,
            Some((parent.open.closed.clone(), parent.next_tx.clone())),
        ));
        let (release, held) = hold_client(&parent);
        let pause = || std::thread::sleep(std::time::Duration::from_millis(300));
        let tx = std::thread::spawn({
            let derived = derived.clone();
            move || derived.tx_begin(true)
        });
        let upload = std::thread::spawn({
            let derived = derived.clone();
            move || derived.upload_begin("todos", "title", "insert", None, None)
        });
        pause();
        let closing = std::thread::spawn({
            let derived = derived.clone();
            move || derived.close()
        });
        pause();
        release.send(()).unwrap();
        let _ = (tx.join().unwrap(), upload.join().unwrap());
        closing.join().unwrap().unwrap();
        held.join().unwrap();
        assert_eq!((open_transactions(&parent), open_uploads(&parent)), (0, 0));
        parent.close().unwrap();
    }

    /// A begin that passed the open check, whose job reaches the client
    /// only after the close's clean-up, begins nothing.
    #[test]
    fn a_begin_sent_after_the_close_begins_nothing() {
        let parent = todos();
        for upload in [false, true] {
            let derived = Arc::new(Core::with(
                parent.jazz.clone(),
                parent.schema.clone(),
                parent.relations.clone(),
                false,
                parent.contract,
                Some((parent.open.closed.clone(), parent.next_tx.clone())),
            ));
            let closing = derived.clone();
            BEFORE_SENDING.set(Some(Box::new(move || closing.close().unwrap())));
            let begun = if upload {
                derived.upload_begin("todos", "title", "insert", None, None)
            } else {
                derived.tx_begin(true)
            };
            assert!(matches!(begun, Err(FfiError::Closed)), "{begun:?}");
        }
        assert_eq!((open_transactions(&parent), open_uploads(&parent)), (0, 0));
        parent.close().unwrap();
    }

    /// Closing a root leaves nothing for its thread's exit to forget.
    #[test]
    fn closing_a_root_cleans_up_transactions_and_uploads() {
        let root = todos();
        root.tx_begin(true).unwrap();
        root.upload_begin("todos", "title", "insert", None, None)
            .unwrap();
        let forgotten: &'static Forgotten = Box::leak(Box::default());
        block_on(root.jazz.with_client(move |_| async move {
            FORGOTTEN.set(Some(forgotten));
            Ok(())
        }))
        .unwrap();
        root.close().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while forgotten.tables.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "the client's thread runs on"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(forgotten.entries.load(Ordering::SeqCst), 0);
    }

    const ONE_TABLE: &str =
        r#"{"format": 1, "tables": {"todos": {"columns": {"title": "string"}}}}"#;

    /// No server answers there: a device account works offline.
    const NOWHERE: &str = "http://127.0.0.1:9/jazz";

    /// Begins a transaction and an upload on `core`, ends its client with
    /// `end`, and counts what the client's thread was left to forget.
    fn left_open(core: &Core, end: impl FnOnce()) -> u64 {
        core.tx_begin(true).unwrap();
        core.upload_begin("todos", "title", "insert", None, None)
            .unwrap();
        let forgotten: &'static Forgotten = Box::leak(Box::default());
        block_on(core.jazz.with_client(move |_| async move {
            FORGOTTEN.set(Some(forgotten));
            Ok(())
        }))
        .unwrap();
        end();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while forgotten.tables.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "the client's thread runs on"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        forgotten.entries.load(Ordering::SeqCst)
    }

    #[test]
    fn deleting_client_storage_cleans_up_transactions_and_uploads() {
        let root = todos();
        let left = left_open(&root, || root.delete_client_storage().unwrap());
        assert_eq!(left, 0);
    }

    #[test]
    fn signing_out_cleans_up_transactions_and_uploads() {
        let key = jazz_app_support::DeviceKey::generate().unwrap().to_string();
        let root = Core::open(
            &serde_json::json!({ "app": ONE_TABLE, "appId": AppId::random().to_string(),
                "serverUrl": NOWHERE, "auth": { "deviceKey": key }, "contract": 2 })
            .to_string(),
        )
        .unwrap();
        let left = left_open(&root, || root.sign_out().unwrap());
        assert_eq!(left, 0);
    }

    #[test]
    fn a_session_ending_its_client_cleans_up_its_handles() {
        #[derive(Debug)]
        enum Ends {
            SignOut,
            ChangeOfUser,
        }
        for ends in [Ends::SignOut, Ends::ChangeOfUser] {
            let session = block_on(jazz_app_support::JazzSession::open(
                AppFiles::new(ONE_TABLE.as_bytes().to_vec(), None),
                jazz_app_support::SessionConfig::new(AppId::random())
                    .server(NOWHERE)
                    .initial(jazz_app_support::Initial::Device),
            ))
            .unwrap();
            let jazz = session.jazz().unwrap();
            let schema =
                block_on(jazz.with_client(|client| async move { Ok(client.schema()?) })).unwrap();
            let handle = Core::for_session(
                jazz,
                Arc::new(schema),
                convert::Relations::read(ONE_TABLE).unwrap(),
                2,
                Closed::new(),
                Arc::new(AtomicU64::new(1)),
                Box::new(|| false),
            );
            let left = left_open(&handle, || {
                match ends {
                    Ends::SignOut => block_on(session.sign_out()),
                    Ends::ChangeOfUser => block_on(session.create_device_account()),
                }
                .unwrap();
            });
            assert_eq!(left, 0, "{ends:?}");
            block_on(session.close()).unwrap();
        }
    }

    /// The relay stand-in can't make the core answer 403, so the mapping is
    /// checked here: a refused identity is `Permission`; `NoAccount` and a
    /// scoped client's `Config` keep their kinds.
    #[test]
    fn for_user_errors_name_the_403_a_permission_denial() {
        let refused = for_user_error(
            "alice",
            jazz_app_support::Error::Refused("identity_not_authorized".into()),
        );
        assert!(
            matches!(&refused, FfiError::Permission(m) if m.contains("alice") && m.contains("identity_not_authorized")),
            "{refused:?}"
        );
        // A bundle mismatch is its own kind, a refusal: never retryable I/O.
        let mismatch = JazzError::from(for_user_error(
            "alice",
            jazz_app_support::Error::BundleMismatch {
                own: "aa".into(),
                served: "bb".into(),
            },
        ));
        assert_eq!(mismatch.kind(), "bundleMismatch");
        assert!(
            matches!(&mismatch, JazzError::BundleMismatch { own, served, .. } if own == "aa" && served == "bb")
        );
        assert_eq!(mismatch.clone().for_contract(1).kind(), "refused");
        assert!(matches!(
            for_user_error("alice", jazz_app_support::Error::NoAccount("alice".into())),
            FfiError::NoAccount { user_id, .. } if user_id == "alice"
        ));
        assert!(matches!(
            for_user_error("alice", jazz_app_support::Error::Config("scoped".into())),
            FfiError::Refused(_)
        ));
    }
}

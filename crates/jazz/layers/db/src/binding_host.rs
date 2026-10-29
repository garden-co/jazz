//! The host-agnostic half of a JavaScript binding (WASM or Node).
//!
//! A binding receives bytes, strings and an options object from JavaScript,
//! calls the database, and hands back bytes, handles and futures. Everything
//! between those edges lives here, so the browser and Node bindings decide
//! nothing on their own: they convert their host's values (through
//! [`HostObject`]), park futures on [`Progress`], and carry sync frames.
//!
//! The database runs every operation to completion synchronously. Only reads
//! and write fates that need the server wait, and they complete on a later
//! tick or write; [`Progress`] is how those waits learn to poll again.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use jazz_model::model::transaction::{OpenTransactionId, TransactionId};
use jazz_model::query::Query;
use jazz_model::schema::JazzSchema;
use jazz_model::tx::{DurabilityTier, TxId};
use jazz_protocol::protocol::{
    BranchSelector, BranchViewBase, PermissionAdvice, PermissionAdviceAction, ReadViewSpec,
};
use jazz_types::ids::{AuthorSubject, AuthorSubjectError, NodeUuid, RowUuid};
use jazz_types::records::Value;
use jazz_types::time::TxTime;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::binding_args;
use crate::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, Error, ErrorCode, ExactWriteTarget, FirstLoad,
    InsertOptions, LargeValueStagingPolicy, LargeValueUpdate, ReadOpts, ReadTier, RemoteLinkHint,
    RestoreOptions, SeededRowIdSource, SerializedReadResult, StreamingMutationKind,
    StreamingValueUpload, SubscriberId, SubscriptionEvent, SubscriptionStream, TransportError,
    UpdateOptions, UpsertOptions, Upstream, WireTransport, WireTransportAdapter, WriteHandle,
    WriteIdentity, block_on,
};

/// Named cells, as the binding codec decodes them.
pub type Cells = BTreeMap<String, Value>;

// ---- errors ---------------------------------------------------------------------

/// Why a binding call failed.
#[derive(Debug)]
pub enum HostError {
    /// The database refused: a host shows the stable [`ErrorCode`] with it.
    Engine(Error),
    /// A malformed argument, caught before the database saw the call.
    Argument(String),
}

impl From<Error> for HostError {
    fn from(error: Error) -> Self {
        Self::Engine(error)
    }
}

impl From<String> for HostError {
    fn from(message: String) -> Self {
        Self::Argument(message)
    }
}

impl From<postcard::Error> for HostError {
    fn from(error: postcard::Error) -> Self {
        Self::Argument(format!("binding codec: {error}"))
    }
}

impl From<&str> for HostError {
    fn from(message: &str) -> Self {
        Self::Argument(message.to_owned())
    }
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine(error) => error.fmt(f),
            Self::Argument(message) => f.write_str(message),
        }
    }
}

/// The result of a binding call.
pub type HostResult<T> = Result<T, HostError>;

/// A binding call for an engine feature the SQLite engine does not have yet.
pub fn not_supported(what: &str) -> HostError {
    HostError::Engine(Error::new(
        ErrorCode::Query,
        format!("{what} is not supported yet"),
    ))
}

// ---- host values ------------------------------------------------------------------

/// A JavaScript options object as the host sees it. `undefined` and `null`
/// properties read as absent, except through [`HostObject::has`], which
/// rejects removed fields by presence.
pub trait HostObject {
    /// One property value.
    type Value;

    /// The property, or `None` when absent, `undefined` or `null` (or when
    /// the object itself is).
    fn get(&self, name: &str) -> HostResult<Option<Self::Value>>;
    /// Whether the object has the property at all, even as `undefined`.
    fn has(&self, name: &str) -> HostResult<bool>;
    /// A byte array (`Uint8Array`) property value.
    fn bytes(value: &Self::Value) -> HostResult<Vec<u8>>;
    /// A number property value.
    fn number(value: &Self::Value) -> Option<f64>;
    /// A string property value.
    fn string(value: &Self::Value) -> Option<String>;
    /// A boolean property value.
    fn boolean(value: &Self::Value) -> Option<bool>;
    /// A structured property value, through serde.
    fn decode<T: DeserializeOwned>(value: Self::Value) -> HostResult<T>;
}

/// JSON options: byte arrays are arrays of numbers. Node passes JSON, and
/// tests use it.
impl HostObject for serde_json::Value {
    type Value = serde_json::Value;

    fn get(&self, name: &str) -> HostResult<Option<serde_json::Value>> {
        Ok(self.get(name).filter(|value| !value.is_null()).cloned())
    }

    fn has(&self, name: &str) -> HostResult<bool> {
        Ok(self
            .as_object()
            .is_some_and(|object| object.contains_key(name)))
    }

    fn bytes(value: &serde_json::Value) -> HostResult<Vec<u8>> {
        serde_json::from_value(value.clone())
            .map_err(|error| HostError::Argument(format!("expected bytes: {error}")))
    }

    fn number(value: &serde_json::Value) -> Option<f64> {
        value.as_f64()
    }

    fn string(value: &serde_json::Value) -> Option<String> {
        value.as_str().map(str::to_owned)
    }

    fn boolean(value: &serde_json::Value) -> Option<bool> {
        value.as_bool()
    }

    fn decode<T: DeserializeOwned>(value: serde_json::Value) -> HostResult<T> {
        serde_json::from_value(value).map_err(|error| HostError::Argument(error.to_string()))
    }
}

fn row_uuid(bytes: &[u8]) -> HostResult<RowUuid> {
    Ok(binding_args::row_uuid(bytes)?)
}

fn author(bytes: &[u8]) -> HostResult<AuthorSubject> {
    Ok(binding_args::author_subject(bytes)?)
}

fn checked_u64(value: f64, name: &str) -> HostResult<u64> {
    Ok(binding_args::checked_u64(value, name)?)
}

/// Named cells from the binding codec.
pub fn decode_cells(bytes: &[u8]) -> HostResult<Cells> {
    crate::binding_codec::decode_named_cells(bytes)
        .map_err(|error| HostError::Argument(format!("decode cells: {error}")))
}

/// An open transaction's id, as the host spells it.
pub fn open_transaction(id: &str) -> HostResult<OpenTransactionId> {
    Ok(id.parse::<OpenTransactionId>()?)
}

/// Session claims for `author`, from a JSON object.
pub fn claims(author: AuthorSubject, claims: serde_json::Value) -> HostResult<Cells> {
    Ok(binding_args::policy_claims(&author, claims)?)
}

// ---- options -------------------------------------------------------------------------

/// Resolves author bytes to the subject a write or read runs as.
type AuthorOf<'a> = &'a dyn Fn(&[u8]) -> HostResult<AuthorSubject>;

fn write_identity<O: HostObject>(options: &O, author: AuthorOf<'_>) -> HostResult<WriteIdentity> {
    match (options.get("author")?, options.get("attribution")?) {
        (Some(_), Some(_)) => {
            Err("write identity cannot contain both author and attribution".into())
        }
        (Some(value), None) => Ok(WriteIdentity::Session(author(&O::bytes(&value)?)?)),
        (None, Some(value)) => Ok(WriteIdentity::Attribution(author(&O::bytes(&value)?)?)),
        (None, None) => Ok(WriteIdentity::Database),
    }
}

fn write_timestamp<O: HostObject>(options: &O) -> HostResult<Option<u64>> {
    options
        .get("updatedAtMs")?
        .map(|value| {
            let value = O::number(&value).ok_or("updatedAtMs must be a number")?;
            checked_u64(value, "updatedAtMs")
        })
        .transpose()
}

fn exact_target<O: HostObject>(options: &O) -> HostResult<ExactWriteTarget> {
    Ok(options
        .get("branch")?
        .map(|branch| O::decode(branch).map(ExactWriteTarget::Branch))
        .transpose()?
        .unwrap_or_default())
}

fn branch_view_target<O: HostObject>(options: &O) -> HostResult<crate::db::WriteTarget> {
    match (options.get("head")?, options.get("base")?) {
        (Some(head), base) => Ok(crate::db::WriteTarget::BranchView {
            head: O::decode(head)?,
            base: base.map(O::decode).transpose()?,
        }),
        (None, None) => Ok(Default::default()),
        (None, Some(_)) => Err("branch view base requires a head selector".into()),
    }
}

/// `{ rowId?, author?, attribution?, branch?, updatedAtMs? }`.
pub fn insert_options<O: HostObject>(options: &O) -> HostResult<InsertOptions> {
    insert_options_as(options, &author)
}

fn insert_options_as<O: HostObject>(
    options: &O,
    author: AuthorOf<'_>,
) -> HostResult<InsertOptions> {
    Ok(InsertOptions {
        row_id: options
            .get("rowId")?
            .map(|row| row_uuid(&O::bytes(&row)?))
            .transpose()?,
        identity: write_identity(options, author)?,
        target: exact_target(options)?,
        updated_at_ms: write_timestamp(options)?,
    })
}

/// `{ author?, attribution?, head?, base?, updatedAtMs? }`.
pub fn update_options<O: HostObject>(options: &O) -> HostResult<UpdateOptions> {
    update_options_as(options, &author)
}

fn update_options_as<O: HostObject>(
    options: &O,
    author: AuthorOf<'_>,
) -> HostResult<UpdateOptions> {
    Ok(UpdateOptions {
        identity: write_identity(options, author)?,
        target: branch_view_target(options)?,
        updated_at_ms: write_timestamp(options)?,
    })
}

/// As [`update_options`]; the removed `branch` field is rejected even when
/// `undefined`, so untyped callers cannot write to the root by accident.
pub fn upsert_options<O: HostObject>(options: &O) -> HostResult<UpsertOptions> {
    upsert_options_as(options, &author)
}

fn upsert_options_as<O: HostObject>(
    options: &O,
    author: AuthorOf<'_>,
) -> HostResult<UpsertOptions> {
    if options.has("branch")? {
        return Err(
            "upsert option `branch` is not supported; use `head` (and optional `base`) for a branch view"
                .into(),
        );
    }
    Ok(UpsertOptions {
        identity: write_identity(options, author)?,
        target: branch_view_target(options)?,
        updated_at_ms: write_timestamp(options)?,
    })
}

/// As [`update_options`].
pub fn delete_options<O: HostObject>(options: &O) -> HostResult<DeleteOptions> {
    delete_options_as(options, &author)
}

fn delete_options_as<O: HostObject>(
    options: &O,
    author: AuthorOf<'_>,
) -> HostResult<DeleteOptions> {
    let options = update_options_as(options, author)?;
    Ok(DeleteOptions {
        identity: options.identity,
        target: options.target,
        updated_at_ms: options.updated_at_ms,
    })
}

/// As [`insert_options`] without a row id.
pub fn restore_options<O: HostObject>(options: &O) -> HostResult<RestoreOptions> {
    restore_options_as(options, &author)
}

fn restore_options_as<O: HostObject>(
    options: &O,
    author: AuthorOf<'_>,
) -> HostResult<RestoreOptions> {
    Ok(RestoreOptions {
        identity: write_identity(options, author)?,
        target: exact_target(options)?,
        updated_at_ms: write_timestamp(options)?,
    })
}

/// `{ read_view? | readView?, tier?, include_deleted?, first_load_remote_wait_ms?, splices? }`.
pub fn read_opts<O: HostObject>(options: &O) -> HostResult<ReadOpts> {
    let mut opts = ReadOpts::default();
    for name in ["read_view", "readView"] {
        if let Some(view) = options.get(name)? {
            opts.read_view = O::decode::<ReadViewSpec>(view)
                .map_err(|error| HostError::Argument(format!("invalid read_view: {error}")))?;
            break;
        }
    }
    if let Some(tier) = options.get("tier")? {
        let tier = O::string(&tier).ok_or("tier must be a string")?;
        opts.tier = binding_args::read_tier(&tier)?;
    }
    if let Some(include_deleted) = options.get("include_deleted")? {
        opts.read_view.include_deleted =
            O::boolean(&include_deleted).ok_or("include_deleted must be a boolean")?;
    }
    if let Some(wait) = options.get("first_load_remote_wait_ms")? {
        let timeout_ms = match O::number(&wait) {
            Some(ms) if ms.is_finite() && ms >= 0.0 => ms.floor() as u64,
            _ => {
                return Err(
                    "first_load_remote_wait_ms must be a non-negative number of milliseconds"
                        .into(),
                );
            }
        };
        if opts.tier == ReadTier::LocalFirst {
            opts.first_load = FirstLoad::WaitForRemote { timeout_ms };
        }
    }
    if let Some(splices) = options.get("splices")? {
        opts.splices = O::decode::<Vec<String>>(splices)
            .map_err(|_| HostError::Argument("splices must be an array of column names".into()))?;
    }
    Ok(opts)
}

/// A transaction kind: `"mergeable"`, or `"serializable"` (also spelled
/// `"exclusive"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionKind {
    /// Concurrent commits merge.
    Mergeable,
    /// Commits only if nothing it read or wrote changed.
    Serializable,
}

impl TransactionKind {
    /// Parse the host's spelling.
    pub fn parse(kind: &str) -> HostResult<Self> {
        match kind {
            "mergeable" => Ok(Self::Mergeable),
            "exclusive" | "serializable" => Ok(Self::Serializable),
            _ => Err(HostError::Argument(format!(
                "unknown transaction kind {kind}"
            ))),
        }
    }
}

/// The host's name for a permission advice.
pub fn advice_str(advice: PermissionAdvice) -> &'static str {
    match advice {
        PermissionAdvice::Allowed => "allowed",
        PermissionAdvice::Denied => "denied",
        PermissionAdvice::Unknown => "unknown",
    }
}

// ---- opening ---------------------------------------------------------------------------

/// The open configuration a host passes as postcard bytes.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpenConfig {
    /// The database's node and author.
    pub identity: OpenIdentity,
    /// Seed for deterministic row ids (tests).
    pub row_id_seed: Option<u64>,
    /// Whether the replica keeps full history. Every store keeps full
    /// history, so either value opens the same store (not an authority).
    pub history_complete: bool,
    /// The public `initialSyncFlushEvery` option. Each applied batch commits
    /// on its own, so there is nothing to tune and any value is accepted.
    pub initial_sync_flush_every: Option<u32>,
    /// The app's schema versions and migrations as JSON (a deployment's
    /// `schemas` and `migrations`), which the binding lowers into
    /// [`DbConfig::with_migrations`] (SPEC 10.7).
    pub migrations: Option<Vec<u8>>,
}

/// The identity in an [`OpenConfig`].
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct OpenIdentity {
    /// This replica's node.
    pub node: NodeUuid,
    /// The author it writes as.
    pub author: AuthorSubject,
}

impl From<OpenIdentity> for DbIdentity {
    fn from(identity: OpenIdentity) -> Self {
        Self {
            node: identity.node,
            author: identity.author,
        }
    }
}

/// Decode a public schema (JSON bytes) and an [`OpenConfig`] (postcard).
pub fn decode_open(schema: &[u8], config: &[u8]) -> HostResult<(JazzSchema, OpenConfig)> {
    let schema = decode_schema(schema)?;
    let config: OpenConfig = postcard::from_bytes(config)
        .map_err(|error| HostError::Argument(format!("decode open config: {error}")))?;
    Ok((schema, config))
}

/// Decode a public schema (JSON bytes).
pub fn decode_schema(schema: &[u8]) -> HostResult<JazzSchema> {
    Ok(jazz_model::model::public_schema_convert::decode_public_schema_json(schema)?)
}

/// How long a Remote one-shot read waits for the server to cover it
/// before failing with `NotObserved`.
pub const REMOTE_READ_COVERAGE_BUDGET: std::time::Duration = std::time::Duration::from_secs(15);

/// How a host opens: the caller-controlled author, the backend's SYSTEM
/// author, or a verified self-signed author.
pub enum OpenAs<'a> {
    /// The configured author, which must not be a Jazz-reserved identity.
    Untrusted,
    /// A deliberate backend runtime: writes as SYSTEM and may attribute.
    Backend,
    /// A Jazz self-signed client identity, proven by `token`.
    SelfSigned {
        /// The signed capability.
        token: &'a str,
        /// The app it is for.
        app_id: &'a str,
        /// The author it claims.
        claimed_author: &'a str,
    },
}

fn untrusted_author(config: &OpenConfig) -> Result<(), AuthorSubjectError> {
    AuthorSubject::from_untrusted_canonical(config.identity.author.canonical()).map(|_| ())
}

/// Verify a Jazz self-signed client identity against the browser-safe
/// clock (`std::time` panics on wasm32).
pub fn verify_self_signed(
    token: &str,
    app_id: &str,
    claimed_author: &str,
) -> HostResult<AuthorSubject> {
    let now_seconds = web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_err(|error| HostError::Argument(error.to_string()))?
        .as_secs();
    Ok(jazz_types::identity::verify_client_runtime_author_at(
        token,
        app_id,
        claimed_author,
        now_seconds,
    )?)
}

/// The account-bound local-first author `proof` proves `claimed_author` is.
fn verify_local_first(proof: &SelfSignedProof, claimed_author: &str) -> HostResult<AuthorSubject> {
    let author = verify_self_signed(&proof.token, &proof.app_id, claimed_author)?;
    if author.principal_parts().0 != AuthorSubject::LOCAL_FIRST_ISSUER
        || author.account_id().is_none()
    {
        return Err("session admission requires an account-bound local-first proof".into());
    }
    Ok(author)
}

/// The identity `config` opens with under `how`, checked; and whether the
/// runtime is a trusted backend.
pub fn open_identity(config: &mut OpenConfig, how: OpenAs<'_>) -> HostResult<(DbIdentity, bool)> {
    match how {
        OpenAs::Untrusted => {
            untrusted_author(config).map_err(|error| HostError::Argument(error.to_string()))?;
            Ok((config.identity.into(), false))
        }
        OpenAs::Backend => {
            // Every caller-controlled field passes the ordinary ingress before
            // this explicit backend form derives SYSTEM.
            untrusted_author(config).map_err(|error| HostError::Argument(error.to_string()))?;
            Ok((
                DbIdentity {
                    node: config.identity.node,
                    author: AuthorSubject::SYSTEM,
                },
                true,
            ))
        }
        OpenAs::SelfSigned {
            token,
            app_id,
            claimed_author,
        } => {
            config.identity.author = verify_self_signed(token, app_id, claimed_author)?;
            Ok((config.identity.into(), false))
        }
    }
}

/// The [`DbConfig`] for `schema` under `config` and `identity`.
pub fn db_config(schema: JazzSchema, config: &OpenConfig, identity: DbIdentity) -> DbConfig {
    let db_config = DbConfig::new(schema, identity);
    match config.row_id_seed {
        Some(seed) => db_config.with_id_source(SeededRowIdSource::new(seed)),
        None => db_config,
    }
}

/// Open an in-memory database. `history_complete` needs nothing: every
/// SQLite store keeps full history.
pub async fn open_memory(db_config: DbConfig) -> Result<Db, Error> {
    Db::open(db_config).await
}

/// Open a durable store in the SQLite file at `path` (see [`open_memory`]).
pub async fn open_file(
    db_config: DbConfig,
    path: &std::path::Path,
    options: crate::db::FileOptions,
) -> Result<Db, Error> {
    Db::open_at(db_config, path, options).await
}

// ---- progress ------------------------------------------------------------------------

/// Wakers of futures waiting on sync progress: a server answer, a write's
/// fate, a subscription's next event. The database does not register
/// wakers; its waits complete when a later tick or write changes their
/// state, so every tick and every local write wakes all of them.
#[derive(Clone, Default)]
pub struct Progress(Rc<RefCell<Vec<Waker>>>);

impl Progress {
    /// Wake `context`'s task on the next progress.
    pub fn park(&self, context: &Context<'_>) {
        let mut wakers = self.0.borrow_mut();
        if !wakers.iter().any(|waker| waker.will_wake(context.waker())) {
            wakers.push(context.waker().clone());
        }
    }

    /// Something may have progressed: every parked task polls again.
    pub fn wake_all(&self) {
        let wakers = std::mem::take(&mut *self.0.borrow_mut());
        for waker in wakers {
            waker.wake();
        }
    }

    /// `future`, parking its waker here whenever it is pending.
    pub fn parked<F: Future>(&self, future: F) -> Parked<F> {
        Parked {
            future: Box::pin(future),
            progress: self.clone(),
            kick: None,
        }
    }
}

/// A future that parks on [`Progress`] while pending.
pub struct Parked<F: Future> {
    future: Pin<Box<F>>,
    progress: Progress,
    /// Asked for a tick after the future's first poll: what it queued for
    /// the upstream (a remote read, or a local-first read's refresh) goes
    /// out without waiting for unrelated work.
    kick: Option<Rc<Hooks>>,
}

impl<F: Future> Future for Parked<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let poll = self.future.as_mut().poll(context);
        if poll.is_pending() {
            self.progress.park(context);
        }
        if let Some(hooks) = self.kick.take() {
            hooks.schedule_tick();
        }
        poll
    }
}

/// A boxed binding future.
pub type HostFuture<T> = Pin<Box<dyn Future<Output = HostResult<T>>>>;

/// See [`Hooks::rejected`].
/// Told of a refused upload no wait claimed; returns whether the refusal
/// reached the application (a listener was there to take it).
pub type RejectedHook = Box<dyn Fn(&TransactionId, &Error, bool, Option<Rc<Cell<bool>>>) -> bool>;

/// Hooks a host gives the database it binds.
#[derive(Default)]
pub struct Hooks {
    /// Waits on sync progress.
    pub progress: Progress,
    /// Asks the host for a tick: there is sync work to send or apply.
    pub schedule_tick: RefCell<Option<Box<dyn Fn()>>>,
    /// Told of an upload the authority refused that no wait claimed yet,
    /// whether it was a serializable transaction, and the write's claim
    /// if it has one: a wait the caller makes before the host delivers the
    /// report (the task that wrote it ends) still claims it.
    pub rejected: RefCell<Option<RejectedHook>>,
    /// Each uploaded write's claim, by commit seq: a wait sets it, and a
    /// dropped receipt leaves no one to claim it.
    claims: RefCell<BTreeMap<u64, Weak<Cell<bool>>>>,
    /// The claim count after the last sweep of dropped receipts.
    swept: Cell<usize>,
}

impl Hooks {
    /// Ask the host for a tick.
    pub fn schedule_tick(&self) {
        if let Some(schedule) = self.schedule_tick.borrow().as_ref() {
            schedule();
        }
    }

    /// After a local write: subscriptions have new events, and the write
    /// has to go upstream.
    pub fn after_write(&self) {
        self.progress.wake_all();
        self.schedule_tick();
    }

    /// Follow `write`'s fate: if the authority refuses it before a wait
    /// sets `claimed`, [`Hooks::rejected`] is told.
    pub fn track(&self, write: &Write, claimed: &Rc<Cell<bool>>) {
        if write.rejected.is_some() {
            return;
        }
        let mut claims = self.claims.borrow_mut();
        claims.insert(write.seq, Rc::downgrade(claimed));
        if claims.len() > 2 * self.swept.get().max(64) {
            claims.retain(|_, claim| claim.strong_count() > 0);
            self.swept.set(claims.len());
        }
    }

    /// Tell [`Hooks::rejected`] of each refused upload no wait claimed.
    /// A relayed session's refused commit is reported under its own id.
    /// Returns the seqs of `node`'s own refusals that reached the
    /// application, through a wait or the hook.
    fn report(&self, node: NodeUuid, rejections: Vec<crate::db::UpstreamRejection>) -> Vec<u64> {
        let mut delivered = Vec::new();
        for rejection in rejections {
            let (seq, error) = (rejection.seq, &rejection.error);
            let writer = NodeUuid::from_bytes(rejection.writer);
            let claim = (writer == node)
                .then(|| self.claims.borrow_mut().remove(&seq))
                .flatten()
                .and_then(|claim| claim.upgrade());
            let reached = if claim.as_ref().is_some_and(|claim| claim.get()) {
                true
            } else if let Some(rejected) = self.rejected.borrow().as_ref() {
                let tx_id = TransactionId::from_committed_tx(TxId::new(TxTime(seq), writer));
                rejected(&tx_id, error, rejection.serializable, claim)
            } else {
                false
            };
            if reached && writer == node {
                delivered.push(seq);
            }
        }
        delivered
    }
}

/// The public `kind` of a transaction in a mutation-error event.
pub fn transaction_kind(serializable: bool) -> &'static str {
    if serializable {
        "serializable"
    } else {
        "mergeable"
    }
}

/// The public code and reason of a mutation-error event for `error`; an
/// `upstream` one is the authority's refusal of an upload.
pub fn mutation_error_parts(error: &Error, upstream: bool) -> (&'static str, String) {
    match (error.is_authorization_denied(), upstream) {
        (true, true) => (
            "permission_denied",
            "Write rejected by server authorization".to_owned(),
        ),
        (true, false) => ("permission_denied", error.message.clone()),
        (false, _) => (error.code.as_str(), error.message.clone()),
    }
}

// ---- writes ---------------------------------------------------------------------------

#[derive(Serialize)]
struct WriteResultPayload {
    row_id: RowUuid,
    tx_id: TxId,
}

/// A committed write as a binding returns it.
#[derive(Clone, Debug)]
pub struct Write {
    node: NodeUuid,
    /// postcard `{ row_id, tx_id }`.
    pub payload: Vec<u8>,
    /// The written row (zero for a transaction commit).
    pub row_id: RowUuid,
    /// The committing transaction.
    pub tx_id: TransactionId,
    /// The local commit seq, for [`Binding::wait_for`].
    pub seq: u64,
    /// Why the write was refused when it applied, if it was: its waits
    /// fail with this, and nothing was committed.
    pub rejected: Option<Error>,
    /// Whether the write commits a serializable transaction.
    pub serializable: bool,
}

// ---- the binding ------------------------------------------------------------------

/// An open database (or one of its schema views) and its host hooks.
#[derive(Clone)]
pub struct Binding {
    /// The database.
    pub db: Rc<Db>,
    /// The host's hooks.
    pub hooks: Rc<Hooks>,
    node: NodeUuid,
    author: AuthorSubject,
    trusted_backend: bool,
    /// Open transactions a staged write was refused in for its row's
    /// state: the transaction's commit reports the first such refusal.
    refused: Rc<RefCell<BTreeMap<OpenTransactionId, Error>>>,
    /// Open transactions bound to an identity: their writes are its, and
    /// their reads run as it.
    bound: Rc<RefCell<BTreeMap<OpenTransactionId, WriteIdentity>>>,
    /// Local-first accounts a backend admitted (see
    /// [`Binding::admit_local_first_session`]), by canonical author.
    admitted: Rc<RefCell<BTreeMap<String, SelfSignedProof>>>,
    /// Whether the next tick reports the retry store's refusals again.
    requeue: Rc<Cell<bool>>,
}

/// A Jazz self-signed proof of a local-first account.
#[derive(Clone)]
struct SelfSignedProof {
    token: String,
    app_id: String,
}

impl Binding {
    /// Bind `db`, opened as `identity`.
    /// Unless `db` is the authority, it is a replica from the start: what
    /// it writes before its upstream connects is uploaded once it does.
    pub fn new(db: Db, identity: DbIdentity, trusted_backend: bool) -> Self {
        if !db.is_authority() {
            db.become_replica();
        }
        db.track_upstream_rejections();
        Self {
            db: Rc::new(db),
            hooks: Rc::default(),
            node: identity.node,
            author: identity.author,
            trusted_backend,
            refused: Rc::default(),
            bound: Rc::default(),
            admitted: Rc::default(),
            requeue: Rc::new(Cell::new(true)),
        }
    }

    /// `future`, parked on this binding's progress, asking for a tick
    /// after its first poll (it may have queued a request upstream).
    fn parked<F: Future>(&self, future: F) -> Parked<F> {
        Parked {
            kick: Some(Rc::clone(&self.hooks)),
            ..self.hooks.progress.parked(future)
        }
    }

    /// Let this backend runtime write and read as the local-first account
    /// `claimed_author`, proven by `token`. Raw author bytes never name a
    /// Jazz-reserved identity otherwise. The admission covers this runtime
    /// and its schema views; the proof is checked again at every use, so an
    /// expired one stops admitting.
    pub fn admit_local_first_session(
        &self,
        token: &str,
        app_id: &str,
        claimed_author: &str,
    ) -> HostResult<()> {
        self.require_trusted_backend()?;
        let proof = SelfSignedProof {
            token: token.to_owned(),
            app_id: app_id.to_owned(),
        };
        verify_local_first(&proof, claimed_author)?;
        self.admitted
            .borrow_mut()
            .insert(claimed_author.to_owned(), proof);
        Ok(())
    }

    /// The subject `bytes` (canonical author JSON) names: an ordinary
    /// author, or a local-first account this backend admitted.
    fn author_of(&self, bytes: &[u8]) -> HostResult<AuthorSubject> {
        match author(bytes) {
            Ok(author) => Ok(author),
            Err(error) => {
                let admitted = std::str::from_utf8(bytes).ok().and_then(|canonical| {
                    Some((canonical, self.admitted.borrow().get(canonical)?.clone()))
                });
                match admitted {
                    Some((canonical, proof)) => verify_local_first(&proof, canonical),
                    None => Err(error),
                }
            }
        }
    }

    /// The author this database writes as.
    pub fn author(&self) -> AuthorSubject {
        self.author
    }

    /// Whether this is an explicit backend runtime.
    pub fn trusted_backend(&self) -> bool {
        self.trusted_backend
    }

    /// A typed schema view backed by the same runtime.
    pub fn register_schema(&self, schema: &[u8]) -> HostResult<Self> {
        let schema = decode_schema(schema)?;
        let view = block_on(self.db.register_schema_view(schema))?;
        Ok(Self {
            db: Rc::new(view),
            ..self.clone()
        })
    }

    fn require_trusted_backend(&self) -> HostResult<()> {
        if self.trusted_backend {
            Ok(())
        } else {
            Err("backend attribution requires an explicit backend runtime".into())
        }
    }

    /// Attributed writes are a privilege-escalation surface: their author is
    /// provenance while admission stays SYSTEM, so only backends make them.
    fn checked(&self, identity: WriteIdentity) -> HostResult<WriteIdentity> {
        if matches!(identity, WriteIdentity::Attribution(_)) {
            self.require_trusted_backend()?;
        }
        Ok(identity)
    }

    /// The author a read runs as: the given one, SYSTEM for a backend, or
    /// the database's own.
    fn read_author(&self, author: Option<&[u8]>) -> HostResult<Option<AuthorSubject>> {
        match author {
            Some(bytes) => Ok(Some(self.author_of(bytes)?)),
            None if self.trusted_backend => Ok(Some(AuthorSubject::SYSTEM)),
            None => Ok(None),
        }
    }

    /// A write's receipt. A refusal decided by the row's stored state (see
    /// [`Error::is_apply_time_rejection`]) is an apply-time rejection:
    /// the call returns a receipt for a rejected transaction, and the
    /// host reports it through the write's waits.
    fn write(&self, write: Result<WriteHandle, Error>) -> HostResult<Write> {
        let write = match write {
            Err(error) if error.is_apply_time_rejection() => return self.rejected(error, false),
            write => write?,
        };
        self.hooks.after_write();
        self.receipt(write.row_uuid(), write.mergeable_tx_id())
    }

    fn rejected(&self, error: Error, serializable: bool) -> HostResult<Write> {
        // Rejected transactions take ids from the top of this node's range,
        // which commits (counted from the bottom) never reach.
        static REJECTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = REJECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut write = self.receipt(RowUuid::from_bytes([0; 16]), u64::MAX - n)?;
        write.rejected = Some(error);
        write.serializable = serializable;
        Ok(write)
    }

    fn receipt(&self, row_id: RowUuid, seq: u64) -> HostResult<Write> {
        let tx_id = TxId::new(TxTime(seq), self.node);
        Ok(Write {
            node: self.node,
            payload: postcard::to_allocvec(&WriteResultPayload { row_id, tx_id })
                .map_err(|error| HostError::Argument(error.to_string()))?,
            row_id,
            tx_id: TransactionId::from_committed_tx(tx_id),
            seq,
            rejected: None,
            serializable: false,
        })
    }

    /// Insert `cells` (binding-codec bytes).
    pub fn insert<O: HostObject>(
        &self,
        table: &str,
        cells: &[u8],
        options: &O,
    ) -> HostResult<Write> {
        let cells = decode_cells(cells)?;
        let mut options = insert_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.checked(options.identity)?;
        self.write(block_on(self.db.insert(table, cells, options)))
    }

    /// Update `row` with `patch`.
    pub fn update<O: HostObject>(
        &self,
        table: &str,
        row: &[u8],
        patch: &[u8],
        options: &O,
    ) -> HostResult<Write> {
        let row = row_uuid(row)?;
        let patch = decode_cells(patch)?;
        let mut options = update_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.checked(options.identity)?;
        self.write(block_on(self.db.update(table, row, patch, options)))
    }

    /// Partial large-value updates together with an ordinary patch.
    pub fn update_large_values(
        &self,
        table: &str,
        row: &[u8],
        patch: &[u8],
        mutations: Vec<LargeValueUpdate>,
        updated_at_ms: Option<f64>,
    ) -> HostResult<Write> {
        let row = row_uuid(row)?;
        let patch = decode_cells(patch)?;
        let options = UpdateOptions {
            updated_at_ms: updated_at_ms
                .map(|value| checked_u64(value, "updatedAtMs"))
                .transpose()?,
            ..Default::default()
        };
        self.write(block_on(self.db.update_with_large_value_mutations(
            table, row, patch, mutations, options,
        )))
    }

    /// Insert or update `row`.
    pub fn upsert<O: HostObject>(
        &self,
        table: &str,
        row: &[u8],
        cells: &[u8],
        options: &O,
    ) -> HostResult<Write> {
        // The removed `branch` option is refused before any payload is read,
        // so a malformed row cannot mask it.
        let mut options = upsert_options_as(options, &|bytes| self.author_of(bytes))?;
        let row = row_uuid(row)?;
        let cells = decode_cells(cells)?;
        options.identity = self.checked(options.identity)?;
        self.write(block_on(self.db.upsert(table, row, cells, options)))
    }

    /// Delete `row`.
    pub fn delete<O: HostObject>(&self, table: &str, row: &[u8], options: &O) -> HostResult<Write> {
        let row = row_uuid(row)?;
        let mut options = delete_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.checked(options.identity)?;
        self.write(block_on(self.db.delete(table, row, options)))
    }

    /// Undelete `row`, with `cells` or (`None`) the values it was deleted with.
    pub fn restore<O: HostObject>(
        &self,
        table: &str,
        row: &[u8],
        cells: Option<&[u8]>,
        options: &O,
    ) -> HostResult<Write> {
        let row = row_uuid(row)?;
        let cells = cells.map(decode_cells).transpose()?;
        let mut options = restore_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.checked(options.identity)?;
        self.write(block_on(self.db.restore(table, row, cells, options)))
    }

    // ---- transactions ----

    /// Begin one owner-wide transaction, bound to the session `author` or
    /// the backend `attribution` if one is given: its writes are that
    /// identity's, and its reads run as it.
    pub fn begin_transaction(
        &self,
        id: &str,
        kind: &str,
        author: Option<&[u8]>,
        attribution: Option<&[u8]>,
    ) -> HostResult<()> {
        let id = open_transaction(id)?;
        if author.is_some() && attribution.is_some() {
            return Err("transaction identity cannot contain both author and attribution".into());
        }
        let identity = match (author, attribution) {
            (Some(author), _) => WriteIdentity::Session(self.author_of(author)?),
            (_, Some(attribution)) => {
                self.checked(WriteIdentity::Attribution(self.author_of(attribution)?))?
            }
            (None, None) => WriteIdentity::Database,
        };
        match TransactionKind::parse(kind)? {
            TransactionKind::Mergeable => block_on(self.db.begin_mergeable(id))?,
            TransactionKind::Serializable => block_on(self.db.begin_serializable(id))?,
        }
        if identity != WriteIdentity::Database {
            self.bound.borrow_mut().insert(id, identity);
        }
        Ok(())
    }

    /// Commit an owner-wide transaction (`kind` defaults to mergeable).
    pub fn commit_transaction(&self, id: &str, kind: Option<&str>) -> HostResult<Write> {
        let id = open_transaction(id)?;
        self.bound.borrow_mut().remove(&id);
        let kind = TransactionKind::parse(kind.unwrap_or("mergeable"))?;
        let serializable = matches!(kind, TransactionKind::Serializable);
        if let Some(error) = self.refused.borrow_mut().remove(&id) {
            self.db.abandon_transaction_handle(id)?;
            return self.rejected(error, serializable);
        }
        let receipt = match kind {
            TransactionKind::Mergeable => block_on(self.db.commit_mergeable_handle(id)),
            TransactionKind::Serializable => block_on(self.db.commit_serializable_handle(id)),
        };
        // A conflict is the commit's outcome, reported through its receipt
        // as the authority's verdict on a replica's commit is.
        let receipt = match receipt {
            Err(error) if error.code == ErrorCode::TransactionConflict => {
                return self.rejected(error, serializable);
            }
            receipt => receipt?,
        };
        self.hooks.after_write();
        let mut write = self.receipt(RowUuid::from_bytes([0; 16]), receipt.0)?;
        write.serializable = serializable;
        Ok(write)
    }

    /// Roll back an open transaction.
    pub fn rollback_transaction(&self, id: &str) -> HostResult<()> {
        let id = open_transaction(id)?;
        self.refused.borrow_mut().remove(&id);
        self.bound.borrow_mut().remove(&id);
        Ok(self.db.abandon_transaction_handle(id)?)
    }

    /// The identity a write staged in transaction `id` runs as: the one the
    /// transaction is bound to, if any, which the write may only repeat.
    fn staged_identity(
        &self,
        id: OpenTransactionId,
        identity: WriteIdentity,
    ) -> HostResult<WriteIdentity> {
        let identity = self.checked(identity)?;
        match self.bound.borrow().get(&id) {
            None => Ok(identity),
            Some(bound) if identity == WriteIdentity::Database || identity == *bound => Ok(*bound),
            Some(_) => Err(HostError::Argument(
                "a staged write's identity must match its open transaction's bound identity".into(),
            )),
        }
    }

    /// The author a read in open transaction `id` runs as: the session the
    /// transaction is bound to, which an explicit author may only repeat.
    fn transaction_reader(
        &self,
        id: OpenTransactionId,
        author: Option<&[u8]>,
    ) -> HostResult<Option<AuthorSubject>> {
        let bound = match self.bound.borrow().get(&id) {
            Some(WriteIdentity::Session(bound)) => *bound,
            _ => return Ok(None),
        };
        match author.map(|bytes| self.author_of(bytes)).transpose()? {
            Some(author) if author != bound => Err(HostError::Argument(
                "the open transaction identity does not match the transaction's bound identity"
                    .into(),
            )),
            _ => Ok(Some(bound)),
        }
    }

    /// A staged write's outcome. Like a direct write's (see
    /// [`Self::write`]), a refusal decided by the row's stored state is not
    /// the call's error: the transaction stays open and its commit is
    /// rejected with the first such refusal. So is a serializable write's
    /// conflict with a row that moved past its snapshot.
    fn staged(&self, id: OpenTransactionId, staged: Result<WriteHandle, Error>) -> HostResult<()> {
        match staged {
            Err(error)
                if error.is_apply_time_rejection()
                    || error.code == ErrorCode::TransactionConflict =>
            {
                self.refused.borrow_mut().entry(id).or_insert(error);
                Ok(())
            }
            staged => staged.map(drop).map_err(Into::into),
        }
    }

    /// Stage an insert; returns the row id.
    pub fn insert_in_transaction<O: HostObject>(
        &self,
        id: &str,
        table: &str,
        cells: &[u8],
        options: &O,
    ) -> HostResult<RowUuid> {
        let id = open_transaction(id)?;
        let cells = decode_cells(cells)?;
        let mut options = insert_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.staged_identity(id, options.identity)?;
        let row = *options.row_id.get_or_insert_with(|| self.db.next_row_id());
        let staged = block_on(self.db.tx_ref(id).insert(table, cells, options));
        self.staged(id, staged)?;
        Ok(row)
    }

    /// Stage an update.
    pub fn update_in_transaction<O: HostObject>(
        &self,
        id: &str,
        table: &str,
        row: &[u8],
        patch: &[u8],
        options: &O,
    ) -> HostResult<()> {
        let id = open_transaction(id)?;
        let row = row_uuid(row)?;
        let patch = decode_cells(patch)?;
        let mut options = update_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.staged_identity(id, options.identity)?;
        let staged = block_on(self.db.tx_ref(id).update(table, row, patch, options));
        self.staged(id, staged)
    }

    /// Stage an upsert.
    pub fn upsert_in_transaction<O: HostObject>(
        &self,
        id: &str,
        table: &str,
        row: &[u8],
        cells: &[u8],
        options: &O,
    ) -> HostResult<()> {
        let mut options = upsert_options_as(options, &|bytes| self.author_of(bytes))?;
        let id = open_transaction(id)?;
        options.identity = self.staged_identity(id, options.identity)?;
        let row = row_uuid(row)?;
        let cells = decode_cells(cells)?;
        let staged = block_on(self.db.tx_ref(id).upsert(table, row, cells, options));
        self.staged(id, staged)
    }

    /// Stage a delete.
    pub fn delete_in_transaction<O: HostObject>(
        &self,
        id: &str,
        table: &str,
        row: &[u8],
        options: &O,
    ) -> HostResult<()> {
        let id = open_transaction(id)?;
        let row = row_uuid(row)?;
        let mut options = delete_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.staged_identity(id, options.identity)?;
        let staged = block_on(self.db.tx_ref(id).delete(table, row, options));
        self.staged(id, staged)
    }

    /// Stage partial large-value updates; they read the row with the
    /// transaction's earlier writes applied.
    pub fn update_large_values_in_transaction(
        &self,
        id: &str,
        table: &str,
        row: &[u8],
        patch: &[u8],
        mutations: Vec<LargeValueUpdate>,
        updated_at_ms: Option<f64>,
    ) -> HostResult<()> {
        let id = open_transaction(id)?;
        let row = row_uuid(row)?;
        let patch = decode_cells(patch)?;
        let options = UpdateOptions {
            updated_at_ms: updated_at_ms
                .map(|value| checked_u64(value, "updatedAtMs"))
                .transpose()?,
            identity: self.staged_identity(id, WriteIdentity::Database)?,
            ..Default::default()
        };
        let staged = block_on(
            self.db
                .tx_ref(id)
                .update_with_large_value_mutations(table, row, patch, mutations, options),
        );
        self.staged(id, staged)
    }

    /// Stage an undelete of `row` with `cells`.
    pub fn restore_in_transaction<O: HostObject>(
        &self,
        id: &str,
        table: &str,
        row: &[u8],
        cells: Option<&[u8]>,
        options: &O,
    ) -> HostResult<()> {
        let id = open_transaction(id)?;
        let row = row_uuid(row)?;
        let cells = cells.map(decode_cells).transpose()?;
        let mut options = restore_options_as(options, &|bytes| self.author_of(bytes))?;
        options.identity = self.staged_identity(id, options.identity)?;
        let staged = block_on(self.db.tx_ref(id).restore(table, row, cells, options));
        self.staged(id, staged)
    }

    // ---- reads ----

    /// The row as stored, deleted or not: the write-merge bridge's exact
    /// local state. Not a query; write authorization stays at the mutation.
    pub fn local_current_row(&self, table: &str, row: &[u8]) -> HostResult<Vec<u8>> {
        let row = row_uuid(row)?;
        let query = Query::from(table).filter(jazz_model::query::eq(
            jazz_model::query::col("id"),
            jazz_model::query::lit(Value::Uuid(row.0)),
        ));
        let prepared = self.db.prepare_query(&query)?;
        let mut opts = ReadOpts::for_read_tier(ReadTier::LocalOnly);
        opts.read_view.include_deleted = true;
        let rows = block_on(self.db.all(&prepared, opts))?;
        Ok(crate::binding_codec::encode_rows(&rows)?)
    }

    /// A one-shot read of a postcard query, as binding-codec rows (or a
    /// relation snapshot). `claims` apply only with an explicit `author`.
    pub fn all<O: HostObject>(
        &self,
        query: Vec<u8>,
        opts: &O,
        open_transaction_id: Option<&str>,
        author: Option<&[u8]>,
        claims: Option<serde_json::Value>,
    ) -> HostResult<HostFuture<Vec<u8>>> {
        let opts = read_opts(opts)?;
        let open_tx = open_transaction_id.map(open_transaction).transpose()?;
        // A transaction bound to a session reads as it.
        let bound = match open_tx {
            Some(id) => self.transaction_reader(id, author)?,
            None => None,
        };
        let explicit = author.is_some() || bound.is_some();
        let author = match bound {
            Some(bound) => Some(bound),
            None => self.read_author(author)?,
        };
        let admission = match (explicit, author) {
            (true, Some(author)) => Some((
                author,
                self::claims(author, claims.unwrap_or(serde_json::Value::Null))?,
            )),
            _ => None,
        };
        let db = self.db.clone();
        // A Remote read must be covered by the server; its budget runs
        // from its first wait.
        let require_coverage = opts.tier == crate::db::ReadTier::Remote;
        let started = std::cell::Cell::new(None::<web_time::Instant>);
        let expired = move || {
            let first = started.get().unwrap_or_else(|| {
                let now = web_time::Instant::now();
                started.set(Some(now));
                now
            });
            first.elapsed() >= REMOTE_READ_COVERAGE_BUDGET
        };
        let hooks = Rc::clone(&self.hooks);
        Ok(Box::pin(self.parked(async move {
            let coverage: Option<&dyn Fn() -> bool> = require_coverage.then_some(&expired);
            let mut read =
                std::pin::pin!(db.all_serialized_query(&query, opts, open_tx, admission, coverage));
            // A row whose large values are missing records their chunks for
            // the next send; ask for the tick that sends it, or the read
            // waits on unrelated sync traffic.
            let result = std::future::poll_fn(|context| {
                let poll = read.as_mut().poll(context);
                if poll.is_pending() && db.owes_chunk_requests() {
                    hooks.schedule_tick();
                }
                poll
            })
            .await?;
            Ok(match result {
                SerializedReadResult::Rows(rows) => crate::binding_codec::encode_rows(&rows),
                SerializedReadResult::Relation(snapshot) => {
                    crate::binding_codec::encode_relation_snapshot(&snapshot)
                }
            }?)
        })))
    }

    /// Subscribe to a postcard query. A remote attachment goes upstream on
    /// the next tick.
    pub fn subscribe<O: HostObject>(
        &self,
        query: &[u8],
        opts: &O,
        author: Option<&[u8]>,
        claims: Option<serde_json::Value>,
    ) -> HostResult<SubscriptionStream> {
        let opts = read_opts(opts)?;
        let explicit = author.is_some();
        let author = self.read_author(author)?;
        let query: Query = postcard::from_bytes(query)
            .map_err(|error| HostError::Argument(format!("undecodable query: {error}")))?;
        let mut prepared = self.db.prepare_query(&query)?;
        let stream = match author {
            Some(author) => {
                if explicit {
                    let claims = self::claims(author, claims.unwrap_or(serde_json::Value::Null))?;
                    prepared = prepared.with_identity_claims(author, claims);
                }
                block_on(self.db.subscribe_for_identity(&prepared, opts, author))
            }
            None => block_on(self.db.subscribe(&prepared, opts)),
        }?;
        self.hooks.schedule_tick();
        Ok(stream)
    }

    /// The stream's next event, waiting for a write or tick when there is
    /// none.
    pub fn next_event<'a>(&'a self, stream: &'a mut SubscriptionStream) -> NextEvent<'a> {
        NextEvent {
            stream,
            progress: &self.hooks.progress,
        }
    }

    /// Bind correlation claims to this client's own identity.
    pub fn set_session_claims(&self, claims: serde_json::Value) -> HostResult<()> {
        let claims = self::claims(self.author, claims)?;
        self.db.set_identity_claims(self.author, claims);
        self.hooks.progress.wake_all();
        Ok(())
    }

    /// Bind correlation claims to `author` (a served identity).
    pub fn set_identity_claims(&self, author: &[u8], claims: serde_json::Value) -> HostResult<()> {
        let author = self.author_of(author)?;
        let claims = self::claims(author, claims)?;
        self.db.set_identity_claims(author, claims);
        self.hooks.progress.wake_all();
        Ok(())
    }

    // ---- advice, snapshots, durability ----

    /// Ask the authority whether `action` would be allowed; the request goes
    /// out on the next tick. Resolves to `"allowed" | "denied" | "unknown"`.
    /// Ask the authority about `action` for this database's own session,
    /// or, on a trusted backend, for `author` with its `claims`.
    pub fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
        author: Option<&[u8]>,
        claims: Option<serde_json::Value>,
    ) -> HostResult<(u64, HostFuture<&'static str>)> {
        let session = match author {
            Some(author) => {
                let author = self.author_of(author)?;
                Some((
                    author,
                    self::claims(author, claims.unwrap_or(serde_json::Value::Null))?,
                ))
            }
            None => None,
        };
        let id = self.db.request_permission_advice(action, session);
        self.hooks.after_write();
        let db = self.db.clone();
        let progress = self.hooks.progress.clone();
        let future =
            Box::pin(
                async move { Ok(advice_str(progress.parked(db.permission_advice(id)).await)) },
            );
        Ok((id, future))
    }

    /// Drop a pending advice request; it resolves `"unknown"`.
    pub fn cancel_permission_advice(&self, id: u64) {
        self.db.cancel_permission_advice_request(id);
        self.hooks.progress.wake_all();
    }

    /// Capture a snapshot reference, as its persisted string's bytes.
    pub fn capture_snapshot(&self) -> HostFuture<Vec<u8>> {
        let db = self.db.clone();
        Box::pin(
            self.parked(
                async move { Ok(db.capture_snapshot().await?.to_persisted().into_bytes()) },
            ),
        )
    }

    /// Resolve once write `seq` reached `tier`: at once for `none` and
    /// `local`, on the server's acceptance for `global` (rejecting if it
    /// refused).
    pub fn wait_for(&self, seq: u64, tier: &str) -> HostResult<HostFuture<()>> {
        let tier = binding_args::durability_tier(tier)?;
        let db = self.db.clone();
        Ok(Box::pin(self.hooks.progress.parked(async move {
            db.wait_for_transaction(seq, tier).await?;
            Ok(())
        })))
    }

    /// Resolve once `write` reached `tier`; fail at once with its
    /// rejection, if it was rejected.
    pub fn wait_for_write(&self, write: &Write, tier: &str) -> HostResult<HostFuture<()>> {
        if self.node != write.node {
            return Err("write belongs to a different database node".into());
        }
        if let Some(error) = &write.rejected {
            binding_args::durability_tier(tier)?;
            let error = error.clone();
            return Ok(Box::pin(async move { Err(HostError::Engine(error)) }));
        }
        self.wait_for(write.seq, tier)
    }

    /// Resolve once every local write so far reached the server. Local
    /// durability is the host's store to flush.
    pub fn wait_for_pending_writes_global(&self) -> HostFuture<()> {
        let db = self.db.clone();
        Box::pin(self.hooks.progress.parked(async move {
            db.wait_for_pending_writes(DurabilityTier::Global).await?;
            Ok(())
        }))
    }

    /// How many local commits still wait for the server's verdict.
    pub fn pending_upload_count(&self) -> usize {
        self.db.pending_upload_count()
    }

    /// Apply received frames and send what is due; waits wake.
    pub async fn tick(&self) -> HostResult<()> {
        let result = self.db.tick().await;
        // Refusals kept from before (a reopened store's, or ones no
        // listener took) go out again first.
        let requeued = if self.requeue.replace(false) {
            self.db.requeue_rejected_transactions()
        } else {
            Ok(())
        };
        let rejections = self.db.take_upstream_rejections();
        let delivered = self.hooks.report(self.node, rejections);
        // Delivered: the retry store lets go of their payloads. One no
        // listener took stays, for the next listener or the next open.
        let discarded = delivered
            .into_iter()
            .try_for_each(|seq| self.db.discard_rejected_transaction(seq));
        self.hooks.progress.wake_all();
        result?;
        requeued?;
        Ok(discarded?)
    }

    /// Report the refusals the retry store still holds again on the next
    /// tick: the host now has a listener for them.
    pub fn redeliver_rejections(&self) {
        self.requeue.set(true);
        self.hooks.schedule_tick();
    }

    /// The slow-query report due now: one line per slow query shape,
    /// naming `app`: the first at once, then at most every 10 seconds. The host
    /// writes the lines where it reports problems (`console.warn`).
    pub fn take_slow_query_report(&self, app: Option<&str>) -> Vec<String> {
        self.db.take_slow_query_report(app)
    }

    /// Report query runs of at least `ms` milliseconds; 0 turns the log
    /// off.
    pub fn set_slow_query_threshold_ms(&self, ms: f64) -> HostResult<()> {
        if !ms.is_finite() || ms < 0.0 {
            return Err(HostError::Argument(format!(
                "slow query threshold must be a non-negative number of milliseconds, not {ms}"
            )));
        }
        self.db.set_slow_query_threshold(
            (ms > 0.0).then(|| std::time::Duration::from_secs_f64(ms / 1000.0)),
        );
        Ok(())
    }

    /// Make this store the optimistic, non-durable foreground of a durable
    /// relay (a browser tab over its worker): its store starts empty, so
    /// every subscription's opening waits for the relay's answer.
    pub fn set_non_durable_client(&self) {
        self.db.set_non_durable_client();
        self.hooks.progress.wake_all();
    }

    /// Report the path to the authoritative server (`"none" | "attempting"
    /// | "live" | "failed"`, or the TypeScript aliases).
    pub fn set_remote_link_hint(&self, state: &str) -> HostResult<()> {
        let hint = RemoteLinkHint::from_host_str(state)
            .ok_or_else(|| HostError::Argument(format!("unknown remote link state {state}")))?;
        self.db.set_remote_link_hint(hint);
        self.hooks.progress.wake_all();
        Ok(())
    }

    // ---- large values ----

    /// Configure upload ingress and unpublished-tree expiry.
    pub fn set_large_value_staging_policy(
        &self,
        incoming_bytes_per_window: f64,
        window_ms: f64,
        max_age_ms: Option<f64>,
    ) -> HostResult<()> {
        let incoming_bytes_per_window =
            checked_u64(incoming_bytes_per_window, "incomingBytesPerWindow")?;
        let window_ms = checked_u64(window_ms, "windowMs")?;
        if window_ms < 1 {
            return Err("windowMs must be at least 1".into());
        }
        let max_age_ms = max_age_ms
            .map(|value| checked_u64(value, "maxAgeMs"))
            .transpose()?
            .unwrap_or(LargeValueStagingPolicy::default().max_age_ms);
        self.db
            .set_large_value_staging_policy(LargeValueStagingPolicy {
                incoming_bytes_per_window,
                window_ms,
                max_age_ms,
            });
        Ok(())
    }

    /// Begin a streamed large-value mutation of `column`.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_streaming_mutation(
        &self,
        table: String,
        row: &[u8],
        cells: &[u8],
        column: String,
        mutation: Option<&str>,
        author: Option<&[u8]>,
        attribution: Option<&[u8]>,
        updated_at_ms: Option<f64>,
        head: Option<BranchSelector>,
        base: Option<BranchViewBase>,
    ) -> HostResult<StreamingMutation> {
        let identity = match (author, attribution) {
            (Some(_), Some(_)) => {
                return Err(
                    "streaming mutation identity cannot contain both author and attribution".into(),
                );
            }
            (Some(bytes), None) => WriteIdentity::Session(self.author_of(bytes)?),
            (None, Some(bytes)) => {
                self.require_trusted_backend()?;
                WriteIdentity::Attribution(self.author_of(bytes)?)
            }
            (None, None) => WriteIdentity::Database,
        };
        let row = row_uuid(row)?;
        let cells = decode_cells(cells)?;
        let mutation = match mutation.unwrap_or("insert") {
            "insert" => StreamingMutationKind::Insert,
            "update" => StreamingMutationKind::Update,
            "upsert" => StreamingMutationKind::Upsert,
            _ => return Err("unknown streaming mutation kind".into()),
        };
        let updated_at_ms = updated_at_ms
            .map(|value| checked_u64(value, "updatedAtMs"))
            .transpose()?;
        if base.is_some() && head.is_none() {
            return Err("streaming mutation base requires a branch head".into());
        }
        if matches!(identity, WriteIdentity::Attribution(_)) && head.is_some() {
            return Err(
                "backend-attributed streaming mutations do not support branch writes".into(),
            );
        }
        let upload = self
            .db
            .begin_streaming_value_upload(&table, &cells, &column)?;
        Ok(StreamingMutation {
            state: Some(Box::new(StreamingState {
                binding: self.clone(),
                upload,
                mutation,
                table,
                row,
                cells,
                column,
                identity,
                updated_at_ms,
                head,
                base,
            })),
            failed: false,
        })
    }

    // ---- sync links ----

    /// Connect to the server over byte frames the host carries (each one
    /// postcard `SyncMessage`). Resubscriptions and unsettled commits go
    /// out on the next tick.
    pub fn connect_upstream(&self) -> Link {
        let queues = WireQueues::default();
        let transport = Box::new(WireTransportAdapter::current(QueueTransport {
            queues: queues.clone(),
        }));
        let upstream = block_on(self.db.connect_upstream(transport));
        self.hooks.schedule_tick();
        Link {
            binding: self.clone(),
            queues,
            kind: LinkKind::Upstream(Some(upstream)),
        }
    }

    /// Serve a subscriber (another replica) as `identity` with `claims`.
    pub fn accept_subscriber(&self, identity: AuthorSubject, claims: Cells) -> Link {
        self.db.set_identity_claims(identity, claims);
        let queues = WireQueues::default();
        let transport = Box::new(WireTransportAdapter::current(QueueTransport {
            queues: queues.clone(),
        }));
        let subscriber = self.db.accept_subscriber(transport, identity);
        Link {
            binding: self.clone(),
            queues,
            kind: LinkKind::Subscriber {
                id: Some(subscriber),
                identity,
            },
        }
    }

    /// Close the database (a schema view leaves its owner open).
    pub async fn close(&self) -> HostResult<()> {
        Ok(self.db.close().await?)
    }
}

/// Permission advice actions from binding arguments.
pub mod advice {
    use super::*;

    /// Would inserting `cells` be allowed?
    pub fn insert(table: String, cells: &[u8]) -> HostResult<PermissionAdviceAction> {
        Ok(PermissionAdviceAction::Insert {
            table,
            cells: decode_cells(cells)?,
        })
    }

    /// Would reading `row` be allowed?
    pub fn read(table: String, row: &[u8]) -> HostResult<PermissionAdviceAction> {
        Ok(PermissionAdviceAction::Read {
            table,
            row: row_uuid(row)?,
        })
    }

    /// Would updating `row` with `patch` be allowed?
    pub fn update(table: String, row: &[u8], patch: &[u8]) -> HostResult<PermissionAdviceAction> {
        Ok(PermissionAdviceAction::Update {
            table,
            row: row_uuid(row)?,
            patch: decode_cells(patch)?,
        })
    }

    /// Would deleting `row` be allowed?
    pub fn delete(table: String, row: &[u8]) -> HostResult<PermissionAdviceAction> {
        Ok(PermissionAdviceAction::Delete {
            table,
            row: row_uuid(row)?,
        })
    }
}

/// The next event of a subscription (see [`Binding::next_event`]).
pub struct NextEvent<'a> {
    stream: &'a mut SubscriptionStream,
    progress: &'a Progress,
}

impl Future for NextEvent<'_> {
    type Output = SubscriptionEvent;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<SubscriptionEvent> {
        match self.stream.try_next_event() {
            Some(event) => Poll::Ready(event),
            None => {
                self.progress.park(context);
                Poll::Pending
            }
        }
    }
}

/// A subscription event as the host hands it to JavaScript.
#[derive(Clone, Debug, PartialEq)]
pub enum EventChunk {
    /// `{ type: "delta", delta, reset, settled, tier }`.
    Delta {
        /// Binding-codec delta bytes.
        delta: Vec<u8>,
        /// Replaces everything delivered so far.
        reset: bool,
        /// The result is settled at `tier`.
        settled: bool,
        /// The durability tier, as `Debug`-spelled.
        tier: String,
    },
    /// `{ type: "closed" }`.
    Closed,
    /// `{ type: "rejected", reason: { type: "ServerFailure", detail } }`.
    Rejected {
        /// Why.
        detail: String,
    },
}

/// Encode one subscription event.
pub fn event_chunk(event: SubscriptionEvent) -> HostResult<EventChunk> {
    Ok(match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            settled,
            tier,
            ..
        } => EventChunk::Delta {
            delta: crate::binding_codec::encode_subscription_delta(&added, &updated, &removed)?,
            reset,
            settled,
            tier: format!("{tier:?}"),
        },
        SubscriptionEvent::Closed => EventChunk::Closed,
        SubscriptionEvent::Rejected { reason } => EventChunk::Rejected { detail: reason },
    })
}

// ---- streaming mutations -----------------------------------------------------------

struct StreamingState {
    binding: Binding,
    upload: StreamingValueUpload,
    mutation: StreamingMutationKind,
    table: String,
    row: RowUuid,
    cells: Cells,
    column: String,
    identity: WriteIdentity,
    updated_at_ms: Option<u64>,
    head: Option<BranchSelector>,
    base: Option<BranchViewBase>,
}

/// A large value uploaded in chunks, then published as one row mutation.
/// Each push applies synchronously, so none is in flight when another call
/// arrives. A failed push closes the mutation.
pub struct StreamingMutation {
    state: Option<Box<StreamingState>>,
    /// A push failed: the upload takes no more chunks and cannot finish,
    /// but stays for [`StreamingMutation::abort`] to release.
    failed: bool,
}

fn closed() -> HostError {
    "streaming mutation is closed".into()
}

impl StreamingMutation {
    /// Upload the next chunk.
    pub fn push(&mut self, chunk: &[u8]) -> HostResult<()> {
        let state = self
            .state
            .as_mut()
            .filter(|_| !self.failed)
            .ok_or_else(closed)?;
        let pushed = block_on(
            state
                .binding
                .db
                .push_streaming_value_upload(&mut state.upload, chunk),
        );
        self.failed = pushed.is_err();
        Ok(pushed?)
    }

    /// Publish the uploaded value as the row mutation.
    pub fn finish(&mut self) -> HostResult<Write> {
        if self.failed {
            return Err(closed());
        }
        let state = *self.state.take().ok_or_else(closed)?;
        let write = block_on(state.binding.db.finish_streaming_value_upload(
            state.upload,
            state.mutation,
            &state.table,
            state.row,
            state.cells,
            &state.column,
            state.identity,
            state.updated_at_ms,
            state.head,
            state.base,
        ));
        state.binding.write(write)
    }

    /// Drop the upload; `false` when already closed.
    pub fn abort(&mut self) -> HostResult<bool> {
        let Some(state) = self.state.take() else {
            return Ok(false);
        };
        block_on(state.binding.db.abort_streaming_value_upload(state.upload))?;
        Ok(true)
    }
}

// ---- links ---------------------------------------------------------------------------

/// The frame queues between a link and its host carrier.
#[derive(Clone, Default)]
pub struct WireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound_ready: Rc<RefCell<Option<Box<dyn Fn()>>>>,
}

struct QueueTransport {
    queues: WireQueues,
}

impl WireTransport for QueueTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        if let Some(ready) = self.queues.outbound_ready.borrow().as_ref() {
            ready();
        }
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.inbound.borrow_mut().pop_front()
    }
}

enum LinkKind {
    Upstream(Option<Upstream>),
    Subscriber {
        id: Option<SubscriberId>,
        identity: AuthorSubject,
    },
}

/// One sync link, carried by the host as byte frames. The database's tick
/// applies received frames and produces outbound ones.
pub struct Link {
    binding: Binding,
    queues: WireQueues,
    kind: LinkKind,
}

impl Link {
    /// The binding this link belongs to.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    /// Frames received from the peer; applied on the next tick.
    pub fn receive(&self, frames: impl IntoIterator<Item = Vec<u8>>) {
        self.queues.inbound.borrow_mut().extend(frames);
        self.binding.hooks.schedule_tick();
    }

    /// Frames to send to the peer, oldest first.
    pub fn take_outbound(&self) -> Vec<Vec<u8>> {
        self.queues.outbound.borrow_mut().drain(..).collect()
    }

    /// How many frames are waiting to be sent.
    pub fn outbound_len(&self) -> usize {
        self.queues.outbound.borrow().len()
    }

    /// A count of the frames waiting to be sent that outlives this borrow,
    /// for a host that reports it after an async tick.
    pub fn outbound_counter(&self) -> impl Fn() -> usize + 'static {
        let outbound = Rc::clone(&self.queues.outbound);
        move || outbound.borrow().len()
    }

    /// Called whenever a frame is queued to send (`None` clears it).
    pub fn set_outbound_ready(&self, ready: Option<Box<dyn Fn()>>) {
        *self.queues.outbound_ready.borrow_mut() = ready;
    }

    /// Rebind a subscriber link's claims.
    pub fn update_claims(&self, claims: serde_json::Value) -> HostResult<()> {
        let LinkKind::Subscriber { identity, .. } = self.kind else {
            return Err("transport is not a subscriber link".into());
        };
        let claims = self::claims(identity, claims)?;
        self.binding.db.set_identity_claims(identity, claims);
        self.binding.hooks.progress.wake_all();
        Ok(())
    }

    /// Detach the link; `false` when already closed.
    pub fn close(&mut self) -> bool {
        match &mut self.kind {
            LinkKind::Upstream(upstream) => upstream
                .take()
                .is_some_and(|upstream| self.binding.db.detach_connection(&upstream)),
            LinkKind::Subscriber { id, .. } => match id.take() {
                Some(id) => {
                    self.binding.db.disconnect_subscriber(id);
                    true
                }
                None => false,
            },
        }
    }
}

//! Thin sessions (SPEC 8.14): connections that hold no replica. The owner
//! evaluates their queries under the session's identity and claims, sends
//! finished results, and runs their writes as that session.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use jazz::binding_codec;
use jazz::binding_host::{
    EventChunk, TransactionKind, decode_cells, decode_schema, delete_options, event_chunk,
    insert_options, open_transaction, read_opts, restore_options, update_options, upsert_options,
};
use jazz::db::{
    Db, Error, ErrorCode, LargeValueUpdate, SerializedReadResult, StreamingMutationKind,
    StreamingValueUpload, SubscriptionEvent, SubscriptionStream, UpdateOptions, WriteHandle,
    WriteIdentity,
};
use jazz::ids::{AuthorSubject, RowUuid, SchemaVersionId};
use jazz::model::transaction::OpenTransactionId;
use jazz::protocol::{BranchSelector, BranchViewBase};
use jazz::records::Value;
use jazz::thin::{ThinBody, ThinOp, ThinRequest, ThinResponse};
use jazz::wire::{WireErrorCode, WireFrame, WireRetry, encode_frame};
use tokio::sync::mpsc as tokio_mpsc;

use super::{Outbound, Owner, SESSION_QUEUE_BUDGET, ServerSession, ThinIngress};

/// A transaction a thin session began and has not ended.
struct ThinTransaction {
    serializable: bool,
    /// The first staged write refused for its row's state: the commit
    /// reports it instead of committing.
    refused: Option<Error>,
}

/// A large value a thin session is uploading in chunks.
struct ThinStream {
    upload: StreamingValueUpload,
    mutation: StreamingMutationKind,
    table: String,
    row: RowUuid,
    cells: BTreeMap<String, Value>,
    column: String,
    updated_at_ms: Option<u64>,
    head: Option<BranchSelector>,
    base: Option<BranchViewBase>,
    transaction: Option<OpenTransactionId>,
}

struct ThinState {
    db: Rc<Db>,
    identity: AuthorSubject,
    /// The identity's claims, bound to every read; `None` for SYSTEM.
    claims: Option<BTreeMap<String, Value>>,
    /// The published permissions bundle its backend pinned it to: its
    /// reads and writes are governed by that bundle's policies.
    bundle: Option<jazz::catalogue::BundleId>,
    /// The schema version the client declared.
    version: Option<SchemaVersionId>,
    /// Live queries, by the id of the request that opened them.
    subscriptions: BTreeMap<u64, SubscriptionStream>,
    /// Open transactions; they end with the session.
    transactions: BTreeMap<OpenTransactionId, ThinTransaction>,
    /// Large-value uploads, by the id of the request that began them.
    streams: BTreeMap<u64, ThinStream>,
    /// Answers produced while the session's egress budget is spent.
    held: Rc<RefCell<VecDeque<ThinResponse>>>,
    /// The live query the next pump drains first, so one busy query
    /// cannot keep the others' events waiting.
    next_subscription: u64,
    output: tokio_mpsc::UnboundedSender<Outbound>,
    egress: Arc<AtomicUsize>,
}

impl ThinState {
    fn new(
        db: Rc<Db>,
        identity: AuthorSubject,
        claims: Option<BTreeMap<String, Value>>,
        bundle: Option<jazz::catalogue::BundleId>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            db,
            identity,
            claims,
            bundle,
            version: None,
            subscriptions: BTreeMap::new(),
            transactions: BTreeMap::new(),
            streams: BTreeMap::new(),
            held: Rc::default(),
            next_subscription: 0,
            output,
            egress,
        }
    }

    fn has_room(&self) -> bool {
        self.egress.load(Ordering::Acquire) < SESSION_QUEUE_BUDGET
    }

    fn send_frames(&self, frames: Vec<Vec<u8>>) -> bool {
        send_frames(&self.output, &self.egress, frames)
    }
}

impl Drop for ThinState {
    fn drop(&mut self) {
        for id in self.transactions.keys() {
            let _ = self.db.abandon_transaction_handle(*id);
        }
        // StreamingValueUpload's lease schedules cleanup on the owning Db.
        // Dropping subscriptions also unregisters their pending reads.
    }
}

struct RequestWake {
    ready: AtomicBool,
    host: Waker,
}
impl Wake for RequestWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.ready.swap(true, Ordering::AcqRel) {
            self.host.wake_by_ref();
        }
    }
}

type RequestFuture = Pin<Box<dyn Future<Output = (ThinState, Option<ThinBody>)>>>;

pub(super) struct ThinSession {
    // A running request owns the state; cancellation drops it and its resources.
    task: Option<(u64, RequestFuture)>,
    state: Option<ThinState>,
    queued: VecDeque<(ThinRequest, ThinIngress)>,
    held: Rc<RefCell<VecDeque<ThinResponse>>>,
    egress: Arc<AtomicUsize>,
    wake: Arc<RequestWake>,
    // Admission/close metadata must be readable even while a request owns
    // the mutable state across await.
    bundle: Option<jazz::catalogue::BundleId>,
    output: tokio_mpsc::UnboundedSender<Outbound>,
}

impl ThinSession {
    fn ready(&self) -> bool {
        match self.task {
            Some(_) => self.wake.ready.load(Ordering::Acquire),
            None => !self.queued.is_empty(),
        }
    }

    fn poll_requests(&mut self) {
        // Bound synchronous work too: a session's queue cannot monopolize a tick.
        for _ in 0..super::INBOUND_MESSAGES_PER_TICK {
            if self.task.is_none() {
                let Some((request, ingress)) = self.queued.pop_front() else {
                    return;
                };
                let mut state = self.state.take().expect("idle session has state");
                let db = Rc::clone(&state.db);
                let id = request.id;
                self.task = Some((
                    id,
                    Box::pin(async move {
                        let _ingress = ingress;
                        let body = serve_thin(&db, &mut state, id, request.op).await;
                        (state, body)
                    }),
                ));
                self.wake.ready.store(true, Ordering::Release);
            }
            if !self.wake.ready.swap(false, Ordering::AcqRel) {
                return;
            }
            let waker = Waker::from(Arc::clone(&self.wake));
            let (id, future) = self.task.as_mut().expect("started above");
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Pending => return,
                Poll::Ready((state, body)) => {
                    let id = *id;
                    self.task = None;
                    if let Some(body) = body {
                        state.held.borrow_mut().push_back(ThinResponse { id, body });
                    }
                    self.state = Some(state);
                }
            }
        }
    }
}

fn error_body(error: &Error) -> ThinBody {
    ThinBody::Error {
        code: format!("{:?}", error.code),
        message: error.message.clone(),
    }
}

fn argument(message: impl Into<String>) -> ThinBody {
    ThinBody::Error {
        code: "Argument".to_owned(),
        message: message.into(),
    }
}

fn host_error(error: jazz::binding_host::HostError) -> ThinBody {
    match error {
        jazz::binding_host::HostError::Engine(error) => error_body(&error),
        jazz::binding_host::HostError::Argument(message) => argument(message),
    }
}

/// Options as the bindings take them; empty bytes are no options.
fn options_json(options: &[u8]) -> Result<serde_json::Value, ThinBody> {
    if options.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(options).map_err(|error| argument(format!("invalid options: {error}")))
}

/// Every thin write runs as the session: options may not name an author
/// or attribution of their own.
fn as_session(identity: &mut WriteIdentity, session: AuthorSubject) -> Result<(), ThinBody> {
    if *identity != WriteIdentity::Database {
        return Err(argument(
            "thin writes run as the session; options name no author or attribution",
        ));
    }
    *identity = WriteIdentity::Session(session);
    Ok(())
}

fn written(write: Result<WriteHandle, Error>) -> ThinBody {
    match write {
        Ok(write) => ThinBody::Written {
            row: *write.row_uuid().as_bytes(),
            seq: write.mergeable_tx_id(),
        },
        Err(error) => error_body(&error),
    }
}

/// A staged write's answer. A refusal decided by the row's stored state, or
/// a serializable conflict, does not fail the call: the transaction stays
/// open and its commit reports the first one (as for the bindings).
fn staged(
    transaction: &mut ThinTransaction,
    row: RowUuid,
    write: Result<WriteHandle, Error>,
) -> ThinBody {
    match write {
        Ok(_) => ThinBody::Staged {
            row: *row.as_bytes(),
        },
        Err(error)
            if error.is_apply_time_rejection() || error.code == ErrorCode::TransactionConflict =>
        {
            transaction.refused.get_or_insert(error);
            ThinBody::Staged {
                row: *row.as_bytes(),
            }
        }
        Err(error) => error_body(&error),
    }
}

fn streaming_mutation(mutation: &str) -> Result<StreamingMutationKind, ThinBody> {
    match mutation {
        "insert" => Ok(StreamingMutationKind::Insert),
        "update" => Ok(StreamingMutationKind::Update),
        "upsert" => Ok(StreamingMutationKind::Upsert),
        _ => Err(argument("unknown streaming mutation kind")),
    }
}

/// `updatedAtMs`, `head` and `base` of a streaming mutation.
type StreamOptions = (Option<u64>, Option<BranchSelector>, Option<BranchViewBase>);

/// `{ updatedAtMs?, head?, base? }` of a streaming mutation.
fn stream_options(options: &serde_json::Value) -> Result<StreamOptions, ThinBody> {
    let field = |name: &str| options.get(name).filter(|value| !value.is_null()).cloned();
    if field("author").is_some() || field("attribution").is_some() {
        return Err(argument(
            "thin writes run as the session; options name no author or attribution",
        ));
    }
    let updated_at_ms = match field("updatedAtMs") {
        Some(value) => Some(
            value
                .as_u64()
                .ok_or_else(|| argument("updatedAtMs must be a nonnegative integer"))?,
        ),
        None => None,
    };
    let head = field("head")
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| argument(format!("invalid head: {error}")))?;
    let base = field("base")
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| argument(format!("invalid base: {error}")))?;
    if base.is_some() && head.is_none() {
        return Err(argument("streaming mutation base requires a branch head"));
    }
    Ok((updated_at_ms, head, base))
}

impl Owner {
    pub fn open_thin_session(
        &mut self,
        session: ServerSession,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) -> Result<(), jazz::db::Error> {
        // Admitted on the same terms as a sync session: a pinned bundle must
        // resolve here, and a createJazz app serves only pinned sessions.
        self.db.check_session_pin(identity, bundle.as_ref())?;
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        let state = ThinState::new(
            Rc::clone(&self.db),
            identity,
            claims,
            bundle,
            output.clone(),
            Arc::clone(&egress),
        );
        self.thin.insert(
            session,
            ThinSession {
                held: Rc::clone(&state.held),
                egress,
                task: None,
                state: Some(state),
                queued: VecDeque::new(),
                wake: Arc::new(RequestWake {
                    ready: AtomicBool::new(false),
                    host: self.wake.clone(),
                }),
                bundle,
                output,
            },
        );
        Ok(())
    }

    /// End every thin session pinned to a bundle `ends` names, telling it
    /// `error` first. How many ended.
    pub(super) fn end_pinned_thin_sessions(
        &mut self,
        ends: &dyn Fn(&jazz::catalogue::BundleId) -> bool,
        error: &jazz::wire::WireError,
    ) -> usize {
        let ending: Vec<ServerSession> = self
            .thin
            .iter()
            .filter(|(_, thin)| thin.bundle.as_ref().is_some_and(ends))
            .map(|(id, _)| *id)
            .collect();
        for id in &ending {
            let _ = self.thin[id].output.send((Err(error.clone()), 0));
            self.close_thin_session(*id);
        }
        ending.len()
    }

    pub fn close_thin_session(&mut self, session: ServerSession) {
        // Dropping the streams ends the subscriptions.
        self.thin.remove(&session);
    }

    pub fn thin_request(
        &mut self,
        session: ServerSession,
        request: ThinRequest,
        ingress: ThinIngress,
    ) {
        if let Some(thin) = self.thin.get_mut(&session) {
            thin.queued.push_back((request, ingress));
            thin.poll_requests();
        }
    }

    pub(super) fn thin_pending(&self) -> bool {
        self.thin.values().any(ThinSession::ready)
    }

    pub(super) fn poll_thin(&mut self) {
        for thin in self.thin.values_mut() {
            // Some engine read/settlement futures are advanced by ticks and
            // intentionally do not register a waker. Poll once per host pass
            // as well as when external I/O explicitly wakes the request.
            thin.wake.ready.store(true, Ordering::Release);
            thin.poll_requests();
        }
    }

    /// After a tick: every thin session's held answers and its live
    /// queries' new events, up to its egress budget. What does not fit
    /// waits (answers in `held`, events in their streams). A drain only
    /// stops early once the encoded batch fills the budget, so the egress
    /// it leaves is over budget and the client draining its socket below
    /// it queues the next tick ([`super::ServerRuntimeFrameStream::recv`]).
    pub(super) fn pump_thin(&mut self) {
        let mut gone = Vec::new();
        for (session, session_state) in &mut self.thin {
            if session_state.egress.load(Ordering::Acquire) >= SESSION_QUEUE_BUDGET {
                continue;
            }
            if session_state.task.is_some() {
                let sent = drain_held(
                    &session_state.held,
                    &session_state.output,
                    &session_state.egress,
                )
                .is_some_and(|(frames, _)| {
                    send_frames(&session_state.output, &session_state.egress, frames)
                });
                if !sent {
                    gone.push(*session);
                }
                continue;
            }
            let thin = session_state
                .state
                .as_mut()
                .expect("idle session has state");
            if !thin.has_room() {
                continue;
            }
            let sent = drain_thin(thin).is_some_and(|frames| thin.send_frames(frames));
            if !sent {
                gone.push(*session);
            }
        }
        for session in gone {
            self.close_thin_session(session);
        }
    }
}

/// Serve one request; `None` when it has no immediate answer (a live
/// query answers from [`Owner::pump_thin`]).
async fn serve_thin(owner: &Db, thin: &mut ThinState, id: u64, op: ThinOp) -> Option<ThinBody> {
    if let ThinOp::Schema { schema } = op {
        // The version a replica built against this schema would declare.
        let version = match decode_schema(&schema) {
            Ok(schema) => schema.version_id(),
            Err(error) => return Some(host_error(error)),
        };
        return Some(match owner.pinned_view(version, thin.bundle.as_ref()) {
            Err(error) => error_body(&error),
            Ok(Some(_)) => {
                thin.version = Some(version);
                ThinBody::Ok
            }
            Ok(None) => ThinBody::Error {
                code: "SchemaNotDeployed".to_owned(),
                message: "the client's schema version is not deployed to this server".to_owned(),
            },
        });
    }
    if let ThinOp::Unsubscribe { subscription } = op {
        let removed = thin.subscriptions.remove(&subscription).is_some();
        // The live query's own id hears that it ended.
        if removed && subscription != id {
            thin.held.borrow_mut().push_back(ThinResponse {
                id: subscription,
                body: ThinBody::Closed,
            });
        }
        return Some(ThinBody::Ok);
    }
    let Some(version) = thin.version else {
        return Some(argument("a thin session declares its schema version first"));
    };
    // Resolved per request: an activation changes the policies that
    // govern the declared version (a pinned bundle's never change).
    let db = match owner.pinned_view(version, thin.bundle.as_ref()) {
        Ok(db) => db,
        Err(error) => return Some(error_body(&error)),
    };
    let Some(db) = db else {
        return Some(ThinBody::Error {
            code: "SchemaNotDeployed".to_owned(),
            message: "the client's schema version is no longer deployed".to_owned(),
        });
    };
    match serve_thin_op(&db, thin, id, op).await {
        Ok(body) => body,
        Err(body) => Some(body),
    }
}

fn drain_held(
    held: &RefCell<VecDeque<ThinResponse>>,
    output: &tokio_mpsc::UnboundedSender<Outbound>,
    egress: &AtomicUsize,
) -> Option<(Vec<Vec<u8>>, usize)> {
    let room = SESSION_QUEUE_BUDGET.saturating_sub(egress.load(Ordering::Acquire));
    let mut frames = Vec::new();
    let mut bytes = 0;
    while bytes < room {
        let Some(response) = held.borrow_mut().pop_front() else {
            break;
        };
        let (encoded, _) = encode_fitting(output, response)?;
        bytes += encoded.iter().map(Vec::len).sum::<usize>();
        frames.extend(encoded);
    }
    Some((frames, bytes))
}

fn send_frames(
    output: &tokio_mpsc::UnboundedSender<Outbound>,
    egress: &AtomicUsize,
    frames: Vec<Vec<u8>>,
) -> bool {
    if frames.is_empty() {
        return true;
    }
    let bytes = frames.iter().map(Vec::len).sum::<usize>();
    egress.fetch_add(bytes, Ordering::AcqRel);
    output.send((Ok(frames), bytes)).is_ok()
}

/// `thin`'s held answers, then its live queries' events in turn from
/// [`ThinState::next_subscription`], encoded until the batch fills the
/// session's egress budget. Answers that do not fit stay held. `None` once
/// the session was closed over a response it could not encode.
fn drain_thin(thin: &mut ThinState) -> Option<Vec<Vec<u8>>> {
    let room = SESSION_QUEUE_BUDGET.saturating_sub(thin.egress.load(Ordering::Acquire));
    let (mut frames, mut bytes) = drain_held(&thin.held, &thin.output, &thin.egress)?;
    if !thin.held.borrow().is_empty() {
        return Some(frames);
    }
    let cursor = thin.next_subscription;
    let order: Vec<u64> = thin
        .subscriptions
        .range(cursor..)
        .chain(thin.subscriptions.range(..cursor))
        .map(|(id, _)| *id)
        .collect();
    let mut ended = Vec::new();
    'streams: for id in order {
        let stream = thin.subscriptions.get_mut(&id).expect("listed above");
        while bytes < room {
            let Some(event) = stream.try_next_event() else {
                continue 'streams;
            };
            let (body, last) = event_body(event);
            let (encoded, refused) = encode_fitting(&thin.output, ThinResponse { id, body })?;
            // A refused event ends its query with the error; the session
            // and its other queries go on.
            let last = last || refused;
            if last {
                ended.push(id);
            }
            bytes += encoded.iter().map(Vec::len).sum::<usize>();
            frames.extend(encoded);
            if last {
                continue 'streams;
            }
        }
        // Full: the next drain starts after this query.
        thin.next_subscription = id + 1;
        break;
    }
    for id in ended {
        thin.subscriptions.remove(&id);
    }
    Some(frames)
}

/// `response` as the wire frames that carry it: one, or its
/// [`jazz::thin::ThinPart`]s when it is over a wire frame (a row holding a
/// value over 2 MiB); past [`jazz::thin::MAX_THIN_MESSAGE_BYTES`], an error
/// for its request in its place, and whether it was refused so. `None` as
/// for [`encode_response`].
fn encode_fitting(
    output: &tokio_mpsc::UnboundedSender<Outbound>,
    response: ThinResponse,
) -> Option<(Vec<Vec<u8>>, bool)> {
    let id = response.id;
    // A body already past the most one answer takes is refused before it is
    // encoded, which would copy it once more.
    let payload = match &response.body {
        ThinBody::Rows { rows, .. } => rows.len(),
        ThinBody::Event { delta, .. } => delta.len(),
        ThinBody::Snapshot { snapshot } => snapshot.len(),
        _ => 0,
    };
    let size = if payload > jazz::thin::MAX_THIN_MESSAGE_BYTES {
        payload
    } else {
        let frame = encode_response(output, response)?;
        match jazz::thin::thin_wire_frames(frame) {
            Ok(frames) => return Some((frames, false)),
            Err(size) => size,
        }
    };
    let refused = ThinResponse {
        id,
        body: ThinBody::Error {
            code: "TooLarge".to_owned(),
            message: format!(
                "this result is {size} bytes or more, over the {} a thin client takes in one \
                 answer: narrow or page the query, and read values that large another way \
                 (streaming them to a thin client is not supported yet)",
                jazz::thin::MAX_THIN_MESSAGE_BYTES
            ),
        },
    };
    encode_response(output, refused).map(|frame| (vec![frame], true))
}

/// `response` on the wire. On failure the session is told it closed, and
/// `None` is returned.
fn encode_response(
    output: &tokio_mpsc::UnboundedSender<Outbound>,
    response: ThinResponse,
) -> Option<Vec<u8>> {
    match encode_frame(&WireFrame::ThinResponse(response)) {
        Ok(frame) => Some(frame),
        Err(error) => {
            tracing::error!(%error, "encode thin response");
            let _ = output.send((
                Err(jazz::wire::WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("thin session closed: {error}"),
                )),
                0,
            ));
            None
        }
    }
}

/// A live query's event as a response body, and whether it ends the query.
fn event_body(event: SubscriptionEvent) -> (ThinBody, bool) {
    match event_chunk(event) {
        Ok(EventChunk::Delta {
            delta,
            reset,
            settled,
            tier,
        }) => (
            ThinBody::Event {
                delta,
                reset,
                settled,
                tier,
            },
            false,
        ),
        Ok(EventChunk::Closed) => (ThinBody::Closed, true),
        Ok(EventChunk::Rejected { detail }) => (
            ThinBody::Error {
                code: "Rejected".to_owned(),
                message: detail,
            },
            true,
        ),
        Err(error) => (argument(format!("{error:?}")), true),
    }
}

/// The open transaction `transaction` names, if this session began it.
fn session_transaction<'a>(
    thin: &'a mut ThinState,
    transaction: &str,
) -> Result<(OpenTransactionId, &'a mut ThinTransaction), ThinBody> {
    let id = open_transaction(transaction).map_err(host_error)?;
    match thin.transactions.get_mut(&id) {
        Some(open) => Ok((id, open)),
        None => Err(argument(format!(
            "transaction {transaction} is not open in this session"
        ))),
    }
}

/// One request against the session's governed view; `Ok(None)` when a live
/// query answers later.
async fn serve_thin_op(
    db: &Db,
    thin: &mut ThinState,
    id: u64,
    op: ThinOp,
) -> Result<Option<ThinBody>, ThinBody> {
    let identity = thin.identity;
    let scope = thin.claims.clone().map(|claims| (identity, claims));
    let body = match op {
        ThinOp::Schema { .. } | ThinOp::Unsubscribe { .. } => unreachable!("served by the owner"),
        ThinOp::Subscribe { query, options } => {
            if thin.subscriptions.contains_key(&id) {
                return Err(argument("a live query with this id is already open"));
            }
            let opts = read_opts(&options_json(&options)?).map_err(host_error)?;
            let query: jazz::query::Query = postcard::from_bytes(&query)
                .map_err(|error| argument(format!("undecodable query: {error}")))?;
            let opened = match db.prepare_query(&query) {
                Ok(mut prepared) => {
                    if let Some((author, claims)) = scope {
                        prepared = prepared.with_identity_claims(author, claims);
                    }
                    db.subscribe_for_identity(&prepared, opts, identity).await
                }
                Err(error) => Err(error),
            };
            match opened {
                Ok(stream) => {
                    thin.subscriptions.insert(id, stream);
                    return Ok(None);
                }
                Err(error) => error_body(&error),
            }
        }
        ThinOp::Query {
            query,
            options,
            transaction,
        } => {
            let opts = read_opts(&options_json(&options)?).map_err(host_error)?;
            let open = match transaction {
                Some(transaction) => Some(session_transaction(thin, &transaction)?.0),
                None => None,
            };
            // Even an authority may await offloaded chunks. Keep the owner
            // available to tick and finish that I/O.
            let read = db
                .all_serialized_query(&query, opts, open, scope, None)
                .await;
            let encoded = match read {
                Ok(SerializedReadResult::Rows(rows)) => {
                    binding_codec::encode_rows(&rows).map(|rows| (rows, false))
                }
                Ok(SerializedReadResult::Relation(snapshot)) => {
                    binding_codec::encode_relation_snapshot(&snapshot).map(|rows| (rows, true))
                }
                Err(error) => return Err(error_body(&error)),
            };
            match encoded {
                Ok((rows, relation)) => ThinBody::Rows { rows, relation },
                Err(error) => argument(format!("encode result: {error}")),
            }
        }
        ThinOp::Insert {
            table,
            cells,
            options,
            transaction,
        } => {
            let cells = decode_cells(&cells).map_err(host_error)?;
            let mut options = insert_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    let row = *options.row_id.get_or_insert_with(|| db.next_row_id());
                    staged(
                        state,
                        row,
                        db.tx_ref(open).insert(&table, cells, options).await,
                    )
                }
                None => written(db.insert(&table, cells, options).await),
            }
        }
        ThinOp::Update {
            table,
            row,
            patch,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let patch = decode_cells(&patch).map_err(host_error)?;
            let mut options = update_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        db.tx_ref(open).update(&table, row, patch, options).await,
                    )
                }
                None => written(db.update(&table, row, patch, options).await),
            }
        }
        ThinOp::Upsert {
            table,
            row,
            cells,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let mut options = upsert_options(&options_json(&options)?).map_err(host_error)?;
            let cells = decode_cells(&cells).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        db.tx_ref(open).upsert(&table, row, cells, options).await,
                    )
                }
                None => written(db.upsert(&table, row, cells, options).await),
            }
        }
        ThinOp::Delete {
            table,
            row,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let mut options = delete_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        db.tx_ref(open).delete(&table, row, options).await,
                    )
                }
                None => written(db.delete(&table, row, options).await),
            }
        }
        ThinOp::Restore {
            table,
            row,
            cells,
            options,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let cells = cells
                .map(|cells| decode_cells(&cells))
                .transpose()
                .map_err(host_error)?;
            let mut options = restore_options(&options_json(&options)?).map_err(host_error)?;
            as_session(&mut options.identity, identity)?;
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        db.tx_ref(open).restore(&table, row, cells, options).await,
                    )
                }
                None => written(db.restore(&table, row, cells, options).await),
            }
        }
        ThinOp::UpdateLargeValues {
            table,
            row,
            patch,
            mutations,
            updated_at_ms,
            transaction,
        } => {
            let row = RowUuid::from_bytes(row);
            let patch = decode_cells(&patch).map_err(host_error)?;
            let mutations: Vec<LargeValueUpdate> =
                serde_json::from_slice(&mutations).map_err(|error| {
                    argument(format!("invalid partial-value update descriptor: {error}"))
                })?;
            let options = UpdateOptions {
                identity: WriteIdentity::Session(identity),
                updated_at_ms,
                ..UpdateOptions::default()
            };
            match transaction {
                Some(transaction) => {
                    let (open, state) = session_transaction(thin, &transaction)?;
                    staged(
                        state,
                        row,
                        db.tx_ref(open)
                            .update_with_large_value_mutations(
                                &table, row, patch, mutations, options,
                            )
                            .await,
                    )
                }
                None => written(
                    db.update_with_large_value_mutations(&table, row, patch, mutations, options)
                        .await,
                ),
            }
        }
        ThinOp::Begin { transaction, kind } => {
            let open = open_transaction(&transaction).map_err(host_error)?;
            let kind = TransactionKind::parse(&kind).map_err(host_error)?;
            let serializable = matches!(kind, TransactionKind::Serializable);
            let begun = if serializable {
                db.begin_serializable(open).await
            } else {
                db.begin_mergeable(open).await
            };
            match begun {
                Ok(()) => {
                    thin.transactions.insert(
                        open,
                        ThinTransaction {
                            serializable,
                            refused: None,
                        },
                    );
                    ThinBody::Ok
                }
                Err(error) => error_body(&error),
            }
        }
        ThinOp::Commit { transaction } => {
            let open = session_transaction(thin, &transaction)?.0;
            let state = thin.transactions.get(&open).expect("checked above");
            if let Some(error) = state.refused.clone() {
                let _ = db.abandon_transaction_handle(open);
                thin.transactions.remove(&open);
                return Err(error_body(&error));
            }
            let receipt = if state.serializable {
                db.commit_serializable_handle(open).await
            } else {
                db.commit_mergeable_handle(open).await
            };
            thin.transactions.remove(&open);
            match receipt {
                Ok(receipt) => ThinBody::Written {
                    row: [0; 16],
                    seq: receipt.0,
                },
                Err(error) => error_body(&error),
            }
        }
        ThinOp::Rollback { transaction } => {
            let open = session_transaction(thin, &transaction)?.0;
            thin.transactions.remove(&open);
            match db.abandon_transaction_handle(open) {
                Ok(()) => ThinBody::Ok,
                Err(error) => error_body(&error),
            }
        }
        ThinOp::CaptureSnapshot => match db.capture_snapshot().await {
            Ok(snapshot) => ThinBody::Snapshot {
                snapshot: snapshot.to_persisted().into_bytes(),
            },
            Err(error) => error_body(&error),
        },
        ThinOp::BeginStream {
            table,
            row,
            cells,
            column,
            mutation,
            options,
            transaction,
        } => {
            if thin.streams.contains_key(&id) {
                return Err(argument("an upload with this id is already open"));
            }
            let transaction = match transaction {
                Some(transaction) => Some(session_transaction(thin, &transaction)?.0),
                None => None,
            };
            let mutation = streaming_mutation(&mutation)?;
            let (updated_at_ms, head, base) = stream_options(&options_json(&options)?)?;
            let cells = decode_cells(&cells).map_err(host_error)?;
            match db.begin_streaming_value_upload(&table, &cells, &column) {
                Ok(upload) => {
                    thin.streams.insert(
                        id,
                        ThinStream {
                            upload,
                            mutation,
                            table,
                            row: RowUuid::from_bytes(row),
                            cells,
                            column,
                            updated_at_ms,
                            head,
                            base,
                            transaction,
                        },
                    );
                    ThinBody::Ok
                }
                Err(error) => error_body(&error),
            }
        }
        ThinOp::StreamChunk { stream, chunk } => {
            let Some(open) = thin.streams.get_mut(&stream) else {
                return Err(argument("no upload with this id is open"));
            };
            match db
                .push_streaming_value_upload(&mut open.upload, &chunk)
                .await
            {
                Ok(()) => ThinBody::Ok,
                Err(error) => {
                    // A failed push ends the upload.
                    let open = thin.streams.remove(&stream).expect("checked above");
                    let _ = db.abort_streaming_value_upload(open.upload).await;
                    error_body(&error)
                }
            }
        }
        ThinOp::FinishStream { stream } => {
            let Some(open) = thin.streams.remove(&stream) else {
                return Err(argument("no upload with this id is open"));
            };
            if let Some(transaction) = open.transaction {
                let Some(state) = thin.transactions.get_mut(&transaction) else {
                    return Err(argument("the upload's transaction is no longer open"));
                };
                let write = db
                    .tx_ref(transaction)
                    .finish_streaming_value_upload(
                        open.upload,
                        open.mutation,
                        &open.table,
                        open.row,
                        open.cells,
                        &open.column,
                        WriteIdentity::Session(identity),
                        open.updated_at_ms,
                        open.head,
                        open.base,
                    )
                    .await;
                return Ok(Some(staged(state, open.row, write)));
            }
            written(
                db.finish_streaming_value_upload(
                    open.upload,
                    open.mutation,
                    &open.table,
                    open.row,
                    open.cells,
                    &open.column,
                    WriteIdentity::Session(identity),
                    open.updated_at_ms,
                    open.head,
                    open.base,
                )
                .await,
            )
        }
        ThinOp::AbortStream { stream } => {
            let Some(open) = thin.streams.remove(&stream) else {
                return Err(argument("no upload with this id is open"));
            };
            match db.abort_streaming_value_upload(open.upload).await {
                Ok(()) => ThinBody::Ok,
                Err(error) => error_body(&error),
            }
        }
    };
    Ok(Some(body))
}

#[cfg(all(test, feature = "native"))]
mod tests;

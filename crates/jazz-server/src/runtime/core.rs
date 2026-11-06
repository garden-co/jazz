//! Thread-affine server state, independent of the native store scheduler.
use super::thin;
use jazz::catalogue::{CatalogueChange, CatalogueRows};
use jazz::db::{Db, LensEffects, SubscriberId, SyncMessage, Transport, TransportError};
use jazz::ids::{AuthorSubject, SchemaVersionId};
use jazz::protocol::MigrationLens;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::wire::{WireErrorCode, WireRetry};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;
use web_time::Instant;

/// Encoded frames exchanged with a host transport.
pub type AbiBytes = Vec<u8>;

/// Opaque handle for one admitted session.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServerSession(pub(super) u64);

impl ServerSession {
    /// A host-assigned id. Do not reuse it while an old input handle is live.
    pub fn from_host_id(id: u64) -> Self {
        Self(id)
    }
}

/// Bytes a session may have queued in each direction. Inbound frames past
/// it are refused (the peer ignored flow control); outbound messages past it
/// wait in the engine until the socket catches up.
pub(crate) const SESSION_QUEUE_BUDGET: usize = 8 * 1024 * 1024;
/// Bytes of split thin requests (each over a wire frame) the whole server
/// may hold, being joined from parts or queued for the owner. A split past
/// it waits for room before its parts are read, so many large writes at
/// once slow down instead of growing memory without bound. Requests that
/// fit one frame are bounded by their session's room alone, so they never
/// queue behind large writes of other sessions.
pub const THIN_INGRESS_SERVER_BUDGET: usize = 256 * 1024 * 1024;
/// Inbound messages one session may have applied per round of ticks.
pub(super) const INBOUND_MESSAGES_PER_TICK: usize = 16;

/// About how long one tick may take. Each tick serves as many sessions as
/// the recent per-session cost fits in it (at least one). A round serves
/// every session with inbound messages once, over as many ticks as that
/// takes, before any is served again: a session waits at most a round, and
/// host work queued meanwhile (catalogue writes, registrations) runs
/// between the ticks. A served session's queued messages (up to
/// [`INBOUND_MESSAGES_PER_TICK`]) are taken in one tick: clients rely on a
/// read sent just before their writes being answered after them. Smaller
/// ticks cost no throughput: each message commits on its own either way.
pub(super) const TICK_SLICE: Duration = Duration::from_millis(4);

/// Room for one thin request, in its session's and the server's ingress
/// budgets, held from before its bytes are buffered until the owner has
/// run it.
pub struct ThinIngress {
    pub(super) _session: tokio::sync::OwnedSemaphorePermit,
    pub(super) _server: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl ThinIngress {
    /// Keep the host's admission budget charged until this request ends.
    pub fn from_permits(
        session: tokio::sync::OwnedSemaphorePermit,
        server: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            _session: session,
            _server: server,
        }
    }
}

/// A session's next frame batch, or the error frame that ends it.
pub(super) type Outbound = (Result<Vec<AbiBytes>, jazz::wire::WireError>, usize);

pub(super) fn session_closed_error(error: &jazz::db::Error) -> jazz::wire::WireError {
    let (code, retry) = match error.code {
        jazz::db::ErrorCode::Backpressure | jazz::db::ErrorCode::Storage => {
            (WireErrorCode::Backpressure, WireRetry::Later)
        }
        _ => (WireErrorCode::Internal, WireRetry::Never),
    };
    jazz::wire::WireError::new(code, retry, format!("sync session closed: {error}"))
}

/// Why a catalogue change was not applied. Either way nothing of it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogueApplyError {
    /// The change does not fit this catalogue: an activation, or a
    /// schema or lens the store refuses to admit.
    Rejected(String),
    /// Storage or the runtime failed.
    Failed(String),
}

impl std::fmt::Display for CatalogueApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) | Self::Failed(message) => f.write_str(message),
        }
    }
}

impl From<CatalogueApplyError> for String {
    fn from(error: CatalogueApplyError) -> Self {
        error.to_string()
    }
}

// ---- session owner ------------------------------------------------------------

pub struct Owner {
    pub(super) db: Rc<Db>,
    pub(super) wake: std::task::Waker,
    pub(super) input_requested: Rc<std::cell::Cell<bool>>,
    pub(super) sessions: HashMap<ServerSession, OwnerSession>,
    /// Thin client connections (SPEC 8.14), which are not sync sessions.
    pub(super) thin: HashMap<ServerSession, thin::ThinSession>,
    /// The tick in progress's time slice, shared with every session's queues.
    pub(super) slice: Rc<TickSlice>,
    pub(super) catalogue: Catalogue,
    /// The app this store serves, named in slow-query reports.
    pub(super) app: Option<String>,
    /// Tests: the next catalogue reload after a commit fails.
    #[cfg(test)]
    pub(super) fail_next_reload: bool,
}

pub(super) struct OwnerSession {
    pub(super) input: SessionInput,
    pub(super) subscriber: SubscriberId,
    /// The published bundle its backend pinned it to, if any.
    pub(super) bundle: Option<jazz::catalogue::BundleId>,
    /// The parts of a message split across frames, until its last part.
    pub(super) joiner: jazz::db::FrameJoiner,
    pub(super) queues: Rc<SessionQueues>,
    pub(super) output: tokio_mpsc::UnboundedSender<Outbound>,
    pub(super) egress: Arc<AtomicUsize>,
}

/// A host can retain this handle while `Owner::advance` borrows the owner.
/// It only queues input; decoding and database access stay with the owner.
#[derive(Clone)]
pub struct SessionInput {
    pub(super) inbox: Rc<RefCell<SessionInbox>>,
    pub(super) requested: Rc<std::cell::Cell<bool>>,
}

#[derive(Default)]
pub(super) struct SessionInbox {
    pub(super) frames: VecDeque<AbiBytes>,
    pub(super) bytes: usize,
    pub(super) closed: bool,
}

impl SessionInput {
    pub fn receive(&self, frames: Vec<AbiBytes>) -> Result<(), String> {
        let mut inbox = self.inbox.borrow_mut();
        if inbox.closed {
            return Err("session is closed".to_owned());
        }
        let bytes = frames
            .iter()
            .try_fold(0usize, |sum, frame| sum.checked_add(frame.len()));
        let Some(bytes) =
            bytes.filter(|bytes| *bytes <= SESSION_QUEUE_BUDGET.saturating_sub(inbox.bytes))
        else {
            return Err("session ingress budget exceeded".to_owned());
        };
        // Empty frames count too: their allocation must not evade the byte cap.
        if frames.len()
            > jazz::protocol_limits::MAX_WIRE_BATCH_FRAMES.saturating_sub(inbox.frames.len())
        {
            return Err("session ingress frame budget exceeded".to_owned());
        }
        if !frames.is_empty() {
            inbox.bytes += bytes;
            inbox.frames.extend(frames);
            self.requested.set(true);
        }
        Ok(())
    }

    pub fn close(&self) {
        let mut inbox = self.inbox.borrow_mut();
        inbox.closed = true;
        inbox.frames.clear();
        inbox.bytes = 0;
        self.requested.set(true);
    }
}

pub(super) struct SessionQueues {
    pub(super) inbound: RefCell<VecDeque<SyncMessage>>,
    /// Output the socket has not taken yet, encoded as it is queued so its
    /// size is known, with its bytes.
    pub(super) outbound: RefCell<VecDeque<(Vec<AbiBytes>, usize)>>,
    /// What the session's output holds in memory (see [`Backlog`]).
    pub(super) backlog: Backlog,
    /// Inbound messages the engine may still take in the tick serving this
    /// session.
    pub(super) budget: std::cell::Cell<usize>,
    /// The tick in progress serves this session.
    pub(super) served: std::cell::Cell<bool>,
    /// A tick of this round served this session already.
    pub(super) done: std::cell::Cell<bool>,
    pub(super) slice: Rc<TickSlice>,
}

/// How many sessions the tick in progress takes inbound messages from,
/// sized to [`TICK_SLICE`].
pub(super) struct TickSlice {
    /// Sessions this tick may serve: at least one, so every tick makes
    /// progress.
    pub(super) cap: std::cell::Cell<usize>,
    pub(super) taken: std::cell::Cell<usize>,
    /// Recent cost of a tick per session it served (a moving average).
    pub(super) per_session: std::cell::Cell<Option<Duration>>,
}

impl Default for TickSlice {
    fn default() -> Self {
        Self {
            cap: std::cell::Cell::new(usize::MAX),
            taken: std::cell::Cell::new(0),
            per_session: std::cell::Cell::new(None),
        }
    }
}

impl TickSlice {
    pub(super) fn take(&self) -> bool {
        let open = self.taken.get() < self.cap.get();
        if open {
            self.taken.set(self.taken.get() + 1);
        }
        open
    }

    /// Learn from a tick that took `elapsed`, and size the next one.
    pub(super) fn ticked(&self, elapsed: Duration) {
        let taken = self.taken.replace(0);
        if taken == 0 {
            return;
        }
        let sample = elapsed / taken as u32;
        let average = match self.per_session.get() {
            Some(average) => (average * 3 + sample) / 4,
            None => sample,
        };
        self.per_session.set(Some(average));
        let fits = TICK_SLICE.as_nanos() / average.as_nanos().max(1);
        self.cap
            .set(usize::try_from(fits).unwrap_or(usize::MAX).max(1));
    }
}

/// Before a tick: a round ends once every session with inbound messages
/// was served in it (a time slice can end a tick mid-round); until then,
/// only sessions it did not serve yet are served.
pub(super) fn start_tick<'a>(sessions: impl Iterator<Item = &'a SessionQueues> + Clone) {
    let round_over = sessions
        .clone()
        .all(|queues| queues.done.get() || queues.inbound.borrow().is_empty());
    for queues in sessions {
        if round_over {
            queues.done.set(false);
        }
        queues.served.set(false);
        queues.backlog.start_tick();
    }
}

/// The engine's side of one session.
pub(super) struct SessionTransport(pub(super) Rc<SessionQueues>);

impl SessionQueues {
    pub(super) fn new(slice: Rc<TickSlice>, egress: Arc<AtomicUsize>) -> Self {
        Self {
            inbound: RefCell::default(),
            outbound: RefCell::default(),
            backlog: Backlog::new(egress),
            budget: std::cell::Cell::new(0),
            served: std::cell::Cell::new(false),
            done: std::cell::Cell::new(false),
            slice,
        }
    }

    /// Queue `message` for the socket, encoded, counting it in the backlog.
    pub(super) fn queue(&self, message: SyncMessage) -> Result<(), TransportError> {
        let (frames, bytes) = encode_outbound(message)?;
        self.backlog.queued(bytes);
        self.outbound.borrow_mut().push_back((frames, bytes));
        Ok(())
    }
}

pub(super) fn encode_outbound(
    message: SyncMessage,
) -> Result<(Vec<AbiBytes>, usize), TransportError> {
    let frames = jazz::db::encode_frames(message).map_err(|error| {
        tracing::error!(%error, "encode outbound sync message");
        TransportError::Failed(format!("encode outbound sync message: {error}"))
    })?;
    let bytes = frames.iter().map(Vec::len).sum();
    Ok((frames, bytes))
}

/// Undrained output a session may hold beyond its biggest tick's: a reader
/// this far behind is closed with `Backpressure` (the client reconnects
/// later) instead of holding server memory without bound.
pub(crate) const SESSION_BACKLOG_FLOOR: usize = 64 * 1024 * 1024;

/// How much of a session's output sits in server memory: queued in the
/// engine and handed to the socket but not yet read off it.
///
/// One tick's output for a session is always taken whole, however large
/// (a first result is as big as what the client asked for). Output of a
/// later tick is refused, and the session closed, once what earlier ticks
/// left undrained exceeds twice the session's biggest tick, and at least
/// [`SESSION_BACKLOG_FLOOR`]: a reader that keeps up drains a burst before
/// the next one doubles it, and one that does not is bounded by about
/// that much.
pub(super) struct Backlog {
    /// Bytes the socket took and the client has not read yet.
    pub(super) egress: Arc<AtomicUsize>,
    /// Bytes queued in the engine, not yet handed to the socket.
    pub(super) queued: std::cell::Cell<usize>,
    /// Undrained bytes (queued and egress) when this tick started.
    pub(super) carried: std::cell::Cell<usize>,
    /// Bytes queued in this tick.
    pub(super) burst: std::cell::Cell<usize>,
    /// The most bytes one earlier tick queued.
    pub(super) peak: std::cell::Cell<usize>,
}

impl Backlog {
    pub(super) fn new(egress: Arc<AtomicUsize>) -> Self {
        Self {
            egress,
            queued: std::cell::Cell::new(0),
            carried: std::cell::Cell::new(0),
            burst: std::cell::Cell::new(0),
            peak: std::cell::Cell::new(0),
        }
    }

    pub(super) fn undrained(&self) -> usize {
        self.queued
            .get()
            .saturating_add(self.egress.load(Ordering::Acquire))
    }

    /// A tick starts: what is undrained now was left by earlier ones.
    pub(super) fn start_tick(&self) {
        self.peak.set(self.peak.get().max(self.burst.replace(0)));
        self.carried.set(self.undrained());
    }

    /// Whether this tick may queue more for the session.
    pub(super) fn admits(&self) -> bool {
        let allowed = SESSION_BACKLOG_FLOOR.max(self.peak.get().saturating_mul(2));
        self.carried.get() <= allowed
    }

    pub(super) fn queued(&self, bytes: usize) {
        self.queued.set(self.queued.get() + bytes);
        self.burst.set(self.burst.get() + bytes);
    }

    /// `bytes` left the engine for the socket.
    pub(super) fn handed_on(&self, bytes: usize) {
        self.queued.set(self.queued.get().saturating_sub(bytes));
    }
}

impl Transport for SessionTransport {
    /// `Backpressure` closes the session (the engine drops a backed-up
    /// subscriber, and the host tells the client to come back later).
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if !self.0.backlog.admits() {
            return Err(TransportError::Backpressure);
        }
        self.0.queue(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let queues = &self.0;
        if queues.inbound.borrow().is_empty() {
            return None;
        }
        if !queues.served.get() {
            if queues.done.get() || !queues.slice.take() {
                return None;
            }
            queues.served.set(true);
            queues.done.set(true);
            queues.budget.set(INBOUND_MESSAGES_PER_TICK);
        }
        let budget = queues.budget.get();
        if budget == 0 {
            return None;
        }
        let message = queues.inbound.borrow_mut().pop_front()?;
        queues.budget.set(budget - 1);
        Some(message)
    }
}

/// What the store's catalogue admitted, read from its rows (see
/// [`jazz::catalogue`]) after every change.
#[derive(Default)]
pub(super) struct Catalogue {
    pub(super) schemas: BTreeMap<SchemaVersionId, JazzSchema>,
    /// Every admitted lens, keyed by target schema.
    pub(super) incoming: BTreeMap<SchemaVersionId, Vec<MigrationLens>>,
}

impl Catalogue {
    pub(super) fn load(rows: &CatalogueRows) -> Result<Self, String> {
        let compile = |schema: &jazz::tools::public_schema::Schema| {
            JazzSchema::new(schema).map_err(|error| format!("catalogue schema: {error}"))
        };
        let mut catalogue = Self::default();
        for schema in &rows.schemas {
            if let Some(version) = schema.version {
                catalogue.schemas.insert(version, compile(&schema.schema)?);
            }
        }
        for lens in &rows.lenses {
            let (Some(source), Some(target)) =
                (rows.schema(&lens.source), rows.schema(&lens.target))
            else {
                continue;
            };
            let target_version = compile(&target.schema)?.version_id();
            if compile(&source.schema)?.version_id() == target_version
                || !catalogue.schemas.contains_key(&target_version)
            {
                continue;
            }
            let lens = jazz::tools::schema_lens::runtime::compile_lens(
                &jazz::tools::schema_lens::Lens::new(
                    lens.source,
                    lens.target,
                    lens.forward.clone(),
                ),
                &source.schema,
                &target.schema,
            )
            .map_err(|error| format!("catalogue lens: {error}"))?;
            catalogue.admit_lens(target_version, lens);
        }
        Ok(catalogue)
    }

    pub(super) fn admit_lens(&mut self, target: SchemaVersionId, lens: MigrationLens) {
        let incoming = self.incoming.entry(target).or_default();
        if !incoming.iter().any(|admitted| admitted.id == lens.id) {
            incoming.push(lens);
        }
    }

    pub(super) fn lenses_for(&self, schema: SchemaVersionId) -> Vec<MigrationLens> {
        self.incoming.get(&schema).cloned().unwrap_or_default()
    }
}

impl Owner {
    pub async fn publish_schema_with_lens(
        &mut self,
        schema: JazzSchema,
        lens: MigrationLens,
    ) -> Result<SchemaVersionId, String> {
        LensEffects::of(&lens).map_err(|error| error.to_string())?;
        if !self.catalogue.schemas.contains_key(&lens.source) {
            return Err("lens source schema is not admitted".to_owned());
        }
        let id = schema.version_id();
        let mut lenses = self.catalogue.lenses_for(id);
        if !lenses.iter().any(|admitted| admitted.id == lens.id) {
            lenses.push(lens.clone());
        }
        // Clients on the published version read and write by it before
        // (and without) it becoming current.
        self.db
            .admit_schema_with_lenses(schema, &lenses)
            .await
            .map_err(|error| error.to_string())?;
        self.reload_catalogue()?;
        self.catalogue.admit_lens(id, lens);
        Ok(id)
    }

    /// Apply a catalogue change, reporting closed sessions even if reload fails.
    pub async fn apply_catalogue(
        &mut self,
        change: CatalogueChange,
        supersedes: Option<jazz::catalogue::BundleId>,
    ) -> (usize, Result<(), CatalogueApplyError>) {
        let mut ended = 0;
        let result = async {
            if change.activate.is_some() {
                return Err(CatalogueApplyError::Rejected(
                    "a server never makes a schema current: sessions pin bundles".to_owned(),
                ));
            }
            self.db.apply_catalogue(change).await.map_err(|error| {
                if error.code == jazz::db::ErrorCode::Storage {
                    CatalogueApplyError::Failed(error.to_string())
                } else {
                    CatalogueApplyError::Rejected(error.to_string())
                }
            })?;
            if let Some(published) = supersedes {
                ended = self.end_pinned_sessions(
                    &|bundle| *bundle != published,
                    &superseded_error(&published),
                );
                if ended > 0 {
                    tracing::info!(
                        bundle = %published,
                        ended,
                        "new permissions bundle: sessions pinned to others reconnect"
                    );
                }
            }
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_reload) {
                // As a reload that failed partway: the runtime's
                // view of the catalogue is no longer the store's.
                self.catalogue = Catalogue::default();
                return Err(CatalogueApplyError::Failed(
                    "injected catalogue reload failure".to_owned(),
                ));
            }
            self.reload_catalogue()
                .map_err(CatalogueApplyError::Failed)?;
            Ok(())
        }
        .await;
        (ended, result)
    }

    /// Attach server session state to an already-open database. The host
    /// owns storage construction and execution of the returned tick future.
    pub fn from_db(db: Db, app: Option<String>, wake: std::task::Waker) -> Result<Self, String> {
        db.set_authority();
        db.require_session_pins();
        db.track_closed_subscribers();

        let catalogue = Catalogue::load(&db.catalogue().map_err(|error| error.to_string())?)?;
        Ok(Self {
            db: Rc::new(db),
            wake,
            input_requested: Rc::default(),
            sessions: HashMap::new(),
            thin: HashMap::new(),
            slice: Rc::default(),
            catalogue,
            app,
            #[cfg(test)]
            fail_next_reload: false,
        })
    }

    pub fn reload_catalogue(&mut self) -> Result<(), String> {
        self.catalogue = Catalogue::load(&self.db.catalogue().map_err(|error| error.to_string())?)?;
        Ok(())
    }

    /// Log the query shapes that ran slow since the last report, when one
    /// is due. Each one held the store's only writer for its whole run,
    /// delaying every session, so the line says how long, what SQLite did,
    /// and how to make it fast.
    /// Text merges that ran past their work budget: each write stood as
    /// sent, so a concurrent edit left the current text (history keeps it).
    pub(super) fn report_merge_fallbacks(&self) {
        let (fallbacks, dropped) = self.db.take_merge_fallbacks();
        let app = self.app.as_deref().unwrap_or_default();
        for fallback in &fallbacks {
            tracing::warn!(
                app,
                table = %fallback.table,
                row = %fallback.row.0,
                column = %fallback.column,
                bytes = fallback.bytes,
                "text merge ran past its work budget in app {app}: the write stood as sent, \
                 and a concurrent edit to {}.{} left the current text (history keeps it)",
                fallback.table,
                fallback.column,
            );
        }
        if dropped > 0 {
            tracing::warn!(
                app,
                dropped,
                "{dropped} more text merges in app {app} ran past their work budget"
            );
        }
    }

    pub(super) fn report_slow_queries(&self) {
        let Some((shapes, dropped)) = self.db.take_due_slow_queries() else {
            return;
        };
        let app = self.app.as_deref().unwrap_or_default();
        for shape in &shapes {
            tracing::warn!(
                app,
                table = %shape.table,
                runs = shape.runs,
                total_ms = shape.total.as_millis() as u64,
                slowest_ms = shape.slowest.as_millis() as u64,
                rows = shape.rows,
                scanned = shape.scanned,
                sorted = shape.sorted,
                plan = %shape.plan,
                sql = %shape.sql,
                hint = %shape.hint().unwrap_or_default(),
                "{}",
                shape.line(self.app.as_deref()),
            );
        }
        if dropped > 0 {
            tracing::warn!(app, dropped, "more slow query shapes than one report holds");
        }
    }

    /// Close every session (their streams end) and the store's runtime.
    pub fn close(mut self) {
        for (_, session) in self.sessions.drain() {
            session.input.close();
            self.db.disconnect_subscriber(session.subscriber);
        }
        self.thin.clear();
    }

    pub fn open_session(
        &mut self,
        session: ServerSession,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) -> Result<(), jazz::db::Error> {
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        let queues = Rc::new(SessionQueues::new(
            Rc::clone(&self.slice),
            Arc::clone(&egress),
        ));
        let subscriber = self.db.accept_pinned_subscriber(
            Box::new(SessionTransport(Rc::clone(&queues))),
            identity,
            claims,
            bundle,
        )?;
        self.sessions.insert(
            session,
            OwnerSession {
                input: SessionInput {
                    inbox: Rc::default(),
                    requested: Rc::clone(&self.input_requested),
                },
                subscriber,
                bundle,
                // An untrusted peer's split message must fit the budget
                // its frames are admitted against (SPEC 8.1).
                joiner: if identity != AuthorSubject::SYSTEM {
                    jazz::db::FrameJoiner::with_ceiling(
                        jazz::protocol_limits::MAX_UNTRUSTED_MESSAGE_BYTES,
                    )
                } else {
                    jazz::db::FrameJoiner::default()
                },
                queues,
                output,
                egress,
            },
        );
        Ok(())
    }

    /// Retain before starting a tick; event handlers can then enqueue without
    /// borrowing the owner. The host schedules a tick on input and checks
    /// `inbound_pending` after flushing to service arrivals during an await.
    pub fn input(&self, session: ServerSession) -> Option<SessionInput> {
        self.sessions
            .get(&session)
            .map(|session| session.input.clone())
    }

    pub fn receive(&mut self, session: ServerSession, frames: Vec<AbiBytes>) {
        let Some(input) = self.input(session) else {
            return;
        };
        if let Err(message) = input.receive(frames) {
            if let Some(session) = self.sessions.get(&session) {
                let _ = session.output.send((
                    Err(jazz::wire::WireError::new(
                        WireErrorCode::Backpressure,
                        WireRetry::Later,
                        message,
                    )),
                    0,
                ));
            }
            self.close_session(session);
            return;
        }
        self.drain_input(session);
    }

    pub(super) fn drain_input(&mut self, session: ServerSession) {
        let Some(input) = self.input(session) else {
            return;
        };
        let (closed, frames) = {
            let mut inbox = input.inbox.borrow_mut();
            inbox.bytes = 0;
            (inbox.closed, std::mem::take(&mut inbox.frames))
        };
        if closed {
            self.close_session(session);
            return;
        }
        let Some(owner_session) = self.sessions.get_mut(&session) else {
            return;
        };
        let mut inbound = owner_session.queues.inbound.borrow_mut();
        for frame in frames {
            match owner_session.joiner.push(&frame) {
                Ok(None) => {}
                Ok(Some(message)) => inbound.push_back(message),
                Err(error) => {
                    drop(inbound);
                    let _ = owner_session.output.send((
                        Err(jazz::wire::WireError::new(
                            WireErrorCode::MalformedFrame,
                            WireRetry::Never,
                            error.to_string(),
                        )),
                        0,
                    ));
                    self.close_session(session);
                    return;
                }
            }
        }
    }

    /// End every session (sync or thin) pinned to a bundle `ends` names,
    /// telling each peer `error` first. Unpinned sessions (the system's,
    /// an admin's) stay. How many ended.
    pub(super) fn end_pinned_sessions(
        &mut self,
        ends: &dyn Fn(&jazz::catalogue::BundleId) -> bool,
        error: &jazz::wire::WireError,
    ) -> usize {
        let ending: Vec<ServerSession> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.bundle.as_ref().is_some_and(ends))
            .map(|(id, _)| *id)
            .collect();
        let mut ended = ending.len();
        for id in ending {
            let _ = self.sessions[&id].output.send((Err(error.clone()), 0));
            self.close_session(id);
        }
        ended += self.end_pinned_thin_sessions(ends, error);
        ended
    }

    pub fn close_session(&mut self, session: ServerSession) {
        if let Some(session) = self.sessions.remove(&session) {
            session.input.close();
            self.db.disconnect_subscriber(session.subscriber);
        }
        self.close_thin_session(session);
    }

    pub fn inbound_pending(&self) -> bool {
        self.input_requested.get()
            || self.thin_pending()
            || self
                .sessions
                .values()
                .any(|session| !session.queues.inbound.borrow().is_empty())
    }

    /// Advance the database without choosing an executor or starting I/O.
    /// The exclusive borrow prevents overlapping ticks; input handles remain
    /// usable during suspension. Complete this future before publishing output.
    pub async fn advance(&mut self) {
        self.poll_thin();
        self.input_requested.set(false);
        let sessions: Vec<_> = self.sessions.keys().copied().collect();
        for session in sessions {
            self.drain_input(session);
        }
        start_tick(self.sessions.values().map(|session| &*session.queues));
        let started = Instant::now();
        let ticked = self.db.tick().await;
        self.slice.ticked(started.elapsed());
        if let Err(error) = ticked {
            tracing::error!(%error, "server runtime tick failed");
        }
    }

    /// Drain output only after the database tick's persistence boundary.
    pub fn flush(&mut self) {
        // A disconnect may arrive while the tick is suspended. Drop its output
        // before publishing, but leave newly arrived frames for the next tick.
        let closed: Vec<_> = self
            .sessions
            .iter()
            .filter_map(|(id, session)| session.input.inbox.borrow().closed.then_some(*id))
            .collect();
        for session in closed {
            self.close_session(session);
        }
        self.report_slow_queries();
        self.report_merge_fallbacks();
        for (subscriber, error) in self.db.take_closed_subscribers() {
            let Some(id) = self
                .sessions
                .iter()
                .find_map(|(id, session)| (session.subscriber == subscriber).then_some(*id))
            else {
                continue;
            };
            tracing::warn!(%error, app = self.app.as_deref().unwrap_or("?"), "closing sync session");
            let _ = self.sessions[&id]
                .output
                .send((Err(session_closed_error(&error)), 0));
            self.close_session(id);
        }
        let mut gone = Vec::new();
        for (id, session) in &self.sessions {
            let mut outbound = session.queues.outbound.borrow_mut();
            let mut frames = Vec::new();
            let mut bytes = 0;
            while session.egress.load(Ordering::Acquire) + bytes < SESSION_QUEUE_BUDGET
                && let Some((encoded, size)) = outbound.pop_front()
            {
                bytes += size;
                frames.extend(encoded);
            }
            drop(outbound);
            session.queues.backlog.handed_on(bytes);
            if frames.is_empty() {
                continue;
            }
            session.egress.fetch_add(bytes, Ordering::AcqRel);
            if session.output.send((Ok(frames), bytes)).is_err() {
                gone.push(*id);
            }
        }
        for id in gone {
            self.close_session(id);
        }
        self.poll_thin();
        self.pump_thin();
    }
}

/// What a session pinned to a bundle other than `published` is told when it
/// ends: reconnect later. `NotReady` with `Later` is the close every client
/// already answers by reconnecting with backoff, so new permissions need no
/// new wire code; the message says why.
pub(super) fn superseded_error(published: &jazz::catalogue::BundleId) -> jazz::wire::WireError {
    jazz::wire::WireError::new(
        WireErrorCode::NotReady,
        WireRetry::Later,
        format!(
            "permissions changed: bundle {published} was published; reconnect through the \
             app's backend"
        ),
    )
}

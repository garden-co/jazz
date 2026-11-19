//! The runtime's engine driven directly by its host, on the host's own
//! thread: what a host without the native store thread (a Cloudflare
//! Durable Object, through jazz-wasm) runs. It serves sync and thin
//! sessions exactly as the native server does ([`Owner`]), with the same
//! ingress and egress budgets, but synchronously: the host hands it frames
//! as its sockets deliver them, ticks it, and drains each session's
//! output to its socket.
//!
//! ```text
//! socket ──frames──► receive_frames ─┐
//!                                    ├─► tick ──► take_output ──► socket
//! socket ──thin────► receive_thin ───┘
//! ```
//!
//! The host's contract:
//! - **Take output after every call** that hands the core input or ticks
//!   it: a thin request is served as it is received, so its answer may be
//!   waiting before any tick.
//! - **Tick when the core wants it** ([`ServerCore::wants_tick`]): after
//!   delivering input, and after taking output that brought a session back
//!   under its egress budget (output held back past the budget then moves
//!   on, as the native store thread is woken to tick).
//! - **Tick periodically** while sessions are open, at least every
//!   [`ServerCore::IDLE_TICK`] (a Durable Object alarm): time-based engine work runs
//!   then (expiring staged large-value chunks, the slow-reader check).
//! - **Egress is released when output is taken**: the host hands what it
//!   takes straight to its socket and relies on the socket's own buffering,
//!   so the egress budget bounds what the core holds, not what the socket
//!   has yet to write.
//! - **After a panic, drop the core** and open a new one on the same store:
//!   its state can't be trusted (the store rolls back what was in flight).
//! - A thin client's open transaction (until the core refuses them, as a
//!   core whose app's backend runs transactions does) holds state in the
//!   core between calls: the host must keep the same core while its
//!   session lives.

use super::*;

/// The runtime's engine without a host thread (see the module docs).
pub struct ServerCore {
    owner: Owner,
    next_session: u64,
    edges: HashMap<ServerSession, CoreEdge>,
    /// The server's budget for split thin requests being joined.
    thin_ingress: Arc<tokio::sync::Semaphore>,
    /// Input was accepted, or output taken brought a session back under
    /// its egress budget: the next tick moves what that changed (other
    /// sessions' live queries and subscriptions, held output).
    tick_wanted: bool,
}

/// A session's side of the core: its output, and its ingress and egress
/// budgets.
struct CoreEdge {
    output: tokio_mpsc::UnboundedReceiver<Outbound>,
    ingress: Arc<tokio::sync::Semaphore>,
    thin_ingress: Arc<tokio::sync::Semaphore>,
    egress: Arc<Egress>,
}

impl ServerCore {
    /// How often a host ticks an idle core while sessions are open (see
    /// the module docs).
    pub const IDLE_TICK: std::time::Duration = std::time::Duration::from_secs(1);

    /// Open the core on `file` (whose store a host opened, e.g. on a
    /// Durable Object's SQL, through `StoreFile::from_storage`). A store with no
    /// published schema yet serves `schema`; `any_stored_catalogue` says
    /// whether the store already holds one. `app` names it in logs.
    pub fn open(
        schema: JazzSchema,
        file: StoreFile,
        any_stored_catalogue: bool,
        app: Option<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            owner: Owner::open_on(schema, file, any_stored_catalogue, app, |config| config)?,
            next_session: 1,
            edges: HashMap::new(),
            thin_ingress: Arc::new(tokio::sync::Semaphore::new(THIN_INGRESS_SERVER_BUDGET)),
            tick_wanted: false,
        })
    }

    /// The core's store.
    pub fn db(&self) -> &Db {
        &self.owner.db
    }

    fn new_edge(
        &mut self,
    ) -> (
        ServerSession,
        tokio_mpsc::UnboundedSender<Outbound>,
        Arc<Egress>,
    ) {
        let session = ServerSession(self.next_session);
        self.next_session += 1;
        let (output, receiver) = tokio_mpsc::unbounded_channel();
        let egress = Arc::new(Egress::default());
        self.edges.insert(
            session,
            CoreEdge {
                output: receiver,
                ingress: Arc::new(tokio::sync::Semaphore::new(SESSION_QUEUE_BUDGET)),
                thin_ingress: Arc::new(tokio::sync::Semaphore::new(
                    jazz::thin::MAX_THIN_MESSAGE_BYTES,
                )),
                egress: Arc::clone(&egress),
            },
        );
        (session, output, egress)
    }

    fn settle(
        &mut self,
        session: ServerSession,
        opened: Result<(), jazz::db::Error>,
    ) -> Result<ServerSession, SessionOpenError> {
        match opened {
            Ok(()) => Ok(session),
            Err(error) => {
                self.edges.remove(&session);
                Err(SessionOpenError::Refused(error))
            }
        }
    }

    /// Open a sync session for an admitted connection.
    pub fn open_session(
        &mut self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        trust: CommitUnitTrust,
    ) -> Result<ServerSession, SessionOpenError> {
        let served_as = trust.served_as(identity);
        let (session, output, egress) = self.new_edge();
        let opened = self
            .owner
            .open_session(session, served_as, claims, bundle, output, egress);
        self.settle(session, opened)
    }

    /// Observe accepted query registrations on a sync session whose host can hibernate.
    pub fn observe_registrations(
        &self,
        session: ServerSession,
        observer: Rc<dyn Fn(&SyncMessage)>,
        restored: bool,
    ) {
        if let Some(state) = self.owner.sessions.get(&session) {
            self.owner
                .db
                .observe_subscriber_registrations(state.subscriber, observer, restored);
        }
    }

    /// End the window for registering saved reads without opening them.
    pub fn finish_session_restore(&self, session: ServerSession) {
        if let Some(state) = self.owner.sessions.get(&session) {
            self.owner.db.finish_subscriber_restore(state.subscriber);
        }
    }

    pub fn has_partial_frame(&self, session: ServerSession) -> bool {
        self.owner
            .sessions
            .get(&session)
            .is_some_and(|state| state.joiner.is_partial())
    }

    /// Notify a hibernating host when the schema or accepted live reads change.
    pub fn observe_thin_registrations(
        &mut self,
        session: ServerSession,
        observe: super::ThinRegistrationObserver,
    ) {
        if let Some(thin) = self.owner.thin.get_mut(&session) {
            thin.observer = Some(observe);
        }
    }

    /// Reinstall saved thin watches under the newly admitted session.
    pub fn restore_thin(
        &mut self,
        session: ServerSession,
        version: Option<jazz::ids::SchemaVersionId>,
        reads: Vec<jazz::thin::ThinRequest>,
    ) -> Result<(), String> {
        self.owner.restore_thin(session, version, reads)
    }

    /// Whether sleeping now would discard an operation the peer still expects to finish.
    pub fn unfinished_thin_work(&self, session: ServerSession) -> bool {
        self.owner
            .thin
            .get(&session)
            .is_some_and(|thin| thin.has_unfinished_work())
    }

    /// Open a thin-client session for an admitted connection.
    pub fn open_thin_session(
        &mut self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        trust: CommitUnitTrust,
    ) -> Result<ServerSession, SessionOpenError> {
        let served_as = trust.served_as(identity);
        let (session, output, egress) = self.new_edge();
        let opened = self
            .owner
            .open_thin_session(session, served_as, claims, bundle, output, egress);
        self.settle(session, opened)
    }

    /// Queue a batch of inbound frames (each `postcard(SyncMessage)`) read
    /// off `session`'s socket, `bytes` long, if its ingress budget has room
    /// now: `Ok(false)` when it has not, and the host should hold the batch
    /// (stop reading) until a tick has taken earlier frames.
    pub fn receive_frames(
        &mut self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
        bytes: usize,
    ) -> Result<bool, String> {
        let edge = self
            .edges
            .get(&session)
            .ok_or_else(|| "unknown server session".to_owned())?;
        let permits =
            u32::try_from(bytes.clamp(1, SESSION_QUEUE_BUDGET)).expect("the budget fits u32");
        let room = match Arc::clone(&edge.ingress).try_acquire_many_owned(permits) {
            Ok(room) => room,
            Err(tokio::sync::TryAcquireError::NoPermits) => return Ok(false),
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err("server runtime stopped".to_owned());
            }
        };
        self.owner
            .receive(session, frames, WireIngress { _room: room });
        self.tick_wanted = true;
        Ok(true)
    }

    /// Queue a thin client's request, `bytes` long on the wire (`split`
    /// if it came in parts), if `session`'s budget (and, split, the
    /// server's) has room now: `Ok(false)` when not, as for frames.
    pub fn receive_thin_request(
        &mut self,
        session: ServerSession,
        request: jazz::thin::ThinRequest,
        bytes: usize,
        split: bool,
    ) -> Result<bool, String> {
        let permits = u32::try_from(bytes)
            .ok()
            .filter(|_| bytes <= jazz::thin::MAX_THIN_MESSAGE_BYTES)
            .ok_or_else(|| format!("a thin request of {bytes} bytes is over the most one takes"))?;
        let edge = self
            .edges
            .get(&session)
            .ok_or_else(|| "unknown server session".to_owned())?;
        let Ok(session_room) = Arc::clone(&edge.thin_ingress).try_acquire_many_owned(permits)
        else {
            return Ok(false);
        };
        let server_room = if split {
            let Ok(room) = Arc::clone(&self.thin_ingress).try_acquire_many_owned(permits) else {
                return Ok(false);
            };
            Some(room)
        } else {
            None
        };
        self.owner.thin_request(
            session,
            request,
            ThinIngress {
                _session: session_room,
                _server: server_room,
            },
        );
        // Served already, but what it wrote reaches other sessions' live
        // queries and subscriptions at the next tick.
        self.tick_wanted = true;
        Ok(true)
    }

    /// Run the engine: apply what sessions sent, answer them, push what
    /// changed. A host ticks after delivering input and whenever
    /// [`Self::inbound_pending`] says input still waits.
    pub fn tick(&mut self) {
        self.tick_wanted = false;
        self.owner.tick();
    }

    /// Whether input waits for a tick.
    pub fn inbound_pending(&self) -> bool {
        self.owner.inbound_pending()
    }

    /// Whether the core wants a tick now: input was accepted since the last
    /// tick (a thin write's effects on other sessions move at a tick), or
    /// output taken brought a session back under its egress budget.
    pub fn wants_tick(&self) -> bool {
        self.tick_wanted || self.inbound_pending()
    }

    /// `session`'s output so far, in order: frame batches to send, or the
    /// error frame that ends the session. Taking it releases its bytes
    /// from the session's egress budget.
    pub fn take_output(
        &mut self,
        session: ServerSession,
    ) -> Vec<Result<Vec<AbiBytes>, jazz::wire::WireError>> {
        let Some(edge) = self.edges.get_mut(&session) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        while let Ok((result, bytes)) = edge.output.try_recv() {
            let before = edge.egress.fetch_sub(bytes, Ordering::AcqRel);
            // Back under the budget: output the core held back can move.
            if before >= SESSION_QUEUE_BUDGET && before - bytes < SESSION_QUEUE_BUDGET {
                self.tick_wanted = true;
            }
            out.push(result);
        }
        out
    }

    /// End `session` (its socket closed).
    pub fn close_session(&mut self, session: ServerSession) {
        self.owner.close_session(session);
        self.edges.remove(&session);
    }

    /// Read the catalogue again after the store's catalogue changed.
    pub fn reload_catalogue(&mut self) -> Result<(), String> {
        self.owner.reload_catalogue()
    }

    /// Apply `change` to the store's catalogue in one transaction, as the
    /// native runtime does: a change that fails leaves nothing behind.
    /// With `supersedes`, the
    /// change publishes that bundle new to this store, and every session
    /// pinned to another bundle ends, told to reconnect later (sessions
    /// that ended stay ended even when a later step failed). The
    /// catalogue is read again after.
    ///
    /// `expected` is the index the change was staged against
    /// ([`crate::portable::catalogue::StagedDeployment::expected`]): if the
    /// store's catalogue no longer reads the same, the change is refused,
    /// as native refuses a deployment whose catalogue moved under it.
    /// Stage it on this core's own catalogue: `expected` must be
    /// `CatalogueIndex::from_rows(&core.db().catalogue()?, None)`, read from
    /// this core. An index of the store read before the core opened differs
    /// (a core admits the schema it opens on), and is refused.
    pub fn apply_catalogue(
        &mut self,
        expected: &crate::portable::catalogue::CatalogueIndex,
        change: CatalogueChange,
        supersedes: Option<jazz::catalogue::BundleId>,
    ) -> Result<(), CatalogueApplyError> {
        let rows = self
            .db()
            .catalogue()
            .map_err(|error| CatalogueApplyError::Failed(error.to_string()))?;
        if crate::portable::catalogue::CatalogueIndex::from_rows(&rows, None) != *expected {
            return Err(CatalogueApplyError::Rejected(
                "catalogue changed while preparing deployment; fetch the graph and retry"
                    .to_owned(),
            ));
        }
        let (ended, result) = self.owner.apply_catalogue(change, supersedes);
        // Sessions that ended are gone even when a later step failed: the
        // host takes their error frames and closes their sockets, and a
        // tick moves what the change made visible.
        if result.is_ok() || ended > 0 {
            self.tick_wanted = true;
        }
        result
    }

    /// End every session pinned to a bundle `ends` names, telling each
    /// `error` first. How many ended.
    pub fn end_pinned_sessions(
        &mut self,
        ends: &dyn Fn(&jazz::catalogue::BundleId) -> bool,
        error: &jazz::wire::WireError,
    ) -> usize {
        self.owner.end_pinned_sessions(ends, error)
    }

    /// Close every session and the core.
    pub fn close(self) {
        self.owner.close();
    }
}

// White-box: these drive `ServerCore` as a host without the store thread
// does (no such host is in this crate), with and without `native`.
#[cfg(test)]
mod tests {
    use super::*;
    use jazz::db::{ReadOpts, ReadTier};
    use jazz::tools::public_schema::{ColumnType, SchemaBuilder, TableSchema};
    use jazz::tools::{PolicyExpr, TablePolicies};
    use std::cell::RefCell;
    use std::collections::VecDeque;

    /// `notes` (open to all) and `locked` (no one may insert, except a
    /// trusted link served as SYSTEM).
    fn source() -> jazz::tools::public_schema::Schema {
        let open = TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True);
        let locked = TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::False);
        SchemaBuilder::new()
            .table(
                TableSchema::builder("notes")
                    .column("body", ColumnType::Bytea)
                    .policies(open),
            )
            .table(
                TableSchema::builder("locked")
                    .column("body", ColumnType::Bytea)
                    .policies(locked),
            )
            .build()
    }

    fn core() -> ServerCore {
        let schema = JazzSchema::new(&source()).unwrap();
        ServerCore::open(schema, StoreFile::memory().unwrap(), false, None).unwrap()
    }

    fn body(bytes: Vec<u8>) -> BTreeMap<String, Value> {
        BTreeMap::from([("body".to_owned(), Value::Bytes(bytes))])
    }

    fn thin_session(
        core: &mut ServerCore,
        identity: AuthorSubject,
        trust: CommitUnitTrust,
    ) -> ServerSession {
        let session = core
            .open_thin_session(identity, BTreeMap::new(), None, trust)
            .unwrap();
        let schema = jazz::thin::ThinOp::Schema {
            schema: serde_json::to_vec(&source()).unwrap(),
        };
        send_thin(core, session, 0, schema);
        assert_eq!(answers(core, session), [(0, jazz::thin::ThinBody::Ok)]);
        session
    }

    fn send_thin(core: &mut ServerCore, session: ServerSession, id: u64, op: jazz::thin::ThinOp) {
        let request = jazz::thin::ThinRequest { id, op };
        assert!(
            core.receive_thin_request(session, request, 1024, false)
                .unwrap(),
            "room for the request"
        );
    }

    /// The thin answers in what the core has for `session`.
    fn answers(core: &mut ServerCore, session: ServerSession) -> Vec<(u64, jazz::thin::ThinBody)> {
        let mut assembler = jazz::thin::ThinAssembler::default();
        let mut out = Vec::new();
        for batch in core.take_output(session) {
            for frame in batch.expect("frames") {
                let frame = match jazz::wire::decode_frame(&frame).unwrap() {
                    jazz::wire::WireFrame::ThinPart(part) => match assembler.push(part).unwrap() {
                        Some(joined) => jazz::wire::decode_postcard_frame(&joined).unwrap(),
                        None => continue,
                    },
                    frame => frame,
                };
                match frame {
                    jazz::wire::WireFrame::ThinResponse(response) => {
                        out.push((response.id, response.body))
                    }
                    other => panic!("expected a thin response, got {other:?}"),
                }
            }
        }
        out
    }

    fn insert_into(table: &str) -> jazz::thin::ThinOp {
        let descriptor =
            jazz::records::RecordDescriptor::new([("body", jazz::records::ValueType::Bytes)]);
        let record = jazz::records::OwnedRecord::new(
            descriptor.create(&[Value::Bytes(b"x".to_vec())]).unwrap(),
            descriptor,
        );
        jazz::thin::ThinOp::Insert {
            table: table.to_owned(),
            cells: jazz::binding_codec::encode_named_cells(&record).unwrap(),
            options: Vec::new(),
            transaction: None,
        }
    }

    fn query(table: &str) -> jazz::thin::ThinOp {
        jazz::thin::ThinOp::Query {
            query: postcard::to_allocvec(&jazz::query::Query::from(table)).unwrap(),
            options: Vec::new(),
            transaction: None,
        }
    }

    /// A host drives the engine itself: it hands a backend's thin requests
    /// in, ticks, and takes the answers, with nothing held between calls.
    #[test]
    fn a_host_drives_server_core_directly() {
        let mut core = core();
        block_on(
            core.db()
                .insert("notes", body(b"hello".to_vec()), Default::default()),
        )
        .unwrap();
        let session = thin_session(
            &mut core,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::TrustedBackend,
        );
        // A thin request is served as it is received: its answer waits
        // for the host to take it.
        send_thin(&mut core, session, 1, query("notes"));
        let answers = answers(&mut core, session);
        let [(1, jazz::thin::ThinBody::Rows { rows, .. })] = answers.as_slice() else {
            panic!("expected rows, got {answers:?}");
        };
        let rows = jazz::binding_codec::decode_rows(rows).unwrap();
        assert_eq!(
            rows[0].rows[0].cells.get("body"),
            Some(&Value::Bytes(b"hello".to_vec()))
        );
        core.tick();
        assert!(!core.wants_tick());
        assert!(core.db().held_for_test().is_nothing());
        core.close_session(session);
        assert!(core.take_output(session).is_empty());
        core.close();
    }

    /// Trust decides who a session is served as. A user session (`alice`)
    /// is served as alice: on a core that requires sessions pinned to a
    /// published permissions bundle (as a createJazz app's does), an
    /// unpinned one is refused. The app's backend, a trusted link, is
    /// served as SYSTEM: it opens, and writes where the table's policy
    /// lets no user insert.
    ///
    /// ```text
    /// alice   (Session, unpinned) ──open──► refused
    /// backend (TrustedBackend)    ──open──► SYSTEM ──insert locked──► written
    /// ```
    #[test]
    fn trust_decides_who_a_session_is_served_as() {
        let mut core = core();
        let alice = AuthorSubject::authenticated("https://idp.example", "alice").unwrap();
        let refused =
            core.open_thin_session(alice, BTreeMap::new(), None, CommitUnitTrust::Session);
        assert!(
            matches!(&refused, Err(SessionOpenError::Refused(error)) if error.message.contains("served through createJazz")),
            "{refused:?}"
        );
        let refused = core.open_session(alice, BTreeMap::new(), None, CommitUnitTrust::Session);
        assert!(matches!(refused, Err(SessionOpenError::Refused(_))));

        let backend = thin_session(&mut core, alice, CommitUnitTrust::TrustedBackend);
        send_thin(&mut core, backend, 1, insert_into("locked"));
        let got = answers(&mut core, backend);
        assert!(
            matches!(got[0], (1, jazz::thin::ThinBody::Written { .. })),
            "{got:?}"
        );
    }

    /// A thin write reaches other sessions' live queries at the next tick,
    /// so a thin request makes the core want one: a host ticking only when
    /// the core wants it (and every `IDLE_TICK`) delivers the event at once.
    ///
    /// ```text
    /// A ──subscribe notes──► initial results
    /// B ──insert note──────► Written; wants_tick ──tick──► A's event
    /// ```
    #[test]
    fn a_thin_write_wants_the_tick_that_reaches_other_sessions_live_queries() {
        let mut core = core();
        let watcher = thin_session(
            &mut core,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::TrustedBackend,
        );
        let writer = thin_session(
            &mut core,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::TrustedBackend,
        );
        send_thin(
            &mut core,
            watcher,
            1,
            jazz::thin::ThinOp::Subscribe {
                query: postcard::to_allocvec(&jazz::query::Query::from("notes")).unwrap(),
                options: Vec::new(),
            },
        );
        while core.wants_tick() {
            core.tick();
        }
        let opening = answers(&mut core, watcher);
        assert!(opening.iter().all(|(id, _)| *id == 1), "{opening:?}");

        send_thin(&mut core, writer, 2, insert_into("notes"));
        let got = answers(&mut core, writer);
        assert!(
            matches!(got[..], [(2, jazz::thin::ThinBody::Written { .. })]),
            "{got:?}"
        );
        assert!(
            core.wants_tick(),
            "the write's effects on others move at a tick"
        );
        core.tick();
        let events = answers(&mut core, watcher);
        assert!(
            events
                .iter()
                .any(|(id, body)| *id == 1 && matches!(body, jazz::thin::ThinBody::Event { .. })),
            "{events:?}"
        );
    }

    /// Output past a session's egress budget waits in the core until the
    /// host has taken what came before it; taking that brings the session
    /// back under its budget, so the core asks for a tick, and the rest
    /// comes. Four answers of 3 MiB each are over the 8 MiB budget.
    ///
    /// ```text
    /// 4 queries ──tick──► answers up to 8 MiB, the rest held
    /// take ──► wants_tick ──tick──► the rest
    /// ```
    #[test]
    fn output_past_the_egress_budget_comes_once_the_host_took_what_came_before() {
        let mut core = core();
        for _ in 0..2 {
            let big = vec![7u8; 1536 * 1024];
            block_on(core.db().insert("notes", body(big), Default::default())).unwrap();
        }
        let session = thin_session(
            &mut core,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::TrustedBackend,
        );
        for id in 1..=4 {
            send_thin(&mut core, session, id, query("notes"));
        }
        core.tick();
        let mut got = answers(&mut core, session);
        assert!(got.len() < 4, "the budget holds some answers back");
        assert!(core.wants_tick(), "taking output wants the held rest sent");
        for _ in 0..8 {
            if !core.wants_tick() {
                break;
            }
            core.tick();
            got.extend(answers(&mut core, session));
        }
        let ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [1, 2, 3, 4]);
        assert!(
            got.iter()
                .all(|(_, body)| matches!(body, jazz::thin::ThinBody::Rows { .. }))
        );
    }

    /// A replica's link to the core, as a host's socket carries it: the
    /// replica's messages as encoded frames for the core, and the core's
    /// frames joined back into messages for the replica.
    #[derive(Default, Clone)]
    struct Link {
        to_core: Rc<RefCell<Vec<Vec<u8>>>>,
        to_replica: Rc<RefCell<VecDeque<SyncMessage>>>,
    }

    impl Transport for Link {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            self.to_core
                .borrow_mut()
                .extend(jazz::db::encode_frames(message)?);
            Ok(())
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            self.to_replica.borrow_mut().pop_front()
        }
    }

    /// A sync session round trip: a replica's write reaches the core's
    /// store through the session, and the core's answer settles it at the
    /// replica.
    ///
    /// ```text
    /// replica ──commit──► core ──accepted──► replica (settled)
    /// ```
    #[test]
    fn a_sync_session_carries_a_replicas_write_and_its_settlement() {
        let mut core = core();
        let session = core
            .open_session(
                AuthorSubject::SYSTEM,
                BTreeMap::new(),
                None,
                CommitUnitTrust::TrustedBackend,
            )
            .unwrap();
        let replica = block_on(Db::open(jazz::db::DbConfig::new(
            JazzSchema::new(&source()).unwrap(),
            jazz::db::DbIdentity {
                node: NodeUuid::from_bytes([0x11; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let link = Link::default();
        block_on(replica.connect_upstream(Box::new(link.clone())));
        block_on(replica.insert(
            "notes",
            body(b"from the replica".to_vec()),
            Default::default(),
        ))
        .unwrap();
        let mut joiner = jazz::db::FrameJoiner::default();
        for _ in 0..20 {
            block_on(replica.tick()).unwrap();
            let frames: Vec<Vec<u8>> = link.to_core.borrow_mut().drain(..).collect();
            if !frames.is_empty() {
                let bytes = frames.iter().map(Vec::len).sum();
                assert!(core.receive_frames(session, frames, bytes).unwrap());
            }
            core.tick();
            for batch in core.take_output(session) {
                for frame in batch.expect("frames") {
                    if let Some(message) = joiner.push(&frame).unwrap() {
                        link.to_replica.borrow_mut().push_back(message);
                    }
                }
            }
        }
        let rows = block_on(
            core.db().all(
                &core
                    .db()
                    .prepare_query(&jazz::query::Query::from("notes"))
                    .unwrap(),
                ReadOpts::for_read_tier(ReadTier::LocalOnly),
            ),
        )
        .unwrap();
        assert_eq!(rows.len(), 1, "the replica's write reached the core");
        let status = replica.upload_status();
        assert_eq!(status.pending, 0, "the core's answer settled the write");
    }

    /// A deploy request publishing `source`'s structure with its table
    /// policies as the permissions, as `createJazz` does on boot.
    fn deploy_request(
        source: &jazz::tools::public_schema::Schema,
    ) -> crate::portable::deployment::DeployRequest {
        let mut structural = source.clone();
        for table in structural.values_mut() {
            table.policies = TablePolicies::default();
        }
        let hash = jazz::tools::public_schema::SchemaHash::compute(&structural).to_string();
        crate::portable::deployment::DeployRequest {
            target_schema_hash: hash.clone(),
            schemas: vec![crate::portable::deployment::DeploySchema {
                hash,
                schema: structural,
            }],
            migrations: Vec::new(),
            permissions: source
                .iter()
                .map(|(name, table)| (*name, table.policies.clone()))
                .collect(),
            allow_dropping_history: false,
        }
    }

    /// Deploy `request` onto `core` as a host without the native store
    /// does: index the core's catalogue, prepare and stage through
    /// `portable`, and apply the change, superseding other bundles when it
    /// publishes a new one.
    fn deploy(
        core: &mut ServerCore,
        request: crate::portable::deployment::DeployRequest,
    ) -> crate::portable::catalogue::DeploymentResponse {
        let rows = core.db().catalogue().unwrap();
        let index = crate::portable::catalogue::CatalogueIndex::from_rows(&rows, None);
        let deployment = crate::portable::deployment::prepare(&index, request, 1_000)
            .unwrap_or_else(|error| panic!("deploy refused: {}", error.body));
        if deployment.staged.response.changed {
            let change = deployment.catalogue_change(&rows).unwrap_or_else(|error| {
                panic!("no catalogue change: {}", error.body);
            });
            core.apply_catalogue(
                deployment.staged.expected(),
                change,
                deployment.staged.new_bundle(),
            )
            .unwrap();
        }
        deployment.staged.response
    }

    fn bundle(
        response: &crate::portable::catalogue::DeploymentResponse,
    ) -> jazz::catalogue::BundleId {
        jazz::catalogue::BundleId::from_hex(&response.bundle).unwrap()
    }

    fn alice() -> AuthorSubject {
        AuthorSubject::authenticated("https://idp.example", "alice").unwrap()
    }

    /// A bundle deployed through `portable` onto a core is one a user
    /// session can pin to: before the deployment, a session pinned to it
    /// is refused; after, it opens.
    ///
    /// ```text
    /// alice (pinned to B) ──open──► refused
    /// deploy B ──► alice (pinned to B) ──open──► served
    /// ```
    #[test]
    fn a_bundle_deployed_onto_a_core_serves_sessions_pinned_to_it() {
        let mut core = core();
        let request = deploy_request(&source());
        let index = crate::portable::catalogue::CatalogueIndex::from_rows(
            &core.db().catalogue().unwrap(),
            None,
        );
        let expected = crate::portable::deployment::prepare(&index, deploy_request(&source()), 1)
            .unwrap()
            .staged
            .response;
        let pinned = bundle(&expected);
        let refused = core.open_session(
            alice(),
            BTreeMap::new(),
            Some(pinned),
            CommitUnitTrust::Session,
        );
        assert!(refused.is_err(), "{refused:?}");

        let response = deploy(&mut core, request);
        assert!(response.changed);
        assert_eq!(bundle(&response), pinned);
        assert_eq!(
            core.db()
                .catalogue()
                .unwrap()
                .bundles
                .iter()
                .map(|known| known.id)
                .collect::<Vec<_>>(),
            [pinned]
        );
        let opened = core.open_session(
            alice(),
            BTreeMap::new(),
            Some(pinned),
            CommitUnitTrust::Session,
        );
        assert!(opened.is_ok(), "{opened:?}");
    }

    /// Deploying a second bundle ends the sessions pinned to the first,
    /// told to reconnect later; an unpinned trusted backend stays.
    ///
    /// ```text
    /// deploy A ──► alice pinned to A
    /// deploy B ──► alice: NotReady/Later "permissions changed"; backend stays
    /// ```
    #[test]
    fn deploying_a_second_bundle_ends_sessions_pinned_to_the_first() {
        let mut core = core();
        let first = bundle(&deploy(&mut core, deploy_request(&source())));
        let session = core
            .open_session(
                alice(),
                BTreeMap::new(),
                Some(first),
                CommitUnitTrust::Session,
            )
            .unwrap();
        let backend = core
            .open_session(
                AuthorSubject::SYSTEM,
                BTreeMap::new(),
                None,
                CommitUnitTrust::TrustedBackend,
            )
            .unwrap();
        core.tick();
        core.take_output(session);
        core.take_output(backend);

        let mut stricter = source();
        for table in stricter.values_mut() {
            table.policies = TablePolicies::new().with_select(PolicyExpr::True);
        }
        let second = deploy(&mut core, deploy_request(&stricter));
        assert!(second.changed);
        assert_ne!(bundle(&second), first);
        assert!(core.wants_tick(), "the change wants a tick");

        let ended = core.take_output(session);
        let [.., Err(error)] = ended.as_slice() else {
            panic!("expected the session's error, got {ended:?}");
        };
        assert_eq!(error.code, jazz::wire::WireErrorCode::NotReady);
        assert_eq!(error.retry, jazz::wire::WireRetry::Later);
        assert!(error.message.contains("permissions changed"), "{error:?}");
        assert!(
            core.take_output(backend).iter().all(Result::is_ok),
            "the unpinned backend stays"
        );
    }

    /// Deploying the same request twice changes nothing the second time:
    /// it reports `changed: false`, publishes no artifacts, and ends no
    /// session pinned to the bundle.
    #[test]
    fn deploying_the_same_request_twice_changes_nothing() {
        let mut core = core();
        let first = deploy(&mut core, deploy_request(&source()));
        assert!(first.changed);
        let session = core
            .open_session(
                alice(),
                BTreeMap::new(),
                Some(bundle(&first)),
                CommitUnitTrust::Session,
            )
            .unwrap();
        core.tick();
        core.take_output(session);

        let again = deploy(&mut core, deploy_request(&source()));
        assert!(!again.changed);
        assert!(again.published.schemas.is_empty());
        assert!(again.published.migrations.is_empty());
        assert_eq!(again.bundle, first.bundle);
        assert!(
            core.take_output(session).iter().all(Result::is_ok),
            "the pinned session stays"
        );
    }

    /// A deployment staged against a catalogue that moved before it was
    /// applied is refused, and leaves the catalogue as it was.
    ///
    /// ```text
    /// stage A on C ── deploy B (C → C') ── apply A on C' ──► Rejected
    /// ```
    #[test]
    fn a_deployment_staged_on_a_catalogue_that_moved_is_refused() {
        let mut core = core();
        let rows = core.db().catalogue().unwrap();
        let index = crate::portable::catalogue::CatalogueIndex::from_rows(&rows, None);
        let stale =
            crate::portable::deployment::prepare(&index, deploy_request(&source()), 1).unwrap();
        let change = stale.catalogue_change(&rows).unwrap();

        let mut other = source();
        for table in other.values_mut() {
            table.policies = TablePolicies::new().with_select(PolicyExpr::True);
        }
        assert!(deploy(&mut core, deploy_request(&other)).changed);
        let moved = core.db().catalogue().unwrap();

        let refused = core
            .apply_catalogue(stale.staged.expected(), change, stale.staged.new_bundle())
            .unwrap_err();
        assert!(
            matches!(&refused, CatalogueApplyError::Rejected(message) if message.contains("catalogue changed")),
            "{refused:?}"
        );
        assert_eq!(core.db().catalogue().unwrap(), moved, "nothing was applied");
    }
}

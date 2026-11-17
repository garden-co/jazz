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

use super::*;

/// The runtime's engine without a host thread (see the module docs).
pub struct ServerCore {
    owner: Owner,
    next_session: u64,
    edges: HashMap<ServerSession, CoreEdge>,
    /// The server's budget for split thin requests being joined.
    thin_ingress: Arc<tokio::sync::Semaphore>,
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
    /// Open the core on `file` (whose store a host opened, e.g. on a
    /// Durable Object's SQL: [`StoreFile::from_storage`]). A store with no
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
        })
    }

    /// The core's store.
    pub fn db(&self) -> &Db {
        &self.owner.db
    }

    /// The identity a session is served as: its own for a user session,
    /// SYSTEM for a trusted backend or authority link (as natively).
    fn served_as(identity: AuthorSubject, trust: CommitUnitTrust) -> AuthorSubject {
        match trust {
            CommitUnitTrust::Session => identity,
            CommitUnitTrust::TrustedBackend | CommitUnitTrust::TrustedAuthority => {
                AuthorSubject::SYSTEM
            }
        }
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
        let served_as = Self::served_as(identity, trust);
        let (session, output, egress) = self.new_edge();
        let opened = self
            .owner
            .open_session(session, served_as, claims, bundle, output, egress);
        self.settle(session, opened)
    }

    /// Open a thin-client session for an admitted connection.
    pub fn open_thin_session(
        &mut self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        bundle: Option<jazz::catalogue::BundleId>,
        trust: CommitUnitTrust,
    ) -> Result<ServerSession, SessionOpenError> {
        let served_as = Self::served_as(identity, trust);
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
        Ok(true)
    }

    /// Run the engine: apply what sessions sent, answer them, push what
    /// changed. A host ticks after delivering input and whenever
    /// [`Self::inbound_pending`] says input still waits.
    pub fn tick(&mut self) {
        self.owner.tick();
    }

    /// Whether input waits for a tick.
    pub fn inbound_pending(&self) -> bool {
        self.owner.inbound_pending()
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
            edge.egress.fetch_sub(bytes, Ordering::AcqRel);
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

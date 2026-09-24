//! The local-first-unless-empty opening gate and the host remote-link hint.
//!
//! A read that asks for [`EmptyOpening::AwaitRemote`] evaluates exactly like a
//! local-first read, except that an *empty, unsettled* opening is withheld
//! while the authoritative server could still answer. "Could answer" is one
//! definition shared by subscriptions and one-shot reads: see
//! [`RemoteLinkHint`]. Everything here is host-API state; none of it is
//! persisted or sent on the wire.

use std::time::Duration;

use web_time::Instant;

use super::*;

/// How long an empty opening may wait on a remote link that is still being
/// attempted, measured from the start of the current attempt (not from the
/// read). A read that begins after the window has elapsed does not wait.
///
/// Once the link is live there is deliberately no bound: the opening is then
/// held until the stream settles, rows arrive, the subscription is rejected,
/// or the link is lost.
pub const REMOTE_LINK_ATTEMPT_WINDOW: Duration = Duration::from_secs(5);

/// What a read does with an empty, unsettled opening.
///
/// This is a host read option, not a durable encoding: it is never persisted
/// or sent to a peer. Its serde form exists only for the host JSON read-option
/// ABI, where an absent field means [`EmptyOpening::Deliver`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum EmptyOpening {
    /// Deliver the opening as evaluated (ordinary local-first behaviour).
    #[default]
    Deliver,
    /// Withhold an empty, unsettled local opening while the remote could
    /// answer ("local-first unless empty").
    ///
    /// Applies to client-local reads at the local tier with full
    /// propagation; other reads ignore it. While the remote could answer:
    /// - a subscription publishes nothing until its own stream settles, holds
    ///   a row, is rejected, or the remote can no longer answer, and then
    ///   behaves exactly like local-first. A non-durable foreground's stream
    ///   settles at its storage owner's local answer, so there the gate
    ///   instead waits for a `Global` witness coverage of the same read,
    ///   answered by the authority through the owner, and retires the witness
    ///   once it releases;
    /// - a one-shot read returns a non-empty local result as is, and otherwise
    ///   the strict remote result, falling back to the empty local result if
    ///   the remote read fails or the remote can no longer answer;
    /// - a query with a non-zero `offset` is read as a strict remote view
    ///   (Global tier, immediate local updates) instead, because local
    ///   pagination over a partially synced cache is literal and would
    ///   produce a wrong or empty page.
    ///
    /// When the remote cannot answer, every read is plain local-first.
    AwaitRemote,
}

/// What the host knows about the path to the authoritative server.
///
/// A core `Db` only sees its own upstream connections. A host that owns the
/// transport (or whose upstream is a relay with its own server link) reports
/// the path through [`Db::set_remote_link_hint`]. Until a host reports
/// anything, the core derives the state from its own upstream connections:
/// an attached upstream is `Live`, no upstream is `NoServer`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteLinkHint {
    /// No server is configured; nothing can answer remotely.
    NoServer,
    /// A connection attempt has started and has neither succeeded nor
    /// failed. The core timestamps the attempt when this is reported;
    /// reporting it again starts a new attempt window.
    Attempting,
    /// The path to the server is admitted and serviceable.
    Live,
    /// The attempt failed, the link is backing off between retries, or the
    /// application disconnected explicitly. Nothing waits.
    Failed,
}

impl RemoteLinkHint {
    /// Parse the host binding spelling. `connecting`, `connected` and
    /// `unavailable` are accepted as aliases for the TypeScript link-state
    /// names.
    pub fn from_host_str(value: &str) -> Option<Self> {
        match value {
            "none" | "None" | "no-server" | "NoServer" => Some(Self::NoServer),
            "attempting" | "Attempting" | "connecting" | "Connecting" => Some(Self::Attempting),
            "live" | "Live" | "connected" | "Connected" => Some(Self::Live),
            "failed" | "Failed" | "unavailable" | "Unavailable" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum HostLink {
    NoServer,
    Attempting { since: Instant },
    Live,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteReach {
    Live,
    Attempting { since: Instant },
    Unreachable,
}

/// Which read an opening gate is holding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OpeningRoute {
    /// A local-first stream whose empty unsettled opening is withheld.
    LocalFirst,
    /// An offset window read as a strict remote view.
    RemoteWindow,
}

/// Per-stream gate state, owned by the stream's publication boundary.
#[derive(Clone, Copy, Debug)]
pub(super) struct OpeningGate {
    /// Link-loss epoch observed when the gate armed.
    epoch: u64,
    pub(super) route: OpeningRoute,
    /// An otherwise publishable opening was withheld by this gate.
    pub(super) withheld: bool,
    /// The stream's own `settled` bit cannot witness the authority (a
    /// non-durable foreground settles at its storage owner's local answer),
    /// so the gate instead waits for the stream's authority witness coverage.
    pub(super) witnessed: bool,
    /// The authority witness coverage has its settled authority answer.
    pub(super) witness_answered: bool,
}

/// Remote reachability shared by every read of one `Db` runtime.
pub(super) struct RemoteLinkTracker {
    hint: Cell<Option<HostLink>>,
    live_upstreams: Cell<usize>,
    /// Incremented whenever a path that was live is lost, so every gate armed
    /// before the loss releases even if a new attempt starts immediately.
    loss_epoch: Cell<u64>,
    attempt_expiry_observed: Cell<bool>,
    scheduler: SharedTickScheduler,
    gated: RefCell<Vec<Weak<RefCell<SubscriptionState>>>>,
    /// Streams still owning authority witness coverage.
    witnessed: RefCell<Vec<Weak<RefCell<SubscriptionState>>>>,
    wakers: RefCell<Vec<Waker>>,
}

impl RemoteLinkTracker {
    pub(super) fn new(scheduler: SharedTickScheduler) -> Self {
        Self {
            hint: Cell::new(None),
            live_upstreams: Cell::new(0),
            loss_epoch: Cell::new(0),
            attempt_expiry_observed: Cell::new(false),
            scheduler,
            gated: RefCell::new(Vec::new()),
            witnessed: RefCell::new(Vec::new()),
            wakers: RefCell::new(Vec::new()),
        }
    }

    fn reach(&self) -> RemoteReach {
        match self.hint.get() {
            Some(HostLink::Live) => RemoteReach::Live,
            Some(HostLink::Attempting { since }) => RemoteReach::Attempting { since },
            Some(HostLink::NoServer | HostLink::Failed) => RemoteReach::Unreachable,
            None if self.live_upstreams.get() > 0 => RemoteReach::Live,
            None => RemoteReach::Unreachable,
        }
    }

    fn may_wait(&self, epoch: u64) -> bool {
        epoch == self.loss_epoch.get()
            && match self.reach() {
                RemoteReach::Live => true,
                RemoteReach::Attempting { since } => {
                    Instant::now() < since + REMOTE_LINK_ATTEMPT_WINDOW
                }
                RemoteReach::Unreachable => false,
            }
    }

    /// Arm a gate if the remote could answer now, returning its loss epoch.
    pub(super) fn arm(&self) -> Option<u64> {
        let epoch = self.loss_epoch.get();
        if !self.may_wait(epoch) {
            return None;
        }
        if let RemoteReach::Attempting { since } = self.reach() {
            self.schedule_attempt_expiry(since);
        }
        Some(epoch)
    }

    fn schedule_attempt_expiry(&self, since: Instant) {
        let remaining =
            (since + REMOTE_LINK_ATTEMPT_WINDOW).saturating_duration_since(Instant::now());
        if let Some(scheduler) = self.scheduler.borrow().as_ref() {
            // Round up so the tick observes an elapsed window.
            scheduler.schedule_tick_after(remaining.as_millis() as u64 + 1);
        }
    }

    pub(super) fn set_hint(&self, hint: RemoteLinkHint) {
        let was_live = self.reach() == RemoteReach::Live;
        let host = match hint {
            RemoteLinkHint::NoServer => HostLink::NoServer,
            RemoteLinkHint::Attempting => HostLink::Attempting {
                since: Instant::now(),
            },
            RemoteLinkHint::Live => HostLink::Live,
            RemoteLinkHint::Failed => HostLink::Failed,
        };
        self.hint.set(Some(host));
        if was_live && self.reach() != RemoteReach::Live {
            self.loss_epoch.set(self.loss_epoch.get().wrapping_add(1));
        }
        if let HostLink::Attempting { since } = host {
            self.attempt_expiry_observed.set(false);
            self.schedule_attempt_expiry(since);
        }
        self.notify();
    }

    pub(super) fn upstream_attached(&self) {
        self.live_upstreams.set(self.live_upstreams.get() + 1);
    }

    /// Any own-upstream detach is a link loss for every armed gate.
    pub(super) fn upstream_detached(&self) {
        self.live_upstreams
            .set(self.live_upstreams.get().saturating_sub(1));
        self.loss_epoch.set(self.loss_epoch.get().wrapping_add(1));
        self.notify();
    }

    /// Observe an elapsed attempt window on the host tick it scheduled.
    pub(super) fn on_tick(&self) {
        let RemoteReach::Attempting { since } = self.reach() else {
            return;
        };
        if !self.attempt_expiry_observed.get()
            && Instant::now() >= since + REMOTE_LINK_ATTEMPT_WINDOW
        {
            self.attempt_expiry_observed.set(true);
            self.notify();
        }
    }

    /// Register a stream whose opening gate is armed, releasing it at once if
    /// the remote stopped being able to answer while it was opening.
    pub(super) fn register(&self, state: &Rc<RefCell<SubscriptionState>>) {
        self.gated.borrow_mut().push(Rc::downgrade(state));
        self.notify();
    }

    /// Release every held opening whose remote can no longer answer, and wake
    /// one-shot reads racing a remote answer.
    fn notify(&self) {
        let gated = std::mem::take(&mut *self.gated.borrow_mut());
        let mut retained = Vec::with_capacity(gated.len());
        let mut busy = false;
        for weak in gated {
            let Some(state) = weak.upgrade() else {
                continue;
            };
            let Ok(state_ref) = state.try_borrow() else {
                // Mid-refresh; re-evaluate on the next owner turn.
                busy = true;
                retained.push(weak);
                continue;
            };
            let Some(gate) = state_ref.sender.opening_gate() else {
                continue;
            };
            if self.may_wait(gate.epoch) {
                retained.push(weak);
            } else {
                state_ref.release_opening_gate();
                // The next owner turn retires the released witness coverage.
                busy |= !state_ref.authority_witness.is_empty();
            }
        }
        self.gated.borrow_mut().extend(retained);
        if busy {
            schedule_tick_in(&self.scheduler, TickUrgency::AfterCurrentTurn);
        }
        for waker in std::mem::take(&mut *self.wakers.borrow_mut()) {
            waker.wake();
        }
    }

    pub(super) fn has_witnesses(&self) -> bool {
        !self.witnessed.borrow().is_empty()
    }

    pub(super) fn register_witness(&self, state: &Rc<RefCell<SubscriptionState>>) {
        self.witnessed.borrow_mut().push(Rc::downgrade(state));
    }

    /// Resolve the authority witnesses at the end of an owner turn, after
    /// every stream has folded that turn's inputs.
    ///
    /// A witness whose coverage has its settled authority answer marks its
    /// gate answered; a still-withheld (hence empty) opening is then released.
    /// Once a gate has released for any reason, the witness coverage is
    /// returned for retirement: afterwards the stream is an ordinary
    /// local-first stream on its own (owner-local) coverage.
    pub(super) fn resolve_witnesses(
        &self,
        answered: impl Fn(&[UpstreamCoverageHandle]) -> bool,
    ) -> Vec<UpstreamCoverageHandle> {
        let witnessed = std::mem::take(&mut *self.witnessed.borrow_mut());
        let mut retained = Vec::with_capacity(witnessed.len());
        let mut retired = Vec::new();
        for weak in witnessed {
            let Some(state) = weak.upgrade() else {
                continue;
            };
            let Ok(mut state_ref) = state.try_borrow_mut() else {
                retained.push(weak);
                continue;
            };
            if state_ref.closed.get() {
                // Stream finalization retires the witness with the stream.
                continue;
            }
            if let Some(gate) = state_ref.sender.opening_gate()
                && !gate.witness_answered
                && answered(&state_ref.authority_witness)
            {
                state_ref.sender.answer_witness();
                if gate.withheld {
                    state_ref.release_opening_gate();
                }
            }
            if state_ref.sender.opening_gate().is_some() {
                retained.push(weak);
            } else {
                retired.append(&mut state_ref.authority_witness);
            }
        }
        self.witnessed.borrow_mut().extend(retained);
        retired
    }

    fn register_waker(&self, waker: &Waker) {
        let mut wakers = self.wakers.borrow_mut();
        if !wakers.iter().any(|existing| existing.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }
}

/// Resolves once the remote can no longer answer a read armed at `epoch`.
struct RemoteAnswerLoss {
    tracker: Rc<RemoteLinkTracker>,
    epoch: u64,
}

impl Future for RemoteAnswerLoss {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.tracker.may_wait(self.epoch) {
            return Poll::Ready(());
        }
        self.tracker.register_waker(cx.waker());
        Poll::Pending
    }
}

impl OpeningGate {
    pub(super) fn armed(epoch: u64, route: OpeningRoute) -> Self {
        Self {
            epoch,
            route,
            withheld: false,
            witnessed: false,
            witness_answered: false,
        }
    }

    /// Still waiting for the answer that may release an empty opening.
    pub(super) fn awaits_answer(&self, settled: bool) -> bool {
        if self.witnessed {
            !self.witness_answered
        } else {
            !settled
        }
    }
}

impl SubscriptionSender {
    pub(super) fn opening_gate(&self) -> Option<OpeningGate> {
        self.publication.borrow().opening_gate
    }

    fn answer_witness(&self) {
        if let Some(gate) = self.publication.borrow_mut().opening_gate.as_mut() {
            gate.witness_answered = true;
        }
    }
}

impl SubscriptionState {
    /// Release an armed opening gate from outside the publication path (link
    /// loss, host hint, elapsed attempt window). A withheld opening is
    /// published now as one reset of the current maintained result, still
    /// unsettled; afterwards the stream publishes as its tier normally does.
    pub(super) fn release_opening_gate(&self) {
        let mut publication = self.sender.publication.borrow_mut();
        let Some(gate) = publication.opening_gate.take() else {
            return;
        };
        // Without a withheld opening, ordinary local-first publication
        // delivers the first materialized result when it exists. A strict
        // remote window would instead keep withholding its unsettled opening
        // until the authority answers, so it opens now with its current
        // (possibly empty) view even if no refresh withheld it yet.
        let window_unopened =
            gate.route == OpeningRoute::RemoteWindow && !self.pending_initial_local_snapshot;
        if publication.opened
            || !(gate.withheld || window_unopened)
            || !publication.unresolved.is_empty()
        {
            return;
        }
        let Ok(current) =
            SubscriptionPublicationSnapshot::capture(&self.snapshot, &self.snapshot_index)
        else {
            return;
        };
        let Ok(added) =
            subscription_outputs_with_occurrence_sidecar(&current.snapshot, &current.occurrences)
        else {
            return;
        };
        // Keep any pending canonical-reset request: a later unpublishable
        // replacement must still reopen from a complete snapshot.
        publication.opened = true;
        publication.deferred = None;
        drop(publication);
        let _ = self.sender.sender.unbounded_send(SubscriptionEvent::Delta {
            reset: true,
            publishable: true,
            added,
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: Vec::new(),
            settled: self.settled,
            tier: self.read_tier,
        });
    }
}

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Report what the host knows about the path to the authoritative server.
    ///
    /// This drives [`EmptyOpening::AwaitRemote`] reads only; it never changes
    /// write durability or turns a strict remote read into a local one.
    /// `NoServer` and `Failed` release every held empty opening; `Attempting`
    /// lets empty openings wait until [`REMOTE_LINK_ATTEMPT_WINDOW`] after
    /// this call; `Live` lets them wait without a bound. Losing a live path
    /// (reporting anything else, or detaching an own upstream) releases held
    /// openings. A host that never calls this gets a state derived from its
    /// own upstream connections.
    pub fn set_remote_link_hint(&self, hint: RemoteLinkHint) {
        self.node.remote_link.set_hint(hint);
    }

    /// Resolve an [`EmptyOpening::AwaitRemote`] subscription request: the
    /// effective read options and the gate to install, if any.
    pub(super) fn resolve_empty_opening(
        &self,
        prepared: &PreparedQuery,
        mut opts: ReadOpts,
        authorization_mode: QueryAuthorizationMode,
    ) -> (ReadOpts, Option<OpeningGate>) {
        let requested = std::mem::take(&mut opts.empty_opening) == EmptyOpening::AwaitRemote;
        if !requested
            || authorization_mode != QueryAuthorizationMode::ClientLocal
            || opts.propagation != Propagation::Full
            || effective_read_tier(&opts) != DurabilityTier::Local
        {
            return (opts, None);
        }
        let Some(epoch) = self.node.remote_link.arm() else {
            return (opts, None);
        };
        if prepared.shape().query().offset > 0 {
            opts.tier = DurabilityTier::Global;
            opts.local_updates = LocalUpdates::Immediate;
            return (
                opts,
                Some(OpeningGate::armed(epoch, OpeningRoute::RemoteWindow)),
            );
        }
        (
            opts,
            Some(OpeningGate::armed(epoch, OpeningRoute::LocalFirst)),
        )
    }

    pub(super) fn register_opening_gate(&self, state: &Rc<RefCell<SubscriptionState>>) {
        if !state.borrow().authority_witness.is_empty() {
            self.node.remote_link.register_witness(state);
        }
        self.node.remote_link.register(state);
    }

    /// One-shot [`EmptyOpening::AwaitRemote`] read shared by host bindings and
    /// the native facade.
    ///
    /// `local` and `remote` produce the local-first and strict remote results
    /// for the same query. A non-empty local result is returned as is. An
    /// empty one is replaced by the remote result while the remote could
    /// answer; if the remote read fails, or the remote can no longer answer
    /// first, the pending remote read is dropped (cancelling its coverage) and
    /// the empty local result is returned. A `windowed` (non-zero offset)
    /// query reads remote first while the remote could answer and falls back
    /// to `local` otherwise.
    #[doc(hidden)]
    pub async fn read_local_first_unless_empty<T, E, L, R>(
        &self,
        windowed: bool,
        local: impl FnOnce() -> L,
        remote: impl FnOnce() -> R,
        is_empty: impl FnOnce(&T) -> bool,
    ) -> Result<T, E>
    where
        L: Future<Output = Result<T, E>>,
        R: Future<Output = Result<T, E>>,
    {
        // Each phase is boxed so this future holds one phase at a time on the
        // heap rather than both inline (and in every poll frame).
        if windowed {
            if let Some(epoch) = self.node.remote_link.arm()
                && let Some(Ok(result)) = self.race_remote_answer(epoch, Box::pin(remote())).await
            {
                return Ok(result);
            }
            return Box::pin(local()).await;
        }
        let local = Box::pin(local()).await?;
        if !is_empty(&local) {
            return Ok(local);
        }
        let Some(epoch) = self.node.remote_link.arm() else {
            return Ok(local);
        };
        match self.race_remote_answer(epoch, Box::pin(remote())).await {
            Some(Ok(remote)) => Ok(remote),
            Some(Err(_)) | None => Ok(local),
        }
    }

    /// Poll `remote` until it completes or the remote can no longer answer.
    /// Returning `None` drops the pending remote read.
    async fn race_remote_answer<T>(
        &self,
        epoch: u64,
        remote: impl Future<Output = T>,
    ) -> Option<T> {
        let mut remote = pin!(remote);
        let mut loss = RemoteAnswerLoss {
            tracker: Rc::clone(&self.node.remote_link),
            epoch,
        };
        std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = remote.as_mut().poll(cx) {
                return Poll::Ready(Some(result));
            }
            if Pin::new(&mut loss).poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await
    }
}

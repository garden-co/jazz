//! One upstream link lifecycle for every host.
//!
//! [`UpstreamLink`] decides when a client connects to its server, how long it
//! backs off, when a lost server is published as an outage, and what happens
//! when the server refuses the client's credential. It does no IO and reads no
//! clock: the host owns the socket and the timer, feeds the machine classified
//! [`LinkInput`]s stamped with the current time, and carries out the
//! [`LinkAction`]s it returns. Observers drain [`LinkEvent`]s.
//!
//! Hosts differ only in what they own: the browser and Node own a JS
//! WebSocket, React Native's relay and the Rust client own a tokio socket.
//! The decisions are the same everywhere because they are made here.
//!
//! ```text
//!              admitted                link drops
//!  Connecting ─────────────► Live ─────────────────► Backoff ──┐
//!     │   ▲                   ▲                        │  ▲    │ timer: retry
//!     │   │                   └────── retry admitted ──┘  └────┘
//!     │   │ new credential                             │
//!     │   │                                            │ refused on retry
//!     ▼   │                                            ▼
//!   AwaitingCredential ◄───────────────────────────────┘
//! ```
//!
//! Invariants:
//! - Every [`LinkAction::Connect`] names a fresh [`AttemptId`]. Input that
//!   names any other attempt, or a timer that is no longer armed, is ignored,
//!   so a late failure from a replaced socket can never move the link.
//! - A refused credential is never retried with the same credential: the link
//!   waits in [`LinkPhase::AwaitingCredential`] with no timer armed until
//!   [`LinkInput::CredentialUpdated`] or an explicit reconnect.
//! - [`LinkEvent::AuthRestored`] follows only an [`LinkEvent::AuthFailed`]
//!   that has not been restored yet.

use std::collections::VecDeque;

use crate::options::RemoteLinkHint;

/// Retry timing. The defaults are the policy every host uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LinkPolicy {
    /// First network retry ceiling; each failure doubles it.
    pub retry_initial_ms: u64,
    /// Ceiling while an outage is not yet published. A server that returns
    /// within the reporting window is found before the report would fire.
    pub retry_unreported_max_ms: u64,
    /// Ceiling once the outage is published.
    pub retry_max_ms: u64,
    /// How long a link must have been failing before the next failed attempt
    /// publishes the outage. Publication is failure-triggered, never a timer:
    /// a wall-clock report could fire after the server is back but before the
    /// pending retry reconnects.
    pub outage_report_ms: u64,
    /// First retry after a server said it is still starting.
    pub not_ready_initial_ms: u64,
    /// Ceiling for those retries.
    pub not_ready_max_ms: u64,
}

impl Default for LinkPolicy {
    fn default() -> Self {
        Self {
            retry_initial_ms: 100,
            retry_unreported_max_ms: 1_000,
            retry_max_ms: 5_000,
            outage_report_ms: 7_500,
            not_ready_initial_ms: 25,
            not_ready_max_ms: 1_000,
        }
    }
}

/// One connection attempt, from [`LinkAction::Connect`] until it ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptId(pub u64);

/// One armed timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimerToken(pub u64);

/// Why the server refused a credential.
///
/// `Expired`, `Missing` and `Invalid` can be fixed by a new credential.
/// `Disabled` (the app turned that auth mode off) and `Revoked` (the identity
/// was revoked or reassigned) cannot; the link still waits, since only the
/// application can decide to recreate its client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthFailureReason {
    Expired,
    Missing,
    Invalid,
    Disabled,
    Revoked,
}

impl AuthFailureReason {
    /// Classify a server's refusal message, as the TypeScript client does.
    pub fn from_message(message: &str) -> Self {
        let lower = message.to_ascii_lowercase();
        if lower.contains("expired") {
            Self::Expired
        } else if lower.contains("missing") {
            Self::Missing
        } else if lower.contains("disabled") {
            Self::Disabled
        } else if lower.contains("revoked") {
            Self::Revoked
        } else {
            Self::Invalid
        }
    }

    /// The spelling the TypeScript `AuthFailureReason` uses.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Disabled => "disabled",
            Self::Revoked => "revoked",
        }
    }
}

/// How an attempt or an established link ended, classified by the host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkFailure {
    /// The network or socket failed, or the server closed the link. Retried
    /// with backoff for as long as it takes.
    Network(String),
    /// The server is still starting and refused the handshake. Retried
    /// quickly; readers keep waiting, since this is still the first attempt.
    ServerNotReady(String),
    /// The server refused the credential. Waits for a new one.
    AuthRejected {
        reason: AuthFailureReason,
        message: String,
    },
    /// The link cannot work without a change on this side (an incompatible
    /// protocol, an invalid endpoint). The link stops until an explicit
    /// reconnect.
    Fatal(String),
}

impl LinkFailure {
    fn message(&self) -> &str {
        match self {
            Self::Network(message)
            | Self::ServerNotReady(message)
            | Self::AuthRejected { message, .. }
            | Self::Fatal(message) => message,
        }
    }
}

/// What the host reports to the link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkInput {
    /// Whether a server is configured at all.
    ServerConfigured(bool),
    /// The application disconnected (`true`) or reconnected (`false`)
    /// explicitly. A reconnect always tries again at once, even after a
    /// refused credential or a fatal failure.
    SetOffline(bool),
    /// The attempt completed its handshake and the server admitted it.
    Admitted(AttemptId),
    /// The attempt failed, or its established link ended.
    Ended(AttemptId, LinkFailure),
    /// The host holds a new credential. A live link is replaced so the server
    /// sees it; a link waiting for a credential connects with it.
    CredentialUpdated,
    /// A timer the link armed has fired.
    TimerFired(TimerToken),
}

/// What the host must do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkAction {
    /// Open a socket with the credential of `credential_epoch` and report
    /// its outcome as `attempt`.
    Connect {
        attempt: AttemptId,
        credential_epoch: u64,
    },
    /// Close this attempt's socket. Its later events are ignored.
    Close(AttemptId),
    /// Report [`LinkInput::TimerFired`] with `token` after `after_ms`.
    ArmTimer { token: TimerToken, after_ms: u64 },
    /// Drop this timer; firing it later is harmless but useless.
    CancelTimer(TimerToken),
}

/// Where the link is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkPhase {
    /// No server is configured.
    NoServer,
    /// The application disconnected explicitly.
    Offline,
    /// A first attempt, or the first after going back online or after a new
    /// credential, including the server's not-ready retries. Readers that
    /// may wait for the server keep waiting.
    Connecting,
    /// Admitted and serviceable.
    Live,
    /// A link failed on the network and is retrying. Nothing waits on it.
    Backoff,
    /// The server refused the credential; nothing happens until a new one
    /// arrives or the application reconnects explicitly.
    AwaitingCredential(AuthFailureReason),
    /// A fatal failure; nothing happens until an explicit reconnect. The one
    /// phase in which waits for the server should fail rather than wait.
    Stopped,
}

/// The link as observers see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkStatus {
    pub phase: LinkPhase,
    /// The last failure's message, while the link is not live.
    pub reason: Option<String>,
    /// Network failures since the link was last admitted.
    pub consecutive_failures: u32,
    /// The server has been unreachable for the reporting window. This is a
    /// report, not a verdict: pending waits for the server stay pending
    /// across an outage and its reconnects (#3923); only a link that stops
    /// for good ([`LinkPhase::Stopped`]) fails them.
    pub outage_published: bool,
}

/// What observers learn, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkEvent {
    Status(LinkStatus),
    /// The server refused the current credential.
    AuthFailed(AuthFailureReason),
    /// A credential was admitted after a refusal.
    AuthRestored,
}

/// The upstream link state machine. See the module docs.
#[derive(Debug)]
pub struct UpstreamLink {
    policy: LinkPolicy,
    rng: u64,
    next_id: u64,
    configured: bool,
    offline: bool,
    phase: LinkPhase,
    attempt: Option<AttemptId>,
    timer: Option<TimerToken>,
    credential_epoch: u64,
    network_failures: u32,
    not_ready_failures: u32,
    outage_started_at: Option<u64>,
    outage_published: bool,
    auth_unrestored: bool,
    reason: Option<String>,
    published: Option<LinkStatus>,
    events: VecDeque<LinkEvent>,
}

impl UpstreamLink {
    /// A link with no server configured. `seed` drives the retry jitter, so
    /// hosts pass something random and tests pass a constant.
    pub fn new(policy: LinkPolicy, seed: u64) -> Self {
        let mut link = Self {
            policy,
            rng: seed,
            next_id: 0,
            configured: false,
            offline: false,
            phase: LinkPhase::NoServer,
            attempt: None,
            timer: None,
            credential_epoch: 0,
            network_failures: 0,
            not_ready_failures: 0,
            outage_started_at: None,
            outage_published: false,
            auth_unrestored: false,
            reason: None,
            published: None,
            events: VecDeque::new(),
        };
        link.publish();
        link
    }

    pub fn phase(&self) -> &LinkPhase {
        &self.phase
    }

    pub fn status(&self) -> LinkStatus {
        LinkStatus {
            phase: self.phase.clone(),
            reason: self.reason.clone(),
            consecutive_failures: self.network_failures,
            outage_published: self.outage_published,
        }
    }

    /// The hint the database's read gate consumes.
    pub fn remote_link_hint(&self) -> RemoteLinkHint {
        match self.phase {
            LinkPhase::NoServer => RemoteLinkHint::NoServer,
            LinkPhase::Connecting => RemoteLinkHint::Attempting,
            LinkPhase::Live => RemoteLinkHint::Live,
            LinkPhase::Offline
            | LinkPhase::Backoff
            | LinkPhase::AwaitingCredential(_)
            | LinkPhase::Stopped => RemoteLinkHint::Failed,
        }
    }

    /// The credential epoch the next attempt connects with.
    pub fn credential_epoch(&self) -> u64 {
        self.credential_epoch
    }

    /// Observer events since the last drain, oldest first.
    pub fn drain_events(&mut self) -> Vec<LinkEvent> {
        self.events.drain(..).collect()
    }

    /// Apply one input observed at `now_ms` and return what the host must do.
    pub fn handle(&mut self, now_ms: u64, input: LinkInput) -> Vec<LinkAction> {
        let mut actions = Vec::new();
        match input {
            LinkInput::ServerConfigured(configured) => {
                if configured != self.configured {
                    self.configured = configured;
                    self.restart(&mut actions);
                }
            }
            LinkInput::SetOffline(offline) => {
                self.offline = offline;
                self.restart(&mut actions);
            }
            LinkInput::Admitted(attempt) => {
                if self.attempt == Some(attempt) && self.phase != LinkPhase::Live {
                    self.phase = LinkPhase::Live;
                    self.reset_failures();
                    self.reason = None;
                    if self.auth_unrestored {
                        self.auth_unrestored = false;
                        self.events.push_back(LinkEvent::AuthRestored);
                    }
                }
            }
            LinkInput::Ended(attempt, failure) => {
                if self.attempt == Some(attempt) {
                    self.attempt = None;
                    self.ended(now_ms, failure, &mut actions);
                }
            }
            LinkInput::CredentialUpdated => {
                self.credential_epoch += 1;
                match self.phase {
                    LinkPhase::AwaitingCredential(_) => {
                        self.reset_failures();
                        self.phase = LinkPhase::Connecting;
                        self.connect(&mut actions);
                    }
                    LinkPhase::Connecting | LinkPhase::Live => {
                        self.close(&mut actions);
                        self.phase = LinkPhase::Connecting;
                        self.connect(&mut actions);
                    }
                    // A new credential is a reason to try now rather than at
                    // the end of the current wait.
                    LinkPhase::Backoff => {
                        self.close(&mut actions);
                        self.connect(&mut actions);
                    }
                    LinkPhase::NoServer | LinkPhase::Offline | LinkPhase::Stopped => {}
                }
            }
            LinkInput::TimerFired(token) => {
                if self.timer == Some(token) {
                    self.timer = None;
                    if matches!(self.phase, LinkPhase::Connecting | LinkPhase::Backoff)
                        && self.attempt.is_none()
                    {
                        self.connect(&mut actions);
                    }
                }
            }
        }
        self.publish();
        actions
    }

    /// Start over from the configuration: an explicit reconnect, a disconnect,
    /// or a server added or removed.
    fn restart(&mut self, actions: &mut Vec<LinkAction>) {
        self.close(actions);
        self.reset_failures();
        self.reason = None;
        if !self.configured {
            self.phase = LinkPhase::NoServer;
        } else if self.offline {
            self.phase = LinkPhase::Offline;
        } else {
            self.phase = LinkPhase::Connecting;
            self.connect(actions);
        }
    }

    fn ended(&mut self, now_ms: u64, failure: LinkFailure, actions: &mut Vec<LinkAction>) {
        self.reason = Some(failure.message().to_owned());
        match failure {
            LinkFailure::AuthRejected { reason, .. } => {
                self.cancel_timer(actions);
                self.phase = LinkPhase::AwaitingCredential(reason);
                self.auth_unrestored = true;
                self.events.push_back(LinkEvent::AuthFailed(reason));
            }
            LinkFailure::Fatal(_) => {
                self.cancel_timer(actions);
                self.phase = LinkPhase::Stopped;
            }
            LinkFailure::ServerNotReady(_) if self.phase == LinkPhase::Connecting => {
                let delay = self
                    .policy
                    .not_ready_initial_ms
                    .saturating_mul(1 << self.not_ready_failures.min(30))
                    .min(self.policy.not_ready_max_ms);
                self.not_ready_failures = self.not_ready_failures.saturating_add(1);
                self.arm_timer(delay, actions);
            }
            // A server that is not ready after it once admitted this link is
            // an outage like any other.
            LinkFailure::ServerNotReady(_) | LinkFailure::Network(_) => {
                let started = *self.outage_started_at.get_or_insert(now_ms);
                if !self.outage_published
                    && now_ms.saturating_sub(started) >= self.policy.outage_report_ms
                {
                    self.outage_published = true;
                }
                let cap = if self.outage_published {
                    self.policy.retry_max_ms
                } else {
                    self.policy.retry_unreported_max_ms
                };
                let ceiling = self
                    .policy
                    .retry_initial_ms
                    .saturating_mul(1 << self.network_failures.min(16))
                    .min(cap);
                self.network_failures = self.network_failures.saturating_add(1);
                let delay = self.equal_jitter(ceiling);
                self.phase = LinkPhase::Backoff;
                self.arm_timer(delay, actions);
            }
        }
    }

    fn connect(&mut self, actions: &mut Vec<LinkAction>) {
        self.cancel_timer(actions);
        let attempt = AttemptId(self.mint());
        self.attempt = Some(attempt);
        actions.push(LinkAction::Connect {
            attempt,
            credential_epoch: self.credential_epoch,
        });
    }

    fn close(&mut self, actions: &mut Vec<LinkAction>) {
        self.cancel_timer(actions);
        if let Some(attempt) = self.attempt.take() {
            actions.push(LinkAction::Close(attempt));
        }
    }

    fn arm_timer(&mut self, after_ms: u64, actions: &mut Vec<LinkAction>) {
        self.cancel_timer(actions);
        let token = TimerToken(self.mint());
        self.timer = Some(token);
        actions.push(LinkAction::ArmTimer { token, after_ms });
    }

    fn cancel_timer(&mut self, actions: &mut Vec<LinkAction>) {
        if let Some(token) = self.timer.take() {
            actions.push(LinkAction::CancelTimer(token));
        }
    }

    fn reset_failures(&mut self) {
        self.network_failures = 0;
        self.not_ready_failures = 0;
        self.outage_started_at = None;
        self.outage_published = false;
    }

    fn mint(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// A delay in `[ceiling / 2, ceiling]`, so a fleet of clients does not
    /// reconnect in lockstep after a server restart.
    fn equal_jitter(&mut self, ceiling: u64) -> u64 {
        // splitmix64
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        let half = ceiling / 2;
        half + z % (ceiling - half + 1)
    }

    fn publish(&mut self) {
        let status = self.status();
        if self.published.as_ref() != Some(&status) {
            self.published = Some(status.clone());
            self.events.push_back(LinkEvent::Status(status));
        }
    }
}

#[cfg(test)]
mod tests;

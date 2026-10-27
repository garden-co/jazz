//! The link lifecycle, driven through the machine's own API.
//!
//! These are lower-level than Jazz's usual client tests on purpose: the
//! machine is the one place every host's reconnect and credential behaviour
//! is decided, and it has no IO, so its whole contract is its inputs, actions
//! and events. Each host later proves only that it wires sockets and timers
//! to it.

use super::*;

/// A host that owns one socket slot and one timer, as every real host does.
struct Host {
    link: UpstreamLink,
    now: u64,
    open: Option<(AttemptId, u64)>,
    timer: Option<(TimerToken, u64)>,
    connects: Vec<(AttemptId, u64)>,
    delays: Vec<u64>,
}

impl Host {
    fn new() -> Self {
        Self {
            link: UpstreamLink::new(LinkPolicy::default(), 7),
            now: 0,
            open: None,
            timer: None,
            connects: Vec::new(),
            delays: Vec::new(),
        }
    }

    /// A client with a configured server, whose first attempt is in flight.
    fn configured() -> Self {
        let mut host = Self::new();
        host.send(LinkInput::ServerConfigured(true));
        host
    }

    fn send(&mut self, input: LinkInput) {
        for action in self.link.handle(self.now, input) {
            match action {
                LinkAction::Connect {
                    attempt,
                    credential_epoch,
                } => {
                    assert!(self.open.is_none(), "a second socket while one is open");
                    self.open = Some((attempt, credential_epoch));
                    self.connects.push((attempt, credential_epoch));
                }
                LinkAction::Close(attempt) => {
                    assert_eq!(self.open.map(|(open, _)| open), Some(attempt));
                    self.open = None;
                }
                LinkAction::ArmTimer { token, after_ms } => {
                    assert!(self.timer.is_none(), "a second timer while one is armed");
                    self.timer = Some((token, self.now + after_ms));
                    self.delays.push(after_ms);
                }
                LinkAction::CancelTimer(token) => {
                    assert_eq!(self.timer.map(|(armed, _)| armed), Some(token));
                    self.timer = None;
                }
            }
        }
    }

    fn attempt(&self) -> AttemptId {
        self.open.expect("an attempt in flight").0
    }

    fn admit(&mut self) {
        let attempt = self.attempt();
        self.send(LinkInput::Admitted(attempt));
    }

    /// The current socket ends; the host forgets it as a real one would.
    fn fail(&mut self, failure: LinkFailure) {
        let attempt = self.attempt();
        self.open = None;
        self.send(LinkInput::Ended(attempt, failure));
    }

    fn network_down(&mut self) {
        self.fail(LinkFailure::Network("connection refused".into()));
    }

    fn refuse(&mut self, reason: AuthFailureReason) {
        self.fail(LinkFailure::AuthRejected {
            reason,
            message: format!("token {}", reason.as_str()),
        });
    }

    /// Advance to the armed timer and fire it.
    fn fire(&mut self) {
        let (token, at) = self.timer.take().expect("a timer armed");
        self.now = self.now.max(at);
        self.send(LinkInput::TimerFired(token));
    }

    fn auth_events(&mut self) -> Vec<LinkEvent> {
        self.link
            .drain_events()
            .into_iter()
            .filter(|event| !matches!(event, LinkEvent::Status(_)))
            .collect()
    }
}

/// alice's client connects to a configured server and is admitted.
///
/// ```text
/// alice ──connect──► server ──admitted──► Live
/// ```
#[test]
fn a_configured_server_is_connected_and_admitted() {
    let mut alice = Host::new();
    assert_eq!(alice.link.remote_link_hint(), RemoteLinkHint::NoServer);
    alice.send(LinkInput::ServerConfigured(true));
    assert_eq!(alice.link.phase(), &LinkPhase::Connecting);
    assert_eq!(alice.link.remote_link_hint(), RemoteLinkHint::Attempting);
    assert_eq!(alice.connects, vec![(AttemptId(1), 0)]);

    alice.admit();
    assert_eq!(alice.link.phase(), &LinkPhase::Live);
    assert_eq!(alice.link.remote_link_hint(), RemoteLinkHint::Live);
    let phases: Vec<_> = alice
        .link
        .drain_events()
        .into_iter()
        .map(|event| match event {
            LinkEvent::Status(status) => status.phase,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(
        phases,
        vec![LinkPhase::NoServer, LinkPhase::Connecting, LinkPhase::Live]
    );
}

/// alice opens her client while offline. The first attempt fails on the
/// network, so she backs off, and nothing waits on the server; once it is
/// reachable a retry is admitted and the failure count resets.
#[test]
fn a_client_opened_offline_retries_until_the_server_is_reachable() {
    let mut alice = Host::configured();
    alice.network_down();
    assert_eq!(alice.link.phase(), &LinkPhase::Backoff);
    assert_eq!(alice.link.remote_link_hint(), RemoteLinkHint::Failed);
    assert!(alice.open.is_none());

    alice.fire();
    alice.network_down();
    alice.fire();
    assert_eq!(alice.link.phase(), &LinkPhase::Backoff);
    assert_eq!(alice.link.status().consecutive_failures, 2);
    assert_eq!(
        alice.link.status().reason.as_deref(),
        Some("connection refused")
    );

    alice.admit();
    assert_eq!(alice.link.phase(), &LinkPhase::Live);
    assert_eq!(alice.link.status().consecutive_failures, 0);
    assert_eq!(alice.link.status().reason, None);
}

/// Backoff doubles from 100 ms with equal jitter and stays at most 1 s until
/// an outage is published, so a server that returns within the reporting
/// window is found before readers are told it is down.
#[test]
fn backoff_is_jittered_and_capped_before_an_outage_is_published() {
    let mut alice = Host::configured();
    alice.admit();
    for _ in 0..6 {
        alice.network_down();
        alice.fire();
    }
    let ceilings = [100, 200, 400, 800, 1_000, 1_000];
    assert_eq!(alice.delays.len(), ceilings.len());
    for (delay, ceiling) in alice.delays.iter().zip(ceilings) {
        assert!(
            (ceiling / 2..=ceiling).contains(delay),
            "{delay} outside [{}, {ceiling}]",
            ceiling / 2
        );
    }
    assert!(!alice.link.status().outage_published);
}

/// bob's server has been down for 7.5 s. The next failed attempt publishes
/// the outage, so reads that require the server fail instead of hanging;
/// the retry ceiling rises to 5 s; a reconnect clears the outage.
///
/// ```text
/// t=0     link drops        outage starts
/// t<7.5s  retries fail      not published
/// t≥7.5s  next retry fails  published, cap 5 s
///         retry admitted    cleared
/// ```
#[test]
fn an_outage_is_published_by_a_failure_after_the_reporting_window() {
    let mut bob = Host::configured();
    bob.admit();
    bob.network_down();
    while bob.now < 7_500 {
        assert!(!bob.link.status().outage_published);
        bob.fire();
        bob.network_down();
    }
    assert!(bob.link.status().outage_published);
    assert_eq!(bob.link.remote_link_hint(), RemoteLinkHint::Failed);

    for _ in 0..8 {
        bob.fire();
        bob.network_down();
    }
    assert!(bob.delays.iter().rev().take(4).all(|delay| *delay > 1_000));
    assert!(bob.delays.iter().all(|delay| *delay <= 5_000));

    bob.fire();
    bob.admit();
    assert!(!bob.link.status().outage_published);
}

/// Publication is failure-triggered, never a clock: a retry that is still
/// pending when the window passes does not publish, because the server may
/// already be back and that retry may succeed.
#[test]
fn time_alone_never_publishes_an_outage() {
    let mut bob = Host::configured();
    bob.admit();
    bob.network_down();
    let (token, _) = bob.timer.take().unwrap();
    bob.now = 60_000;
    bob.send(LinkInput::TimerFired(token));
    assert!(!bob.link.status().outage_published);
    bob.admit();
    assert_eq!(bob.link.phase(), &LinkPhase::Live);
}

/// A server that is still starting refuses the first handshake. alice keeps
/// connecting with a short bounded retry, and her reads keep waiting.
#[test]
fn a_starting_server_is_retried_quickly_while_readers_wait() {
    let mut alice = Host::configured();
    for _ in 0..7 {
        alice.fail(LinkFailure::ServerNotReady(
            "catalogue bootstrapping".into(),
        ));
        assert_eq!(alice.link.phase(), &LinkPhase::Connecting);
        assert_eq!(alice.link.remote_link_hint(), RemoteLinkHint::Attempting);
        alice.fire();
    }
    assert_eq!(alice.delays, vec![25, 50, 100, 200, 400, 800, 1_000]);
    alice.admit();
    assert_eq!(alice.link.phase(), &LinkPhase::Live);
}

/// After a link was live, "not ready" is an outage like any other.
#[test]
fn a_server_not_ready_after_admission_backs_off_like_an_outage() {
    let mut alice = Host::configured();
    alice.admit();
    alice.fail(LinkFailure::ServerNotReady("restarting".into()));
    assert_eq!(alice.link.phase(), &LinkPhase::Backoff);
}

/// The server refuses alice's expired token. Her client waits, without a
/// timer, until the app hands it a new token, then connects with it and
/// reports that auth is restored.
///
/// ```text
/// alice ──token v0──► server ──✗ expired──► AwaitingCredential (no timer)
/// app ──new token──► alice ──token v1──► server ──admitted──► AuthRestored
/// ```
#[test]
fn a_refused_credential_waits_for_a_new_one() {
    let mut alice = Host::configured();
    alice.admit();
    alice.network_down();
    alice.fire();
    alice.link.drain_events();
    alice.refuse(AuthFailureReason::Expired);
    assert_eq!(
        alice.link.phase(),
        &LinkPhase::AwaitingCredential(AuthFailureReason::Expired)
    );
    assert_eq!(alice.link.remote_link_hint(), RemoteLinkHint::Failed);
    assert!(
        alice.timer.is_none(),
        "a refused credential is never retried"
    );
    assert_eq!(
        alice.auth_events(),
        vec![LinkEvent::AuthFailed(AuthFailureReason::Expired)]
    );

    alice.now += 3_600_000;
    assert!(alice.timer.is_none());
    assert!(alice.open.is_none());

    alice.send(LinkInput::CredentialUpdated);
    assert_eq!(alice.link.phase(), &LinkPhase::Connecting);
    assert_eq!(alice.connects.last().map(|(_, epoch)| *epoch), Some(1));
    alice.admit();
    assert_eq!(alice.auth_events(), vec![LinkEvent::AuthRestored]);
    assert_eq!(alice.link.status().consecutive_failures, 0);
}

/// The renewed token is refused too: each refusal is reported, and a single
/// restoration follows the credential that is finally admitted.
#[test]
fn every_refusal_is_reported_and_one_restoration_follows() {
    let mut alice = Host::configured();
    alice.link.drain_events();
    alice.refuse(AuthFailureReason::Invalid);
    alice.send(LinkInput::CredentialUpdated);
    alice.refuse(AuthFailureReason::Expired);
    alice.send(LinkInput::CredentialUpdated);
    alice.admit();
    assert_eq!(
        alice.auth_events(),
        vec![
            LinkEvent::AuthFailed(AuthFailureReason::Invalid),
            LinkEvent::AuthFailed(AuthFailureReason::Expired),
            LinkEvent::AuthRestored,
        ]
    );
}

/// The #4296 race: alice's app resumes, a renewal promotes a new token while
/// the old socket's refusal is still on its way. The late refusal names the
/// replaced attempt, so it cannot move the link or report an auth failure.
///
/// ```text
/// attempt 1 (token v0) ── live
/// app ──new token──► close 1, connect 2 (token v1)
/// attempt 1 ──✗ expired (late)──► ignored
/// attempt 2 ──admitted──► Live
/// ```
#[test]
fn a_late_failure_from_a_replaced_attempt_is_ignored() {
    let mut alice = Host::configured();
    alice.admit();
    let old = alice.attempt();
    alice.link.drain_events();

    alice.send(LinkInput::CredentialUpdated);
    let new = alice.attempt();
    assert_ne!(old, new);
    assert_eq!(alice.link.phase(), &LinkPhase::Connecting);

    alice.send(LinkInput::Ended(
        old,
        LinkFailure::AuthRejected {
            reason: AuthFailureReason::Expired,
            message: "token expired".into(),
        },
    ));
    alice.send(LinkInput::Admitted(old));
    assert_eq!(alice.link.phase(), &LinkPhase::Connecting);
    assert_eq!(alice.attempt(), new);

    alice.admit();
    assert_eq!(alice.link.phase(), &LinkPhase::Live);
    assert_eq!(alice.auth_events(), vec![]);
}

/// A new credential during backoff connects at once instead of waiting out
/// the remaining delay.
#[test]
fn a_new_credential_during_backoff_connects_now() {
    let mut alice = Host::configured();
    alice.admit();
    alice.network_down();
    assert!(alice.timer.is_some());
    alice.send(LinkInput::CredentialUpdated);
    assert!(alice.timer.is_none());
    assert_eq!(alice.connects.last().map(|(_, epoch)| *epoch), Some(1));
    assert_eq!(alice.link.phase(), &LinkPhase::Backoff);
}

/// bob disconnects explicitly while backing off: the pending retry is
/// cancelled and stays cancelled, even if its timer fires anyway. Reconnect
/// connects at once with a clean failure count.
#[test]
fn explicit_offline_cancels_retries_and_reconnect_starts_fresh() {
    let mut bob = Host::configured();
    bob.admit();
    bob.network_down();
    let (stale, _) = bob.timer.unwrap();
    bob.send(LinkInput::SetOffline(true));
    assert_eq!(bob.link.phase(), &LinkPhase::Offline);
    assert_eq!(bob.link.remote_link_hint(), RemoteLinkHint::Failed);
    assert!(bob.timer.is_none());

    bob.send(LinkInput::TimerFired(stale));
    assert!(bob.open.is_none());

    bob.send(LinkInput::SetOffline(false));
    assert_eq!(bob.link.phase(), &LinkPhase::Connecting);
    assert_eq!(bob.link.status().consecutive_failures, 0);
    assert!(bob.open.is_some());
}

/// An explicit reconnect is the application asking to try again, so it tries
/// even after a refusal, with the same credential.
#[test]
fn explicit_reconnect_retries_after_a_refusal() {
    let mut alice = Host::configured();
    alice.refuse(AuthFailureReason::Missing);
    alice.send(LinkInput::SetOffline(false));
    assert_eq!(alice.link.phase(), &LinkPhase::Connecting);
    assert_eq!(alice.connects.last().map(|(_, epoch)| *epoch), Some(0));
}

/// A revoked identity waits like any refusal; the application decides
/// whether to recreate its client.
#[test]
fn disabled_and_revoked_wait_like_other_refusals() {
    for reason in [AuthFailureReason::Disabled, AuthFailureReason::Revoked] {
        let mut alice = Host::configured();
        alice.refuse(reason);
        assert_eq!(alice.link.phase(), &LinkPhase::AwaitingCredential(reason));
        assert!(alice.timer.is_none());
    }
}

/// Waits for the server stay pending while the link can still recover on its
/// own or with a refreshed token, and fail once it cannot (#3923).
#[test]
fn only_unrecoverable_phases_are_terminal_for_waits() {
    let mut alice = Host::configured();
    assert!(!alice.link.phase().is_terminal());
    alice.admit();
    alice.network_down();
    assert!(!alice.link.phase().is_terminal(), "a retrying outage");
    alice.fire();
    alice.refuse(AuthFailureReason::Expired);
    assert!(!alice.link.phase().is_terminal(), "a renewable refusal");
    for reason in [AuthFailureReason::Disabled, AuthFailureReason::Revoked] {
        let mut bob = Host::configured();
        bob.refuse(reason);
        assert!(bob.link.phase().is_terminal());
    }
    alice.send(LinkInput::SetOffline(false));
    alice.fail(LinkFailure::Fatal(
        "protocol version 9 is not supported".into(),
    ));
    assert!(alice.link.phase().is_terminal());
}

/// A fatal failure stops the link: no retries, and a new credential does not
/// restart it. Only an explicit reconnect does.
#[test]
fn a_fatal_failure_stops_until_explicit_reconnect() {
    let mut alice = Host::configured();
    alice.fail(LinkFailure::Fatal(
        "protocol version 9 is not supported".into(),
    ));
    assert_eq!(alice.link.phase(), &LinkPhase::Stopped);
    assert_eq!(
        alice.link.status().reason.as_deref(),
        Some("protocol version 9 is not supported")
    );
    assert!(alice.timer.is_none());
    alice.send(LinkInput::CredentialUpdated);
    assert!(alice.open.is_none());
    alice.send(LinkInput::SetOffline(false));
    assert_eq!(alice.link.phase(), &LinkPhase::Connecting);
}

/// Without a server nothing connects, whatever else happens.
#[test]
fn no_server_never_connects() {
    let mut alice = Host::new();
    alice.send(LinkInput::SetOffline(true));
    alice.send(LinkInput::SetOffline(false));
    alice.send(LinkInput::CredentialUpdated);
    assert!(alice.connects.is_empty());
    assert_eq!(alice.link.phase(), &LinkPhase::NoServer);

    alice.send(LinkInput::ServerConfigured(true));
    alice.admit();
    alice.send(LinkInput::ServerConfigured(false));
    assert!(alice.open.is_none());
    assert_eq!(alice.link.remote_link_hint(), RemoteLinkHint::NoServer);
}

#[test]
fn refusal_messages_map_like_the_typescript_client() {
    for (message, reason) in [
        ("JWT expired", AuthFailureReason::Expired),
        ("missing credentials", AuthFailureReason::Missing),
        ("auth mode disabled for app", AuthFailureReason::Disabled),
        ("identity Revoked", AuthFailureReason::Revoked),
        ("bad signature", AuthFailureReason::Invalid),
    ] {
        assert_eq!(AuthFailureReason::from_message(message), reason);
    }
}

/// Random interleavings of everything a host can report, including stale
/// attempts and timers, never break the invariants: one socket and one timer
/// at a time (the host asserts both), fresh attempt ids, no timer while
/// waiting for a credential or stopped, delays within the policy's bounds,
/// and a restoration only after an unrestored refusal.
#[test]
fn random_interleavings_keep_the_invariants() {
    let policy = LinkPolicy::default();
    let mut rng: u64 = 0x5eed;
    let mut next = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    for seed in 0..200 {
        let mut host = Host::new();
        host.link = UpstreamLink::new(policy, seed);
        host.link.drain_events();
        let mut stale_attempts = Vec::new();
        let mut stale_timers = Vec::new();
        let mut unrestored = false;
        for _ in 0..300 {
            host.now += next(3_000);
            if let Some((attempt, _)) = host.open {
                stale_attempts.push(attempt);
            }
            if let Some((token, _)) = host.timer {
                stale_timers.push(token);
            }
            let input = match next(12) {
                0 => LinkInput::ServerConfigured(next(4) != 0),
                1 => LinkInput::SetOffline(next(3) == 0),
                2 => LinkInput::CredentialUpdated,
                3 | 4 if host.open.is_some() => LinkInput::Admitted(host.attempt()),
                5 | 6 if host.open.is_some() => {
                    let attempt = host.attempt();
                    host.open = None;
                    let failure = match next(5) {
                        0 => LinkFailure::ServerNotReady("starting".into()),
                        1 => LinkFailure::AuthRejected {
                            reason: AuthFailureReason::Expired,
                            message: "expired".into(),
                        },
                        2 if next(4) == 0 => LinkFailure::Fatal("fatal".into()),
                        _ => LinkFailure::Network("down".into()),
                    };
                    LinkInput::Ended(attempt, failure)
                }
                7 | 8 if host.timer.is_some() => {
                    let (token, at) = host.timer.take().unwrap();
                    host.now = host.now.max(at);
                    LinkInput::TimerFired(token)
                }
                9 if !stale_attempts.is_empty() => {
                    let attempt = stale_attempts[next(stale_attempts.len() as u64) as usize];
                    if host.open.map(|(open, _)| open) == Some(attempt) {
                        continue;
                    }
                    LinkInput::Ended(attempt, LinkFailure::Network("stale".into()))
                }
                10 if !stale_timers.is_empty() => {
                    let token = stale_timers[next(stale_timers.len() as u64) as usize];
                    if host.timer.map(|(armed, _)| armed) == Some(token) {
                        continue;
                    }
                    LinkInput::TimerFired(token)
                }
                _ => continue,
            };
            let connects_before = host.connects.len();
            host.send(input);
            for pair in host.connects[connects_before.saturating_sub(1)..].windows(2) {
                assert!(pair[0].0 < pair[1].0, "attempt ids are fresh");
            }
            if matches!(
                host.link.phase(),
                LinkPhase::AwaitingCredential(_) | LinkPhase::Stopped
            ) {
                assert!(host.timer.is_none());
            }
            assert!(
                host.delays
                    .last()
                    .is_none_or(|delay| *delay <= policy.retry_max_ms)
            );
            for event in host.link.drain_events() {
                match event {
                    LinkEvent::AuthFailed(_) => unrestored = true,
                    LinkEvent::AuthRestored => {
                        assert!(unrestored, "restored without a refusal");
                        unrestored = false;
                    }
                    LinkEvent::Status(status) => {
                        assert_eq!(status, host.link.status());
                    }
                }
            }
        }
    }
}

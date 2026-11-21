//! The link machine for hosts that reach it through a binding.
//!
//! The browser and Node runtimes drive [`UpstreamLink`] from TypeScript. They
//! exchange one JSON document per step, so the spelling of every input,
//! action and event is defined here once and both bindings forward strings.
//!
//! Input: `{"type": "serverConfigured", "configured": bool}`,
//! `{"type": "setOffline", "offline": bool}`, `{"type": "admitted", "attempt": n}`,
//! `{"type": "ended", "attempt": n, "failure": {"kind": k, "message": s}}` with
//! `k` one of `network`, `refused`, `serverNotReady`, `authRejected`, `fatal`,
//! `reauthenticate`; or `{"kind": "wire", "code", "retry", "message",
//! "admitted", "reason"?}`, a server's `WireError` in its snake_case spelling, which the
//! link classifies itself ([`LinkFailure::from_wire_error`]; a code or retry
//! this build does not know is a refusal, retried),
//! `{"type": "credentialUpdated"}`, `{"type": "timerFired", "token": n}`.
//!
//! Output: `{"actions": [...], "events": [...], "status": {...}}`, where an
//! action is `{"type": "connect", "attempt", "credentialEpoch"}`,
//! `{"type": "close", "attempt"}`, `{"type": "armTimer", "token", "afterMs"}`
//! or `{"type": "cancelTimer", "token"}`, and an event is a `status`, an
//! `authFailed` (`reason`, `message`) or an `authRestored`.
//!
//! The surface is append-only: wasm, napi and the TypeScript runtime ship
//! separately, so a field or variant is added, never renamed or removed.

use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    AttemptId, AuthFailureReason, LinkAction, LinkEvent, LinkFailure, LinkInput, LinkPhase,
    LinkPolicy, LinkStatus, TimerToken, UpstreamLink,
};
use crate::options::RemoteLinkHint;
use crate::wire::{WireError, WireErrorCode, WireErrorReason, WireRetry};

/// An [`UpstreamLink`] with the default policy, stepped by JSON documents.
#[derive(Debug)]
pub struct HostLink {
    link: UpstreamLink,
}

impl HostLink {
    pub fn new(seed: u64) -> Self {
        Self {
            link: UpstreamLink::new(LinkPolicy::default(), seed),
        }
    }

    /// Apply one input observed at `now_ms` and return what the host must do
    /// and what observers learn.
    pub fn handle_json(&mut self, now_ms: f64, input: &str) -> Result<String, String> {
        let input: HostInput =
            serde_json::from_str(input).map_err(|error| format!("invalid link input: {error}"))?;
        let now_ms = if now_ms.is_finite() && now_ms > 0.0 {
            now_ms as u64
        } else {
            0
        };
        let actions = self.link.handle(now_ms, input.into_link_input());
        let events = self.link.drain_events();
        Ok(json!({
            "actions": actions.iter().map(action_json).collect::<Vec<_>>(),
            "events": events.iter().map(event_json).collect::<Vec<_>>(),
            "status": status_json(&self.link.status()),
        })
        .to_string())
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum HostInput {
    ServerConfigured { configured: bool },
    SetOffline { offline: bool },
    Admitted { attempt: u64 },
    Ended { attempt: u64, failure: HostFailure },
    CredentialUpdated,
    TimerFired { token: u64 },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum HostFailure {
    Network {
        message: String,
    },
    Refused {
        message: String,
    },
    ServerNotReady {
        message: String,
    },
    AuthRejected {
        message: String,
    },
    Fatal {
        message: String,
    },
    Reauthenticate {
        message: String,
    },
    Wire {
        code: String,
        retry: String,
        message: String,
        admitted: bool,
        /// The server's [`WireErrorReason`], when it named one.
        #[serde(default)]
        reason: Option<String>,
    },
}

impl HostInput {
    fn into_link_input(self) -> LinkInput {
        match self {
            Self::ServerConfigured { configured } => LinkInput::ServerConfigured(configured),
            Self::SetOffline { offline } => LinkInput::SetOffline(offline),
            Self::Admitted { attempt } => LinkInput::Admitted(AttemptId(attempt)),
            Self::Ended { attempt, failure } => LinkInput::Ended(
                AttemptId(attempt),
                match failure {
                    HostFailure::Network { message } => LinkFailure::Network(message),
                    HostFailure::Refused { message } => LinkFailure::Refused(message),
                    HostFailure::ServerNotReady { message } => LinkFailure::ServerNotReady(message),
                    HostFailure::AuthRejected { message } => LinkFailure::AuthRejected {
                        reason: AuthFailureReason::from_message(&message),
                        message,
                    },
                    HostFailure::Fatal { message } => LinkFailure::Fatal(message),
                    HostFailure::Reauthenticate { message } => LinkFailure::Reauthenticate(message),
                    HostFailure::Wire {
                        code,
                        retry,
                        message,
                        admitted,
                        reason,
                    } => wire_failure(&code, &retry, message, admitted, reason.as_deref()),
                },
            ),
            Self::CredentialUpdated => LinkInput::CredentialUpdated,
            Self::TimerFired { token } => LinkInput::TimerFired(TimerToken(token)),
        }
    }
}

/// Classify a server's error from its wire spelling. A code or retry this
/// build cannot name came from a newer server. Nothing negotiates versions
/// before hello, so it is a refusal the link retries (and reports at once),
/// never a reason to stop for good.
fn wire_failure(
    code: &str,
    retry: &str,
    message: String,
    admitted: bool,
    reason: Option<&str>,
) -> LinkFailure {
    // A reason this build cannot name is left out: the message still says it.
    let reason = reason
        .and_then(|reason| serde_json::from_value::<WireErrorReason>(Value::from(reason)).ok());
    let parsed = serde_json::from_value::<WireErrorCode>(Value::from(code)).and_then(|code| {
        serde_json::from_value::<WireRetry>(Value::from(retry)).map(|retry| (code, retry))
    });
    match parsed {
        Ok((code, retry)) => {
            let mut error = WireError::new(code, retry, message);
            error.reason = reason;
            LinkFailure::from_wire_error(&error, admitted)
        }
        Err(_) => LinkFailure::Refused(format!("{code} ({retry}): {message}")),
    }
}

fn action_json(action: &LinkAction) -> Value {
    match action {
        LinkAction::Connect {
            attempt,
            credential_epoch,
        } => json!({"type": "connect", "attempt": attempt.0, "credentialEpoch": credential_epoch}),
        LinkAction::Close(attempt) => json!({"type": "close", "attempt": attempt.0}),
        LinkAction::ArmTimer { token, after_ms } => {
            json!({"type": "armTimer", "token": token.0, "afterMs": after_ms})
        }
        LinkAction::CancelTimer(token) => json!({"type": "cancelTimer", "token": token.0}),
    }
}

fn event_json(event: &LinkEvent) -> Value {
    match event {
        LinkEvent::Status(status) => {
            let mut value = status_json(status);
            value["type"] = json!("status");
            value
        }
        LinkEvent::AuthFailed { reason, message } => {
            json!({"type": "authFailed", "reason": reason.as_str(), "message": message})
        }
        LinkEvent::AuthRestored => json!({"type": "authRestored"}),
    }
}

fn status_json(status: &LinkStatus) -> Value {
    let (phase, auth_reason) = match &status.phase {
        LinkPhase::NoServer => ("noServer", None),
        LinkPhase::Offline => ("offline", None),
        LinkPhase::Connecting => ("connecting", None),
        LinkPhase::Live => ("live", None),
        LinkPhase::Backoff => ("backoff", None),
        LinkPhase::AwaitingCredential(reason) => ("awaitingCredential", Some(reason.as_str())),
        LinkPhase::Stopped => ("stopped", None),
    };
    let hint = match status.phase.remote_link_hint() {
        RemoteLinkHint::NoServer => "none",
        RemoteLinkHint::Attempting => "attempting",
        RemoteLinkHint::Live => "live",
        RemoteLinkHint::Failed => "failed",
    };
    json!({
        "phase": phase,
        "authReason": auth_reason,
        "reason": status.reason,
        "consecutiveFailures": status.consecutive_failures,
        "outagePublished": status.outage_published,
        "terminal": status.phase.is_terminal(),
        "hint": hint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(link: &mut HostLink, now_ms: f64, input: Value) -> Value {
        serde_json::from_str(&link.handle_json(now_ms, &input.to_string()).unwrap()).unwrap()
    }

    /// A browser runtime configures its server, connects, loses the link and
    /// is told to retry, all in the JSON spelling its binding forwards.
    #[test]
    fn a_host_steps_the_link_in_json() {
        let mut link = HostLink::new(7);
        let out = step(
            &mut link,
            0.0,
            json!({"type": "serverConfigured", "configured": true}),
        );
        assert_eq!(
            out["actions"],
            json!([{"type": "connect", "attempt": 1, "credentialEpoch": 0}])
        );
        assert_eq!(out["status"]["phase"], "connecting");
        assert_eq!(out["status"]["hint"], "attempting");

        let out = step(&mut link, 5.0, json!({"type": "admitted", "attempt": 1}));
        assert_eq!(out["status"]["hint"], "live");
        assert_eq!(out["events"][0]["type"], "status");

        let out = step(
            &mut link,
            10.0,
            json!({"type": "ended", "attempt": 1, "failure": {"kind": "network", "message": "websocket closed"}}),
        );
        assert_eq!(out["status"]["phase"], "backoff");
        assert_eq!(out["status"]["reason"], "websocket closed");
        assert_eq!(out["actions"][0]["type"], "armTimer");
        let after = out["actions"][0]["afterMs"].as_u64().unwrap();
        assert!(
            (50..=100).contains(&after),
            "first retry within [50, 100]: {after}"
        );
    }

    /// A refused credential reports its reason in the TypeScript spelling and
    /// waits for a new one.
    #[test]
    fn a_refused_credential_reports_its_reason() {
        let mut link = HostLink::new(7);
        step(
            &mut link,
            0.0,
            json!({"type": "serverConfigured", "configured": true}),
        );
        let out = step(
            &mut link,
            1.0,
            json!({"type": "ended", "attempt": 1, "failure": {"kind": "authRejected", "message": "token expired"}}),
        );
        assert_eq!(out["actions"], json!([]));
        assert_eq!(out["status"]["phase"], "awaitingCredential");
        assert_eq!(out["status"]["authReason"], "expired");
        assert_eq!(out["status"]["terminal"], false);
        assert!(out["events"].as_array().unwrap().contains(
            &json!({"type": "authFailed", "reason": "expired", "message": "token expired"})
        ));
    }

    /// A browser runtime hands the link the server's error as it arrived; the
    /// link classifies it as it does for native hosts. A code this build does
    /// not know is retried, never a stop.
    #[test]
    fn a_host_reports_a_server_wire_error_for_the_link_to_classify() {
        let ended = |code: &str, retry: &str, admitted: bool| {
            let mut link = HostLink::new(7);
            step(
                &mut link,
                0.0,
                json!({"type": "serverConfigured", "configured": true}),
            );
            step(
                &mut link,
                1.0,
                json!({"type": "ended", "attempt": 1, "failure": {
                    "kind": "wire", "code": code, "retry": retry,
                    "message": "token expired", "admitted": admitted,
                }}),
            )["status"]["phase"]
                .clone()
        };
        // Still starting: the first attempt keeps waiting and retries.
        assert_eq!(ended("not_ready", "later", false), "connecting");
        assert_eq!(ended("not_ready", "later", true), "backoff");
        assert_eq!(ended("auth_failed", "never", false), "awaitingCredential");
        assert_eq!(ended("malformed_frame", "later", false), "stopped");
        assert_eq!(ended("unknown:9", "later", false), "backoff");
        assert_eq!(ended("not_ready", "unknown:7", false), "backoff");
    }

    /// A browser runtime passes the reason a server named along with its
    /// error; one this build cannot name is left out and the message decides.
    #[test]
    fn a_host_passes_the_servers_reason() {
        let reason = |reason: &str, message: &str| {
            let mut link = HostLink::new(7);
            step(
                &mut link,
                0.0,
                json!({"type": "serverConfigured", "configured": true}),
            );
            step(
                &mut link,
                1.0,
                json!({"type": "ended", "attempt": 1, "failure": {
                    "kind": "wire", "code": "auth_failed", "retry": "never",
                    "message": message, "admitted": false, "reason": reason,
                }}),
            )["status"]["authReason"]
                .clone()
        };
        assert_eq!(reason("revoked", "token expired"), "revoked");
        assert_eq!(reason("unknown:9", "token expired"), "expired");
    }

    /// A malformed document is refused instead of guessed at.
    #[test]
    fn malformed_input_is_refused() {
        let mut link = HostLink::new(7);
        assert!(link.handle_json(0.0, r#"{"type": "reconnect"}"#).is_err());
    }
}

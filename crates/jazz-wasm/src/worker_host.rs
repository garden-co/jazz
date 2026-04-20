//! WorkerHost — in-worker Rust entry point.
//!
//! Replaces the dispatch logic of
//! `packages/jazz-tools/src/worker/jazz-worker.ts`.
//!
//! Scaffolding only in this file for now. Later sub-tasks fill in:
//! - The async `run_worker()` entry point (6b)
//! - `handle_init` + outbox drainer (6b)
//! - Peer client registry + sync routing (6c)
//! - Lifecycle / auth / upstream / debug / shutdown dispatch (6d)

use jazz_tools::worker_frame::AuthFailureReason;
use std::collections::HashMap;

/// Build the WebSocket URL for the Rust transport from the init-payload fields.
///
/// Mirrors `httpUrlToWs` in `packages/jazz-tools/src/runtime/url.ts`:
/// - `http://host`   → `ws://host/ws`
/// - `https://host`  → `wss://host/ws`
/// - optional prefix is inserted before `/ws`
/// - `ws://`/`wss://` URLs are passed through; `/ws` appended if absent
/// - prefix with already-`/ws`-suffixed ws URL: strip existing `/ws`, append prefix + `/ws`
pub fn compose_connect_url(server_url: &str, path_prefix: Option<&str>) -> Result<String, String> {
    let base = server_url.trim_end_matches('/');
    let prefix = path_prefix
        .unwrap_or("")
        .trim_start_matches('/')
        .trim_end_matches('/');
    let tail = if prefix.is_empty() {
        "/ws".to_string()
    } else {
        format!("/{prefix}/ws")
    };

    if let Some(rest) = base.strip_prefix("http://") {
        return Ok(format!("ws://{rest}{tail}"));
    }
    if let Some(rest) = base.strip_prefix("https://") {
        return Ok(format!("wss://{rest}{tail}"));
    }
    if base.starts_with("ws://") || base.starts_with("wss://") {
        if !prefix.is_empty() {
            let trimmed = base.strip_suffix("/ws").unwrap_or(base);
            return Ok(format!("{trimmed}{tail}"));
        }
        return Ok(if base.ends_with("/ws") {
            base.to_string()
        } else {
            format!("{base}/ws")
        });
    }
    Err(format!(
        "Invalid server URL \"{server_url}\": expected http://, https://, ws://, or wss://"
    ))
}

/// Merge an incoming JWT into the worker-held auth map.
///
/// - `Some(non_empty)` replaces/sets the `jwt_token` entry.
/// - `Some("")` or `None` removes `jwt_token`.
/// - All other keys (e.g. `admin_secret`) are preserved.
///
/// Mirrors `mergeAuth` in `jazz-worker.ts`.
pub fn merge_auth(current: &mut HashMap<String, String>, incoming_jwt: Option<&str>) {
    match incoming_jwt {
        Some(j) if !j.is_empty() => {
            current.insert("jwt_token".into(), j.to_string());
        }
        _ => {
            current.remove("jwt_token");
        }
    }
}

/// Map a Rust auth-failure reason string to a typed `AuthFailureReason`.
///
/// Mirrors `mapAuthReason` in `packages/jazz-tools/src/runtime/auth-state.ts`.
/// The Rust transport sends the server's error message verbatim; we look for
/// well-known sub-strings and fall back to `Invalid` (== `invalid`).
pub fn map_auth_reason(reason: &str) -> AuthFailureReason {
    let lower = reason.to_lowercase();
    if lower.contains("expired") {
        return AuthFailureReason::Expired;
    }
    if lower.contains("missing") {
        // The TS side has `"missing"`; our enum uses `Unauthorized` for that case.
        // If a richer mapping is needed later, extend AuthFailureReason.
        return AuthFailureReason::Unauthorized;
    }
    if lower.contains("disabled") {
        // Same note as `missing` — treat as Unauthorized until the enum grows.
        return AuthFailureReason::Unauthorized;
    }
    AuthFailureReason::Invalid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_auth_sets_and_clears_jwt() {
        let mut m = HashMap::new();
        m.insert("admin_secret".into(), "s".into());
        merge_auth(&mut m, Some("tok"));
        assert_eq!(m.get("jwt_token"), Some(&"tok".to_string()));
        merge_auth(&mut m, None);
        assert!(m.get("jwt_token").is_none());
        assert_eq!(m.get("admin_secret"), Some(&"s".to_string()));
    }

    #[test]
    fn merge_auth_empty_string_clears_jwt() {
        let mut m = HashMap::new();
        m.insert("jwt_token".into(), "old".into());
        merge_auth(&mut m, Some(""));
        assert!(m.get("jwt_token").is_none());
    }

    #[test]
    fn compose_connect_url_http_to_ws_plain() {
        assert_eq!(
            compose_connect_url("http://host", None).unwrap(),
            "ws://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_https_to_wss_plain() {
        assert_eq!(
            compose_connect_url("https://host", None).unwrap(),
            "wss://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_http_with_path_prefix() {
        assert_eq!(
            compose_connect_url("http://host", Some("/apps/xyz")).unwrap(),
            "ws://host/apps/xyz/ws"
        );
    }

    #[test]
    fn compose_connect_url_https_trims_trailing_slash() {
        assert_eq!(
            compose_connect_url("https://host/", None).unwrap(),
            "wss://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_ws_passthrough_idempotent_suffix() {
        assert_eq!(
            compose_connect_url("ws://host", None).unwrap(),
            "ws://host/ws"
        );
        assert_eq!(
            compose_connect_url("ws://host/ws", None).unwrap(),
            "ws://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_ws_with_prefix_strips_existing_suffix() {
        assert_eq!(
            compose_connect_url("ws://host/ws", Some("apps/a")).unwrap(),
            "ws://host/apps/a/ws"
        );
        assert_eq!(
            compose_connect_url("wss://host", Some("/apps/a/")).unwrap(),
            "wss://host/apps/a/ws"
        );
    }

    #[test]
    fn compose_connect_url_invalid_scheme_errors() {
        assert!(compose_connect_url("file://host", None).is_err());
        assert!(compose_connect_url("host", None).is_err());
    }

    #[test]
    fn map_auth_reason_matches_known_substrings() {
        assert!(matches!(
            map_auth_reason("token expired"),
            AuthFailureReason::Expired
        ));
        assert!(matches!(
            map_auth_reason("missing credentials"),
            AuthFailureReason::Unauthorized
        ));
        assert!(matches!(
            map_auth_reason("account disabled"),
            AuthFailureReason::Unauthorized
        ));
        assert!(matches!(
            map_auth_reason("whatever else"),
            AuthFailureReason::Invalid
        ));
    }
}

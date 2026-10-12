//! Process heap profiles for operators.
//!
//! The allocator is a per-executable choice, so this library never talks to
//! one directly. An executable whose allocator can sample the heap passes a
//! [`HeapProfileDump`] to [`ServerBuilder::with_heap_profiler`] and to
//! [`DiagnosticsConfig`]; everything else answers `501 Not Implemented`.
//! A profiler that is built in but switched off answers `404 Not Found` with
//! [`HeapProfileError::NotEnabled`]'s explanation.
//!
//! The profile is served at `GET /debug/pprof/heap` as a gzipped pprof
//! protobuf of in-use bytes, the format `go tool pprof` and continuous
//! profilers scrape. It is reachable in two ways:
//!
//! - on the public listener, with `X-Jazz-Admin-Secret`;
//! - on the optional diagnostics listener ([`DiagnosticsConfig::listen`]),
//!   with `Authorization: Bearer <DiagnosticsConfig::token>`. That token is a
//!   read-only scrape credential an operator can share across servers
//!   without handing out admin secrets. Only a loopback listener may run
//!   without a token.
//!
//! [`ServerBuilder::with_heap_profiler`]: crate::ServerBuilder::with_heap_profiler

use std::net::SocketAddr;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use jazz::tools::transport_error::ErrorResponse;

use crate::middleware::AuthConfig;
use crate::middleware::auth::validate_admin_secret;

/// Path of the heap profile on both listeners.
pub const HEAP_PROFILE_PATH: &str = "/debug/pprof/heap";

/// Dump the sampled in-use heap as a gzipped pprof protobuf.
///
/// Called on a blocking thread; implementations may take tens of milliseconds.
pub type HeapProfileDump = fn() -> Result<Vec<u8>, HeapProfileError>;

/// Why a [`HeapProfileDump`] produced no profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeapProfileError {
    /// The profiler is built in but was not switched on; the message says how.
    NotEnabled(String),
    /// The dump itself failed.
    Failed(String),
}

/// Process diagnostics the server shell serves next to the app routes.
#[derive(Debug, Clone, Default)]
pub struct DiagnosticsConfig {
    /// Heap profiler of the running executable, when its allocator has one.
    pub heap_profiler: Option<HeapProfileDump>,
    /// Address of the diagnostics listener. `None` disables it.
    pub listen: Option<SocketAddr>,
    /// Bearer token the diagnostics listener requires.
    pub token: Option<String>,
}

impl DiagnosticsConfig {
    /// Reject listener setups that would expose profiles to anyone who can
    /// reach the address.
    pub fn validate(&self) -> Result<(), String> {
        match (self.listen, &self.token) {
            (Some(addr), None) if !addr.ip().is_loopback() => Err(format!(
                "the diagnostics listener on {addr} requires a diagnostics token; \
                 only loopback addresses may run without one"
            )),
            (None, Some(_)) => {
                Err("a diagnostics token was given without a diagnostics listener".to_owned())
            }
            (_, Some(token)) if token.is_empty() => {
                Err("the diagnostics token must not be empty".to_owned())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone)]
struct AdminHeapProfileState {
    heap_profiler: Option<HeapProfileDump>,
    auth_config: AuthConfig,
}

/// Heap profile route for the public listener, gated by the admin secret.
pub(crate) fn admin_router(
    heap_profiler: Option<HeapProfileDump>,
    auth_config: AuthConfig,
) -> Router {
    Router::new()
        .route(HEAP_PROFILE_PATH, get(admin_heap_profile_handler))
        .with_state(AdminHeapProfileState {
            heap_profiler,
            auth_config,
        })
}

#[derive(Clone)]
struct DiagnosticsHeapProfileState {
    heap_profiler: Option<HeapProfileDump>,
    token: Option<String>,
}

/// Router for the diagnostics listener. With a token, every request must
/// carry `Authorization: Bearer <token>`; without one it is open, which
/// [`DiagnosticsConfig::validate`] allows only on loopback.
pub fn diagnostics_router(heap_profiler: Option<HeapProfileDump>, token: Option<String>) -> Router {
    Router::new()
        .route(HEAP_PROFILE_PATH, get(diagnostics_heap_profile_handler))
        .with_state(DiagnosticsHeapProfileState {
            heap_profiler,
            token,
        })
}

async fn admin_heap_profile_handler(
    State(state): State<AdminHeapProfileState>,
    headers: HeaderMap,
) -> Response {
    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|value| value.to_str().ok());
    if let Err((status, message)) = validate_admin_secret(admin_secret, &state.auth_config) {
        return (status, Json(ErrorResponse::unauthorized(message))).into_response();
    }
    heap_profile_response(state.heap_profiler).await
}

async fn diagnostics_heap_profile_handler(
    State(state): State<DiagnosticsHeapProfileState>,
    headers: HeaderMap,
) -> Response {
    if let Some(expected) = &state.token {
        let provided = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if provided != Some(expected.as_str()) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::unauthorized(
                    "Diagnostics token required for this operation",
                )),
            )
                .into_response();
        }
    }
    heap_profile_response(state.heap_profiler).await
}

async fn heap_profile_response(heap_profiler: Option<HeapProfileDump>) -> Response {
    let Some(dump) = heap_profiler else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse::internal(
                "Heap profiling is not supported by this server build yet",
            )),
        )
            .into_response();
    };
    match tokio::task::spawn_blocking(dump).await {
        Ok(Ok(profile)) => (
            [(header::CONTENT_TYPE, "application/octet-stream")],
            profile,
        )
            .into_response(),
        Ok(Err(HeapProfileError::NotEnabled(message))) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(message)),
        )
            .into_response(),
        Ok(Err(HeapProfileError::Failed(message))) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "Heap profile dump failed: {message}"
            ))),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "Heap profile dump panicked: {error}"
            ))),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    //! Router-level tests: the Linux binary always installs a heap profiler,
    //! so the no-profiler and failed-dump branches, and the
    //! diagnostics router in isolation, are only reachable from here. The
    //! end-to-end path is covered by `jazz-cli`'s process test.

    use axum::body::{self, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    const FAKE_PROFILE: &[u8] = b"\x1f\x8bfake-pprof";

    fn fake_dump() -> Result<Vec<u8>, HeapProfileError> {
        Ok(FAKE_PROFILE.to_vec())
    }

    fn failing_dump() -> Result<Vec<u8>, HeapProfileError> {
        Err(HeapProfileError::Failed("heap dump failed".to_owned()))
    }

    fn disabled_dump() -> Result<Vec<u8>, HeapProfileError> {
        Err(HeapProfileError::NotEnabled(
            "Heap profiling is not enabled on this server".to_owned(),
        ))
    }

    fn admin_auth(secret: Option<&str>) -> AuthConfig {
        AuthConfig {
            admin_secret: secret.map(str::to_owned),
            ..Default::default()
        }
    }

    async fn get(router: Router, admin_secret: Option<&str>) -> (StatusCode, Vec<u8>) {
        get_with(
            router,
            admin_secret.map(|secret| ("X-Jazz-Admin-Secret", secret.to_owned())),
        )
        .await
    }

    async fn get_with(router: Router, header: Option<(&str, String)>) -> (StatusCode, Vec<u8>) {
        let mut request = Request::builder().uri(HEAP_PROFILE_PATH);
        if let Some((name, value)) = header {
            request = request.header(name, value);
        }
        let response = router
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, body)
    }

    /// On the public listener only the admin secret opens the heap profile:
    /// mallory, with no or a wrong secret, is refused; alice, the admin, gets it.
    #[tokio::test]
    async fn public_heap_profile_requires_the_admin_secret() {
        let router = || admin_router(Some(fake_dump), admin_auth(Some("admin")));

        assert_eq!(get(router(), None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            get(router(), Some("wrong")).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(router(), Some("admin")).await,
            (StatusCode::OK, FAKE_PROFILE.to_vec())
        );
    }

    /// A server started without an admin secret never serves its heap profile
    /// publicly, whatever secret mallory sends.
    #[tokio::test]
    async fn public_heap_profile_is_closed_without_a_configured_admin_secret() {
        let router = admin_router(Some(fake_dump), admin_auth(None));

        assert_eq!(get(router, Some("anything")).await.0, StatusCode::FORBIDDEN);
    }

    /// The diagnostics listener serves the profile to alice's scraper with
    /// the shared diagnostics token, and refuses mallory, a neighbouring pod
    /// with no or a wrong token, or one presenting the token outside a
    /// bearer header.
    #[tokio::test]
    async fn diagnostics_heap_profile_requires_the_diagnostics_token() {
        let router = || diagnostics_router(Some(fake_dump), Some("scrape".to_owned()));
        let bearer = |token: &str| Some(("authorization", format!("Bearer {token}")));

        assert_eq!(get_with(router(), None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            get_with(router(), bearer("wrong")).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get_with(router(), Some(("authorization", "scrape".to_owned())))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get_with(router(), bearer("scrape")).await,
            (StatusCode::OK, FAKE_PROFILE.to_vec())
        );
    }

    /// Without a token only a loopback listener is allowed, so profiles are
    /// never open to other pods on the network.
    #[test]
    fn open_diagnostics_listeners_are_loopback_only() {
        let config = |listen: &str, token: Option<&str>| DiagnosticsConfig {
            heap_profiler: None,
            listen: Some(listen.parse().unwrap()),
            token: token.map(str::to_owned),
        };

        assert!(config("127.0.0.1:6060", None).validate().is_ok());
        assert!(config("[::1]:6060", None).validate().is_ok());
        assert!(config("0.0.0.0:6060", None).validate().is_err());
        assert!(config("0.0.0.0:6060", Some("")).validate().is_err());
        assert!(config("0.0.0.0:6060", Some("scrape")).validate().is_ok());
        assert!(
            DiagnosticsConfig {
                token: Some("scrape".to_owned()),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    /// An open loopback listener serves the profile without credentials.
    #[tokio::test]
    async fn diagnostics_heap_profile_without_a_token_is_open() {
        assert_eq!(
            get(diagnostics_router(Some(fake_dump), None), None).await,
            (StatusCode::OK, FAKE_PROFILE.to_vec())
        );
    }

    /// Builds whose allocator cannot sample (macOS, embedders) answer with an
    /// explicit "not supported yet" instead of an empty profile.
    #[tokio::test]
    async fn builds_without_a_heap_profiler_say_so() {
        let (status, body) = get(diagnostics_router(None, None), None).await;

        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .contains("not supported by this server build yet")
        );
    }

    /// A profiler that is built in but off says so instead of serving an
    /// empty profile.
    #[tokio::test]
    async fn disabled_heap_profilers_say_so() {
        let (status, body) = get(diagnostics_router(Some(disabled_dump), None), None).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .contains("not enabled on this server")
        );
    }

    /// A failed dump reaches alice as a 500 carrying the allocator's reason.
    #[tokio::test]
    async fn dump_failures_surface_as_internal_errors() {
        let (status, body) = get(diagnostics_router(Some(failing_dump), None), None).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .contains("heap dump failed")
        );
    }
}

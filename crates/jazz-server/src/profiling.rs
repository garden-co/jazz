//! Process heap profiles for operators.
//!
//! The allocator is a per-executable choice, so this library never talks to
//! one directly. An executable whose allocator can sample the heap passes a
//! [`HeapProfileDump`] to [`ServerBuilder::with_heap_profiler`] and to
//! [`DiagnosticsConfig`]; everything else answers `501 Not Implemented`.
//!
//! The profile is served at `GET /debug/pprof/heap` as a gzipped pprof
//! protobuf of in-use bytes, the format `go tool pprof` and continuous
//! profilers scrape. It is reachable in two ways:
//!
//! - on the public listener, with `X-Jazz-Admin-Secret`;
//! - on the optional diagnostics listener ([`DiagnosticsConfig::listen`]),
//!   without credentials. Operators bind that listener to an address only
//!   their own infrastructure can reach.
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
pub type HeapProfileDump = fn() -> Result<Vec<u8>, String>;

/// Process diagnostics the server shell serves next to the app routes.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiagnosticsConfig {
    /// Heap profiler of the running executable, when its allocator has one.
    pub heap_profiler: Option<HeapProfileDump>,
    /// Address of the unauthenticated diagnostics listener. `None` disables it.
    pub listen: Option<SocketAddr>,
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

/// Router for the diagnostics listener. It performs no authentication.
pub fn diagnostics_router(heap_profiler: Option<HeapProfileDump>) -> Router {
    Router::new()
        .route(HEAP_PROFILE_PATH, get(diagnostics_heap_profile_handler))
        .with_state(heap_profiler)
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
    State(heap_profiler): State<Option<HeapProfileDump>>,
) -> Response {
    heap_profile_response(heap_profiler).await
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
        Ok(Err(message)) => (
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
    //! Router-level tests: the Linux binary always installs a working
    //! jemalloc profiler, so the no-profiler and failed-dump branches, and the
    //! diagnostics router in isolation, are only reachable from here. The
    //! end-to-end path is covered by `jazz-cli`'s process test.

    use axum::body::{self, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    const FAKE_PROFILE: &[u8] = b"\x1f\x8bfake-pprof";

    fn fake_dump() -> Result<Vec<u8>, String> {
        Ok(FAKE_PROFILE.to_vec())
    }

    fn failing_dump() -> Result<Vec<u8>, String> {
        Err("prof.dump failed".to_owned())
    }

    fn admin_auth(secret: Option<&str>) -> AuthConfig {
        AuthConfig {
            admin_secret: secret.map(str::to_owned),
            ..Default::default()
        }
    }

    async fn get(router: Router, admin_secret: Option<&str>) -> (StatusCode, Vec<u8>) {
        let mut request = Request::builder().uri(HEAP_PROFILE_PATH);
        if let Some(secret) = admin_secret {
            request = request.header("X-Jazz-Admin-Secret", secret);
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

    /// The diagnostics listener serves the profile to alice's in-cluster
    /// scraper without credentials; the operator decides who can reach it.
    #[tokio::test]
    async fn diagnostics_heap_profile_needs_no_credentials() {
        assert_eq!(
            get(diagnostics_router(Some(fake_dump)), None).await,
            (StatusCode::OK, FAKE_PROFILE.to_vec())
        );
    }

    /// Builds whose allocator cannot sample (macOS, embedders) answer with an
    /// explicit "not supported yet" instead of an empty profile.
    #[tokio::test]
    async fn builds_without_a_heap_profiler_say_so() {
        let (status, body) = get(diagnostics_router(None), None).await;

        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .contains("not supported by this server build yet")
        );
    }

    /// A failed dump reaches alice as a 500 carrying the allocator's reason.
    #[tokio::test]
    async fn dump_failures_surface_as_internal_errors() {
        let (status, body) = get(diagnostics_router(Some(failing_dump)), None).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .contains("prof.dump failed")
        );
    }
}

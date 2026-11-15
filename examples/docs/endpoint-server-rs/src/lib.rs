//! Snippets for the docs: a Rust main backend serving the app's Jazz endpoint
//! at `/jazz`, routes acting as the request's user, and a backend client of a
//! main backend. The docs include them by region; this crate only has to
//! compile.

use todo_server_docs::jazz_app;

/// Stand-in for the app's own sign-in.
pub struct SignedIn {
    pub user_id: String,
    /// When the session stops holding, in Unix seconds.
    pub expires_at: u64,
}

/// Stand-in for the app's own sign-in: the session a cookie names.
pub async fn session_from_cookie(_sid: Option<&str>) -> Option<SignedIn> {
    None
}

/// Serves the app's Jazz endpoint, and the app's own routes, on port 3000.
pub async fn serve(jazz: ::jazz_app::Jazz) -> Result<(), Box<dyn std::error::Error>> {
    // #region backend-serve-rust
    use ::jazz_app::endpoint::{Answer, AuthUser, Endpoint, Request};
    use axum::Router;
    use axum::routing::get;

    // `jazz` is the backend's handle, opened with `Config::backend`.
    let endpoint = Endpoint::new(&jazz, |request: Request| async move {
        // Your app's own sign-in decides who the request's user is.
        Ok(match session_from_cookie(request.cookie("sid")).await {
            Some(s) => Some(Answer::User(AuthUser::new(s.user_id, s.expires_at))),
            // Let device accounts (local-first guests) in; `None` refuses them.
            None => Some(Answer::DeviceAccount),
        })
    })?;

    let app = Router::new()
        .route("/api/my-todos", get(my_todos))
        .with_state(endpoint.clone())
        .merge(endpoint.router()); // everything under /jazz

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;
    endpoint.shutdown().await?;
    jazz.shutdown().await?;
    // #endregion backend-serve-rust
    Ok(())
}

// #region backend-for-user-rust
use ::jazz_app::endpoint::{Endpoint, Request};
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use jazz_app::todos; // the generated bindings

async fn my_todos(
    State(endpoint): State<Endpoint>,
    headers: HeaderMap,
) -> Result<Json<Vec<String>>, StatusCode> {
    let request = headers.iter().fold(
        Request::new("GET", "/api/my-todos"),
        |request, (name, value)| request.header(name.as_str(), value.to_str().unwrap_or_default()),
    );
    // The backend's handle acting as the request's user, under your permissions.
    let as_user = endpoint
        .for_user(&request)
        .await
        .map_err(|error| match error {
            ::jazz_app::Error::SignedOut => StatusCode::UNAUTHORIZED,
            ::jazz_app::Error::NoAccount(_) => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    let todos = as_user
        .all(todos::query())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(todos.into_iter().map(|todo| todo.title).collect()))
}
// #endregion backend-for-user-rust

/// A backend client of a main backend: a machine that connects to the main
/// backend's `/jazz` with its workload's OIDC token, as a scoped machine.
pub async fn backend_client() -> ::jazz_app::Result<()> {
    // #region backend-client-rust
    use ::jazz_app::{AppId, Auth, Config, Storage, TokenReason};
    use jazz_app::todos; // the generated bindings

    // The main backend trusts this workload as a user:
    // `backendClients: [trustWorkload({ issuer, audience, claims, as: (claims) => ({ userId }) })]`.
    let config = Config::new(AppId::from_name("todos"))
        .server("https://app.example/jazz") // the main backend's Jazz endpoint
        .storage(Storage::Memory)
        .user(Auth::bearer(|_reason: TokenReason| async move {
            // The workload's OIDC token, read fresh each time it is asked for.
            Ok(std::fs::read_to_string("/var/run/secrets/jazz/token")
                .ok()
                .map(|token| token.trim().to_owned()))
        }));
    let jazz = jazz_app::open(config).await?;
    let open = jazz
        .all(todos::query().filter(todos::done().eq(false)))
        .await?;
    println!("{} open todos", open.len());
    jazz.shutdown().await?;
    // #endregion backend-client-rust
    Ok(())
}

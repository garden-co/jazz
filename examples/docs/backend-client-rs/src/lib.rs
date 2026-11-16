//! Snippets for the docs: a Rust backend client of a TypeScript main backend.
//! The docs include them by region; this crate only has to compile.

use todo_server_docs::jazz_app;

/// Connects to the main backend's `/jazz` as a scoped machine, reads, and
/// closes.
pub async fn backend_client() -> ::jazz_app::Result<()> {
    // #region backend-client-connect-rust
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
    // #endregion backend-client-connect-rust
    Ok(())
}

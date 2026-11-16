//! Snippets for the docs: a Rust backend client of a TypeScript main backend.
//! The docs include them by region; this crate only has to compile.

use todo_server_docs::jazz_app;

/// Connects to the main backend's `/jazz` as a scoped machine, reads, and
/// closes.
pub async fn backend_client() -> ::jazz_app_support::Result<()> {
    // #region backend-client-connect-rust
    use ::jazz_app_support::{AppFiles, AppId, CLOSE_TIMEOUT, Config, Jazz, Storage, TokenReason};
    use jazz_app::todos; // the generated bindings

    // The main backend trusts this workload in `backendClients`, with
    // `trustWorkload({ issuer, audience, claims, as })`.
    let app_id = std::env::var("JAZZ_APP_ID").unwrap_or_default(); // the main backend's `jazz.appId`
    let config = Config::new(
        AppId::from_string(&app_id)
            .map_err(|_| ::jazz_app_support::Error::Config("JAZZ_APP_ID".into()))?,
    )
    .server("https://app.example/jazz") // the main backend's Jazz endpoint
    .storage(Storage::Memory)
    .credential(|_reason: TokenReason| async move {
        // The workload's OIDC token, read fresh each time it is asked for.
        std::fs::read_to_string("/var/run/secrets/jazz/token")
            .map(|token| Some(token.trim().to_owned()))
            .map_err(|e| {
                ::jazz_app_support::Error::Config(format!("reading the workload token: {e}"))
            })
    });
    // A backend client opens with the app's permissions too, as TypeScript's
    // `createJazzClient` does: the generated bindings embed only the schema.
    let files = AppFiles::new(jazz_app::APP, None)
        // your app's jazz.permissions.json
        .with_permissions(include_bytes!("../../todo-server-rs/jazz.permissions.json").as_slice());
    let jazz = Jazz::open(files, config).await?;
    let open = jazz
        .all(todos::query().filter(todos::done().eq(false)))
        .await?;
    println!("{} open todos", open.len());
    // Waits up to 10 seconds for queued writes to reach the main backend.
    jazz.close(CLOSE_TIMEOUT).await?;
    // #endregion backend-client-connect-rust
    Ok(())
}

/// A trusted backend client: what it is trusted as, acting for a user, and
/// closing once its writes have arrived.
pub async fn trusted(job: ::jazz_app_support::Jazz) -> ::jazz_app_support::Result<()> {
    // #region backend-client-trusted-rust
    use ::jazz_app_support::BackendClientKind;

    // `job` is opened with `Config::credential`, as above. The main backend
    // decides what the credential is trusted as.
    match job.backend_client() {
        // `as: "backend"`: it syncs as the system, and may act for any user.
        Some(BackendClientKind::Trusted) => println!("a trusted machine"),
        // `as: (claims) => ({ userId })`: it acts as that one user.
        Some(BackendClientKind::Scoped) => println!("a scoped machine"),
        None => println!("not a backend client"),
    }
    // #endregion backend-client-trusted-rust

    // #region backend-client-for-user-rust
    use ::jazz_app_support::Error;
    use jazz_app::NewTodo;

    // A trusted client acting for one of the app's users: its reads and
    // writes carry their identity, under their permissions.
    match job
        .for_user("user_123", serde_json::json!({ "org": "acme" }))
        .await
    {
        Ok(as_user) => {
            as_user.insert(NewTodo::new("Follow up", false)).await?;
        }
        // The user has never signed in: no account is created for them.
        Err(Error::NoAccount(user)) => println!("{user} has no account yet"),
        Err(error) => return Err(error),
    }
    // #endregion backend-client-for-user-rust

    // #region backend-client-close-rust
    use ::jazz_app_support::CLOSE_TIMEOUT;

    // Waits up to 10 seconds for queued writes to reach the server, then
    // stops the client. With `Storage::Memory`, writes still queued are lost.
    match job.close(CLOSE_TIMEOUT).await {
        Err(Error::Unsynced { pending }) => eprintln!("{pending} writes were lost"),
        closed => closed?,
    }
    // #endregion backend-client-close-rust
    Ok(())
}

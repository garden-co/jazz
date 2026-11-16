//! A Rust backend client of a TS main backend, as a scoped machine: signs in
//! at the main backend's /jazz with a workload's OIDC token (env TOKEN),
//! writes a todo as its user and reads it back from the server.
mod jazz_app {
    include!(concat!(env!("OUT_DIR"), "/jazz_app.rs"));
}

use std::time::Duration;

use ::jazz_app::{AppId, Auth, Config, Link, ReadTier, SessionState, Storage, TokenReason};
use futures::StreamExt;
use jazz_app::{NewTodo, todos};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = std::env::var("SERVER_URL")?;
    let app_id = AppId::from_string(&std::env::var("APP_ID")?).map_err(|e| format!("{e:?}"))?;
    let token = std::env::var("TOKEN")?;
    let config = Config::new(app_id)
        .server(&server)
        .storage(Storage::Memory)
        .user(Auth::bearer(move |reason: TokenReason| {
            let token = token.clone();
            async move {
                eprintln!("token asked: {reason:?}");
                Ok(Some(token))
            }
        }));
    let jazz = jazz_app::open(config).await?;
    let session = jazz.session().expect("an end user's session").clone();
    let SessionState::SignedIn { user } = session.state() else {
        return Err(format!("not signed in: {:?}", session.state()).into());
    };
    println!("SIGNED_IN subject={} account={} kind={:?}", user.subject, user.account.0, user.kind);

    let mut changes = jazz.sync_changes();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(status) = changes.next().await {
            if status.link == Link::Connected {
                return;
            }
        }
    })
    .await
    .map_err(|_| "never connected")?;
    println!("CONNECTED");

    let owner = ::jazz_app::ObjectId::from_uuid(user.account.0);
    let written = jazz.insert(NewTodo::new("from rust", false, owner)).await?;
    println!("INSERTED {:?}", written.id);

    let rows = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let rows = jazz.all_with(todos::query(), ReadTier::Remote).await.unwrap();
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| "the row never reached the server")?;
    println!("REMOTE_ROWS {}", rows.len());
    jazz.shutdown().await?;
    Ok(())
}

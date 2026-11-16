//! A Rust trusted backend client of a TS main backend: opens with
//! Config::credential (env TOKEN), writes as the system, acts for a user
//! (env ALICE_ACCOUNT is her account, which the TS side created), and closes.
mod jazz_app {
    include!(concat!(env!("OUT_DIR"), "/jazz_app.rs"));
}

use std::time::Duration;

use ::jazz_app::{
    AppId, BackendClientKind, CLOSE_TIMEOUT, Config, Error, Link, ObjectId, Storage, TokenReason,
};
use futures::StreamExt;
use jazz_app::NewTodo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = std::env::var("SERVER_URL")?;
    let app_id = AppId::from_string(&std::env::var("APP_ID")?).map_err(|e| format!("{e:?}"))?;
    let token = std::env::var("TOKEN")?;
    let config = Config::new(app_id)
        .server(&server)
        .storage(Storage::Memory)
        .credential(move |reason: TokenReason| {
            let token = token.clone();
            async move {
                eprintln!("credential asked: {reason:?}");
                Ok(Some(token))
            }
        });
    let job = jazz_app::open(config).await?;
    println!("KIND {:?}", job.backend_client());
    if job.backend_client() != Some(BackendClientKind::Trusted) {
        return Err("not trusted".into());
    }
    let mut changes = job.sync_changes();
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

    // As the system: any owner, permissions not checked.
    let system = job
        .insert(NewTodo::new("from the system", false, ObjectId::new()))
        .await?;
    println!("SYSTEM_INSERTED {:?}", system.id);

    // A user with no account.
    match job.for_user("nobody", serde_json::Value::Null).await {
        Err(Error::NoAccount(user)) => println!("NO_ACCOUNT {user}"),
        Err(other) => println!("NOBODY_OTHER {other}"),
        Ok(_) => println!("NOBODY_OPENED"),
    }

    if std::env::var_os("SKIP_ALICE").is_none() {
    // Alice, who has an account.
    let alice: uuid::Uuid = std::env::var("ALICE_ACCOUNT")?.parse()?;
    let as_alice = job.for_user("alice", serde_json::json!({})).await?;
    let mine = as_alice
        .insert(NewTodo::new("for alice", false, ObjectId::from_uuid(alice)))
        .await?;
    println!("ALICE_INSERTED {:?}", mine.id);
    // Someone else's row, as Alice: her permissions refuse it at the core.
    if std::env::var_os("SKIP_FOREIGN").is_none() {
    let theirs = as_alice
        .insert(NewTodo::new("not alice's", false, ObjectId::new()))
        .await;
    println!("ALICE_FOREIGN_LOCAL {}", theirs.is_ok());
    }

    }
    if std::env::var_os("WAIT_FIRST").is_some() {
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    let started = std::time::Instant::now();
    let closed = job.close(CLOSE_TIMEOUT).await;
    println!("CLOSE_TOOK {:?}", started.elapsed());
    match closed {
        Ok(()) => println!("CLOSED ok"),
        Err(Error::Unsynced { pending }) => println!("CLOSED unsynced {pending}"),
        Err(other) => println!("CLOSED err {other}"),
    }
    Ok(())
}


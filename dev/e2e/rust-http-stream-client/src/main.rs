//! A Rust trusted backend client of a TS main backend that can't upgrade
//! sockets: it syncs over the HTTP stream. Opens with `Config::credential`
//! (env TOKEN), writes as the system, stays idle for IDLE_SECS (past the
//! socket's 45 s silence deadline), writes again, and closes.
mod jazz_app {
    include!(concat!(env!("OUT_DIR"), "/jazz_app.rs"));
}

use std::time::Duration;

use ::jazz_app::{
    AppFiles, AppId, CLOSE_TIMEOUT, Config, Jazz, Link, ObjectId, Storage, TokenReason,
};
use futures::StreamExt;
use jazz_app::NewTodo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = std::env::var("SERVER_URL")?;
    let app_id = AppId::from_string(&std::env::var("APP_ID")?).map_err(|e| format!("{e:?}"))?;
    let token = std::env::var("TOKEN")?;
    let idle: u64 = std::env::var("IDLE_SECS")
        .ok()
        .and_then(|secs| secs.parse().ok())
        .unwrap_or(0);
    let config = Config::new(app_id)
        .server(&server)
        .storage(Storage::Memory)
        .credential(move |_: TokenReason| {
            let token = token.clone();
            async move { Ok(Some(token)) }
        });
    // A backend client needs the app's permissions, as createJazzClient does.
    let files = AppFiles::new(jazz_app::APP, None)
        .with_permissions(include_bytes!("../jazz.permissions.json").to_vec());
    let job = Jazz::open(files, config).await?;
    println!("KIND {:?}", job.backend_client());
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

    job.insert(NewTodo::new("before idle", false, ObjectId::new()))
        .await?;
    println!("INSERTED before idle");

    // Disconnects while idle show up here.
    let watch = tokio::spawn(async move {
        let mut drops = 0;
        while let Some(status) = changes.next().await {
            if status.link != Link::Connected {
                drops += 1;
                println!("LINK {:?}", status.link);
            }
        }
        drops
    });
    tokio::time::sleep(Duration::from_secs(idle)).await;
    println!("IDLE_DONE {idle}");

    job.insert(NewTodo::new("after idle", false, ObjectId::new()))
        .await?;
    println!("INSERTED after idle");
    match job.close(CLOSE_TIMEOUT).await {
        Ok(()) => println!("CLOSED ok"),
        Err(other) => println!("CLOSED err {other}"),
    }
    watch.abort();
    Ok(())
}

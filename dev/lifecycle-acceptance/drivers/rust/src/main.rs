//! The Rust lifecycle driver: drivers/PROTOCOL.md on jazz-app-support's public API.
#[allow(dead_code)]
mod jazz_app;

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};

use jazz_app::{
    AppFiles, AppId, Auth, Config, DeviceKey, DurabilityTier, Initial, Jazz, JazzSession, NewNote,
    ObjectId, ReadTier, SessionConfig, Storage,
};
use serde_json::{Value, json};
use tokio::sync::oneshot;

type Token = Arc<Mutex<String>>;
type Failure = (String, String);

#[derive(Default)]
struct State {
    clients: Mutex<HashMap<String, Jazz>>,
    sessions: Mutex<HashMap<String, (JazzSession, Token)>>,
    running: Mutex<HashMap<u64, oneshot::Sender<()>>>,
}

/// The variant's name, read from Debug: variants added later need no change here.
fn failed(error: jazz_app::Error) -> Failure {
    let debug = format!("{error:?}");
    let end = debug.find(['(', ' ', '{']).unwrap_or(debug.len());
    (debug[..end].to_owned(), error.to_string())
}

fn bad(message: impl std::fmt::Display) -> Failure {
    ("driver.bad".to_owned(), message.to_string())
}

fn text(request: &Value, name: &str) -> String {
    request[name].as_str().unwrap_or_default().to_owned()
}

fn provider(token: &Token) -> impl jazz_app::TokenProvider + 'static + use<> {
    let token = token.clone();
    move |_: jazz_app::TokenReason| {
        let token = token.lock().unwrap().clone();
        async move { Ok((!token.is_empty()).then_some(token)) }
    }
}

async fn perform(state: &State, request: &Value) -> Result<Value, Failure> {
    let (op, h) = (text(request, "op"), text(request, "h"));
    let client = || state.clients.lock().unwrap().get(&h).cloned().ok_or_else(|| bad("no handle"));
    let session = || state.sessions.lock().unwrap().get(&h).cloned().ok_or_else(|| bad("no session"));
    let app = || AppId::from_string(&text(request, "app")).map_err(|_| bad("bad app id"));
    match op.as_str() {
        "genkey" => return Ok(json!(DeviceKey::generate().map_err(failed)?.to_string())),
        "open" => {
            let auth = match request["key"].as_str() {
                Some(key) => Auth::Device(key.parse().map_err(failed)?),
                None => Auth::bearer(provider(&Arc::new(Mutex::new(text(request, "token"))))),
            };
            let storage = match text(request, "storage").as_str() {
                "memory" => Storage::Memory,
                _ => Storage::Dir(text(request, "dir").into()),
            };
            let config = if request["local"].as_bool() == Some(true) {
                Config::new(app()?).storage(storage)
            } else {
                Config::new(app()?).server(text(request, "url")).storage(storage).user(auth)
            };
            let jazz = jazz_app::open(config).await.map_err(failed)?;
            state.clients.lock().unwrap().insert(h.clone(), jazz);
        }
        "account" => return Ok(json!(client()?.account().map(|account| account.0.to_string()))),
        "write" => {
            let owner = uuid::Uuid::parse_str(&text(request, "owner")).map_err(bad)?;
            let new = NewNote::new(text(request, "title"), ObjectId::from_uuid(owner));
            let jazz = match text(request, "tier").as_str() {
                "global" => client()?.durable(DurabilityTier::GlobalServer),
                _ => client()?,
            };
            return Ok(json!(jazz.insert(new).await.map_err(failed)?.id.to_string()));
        }
        "read" => {
            let tier = match text(request, "tier").as_str() {
                "remote" => ReadTier::Remote,
                _ => ReadTier::LocalOnly,
            };
            let rows = client()?.all_with(jazz_app::notes::query(), tier).await.map_err(failed)?;
            let mut titles: Vec<String> = rows.into_iter().map(|note| note.title).collect();
            titles.sort();
            return Ok(json!(titles));
        }
        "sync" => client()?.wait_for_sync().await.map_err(failed)?,
        "close" => match request["ms"].as_u64() {
            Some(ms) => client()?.close(std::time::Duration::from_millis(ms)).await.map_err(failed)?,
            None => client()?.shutdown().await.map_err(failed)?,
        },
        "delete" => client()?.delete_client_storage().await.map_err(failed)?,
        "disconnect" => client()?.disconnect().await.map_err(failed)?,
        "reconnect" => client()?.reconnect().await.map_err(failed)?,
        "signout" => client()?.sign_out().await.map_err(failed)?,
        "sopen" => {
            let token: Token = Arc::new(Mutex::new(text(request, "token")));
            let config = SessionConfig::new(app()?)
                .server(text(request, "url"))
                .storage(Storage::Dir(text(request, "dir").into()))
                .initial(match text(request, "start").as_str() {
                    "device" => Initial::Device,
                    "signed-out" => Initial::SignedOut,
                    _ => Initial::Backend,
                })
                .bearer(provider(&token));
            let files = AppFiles::new(jazz_app::APP, None);
            let opened = JazzSession::open(files, config).await.map_err(failed)?;
            state.sessions.lock().unwrap().insert(h.clone(), (opened, token));
        }
        "stoken" => *session()?.1.lock().unwrap() = text(request, "token"),
        "ssnap" => {
            let snapshot = session()?.0.snapshot();
            return Ok(json!({
                "status": snapshot.status.to_string(),
                "clientEpoch": snapshot.client_epoch,
                "error": snapshot.error.map(|error| json!({"kind": error.kind, "message": error.message})),
            }));
        }
        "sjazz" => {
            let jazz = session()?.0.jazz().map_err(failed)?;
            state.clients.lock().unwrap().insert(text(request, "as"), jazz);
        }
        "screate" => session()?.0.create_device_account().await.map_err(failed)?,
        "sreopen" => session()?.0.reopen_device_account().await.map_err(failed)?,
        "srefresh" => session()?.0.refresh().await.map_err(failed)?,
        "ssignout" => session()?.0.sign_out().await.map_err(failed)?,
        "sclose" => session()?.0.close().await.map_err(failed)?,
        "cancel" => {
            let target = request["target"].as_u64().unwrap_or_default();
            if let Some(cancel) = state.running.lock().unwrap().remove(&target) {
                let _ = cancel.send(());
            }
        }
        _ => return Err(("driver.unsupported".to_owned(), op.clone())),
    }
    Ok(Value::Null)
}

fn answer(line: Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

#[tokio::main]
async fn main() {
    let state = Arc::new(State::default());
    let (lines, mut requests) = tokio::sync::mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    answer(json!({"ready": true, "pid": std::process::id()}));
    while let Some(line) = requests.recv().await {
        let Ok(request) = serde_json::from_str::<Value>(&line) else { continue };
        let id = request["id"].as_u64().unwrap_or_default();
        let (cancel, cancelled) = oneshot::channel();
        state.running.lock().unwrap().insert(id, cancel);
        let state = state.clone();
        tokio::spawn(async move {
            // Rust cancels by dropping the future.
            let result = tokio::select! {
                result = perform(&state, &request) => result,
                Ok(()) = cancelled => Err(("driver.cancelled".to_owned(), "future dropped".to_owned())),
            };
            state.running.lock().unwrap().remove(&id);
            answer(match result {
                Ok(value) => json!({"id": id, "ok": true, "v": value}),
                Err((kind, msg)) => json!({"id": id, "ok": false, "kind": kind, "msg": msg}),
            });
        });
    }
    // End of input: exit as a process does, closing nothing.
    std::process::exit(0);
}

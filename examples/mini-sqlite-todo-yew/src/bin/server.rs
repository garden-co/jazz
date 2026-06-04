#[cfg(not(target_arch = "wasm32"))]
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
#[cfg(not(target_arch = "wasm32"))]
use futures_util::{SinkExt, StreamExt};
#[cfg(not(target_arch = "wasm32"))]
use mini_jazz_sqlite::{
    connection::UpstreamConnectionManager, protocol::ClientMessage, Runtime, Storage,
};
#[cfg(not(target_arch = "wasm32"))]
use mini_sqlite_todo_yew::{
    native_sync::{decode_client_frame, encode_server_frame},
    todo_schema::todo_schema,
};
#[cfg(not(target_arch = "wasm32"))]
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::broadcast;

#[cfg(not(target_arch = "wasm32"))]
const SUBSCRIPTION_REFRESH_DEBOUNCE: Duration = Duration::from_millis(50);

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct AppState {
    runtime: Arc<Mutex<Runtime>>,
    changes: broadcast::Sender<u64>,
    next_connection_id: Arc<AtomicU64>,
    user: String,
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("MINI_SQLITE_TODO_SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8787".to_owned())
        .parse::<SocketAddr>()?;
    let db_path = std::env::var("MINI_SQLITE_TODO_SERVER_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("mini-sqlite-todo-yew-server.sqlite3"));
    let user = std::env::var("MINI_SQLITE_TODO_USER").unwrap_or_else(|_| "alice".to_owned());
    let runtime = Runtime::open_with_schema(
        Storage::File(db_path.clone()),
        "mini-sqlite-todo-yew-native",
        &user,
        todo_schema(),
    )?;
    let state = AppState {
        runtime: Arc::new(Mutex::new(runtime)),
        changes: broadcast::channel(64).0,
        next_connection_id: Arc::new(AtomicU64::new(1)),
        user,
    };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/sync", get(sync_websocket))
        .with_state(state);

    eprintln!("mini-sqlite-todo-yew sync server listening on ws://{addr}/sync");
    eprintln!("database: {}", db_path.display());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn sync_websocket(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[cfg(not(target_arch = "wasm32"))]
async fn handle_socket(socket: WebSocket, state: AppState) {
    let connection_id = state.next_connection_id.fetch_add(1, Ordering::Relaxed);
    let (schema_fingerprint, policy_fingerprint) = {
        let Ok(runtime) = state.runtime.lock() else {
            return;
        };
        (
            runtime.local_schema_fingerprint(),
            runtime.local_policy_fingerprint(),
        )
    };
    let mut upstream = UpstreamConnectionManager::new_authenticated(
        "mini-sqlite-todo-yew-native-session",
        "mini-sqlite-todo-yew-native",
        schema_fingerprint,
        policy_fingerprint,
        state.user.clone(),
    );
    let (mut sender, mut receiver) = socket.split();
    let mut changes = state.changes.subscribe();

    loop {
        tokio::select! {
            message = receiver.next() => {
                let Some(message) = message else {
                    break;
                };
                let Ok(message) = message else {
                    break;
                };
                let Message::Text(encoded) = message else {
                    if matches!(message, Message::Close(_)) {
                        break;
                    }
                    continue;
                };
                let frame = match decode_client_frame(&encoded) {
                    Ok(frame) => frame,
                    Err(error) => {
                        eprintln!("invalid native sync frame: {error}");
                        break;
                    }
                };
                let should_notify = frame.client_messages.iter().any(|message| {
                    matches!(message, ClientMessage::UploadTx { .. })
                });
                let server_messages = {
                    let Ok(mut runtime) = state.runtime.lock() else {
                        break;
                    };
                    match upstream.receive(&mut runtime, frame.client_messages) {
                        Ok(server_messages) => server_messages,
                        Err(error) => {
                            eprintln!("native sync protocol error: {error}");
                            break;
                        }
                    }
                };
                if !server_messages.is_empty() && send_server_messages(&mut sender, server_messages).await.is_err() {
                    break;
                }
                if should_notify {
                    let _ = state.changes.send(connection_id);
                }
            }
            change = changes.recv() => {
                match change {
                    Ok(origin_connection_id) if origin_connection_id == connection_id => {
                        continue;
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                        tokio::time::sleep(SUBSCRIPTION_REFRESH_DEBOUNCE).await;
                        while changes.try_recv().is_ok() {}
                        let server_messages = {
                            let Ok(runtime) = state.runtime.lock() else {
                                break;
                            };
                            match upstream.refresh_active_subscriptions(&runtime) {
                                Ok(server_messages) => server_messages,
                                Err(error) => {
                                    eprintln!("native sync protocol error: {error}");
                                    break;
                                }
                            }
                        };
                        if !server_messages.is_empty() && send_server_messages(&mut sender, server_messages).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn send_server_messages(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    server_messages: Vec<mini_jazz_sqlite::protocol::ServerMessage>,
) -> Result<(), ()> {
    let encoded = encode_server_frame(server_messages).map_err(|_| ())?;
    sender.send(Message::Text(encoded)).await.map_err(|_| ())
}

#[cfg(target_arch = "wasm32")]
fn main() {}

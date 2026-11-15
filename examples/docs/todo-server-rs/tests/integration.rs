//! The todo API end to end: the real router over the app opened with its
//! generated bindings, locally and as a backend of a real Jazz server.

use std::time::Duration;

use ::jazz_app::{AppId, Backend, Config, Jazz, ReadTier, Storage};
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jazz_server::JazzServer;
use tempfile::TempDir;
use todo_server_docs::{
    CreateTodoRequest, Todo, UpdateTodoRequest, jazz_app, router, router_until,
};
use tower::ServiceExt;

async fn local(data: &TempDir) -> Jazz {
    jazz_app::open(
        Config::new(AppId::from_name("test-todos"))
            .storage(Storage::Dir(data.path().to_path_buf())),
    )
    .await
    .expect("open the app locally")
}

async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<String>,
) -> (StatusCode, Vec<u8>) {
    let request = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(body)),
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    (
        status,
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
}

fn json<T: serde::de::DeserializeOwned>(body: &[u8]) -> T {
    serde_json::from_slice(body).unwrap()
}

#[tokio::test]
async fn health_check() {
    let data = TempDir::new().unwrap();
    let app = router(local(&data).await);
    let (status, _) = call(&app, "GET", "/health", None).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn crud() {
    let data = TempDir::new().unwrap();
    let app = router(local(&data).await);

    let (status, body) = call(&app, "GET", "/todos", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json::<Vec<Todo>>(&body).is_empty());

    let create = CreateTodoRequest {
        title: "Buy milk".into(),
        description: None,
    };
    let (status, body) = call(
        &app,
        "POST",
        "/todos",
        Some(serde_json::to_string(&create).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let created: Todo = json(&body);
    assert_eq!(created.title, "Buy milk");
    assert!(!created.done);
    assert_eq!(created.description, None);

    let (_, body) = call(&app, "GET", "/todos", None).await;
    assert_eq!(json::<Vec<Todo>>(&body), vec![created.clone()]);

    let update = UpdateTodoRequest {
        done: Some(true),
        description: Some("semi-skimmed".into()),
        ..Default::default()
    };
    let uri = format!("/todos/{}", created.id);
    let (status, body) = call(
        &app,
        "PUT",
        &uri,
        Some(serde_json::to_string(&update).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let updated: Todo = json(&body);
    assert!(updated.done);
    assert_eq!(updated.description.as_deref(), Some("semi-skimmed"));

    let (status, body) = call(&app, "GET", &uri, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json::<Todo>(&body), updated);

    let (status, _) = call(&app, "DELETE", &uri, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(&app, "GET", &uri, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(&app, "DELETE", &uri, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(&app, "PUT", &uri, Some("{}".into())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn todos_persist_across_restarts() {
    let data = TempDir::new().unwrap();
    let jazz = local(&data).await;
    let app = router(jazz.clone());
    let create = CreateTodoRequest {
        title: "Persist me".into(),
        description: None,
    };
    let (_, body) = call(
        &app,
        "POST",
        "/todos",
        Some(serde_json::to_string(&create).unwrap()),
    )
    .await;
    let created: Todo = json(&body);
    jazz.shutdown().await.unwrap();
    drop(app);

    let app = router(local(&data).await);
    let (_, body) = call(&app, "GET", "/todos", None).await;
    assert_eq!(json::<Vec<Todo>>(&body), vec![created]);
}

#[tokio::test]
async fn live_todos_stream_every_change() {
    use futures_util::StreamExt;
    use reqwest_eventsource::{Event, EventSource};

    let data = TempDir::new().unwrap();
    let app = router(local(&data).await);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut events = EventSource::get(format!("{base}/todos/live"));
    async fn next(events: &mut EventSource) -> Vec<Todo> {
        loop {
            match tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("an event")
            {
                Some(Ok(Event::Message(message))) => {
                    return serde_json::from_str(&message.data).unwrap();
                }
                Some(Ok(Event::Open)) => continue,
                other => panic!("the stream ended: {other:?}"),
            }
        }
    }
    assert!(next(&mut events).await.is_empty());

    let http = reqwest::Client::new();
    let created: Todo = http
        .post(format!("{base}/todos"))
        .json(&CreateTodoRequest {
            title: "SSE".into(),
            description: None,
        })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, vec![created.clone()]);

    http.put(format!("{base}/todos/{}", created.id))
        .json(&UpdateTodoRequest {
            done: Some(true),
            ..Default::default()
        })
        .send()
        .await
        .unwrap();
    let list = next(&mut events).await;
    assert_eq!(list.len(), 1);
    assert!(list[0].done);

    http.delete(format!("{base}/todos/{}", created.id))
        .send()
        .await
        .unwrap();
    assert!(next(&mut events).await.is_empty());

    events.close();
    server.abort();
}

/// As the app's backend: the service publishes the app's bundle, acts as
/// one user, and what it writes reaches another user's backend (with a
/// fresh store) through the server, under the app's permissions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_resync_through_the_server() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let open = |user: &str, data: &TempDir| {
        let backend = Backend::new(server.backend_secret(), server.admin_secret(), user)
            .permissions(include_bytes!("../jazz.permissions.json").as_slice());
        jazz_app::open(
            Config::new(app_id)
                .server(server.base_url())
                .storage(Storage::Dir(data.path().to_path_buf()))
                .backend(backend),
        )
    };

    let first = TempDir::new().unwrap();
    let writer = open("alice", &first)
        .await
        .expect("open as Alice's backend");
    let app = router(writer.clone());
    let create = CreateTodoRequest {
        title: "Synced".into(),
        description: Some("through the server".into()),
    };
    let (status, body) = call(
        &app,
        "POST",
        "/todos",
        Some(serde_json::to_string(&create).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let created: Todo = json(&body);
    // The server has the todo once a remote read sees it.
    let remote = tokio::time::timeout(
        Duration::from_secs(10),
        writer.all_with(jazz_app::todos::query(), ReadTier::Remote),
    )
    .await
    .expect("the server answers")
    .unwrap();
    assert_eq!(remote.len(), 1);
    writer.shutdown().await.unwrap();

    let second = TempDir::new().unwrap();
    let reader = open("bob", &second).await.expect("open as Bob's backend");
    assert_eq!(reader.bundle(), writer.bundle());
    let seen = tokio::time::timeout(
        Duration::from_secs(10),
        reader.all_with(jazz_app::todos::query(), ReadTier::Remote),
    )
    .await
    .expect("the server answers")
    .unwrap();
    assert_eq!(
        seen.into_iter().map(Todo::from).collect::<Vec<_>>(),
        vec![created]
    );

    reader.shutdown().await.unwrap();
    server.shutdown().await;
}

/// The committed bindings are what `jazz-tools generate` generates from
/// the committed jazz.app.json.
#[test]
fn bindings_are_current() {
    let app = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/jazz.app.json")).unwrap();
    assert_eq!(
        jazz_app::SOURCE,
        jazz_app_source(&app),
        "src/jazz_app.rs is stale: run `npx jazz-tools generate`"
    );
}

fn jazz_app_source(file: &[u8]) -> String {
    ::jazz_app::jazz::tools::deployment::artifact::source_hash(file)
}

/// Stopping ends the live streams, so a graceful shutdown isn't held open
/// by a client still listening.
#[tokio::test]
async fn stopping_ends_live_streams() {
    use futures_util::{FutureExt, StreamExt};
    use reqwest_eventsource::{Event, EventSource};

    let data = TempDir::new().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let stopping = async move {
        let _ = stopped.await;
    }
    .boxed()
    .shared();
    let app = router_until(local(&data).await, stopping.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(stopping)
            .await
            .unwrap()
    });

    let mut events = EventSource::get(format!("{base}/todos/live"));
    loop {
        match events.next().await {
            Some(Ok(Event::Open)) => continue,
            Some(Ok(Event::Message(_))) => break,
            other => panic!("no first list: {other:?}"),
        }
    }
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("the server stops with a live stream open")
        .unwrap();
    events.close();
}

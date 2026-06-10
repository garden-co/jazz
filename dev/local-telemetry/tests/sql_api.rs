use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::routing::post;
use local_telemetry::http;
use reqwest::StatusCode;
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

struct TestServer {
    _temp: TempDir,
    base_url: String,
}

async fn start_server() -> TestServer {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let web_dist = temp.path().join("web").join("dist");
    std::fs::create_dir_all(&data_dir).expect("data dir");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    let app = http::app(data_dir, web_dist);
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });

    TestServer {
        _temp: temp,
        base_url: format!("http://{addr}"),
    }
}

#[tokio::test]
async fn health_returns_ok() {
    let server = start_server().await;
    let response = reqwest::get(format!("{}/health", server.base_url))
        .await
        .expect("health response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("health body"), "ok");
}

#[tokio::test]
async fn sql_rejects_missing_query() {
    let server = start_server().await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{}/sql", server.base_url))
        .json(&json!({ "query": "" }))
        .send()
        .await
        .expect("sql response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = response.json().await.expect("error json");
    assert_eq!(body, json!({ "error": "missing query" }));
}

#[tokio::test]
async fn sql_reads_rotel_trace_json() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let spans_dir = data_dir.join("spans");
    let web_dist = temp.path().join("web").join("dist");
    std::fs::create_dir_all(&spans_dir).expect("spans dir");
    std::fs::write(
        spans_dir.join("traces_20260610_120000.json"),
        r#"
[
  {
    "resource": {
      "attributes": [
        { "key": "service.name", "value": { "stringValue": "jazz-browser" } }
      ]
    },
    "scopeSpans": [
      {
        "scope": { "name": "jazz-tools" },
        "spans": [
          {
            "traceId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "spanId": "bbbbbbbbbbbbbbbb",
            "name": "sync.send",
            "kind": 1,
            "startTimeUnixNano": "1000",
            "endTimeUnixNano": "2500",
            "attributes": [
              { "key": "payload", "value": { "stringValue": "SyncMessage" } },
              { "key": "peer_kind", "value": { "stringValue": "server" } },
              { "key": "peer_id", "value": { "stringValue": "peer-123456789" } }
            ]
          }
        ]
      }
    ]
  }
]
"#,
    )
    .expect("sample trace file");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, http::app(data_dir, web_dist))
            .await
            .expect("serve test app");
    });

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/sql"))
        .json(&json!({
            "query": "SELECT name, service_name, duration_ns FROM spans ORDER BY start_time_unix_nano"
        }))
        .send()
        .await
        .expect("sql response");

    let status = response.status();
    let body: Value = response.json().await.expect("sql json");
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body,
        json!({
            "columns": ["name", "service_name", "duration_ns"],
            "rows": [["sync.send", "jazz-browser", 1500]]
        })
    );
}

#[tokio::test]
async fn otlp_proxy_handles_browser_preflight_and_forwards_posts() {
    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let upstream_received = Arc::clone(&received);
    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream listener");
    let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
    tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/traces", post(capture_otlp_body))
            .with_state(upstream_received);
        axum::serve(upstream_listener, app)
            .await
            .expect("serve upstream app");
    });

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy listener");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    tokio::spawn(async move {
        axum::serve(proxy_listener, http::otlp_proxy_app(upstream_addr))
            .await
            .expect("serve proxy app");
    });

    let client = reqwest::Client::new();
    let preflight = client
        .request(
            reqwest::Method::OPTIONS,
            format!("http://{proxy_addr}/v1/traces"),
        )
        .header("origin", "http://127.0.0.1:5177")
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type")
        .send()
        .await
        .expect("preflight response");

    assert_eq!(preflight.status(), StatusCode::OK);
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-origin")
            .expect("allow origin")
            .to_str()
            .expect("allow origin text"),
        "*"
    );

    let response = client
        .post(format!("http://{proxy_addr}/v1/traces"))
        .header("origin", "http://127.0.0.1:5177")
        .header("content-type", "application/json")
        .body(r#"{"resourceSpans":[]}"#)
        .send()
        .await
        .expect("proxy response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        *received.lock().expect("received lock"),
        vec![br#"{"resourceSpans":[]}"#.to_vec()]
    );
}

async fn capture_otlp_body(
    State(received): State<Arc<Mutex<Vec<Vec<u8>>>>>,
    body: Bytes,
) -> &'static str {
    received.lock().expect("received lock").push(body.to_vec());
    "{}"
}

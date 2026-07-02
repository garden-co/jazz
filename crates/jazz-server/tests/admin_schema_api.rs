use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};

use jazz::db::DbIdentity;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::JazzSchema;
use jazz_server::{InMemoryServerShellConfig, loopback_http::LoopbackHttpServer};
use serde_json::{Value, json};

fn identity() -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([0x5e; 16]),
        author: AuthorId::SYSTEM,
    }
}

fn server_config() -> InMemoryServerShellConfig {
    InMemoryServerShellConfig::new(JazzSchema::new([]), identity())
}

#[test]
fn admin_schema_api_requires_secret_and_rejects_permissions() {
    let server = LoopbackHttpServer::start_with_admin_secret(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        server_config(),
        "secret",
    )
    .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let body = json!({
        "schema": { "tables": [] },
        "permissions": null
    })
    .to_string();
    assert_eq!(
        request(addr, "POST", "/apps/app-a/admin/schemas", &[], &body).status,
        401
    );
    assert_eq!(
        request(
            addr,
            "POST",
            "/apps/app-a/admin/schemas",
            &[("X-Jazz-Admin-Secret", "wrong")],
            &body
        )
        .status,
        401
    );

    let unsupported = json!({
        "schema": { "tables": [] },
        "permissions": { "read": "everyone" }
    })
    .to_string();
    let rejected = request(
        addr,
        "POST",
        "/apps/app-a/admin/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        &unsupported,
    );
    assert_eq!(rejected.status, 400);
    assert_eq!(rejected.json()["error"], "unsupported_permissions");

    server.shutdown();
}

#[test]
fn admin_schema_api_publishes_lists_and_gets_schema_json() {
    let server = LoopbackHttpServer::start_with_admin_secret(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        server_config(),
        "secret",
    )
    .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let publish_body = json!({
        "schema": {
            "tables": [
                {
                    "name": "todos",
                    "columns": [
                        { "name": "title", "type": "string" }
                    ]
                }
            ]
        },
        "permissions": null
    })
    .to_string();
    let published = request(
        addr,
        "POST",
        "/apps/app-a/admin/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        &publish_body,
    );
    assert_eq!(published.status, 201);
    let published_json = published.json();
    let hash = published_json["hash"].as_str().expect("schema hash");
    assert_eq!(hash.len(), 64);
    assert_eq!(published_json["objectId"], format!("schema:app-a:{hash}"));

    let unauthenticated_list = request(addr, "GET", "/apps/app-a/schemas", &[], "");
    assert_eq!(unauthenticated_list.status, 401);

    let list = request(
        addr,
        "GET",
        "/apps/app-a/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        "",
    );
    assert_eq!(list.status, 200);
    let list_json = list.json();
    assert_eq!(list_json["hashes"], json!([hash]));

    let app_b_list = request(
        addr,
        "GET",
        "/apps/app-b/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        "",
    );
    assert_eq!(app_b_list.status, 200);
    assert_eq!(app_b_list.json()["hashes"], json!([]));

    let fetched = request(
        addr,
        "GET",
        &format!("/apps/app-a/schema/{hash}"),
        &[("X-Jazz-Admin-Secret", "secret")],
        "",
    );
    assert_eq!(fetched.status, 200);
    let fetched_json = fetched.json();
    assert!(fetched_json["publishedAt"].as_u64().expect("publishedAt") > 0);
    assert_eq!(fetched_json["schema"]["tables"][0]["name"], "todos");

    let missing = request(
        addr,
        "GET",
        "/apps/app-a/schema/missing",
        &[("X-Jazz-Admin-Secret", "secret")],
        "",
    );
    assert_eq!(missing.status, 404);
    assert_eq!(missing.json()["error"], "schema_not_found");

    server.shutdown();
}

#[test]
fn admin_schema_api_accepts_bare_upstream_table_map_and_preserves_raw_json() {
    let server = LoopbackHttpServer::start_with_admin_secret(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        server_config(),
        "secret",
    )
    .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let raw_schema = json!({
        "todos": {
            "columns": [
                { "name": "title", "column_type": "Text" },
                { "name": "done", "column_type": "Boolean", "nullable": true },
                { "name": "owner", "column_type": "Uuid", "references": "users" },
                { "name": "tags", "column_type": { "type": "Array", "element": "Text" } },
                { "name": "status", "column_type": "Text", "enum": ["open", "done"] },
                { "name": "updatedAt", "column_type": "Text", "timestamp": true }
            ],
            "indexed_columns": ["title"]
        }
    });
    let publish_body = json!({ "schema": raw_schema }).to_string();
    let published = request(
        addr,
        "POST",
        "/apps/app-a/admin/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        &publish_body,
    );
    assert_eq!(published.status, 201);
    let hash = published.json()["hash"]
        .as_str()
        .expect("schema hash")
        .to_owned();

    let fetched = request(
        addr,
        "GET",
        &format!("/apps/app-a/schema/{hash}"),
        &[("X-Jazz-Admin-Secret", "secret")],
        "",
    );
    assert_eq!(fetched.status, 200);
    assert_eq!(fetched.json()["schema"], raw_schema);
    assert!(fetched.json().get("localSchemaId").is_none());

    server.shutdown();
}

#[test]
fn admin_schema_api_rejects_unsupported_schema_type() {
    let server = LoopbackHttpServer::start_with_admin_secret(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        server_config(),
        "secret",
    )
    .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let publish_body = json!({
        "schema": {
            "todos": {
                "columns": [
                    { "name": "metadata", "column_type": "Row" }
                ]
            }
        }
    })
    .to_string();
    let rejected = request(
        addr,
        "POST",
        "/apps/app-a/admin/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        &publish_body,
    );
    assert_eq!(rejected.status, 400);
    assert_eq!(rejected.json()["error"], "unsupported_admin_schema");
    assert!(
        rejected.json()["message"]
            .as_str()
            .expect("message")
            .contains("Row")
    );

    server.shutdown();
}

#[test]
fn admin_schema_api_accepts_benchmark_schema_tables_wrapper() {
    let server = LoopbackHttpServer::start_with_admin_secret(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        server_config(),
        "secret",
    )
    .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let raw_schema = json!({
        "schema": {
            "tables": {
                "events": {
                    "columns": [
                        { "name": "id", "column_type": { "type": "Uuid" } },
                        { "name": "seenAt", "column_type": { "type": "Timestamp" } },
                        { "name": "score", "column_type": { "type": "Double" } }
                    ]
                }
            }
        }
    });
    let published = request(
        addr,
        "POST",
        "/apps/app-a/admin/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        &json!({ "schema": raw_schema, "permissions": null }).to_string(),
    );
    assert_eq!(published.status, 201);

    server.shutdown();
}

#[test]
fn admin_schema_api_persists_catalogue_to_schema_store_file() {
    let data_dir = std::env::temp_dir().join(format!(
        "jazz-server-admin-schema-store-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    let server = LoopbackHttpServer::start_with_admin_secret_and_data_dir(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        server_config(),
        "secret",
        &data_dir,
    )
    .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let publish_body = json!({
        "schema": {
            "tables": [
                {
                    "name": "notes",
                    "columns": [
                        { "name": "body", "type": "string" }
                    ]
                }
            ]
        }
    })
    .to_string();
    let published = request(
        addr,
        "POST",
        "/apps/app-a/admin/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        &publish_body,
    );
    assert_eq!(published.status, 201);
    let hash = published.json()["hash"]
        .as_str()
        .expect("schema hash")
        .to_owned();
    server.shutdown();

    let restarted = LoopbackHttpServer::start_with_admin_secret_and_data_dir(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        server_config(),
        "secret",
        &data_dir,
    )
    .expect("restart loopback HTTP listener");
    let restarted_addr = restarted.local_addr();

    let list = request(
        restarted_addr,
        "GET",
        "/apps/app-a/schemas",
        &[("X-Jazz-Admin-Secret", "secret")],
        "",
    );
    assert_eq!(list.status, 200);
    assert_eq!(list.json()["hashes"], json!([hash]));

    let fetched = request(
        restarted_addr,
        "GET",
        &format!("/apps/app-a/schema/{hash}"),
        &[("X-Jazz-Admin-Secret", "secret")],
        "",
    );
    assert_eq!(fetched.status, 200);
    assert_eq!(fetched.json()["schema"]["tables"][0]["name"], "notes");
    assert!(fetched.json().get("localSchemaId").is_none());

    let store_json: Value =
        serde_json::from_slice(&std::fs::read(data_dir.join("admin-schemas.json")).unwrap())
            .unwrap();
    let local_schema_id = store_json["app-a"][0]["localSchemaId"]
        .as_str()
        .expect("local schema id is persisted beside raw schema");
    assert_eq!(local_schema_id.len(), 36);

    restarted.shutdown();
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[derive(Debug)]
struct Response {
    status: u16,
    body: String,
}

impl Response {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).expect("response body is json")
    }
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Response {
    request_attempt(addr, method, path, headers, body, 0)
}

fn request_attempt(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
    attempt: usize,
) -> Response {
    let mut stream = TcpStream::connect(addr).expect("connect loopback HTTP listener");
    let mut header_text = String::new();
    for (name, value) in headers {
        header_text.push_str(name);
        header_text.push_str(": ");
        header_text.push_str(value);
        header_text.push_str("\r\n");
    }
    if let Err(error) = write!(
        stream,
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\n{header_text}content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    ) {
        if matches!(
            error.kind(),
            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
        ) && attempt < 3
        {
            return request_attempt(addr, method, path, headers, body, attempt + 1);
        }
        panic!("write HTTP request: {error}");
    }

    let mut raw = Vec::new();
    match stream.read_to_end(&mut raw) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset && !raw.is_empty() => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset && attempt < 3 => {
            return request_attempt(addr, method, path, headers, body, attempt + 1);
        }
        Err(error) => panic!("read HTTP response: {error}"),
    }
    let response = String::from_utf8(raw).expect("HTTP response is utf-8");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("response has header terminator");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .expect("response status");

    Response {
        status,
        body: body.to_owned(),
    }
}

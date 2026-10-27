//! The pinned server data directory: a `jazz.sqlite` a catalogue-driven
//! server wrote, holding the rows, the runtime catalogue and the server's
//! host tables (`_jazz_host_catalogue`, `_jazz_host_accounts`,
//! `_jazz_host_runtime_catalogue`; see `dev/proofs/server-host-tables.md`).
//!
//! `fixtures/server-data-dir-v1/` was written once by
//! `produce_server_data_dir_corpus_v1` and is never regenerated after
//! alpha.60 ships: its bytes are what an alpha.60 server has on disk.
//! Current code must serve it as it was (deployed schemas, migration and
//! permissions, accounts, rows), accept new deployments, accounts and
//! rows, and reopen again.
//!
//! `dev/gates/storage-compat.sh` runs this by exact name.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use base64::Engine;
use jazz::account_registry::{AccountId, Assignment, Principal};
use jazz::db::{CurrentRow, InsertOptions, ReadOpts, block_on};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::AppId;
use jazz::tools::public_schema::{
    ColumnType, PolicyExpr, Schema, SchemaBuilder, SchemaHash, TableName, TablePolicies,
    TableSchema,
};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use super::routes::create_router;
use super::{BuiltServer, ServerBuilder, StorageBackend};
use crate::middleware::AuthConfig;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/server-data-dir-v1/jazz.sqlite.gz.base64"
);
/// SHA-256 of the decoded file.
const SHA256: &str = "4ecff0458673c7bee316304dc5ca594bd7ddd36ed521b77607af95443e82756b";
/// The account the producer registered for Alice.
const ALICE_ACCOUNT: &str = "81a787c1-761f-4d56-a625-da95e1fbc306";
const ADMIN_SECRET: &str = "corpus-admin";

fn app_id() -> AppId {
    AppId::from_name("server-data-dir-corpus")
}

fn alice() -> Principal {
    Principal {
        issuer: "https://issuer.example".into(),
        subject: "alice".into(),
    }
}

/// `notes(title)`, then `notes(title, body)` with `body` introduced by a
/// migration.
fn v1() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("notes").column("title", ColumnType::Text))
        .build()
}

fn v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("body", ColumnType::Text),
        )
        .build()
}

fn hash(schema: &Schema) -> String {
    SchemaHash::compute(schema).to_string()
}

fn deploy_v1() -> Json {
    json!({
        "targetSchemaHash": hash(&v1()),
        "schemas": [{"hash": hash(&v1()), "schema": v1()}],
        "migrations": [],
        "permissions": HashMap::from([(
            TableName::new("notes"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]),
    })
}

fn deploy_v2() -> Json {
    json!({
        "targetSchemaHash": hash(&v2()),
        "schemas": [{"hash": hash(&v2()), "schema": v2()}],
        "migrations": [{
            "fromHash": hash(&v1()),
            "toHash": hash(&v2()),
            "forward": [{"table": "notes", "operations": [{
                "type": "introduce",
                "column": "body",
                "column_type": ColumnType::Text,
                "value": jazz::tools::public_schema::Value::Text("(none)".into()),
            }]}],
        }],
        "permissions": HashMap::from([(
            TableName::new("notes"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]),
    })
}

async fn open(dir: &Path) -> BuiltServer {
    ServerBuilder::new(app_id())
        .with_auth_config(AuthConfig {
            admin_secret: Some(ADMIN_SECRET.into()),
            ..Default::default()
        })
        .with_storage(StorageBackend::Persistent {
            path: dir.to_path_buf(),
        })
        .build()
        .await
        .expect("open the server data dir")
}

async fn admin(server: &BuiltServer, method: &str, path: &str, body: Json) -> Json {
    let response = create_router(server.state.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(format!("/apps/{}/admin{path}", app_id()))
                .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Json = serde_json::from_slice(&bytes).unwrap_or(Json::Null);
    assert_eq!(status, StatusCode::OK, "{method} {path}: {body}");
    body
}

async fn login(server: &BuiltServer, principal: Principal) -> Assignment {
    server
        .state
        .accounts
        .as_ref()
        .unwrap()
        .login_or_register(principal)
        .await
        .unwrap()
}

async fn insert_note(server: &BuiltServer, title: &'static str, body: Option<&'static str>) {
    server
        .state
        .runtime()
        .expect("a deployed server runs its runtime")
        .with_db_for_test(move |db| {
            let mut cells = BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]);
            if let Some(body) = body {
                cells.insert("body".to_owned(), Value::String(body.to_owned()));
            }
            block_on(db.insert("notes", cells, InsertOptions::default()))
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
        .await
        .unwrap();
}

/// `(title, body)` of every note, sorted.
async fn notes(server: &BuiltServer) -> Vec<(String, String)> {
    let mut notes = server
        .state
        .runtime()
        .unwrap()
        .with_db_for_test(|db| {
            let prepared = db
                .prepare_query(&Query::from("notes"))
                .map_err(|error| error.to_string())?;
            let rows: Vec<CurrentRow> = block_on(db.all(&prepared, ReadOpts::default()))
                .map_err(|error| error.to_string())?;
            Ok(rows
                .iter()
                .map(|row| {
                    let cell = |name: &str| match row.cells().find(|(column, _)| *column == name) {
                        Some((_, Some(Value::String(text)))) => text.clone(),
                        other => panic!("{name}: {other:?}"),
                    };
                    (cell("title"), cell("body"))
                })
                .collect::<Vec<_>>())
        })
        .await
        .unwrap();
    notes.sort();
    notes
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A fresh copy of the pinned file as `dir/jazz.sqlite`, after checking its
/// digest.
fn install(dir: &Path) -> PathBuf {
    let encoded = std::fs::read_to_string(FIXTURE).unwrap();
    let gz = base64::engine::general_purpose::STANDARD
        .decode(encoded.split_whitespace().collect::<String>())
        .unwrap();
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(gz.as_slice())
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(hex(&Sha256::digest(&bytes)), SHA256, "the corpus is pinned");
    let path = dir.join("jazz.sqlite");
    std::fs::write(&path, bytes).unwrap();
    path
}

/// Writes the corpus when `JAZZ_WRITE_SERVER_DATA_DIR_CORPUS=1`. Run once
/// per storage format; once a format has shipped its committed file is
/// never regenerated, only added beside.
#[tokio::test]
#[ignore = "manual corpus producer; run once per new storage format"]
async fn produce_server_data_dir_corpus_v1() {
    assert_eq!(
        std::env::var("JAZZ_WRITE_SERVER_DATA_DIR_CORPUS").as_deref(),
        Ok("1"),
        "set JAZZ_WRITE_SERVER_DATA_DIR_CORPUS=1 to overwrite the pinned corpus"
    );
    let dir = tempfile::tempdir().unwrap();
    {
        let server = open(dir.path()).await;
        admin(&server, "POST", "/deploy", deploy_v1()).await;
        insert_note(&server, "before the migration", None).await;
        admin(&server, "POST", "/deploy", deploy_v2()).await;
        insert_note(&server, "after the migration", Some("written with v2")).await;
        let alice = login(&server, alice()).await;
        println!("ALICE_ACCOUNT: {}", alice.account.0);
        server.shutdown().await;
    }
    let path = dir.path().join("jazz.sqlite");
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    assert!(!wal.exists(), "closing checkpoints the WAL into the file");
    let bytes = std::fs::read(&path).unwrap();
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    gz.write_all(&bytes).unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(gz.finish().unwrap());
    std::fs::create_dir_all(Path::new(FIXTURE).parent().unwrap()).unwrap();
    std::fs::write(FIXTURE, encoded).unwrap();
    println!("SHA256: {}", hex(&Sha256::digest(&bytes)));
}

/// The pinned data dir opens with its catalogue, migration, permissions,
/// accounts and rows; takes a new deployment, account and row; and reopens
/// with all of them.
#[tokio::test]
async fn pinned_server_data_dir_reopens_and_accepts_current_writes() {
    let dir = tempfile::tempdir().unwrap();
    install(dir.path());
    let pinned = AccountId(uuid::Uuid::parse_str(ALICE_ACCOUNT).unwrap());
    let before = vec![
        (
            "after the migration".to_owned(),
            "written with v2".to_owned(),
        ),
        ("before the migration".to_owned(), "(none)".to_owned()),
    ];
    {
        let server = open(dir.path()).await;
        let graph = admin(&server, "GET", "/migrations/graph", Json::Null).await;
        let mut schemas = vec![hash(&v1()), hash(&v2())];
        schemas.sort();
        assert_eq!(
            graph,
            json!({
                "activeSchemaHash": hash(&v2()),
                "migrations": [{"fromHash": hash(&v1()), "toHash": hash(&v2())}],
                "schemas": schemas,
            })
        );
        let permissions = admin(&server, "GET", "/permissions", Json::Null).await;
        assert!(
            permissions["permissions"]["notes"].is_object(),
            "{permissions}"
        );
        assert_eq!(login(&server, alice()).await.account, pinned);
        assert_eq!(notes(&server).await, before);

        // Unchanged deployments stay unchanged; current writes are accepted.
        assert_eq!(
            admin(&server, "POST", "/deploy", deploy_v2()).await["changed"],
            false
        );
        insert_note(&server, "after reopening", Some("current")).await;
        let bob = Principal {
            issuer: "https://issuer.example".into(),
            subject: "bob".into(),
        };
        let bob_account = login(&server, bob.clone()).await.account;
        server.shutdown().await;

        let server = open(dir.path()).await;
        assert_eq!(login(&server, alice()).await.account, pinned);
        assert_eq!(login(&server, bob).await.account, bob_account);
        let mut after = before.clone();
        after.push(("after reopening".to_owned(), "current".to_owned()));
        after.sort();
        assert_eq!(notes(&server).await, after);
        server.shutdown().await;
    }
}

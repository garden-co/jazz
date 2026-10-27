//! `a59-produce`: write a synthetic alpha.59 tenant data dir for the
//! migration tool's tests. Fixed ids, authors and times, so two runs hold the
//! same logical content.
//!
//! The catalogue goes through a real alpha.59 server's admin API (exactly how
//! `jazz-tools deploy` publishes), so the admin catalogue and the server
//! shell's runtime lineage are the server's own. Rows and accounts are then
//! written with the server stopped, through alpha.59's public Db and account
//! registry APIs. It covers what tenants' stores hold: two schema versions
//! joined by a rename + add-column migration, two permission bundles,
//! interned authors, updates (history), a delete and a restore, a 300 KiB
//! large text value, bytes and nulls, and an account journal with a link and
//! a revocation.
//!
//!   a59-produce --out <dir> --server-bin <alpha.59 jazz-tools>

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

use jazz::account_registry::storage::StoredAccountRegistry;
use jazz::account_registry::{AccountCommand, AccountId, Principal};
use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, RestoreOptions, UpdateOptions,
    WriteIdentity, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::StorageFactory;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::NodeState;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{Schema, SchemaHash};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_storage_rocksdb::RocksDbStorageFactory;
use serde_json::json;

const SERVER_NODE: [u8; 16] = [0x5e; 16];
const APP_ID: &str = "0a590000-0000-4000-8000-000000000059";
const ADMIN_SECRET: &str = "a59-produce-admin";
/// 2026-10-01T00:00:00Z.
const EPOCH_MS: u64 = 1_790_812_800_000;
const LARGE: usize = 300 * 1024;

fn allow_all() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

fn projects() -> TableSchemaBuilder {
    TableSchemaBuilder::new("projects")
        .column("name", ColumnType::Text)
        .column("archived", ColumnType::Boolean)
}

fn documents(title: &str, pinned: bool) -> TableSchemaBuilder {
    let table = TableSchemaBuilder::new("documents")
        .column("project", ColumnType::Uuid)
        .column(title, ColumnType::Text)
        .column("body", ColumnType::Text)
        .nullable_column("blob", ColumnType::Bytea)
        .nullable_column("rank", ColumnType::Integer);
    if pinned {
        table.column("pinned", ColumnType::Boolean)
    } else {
        table
    }
}

/// Structural schemas, as `deploy` publishes them (permissions go separately).
fn v1() -> Schema {
    SchemaBuilder::new()
        .table(projects())
        .table(documents("title", false))
        .build()
}

fn v2() -> Schema {
    SchemaBuilder::new()
        .table(projects())
        .table(documents("heading", true))
        .build()
}

/// Permissions bundle `n`: allow-all, except that bundle 2 forbids deleting
/// documents, so the two bundles differ.
fn permissions(schema: &Schema, n: u8) -> HashMap<String, TablePolicies> {
    schema
        .keys()
        .map(|name| {
            let mut policies = allow_all();
            if n == 2 && name.as_str() == "documents" {
                policies = policies.with_delete(PolicyExpr::False);
            }
            (name.as_str().to_owned(), policies)
        })
        .collect()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", &format!("user-{n}"))
        .expect("valid principal")
        .with_account(AccountId(uuid::Uuid::from_bytes([0xa0 + n; 16])))
}

fn row(table: u8, n: u8) -> RowUuid {
    let mut bytes = [n; 16];
    bytes[0] = table;
    RowUuid::from_bytes(bytes)
}

fn at(n: u64) -> Option<u64> {
    Some(EPOCH_MS + n * 60_000)
}

fn text(seed: u8, len: usize) -> String {
    (0..len)
        .map(|i| char::from(b'a' + ((i + seed as usize) % 26) as u8))
        .collect()
}

fn cells(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn s(value: &str) -> Value {
    Value::String(value.to_owned())
}

/// A running alpha.59 server on a data dir.
struct Server {
    child: Child,
    base: String,
}

impl Server {
    fn start(bin: &Path, data_dir: &Path) -> Server {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("free port")
            .port();
        let child = Command::new(bin)
            .args([
                "server",
                APP_ID,
                "--port",
                &port.to_string(),
                "--admin-secret",
                ADMIN_SECRET,
            ])
            .arg("--data-dir")
            .arg(data_dir)
            .env("RUST_LOG", "warn")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn alpha.59 server");
        let server = Server {
            child,
            base: format!("http://127.0.0.1:{port}"),
        };
        for _ in 0..200 {
            if curl(&["-sf", &format!("{}/health", server.base)]).is_ok() {
                return server;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        panic!(
            "alpha.59 server did not become healthy (run it by hand on {} to see why)",
            data_dir.display()
        );
    }

    fn admin(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let url = format!("{}/apps/{APP_ID}/admin{path}", self.base);
        let header = format!("X-Jazz-Admin-Secret: {ADMIN_SECRET}");
        let mut args = vec![
            "-sS",
            "-X",
            method,
            "-H",
            &header,
            "-H",
            "Content-Type: application/json",
            "-w",
            "\n%{http_code}",
        ];
        let body = body.map(|body| body.to_string());
        if let Some(body) = &body {
            args.extend(["--data-binary", body]);
        }
        args.push(&url);
        let out = curl(&args).expect("curl");
        let (json, status) = out.trim_end().rsplit_once('\n').unwrap_or(("", out.trim()));
        assert!(
            status.starts_with('2'),
            "{method} {path} -> {status}: {json}"
        );
        serde_json::from_str(json).unwrap_or(serde_json::Value::Null)
    }

    fn stop(mut self) {
        // Graceful: the server flushes and closes its roots on SIGTERM.
        Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("kill");
        let status = self.child.wait().expect("wait for server");
        assert!(
            status.success() || status.code().is_none(),
            "server exited with {status}"
        );
    }
}

fn curl(args: &[&str]) -> Result<String, String> {
    let out = Command::new("curl")
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}

fn hash(schema: &Schema) -> String {
    SchemaHash::compute(schema).to_string()
}

fn publish_schema(server: &Server, schema: &Schema) {
    server.admin("POST", "/schemas", Some(json!({ "schema": schema })));
}

fn publish_permissions(server: &Server, schema: &Schema, n: u8) {
    let head = server.admin("GET", "/permissions/head", None);
    let parent = head["head"]["bundleObjectId"].clone();
    server.admin(
        "POST",
        "/permissions",
        Some(json!({
            "schemaHash": hash(schema),
            "permissions": permissions(schema, n),
            "expectedParentBundleObjectId": parent,
        })),
    );
}

/// Open the server shell the way the server does on restart: with the
/// schema its durable catalogue selects.
fn open_shell(path: &Path) -> Db {
    let storage = block_on(RocksDbStorageFactory.open(
        path.to_path_buf(),
        JazzSchema::empty().column_families(),
        jazz::storage_codec_profile::epoch_1_storage_codec_profile().expect("profile"),
    ))
    .expect("open rocksdb");
    let (storage, schema) = block_on(NodeState::select_durable_reopen_schema(
        storage,
        JazzSchema::empty(),
    ))
    .expect("reopen schema");
    let db = block_on(Db::open_history_complete(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes(SERVER_NODE),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open db");
    db.declare_upload_root();
    db
}

fn settle(db: &Db) {
    for _ in 0..4 {
        block_on(db.tick()).expect("tick");
    }
}

fn insert(db: &Db, table: &str, id: RowUuid, by: AuthorSubject, n: u64, values: &[(&str, Value)]) {
    block_on(db.insert(
        table,
        cells(values),
        InsertOptions {
            row_id: Some(id),
            identity: WriteIdentity::Session(by),
            updated_at_ms: at(n),
            ..Default::default()
        },
    ))
    .expect("insert");
}

fn update(db: &Db, table: &str, id: RowUuid, by: AuthorSubject, n: u64, values: &[(&str, Value)]) {
    block_on(db.update(
        table,
        id,
        cells(values),
        UpdateOptions {
            identity: WriteIdentity::Session(by),
            updated_at_ms: at(n),
            ..Default::default()
        },
    ))
    .expect("update");
}

fn delete(db: &Db, table: &str, id: RowUuid, by: AuthorSubject, n: u64) {
    block_on(db.delete(
        table,
        id,
        DeleteOptions {
            identity: WriteIdentity::Session(by),
            updated_at_ms: at(n),
            ..Default::default()
        },
    ))
    .expect("delete");
}

fn v1_rows(db: &Db) {
    insert(
        db,
        "projects",
        row(1, 1),
        user(1),
        1,
        &[("name", s("Alpha")), ("archived", Value::Bool(false))],
    );
    insert(
        db,
        "projects",
        row(1, 2),
        user(2),
        2,
        &[("name", s("Beta")), ("archived", Value::Bool(false))],
    );
    update(
        db,
        "projects",
        row(1, 1),
        user(2),
        3,
        &[("name", s("Alpha 2"))],
    );
    insert(
        db,
        "documents",
        row(2, 1),
        user(1),
        4,
        &[
            ("project", Value::Uuid(row(1, 1).0)),
            ("title", s("Spec")),
            ("body", Value::String(text(1, LARGE))),
            (
                "blob",
                Value::Nullable(Some(Box::new(Value::Bytes(vec![1, 2, 3, 0, 255])))),
            ),
            ("rank", Value::Nullable(None)),
        ],
    );
    insert(
        db,
        "documents",
        row(2, 2),
        user(2),
        5,
        &[
            ("project", Value::Uuid(row(1, 2).0)),
            ("title", s("Notes")),
            ("body", s("short")),
            ("blob", Value::Nullable(None)),
            ("rank", Value::Nullable(Some(Box::new(Value::I32(5))))),
        ],
    );
    update(
        db,
        "documents",
        row(2, 2),
        user(1),
        6,
        &[("body", s("short, edited"))],
    );
    delete(db, "projects", row(1, 2), user(2), 7);
    insert(
        db,
        "projects",
        row(1, 3),
        AuthorSubject::SYSTEM,
        8,
        &[("name", s("System")), ("archived", Value::Bool(true))],
    );
    delete(db, "projects", row(1, 3), user(1), 9);
    block_on(db.restore(
        "projects",
        row(1, 3),
        None,
        RestoreOptions {
            identity: WriteIdentity::Session(user(1)),
            updated_at_ms: at(10),
            ..Default::default()
        },
    ))
    .expect("restore");
}

fn v2_rows(db: &Db) {
    update(
        db,
        "documents",
        row(2, 1),
        user(2),
        11,
        &[("heading", s("Spec v2")), ("pinned", Value::Bool(true))],
    );
    insert(
        db,
        "documents",
        row(2, 3),
        user(1),
        12,
        &[
            ("project", Value::Uuid(row(1, 1).0)),
            ("heading", s("Plan")),
            ("body", s("written under v2")),
            ("blob", Value::Nullable(None)),
            ("rank", Value::Nullable(Some(Box::new(Value::I32(-1))))),
            ("pinned", Value::Bool(false)),
        ],
    );
}

fn principal(n: u8) -> Principal {
    Principal {
        issuer: "https://issuer.example".to_owned(),
        subject: format!("user-{n}"),
    }
}

fn accounts(path: &Path) {
    let profile = jazz::storage_codec_profile::epoch_1_storage_codec_profile()
        .and_then(|profile| profile.with_additional_codecs(["jazz.account-journal.v1"]))
        .expect("profile");
    let storage =
        block_on(RocksDbStorageFactory.open(path.to_path_buf(), vec!["default".into()], profile))
            .expect("open accounts root");
    let mut registry = block_on(StoredAccountRegistry::open(storage)).expect("open registry");
    let now = EPOCH_MS / 1000;
    let nonce = uuid::Uuid::from_bytes([0x11; 16]);
    for command in [
        AccountCommand::Register {
            principal: principal(1),
            account: AccountId(uuid::Uuid::from_bytes([0xa1; 16])),
        },
        AccountCommand::Register {
            principal: principal(2),
            account: AccountId(uuid::Uuid::from_bytes([0xa2; 16])),
        },
        AccountCommand::RequestLink {
            approver: principal(1),
            candidate: principal(3),
            nonce,
            now,
            expires_at: now + 600,
        },
        AccountCommand::AcceptLink {
            candidate: principal(3),
            nonce,
            now: now + 60,
        },
        AccountCommand::Revoke {
            approver: principal(1),
            target: principal(3),
        },
    ] {
        block_on(registry.execute(&command)).unwrap_or_else(|error| panic!("{command:?}: {error}"));
    }
    block_on(registry.close()).expect("close registry");
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut out = None;
    let mut bin = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out = args.next(),
            "--server-bin" => bin = args.next(),
            other => panic!("unknown argument {other}"),
        }
    }
    let out = PathBuf::from(out.expect("--out <dir>"));
    let bin = PathBuf::from(bin.expect("--server-bin <alpha.59 jazz-tools>"));
    assert!(!out.exists(), "{} exists; pick a fresh dir", out.display());
    std::fs::create_dir_all(&out).expect("create out");
    let shell = out.join("server-shell.rocksdb");
    let (v1, v2) = (v1(), v2());

    let server = Server::start(&bin, &out);
    publish_schema(&server, &v1);
    publish_permissions(&server, &v1, 1);
    server.stop();

    let db = open_shell(&shell);
    v1_rows(&db);
    settle(&db);
    block_on(db.close()).expect("close");
    drop(db);

    // v2: documents.title -> heading, and a defaulted `pinned`.
    let server = Server::start(&bin, &out);
    publish_schema(&server, &v2);
    server.admin(
        "POST",
        "/migrations",
        Some(json!({
            "fromHash": hash(&v1),
            "toHash": hash(&v2),
            "forward": [
                {
                    "table": "documents",
                    "operations": [
                        { "type": "rename", "column": "title", "value": "heading" },
                        {
                            "type": "introduce",
                            "column": "pinned",
                            "column_type": ColumnType::Boolean,
                            "value": jazz::tools::public_schema::Value::Boolean(false),
                        },
                    ],
                },
            ],
        })),
    );
    publish_permissions(&server, &v2, 2);
    server.stop();

    let db = open_shell(&shell);
    v2_rows(&db);
    settle(&db);
    block_on(db.close()).expect("close");
    drop(db);

    accounts(&out.join("accounts.rocksdb"));
    println!("wrote {}", out.display());
}

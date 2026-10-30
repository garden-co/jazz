//! The pinned native storage corpus: SQLite files written by the storage
//! format this build reads ([`jazz::format::STORAGE_FORMAT`]), plus stores
//! from before SQLite.
//!
//! - `fixtures/sqlite-storage-v1/` holds an authority and a client replica
//!   that synced with it, written once by
//!   `produce_sqlite_storage_corpus_v1` and never regenerated: their bytes
//!   are what an alpha.60 user has on disk. Current code must reopen them
//!   with every row, history version, branch, large value, renamed column
//!   and pending upload intact, accept current writes, and reopen again.
//! - A damaged copy is refused with a storage error, never read as data; a
//!   file in another storage format, or one Jazz did not write, is refused
//!   with `UnsupportedStorageCodecs` and left byte-for-byte as it was.
//! - `fixtures/pre-sqlite/` holds Groove stores (RocksDB directories from the
//!   published alpha.54 and alpha.56 packages and the last Groove format, and
//!   a Groove SQLite-adapter file). Each is refused with
//!   `UnsupportedStorageCodecs` without being changed.
//!
//! `dev/gates/storage-compat.sh` runs these by exact name.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use base64::Engine;
use duplex_transport::duplex;
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ErrorCode, ExactWriteTarget, FileOptions, InsertOptions,
    ReadOpts, ReadTier, Reason, UpdateOptions, WireError, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, LensOp, MigrationLens, TableLens};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnMergeStrategy, ColumnType, OpenTransactionId, SchemaBuilder, TableName,
    TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;
use sha2::{Digest, Sha256};

use common::{allow_all_policies, compile_schema};

// ---- the corpus ------------------------------------------------------------

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures");
const SERVER_FILE: &str = "sqlite-storage-v1/server.sqlite.gz.base64";
const CLIENT_FILE: &str = "sqlite-storage-v1/client.sqlite.gz.base64";
/// SHA-256 of the decoded SQLite files. Until alpha.60 ships, a format-1
/// change re-runs the producer and re-pins these; after that the corpus is
/// immutable and a new storage format gets a new corpus beside it (see
/// `fixtures/sqlite-storage-v1/README.md`).
const SERVER_SHA256: &str = "a37fdbfdf40085395a6ec2fc497d4b8e7f82f601ab9d58e3fe40af3a03620be9";
const CLIENT_SHA256: &str = "12630759ab0ee481395fd6b861dfe49a39dcfccd3efed5c0f9508984f198d268";

const SERVER_NODE: [u8; 16] = [0xa0; 16];
const CLIENT_NODE: [u8; 16] = [0xc0; 16];
/// A write time far ahead of any real clock (the year 3000).
const FAR_FUTURE_MS: u64 = 32_503_680_000_000;
/// Large enough to be stored as a chunked large value.
const LARGE: usize = 300 * 1024;

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x30 + n; 16])
}

fn row(kind: u8, n: u8) -> RowUuid {
    let mut bytes = [kind; 16];
    bytes[15] = n;
    RowUuid::from_bytes(bytes)
}

fn project(n: u8) -> RowUuid {
    row(0x01, n)
}

fn document(n: u8) -> RowUuid {
    row(0x02, n)
}

fn draft(n: u8) -> RowUuid {
    row(0x03, n)
}

fn draft_branch() -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0xbb; 16])))])
}

/// The branch `draft_branch()` is viewed over.
fn draft_base() -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0xba; 16])))])
}

/// The client's view of the drafts: the draft branch over its base.
fn draft_view() -> ReadOpts {
    ReadOpts::default().branch_view(
        draft_branch(),
        Some(jazz::protocol::BranchViewBase::Current(draft_base())),
    )
}

/// `who`'s subject, as a draft's `owner`.
fn subject(who: AuthorSubject) -> Value {
    Value::String(who.principal_parts().1)
}

/// Deterministic letters.
fn text(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect()
}

fn blob(n: u8) -> Vec<u8> {
    (0..64u8)
        .map(|i| i.wrapping_mul(n).wrapping_add(7))
        .collect()
}

/// `projects(name)`, `documents(project -> projects, title, body, blob,
/// rank, status: enum)` with an index on `title` and a text-merging `body`, and
/// `drafts(branch_id, title, owner)` branched by `branch_id`, readable when
/// it has no owner or the reader owns it. `title_column` names
/// `documents.title`.
fn schema(title_column: &str) -> JazzSchema {
    use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
    let owned = PolicyExpr::Or(vec![
        PolicyExpr::IsNull {
            column: "owner".to_owned(),
        },
        PolicyExpr::Cmp {
            column: "owner".to_owned(),
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(vec![
                "user".to_owned(),
                "identity".to_owned(),
                "subject".to_owned(),
            ]),
        },
    ]);
    let mut schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .table(
            TableSchemaBuilder::new("documents")
                .fk_column("project", "projects")
                .column(title_column, ColumnType::Text)
                .column("body", ColumnType::Text)
                .column("blob", ColumnType::Bytea)
                .column_with_default("rank", ColumnType::BigInt, jazz::tools::Value::BigInt(0))
                .column_with_default(
                    "status",
                    ColumnType::Enum {
                        variants: ["draft", "review", "published"].map(str::to_owned).to_vec(),
                    },
                    jazz::tools::Value::Text("draft".to_owned()),
                )
                .index_only([title_column])
                .policies(allow_all_policies()),
        )
        .table(
            TableSchemaBuilder::new("drafts")
                .column("branch_id", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .nullable_column("owner", ColumnType::Text)
                .branch_by("branch_id")
                .policies(
                    TablePolicies::new()
                        .with_select(owned)
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build();
    let documents = schema.get_mut(&TableName::new("documents")).unwrap();
    for column in &mut documents.columns.columns {
        if column.name == "body" {
            *column = column.clone().merge_strategy(ColumnMergeStrategy::Text);
        }
    }
    compile_schema(&schema)
}

/// The schema the client and the first server version use.
fn v1() -> JazzSchema {
    schema("title")
}

/// v1 with `documents.title` renamed to `heading`, which the server
/// activated last.
fn v2() -> JazzSchema {
    schema("heading")
}

fn rename_title() -> MigrationLens {
    MigrationLens::new(
        v1().version_id(),
        v2().version_id(),
        vec![TableLens {
            source_table: "documents".into(),
            target_table: "documents".into(),
            ops: vec![LensOp::RenameColumn {
                from: "title".into(),
                to: "heading".into(),
            }],
        }],
    )
    .unwrap()
}

fn config(schema: JazzSchema, node: [u8; 16], author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes(node),
            author,
        },
    )
}

fn open_server(path: &Path) -> Result<Db, jazz::db::Error> {
    let db = block_on(Db::open_at(
        config(v2(), SERVER_NODE, AuthorSubject::SYSTEM),
        path,
        FileOptions::default(),
    ))?;
    db.set_authority();
    Ok(db)
}

fn open_client(path: &Path) -> Db {
    block_on(Db::open_at(
        config(v1(), CLIENT_NODE, user(3)),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(column, value)| ((*column).to_owned(), value.clone()))
        .collect()
}

fn string(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn uuid(id: RowUuid) -> Value {
    Value::Uuid(id.0)
}

/// Fixed write times, so the corpus's provenance is fixed too.
fn at(n: u64) -> Option<u64> {
    Some(1_790_000_000_000 + n * 1000)
}

fn insert_as(
    db: &Db,
    table: &str,
    id: RowUuid,
    by: AuthorSubject,
    n: u64,
    values: &[(&str, Value)],
) {
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
    .unwrap();
}

fn update_as(
    db: &Db,
    table: &str,
    id: RowUuid,
    by: AuthorSubject,
    n: u64,
    values: &[(&str, Value)],
) {
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
    .unwrap();
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn pump(dbs: &[&Db]) {
    for _ in 0..8 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}

type Rows = Vec<(RowUuid, Vec<(String, Option<Value>)>)>;

fn rows(db: &Db, query: Query, opts: ReadOpts) -> Result<Rows, jazz::db::Error> {
    let prepared = db.prepare_query(&query.order_by("id", OrderDirection::Asc))?;
    let found: Vec<CurrentRow> = block_on(db.all(&prepared, opts))?;
    Ok(found
        .iter()
        .map(|row| {
            let cells = row
                .cells()
                .map(|(name, value)| (name.to_owned(), value.cloned()))
                .collect();
            (row.row_uuid(), cells)
        })
        .collect())
}

fn local() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    }
}

fn expect(id: RowUuid, values: &[(&str, Value)]) -> (RowUuid, Vec<(String, Option<Value>)>) {
    (
        id,
        values
            .iter()
            .map(|(name, value)| ((*name).to_owned(), Some(value.clone())))
            .collect(),
    )
}

// ---- producing the corpus --------------------------------------------------

/// Writes the corpus into `fixtures/sqlite-storage-v1/` when
/// `JAZZ_WRITE_SQLITE_STORAGE_CORPUS=1`. Run once per storage format; once
/// a format has shipped its committed files are never regenerated, only
/// added beside (see the corpus README).
#[test]
#[ignore = "#1787: manual corpus producer; run once per new storage format"]
fn produce_sqlite_storage_corpus_v1() {
    assert_eq!(
        std::env::var("JAZZ_WRITE_SQLITE_STORAGE_CORPUS").as_deref(),
        Ok("1"),
        "set JAZZ_WRITE_SQLITE_STORAGE_CORPUS=1 to overwrite the pinned corpus"
    );
    let dir = scratch("produce");
    let (server_path, client_path) = (dir.join("server.sqlite"), dir.join("client.sqlite"));
    {
        let server = block_on(Db::open_at(
            config(v1(), SERVER_NODE, AuthorSubject::SYSTEM),
            &server_path,
            FileOptions::default(),
        ))
        .unwrap();
        server.set_authority();
        let client = open_client(&client_path);

        // Two authors (interned), history (updates), a soft delete, a large
        // value, bytes and a branch.
        insert_as(
            &server,
            "projects",
            project(1),
            user(1),
            1,
            &[("name", string("Alpha"))],
        );
        insert_as(
            &server,
            "projects",
            project(2),
            user(2),
            2,
            &[("name", string("Beta"))],
        );
        update_as(
            &server,
            "projects",
            project(1),
            user(2),
            3,
            &[("name", string("Alpha 2"))],
        );
        insert_as(
            &server,
            "documents",
            document(1),
            user(1),
            4,
            &[
                ("project", uuid(project(1))),
                ("title", string("Spec")),
                ("body", string(&text(1, LARGE))),
                ("blob", Value::Bytes(blob(1))),
            ],
        );
        insert_as(
            &server,
            "documents",
            document(2),
            user(2),
            5,
            &[
                ("project", uuid(project(2))),
                ("title", string("Notes")),
                ("body", string("short")),
                ("blob", Value::Bytes(blob(2))),
                ("rank", Value::I64(5)),
                ("status", string("published")),
            ],
        );
        update_as(
            &server,
            "documents",
            document(2),
            user(1),
            6,
            &[("body", string("short, edited"))],
        );
        insert_as(
            &server,
            "documents",
            document(3),
            user(1),
            7,
            &[
                ("project", uuid(project(2))),
                ("title", string("Scratch")),
                ("body", string("to be deleted")),
                ("blob", Value::Bytes(Vec::new())),
            ],
        );
        block_on(server.delete("documents", document(3), Default::default())).unwrap();
        block_on(server.insert(
            "drafts",
            cells(&[("title", string("On a branch"))]),
            InsertOptions {
                row_id: Some(draft(1)),
                target: ExactWriteTarget::Branch(draft_branch()),
                updated_at_ms: at(8),
                ..Default::default()
            },
        ))
        .unwrap();

        // The client syncs everything, then writes while connected; two of
        // its writes never reach the server before it quits.
        let (client_side, server_side) = duplex();
        let _upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, user(3)).unwrap();
        let streams: Vec<_> = ["projects", "documents"]
            .into_iter()
            .map(|table| {
                let query = client.prepare_query(&Query::from(table)).unwrap();
                block_on(client.subscribe(&query, ReadOpts::default())).unwrap()
            })
            .collect();
        // A row of the draft's base that the draft overrides with a version
        // the client may not read: its view hides the row, and the client
        // keeps a marker of it (`_jazz_withheld`).
        block_on(server.insert(
            "drafts",
            cells(&[("title", string("Shared"))]),
            InsertOptions {
                row_id: Some(draft(2)),
                target: ExactWriteTarget::Branch(draft_base()),
                updated_at_ms: at(15),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(server.update(
            "drafts",
            draft(2),
            cells(&[("owner", subject(user(1)))]),
            UpdateOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: draft_branch(),
                    base: Some(jazz::protocol::BranchViewBase::Current(draft_base())),
                },
                updated_at_ms: at(16),
                ..Default::default()
            },
        ))
        .unwrap();
        let drafts = client.prepare_query(&Query::from("drafts")).unwrap();
        let _view = block_on(client.subscribe(&drafts, draft_view())).unwrap();
        pump(&[&client, &server]);
        // A text edit the server merges with one it made meanwhile: it keeps
        // the client's write as sent beside the merged version.
        update_as(
            &client,
            "documents",
            document(2),
            user(3),
            13,
            &[("body", string("short, edited!"))],
        );
        update_as(
            &server,
            "documents",
            document(2),
            user(1),
            14,
            &[("body", string("Short, edited"))],
        );
        drive(
            &[&client, &server],
            client.wait_for_pending_writes(DurabilityTier::Global),
        )
        .unwrap();
        insert_as(
            &client,
            "projects",
            project(3),
            user(3),
            9,
            &[("name", string("Gamma"))],
        );
        drive(
            &[&client, &server],
            client.wait_for_pending_writes(DurabilityTier::Global),
        )
        .unwrap();
        // A write stamped far ahead of the server's clock: the server
        // records its rejection, with its reason, as the commit's fate.
        block_on(client.insert(
            "projects",
            cells(&[("name", string("From the future"))]),
            InsertOptions {
                row_id: Some(project(7)),
                identity: WriteIdentity::Session(user(3)),
                updated_at_ms: Some(FAR_FUTURE_MS),
                ..Default::default()
            },
        ))
        .unwrap();
        let refused = drive(
            &[&client, &server, &client],
            client.wait_for_pending_writes(DurabilityTier::Global),
        )
        .expect_err("the server refuses a write from the future");
        assert_eq!(refused.reason, Some(Reason::ClientClockTooFarAhead));
        // A serializable transaction reads project 1 now; it inserts
        // two after the writes below, and commits with them, never
        // reaching the server: one pending commit of two ops and a read,
        // each stored as its own row.
        let tx = OpenTransactionId::new();
        block_on(client.begin_serializable(tx)).unwrap();
        let projects = client
            .prepare_query(&Query::from("projects").filter(eq(col("id"), lit(uuid(project(1))))))
            .unwrap();
        drive(
            &[&client, &server],
            client.tx_ref(tx).all_prepared(&projects),
        )
        .unwrap();
        insert_as(
            &client,
            "projects",
            project(4),
            user(3),
            10,
            &[("name", string("Pending"))],
        );
        update_as(
            &client,
            "documents",
            document(2),
            user(3),
            11,
            &[("title", string("Notes (offline)"))],
        );
        for (n, name) in [(8, "Serial A"), (9, "Serial B")] {
            block_on(client.tx_ref(tx).insert(
                "projects",
                cells(&[("name", string(name))]),
                InsertOptions {
                    row_id: Some(project(n)),
                    updated_at_ms: at(u64::from(n) + 7),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        // Committed with the server not ticking, so it stays pending.
        block_on(client.commit_serializable_handle(tx)).unwrap();
        drop(streams);

        // Then the server renames a column.
        block_on(server.activate_schema_with_lenses(v2(), &[rename_title()])).unwrap();
        insert_as(
            &server,
            "documents",
            document(4),
            user(2),
            12,
            &[
                ("project", uuid(project(1))),
                ("heading", string("After the rename")),
                ("body", string("v2")),
                ("blob", Value::Bytes(blob(4))),
            ],
        );
    }
    for path in [&server_path, &client_path] {
        let wal = PathBuf::from(format!("{}-wal", path.display()));
        assert!(!wal.exists(), "closing checkpoints the WAL into the file");
    }
    for (path, file) in [(&server_path, SERVER_FILE), (&client_path, CLIENT_FILE)] {
        let bytes = std::fs::read(path).unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        gz.write_all(&bytes).unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(gz.finish().unwrap());
        let target = Path::new(FIXTURES).join(file);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, encoded).unwrap();
        println!("{file}: sha256 {}", hex(&Sha256::digest(&bytes)));
    }
}

// ---- reading it back -------------------------------------------------------

fn scratch(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("jazz-storage-corpus-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn fixture_bytes(file: &str) -> Vec<u8> {
    let encoded = std::fs::read_to_string(Path::new(FIXTURES).join(file)).unwrap();
    let gz = base64::engine::general_purpose::STANDARD
        .decode(encoded.split_whitespace().collect::<String>())
        .unwrap();
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(gz.as_slice())
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
}

/// A fresh copy of a pinned corpus file, after checking its digest.
fn install(file: &str, sha256: &str, dir: &Path) -> PathBuf {
    let bytes = fixture_bytes(file);
    assert_eq!(hex(&Sha256::digest(&bytes)), sha256, "{file} is pinned");
    let path = dir.join(Path::new(file).file_stem().unwrap());
    std::fs::write(&path, bytes).unwrap();
    path
}

fn expected_projects(with_client_writes: bool) -> Rows {
    let mut expected = vec![
        expect(project(1), &[("name", string("Alpha 2"))]),
        expect(project(2), &[("name", string("Beta"))]),
        expect(project(3), &[("name", string("Gamma"))]),
    ];
    if with_client_writes {
        expected.push(expect(project(4), &[("name", string("Pending"))]));
        expected.push(expect(project(8), &[("name", string("Serial A"))]));
        expected.push(expect(project(9), &[("name", string("Serial B"))]));
    }
    expected
}

/// (tx, part, items) of each list the client corpus's pending commits keep
/// in `_jazz_pending_items`.
fn pending_items(path: &Path) -> Vec<(i64, i64, i64)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut statement = conn
        .prepare(
            "SELECT tx, part, count(*) FROM _jazz_pending_items GROUP BY tx, part ORDER BY tx, part",
        )
        .unwrap();
    statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn expected_documents(title: &str, offline_edit: bool) -> Rows {
    let notes = if offline_edit {
        "Notes (offline)"
    } else {
        "Notes"
    };
    vec![
        expect(
            document(1),
            &[
                ("project", uuid(project(1))),
                (title, string("Spec")),
                ("body", string(&text(1, LARGE))),
                ("blob", Value::Bytes(blob(1))),
                ("rank", Value::I64(0)),
                ("status", string("draft")),
            ],
        ),
        expect(
            document(2),
            &[
                ("project", uuid(project(2))),
                (title, string(notes)),
                ("body", string("Short, edited!")),
                ("blob", Value::Bytes(blob(2))),
                ("rank", Value::I64(5)),
                ("status", string("published")),
            ],
        ),
        expect(
            document(4),
            &[
                ("project", uuid(project(1))),
                (title, string("After the rename")),
                ("body", string("v2")),
                ("blob", Value::Bytes(blob(4))),
                ("rank", Value::I64(0)),
                ("status", string("draft")),
            ],
        ),
    ]
}

fn assert_server_corpus(server: &Db, with_client_writes: bool) {
    assert_eq!(
        rows(server, Query::from("projects"), ReadOpts::default()).unwrap(),
        expected_projects(with_client_writes)
    );
    assert_eq!(
        rows(server, Query::from("documents"), ReadOpts::default()).unwrap(),
        expected_documents("heading", with_client_writes)
    );
    // The old schema version reads the renamed column by its old name.
    let old = block_on(server.register_schema_view(v1())).unwrap();
    assert_eq!(
        rows(&old, Query::from("documents"), ReadOpts::default()).unwrap(),
        expected_documents("title", with_client_writes)
    );
    // The enum sorts in the order its variants were added and filters by
    // name.
    let by_status = rows(
        server,
        Query::from("documents")
            .order_by("status", OrderDirection::Desc)
            .select(["status"]),
        ReadOpts::default(),
    )
    .unwrap();
    let ids: Vec<RowUuid> = by_status.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, [document(2), document(1), document(4)]);
    let published = rows(
        server,
        Query::from("documents")
            .filter(eq(col("status"), lit(string("published"))))
            .select(["status"]),
        ReadOpts::default(),
    )
    .unwrap();
    assert_eq!(
        published,
        vec![expect(document(2), &[("status", string("published"))])]
    );
    // History: both versions of project 1, oldest first.
    let history = rows(
        server,
        Query::from("history.projects")
            .filter(eq(col("id"), lit(uuid(project(1)))))
            .select(["name"]),
        ReadOpts::default(),
    )
    .unwrap();
    let names: Vec<_> = history
        .iter()
        .map(|(_, cells)| cells[0].1.clone())
        .collect();
    assert_eq!(names, [Some(string("Alpha")), Some(string("Alpha 2"))]);
    // The merged text edit: its value as sent is kept, but never listed,
    // until its writer's next write to the row (the offline title edit) is
    // built on an accepted version.
    assert_eq!(
        server.as_sent_versions_for_test().unwrap(),
        u64::from(!with_client_writes)
    );
    let bodies: Vec<_> = rows(
        server,
        Query::from("history.documents")
            .filter(eq(col("id"), lit(uuid(document(2)))))
            .select(["body"]),
        ReadOpts::default(),
    )
    .unwrap()
    .into_iter()
    .map(|(_, cells)| cells[0].1.clone().unwrap())
    .collect();
    let mut expected = ["short", "short, edited", "Short, edited", "Short, edited!"]
        .map(string)
        .to_vec();
    if with_client_writes {
        // The client's offline title edit, a version of its own.
        expected.push(string("Short, edited!"));
    }
    assert_eq!(bodies, expected);
    // The branch row, through its branch.
    assert_eq!(
        rows(
            server,
            Query::from("drafts").select(["title"]),
            ReadOpts::default().branch_view(draft_branch(), None),
        )
        .unwrap(),
        vec![
            expect(draft(1), &[("title", string("On a branch"))]),
            expect(draft(2), &[("title", string("Shared"))]),
        ]
    );
}

#[test]
fn pinned_sqlite_corpus_reopens_and_accepts_current_writes() {
    let dir = scratch("reopen");
    let server_path = install(SERVER_FILE, SERVER_SHA256, &dir);
    let client_path = install(CLIENT_FILE, CLIENT_SHA256, &dir);
    // Three pending commits: two of one op, and the serializable one of two
    // ops and one read.
    let lists: Vec<_> = pending_items(&client_path)
        .into_iter()
        .map(|(_, part, items)| (part, items))
        .collect();
    assert_eq!(lists, [(0, 1), (0, 1), (0, 2), (1, 1)], "pinned layout");
    {
        let server = open_server(&server_path).unwrap();
        assert_server_corpus(&server, false);
        let client = open_client(&client_path);
        // The client still has what it synced and its own optimistic writes.
        assert_eq!(
            rows(&client, Query::from("projects"), local()).unwrap(),
            expected_projects(true)
        );
        assert_eq!(
            rows(&client, Query::from("documents").select(["title"]), local()).unwrap(),
            vec![
                expect(document(1), &[("title", string("Spec"))]),
                expect(document(2), &[("title", string("Notes (offline)"))]),
            ]
        );
        // Its view of the drafts still hides the row the draft overrides
        // with a version it may not read.
        let mut view = draft_view();
        view.tier = ReadTier::LocalOnly;
        assert_eq!(
            rows(&client, Query::from("drafts").select(["title"]), view).unwrap(),
            vec![expect(draft(1), &[("title", string("On a branch"))])]
        );
        // Its pending uploads are resent and land on the server.
        let (client_side, server_side) = duplex();
        let _upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, user(3)).unwrap();
        drive(
            &[&client, &server, &client],
            client.wait_for_pending_writes(DurabilityTier::Global),
        )
        .unwrap();
        assert_server_corpus(&server, true);

        // Current writes on both.
        insert_as(
            &server,
            "projects",
            project(5),
            user(1),
            20,
            &[("name", string("Delta"))],
        );
        update_as(
            &server,
            "documents",
            document(1),
            user(1),
            21,
            &[("rank", Value::I64(9))],
        );
        insert_as(
            &client,
            "projects",
            project(6),
            user(3),
            22,
            &[("name", string("Epsilon"))],
        );
        drive(
            &[&client, &server, &client],
            client.wait_for_pending_writes(DurabilityTier::Global),
        )
        .unwrap();
    }
    // And everything is there after another reopen.
    let server = open_server(&server_path).unwrap();
    let names: Vec<_> = rows(&server, Query::from("projects"), ReadOpts::default())
        .unwrap()
        .into_iter()
        .map(|(_, cells)| cells[0].1.clone().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "Alpha 2", "Beta", "Gamma", "Pending", "Delta", "Epsilon", "Serial A", "Serial B"
        ]
        .map(string)
    );
    let ranks = rows(
        &server,
        Query::from("documents").select(["rank"]),
        ReadOpts::default(),
    )
    .unwrap();
    assert_eq!(ranks[0].1, vec![("rank".to_owned(), Some(Value::I64(9)))]);
    // The client did not subscribe this time, so it has its own new row but
    // not the server's.
    let client = open_client(&client_path);
    let names: Vec<_> = rows(&client, Query::from("projects"), local())
        .unwrap()
        .into_iter()
        .map(|(_, cells)| cells[0].1.clone().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "Alpha 2", "Beta", "Gamma", "Pending", "Epsilon", "Serial A", "Serial B"
        ]
        .map(string)
    );
}

/// The server stores `documents.status` as tags in the order the variants
/// were declared, and the client mirrored the same tags.
#[test]
fn pinned_corpus_stores_enum_tags() {
    let dir = scratch("enum-tags");
    for (file, sha256) in [(SERVER_FILE, SERVER_SHA256), (CLIENT_FILE, CLIENT_SHA256)] {
        let path = install(file, sha256, &dir);
        let conn = rusqlite::Connection::open(&path).unwrap();
        let tags: Vec<(i64, String)> = conn
            .prepare("SELECT tag, name FROM _jazz_enum_tags ORDER BY tag")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            tags,
            [
                (0, "draft".into()),
                (1, "review".into()),
                (2, "published".into())
            ],
            "{file}"
        );
        let (table, column): (String, String) = conn
            .query_row("SELECT tbl, col FROM _jazz_enum_tags LIMIT 1", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        let cells: Vec<String> = conn
            .prepare(&format!(
                "SELECT DISTINCT typeof(\"{column}\") FROM \"{table}\""
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(cells, ["integer"], "{file}");
    }
}

/// The server's recorded fates are format 1 too: the rejected commit's fate
/// still decodes, with its reason name, from `_jazz_transactions`.
#[test]
fn pinned_server_corpus_keeps_a_rejected_fate_with_its_reason() {
    let dir = scratch("fates");
    let server_path = install(SERVER_FILE, SERVER_SHA256, &dir);
    let conn = rusqlite::Connection::open(&server_path).unwrap();
    let mut statement = conn
        .prepare(
            "SELECT n.node, t.result FROM _jazz_transactions t JOIN _jazz_nodes n USING (n) \
             WHERE t.result IS NOT NULL ORDER BY n.node, t.tx",
        )
        .unwrap();
    let fates: Vec<(Vec<u8>, Result<(), WireError>)> = statement
        .query_map([], |row| Ok((row.get(0)?, row.get::<_, Vec<u8>>(1)?)))
        .unwrap()
        .map(|row| {
            let (node, bytes) = row.unwrap();
            let fate = postcard::from_bytes(&bytes).expect("a recorded fate decodes");
            (node, fate)
        })
        .collect();
    let rejected: Vec<_> = fates
        .iter()
        .filter_map(|(node, fate)| match fate {
            Err(WireError::Rejected { reason, message }) => Some((node, reason, message)),
            _ => None,
        })
        .collect();
    assert_eq!(rejected.len(), 1, "{fates:?}");
    let (node, reason, message) = rejected[0];
    assert_eq!(node.as_slice(), CLIENT_NODE.as_slice());
    assert_eq!(
        reason.as_deref(),
        Some(Reason::ClientClockTooFarAhead.as_str())
    );
    assert!(
        message.starts_with(jazz::db::CLIENT_CLOCK_TOO_FAR_AHEAD),
        "{message}"
    );
}

// ---- refusals --------------------------------------------------------------

fn digest_tree(path: &Path) -> BTreeMap<PathBuf, String> {
    let mut found = BTreeMap::new();
    if path.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            found.extend(digest_tree(&entry.unwrap().path()));
        }
    } else {
        found.insert(
            path.to_owned(),
            hex(&Sha256::digest(std::fs::read(path).unwrap())),
        );
    }
    found
}

fn open_error(path: &Path) -> jazz::db::Error {
    match open_server(path) {
        Ok(_) => panic!("{} must be refused", path.display()),
        Err(error) => error,
    }
}

/// Edit a copy of the server corpus with plain SQLite.
fn edited_server_corpus(name: &str, sql: &str) -> PathBuf {
    let dir = scratch(name);
    let path = install(SERVER_FILE, SERVER_SHA256, &dir);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(sql).unwrap();
    drop(conn);
    path
}

#[test]
fn another_storage_format_is_refused_without_changing_the_file() {
    for (name, sql) in [
        ("newer", "UPDATE _jazz_format SET version = version + 1"),
        ("older", "UPDATE _jazz_format SET version = 0"),
        // Tables but no stamp: not a file Jazz wrote.
        ("unstamped", "DROP TABLE _jazz_format"),
        // A development format-1 store from before the catalogue was rows:
        // stored names keyed by version id.
        (
            "versioned names",
            "ALTER TABLE _jazz_names ADD COLUMN version BLOB",
        ),
        // One from before a commit's record was one `_jazz_transactions`
        // row: fates and landings in their own tables.
        (
            "per-table fates",
            "CREATE TABLE _jazz_applied (node BLOB NOT NULL, tx INTEGER NOT NULL, result BLOB, \
             PRIMARY KEY (node, tx))",
        ),
        (
            "uninterned nodes",
            "DROP TABLE _jazz_nodes; \
             CREATE TABLE _jazz_nodes (node BLOB PRIMARY KEY, author INTEGER) WITHOUT ROWID",
        ),
    ] {
        let path = edited_server_corpus(name, sql);
        let before = digest_tree(&path);
        let error = open_error(&path);
        assert_eq!(
            error.code,
            ErrorCode::UnsupportedStorageCodecs,
            "{name}: {error:?}"
        );
        assert!(
            error.message.contains("storage format"),
            "{name}: {error:?}"
        );
        assert_eq!(digest_tree(&path), before, "{name}: the file is unchanged");
    }
}

#[test]
fn a_damaged_store_is_refused_rather_than_read() {
    let dir = scratch("damaged");
    let intact = install(SERVER_FILE, SERVER_SHA256, &dir);
    let (page_size, documents_root): (i64, i64) = {
        let conn = rusqlite::Connection::open(&intact).unwrap();
        let page_size = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        let root = conn
            .query_row(
                "SELECT rootpage FROM sqlite_master WHERE type = 'table' AND name = 'documents'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        (page_size, root)
    };
    let original = std::fs::read(&intact).unwrap();

    // A damaged header: refused at open, before anything writes.
    let header = dir.join("header.sqlite");
    let mut bytes = original.clone();
    bytes[..16].copy_from_slice(b"not a jazz store");
    std::fs::write(&header, &bytes).unwrap();
    let error = open_error(&header);
    assert_eq!(error.code, ErrorCode::Storage, "{error:?}");
    assert_eq!(
        std::fs::read(&header).unwrap(),
        bytes,
        "the file is unchanged"
    );

    // A damaged table page and a truncated file: refused at open or by the
    // first read of the damaged table, never read as rows.
    let mut page = original.clone();
    let start = ((documents_root - 1) * page_size) as usize;
    page[start..start + page_size as usize].fill(0x5a);
    let truncated = original[..original.len() / 2].to_vec();
    for (name, bytes) in [("page", page), ("truncated", truncated)] {
        let path = dir.join(format!("{name}.sqlite"));
        std::fs::write(&path, &bytes).unwrap();
        let error = match open_server(&path) {
            Err(error) => error,
            Ok(db) => rows(&db, Query::from("documents"), ReadOpts::default())
                .and_then(|_| rows(&db, Query::from("projects"), ReadOpts::default()))
                .expect_err(&format!("{name}: a damaged store must not read as data")),
        };
        assert_eq!(error.code, ErrorCode::Storage, "{name}: {error:?}");
    }
}

/// The Groove engine's native stores: RocksDB directories (alpha.54 and
/// alpha.56 as published, and the last Groove format) and a Groove
/// SQLite-adapter file.
const PRE_SQLITE_ROCKSDB: [&str; 3] = [
    "pre-sqlite/published-alpha54-native-rocksdb.tar.gz.base64",
    "pre-sqlite/published-alpha56-legacy-edge-receipt-rocksdb.tar.gz.base64",
    "pre-sqlite/groove-linear-history-rocksdb.tar.gz.base64",
];
const PRE_SQLITE_GROOVE_SQLITE: &str = "pre-sqlite/groove-linear-history.sqlite.gz.base64";

/// Unpack a pinned RocksDB directory into `dir/<name>`.
fn unpack_rocksdb(file: &str, dir: &Path) -> PathBuf {
    let tarball = fixture_bytes(file);
    let mut archive = tar::Archive::new(tarball.as_slice());
    let target = dir.join("unpacked");
    archive.unpack(&target).unwrap();
    // Each archive holds one top-level directory.
    let root = std::fs::read_dir(&target)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert!(
        jazz::format::is_groove_store(&root),
        "{file} is a RocksDB store"
    );
    root
}

#[test]
fn pre_sqlite_stores_are_refused_with_a_typed_error_and_left_unchanged() {
    for (n, file) in PRE_SQLITE_ROCKSDB.iter().enumerate() {
        let dir = scratch(&format!("rocksdb-{n}"));
        let store = unpack_rocksdb(file, &dir);
        let before = digest_tree(&store);
        // A host that keeps its store in a data directory (the Node binding).
        let error = jazz::format::refuse_groove_store(&store).unwrap_err();
        assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs, "{file}");
        assert!(error.message.contains("Groove"), "{file}: {error:?}");
        // Opening the directory itself as a store.
        let error = open_error(&store);
        assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs, "{file}");
        assert_eq!(digest_tree(&store), before, "{file} is unchanged");
    }

    let dir = scratch("groove-sqlite");
    let path = dir.join("groove.sqlite");
    std::fs::write(&path, fixture_bytes(PRE_SQLITE_GROOVE_SQLITE)).unwrap();
    let before = digest_tree(&path);
    let error = open_error(&path);
    assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs, "{error:?}");
    assert_eq!(
        digest_tree(&path),
        before,
        "the Groove SQLite file is unchanged"
    );
}

/// The native client keeps its store under its data directory; a Groove
/// store there is refused with `JazzError::UnsupportedStorage`.
#[test]
fn the_native_client_refuses_a_data_directory_from_before_sqlite() {
    let dir = scratch("client-data-dir");
    let store = unpack_rocksdb(PRE_SQLITE_ROCKSDB[0], &dir);
    let data_dir = dir.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::rename(&store, data_dir.join("jazz-core.rocksdb")).unwrap();
    let before = digest_tree(&data_dir);
    let mut context = jazz::tools::AppContext::test(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
            .build(),
    );
    context.data_dir = data_dir.clone();
    context.storage = jazz::tools::ClientStorage::Persistent;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let result = local.block_on(&runtime, jazz::tools::JazzClient::connect(context));
    match result {
        Err(jazz::tools::JazzError::UnsupportedStorage(message)) => {
            assert!(message.contains("before the SQLite engine"), "{message}")
        }
        Err(other) => panic!("expected UnsupportedStorage, got {other:?}"),
        Ok(_) => panic!("a pre-SQLite data directory must be refused"),
    }
    assert_eq!(
        digest_tree(&data_dir),
        before,
        "the data directory is unchanged"
    );
}

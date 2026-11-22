//! Which rows a replica's unsettled commits write: a host showing "not
//! sent yet" beside a row asks the store, which keeps its pending commits
//! across a restart, instead of remembering its own writes.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::path::PathBuf;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::compile_schema;

fn schema() -> JazzSchema {
    let mine = PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["identity".to_owned(), "subject".to_owned()]),
    };
    let table = |name: &str| {
        TableSchemaBuilder::new(name)
            .column("owner", ColumnType::Text)
            .column("title", ColumnType::Text)
            .policies(
                TablePolicies::new()
                    .with_select(PolicyExpr::True)
                    .with_insert(mine.clone())
                    .with_update(Some(mine.clone()), mine.clone()),
            )
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(table("notes"))
            .table(table("tags"))
            .build(),
    )
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn identity(node: u8, author: AuthorSubject) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    }
}

fn authority() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        identity(0xee, AuthorSubject::SYSTEM),
    )))
    .unwrap()
}

fn client() -> Db {
    block_on(Db::open(DbConfig::new(schema(), identity(0xa1, alice())))).unwrap()
}

fn durable(path: &PathBuf) -> Db {
    block_on(Db::open_at(
        DbConfig::new(schema(), identity(0xa1, alice())),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn connect(client: &Db, server: &Db) {
    let (client_side, server_side) = duplex_transport::duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
}

fn pump(client: &Db, server: &Db) {
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

/// Alice writes a row in her own name (accepted) or in bob's (refused).
fn insert(db: &Db, table: &str, id: RowUuid, owner: AuthorSubject) {
    block_on(db.insert(
        table,
        BTreeMap::from([
            ("owner".to_owned(), subject(owner)),
            ("title".to_owned(), Value::String("a title".into())),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn retitle(db: &Db, table: &str, id: RowUuid, title: &str) {
    block_on(db.update(
        table,
        id,
        BTreeMap::from([("title".to_owned(), Value::String(title.into()))]),
        Default::default(),
    ))
    .unwrap();
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-pending-rows-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

/// ```text
/// alice ──insert(n1), insert(n2)──► (no upstream yet)
///   pending rows of notes: n1, n2 — of tags: none
/// alice ──connect──► core ──✓ accepted
///   pending rows of notes: none
/// ```
#[test]
fn a_commit_names_its_rows_until_the_upstream_accepts_it() {
    let server = authority();
    let client = client();
    connect(&client, &server);
    insert(&client, "notes", row(2), alice());
    insert(&client, "notes", row(1), alice());

    assert_eq!(
        client.pending_upload_rows("notes").unwrap(),
        [row(1), row(2)]
    );
    assert!(
        client.pending_upload_rows("tags").unwrap().is_empty(),
        "only the table asked for"
    );

    pump(&client, &server);
    assert_eq!(client.pending_upload_count(), 0);
    assert!(client.pending_upload_rows("notes").unwrap().is_empty());
}

/// ```text
/// alice ──insert(n1), update(n1)──► store ──✗ closed before any upstream
/// alice ──reopen──► pending rows of notes: n1 (its two commits recovered)
/// alice ──connect──► core ──✓ accepted ──► pending rows: none
/// ```
#[test]
fn a_recovered_commit_names_its_rows_after_a_reopen() {
    let path = scratch("reopen");
    let server = authority();
    {
        let client = durable(&path);
        // A store learns it is a client from its first upstream; give it one
        // that never answers, so the commits stay pending when it closes.
        let (client_side, _never_answered) = duplex_transport::duplex();
        block_on(client.connect_upstream(client_side));
        insert(&client, "notes", row(1), alice());
        retitle(&client, "notes", row(1), "retitled");
        assert_eq!(client.pending_upload_rows("notes").unwrap(), [row(1)]);
    }
    let reopened = durable(&path);
    assert_eq!(
        reopened.pending_upload_count(),
        2,
        "both commits were recovered"
    );
    assert_eq!(reopened.pending_upload_rows("notes").unwrap(), [row(1)]);

    connect(&reopened, &server);
    pump(&reopened, &server);
    assert_eq!(reopened.pending_upload_count(), 0);
    assert!(reopened.pending_upload_rows("notes").unwrap().is_empty());
}

/// ```text
/// alice ──insert(n1 in bob's name)──► core ──✗ refused
///   pending rows of notes: n1 → none (refused is not pending)
/// ```
#[test]
fn a_refused_commit_stops_naming_its_row() {
    let server = authority();
    let client = client();
    connect(&client, &server);
    insert(
        &client,
        "notes",
        row(1),
        AuthorSubject::for_test_bytes([0xb0; 16]),
    );
    assert_eq!(client.pending_upload_rows("notes").unwrap(), [row(1)]);

    pump(&client, &server);
    assert_eq!(client.upload_status().rejected, 1);
    assert!(client.pending_upload_rows("notes").unwrap().is_empty());
}

/// ```text
/// every commit and every verdict moves the pending revision, also when
/// the count stays what it was (one commit settles while another joins)
/// ```
#[test]
fn the_pending_revision_moves_with_every_change_of_the_pending_set() {
    let server = authority();
    let client = client();
    connect(&client, &server);
    let start = client.upload_status().pending_revision;

    insert(&client, "notes", row(1), alice());
    let one = client.upload_status();
    assert_eq!(one.pending, 1);
    assert!(one.pending_revision > start, "a commit joined");

    pump(&client, &server);
    let settled = client.upload_status();
    assert_eq!(settled.pending, 0);
    assert!(
        settled.pending_revision > one.pending_revision,
        "a verdict arrived"
    );

    insert(&client, "tags", row(2), alice());
    let other = client.upload_status();
    assert_eq!(other.pending, 1, "the same count as before the verdict");
    assert!(
        other.pending_revision > settled.pending_revision,
        "another commit joined"
    );

    let unchanged = client.upload_status();
    assert_eq!(
        unchanged.pending_revision, other.pending_revision,
        "nothing changed"
    );
}

/// ```text
/// alice ──insert(n1)──► read: 1 pending
/// core ──✓ accepts n1;  alice ──insert(n2)──► read: 1 pending
/// ```
/// Equal counts on both sides of a swap: one commit settles while another
/// joins between two reads. A status that compares only `pending` and
/// `oldest_pending_at` would call the two reads equal; the revision must not.
#[test]
fn the_pending_revision_tells_apart_a_swap_of_equal_count() {
    let server = authority();
    let client = client();
    connect(&client, &server);
    insert(&client, "notes", row(1), alice());
    let before = client.upload_status();

    pump(&client, &server);
    insert(&client, "notes", row(2), alice());
    let after = client.upload_status();

    assert_eq!(after.pending, before.pending, "one pending on both sides");
    assert_ne!(
        after.pending_revision, before.pending_revision,
        "but another commit"
    );
    assert_eq!(client.pending_upload_rows("notes").unwrap(), [row(2)]);
}

/// A table the schema does not have is an error, not an empty answer: a
/// host asking under a wrong name must not read "nothing pending".
#[test]
fn an_unknown_table_is_an_error() {
    let client = client();
    insert(&client, "notes", row(1), alice());
    assert!(client.pending_upload_rows("no_such_table").is_err());
}

/// Differential check: random writes (some the server refuses), going
/// online, pumping and reopening the store, against a model of the pending
/// commits. After every step the rows each table names, the count and the
/// revision must agree with the model.
#[test]
fn pending_upload_rows_follow_a_model_of_the_pending_commits() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let tables = ["notes", "tags"];
    for seed in 1..=24u64 {
        let path = scratch(&format!("model-{seed}"));
        let server = authority();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut db = durable(&path);
        let (dead, mut _never_answered) = duplex_transport::duplex();
        block_on(db.connect_upstream(dead));
        let mut online = false;
        // Pending commits of the model: (table, row) each one writes.
        let mut pending: Vec<(usize, u8)> = Vec::new();
        // Rows that exist locally: alice's (writable) and bob's (refused).
        let mut alices: BTreeMap<(usize, u8), ()> = BTreeMap::new();
        let mut bobs: BTreeMap<(usize, u8), ()> = BTreeMap::new();
        let mut next_row = 1u8;
        let mut revision = db.upload_status().pending_revision;
        for step in 0..60 {
            let at = format!("seed {seed} step {step}");
            let op = rng.next(10);
            let joined = match op {
                0..=2 => {
                    let table = rng.next(2) as usize;
                    let id = next_row;
                    next_row += 1;
                    let refused = rng.next(4) == 0;
                    insert(
                        &db,
                        tables[table],
                        row(id),
                        if refused { bob } else { alice() },
                    );
                    if refused { &mut bobs } else { &mut alices }.insert((table, id), ());
                    pending.push((table, id));
                    true
                }
                3 | 4 if !alices.is_empty() => {
                    let pick = rng.next(alices.len() as u64) as usize;
                    let (table, id) = *alices.keys().nth(pick).unwrap();
                    retitle(&db, tables[table], row(id), &at);
                    pending.push((table, id));
                    true
                }
                5 if !alices.is_empty() => {
                    let pick = rng.next(alices.len() as u64) as usize;
                    let key = *alices.keys().nth(pick).unwrap();
                    block_on(db.delete(tables[key.0], row(key.1), Default::default())).unwrap();
                    alices.remove(&key);
                    pending.push(key);
                    true
                }
                6 | 7 => {
                    pump(&db, &server);
                    if online {
                        // Every verdict arrived: refused rows revert.
                        let settled = !pending.is_empty();
                        pending.clear();
                        bobs.clear();
                        settled
                    } else {
                        false
                    }
                }
                8 if !online => {
                    connect(&db, &server);
                    online = true;
                    false
                }
                9 => {
                    drop(db);
                    db = durable(&path);
                    let (dead, never) = duplex_transport::duplex();
                    _never_answered = never;
                    block_on(db.connect_upstream(dead));
                    online = false;
                    revision = db.upload_status().pending_revision;
                    false
                }
                _ => false,
            };
            for (table, name) in tables.iter().enumerate() {
                let mut want = pending
                    .iter()
                    .filter(|(t, _)| *t == table)
                    .map(|(_, id)| row(*id))
                    .collect::<Vec<_>>();
                want.sort_unstable();
                want.dedup();
                assert_eq!(db.pending_upload_rows(name).unwrap(), want, "{at} {name}");
            }
            let status = db.upload_status();
            assert_eq!(status.pending, pending.len(), "{at}: count");
            if joined {
                assert!(status.pending_revision > revision, "{at}: revision moves");
            } else if op != 9 {
                assert_eq!(status.pending_revision, revision, "{at}: revision stays");
            }
            revision = status.pending_revision;
        }
    }
}

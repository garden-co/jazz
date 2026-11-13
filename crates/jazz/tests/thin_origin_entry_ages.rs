//! A thin client resends a write for at most 10 minutes after its first
//! send; the authority keeps the dedupe entry of a thin write to
//! latest-only tables for an hour on its own clock, then drops it, and the
//! writer node's interned row with its last entry (SPEC 8 §8.14). A thin
//! write to a table with history keeps its entry, which is also its
//! history's record, as every writer's is.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use duplex_transport::duplex;

use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ORIGIN_ENTRY_RETENTION_MS,
    OriginWrite, ReadOpts, RemoteLinkHint, UpdateOptions, WriteIdentity, block_on, origin_node,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::time::TxTime;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::TxId;

use common::{allow_all_policies, compile_schema};

const MINUTE: i64 = 60 * 1000;
/// The client nodes of two thin runtimes.
const ALICE_TAB: [u8; 16] = [0x7c; 16];
const ALICE_LAPTOP: [u8; 16] = [0x7d; 16];

/// `presence`, latest-only, and `notes`, with full history.
fn schema() -> JazzSchema {
    let table = |name: &str| {
        TableSchemaBuilder::new(name)
            .column("body", ColumnType::Text)
            .policies(allow_all_policies())
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(table("presence").latest_only())
            .table(table("notes"))
            .build(),
    )
}

fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn open(node: u8) -> Db {
    block_on(Db::open(config(node))).unwrap()
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}

/// The authority's clock starts at the real time (so a replica's commits,
/// stamped by the real clock, are not ahead of it) and moves only when a
/// test moves it.
fn start_clock(core: &Db) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1_000;
    core.set_clock_for_test(Some(now));
    now
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn insert(db: &Db, table: &str, id: RowUuid, text: &str) {
    block_on(db.insert(
        table,
        body(text),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Alice's thin runtime on `client` sends its `seq`th write: an update of
/// `id` in `table`, through `db`.
fn update_as(
    db: &Db,
    client: [u8; 16],
    seq: u64,
    table: &str,
    id: RowUuid,
    text: &str,
) -> OriginWrite {
    block_on(db.write_as_origin(
        TxId::new(TxTime(seq), NodeUuid::from_bytes(client)),
        &alice(),
        async || {
            db.update(
                table,
                id,
                body(text),
                UpdateOptions {
                    identity: WriteIdentity::Session(alice()),
                    ..Default::default()
                },
            )
            .await
        },
    ))
    .unwrap()
}

fn written(write: OriginWrite) -> bool {
    match write {
        OriginWrite::Written(_) => true,
        OriginWrite::Repeat => false,
    }
}

/// Alice's tab moves her cursor (latest-only `presence`) on the authority
/// three times, the authority's clock moving between them. A resend
/// inside the hour is answered from the first write; once a later thin
/// write finds the first entry more than an hour old, the entry is gone,
/// and a resend of it (which a client no longer sends by then) is written
/// again: the known limit.
///
/// ```
/// t0       write 1
/// t0+50m   write 2, resend 1 → repeat
/// t0+61m   write 3 (drops 1's entry), resend 2 → repeat, resend 1 → written
/// ```
#[test]
fn a_latest_only_thin_writes_entry_dedupes_for_an_hour_then_goes() {
    let core = open(0);
    let t0 = start_clock(&core);
    insert(&core, "presence", row(1), "start");
    assert!(written(update_as(
        &core,
        ALICE_TAB,
        1,
        "presence",
        row(1),
        "a"
    )));
    core.set_clock_for_test(Some(t0 + 50 * MINUTE));
    assert!(written(update_as(
        &core,
        ALICE_TAB,
        2,
        "presence",
        row(1),
        "b"
    )));
    assert!(!written(update_as(
        &core,
        ALICE_TAB,
        1,
        "presence",
        row(1),
        "a"
    )));
    core.set_clock_for_test(Some(t0 + 61 * MINUTE));
    assert!(written(update_as(
        &core,
        ALICE_TAB,
        3,
        "presence",
        row(1),
        "c"
    )));
    assert!(!written(update_as(
        &core,
        ALICE_TAB,
        2,
        "presence",
        row(1),
        "b"
    )));
    assert!(written(update_as(
        &core,
        ALICE_TAB,
        1,
        "presence",
        row(1),
        "a"
    )));
}

/// Alice's tab updates a note (`notes` keeps history) on the authority.
/// Two hours later another thin write lands: the note's entry is its
/// history's record, so it stays, a resend is still answered from it, and
/// history still lists the version under Alice's thin writer.
#[test]
fn a_thin_write_to_a_table_with_history_keeps_its_entry() {
    let core = open(0);
    let t0 = start_clock(&core);
    insert(&core, "notes", row(1), "draft");
    insert(&core, "presence", row(2), "start");
    assert!(written(update_as(
        &core,
        ALICE_TAB,
        1,
        "notes",
        row(1),
        "mine"
    )));
    core.set_clock_for_test(Some(t0 + 2 * 60 * MINUTE));
    assert!(written(update_as(
        &core,
        ALICE_TAB,
        2,
        "presence",
        row(2),
        "here"
    )));
    assert!(!written(update_as(
        &core,
        ALICE_TAB,
        1,
        "notes",
        row(1),
        "mine"
    )));
    let history = Query::from("history.notes")
        .filter(eq(col("id"), lit(Value::Uuid(row(1).0))))
        .order_by("$acceptedAt", OrderDirection::Asc);
    let versions: Vec<CurrentRow> =
        block_on(core.all(&core.prepare_query(&history).unwrap(), ReadOpts::default())).unwrap();
    let writers: Vec<Option<(u64, [u8; 16])>> = versions
        .iter()
        .map(|version| {
            match version
                .cells()
                .find(|(name, _)| *name == "$transactionId")
                .and_then(|(_, value)| value.cloned())
            {
                Some(Value::Tuple(parts)) => match parts.as_slice() {
                    [Value::U64(time), Value::Uuid(node)] => Some((*time, *node.as_bytes())),
                    _ => None,
                },
                _ => None,
            }
        })
        .collect();
    assert_eq!(versions.len(), 2);
    assert_eq!(writers[1], Some((1, origin_node(&alice(), ALICE_TAB))));
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "jazz-thin-origin-entry-ages-{}-{name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("core.sqlite")
}

fn entries_of(path: &Path, node: [u8; 16]) -> (bool, i64) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let interned: bool = conn
        .query_row(
            "SELECT count(*) > 0 FROM _jazz_nodes WHERE node = ?1",
            [node.to_vec()],
            |r| r.get(0),
        )
        .unwrap();
    let entries: i64 = conn
        .query_row(
            "SELECT count(*) FROM _jazz_transactions AS t JOIN _jazz_nodes AS n ON n.n = t.n \
             WHERE n.node = ?1",
            [node.to_vec()],
            |r| r.get(0),
        )
        .unwrap();
    (interned, entries)
}

/// White-box, because what it pins is storage no public API shows: the
/// writer node's interned row going with its last entry, and the bytes of
/// the entry-age table (a durable encoding, storage format 1).
///
/// Alice's tab writes `presence` through a replica (the authority records
/// the upload) and `notes` on the authority; her laptop's runtime writes
/// `presence` on the authority 61 minutes later. The tab's `presence`
/// entry goes; its `notes` entry, and so its node, stays. Another hour
/// on, the laptop's entry is dropped by a third runtime's write, and with it
/// the laptop's node.
///
/// ```
/// tab ──presence──► replica ──upload──► core (t0)      tab ──notes──► core (t0)
/// laptop ──presence──► core (t0+61m: drops tab's presence entry)
/// phone ──presence──► core (t0+122m: drops laptop's entry and node)
/// ```
#[test]
fn an_aged_out_thin_writer_leaves_no_rows_behind() {
    const PHONE: [u8; 16] = [0x7e; 16];
    let path = scratch("nodes");
    let core = block_on(Db::open_at(config(0), &path, FileOptions::default())).unwrap();
    core.set_authority();
    let t0 = start_clock(&core);
    let replica = open(1);
    let (up, down) = duplex();
    let _link = block_on(replica.connect_upstream(up));
    core.accept_subscriber(down, AuthorSubject::SYSTEM).unwrap();
    replica.set_remote_link_hint(RemoteLinkHint::Live);
    let settle = || {
        for _ in 0..8 {
            for db in [&replica, &core] {
                block_on(db.tick()).unwrap();
            }
        }
    };
    insert(&core, "presence", row(1), "start");
    insert(&core, "notes", row(2), "draft");
    let prepared = replica.prepare_query(&Query::from("presence")).unwrap();
    let _live = block_on(replica.subscribe(&prepared, ReadOpts::default())).unwrap();
    settle();
    assert!(written(update_as(
        &replica,
        ALICE_TAB,
        1,
        "presence",
        row(1),
        "tab"
    )));
    settle();
    assert!(written(update_as(
        &core,
        ALICE_TAB,
        2,
        "notes",
        row(2),
        "tab"
    )));
    let (tab, laptop, phone) = (
        origin_node(&alice(), ALICE_TAB),
        origin_node(&alice(), ALICE_LAPTOP),
        origin_node(&alice(), PHONE),
    );
    assert_eq!(entries_of(&path, tab), (true, 2));

    core.set_clock_for_test(Some(t0 + 61 * MINUTE));
    assert!(written(update_as(
        &core,
        ALICE_LAPTOP,
        1,
        "presence",
        row(1),
        "laptop"
    )));
    assert_eq!(entries_of(&path, tab), (true, 1));
    assert_eq!(entries_of(&path, laptop), (true, 1));

    // The entry-age table as stored: its DDL, and the laptop's entry at the
    // authority's clock when it recorded it.
    let conn = rusqlite::Connection::open(&path).unwrap();
    let ddl: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = '_jazz_origin_entries'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        ddl,
        "CREATE TABLE _jazz_origin_entries (at INTEGER NOT NULL, n INTEGER NOT NULL, \
         tx INTEGER NOT NULL, PRIMARY KEY (at, n, tx)) WITHOUT ROWID"
    );
    let ages: Vec<(i64, Vec<u8>, i64)> = conn
        .prepare(
            "SELECT e.at, n.node, e.tx FROM _jazz_origin_entries AS e \
             JOIN _jazz_nodes AS n ON n.n = e.n ORDER BY e.at",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ages, vec![(t0 + 61 * MINUTE, laptop.to_vec(), 1)]);
    drop(conn);

    core.set_clock_for_test(Some(t0 + 122 * MINUTE));
    assert!(written(update_as(
        &core,
        PHONE,
        1,
        "presence",
        row(1),
        "phone"
    )));
    assert_eq!(entries_of(&path, laptop), (false, 0));
    assert_eq!(entries_of(&path, tab), (true, 1));
    assert_eq!(entries_of(&path, phone), (true, 1));
    assert!(ORIGIN_ENTRY_RETENTION_MS == 60 * MINUTE);
}

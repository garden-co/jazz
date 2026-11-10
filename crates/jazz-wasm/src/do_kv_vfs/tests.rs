//! The VFS on a double of a Durable Object's synchronous KV storage, through
//! the same SQLite the native build links (the wasm build's is the same
//! amalgamation). A workerd smoke test runs it on the real storage
//! (`tests/durable-object-kv`).
//!
//! These are lower-level than Jazz's usual client tests on purpose: the
//! contracts are the VFS's (what storage holds after each commit, failed
//! commit or reopen, and its byte layout), which no client API shows. The
//! last test opens Jazz's engine on it through its public API.

use std::collections::BTreeMap;
use std::sync::Mutex;

use rusqlite::{Connection, OpenFlags};

use super::*;

const VFS: &str = "jazz-do-kv-test";

/// The VFS state is single-threaded (as a Durable Object is); tests run one
/// at a time.
static SERIAL: Mutex<()> = Mutex::new(());

struct NativeOs;

impl OsCallback for NativeOs {
    fn sleep(dur: Duration) {
        std::thread::sleep(dur);
    }

    fn random(buf: &mut [u8]) {
        // Names temporary files only.
        static STATE: Mutex<u64> = Mutex::new(0x9e37_79b9_7f4a_7c15);
        let mut state = STATE.lock().unwrap();
        for byte in buf {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *byte = *state as u8;
        }
    }

    fn epoch_timestamp_in_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

/// One object's storage: a sorted map, with commits that can be made to
/// fail.
#[derive(Default)]
struct MemKv {
    data: RefCell<BTreeMap<String, Vec<u8>>>,
    fail_commits: Cell<bool>,
    commits: Cell<u32>,
}

impl PageKv for MemKv {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.data.borrow().get(key).cloned())
    }

    fn commit(&self, puts: &[(String, &[u8])], deletes: &[String]) -> Result<(), String> {
        if self.fail_commits.get() {
            return Err("storage is full (test)".into());
        }
        let mut data = self.data.borrow_mut();
        for (key, value) in puts {
            data.insert(key.clone(), value.to_vec());
        }
        for key in deletes {
            data.remove(key);
        }
        self.commits.set(self.commits.get() + 1);
        Ok(())
    }
}

impl MemKv {
    fn snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        self.data.borrow().clone()
    }

    fn page_keys(&self, name: &str) -> usize {
        let prefix = format!("jazz-vfs:{name}:p:");
        self.data
            .borrow()
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .count()
    }
}

fn util() -> KvVfsUtil {
    install::<NativeOs>(VFS).unwrap()
}

fn open(path: &str, page_size: u32) -> Connection {
    open_journal(path, page_size, "MEMORY")
}

/// As Jazz opens a file (any rollback journal is safe on this VFS).
fn open_journal(path: &str, page_size: u32, journal: &str) -> Connection {
    let conn = Connection::open_with_flags_and_vfs(path, OpenFlags::default(), VFS).unwrap();
    conn.execute_batch(&format!("PRAGMA page_size = {page_size};"))
        .unwrap();
    let mode: String = conn
        .query_row(&format!("PRAGMA journal_mode = {journal}"), [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(mode.eq_ignore_ascii_case(journal), "{mode}");
    conn.execute_batch("PRAGMA synchronous = OFF; PRAGMA cache_spill = OFF;")
        .unwrap();
    conn
}

fn attach(util: &KvVfsUtil, path: &str, kv: &Rc<MemKv>) {
    util.attach(path, kv.clone(), "store").unwrap();
}

fn count(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap()
}

/// A file written, then reopened from storage alone (a new isolate), holds
/// every committed row, and attaching it reads nothing but its metadata.
#[test]
fn a_file_reopens_from_storage_reading_only_its_metadata() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/reopen.sqlite";
    attach(&util, path, &kv);
    {
        let conn = open(path, 4096);
        conn.execute_batch("CREATE TABLE t (x TEXT)").unwrap();
        for i in 0..200 {
            conn.execute("INSERT INTO t VALUES (?1)", [format!("row {i:04}")])
                .unwrap();
        }
    }
    util.detach(path);

    // A fresh isolate: the VFS keeps nothing but what storage holds.
    attach(&util, path, &kv);
    assert_eq!(
        util.stats(path).unwrap().gets,
        1,
        "attach reads only the metadata"
    );
    let conn = open(path, 4096);
    assert_eq!(count(&conn), 200);
    let last: String = conn
        .query_row("SELECT x FROM t ORDER BY x DESC LIMIT 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(last, "row 0199");
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    drop(conn);
    util.detach(path);
}

/// Each SQLite commit is one storage commit of the pages it changed (and the
/// metadata only if the size changed); reads write nothing.
#[test]
fn each_commit_is_one_storage_commit_of_only_the_pages_it_changed() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/pages.sqlite";
    attach(&util, path, &kv);
    let conn = open(path, 4096);
    conn.execute_batch("CREATE TABLE t (x BLOB)").unwrap();
    for _ in 0..100 {
        conn.execute("INSERT INTO t VALUES (randomblob(3000))", [])
            .unwrap();
    }
    let pages = kv.page_keys("store");
    assert!(pages > 100, "{pages} pages");

    let before = util.stats(path).unwrap();
    let commits = kv.commits.get();
    conn.execute("INSERT INTO t VALUES (x'00')", []).unwrap();
    let after = util.stats(path).unwrap();
    assert_eq!(
        kv.commits.get(),
        commits + 1,
        "one storage commit per SQLite commit"
    );
    // The last leaf, page 1 (change counter) and the metadata: never the
    // whole file.
    let puts = after.puts - before.puts;
    assert!((2..=4).contains(&puts), "{puts} puts for a one-row insert");

    // A commit that keeps the size writes no metadata: page 1 (its change
    // counter) and the one leaf.
    let before = util.stats(path).unwrap();
    conn.execute("UPDATE t SET x = x'02' WHERE rowid = 1", [])
        .unwrap();
    assert_eq!(util.stats(path).unwrap().puts - before.puts, 2);

    // A transaction of many statements is still one storage commit.
    conn.execute_batch(
        "BEGIN; INSERT INTO t VALUES (x'01'); INSERT INTO t VALUES (x'02'); COMMIT;",
    )
    .unwrap();
    assert_eq!(kv.commits.get(), commits + 3);
    // A read-only statement writes nothing.
    assert_eq!(count(&conn), 103);
    assert_eq!(kv.commits.get(), commits + 3);
    drop(conn);
    util.detach(path);
}

/// A storage commit that fails fails the SQLite commit, leaves storage at the
/// commit before, and the connection goes on from there, for every rollback
/// journal mode:
///
/// ```text
/// insert 1 ──commit──► storage {1}
/// insert 2,3 ──commit──✗ (storage full) ──► storage {1}, reads {1}
/// insert 4 ──commit──► storage {1, 4}
/// ```
#[test]
fn a_failed_storage_commit_fails_the_sqlite_commit_and_leaves_the_last_one() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for journal in ["MEMORY", "TRUNCATE", "DELETE", "PERSIST"] {
        failed_commit(journal);
    }
}

fn failed_commit(journal: &str) {
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = &format!("/failed-{journal}.sqlite");
    attach(&util, path, &kv);
    let conn = open_journal(path, 4096, journal);
    conn.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)", []).unwrap();
    let stored = kv.snapshot();

    kv.fail_commits.set(true);
    let error = conn
        .execute_batch("BEGIN; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); COMMIT;")
        .unwrap_err();
    assert!(
        matches!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::SystemIoFailure)
        ),
        "{journal}: {error}"
    );
    assert_eq!(kv.snapshot(), stored, "storage holds the commit before");
    // A transaction may still be open on the connection after the error.
    let _ = conn.execute_batch("ROLLBACK");
    // The same connection reads the file as storage holds it.
    assert_eq!(count(&conn), 1);

    kv.fail_commits.set(false);
    conn.execute("INSERT INTO t VALUES (4)", []).unwrap();
    drop(conn);
    util.detach(path);
    attach(&util, path, &kv);
    let conn = open_journal(path, 4096, journal);
    let rows: Vec<i64> = conn
        .prepare("SELECT x FROM t ORDER BY x")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(rows, [1, 4], "{journal}");
    drop(conn);
    util.detach(path);
}

/// A new file whose first commit fails stays empty in storage, and the next
/// commit creates it.
#[test]
fn a_failed_first_commit_leaves_a_new_file_empty() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/failed-first.sqlite";
    attach(&util, path, &kv);
    let conn = open(path, 4096);
    kv.fail_commits.set(true);
    conn.execute_batch("CREATE TABLE t (x INTEGER)")
        .unwrap_err();
    assert!(kv.snapshot().is_empty());
    kv.fail_commits.set(false);
    conn.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();
    drop(conn);
    util.detach(path);
    attach(&util, path, &kv);
    let conn = open(path, 4096);
    assert_eq!(count(&conn), 1);
    drop(conn);
    util.detach(path);
}

/// Shrinking the file (VACUUM) deletes the pages past its end from storage,
/// and growing it again never reads one of them back.
#[test]
fn shrinking_the_file_deletes_the_pages_it_dropped() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/vacuum.sqlite";
    attach(&util, path, &kv);
    let conn = open(path, 4096);
    conn.execute_batch("CREATE TABLE t (x BLOB)").unwrap();
    conn.execute(
        "INSERT INTO t SELECT randomblob(2000) FROM generate_series(1, 300)",
        [],
    )
    .or_else(|_| {
        // Without the series extension.
        conn.execute_batch(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300) \
                 INSERT INTO t SELECT randomblob(2000) FROM n",
        )
        .map(|_| 0)
    })
    .unwrap();
    let grown = kv.page_keys("store");
    conn.execute_batch("DELETE FROM t WHERE rowid > 10; VACUUM;")
        .unwrap();
    let shrunk = kv.page_keys("store");
    let file_pages: i64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    assert!(shrunk < grown / 4, "{grown} -> {shrunk}");
    assert_eq!(shrunk as i64, file_pages, "no page beyond the file is left");

    // Growing again over the dropped range reads no stale page.
    conn.execute_batch(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100) \
         INSERT INTO t SELECT zeroblob(2000) FROM n",
    )
    .unwrap();
    drop(conn);
    util.detach(path);
    attach(&util, path, &kv);
    let conn = open(path, 4096);
    assert_eq!(count(&conn), 110);
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    drop(conn);
    util.detach(path);
}

/// A second connection on the file (Jazz opens a reader) sees each commit
/// of the first.
#[test]
fn a_second_connection_reads_what_the_first_committed() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/two.sqlite";
    attach(&util, path, &kv);
    let writer = open(path, 4096);
    writer.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
    let reader =
        Connection::open_with_flags_and_vfs(path, OpenFlags::SQLITE_OPEN_READ_ONLY, VFS).unwrap();
    assert_eq!(count(&reader), 0);
    writer.execute("INSERT INTO t VALUES (1)", []).unwrap();
    assert_eq!(count(&reader), 1);
    drop((writer, reader));
    util.detach(path);
}

/// A file not attached to storage cannot be opened, and WAL is not
/// available (SQLite keeps its rollback journal).
#[test]
fn wal_and_unattached_files_are_refused() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/wal.sqlite";

    let error = Connection::open_with_flags_and_vfs(path, OpenFlags::default(), VFS)
        .and_then(|conn| conn.execute_batch("CREATE TABLE t (x)"))
        .unwrap_err();
    assert!(error.to_string().contains("unable to open"), "{error}");

    attach(&util, path, &kv);
    let conn = Connection::open_with_flags_and_vfs(path, OpenFlags::default(), VFS).unwrap();
    // No shared memory: SQLite keeps its rollback journal.
    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    drop(conn);
    util.detach(path);
}

/// Destroying a file deletes all of its keys and none of the object's
/// others.
#[test]
fn destroy_deletes_every_key_of_the_file_and_only_those() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    kv.data
        .borrow_mut()
        .insert("app-key".into(), b"kept".to_vec());
    let path = "/destroy.sqlite";
    attach(&util, path, &kv);
    let conn = open(path, 4096);
    conn.execute_batch("CREATE TABLE t (x BLOB); INSERT INTO t VALUES (randomblob(20000));")
        .unwrap();
    drop(conn);
    assert!(kv.data.borrow().len() > 3);
    util.destroy(path).unwrap();
    assert_eq!(kv.snapshot().into_keys().collect::<Vec<_>>(), ["app-key"]);
    assert!(util.stats(path).is_none(), "destroy unbinds the file");
}

/// Metadata of a later format version is refused, not misread.
#[test]
fn metadata_it_does_not_know_is_refused() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let mut meta = [0u8; 16];
    meta[0..4].copy_from_slice(&2u32.to_le_bytes());
    kv.data
        .borrow_mut()
        .insert("jazz-vfs:store:meta".into(), meta.to_vec());
    let error = util
        .attach("/future.sqlite", kv.clone(), "store")
        .unwrap_err();
    assert!(error.contains("version 2 is not supported yet"), "{error}");
}

/// Random transactions, some rolled back, some failing in storage, with
/// VACUUMs and reopens in between: the file always reads exactly the rows
/// committed so far.
#[test]
fn random_transactions_leave_storage_at_the_last_committed_state() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/random.sqlite";
    attach(&util, path, &kv);
    let mut conn = open(path, 1024);
    conn.execute_batch("CREATE TABLE t (k INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    let mut model: BTreeMap<i64, Vec<u8>> = BTreeMap::new();
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    for round in 0..300 {
        let mut staged = model.clone();
        let tx = conn.transaction().unwrap();
        for _ in 0..1 + next(8) {
            let key = next(400) as i64;
            if next(4) == 0 {
                tx.execute("DELETE FROM t WHERE k = ?1", [key]).unwrap();
                staged.remove(&key);
            } else {
                let value = vec![round as u8; next(3000) as usize];
                tx.execute(
                    "INSERT OR REPLACE INTO t VALUES (?1, ?2)",
                    rusqlite::params![key, value],
                )
                .unwrap();
                staged.insert(key, value);
            }
        }
        match next(10) {
            0 => tx.rollback().unwrap(),
            1 => {
                kv.fail_commits.set(true);
                let committed = tx.commit();
                kv.fail_commits.set(false);
                if committed.is_ok() {
                    // Nothing changed (deletes of missing rows): no write.
                    assert_eq!(staged, model, "round {round}");
                }
                let _ = conn.execute_batch("ROLLBACK");
            }
            _ => {
                tx.commit().unwrap();
                model = staged;
            }
        }
        if next(20) == 0 {
            conn.execute_batch("VACUUM").unwrap();
        }
        if next(15) == 0 {
            drop(conn);
            util.detach(path);
            attach(&util, path, &kv);
            conn = open(path, 1024);
        }
        let rows: BTreeMap<i64, Vec<u8>> = conn
            .prepare("SELECT k, v FROM t")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows, model, "round {round}");
    }
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    drop(conn);
    util.detach(path);
}

/// Jazz's engine opens on the VFS the way `do_kv::open` does (4 KiB pages,
/// in-memory journal), and a store reopened from storage alone reads back
/// every row; WAL is refused at open.
#[test]
fn a_jazz_store_reopens_from_storage() {
    use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, Journal, ReadOpts};
    use jazz::ids::{AuthorSubject, NodeUuid};
    use jazz::query::Query;
    use jazz::records::Value;
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/jazz.sqlite";
    let config = || {
        let policies = TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True);
        DbConfig::new(
            JazzSchema::new(
                &SchemaBuilder::new()
                    .table(
                        TableSchemaBuilder::new("todos")
                            .column("title", ColumnType::Text)
                            .policies(policies),
                    )
                    .build(),
            )
            .unwrap(),
            DbIdentity {
                node: NodeUuid::from_bytes([7; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    let options = || FileOptions {
        vfs: Some(VFS),
        journal: Journal::Memory,
        page_size: Some(PAGE_SIZE),
        ..FileOptions::default()
    };
    let titles = |db: &Db| -> Vec<String> {
        let prepared = db.prepare_query(&Query::from("todos")).unwrap();
        let mut titles: Vec<String> = block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .filter_map(|row| match row.cells().find(|(name, _)| *name == "title") {
                Some((_, Some(Value::String(title)))) => Some(title.clone()),
                _ => None,
            })
            .collect();
        titles.sort();
        titles
    };
    use jazz::db::block_on;

    attach(&util, path, &kv);
    {
        let db = block_on(Db::open_at(config(), std::path::Path::new(path), options())).unwrap();
        for i in 0..20 {
            block_on(db.insert(
                "todos",
                BTreeMap::from([("title".to_owned(), Value::String(format!("todo {i:02}")))]),
                Default::default(),
            ))
            .unwrap();
        }
        block_on(db.close()).unwrap();
    }
    util.detach(path);

    attach(&util, path, &kv);
    let db = block_on(Db::open_at(config(), std::path::Path::new(path), options())).unwrap();
    let titles = titles(&db);
    assert_eq!(titles.len(), 20);
    assert_eq!(titles[0], "todo 00");
    block_on(db.close()).unwrap();
    drop(db);

    // WAL is refused at open, not at the first write.
    let Err(error) = block_on(Db::open_at(
        config(),
        std::path::Path::new(path),
        FileOptions {
            journal: Journal::Wal,
            ..options()
        },
    )) else {
        panic!("WAL must be refused");
    };
    assert!(error.to_string().contains("not supported"), "{error}");
    util.detach(path);
}

/// The storage layout is a durable encoding: version 1, pinned byte for byte.
///
/// ```text
/// jazz-vfs:{name}:meta        01000000 00100000 0020000000000000
///                             version  page     size (2 pages)
/// jazz-vfs:{name}:p:00000000  page 0 (4096 bytes)
/// jazz-vfs:{name}:p:00000001  page 1
/// ```
#[test]
fn the_storage_layout_is_pinned() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let util = util();
    let kv = Rc::new(MemKv::default());
    let path = "/layout.sqlite";
    util.attach(path, kv.clone(), "pinned").unwrap();
    let conn = open(path, 4096);
    conn.execute_batch("CREATE TABLE t (x)").unwrap();
    drop(conn);
    util.detach(path);
    let data = kv.snapshot();
    assert_eq!(
        data.keys().collect::<Vec<_>>(),
        [
            "jazz-vfs:pinned:meta",
            "jazz-vfs:pinned:p:00000000",
            "jazz-vfs:pinned:p:00000001",
        ]
    );
    assert_eq!(
        data["jazz-vfs:pinned:meta"],
        [1, 0, 0, 0, 0, 0x10, 0, 0, 0, 0x20, 0, 0, 0, 0, 0, 0]
    );
    let page0 = &data["jazz-vfs:pinned:p:00000000"];
    assert_eq!(page0.len(), 4096);
    assert_eq!(
        &page0[..16],
        b"SQLite format 3\0",
        "page 0 is the file's first page"
    );
    assert_eq!(data["jazz-vfs:pinned:p:00000001"].len(), 4096);

    // Page keys above 0xff sort as page order.
    assert!(page_key("n", 0xff) < page_key("n", 0x100));
    assert_eq!(page_key("n", 0x1234_5678), "jazz-vfs:n:p:12345678");
}

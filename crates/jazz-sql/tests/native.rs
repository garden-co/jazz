#![cfg(feature = "native")]

use jazz_sql::{Cell, HostTable, Param, Row, Sql, inline, native::Native};
use std::ops::ControlFlow;

/// Alice reads every SQLite cell type through the borrowed row interface.
/// This low-level test checks lossless TEXT bytes and integer precision that
/// higher-level Jazz values cannot express, and verifies immediate completion.
#[test]
fn native_rows_borrow_lossless_sqlite_cells() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let mut sql = Native(&conn);
    let mut seen = 0;
    inline(sql.query(
        "SELECT ?1, ?2, ?3, ?4, ?5, ?6",
        &[
            Param::Null,
            Param::Integer(i64::MAX),
            Param::Real(1.25),
            Param::Text("a\0é"),
            Param::Blob(&[0, 255]),
            Param::TextBytes(&[255, 0, 97]),
        ],
        |row| {
            assert_eq!(row.len(), 6);
            assert_eq!(row.cell(0), Some(Cell::Null));
            assert_eq!(row.cell(1), Some(Cell::Integer(i64::MAX)));
            assert_eq!(row.cell(2), Some(Cell::Real(1.25)));
            assert_eq!(row.cell(3), Some(Cell::Text("a\0é".as_bytes())));
            assert_eq!(row.cell(4), Some(Cell::Blob(&[0, 255])));
            assert_eq!(row.cell(5), Some(Cell::Text(&[255, 0, 97])));
            assert_eq!(row.cell(6), None);
            assert!(row.get::<String>(5).is_err());
            assert!(row.get::<i64>(6).is_err());
            assert_eq!(row.get::<Option<i64>>(0).unwrap(), None);
            assert_eq!(row.get::<i64>(1).unwrap(), i64::MAX);
            assert!(row.get::<i32>(1).is_err());
            assert_eq!(row.get::<f64>(2).unwrap(), 1.25);
            seen += 1;
            Ok(ControlFlow::Continue(()))
        },
    ))
    .unwrap();
    assert_eq!(seen, 1);
}

/// Alice stops visiting after the first row; Bob can immediately reuse the
/// cached statement. The low-level callback protocol is not exposed by Db.
#[test]
fn stopping_a_query_releases_its_statement() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let mut sql = Native(&conn);
    for limit in [1, 3] {
        let mut seen = 0;
        inline(sql.query(
            "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
            &[],
            |_| {
                seen += 1;
                Ok(if seen == limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            },
        ))
        .unwrap();
        assert_eq!(seen, limit);
    }
}

/// Alice writes keys on both sides of binary prefix boundaries; Bob sees only
/// matching keys. This low-level test runs the exact provider-shared SQL and
/// checks its all-FF and empty-prefix bounds without a Jazz schema layer.
#[test]
fn binary_prefixes_and_batch_updates() {
    inline(async {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut sql = Native(&conn);
        let table = HostTable::new("prefix_probe");
        let pairs: Vec<_> = [b"".as_slice(), &[0], &[0, 255], &[1], &[255], &[255, 255]]
            .into_iter()
            .map(|key| (key.to_vec(), key.to_vec()))
            .collect();
        sql.execute("BEGIN", &[]).await.unwrap();
        jazz_sql::put_many_in_transaction(&mut sql, table, &pairs)
            .await
            .unwrap();
        sql.execute("COMMIT", &[]).await.unwrap();
        for prefix in [
            b"".as_slice(),
            &[0],
            &[0, 255],
            &[1],
            &[255],
            &[255, 255],
            &[2],
        ] {
            let expected: Vec<_> = pairs
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .cloned()
                .collect();
            assert_eq!(
                jazz_sql::prefix(&mut sql, table, prefix).await.unwrap(),
                expected
            );
        }
        jazz_sql::delete(&mut sql, table, &[0]).await.unwrap();
        assert_eq!(jazz_sql::get(&mut sql, table, &[0]).await.unwrap(), None);
        assert_eq!(
            jazz_sql::get(&mut sql, table, &[0, 255]).await.unwrap(),
            Some(vec![0, 255])
        );
    });
}

/// Alice adds a column after Bob's SELECT * was cached. Sync image decoding
/// must see the re-prepared column names and distinguish NULL from no row.
/// This low-level contract is shared by sync's provider-independent decoder.
#[test]
fn cached_row_metadata_tracks_schema_changes() {
    inline(async {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut sql = Native(&conn);
        sql.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap();
        sql.execute("INSERT INTO items VALUES (1)", &[])
            .await
            .unwrap();
        let names = |row: jazz_sql::native::NativeRow<'_>| {
            Ok((0..row.len())
                .map(|i| row.column_name(i).unwrap().to_owned())
                .collect::<Vec<_>>())
        };
        assert_eq!(
            sql.query_one("SELECT * FROM items WHERE id = 1", &[], names)
                .await
                .unwrap(),
            ["id"]
        );
        sql.execute("ALTER TABLE items ADD COLUMN body TEXT", &[])
            .await
            .unwrap();
        assert_eq!(
            sql.query_one("SELECT * FROM items WHERE id = 1", &[], names)
                .await
                .unwrap(),
            ["id", "body"]
        );
        assert_eq!(
            sql.query_optional("SELECT body FROM items WHERE id = 1", &[], |r| r
                .get::<Option<String>>(0))
                .await
                .unwrap(),
            Some(None)
        );
        assert_eq!(
            sql.query_optional("SELECT body FROM items WHERE id = 2", &[], |r| r
                .get::<Option<String>>(0))
                .await
                .unwrap(),
            None
        );
    });
}

/// Alice's schema script contains a trigger body and semicolons in literals.
/// Bob sees its effects inside the caller's transaction, and none after rollback.
/// The provider-level test guards parsing and transaction ownership, not Jazz DDL.
#[test]
fn scripts_preserve_triggers_and_the_callers_transaction() {
    inline(async {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut sql = Native(&conn);
        sql.execute("BEGIN", &[]).await.unwrap();
        sql.execute_script(
            "CREATE TABLE notes (body TEXT); CREATE TABLE audit (body TEXT);
            CREATE TRIGGER note_added AFTER INSERT ON notes BEGIN
                INSERT INTO audit VALUES ('first;entry');
                INSERT INTO audit VALUES (NEW.body);
            END;
            INSERT INTO notes VALUES ('hello;world')",
        )
        .await
        .unwrap();
        assert_eq!(
            sql.query_rows("SELECT body FROM audit ORDER BY rowid", &[], |row| row
                .get::<String>(
                0
            ))
            .await
            .unwrap(),
            ["first;entry", "hello;world"]
        );
        sql.execute("ROLLBACK", &[]).await.unwrap();
        assert_eq!(
            sql.query_one(
                "SELECT count(*) FROM sqlite_master WHERE name IN ('notes', 'audit', 'note_added')",
                &[],
                |row| row.get::<i64>(0)
            )
            .await
            .unwrap(),
            0
        );
    });
}

/// Alice's failed statement rolls back her whole transaction. The native
/// provider reports engine state, its configured parameter bound, and change
/// counts without suspending. These facts are below the row-level Db API.
#[test]
fn connection_facts_follow_sqlite_even_after_an_automatic_rollback() {
    use jazz_sql::Connection as _;
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 100)
        .unwrap();
    let mut sql = Native(&conn);
    assert_eq!(sql.parameter_limit().unwrap(), 100);
    assert!(!sql.transaction_open());
    inline(sql.execute_script("CREATE TABLE notes (id INTEGER PRIMARY KEY); SAVEPOINT alice"))
        .unwrap();
    assert!(sql.transaction_open());
    inline(sql.execute("INSERT INTO notes VALUES (1)", &[])).unwrap();
    assert_eq!(inline(sql.total_changes()).unwrap(), 1);
    assert!(inline(sql.execute("INSERT OR ROLLBACK INTO notes VALUES (1)", &[])).is_err());
    assert!(!sql.transaction_open());
    assert_eq!(
        inline(sql.query_one::<i64, _>("SELECT count(*) FROM notes", &[], |r| r.get(0))).unwrap(),
        0
    );
    // SQLite's counter includes the insert even though it was rolled back.
    assert_eq!(inline(sql.total_changes()).unwrap(), 1);
}

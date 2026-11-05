//! Physical format tests: application reads cannot observe index keys or
//! trigger atomicity, so these use a private SQLite fixture for the derived role.
use super::*;
use jazz_model::model::public_schema::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_model::schema::JazzSchema;

fn schema() -> Schema {
    Schema::new(
        &JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("docs").column("title", ColumnType::Text))
                .table(TableSchemaBuilder::new("grants").array_fk_column("targets", "docs"))
                .build(),
        )
        .unwrap(),
    )
}
fn connection() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE grants(id BLOB NOT NULL, _b BLOB NOT NULL, _seq INTEGER NOT NULL, targets TEXT, PRIMARY KEY(id,_b));
         CREATE TABLE grants__h(id BLOB NOT NULL, _b BLOB NOT NULL, _seq INTEGER NOT NULL, targets TEXT, PRIMARY KEY(id,_b,_seq));"
    ).unwrap();
    conn
}
fn key_rows(conn: &Connection) -> Vec<(String, i64, String, String, i64)> {
    conn.prepare("SELECT hex(member),kind,hex(id),hex(branch),seq FROM \"_jazz_array_members_v1_6:grants_7:targets\" ORDER BY member,kind,id,branch,seq")
        .unwrap().query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))
        .unwrap().collect::<rusqlite::Result<_>>().unwrap()
}
fn put(conn: &Connection, history: bool, seq: i64, targets: &str) {
    let table = if history { "grants__h" } else { "grants" };
    conn.execute(
        &format!("INSERT INTO {table}(id,_b,_seq,targets) VALUES(?1,?2,?3,?4)"),
        rusqlite::params![vec![0x11u8; 16], Vec::<u8>::new(), seq, targets],
    )
    .unwrap();
}
const TARGETS: &str =
    "[\"22222222-2222-2222-2222-222222222222\",\"22222222-2222-2222-2222-222222222222\",null]";

/// Alice's derived index has fixed DDL bytes and raw UUID keys. Physical
/// inspection is necessary to pin this durable role independently of queries.
#[test]
fn uuid_array_index_v1_pins_ddl_and_member_bytes() {
    let schema = schema();
    let table = schema.table("grants").unwrap();
    let name = name(table, "targets").unwrap();
    assert_eq!(
        ddl(table, "targets", &name).as_bytes(),
        include_bytes!("../../../fixtures/sqlite_uuid_array_index_v1.sql")
    );
    let conn = connection();
    install(&conn, &schema).unwrap();
    put(&conn, false, 7, TARGETS);
    assert_eq!(
        key_rows(&conn),
        vec![("22".repeat(16), 0, "11".repeat(16), String::new(), 7)]
    );
    let plan: String = conn.query_row("EXPLAIN QUERY PLAN SELECT id FROM \"_jazz_array_members_v1_6:grants_7:targets\" WHERE member=?1 AND kind=0",[vec![0x22;16]],|r|r.get(3)).unwrap();
    assert!(
        plan.contains("SEARCH") && plan.contains("PRIMARY KEY"),
        "{plan}"
    );
}

/// Alice moves a current image to history while Bob changes its members.
/// Trigger writes share the enclosing transaction and exact version lifetime;
/// low-level inspection is needed to distinguish current and historical keys.
#[test]
fn membership_tracks_history_updates_deletes_and_rollback_atomically() {
    let conn = connection();
    install(&conn, &schema()).unwrap();
    put(&conn, false, 7, TARGETS);
    put(&conn, true, 7, TARGETS);
    conn.execute(
        "UPDATE grants SET _seq=8,targets=?1",
        ["[\"33333333-3333-3333-3333-333333333333\"]"],
    )
    .unwrap();
    assert_eq!(
        key_rows(&conn),
        vec![
            ("22".repeat(16), 1, "11".repeat(16), String::new(), 7),
            ("33".repeat(16), 0, "11".repeat(16), String::new(), 8),
        ]
    );
    let accepted = key_rows(&conn);
    conn.execute_batch("SAVEPOINT rejected; UPDATE grants SET _seq=9,targets='[]'; DELETE FROM grants__h; ROLLBACK TO rejected; RELEASE rejected;").unwrap();
    assert_eq!(key_rows(&conn), accepted);
    conn.execute_batch("DELETE FROM grants__h").unwrap();
    assert_eq!(
        key_rows(&conn),
        vec![("33".repeat(16), 0, "11".repeat(16), String::new(), 8)]
    );
    conn.execute_batch("UPDATE grants SET targets=NULL; DELETE FROM grants")
        .unwrap();
    assert!(key_rows(&conn).is_empty());
}

/// Alice's pre-index current and historical rows are backfilled once. A failed
/// installation leaves no marker or partial triggers; Bob can retry safely.
/// Physical fixture control is needed to inject a DDL failure without hooks.
#[test]
fn installation_backfills_once_and_rolls_back_partial_ddl() {
    let conn = connection();
    let schema = schema();
    put(&conn, false, 8, TARGETS);
    put(&conn, true, 7, TARGETS);
    conn.execute_batch("CREATE TRIGGER \"_jazz_array_members_v1_6:grants_7:targets_0_update\" AFTER UPDATE ON grants BEGIN SELECT 1; END;").unwrap();
    assert!(install(&conn, &schema).is_err());
    let partial:i64=conn.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE name='_jazz_array_members_v1_6:grants_7:targets'",[],|r|r.get(0)).unwrap();
    assert_eq!(partial, 0);
    conn.execute_batch("DROP TRIGGER \"_jazz_array_members_v1_6:grants_7:targets_0_update\"")
        .unwrap();
    install(&conn, &schema).unwrap();
    assert_eq!(
        key_rows(&conn),
        vec![
            ("22".repeat(16), 0, "11".repeat(16), String::new(), 8),
            ("22".repeat(16), 1, "11".repeat(16), String::new(), 7),
        ]
    );
    let before = conn.total_changes();
    install(&conn, &schema).unwrap();
    assert_eq!(conn.total_changes(), before);
    assert_eq!(
        members(&Sql::Text(TARGETS.as_bytes().to_vec())).unwrap(),
        vec![Sql::Blob(vec![0x22; 16])]
    );
    assert!(members(&Sql::Text(b"[]".to_vec())).unwrap().is_empty());
    assert!(members(&Sql::Null).unwrap().is_empty());
    assert!(members(&Sql::Text(b"[7]".to_vec())).is_err());
}

//! A backend holds no encryption keys (`E2EE_PROTOCOL.md` §4): it relays
//! the clients' ciphertext, runs code on the plain columns of an encrypted
//! table and on plain tables, and never answers with, writes or filters on an
//! encrypted cell.

mod common;

use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, DbConfig, FileOptions, ReadOpts, ReadTier, block_on};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee_client::{KeylessHooks, MemoryStore};

/// A read that fetches from the server, which the test drives.
fn fetch(server: &Db, db: &Db, query: Query) -> Result<Vec<jazz::db::CurrentRow>, jazz::db::Error> {
    let prepared = db.prepare_query(&query)?;
    let live = block_on(db.subscribe(&prepared, Default::default()))?;
    pump(server, db);
    let rows = block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly)));
    drop(live);
    rows
}

const SECRET: &str = "PLAINTEXT-ONLY-ALICE-KNOWS";

/// Contract: the backend relays alice's encrypted row. It reads the row's
/// plain columns but never its encrypted ones (no ciphertext either); it
/// reads and writes plain tables and plain columns as usual; a write naming
/// an encrypted column, and a filter, ordering or selection of one, are
/// refused. Alice's secret never reaches the backend's store as plaintext.
/// Alice sees what the backend writes to plain columns, including one whose
/// name resembles an equality token for an unindexed field.
///
/// ```text
/// alice ──sealed row──► server ──► backend (keyless hooks)
///                                     │ reads title, plainNote; not body
///                                     │ writes plainNote ──► server ──► alice
///                                     └ writes body ✗ (refused, nothing stored)
/// ```
#[test]
fn a_keyless_backend_relays_ciphertext_and_works_on_plain_columns() {
    const PLAIN_COLUMN: &str = "__e2ee_eq_note";
    let plain_bytes = |bytes: &[u8]| Value::Nullable(Some(Box::new(Value::Bytes(bytes.to_vec()))));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("backend.sqlite");
    let server = authority();
    let alice_client = open_client(Rc::new(MemoryStore::new()));
    let (alice, _link, _wire) = online(&alice_client, &server, None);
    let space = project(&alice, "plans");
    hold_new_space(&alice_client, &alice, space);
    let mut cells = note(space, SECRET);
    cells.insert("plainNote".to_owned(), text("hello"));
    cells.insert(PLAIN_COLUMN.to_owned(), plain_bytes(b"hello"));
    let row = block_on(alice.insert("notes", cells, Default::default()))
        .unwrap()
        .row_uuid();
    let prepared = alice.prepare_query(&Query::from("notes")).unwrap();
    let _live = block_on(alice.subscribe(&prepared, Default::default())).unwrap();
    pump(&server, &alice);
    assert_eq!(
        all(&server, Query::from("notes")).unwrap().len(),
        1,
        "alice's row reached the server"
    );

    let config =
        DbConfig::new(schema(), identity(2)).with_e2ee(KeylessHooks::hooks(declarations()));
    let backend = block_on(Db::open_at(config, &path, FileOptions::default())).unwrap();
    let (_upstream, _wire) = link(&backend, &server);
    pump(&server, &backend);

    // Plain columns of the encrypted table's row, and a plain table.
    let rows = fetch(&server, &backend, Query::from("notes")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("title"), Some(text("title")));
    assert_eq!(
        rows[0].cell("plainNote"),
        Some(Value::Nullable(Some(Box::new(text("hello")))))
    );
    assert_eq!(rows[0].cell(PLAIN_COLUMN), Some(plain_bytes(b"hello")));
    for sealed in ["body", "note", "attachment", "rank", "__e2ee_eq_body"] {
        assert_eq!(rows[0].cell(sealed), None, "{sealed} is not answered");
    }
    assert_eq!(
        fetch(&server, &backend, Query::from("projects"))
            .unwrap()
            .len(),
        1
    );

    // A write to a plain column works and reaches alice.
    block_on(backend.update(
        "notes",
        row,
        BTreeMap::from([
            ("plainNote".to_owned(), text("from the backend")),
            (PLAIN_COLUMN.to_owned(), plain_bytes(b"from the backend")),
        ]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &backend);
    pump(&server, &alice);
    let seen = fetch(&server, &alice, Query::from("notes")).unwrap();
    assert_eq!(
        seen[0].cell("plainNote"),
        Some(Value::Nullable(Some(Box::new(text("from the backend")))))
    );
    assert_eq!(
        seen[0].cell(PLAIN_COLUMN),
        Some(plain_bytes(b"from the backend"))
    );
    assert_eq!(
        seen[0].cell("body"),
        Some(text(SECRET)),
        "alice still reads hers"
    );

    // Encrypted columns are refused, for writes and for queries.
    let refused = block_on(backend.update(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), text("BACKEND-PLAINTEXT"))]),
        Default::default(),
    ))
    .unwrap_err();
    assert!(
        refused.to_string().contains("holds no encryption keys"),
        "{refused}"
    );
    let inserted = block_on(backend.insert(
        "notes",
        {
            let mut cells = note(space, "BACKEND-PLAINTEXT");
            cells.insert("plainNote".to_owned(), text("x"));
            cells
        },
        Default::default(),
    ))
    .unwrap_err();
    assert!(
        inserted.to_string().contains("holds no encryption keys"),
        "{inserted}"
    );
    for query in [
        Query::from("notes").filter(eq(col("body"), lit(text("anything")))),
        Query::from("notes").select(["title", "body"]),
    ] {
        let error = fetch(&server, &backend, query.clone());
        assert!(error.is_err(), "refused: {query:?}");
    }
    let error = fetch(
        &server,
        &backend,
        Query::from("notes").select([PLAIN_COLUMN]),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("Reading reserved column"),
        "{error}"
    );
    pump(&server, &backend);
    let stored = stored_bytes(&path);
    assert!(
        !contains(&stored, b"BACKEND-PLAINTEXT"),
        "nothing plaintext was stored"
    );
    assert!(
        !contains(&stored, SECRET.as_bytes()),
        "alice's plaintext never reached it"
    );
}

/// Contract: a keyless backend never seals a streamed value, whatever column
/// it names (it has no key to seal under), and the refusal says why. This
/// calls the hook directly because core only reaches it for a stream write
/// the app's binding starts.
#[test]
fn a_keyless_backend_refuses_to_seal_a_stream() {
    use jazz::e2ee::{SealError, StreamTarget};
    let hooks = KeylessHooks::hooks(declarations());
    let target = StreamTarget {
        table: "notes",
        row: jazz::ids::RowUuid(uuid::Uuid::new_v4()),
        column: "attachment",
        space: None,
    };
    match hooks.seal_stream(&target) {
        Err(SealError::Refused(error)) => {
            assert!(
                error.to_string().contains("holds no encryption keys"),
                "{error}"
            );
        }
        _ => panic!("a stream was sealed or refused for another reason"),
    }
}

/// Contract: a backend of a version that keeps Jazz's system tables without
/// declaring an encrypted column (its declarations name no table) still
/// opens, with keyless hooks; the same schema is refused without hooks. The
/// open goes through the host entry point the bindings use.
#[test]
fn a_keyless_backend_opens_a_version_with_system_tables_and_no_encrypted_column() {
    use jazz::binding_host::open_memory;
    let mut none = declarations();
    none.tables.clear();
    let config = || {
        DbConfig::new(
            secure_schema_with(&jazz::e2ee::EncryptionSettings::default()),
            identity(2),
        )
    };
    let opened = block_on(open_memory(config().with_e2ee(KeylessHooks::hooks(none))));
    assert!(opened.is_ok(), "{:?}", opened.err());
}

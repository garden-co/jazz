//! Exact authority origins paired with one snapshot's projected row bodies.
use crate::common::{allow_all_policies, compile_schema, read_and_allow_all_writes};
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}
fn open(schema: JazzSchema, node: u8, authority: bool) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
    }
    db
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn body(value: &str) -> BTreeMap<String, Value> {
    [("body".into(), Value::String(value.into()))].into()
}
fn insert(db: &Db, n: u8, value: &str) -> u64 {
    block_on(db.insert(
        "notes",
        body(value),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id()
}

/// Alice's authority can issue exact evidence; Bob's ordinary local store,
/// and a requested cut ahead of Alice's accepted state, cannot.
#[test]
fn only_the_authority_can_issue_evidence_at_an_existing_coordinate() {
    let bob = open(schema(), 2, false);
    let query = bob.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        bob.accepted_read(&query, None, false, None)
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
    let alice = open(schema(), 1, true);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        alice
            .accepted_read(&query, Some(1), false, None)
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
    let empty = alice.accepted_read(&query, Some(0), false, None).unwrap();
    assert!(empty.rows.is_empty());
    assert!(empty.evidence.is_empty());
}

/// Alice deletes, restores, replaces, and deletes a note. Each deletion or
/// content-free restore keeps the preceding content's own accepted origin,
/// alongside the new operation's distinct state origin.
#[test]
fn deletes_and_restores_keep_content_and_state_origins_separate() {
    let alice = open(schema(), 1, true);
    let first = insert(&alice, 1, "first");
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let initial = alice.accepted_read(&query, None, true, None).unwrap();
    assert_eq!(initial.evidence[0].content.sequence, first);
    assert_eq!(initial.evidence[0].state, initial.evidence[0].content);
    let deleted = block_on(alice.delete("notes", id(1), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    let read = alice.accepted_read(&query, None, true, None).unwrap();
    assert!(read.rows[0].is_deleted());
    assert_eq!(
        read.rows[0].cell("body"),
        Some(Value::String("first".into()))
    );
    assert_eq!(read.evidence[0].state.sequence, deleted);
    assert_eq!(read.evidence[0].content, initial.evidence[0].content);
    assert!(
        alice
            .accepted_read(&query, None, false, None)
            .unwrap()
            .rows
            .is_empty()
    );
    let restored = block_on(alice.restore("notes", id(1), None, Default::default()))
        .unwrap()
        .mergeable_tx_id();
    let read = alice.accepted_read(&query, None, false, None).unwrap();
    assert_eq!(read.evidence[0].state.sequence, restored);
    assert_eq!(read.evidence[0].content, initial.evidence[0].content);
    let replaced = block_on(alice.update("notes", id(1), body("replacement"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    block_on(alice.delete("notes", id(1), Default::default())).unwrap();
    let read = alice.accepted_read(&query, None, true, None).unwrap();
    assert_eq!(
        read.rows[0].cell("body"),
        Some(Value::String("replacement".into()))
    );
    assert_eq!(read.evidence[0].content.sequence, replaced);
    assert_ne!(read.evidence[0].state, read.evidence[0].content);
    let restored = block_on(alice.restore(
        "notes",
        id(1),
        Some(body("new restore content")),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id();
    let read = alice.accepted_read(&query, None, false, None).unwrap();
    assert_eq!(
        read.rows[0].cell("body"),
        Some(Value::String("new restore content".into()))
    );
    assert_eq!(read.evidence[0].content.sequence, restored);
    assert_eq!(read.evidence[0].content, read.evidence[0].state);
}

/// After Alice updates her authority, an older accepted cut still pairs
/// the original body and origin. Narrow projections do not change evidence.
#[test]
fn snapshot_and_projection_cannot_substitute_a_later_origin() {
    let alice = open(schema(), 1, true);
    let first = insert(&alice, 1, "first");
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let original = alice.accepted_read(&query, None, false, None).unwrap();
    block_on(alice.update("notes", id(1), body("later"), Default::default())).unwrap();
    let older = alice
        .accepted_read(&query, Some(first), false, None)
        .unwrap();
    assert_eq!(
        older.rows[0].cell("body"),
        Some(Value::String("first".into()))
    );
    assert_eq!(older.evidence, original.evidence);
    let projected = alice
        .prepare_query(&Query::from("notes").select(["id"]))
        .unwrap();
    let narrow = alice
        .accepted_read(&projected, Some(first), false, None)
        .unwrap();
    assert!(narrow.rows[0].cell("body").is_none());
    assert_eq!(narrow.evidence, original.evidence);
}

/// Alice writes two notes atomically and a third later. Evidence preserves
/// the shared transaction and authority order independently of row-ID order.
#[test]
fn atomic_siblings_share_an_origin_and_later_content_has_a_later_sequence() {
    let alice = open(schema(), 1, true);
    let tx = OpenTransactionId::new();
    block_on(alice.begin_mergeable(tx)).unwrap();
    for n in [3, 2] {
        block_on(alice.tx_ref(tx).insert(
            "notes",
            body("same commit"),
            InsertOptions {
                row_id: Some(id(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let first = block_on(alice.commit_mergeable_handle(tx)).unwrap().0;
    let later = insert(&alice, 1, "later commit");
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let read = alice.accepted_read(&query, None, false, None).unwrap();
    let by_id: BTreeMap<_, _> = read
        .evidence
        .iter()
        .map(|item| (item.row, item.content))
        .collect();
    assert_eq!(by_id[&id(2)], by_id[&id(3)]);
    assert_eq!(by_id[&id(2)].sequence, first);
    assert_eq!(by_id[&id(1)].sequence, later);
    assert!(later > first);
    assert_ne!(by_id[&id(1)].transaction, by_id[&id(2)].transaction);
    assert_eq!(
        read.rows
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        read.evidence.iter().map(|e| e.row).collect::<Vec<_>>()
    );
}

/// Bob uploads a write to Alice's authority. Alice's evidence names Bob's
/// original node and transaction, not her own acceptance sequence as a writer.
#[test]
fn uploaded_content_keeps_the_original_writer_transaction() {
    uploaded_origin(false);
}

/// Bob uploads through a trusted relay to Alice's authority. The resulting
/// content keeps Bob's original transaction rather than the relay's upload ID.
#[test]
fn relayed_content_keeps_the_original_writer_transaction() {
    uploaded_origin(true);
}

fn uploaded_origin(relayed: bool) {
    let alice = open(schema(), 1, true);
    let bob = open(schema(), 2, false);
    let relay = open(schema(), 3, false);
    let (up, down) = duplex();
    let _connection = block_on(bob.connect_upstream(up));
    let _relay_connection = if relayed {
        relay
            .accept_subscriber(down, AuthorSubject::SYSTEM)
            .unwrap();
        let (up, down) = duplex();
        let connection = block_on(relay.connect_upstream(up));
        alice
            .accept_subscriber(down, AuthorSubject::SYSTEM)
            .unwrap();
        Some(connection)
    } else {
        alice
            .accept_subscriber(down, AuthorSubject::SYSTEM)
            .unwrap();
        None
    };
    let tx = insert(&bob, 1, "uploaded");
    for _ in 0..12 {
        block_on(bob.tick()).unwrap();
        block_on(relay.tick()).unwrap();
        block_on(alice.tick()).unwrap();
    }
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let read = alice.accepted_read(&query, None, false, None).unwrap();
    assert_eq!(read.rows.len(), 1);
    assert_eq!(
        read.evidence[0].content.transaction.node,
        NodeUuid::from_bytes([2; 16])
    );
    assert_eq!(read.evidence[0].content.transaction.time.0, tx);
    assert_eq!(read.evidence[0].state, read.evidence[0].content);
}

/// Alice's note is visible only to her; Mallory receives neither its body
/// nor its evidence. A caller-supplied projection cannot bypass the policy.
#[test]
fn evidence_obeys_the_same_read_policy_as_its_body() {
    let alice = AuthorSubject::for_test_bytes([1; 16]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
                    .policies(read_and_allow_all_writes(PolicyExpr::eq_session(
                        "owner",
                        vec!["user".into(), "account".into()],
                    ))),
            )
            .build(),
    );
    let authority = open(schema, 9, true);
    let mut cells = body("private");
    cells.insert("owner".into(), Value::Uuid(alice.account_id().unwrap().0));
    block_on(authority.insert("notes", cells, Default::default())).unwrap();
    let query = authority
        .prepare_query(&Query::from("notes").select(["id"]))
        .unwrap();
    let allowed = authority
        .accepted_read(&query, None, false, Some(alice))
        .unwrap();
    assert_eq!(allowed.rows.len(), 1);
    assert_eq!(allowed.evidence.len(), 1);
    let mallory = AuthorSubject::for_test_bytes([2; 16]);
    let denied = authority
        .accepted_read(&query, None, false, Some(mallory))
        .unwrap();
    assert!(denied.rows.is_empty());
    assert!(denied.evidence.is_empty());
    // A root predicate that matches nothing also returns no evidence.
    let empty = authority
        .prepare_query(
            &Query::from("notes").filter(eq(col("body"), lit(Value::String("absent".into())))),
        )
        .unwrap();
    assert!(
        authority
            .accepted_read(&empty, None, false, Some(alice))
            .unwrap()
            .evidence
            .is_empty()
    );
}

/// Alice reopens an authority whose retained content witness was lost.
/// Its tombstone still holds body cells, but these cannot justify inventing
/// an accepted content origin. Direct SQL is needed to create this corrupt
/// fixture: the public API never removes a tombstone's retained history.
#[test]
fn a_missing_retained_content_witness_is_refused_after_reopen() {
    use jazz::db::FileOptions;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let open_file = || {
        let db = block_on(Db::open_at(
            DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        db.set_authority();
        db
    };
    {
        let alice = open_file();
        insert(&alice, 1, "lost content witness");
        block_on(alice.delete("notes", id(1), Default::default())).unwrap();
    }
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("DELETE FROM notes__h", [])
        .unwrap();
    let alice = open_file();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        alice
            .accepted_read(&query, None, true, None)
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
}

/// Alice's old and renamed schema views read the same accepted content
/// through their respective names, without changing either origin.
#[test]
fn renamed_schema_views_keep_the_exact_accepted_origin() {
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    let alice = open(schema(), 1, true);
    insert(&alice, 1, "before rename");
    let old = block_on(alice.register_schema_view(schema())).unwrap();
    let old_query = old.prepare_query(&Query::from("notes")).unwrap();
    let before = old.accepted_read(&old_query, None, false, None).unwrap();
    let renamed = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let lens = MigrationLens::new(
        schema().version_id(),
        renamed.version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "tasks".into(),
            ops: vec![LensOp::RenameColumn {
                from: "body".into(),
                to: "title".into(),
            }],
        }],
    )
    .unwrap();
    block_on(alice.activate_schema_with_lenses(renamed, &[lens])).unwrap();
    let query = alice.prepare_query(&Query::from("tasks")).unwrap();
    let after = alice.accepted_read(&query, None, false, None).unwrap();
    assert_eq!(
        after.rows[0].cell("title"),
        Some(Value::String("before rename".into()))
    );
    assert_eq!(after.evidence, before.evidence);
    assert_eq!(
        old.accepted_read(&old_query, None, false, None)
            .unwrap()
            .evidence,
        before.evidence
    );
}

/// Alice's shard holds an unfinished part while Bob's later write lands.
/// Evidence stays below the hold until it is released, even for empty reads.
/// Alice --hold--> shard <--write-- Bob; reader sees only the settled prefix.
#[test]
fn accepted_evidence_stays_below_an_unresolved_shard_hold() {
    use jazz::db::shard::CommitRef;
    let alice = open(schema(), 1, true);
    insert(&alice, 1, "before");
    let part = CommitRef {
        node: [2; 16],
        tx: 1,
    };
    let held = alice.shard().hold(part, &[]).unwrap();
    let after = insert(&alice, 2, "after");
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let read = alice.accepted_read(&query, None, false, None).unwrap();
    assert_eq!(read.through, held - 1);
    assert_eq!(read.rows.len(), 1);
    assert_eq!(read.rows[0].row_uuid(), id(1));
    for requested in [held, after] {
        assert_eq!(
            alice
                .accepted_read(&query, Some(requested), false, None)
                .unwrap_err()
                .code,
            ErrorCode::NotObserved
        );
    }
    let empty = alice.prepare_query(&Query::from("notes").limit(0)).unwrap();
    assert_eq!(
        alice
            .accepted_read(&empty, Some(held), false, None)
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
    alice.shard().release(part).unwrap();
    let complete = alice.accepted_read(&query, None, false, None).unwrap();
    assert_eq!(complete.through, alice.shard().served_through());
    assert_eq!(complete.rows.len(), 2);
    assert_eq!(complete.evidence.len(), 2);
}

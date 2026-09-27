//! Public Db receipts for recovery across browser transaction-node ownership.
//!
//! JazzClient's factory does not expose reopening one durable root with a new
//! transaction-node id. Use the same public Db recovery boundary as WASM and
//! real RocksDB storage; assertions inspect query results, not runtime state.

use super::*;
use crate::tools::test_support::AllowAll;

struct RecoveredBrowser {
    db: Db<RocksDbStorage>,
    _directory: tempfile::TempDir,
    schema: JazzSchema,
    alice: AuthorSubject,
    bob: AuthorSubject,
    alice_row: RowUuid,
    bob_row: RowUuid,
}

// The public row builder is converted only at Db's physical RowCells boundary.
fn note_cells(title: &str) -> RowCells {
    crate::row_input!("title" => title)
        .into_iter()
        .map(|(name, value)| {
            let PublicValue::Text(text) = value else {
                unreachable!("text-only fixture")
            };
            (name, Value::String(text))
        })
        .collect()
}

fn old_browser_writes() -> RecoveredBrowser {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text))
            .allow_all(),
    );
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let directory = tempfile::tempdir().unwrap();
    let alice = AuthorSubject::for_test_bytes([0xe1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xe2; 16]);
    let open = |node, author| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            RocksDbStorage::open(directory.path(), &refs).unwrap(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let write = |node, author, title| {
        let db = open(node, author);
        let cells = note_cells(title);
        let receipt = db.insert("notes", cells, Default::default()).unwrap();
        let row = receipt.row_uuid();
        block_on(receipt.wait(DurabilityTier::Local)).unwrap();
        drop(receipt);
        block_on(db.close()).unwrap();
        drop(db);
        row
    };
    let alice_row = write(0xe3, alice, "alice pending");
    let bob_row = write(0xe4, bob, "bob pending");
    let db = open(0xe5, alice);
    RecoveredBrowser {
        db,
        _directory: directory,
        schema,
        alice,
        bob,
        alice_row,
        bob_row,
    }
}

fn transaction_rows(db: &Db<RocksDbStorage>, id: OpenTransactionId) -> Vec<RowUuid> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(db.all_in_open_transaction(id, &query, ReadOpts::default(), None)).unwrap();
    row_ids(&rows)
}

/// Alice migrates an offline durable root from older tab nodes to one worker.
/// Explicit recovery admits Alice's pending writes to both transaction kinds;
/// Bob's pending writes and later arrivals never enter Alice's frozen snapshot.
///
/// alice old tab --persist--> root --recover--> worker --open tx--> later write
#[test]
fn browser_recovery_preserves_pending_rows_and_frozen_snapshot() {
    let fixture = old_browser_writes();
    let db = &fixture.db;
    block_on(db.restore_browser_relay_pending_uploads()).unwrap();
    let mergeable = OpenTransactionId::new();
    let exclusive = OpenTransactionId::new();
    block_on(db.begin_mergeable(mergeable)).unwrap();
    block_on(db.begin_exclusive(exclusive)).unwrap();
    let owned_exclusive = block_on(db.exclusive_tx()).unwrap();
    for id in [mergeable, exclusive] {
        assert_eq!(transaction_rows(db, id), vec![fixture.alice_row]);
    }
    assert!(
        block_on(owned_exclusive.read("notes", fixture.alice_row))
            .unwrap()
            .is_some()
    );
    assert!(
        block_on(owned_exclusive.read("notes", fixture.bob_row))
            .unwrap()
            .is_none()
    );
    let later = db
        .insert("notes", note_cells("later"), Default::default())
        .unwrap();
    block_on(later.wait(DurabilityTier::Local)).unwrap();
    for id in [mergeable, exclusive] {
        assert_eq!(transaction_rows(db, id), vec![fixture.alice_row]);
        db.abandon_transaction_handle(id).unwrap();
    }
    assert!(
        block_on(owned_exclusive.read("notes", later.row_uuid()))
            .unwrap()
            .is_none()
    );
    drop(owned_exclusive);
    let next = block_on(db.exclusive_tx()).unwrap();
    assert!(
        block_on(next.read("notes", fixture.alice_row))
            .unwrap()
            .is_some()
    );
    assert!(
        block_on(next.read("notes", later.row_uuid()))
            .unwrap()
            .is_some()
    );
    assert!(
        block_on(next.read("notes", fixture.bob_row))
            .unwrap()
            .is_none()
    );
}

/// Reopening storage alone does not adopt unfated foreign payloads. Alice's
/// explicit recovery also cannot lend her pending snapshot dots to Bob.
#[test]
fn browser_recovery_snapshot_requires_explicit_owner_admission() {
    let fixture = old_browser_writes();
    let db = &fixture.db;
    let before = OpenTransactionId::new();
    block_on(db.begin_mergeable(before)).unwrap();
    assert!(transaction_rows(db, before).is_empty());
    block_on(db.restore_browser_relay_pending_uploads()).unwrap();
    assert!(transaction_rows(db, before).is_empty());
    let bob = OpenTransactionId::new();
    block_on(db.begin_mergeable_for_identity(bob, fixture.bob)).unwrap();
    assert!(transaction_rows(db, bob).is_empty());
    let alice = OpenTransactionId::new();
    block_on(db.begin_mergeable_for_identity(alice, fixture.alice)).unwrap();
    assert_eq!(transaction_rows(db, alice), vec![fixture.alice_row]);
    for id in [before, bob, alice] {
        db.abandon_transaction_handle(id).unwrap();
    }
}

/// Alice's new worker reads an older pending write, then commits an exclusive
/// dependent transaction. The real authority accepts both through ordinary
/// session admission; recovering a local dot does not mint a Global fate.
///
/// old alice --pending--> worker --read + commit--> core --fates--> worker
#[test]
fn browser_recovery_snapshot_survives_authority_validation() {
    let fixture = old_browser_writes();
    let db = &fixture.db;
    block_on(db.restore_browser_relay_pending_uploads()).unwrap();
    let tx = block_on(db.exclusive_tx()).unwrap();
    assert!(
        block_on(tx.read("notes", fixture.alice_row))
            .unwrap()
            .is_some()
    );
    block_on(tx.insert("notes", note_cells("dependent"), Default::default())).unwrap();
    let committed = block_on(tx.commit()).unwrap();
    assert_eq!(db.write_state(committed).unwrap().fate, Fate::Pending);

    let core = open_core(0xe6, AuthorSubject::SYSTEM, &fixture.schema);
    let (upstream, downstream) = duplex();
    let _connection = block_on(db.connect_upstream(upstream));
    let peer = core.accept_subscriber(downstream, fixture.alice);
    for _ in 0..32 {
        block_on(db.tick()).unwrap();
        peer.borrow_mut().tick().unwrap();
        block_on(db.tick()).unwrap();
        if db.write_state(committed).unwrap().fate != Fate::Pending {
            break;
        }
    }
    let state = db.write_state(committed).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert!(state.durability >= DurabilityTier::Global);
}

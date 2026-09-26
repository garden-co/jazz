//! Owner-only reconstruction counters require this internal topology: a public
//! JazzClient does not expose the boundary between its owner's publication and
//! its own byte materialization. Schema/query builders and returned rows use Db
//! APIs, and independent MemoryStorage snapshots contain synthetic data only.
use super::*;
use groove::large_values::{LEAF_MAX_BYTES, full_materializations_for_test};
use groove::storage::MemoryStorage;

fn cached_pair(
    with_reference: bool,
) -> (
    Db<MemoryStorage>,
    Db<MemoryStorage>,
    JazzSchema,
    RowUuid,
    Vec<u8>,
) {
    let mut assets = PublicTableSchemaBuilder::new("assets")
        .column("label", PublicColumnType::Text)
        .column("contents", PublicColumnType::Bytea);
    if with_reference {
        assets = assets.fk_column("folder", "folders");
    }
    let schema =
        build_public_db_test_schema(PublicSchemaBuilder::new().table(assets).table(
            PublicTableSchemaBuilder::new("folders").column("name", PublicColumnType::Text),
        ));
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs).unwrap();
    let config = |storage, tag| {
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([tag; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    let seed = block_on(Db::open_history_complete(config(storage.clone(), 0xb1))).unwrap();
    let folder = row(0xb2);
    let asset = row(0xb3);
    if with_reference {
        let write = block_on(seed.insert(
            "folders",
            BTreeMap::from([("name".to_owned(), Value::String("group".to_owned()))]),
            InsertOptions {
                row_id: Some(folder),
                ..Default::default()
            },
        ))
        .unwrap();
        seed.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .unwrap();
    }
    let bytes = (0..LEAF_MAX_BYTES * 2 + 17)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let mut cells = BTreeMap::from([
        ("label".to_owned(), Value::String("asset".to_owned())),
        ("contents".to_owned(), Value::Bytes(bytes.clone())),
    ]);
    if with_reference {
        cells.insert("folder".to_owned(), Value::Uuid(folder.0));
    }
    let write = block_on(seed.insert(
        "assets",
        cells,
        InsertOptions {
            row_id: Some(asset),
            ..Default::default()
        },
    ))
    .unwrap();
    seed.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .unwrap();
    block_on(seed.close()).unwrap();
    drop(seed);
    let snapshot = storage.export_snapshot().unwrap();
    let copy = MemoryStorage::default();
    copy.import_snapshot(&snapshot).unwrap();
    // SAFETY: this test exclusively owns the synthetic owner store and admits
    // exactly the one foreground with the same SYSTEM identity.
    let scope = unsafe {
        ClientRelayScope::from_admitted_storage_owner(
            "peer-blob-test".to_owned(),
            AuthorSubject::SYSTEM,
        )
    };
    let owner =
        block_on(unsafe { Db::open_scope_isolated_client_relay(config(storage, 0xb4), scope) })
            .unwrap();
    let foreground = block_on(Db::open(config(copy, 0xb5))).unwrap();
    foreground.set_non_durable_client();
    let (up, down) = duplex();
    block_on(foreground.connect_upstream(up));
    owner.accept_subscriber(down, AuthorSubject::SYSTEM);
    (owner, foreground, schema, asset, bytes)
}

fn assert_cached_blob_reads(with_reference: bool) {
    let (owner, foreground, schema, id, bytes) = cached_pair(with_reference);
    let query = foreground
        .prepare_query(
            &Query::from("assets")
                .filter(eq(col("id"), lit(id.0)))
                .limit(1),
        )
        .unwrap();
    let table = schema.tables().iter().find(|t| t.name == "assets").unwrap();
    for _ in 0..3 {
        let attachment = foreground.attach_query(&query).unwrap();
        assert!(
            !foreground.query_attachment_is_covered(&attachment),
            "cached bytes do not replace a fresh owner receipt"
        );
        let mut owner_rebuilds = 0;
        for turn in 0..32 {
            block_on(foreground.tick()).unwrap();
            let before = full_materializations_for_test();
            block_on(owner.tick()).unwrap();
            owner_rebuilds += full_materializations_for_test() - before;
            block_on(foreground.tick()).unwrap();
            if foreground.query_attachment_is_covered(&attachment) {
                break;
            }
            assert!(turn < 31, "fresh coverage completes");
        }
        assert!(
            owner_rebuilds <= 2,
            "owner must not build an unused application blob collector: {owner_rebuilds}"
        );
        let rows = block_on(foreground.all(&query, ReadOpts::default())).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), id);
        assert_eq!(
            rows[0].cell(table, "contents"),
            Some(Value::Bytes(bytes.clone()))
        );
        foreground.detach_query(attachment);
        for _ in 0..3 {
            block_on(foreground.tick()).unwrap();
            block_on(owner.tick()).unwrap();
        }
    }
    // The application still owns its full terminal and must receive a reset
    // containing the complete blob after the one-shot usages have retired.
    let mut stream = block_on(foreground.subscribe(&query, ReadOpts::default())).unwrap();
    let mut initial = None;
    for _ in 0..32 {
        block_on(foreground.tick()).unwrap();
        block_on(owner.tick()).unwrap();
        if let Some(event) = stream.try_next_event() {
            initial = Some(event);
            break;
        }
    }
    let Some(SubscriptionEvent::Delta {
        reset: true, added, ..
    }) = initial
    else {
        panic!("application reset missing: {initial:?}");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(
        added[0].cell(table, "contents"),
        Some(Value::Bytes(bytes.clone()))
    );
    // The owner's retained publication must still carry replacement versions
    // and deletion witnesses after its unused application collector is omitted.
    let replacement = bytes.iter().map(|byte| byte ^ 0x5a).collect::<Vec<_>>();
    let write = block_on(owner.update(
        "assets",
        id,
        BTreeMap::from([("contents".to_owned(), Value::Bytes(replacement.clone()))]),
        UpdateOptions::default(),
    ))
    .unwrap();
    owner
        .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .unwrap();
    let mut saw_replacement = false;
    for _ in 0..32 {
        block_on(owner.tick()).unwrap();
        block_on(foreground.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta { added, updated, .. } = event {
                saw_replacement |= added.iter().chain(updated.iter()).any(|row| {
                    row.row_uuid() == id
                        && row.cell(table, "contents") == Some(Value::Bytes(replacement.clone()))
                });
            }
        }
        if saw_replacement {
            break;
        }
    }
    assert!(
        saw_replacement,
        "the live application terminal receives exact replacement bytes"
    );
    let write = block_on(owner.delete("assets", id, DeleteOptions::default())).unwrap();
    owner
        .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .unwrap();
    let mut saw_delete = false;
    for _ in 0..32 {
        block_on(owner.tick()).unwrap();
        block_on(foreground.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta { removed, .. } = event {
                saw_delete |= removed.iter().any(|row| row.row_uuid == id);
            }
        }
        if saw_delete {
            break;
        }
    }
    assert!(
        saw_delete,
        "the live application terminal retracts the deleted row"
    );
    assert!(
        block_on(foreground.all(&query, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
    block_on(stream.close()).unwrap();
    block_on(foreground.close()).unwrap();
    block_on(owner.close()).unwrap();
}

/// A cached foreground point read gets fresh owner coverage on every open and exact
/// bytes; the owner's fact publication omits an unused application collector.
/// foreground -> owner receipt -> foreground byte materialization.
#[test]
fn cached_plain_peer_blob_reads_avoid_unused_app_collector() {
    assert_cached_blob_reads(false);
}

/// The cached blob row keeps its implicit reference closure while peer publication
/// omits only the application collector; the foreground reset retains full bytes.
/// foreground -> owner + referenced row -> complete foreground blob.
#[test]
fn cached_reference_peer_blob_reads_keep_source_closure() {
    assert_cached_blob_reads(true);
}

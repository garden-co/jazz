//! Two real resident databases isolate owner-only reconstruction counts, which
//! the public JazzClient API cannot expose. Inputs, queries, reads and streams
//! use the public builders/Db facade; no compiler or terminal state is forged.
use super::*;
use groove::large_values::{LEAF_MAX_BYTES, full_materializations_for_test};
use groove::storage::MemoryStorage;

fn cells(title: &str, rank: i64, body: &str) -> BTreeMap<String, Value> {
    crate::row_input!("title" => title, "rank" => rank, "body" => body)
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                PublicValue::Text(text) => Value::String(text),
                PublicValue::BigInt(number) => Value::I64(number),
                other => panic!("unexpected fixture value {other:?}"),
            };
            (name, value)
        })
        .collect()
}

fn payload(json: bool, revision: &str) -> String {
    if json {
        format!(
            r#"{{"revision":"{revision}","n":-0,"text":"{}"}}"#,
            "x".repeat(LEAF_MAX_BYTES + 31)
        )
    } else {
        format!("{revision}:{}", "é🙂 ".repeat(50_000))
    }
}

fn pair(json: bool) -> (Db<MemoryStorage>, Db<MemoryStorage>, JazzSchema, String) {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("entries")
                .column("title", PublicColumnType::Text)
                .column("rank", PublicColumnType::BigInt)
                .column(
                    "body",
                    if json {
                        PublicColumnType::Json { schema: None }
                    } else {
                        PublicColumnType::Text
                    },
                ),
        ),
    );
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
    let seed = block_on(Db::open_history_complete(config(storage.clone(), 0xc1))).unwrap();
    let body = payload(json, "before");
    for (id, title, rank, body) in [
        (1, "one", 1, body.as_str()),
        (2, "two", 2, "null"),
        (3, "three", 3, "null"),
    ] {
        let write = block_on(seed.insert(
            "entries",
            cells(title, rank, body),
            InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        ))
        .unwrap();
        seed.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .unwrap();
    }
    block_on(seed.close()).unwrap();
    drop(seed);
    let copy = MemoryStorage::default();
    copy.import_snapshot(&storage.export_snapshot().unwrap())
        .unwrap();
    // SAFETY: this fixture owns the store and admits exactly one foreground
    // with the same SYSTEM identity through its local relay scope.
    let scope = unsafe {
        ClientRelayScope::from_admitted_storage_owner(
            "plain-peer-test".to_owned(),
            AuthorSubject::SYSTEM,
        )
    };
    let owner =
        block_on(unsafe { Db::open_scope_isolated_client_relay(config(storage, 0xc2), scope) })
            .unwrap();
    let foreground = block_on(Db::open(config(copy, 0xc3))).unwrap();
    foreground.set_non_durable_client();
    let (up, down) = duplex();
    block_on(foreground.connect_upstream(up));
    owner.accept_subscriber(down, AuthorSubject::SYSTEM);
    (owner, foreground, schema, body)
}

fn covered_read(
    owner: &Db<MemoryStorage>,
    foreground: &Db<MemoryStorage>,
    query: &PreparedQuery,
) -> Vec<CurrentRow> {
    let attachment = foreground.attach_query(query).unwrap();
    assert!(
        !foreground.query_attachment_is_covered(&attachment),
        "resident payloads still need fresh owner coverage"
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
        assert!(turn < 31, "fresh coverage must complete");
    }
    assert!(
        owner_rebuilds <= 2,
        "unused owner text/JSON collection returned: {owner_rebuilds}"
    );
    let result = block_on(foreground.all(query, ReadOpts::default())).unwrap();
    foreground.detach_query(attachment);
    for _ in 0..3 {
        block_on(foreground.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    result
}

fn plain_query_lifecycle(json: bool) {
    let (owner, foreground, schema, body) = pair(json);
    let table = schema
        .tables()
        .iter()
        .find(|table| table.name == "entries")
        .unwrap();
    let point = foreground
        .prepare_query(
            &Query::from("entries")
                .filter(eq(col("id"), lit(row(1).0)))
                .select(["title", "body"])
                .limit(1),
        )
        .unwrap();
    for _ in 0..3 {
        let rows = covered_read(&owner, &foreground, &point);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), row(1));
        assert_eq!(
            rows[0].cell(table, "title"),
            Some(Value::String("one".to_owned()))
        );
        assert_eq!(
            rows[0].cell(table, "body"),
            Some(Value::String(body.clone()))
        );
        assert_eq!(rows[0].cell(table, "rank"), None);
    }
    let page = foreground
        .prepare_query(
            &Query::from("entries")
                .order_by("rank", OrderDirection::Desc)
                .offset(1)
                .limit(2)
                .select(["title"]),
        )
        .unwrap();
    let rows = covered_read(&owner, &foreground, &page);
    assert_eq!(
        rows.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>(),
        [row(2), row(1)]
    );
    assert_eq!(
        rows.iter()
            .map(|r| r.cell(table, "title"))
            .collect::<Vec<_>>(),
        [
            Some(Value::String("two".into())),
            Some(Value::String("one".into()))
        ]
    );
    assert!(
        rows.iter()
            .all(|r| r.cell(table, "rank").is_none() && r.cell(table, "body").is_none())
    );

    let mut stream = block_on(foreground.subscribe(&point, ReadOpts::default())).unwrap();
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
        panic!("missing application reset: {initial:?}")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].cell(table, "body"), Some(Value::String(body)));

    let replacement = payload(json, "after");
    let write = block_on(owner.update(
        "entries",
        row(1),
        cells("changed", 4, &replacement),
        UpdateOptions::default(),
    ))
    .unwrap();
    owner
        .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .unwrap();
    let mut updated = false;
    for _ in 0..32 {
        block_on(owner.tick()).unwrap();
        block_on(foreground.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta {
                added,
                updated: changed,
                ..
            } = event
            {
                updated |= added.iter().chain(&changed).any(|r| {
                    r.row_uuid() == row(1)
                        && r.cell(table, "body") == Some(Value::String(replacement.clone()))
                        && r.cell(table, "title") == Some(Value::String("changed".into()))
                });
            }
        }
        if updated {
            break;
        }
    }
    assert!(
        updated,
        "application stream must retain exact replacement payloads"
    );
    let write = block_on(owner.delete("entries", row(1), DeleteOptions::default())).unwrap();
    owner
        .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .unwrap();
    let mut removed = false;
    for _ in 0..32 {
        block_on(owner.tick()).unwrap();
        block_on(foreground.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta { removed: rows, .. } = event {
                removed |= rows.iter().any(|r| r.row_uuid == row(1));
            }
        }
        if removed {
            break;
        }
    }
    assert!(removed, "deletion still retracts the live result");
    assert!(
        block_on(foreground.all(&point, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
    block_on(stream.close()).unwrap();
    block_on(foreground.close()).unwrap();
    block_on(owner.close()).unwrap();
}

/// Alice's cached text reads still obtain Bob's owner coverage, preserve an
/// unselected sort key and receive exact edits/deletes without a peer collector.
/// alice -> bob coverage -> alice rows; bob update/delete -> alice live delta.
#[test]
fn plain_peer_text_reads_preserve_projection_order_and_live_changes() {
    plain_query_lifecycle(false);
}

/// Alice's literal JSON follows the same peer lifecycle as text; syntax and
/// source bytes survive the receiver's own result construction.
/// alice -> bob coverage -> literal JSON; bob replacement/delete -> alice delta.
#[test]
fn plain_peer_json_reads_preserve_literal_bytes_and_live_changes() {
    plain_query_lifecycle(true);
}

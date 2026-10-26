//! Retained output aliases survive local, synced and serializable reads.
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier,
    RemoteLinkHint, SerializedReadResult, SubscriptionEvent, SyncMessage, Transport,
    TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{
    OrderDirection, Query, RelationColumnRef, RelationExpr, RelationJoinCondition,
    RelationJoinKind, RelationOrderBy, RelationProjectColumn, RelationProjectExpr, RelationQuery,
    RelationRowIdRef, relation_query_to_query,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::{DurabilityTier, Fate};
use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://alias.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn config(node: u8) -> DbConfig {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session(
            "owner",
            vec!["user".into(), "identity".into(), "subject".into()],
        ))
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::BigInt)
                    .column("owner", ColumnType::Text)
                    .policies(policies.clone()),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("doc", "docs")
                    .column("owner", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
    .unwrap();
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )
}
fn open(node: u8) -> Db {
    let config = config(node);
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}
fn doc(db: &Db, n: u8, title: &str, owner: &str) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("title".into(), Value::String(title.into())),
            ("rank".into(), Value::I64(i64::from(n))),
            ("owner".into(), Value::String(owner.into())),
        ]),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn seed(db: &Db) {
    doc(db, 1, "z", "alice");
    doc(db, 2, "a", "alice");
    doc(db, 3, "secret", "bob");
    for (n, root, owner) in [
        (11, 1, "alice"),
        (12, 1, "alice"),
        (13, 2, "bob"),
        (14, 3, "alice"),
    ] {
        block_on(db.insert(
            "access",
            BTreeMap::from([
                ("doc".into(), Value::Uuid(id(root).0)),
                ("owner".into(), Value::String(owner.into())),
            ]),
            InsertOptions {
                row_id: Some(id(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
}
fn column(scope: &str, name: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(scope.into()),
        column: name.into(),
    }
}
fn relation(join: bool, aliases: &[(&str, &str)], window: bool) -> RelationQuery {
    let mut input = RelationExpr::TableScan {
        table: "docs".into(),
        alias: None,
    };
    if join {
        input = RelationExpr::Join {
            left: Box::new(input),
            right: Box::new(RelationExpr::TableScan {
                table: "access".into(),
                alias: Some("grant".into()),
            }),
            on: vec![RelationJoinCondition {
                left: column("docs", "id"),
                right: column("grant", "doc"),
            }],
            join_kind: RelationJoinKind::Inner,
        };
    }
    let mut rel = RelationExpr::Project {
        input: Box::new(input),
        columns: aliases
            .iter()
            .map(|(source, alias)| RelationProjectColumn {
                alias: (*alias).into(),
                expr: if *source == "id" {
                    RelationProjectExpr::RowId(RelationRowIdRef::Current)
                } else {
                    RelationProjectExpr::Column(column("docs", source))
                },
            })
            .collect(),
    };
    if window {
        rel = RelationExpr::Limit {
            input: Box::new(RelationExpr::OrderBy {
                input: Box::new(rel),
                terms: vec![RelationOrderBy {
                    column: column("docs", "rank"),
                    direction: OrderDirection::Asc,
                }],
            }),
            limit: 1,
        };
    }
    RelationQuery { rel }
}
fn query(join: bool, aliases: &[(&str, &str)], window: bool) -> Query {
    relation_query_to_query(&relation(join, aliases, window)).unwrap()
}
fn cells(row: &CurrentRow) -> Vec<(String, Option<Value>)> {
    row.cells()
        .map(|(name, value)| (name.to_owned(), value.cloned()))
        .collect()
}
fn connect(client: &Db, server: &Db) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice());
}
fn turns(nodes: &[&Db]) {
    for _ in 0..20 {
        for db in nodes {
            block_on(db.tick()).unwrap();
        }
    }
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        turns(nodes);
    }
    panic!("read did not finish");
}
fn fetch(db: &Db, nodes: &[&Db], query: &Query) -> Vec<CurrentRow> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let SerializedReadResult::Rows(rows) = drive(
        nodes,
        db.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap() else {
        panic!("flat rows")
    };
    rows
}
fn begin(db: &Db) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    tx
}

/// Alice's bare and normalized queries preserve UUID identity, repeated source
/// columns, quoted aliases and exact big integers without leaking unselected cells.
#[test]
fn aliases_keep_types_identity_and_shape() {
    let db = open(0);
    seed(&db);
    let large = "chunked".repeat(15_000);
    block_on(db.update(
        "docs",
        id(1),
        BTreeMap::from([
            ("title".into(), Value::String(large.clone())),
            ("rank".into(), Value::I64((1_i64 << 54) + 1)),
        ]),
        Default::default(),
    ))
    .unwrap();
    let rel = relation(
        false,
        &[
            ("id", "key"),
            ("title", "label"),
            ("title", "a\"b"),
            ("rank", "number"),
        ],
        false,
    );
    let mut bare = Query::from("docs");
    bare.relation = Some(rel.clone());
    let bare = db.prepare_query(&bare).unwrap();
    let normal = db
        .prepare_query(&relation_query_to_query(&rel).unwrap())
        .unwrap();
    assert_eq!(bare.shape(), normal.shape());
    assert_eq!(bare.query(), normal.query());
    assert!(normal.query().relation.is_some());
    let rows = db.read(&normal).unwrap();
    let row = rows.iter().find(|row| row.row_uuid() == id(1)).unwrap();
    assert_eq!(
        cells(row),
        vec![
            ("key".into(), Some(Value::Uuid(id(1).0))),
            ("label".into(), Some(Value::String(large.clone()))),
            ("a\"b".into(), Some(Value::String(large))),
            ("number".into(), Some(Value::I64((1_i64 << 54) + 1))),
        ]
    );
    let renamed = db
        .prepare_query(&query(false, &[("title", "other")], false))
        .unwrap();
    assert_ne!(normal.shape(), renamed.shape());
}

/// Alice names title "rank", but her maintained window must still sort by
/// physical rank. Simultaneous subscriptions cannot exchange output names.
#[test]
fn maintained_aliases_do_not_replace_sort_keys_or_share_output_names() {
    let db = open(0);
    seed(&db);
    let first = db
        .prepare_query(&query(false, &[("title", "rank")], true))
        .unwrap();
    let second = db
        .prepare_query(&query(false, &[("title", "label")], true))
        .unwrap();
    let mut streams =
        [&first, &second].map(|query| block_on(db.subscribe(query, ReadOpts::default())).unwrap());
    for stream in &mut streams {
        while stream.try_next_event().is_some() {}
    }
    assert_eq!(db.read(&first).unwrap()[0].row_uuid(), id(1));
    block_on(db.update(
        "docs",
        id(2),
        BTreeMap::from([("rank".into(), Value::I64(0))]),
        Default::default(),
    ))
    .unwrap();
    for (stream, alias) in streams.iter_mut().zip(["rank", "label"]) {
        let mut added = vec![];
        let mut removed = vec![];
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta {
                added: a,
                removed: r,
                ..
            } = event
            {
                added.extend(
                    a.into_iter()
                        .map(|item| (item.row.row_uuid(), cells(&item.row))),
                );
                removed.extend(r.into_iter().map(|item| item.row_uuid));
            }
        }
        assert_eq!(
            added,
            vec![(id(2), vec![(alias.into(), Some(Value::String("a".into())))])]
        );
        assert_eq!(removed, vec![id(1)]);
    }
}

/// Alice cold-reads renamed joined output through direct, ordinary-relay and
/// private-owner paths. Bob's roots/grants remain hidden and final revocation wins.
#[test]
fn remote_aliases_keep_join_permissions_and_revocation() {
    for mode in 0..3 {
        let server = open(0);
        seed(&server);
        let client = open(1);
        let relay = open(2);
        let nodes = if mode == 0 {
            connect(&client, &server);
            vec![&client, &server]
        } else {
            if mode == 2 {
                relay.set_scope_isolated_storage_owner().unwrap();
            }
            connect(&relay, &server);
            connect(&client, &relay);
            vec![&client, &relay, &server]
        };
        let q = query(true, &[("id", "key"), ("title", "label")], false);
        let rows = fetch(&client, &nodes, &q);
        assert_eq!(rows.len(), 1, "mode {mode}");
        assert_eq!(
            cells(&rows[0]),
            vec![
                ("key".into(), Some(Value::Uuid(id(1).0))),
                ("label".into(), Some(Value::String("z".into()))),
            ]
        );
        let prepared = client.prepare_query(&q).unwrap();
        let mut stream =
            block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap();
        turns(&nodes);
        while stream.try_next_event().is_some() {}
        for grant in [11, 12] {
            block_on(server.update(
                "access",
                id(grant),
                BTreeMap::from([("owner".into(), Value::String("bob".into()))]),
                Default::default(),
            ))
            .unwrap();
            turns(&nodes);
            let rows =
                block_on(client.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
            assert_eq!(rows.len(), usize::from(grant == 11), "mode {mode}");
        }
        let mut removed = vec![];
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta { removed: items, .. } = event {
                removed.extend(items.into_iter().map(|item| item.row_uuid));
            }
        }
        assert_eq!(removed, vec![id(1)]);
    }
}

/// Alice reads renamed cells at a pinned remote snapshot after Bob changes the
/// live title. The cached newer version then prevents committing the stale read.
#[test]
fn cold_alias_snapshot_is_pinned_and_conflicts_at_commit() {
    let server = open(0);
    seed(&server);
    let client = open(1);
    connect(&client, &server);
    let nodes = [&client, &server];
    let empty = client.prepare_query(&Query::from("docs").limit(0)).unwrap();
    let _stream =
        block_on(client.subscribe(&empty, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&nodes);
    let tx = begin(&client);
    drive(&nodes, client.serializable_tx_ref(tx).all_prepared(&empty)).unwrap();
    block_on(server.update(
        "docs",
        id(1),
        BTreeMap::from([("title".into(), Value::String("changed".into()))]),
        Default::default(),
    ))
    .unwrap();
    let q = client
        .prepare_query(&query(true, &[("title", "label")], false))
        .unwrap();
    let rows = drive(&nodes, client.serializable_tx_ref(tx).all_prepared(&q)).unwrap();
    assert_eq!(
        cells(&rows[0]),
        vec![("label".into(), Some(Value::String("z".into())))]
    );
    assert_eq!(
        block_on(client.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}

/// Alice's paired accepted read retains renamed output and real content
/// attribution. Bob accepts an unchanged read and rejects a later title change.
#[test]
fn paired_aliases_keep_attribution_and_read_only_validation() {
    for changed in [false, true] {
        let server = open(0);
        seed(&server);
        let client = open(1);
        connect(&client, &server);
        let nodes = [&client, &server];
        let tx = begin(&client);
        let q = client
            .prepare_query(&query(false, &[("title", "label")], true))
            .unwrap();
        let read = drive(
            &nodes,
            client
                .all_settled(
                    q,
                    ReadOpts::for_read_tier(ReadTier::Remote),
                    tx,
                    false,
                    None,
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            cells(&read.rows[0]),
            vec![("label".into(), Some(Value::String("z".into())))]
        );
        assert_eq!(read.settlements.len(), 1);
        assert_eq!(read.settlements[0].0.node, NodeUuid::from_bytes([0; 16]));
        assert!(read.settlements[0].0.time.0 > 0);
        if changed {
            block_on(server.update(
                "docs",
                id(1),
                BTreeMap::from([("title".into(), Value::String("changed".into()))]),
                Default::default(),
            ))
            .unwrap();
        }
        let receipt = block_on(client.commit_serializable_handle(tx)).unwrap();
        let result = drive(
            &nodes,
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        );
        if changed {
            let error = result.unwrap_err();
            assert!(error.is_serialization_conflict(), "{error}");
            assert!(error.is_conflict(), "authority conflicts remain retryable");
        } else {
            result.unwrap();
        }
    }
}

/// Alice queues a write after an offline aliased join read. The authority
/// accepts unchanged content and rejects title or witness changes atomically.
#[test]
fn offline_alias_evidence_validates_sources_and_witnesses() {
    for changed in ["none", "title", "witness"] {
        let server = open(0);
        seed(&server);
        let client = open(1);
        connect(&client, &server);
        let nodes = [&client, &server];
        let q = query(true, &[("title", "label")], false);
        assert_eq!(fetch(&client, &nodes, &q).len(), 1);
        client.set_remote_link_hint(RemoteLinkHint::Failed);
        let tx = begin(&client);
        let bytes = postcard::to_allocvec(&q).unwrap();
        let rows = block_on(client.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            Some(tx),
            None,
            None,
            false,
            || false,
            |_| {},
        ))
        .unwrap();
        let SerializedReadResult::Rows(rows) = rows else {
            panic!("rows")
        };
        assert_eq!(
            cells(&rows[0]),
            vec![("label".into(), Some(Value::String("z".into())))]
        );
        block_on(client.serializable_tx_ref(tx).insert(
            "docs",
            BTreeMap::from([
                ("title".into(), Value::String("companion".into())),
                ("rank".into(), Value::I64(99)),
                ("owner".into(), Value::String("alice".into())),
            ]),
            InsertOptions {
                row_id: Some(id(99)),
                ..Default::default()
            },
        ))
        .unwrap();
        let receipt = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        if changed == "title" {
            block_on(server.update(
                "docs",
                id(1),
                BTreeMap::from([("title".into(), Value::String("changed".into()))]),
                Default::default(),
            ))
            .unwrap();
        }
        if changed == "witness" {
            for grant in [11, 12] {
                block_on(server.update(
                    "access",
                    id(grant),
                    BTreeMap::from([("owner".into(), Value::String("bob".into()))]),
                    Default::default(),
                ))
                .unwrap();
            }
        }
        client.set_remote_link_hint(RemoteLinkHint::Live);
        turns(&nodes);
        let fate = client.write_state(receipt).unwrap().fate;
        let written = server
            .read(&server.prepare_query(&Query::from("docs")).unwrap())
            .unwrap()
            .iter()
            .any(|row| row.row_uuid() == id(99));
        if changed == "none" {
            assert_eq!(fate, Fate::Accepted);
            assert!(written);
        } else {
            assert!(matches!(fate, Fate::Rejected(_)), "{changed}: {fate:?}");
            assert!(!written);
        }
    }
}

/// Alice can rename provenance, including deletion metadata, but a name change
/// cannot bypass includeDeleted. An output named id cannot replace row identity.
#[test]
fn renamed_metadata_keeps_read_options_and_row_identity() {
    let db = open(0);
    seed(&db);
    let q = db
        .prepare_query(&query(
            false,
            &[("title", "id"), ("$createdBy.identity.subject", "creator")],
            false,
        ))
        .unwrap();
    let row = db
        .read(&q)
        .unwrap()
        .into_iter()
        .find(|row| row.row_uuid() == id(1))
        .unwrap();
    assert_eq!(
        cells(&row),
        vec![
            ("id".into(), Some(Value::String("z".into()))),
            (
                "creator".into(),
                Some(Value::Nullable(Some(Box::new(Value::String(
                    "alice".into()
                ))))),
            ),
        ]
    );
    let deleted = db
        .prepare_query(&query(
            false,
            &[("title", "label"), ("$deletedAt", "deleted")],
            false,
        ))
        .unwrap();
    assert!(
        db.read(&deleted)
            .unwrap_err()
            .to_string()
            .contains("includeDeleted")
    );
    block_on(db.delete("docs", id(1), Default::default())).unwrap();
    let mut opts = ReadOpts::default();
    opts.read_view.include_deleted = true;
    let rows = block_on(db.all(&deleted, opts)).unwrap();
    let row = rows.iter().find(|row| row.row_uuid() == id(1)).unwrap();
    assert!(row.is_deleted());
    let values = cells(row);
    assert_eq!(values[0], ("label".into(), Some(Value::String("z".into()))));
    assert_eq!(values[1].0, "deleted");
    assert_ne!(values[1].1, Some(Value::Nullable(None)));
}

struct Tamper {
    link: Box<dyn Transport>,
    mode: &'static str,
    changed: std::rc::Rc<std::cell::Cell<usize>>,
}
impl Transport for Tamper {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let mut json = serde_json::to_value(message).unwrap();
        if let Some(body) = json
            .get_mut("Commit")
            .and_then(|commit| commit.get_mut("kind"))
            .and_then(|kind| kind.get_mut("SerializableUnionObservedV1"))
        {
            self.changed.set(self.changed.get() + 1);
            if self.mode == "alias" {
                body["reads"][0]["query"]["relation"]["rel"]["Project"]["columns"][0]["alias"] =
                    "forged".into();
            } else {
                body["reads"][0]["query"]["limit"] = 0.into();
            }
        }
        self.link.send(serde_json::from_value(json).unwrap())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.link.try_recv()
    }
}

/// Mallory alters Alice's alias or adds a conflicting ordinary limit in a real
/// upload. Wire mutation is needed to verify authority admission; neither forged
/// read may commit its companion write or silently discard the altered clause.
#[test]
fn tampered_alias_evidence_and_envelopes_are_rejected() {
    for mode in ["alias", "envelope"] {
        let server = open(0);
        doc(&server, 1, "z", "alice");
        let client = open(1);
        connect(&client, &server);
        let q = query(false, &[("title", "label")], false);
        fetch(&client, &[&client, &server], &q);
        client.set_remote_link_hint(RemoteLinkHint::Failed);
        let tx = begin(&client);
        let bytes = postcard::to_allocvec(&q).unwrap();
        block_on(client.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            Some(tx),
            None,
            None,
            false,
            || false,
            |_| {},
        ))
        .unwrap();
        block_on(client.serializable_tx_ref(tx).update(
            "docs",
            id(1),
            BTreeMap::from([("title".into(), Value::String("forged write".into()))]),
            Default::default(),
        ))
        .unwrap();
        let receipt = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        let (up, down) = duplex();
        let changed = std::rc::Rc::new(std::cell::Cell::new(0));
        block_on(client.connect_upstream(Box::new(Tamper {
            link: up,
            mode,
            changed: changed.clone(),
        })));
        server.accept_subscriber(down, alice());
        client.set_remote_link_hint(RemoteLinkHint::Live);
        turns(&[&client, &server]);
        assert!(changed.get() > 0);
        assert!(
            matches!(client.write_state(receipt).unwrap().fate, Fate::Rejected(_)),
            "{mode}"
        );
        assert_eq!(
            cells(&server.read(&server.prepare_query(&q).unwrap()).unwrap()[0]),
            vec![("label".into(), Some(Value::String("z".into())))]
        );
    }
}

/// Alice's real aliased upload keeps the existing v1 plain-row content role and
/// retained-relation wire kind. Byte inspection pins the persisted AST, output
/// cell name and discriminants; round-trip serialization alone is insufficient.
#[test]
fn alias_evidence_uses_existing_versioned_bytes() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v2/alias-evidence.json"
    ))
    .unwrap();
    let client = open(1);
    client.set_remote_link_hint(RemoteLinkHint::Failed);
    doc(&client, 1, "z", "alice");
    let q = query(false, &[("title", "label")], false);
    let tx = begin(&client);
    let bytes = postcard::to_allocvec(&q).unwrap();
    block_on(client.all_serialized_query(
        &bytes,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        Some(tx),
        None,
        None,
        false,
        || false,
        |_| {},
    ))
    .unwrap();
    block_on(client.commit_serializable_handle(tx)).unwrap();
    let (up, mut down) = duplex();
    block_on(client.connect_upstream(up));
    block_on(client.tick()).unwrap();
    let message = std::iter::from_fn(|| down.try_recv())
        .map(|message| serde_json::to_value(message).unwrap())
        .find(|message| {
            message["Commit"]["kind"]
                .get("SerializableUnionObservedV1")
                .is_some()
        })
        .unwrap();
    type SourceVersion = (u64, [u8; 16]);
    let empty_sources: Vec<([u8; 16], Option<SourceVersion>)> = vec![];
    let no_version: Option<SourceVersion> = None;
    let preimage = postcard::to_allocvec(&(
        "docs",
        [1_u8; 16],
        false,
        vec![("label", Some(Value::String("z".into())))],
        empty_sources,
        no_version,
    ))
    .unwrap();
    assert_eq!(
        fixture["wireRole"],
        "jazz.sqlite.serializable-union-observed.v1"
    );
    assert_eq!(
        fixture["contentRole"],
        "jazz.sqlite.serializable-read-content.v1"
    );
    assert_eq!(
        preimage,
        hex::decode(fixture["preimageHex"].as_str().unwrap()).unwrap()
    );
    let content = blake3::derive_key(fixture["contentRole"].as_str().unwrap(), &preimage);
    assert_eq!(
        message["Commit"]["kind"]["SerializableUnionObservedV1"]["content"],
        serde_json::json!([[content.to_vec()]])
    );
    let wire: SyncMessage = serde_json::from_value(serde_json::json!({"Commit": {
        "node": vec![0x22_u8; 16], "tx": 128,
        "kind": {"SerializableUnionObservedV1": {"snapshot": 127,
            "reads": [{"query": q, "binding": {}, "include_deleted": false, "rows": [[vec![1_u8;16], 1]]}],
            "own": [], "content": [vec![content.to_vec()]]}},
        "ops": [], "author": null, "session": null, "expect": []
    }})).unwrap();
    let bytes = hex::decode(fixture["commitHex"].as_str().unwrap()).unwrap();
    assert_eq!(jazz::db::encode_message(&wire).unwrap(), bytes);
    let decoded = jazz::sync_codec::decode_sync_message(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(wire).unwrap()
    );
}

/// Alice closes with an offline aliased read awaiting settlement. Reopening
/// preserves its projection evidence: Bob accepts unchanged content and rejects
/// a later source change through the normal upload path.
#[test]
fn pending_alias_evidence_survives_reopen() {
    for changed in [false, true] {
        let server = open(0);
        doc(&server, 1, "z", "alice");
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("alice.sqlite");
        let q = query(false, &[("title", "label")], false);
        let receipt = {
            let client = block_on(Db::open_at(config(1), &path, Default::default())).unwrap();
            connect(&client, &server);
            fetch(&client, &[&client, &server], &q);
            client.set_remote_link_hint(RemoteLinkHint::Failed);
            let tx = begin(&client);
            let bytes = postcard::to_allocvec(&q).unwrap();
            let result = block_on(client.all_serialized_query(
                &bytes,
                ReadOpts::for_read_tier(ReadTier::LocalOnly),
                Some(tx),
                None,
                None,
                false,
                || false,
                |_| {},
            ))
            .unwrap();
            let SerializedReadResult::Rows(rows) = result else {
                panic!("rows")
            };
            assert_eq!(
                cells(&rows[0]),
                vec![("label".into(), Some(Value::String("z".into())))]
            );
            block_on(client.commit_serializable_handle(tx)).unwrap().0
        };
        if changed {
            block_on(server.update(
                "docs",
                id(1),
                BTreeMap::from([("title".into(), Value::String("changed".into()))]),
                Default::default(),
            ))
            .unwrap();
        }
        let client = block_on(Db::open_at(config(1), &path, Default::default())).unwrap();
        connect(&client, &server);
        turns(&[&client, &server]);
        let fate = client.write_state(receipt).unwrap().fate;
        if changed {
            assert!(matches!(fate, Fate::Rejected(_)));
        } else {
            assert_eq!(fate, Fate::Accepted);
        }
    }
}

//! Catalogue policy values come from accepted manifests, never session claims.
mod common;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier, UpdateOptions,
    WriteIdentity, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, LensOp, MigrationLens, PhysicalIdentityManifest,
    SchemaLineagePublication, SchemaVersion, TableLens,
};
use jazz::query::{
    Query, RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationValueRef,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, ObjectId, Operation, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder, Value as PublicValue,
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn path(table: &str) -> Vec<String> {
    vec!["__jazz_table_identity_v1".into(), table.into()]
}
fn intrinsic(table: &str) -> PolicyExpr {
    PolicyExpr::eq_session("scopeId", path(table))
}
fn schema(table: &str, policy: PolicyExpr) -> JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column("scopeId", ColumnType::Uuid)
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(policy.clone())
                            .with_insert(policy.clone())
                            .with_update(Some(policy.clone()), policy.clone())
                            .with_delete(policy),
                    ),
            )
            .build(),
    )
}
fn snapshot(schema: JazzSchema) -> CatalogueSnapshot {
    CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&schema),
        schemas: vec![SchemaVersion::new(schema.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: schema.version_id(),
        },
    }
}
fn open(node: u8, schema: JazzSchema) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}
fn authority(snapshot: &CatalogueSnapshot) -> Db {
    let db = open(3, snapshot.schemas[0].schema.clone());
    db.set_authority();
    db.install_authority_catalogue(snapshot).unwrap();
    db
}
fn cells(scope: uuid::Uuid, text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("scopeId".into(), Value::Uuid(scope)),
        ("text".into(), Value::String(text.into())),
    ])
}
fn seed(db: &Db, table: &str, scope: uuid::Uuid, text: &str) -> RowUuid {
    block_on(db.insert(table, cells(scope, text), Default::default()))
        .unwrap()
        .row_uuid()
}
fn ids(db: &Db, table: &str) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let mut ids = block_on(db.all_for_identity(
        &prepared,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        alice(),
    ))
    .unwrap()
    .iter()
    .map(|r| r.row_uuid())
    .collect::<Vec<_>>();
    ids.sort();
    ids
}
fn spoof(db: &Db, scope: uuid::Uuid) {
    db.set_identity_claims(
        alice(),
        BTreeMap::from([
            (
                format!("{}documents", jazz::query::TABLE_IDENTITY_OPERAND_PREFIX),
                Value::Uuid(scope),
            ),
            ("__jazz_table_identity_v1".into(), Value::Uuid(scope)),
            (
                jazz::query::provider_claim_key("__jazz_table_identity_v1"),
                Value::Uuid(scope),
            ),
        ]),
    );
}
fn connect(client: &Db, server: &Db, author: AuthorSubject) {
    let (a, b) = duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, author);
}
fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        turn(dbs);
    }
    panic!("operation did not finish")
}

/// Alice reads only the scope named by the authority's UUID, even when Mallory
/// supplies matching spoofed claims for a different stored scope.
#[test]
fn uuid_policy_and_lowered_operand_ignore_spoofed_claims() {
    let snapshot = snapshot(schema("documents", intrinsic("documents")));
    let db = authority(&snapshot);
    let scope = db.table_identity("documents").unwrap().unwrap().0;
    let fake = uuid::Uuid::new_v4();
    let good = seed(&db, "documents", scope, "accepted");
    seed(&db, "documents", fake, "forged");
    spoof(&db, fake);
    assert_eq!(ids(&db, "documents"), vec![good]);
    let key = format!("{}documents", jazz::query::TABLE_IDENTITY_OPERAND_PREFIX);
    let query = Query::from("documents").filter(jazz::query::eq(
        jazz::query::col("scopeId"),
        jazz::query::claim(&key),
    ));
    // SYSTEM skips row policies, so this separately tests the lowered operand.
    let prepared = db.prepare_query(&query).unwrap().with_identity_claims(
        AuthorSubject::SYSTEM,
        BTreeMap::from([(key, Value::Uuid(fake))]),
    );
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), good);
}

/// The generated scope binding governs Alice's INSERT and both UPDATE clauses;
/// a rejected scope reassignment rolls back and cannot replace the accepted row.
#[test]
fn insert_and_update_use_accepted_scope_ids() {
    let snapshot = snapshot(schema("documents", intrinsic("documents")));
    let db = authority(&snapshot);
    let scope = db.table_identity("documents").unwrap().unwrap().0;
    let fake = uuid::Uuid::new_v4();
    spoof(&db, fake);
    let opts = InsertOptions {
        identity: WriteIdentity::Session(alice()),
        ..Default::default()
    };
    let good = block_on(db.insert("documents", cells(scope, "good"), opts.clone()))
        .unwrap()
        .row_uuid();
    assert!(block_on(db.insert("documents", cells(fake, "bad"), opts)).is_err());
    let opts = UpdateOptions {
        identity: WriteIdentity::Session(alice()),
        ..Default::default()
    };
    assert!(
        block_on(db.update(
            "documents",
            good,
            BTreeMap::from([("scopeId".into(), Value::Uuid(fake))]),
            opts.clone()
        ))
        .is_err()
    );
    block_on(db.update(
        "documents",
        good,
        BTreeMap::from([("text".into(), Value::String("updated".into()))]),
        opts,
    ))
    .unwrap();
    assert_eq!(ids(&db, "documents"), vec![good]);
    let rows = block_on(db.all(
        &db.prepare_query(&Query::from("documents")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cells().find(|(n, _)| *n == "scopeId").unwrap().1,
        Some(&Value::Uuid(scope))
    );
}

/// Alice's scalar, list and null predicates resolve the same non-null UUID.
/// Missing metadata never becomes a true IS NULL or a provider-claim fallback.
#[test]
fn every_policy_value_form_uses_metadata_and_missing_metadata_rejects() {
    let id = uuid::Uuid::from_bytes([0x31; 16]);
    let literal = PublicValue::Uuid(ObjectId::from_uuid(id));
    let cases = vec![
        (intrinsic("documents"), true),
        (
            PolicyExpr::In {
                column: "scopeId".into(),
                session_path: path("documents"),
            },
            true,
        ),
        (
            PolicyExpr::InList {
                column: "scopeId".into(),
                values: vec![PolicyValue::SessionRef(path("documents"))],
            },
            true,
        ),
        (
            PolicyExpr::SessionCmp {
                path: path("documents"),
                op: CmpOp::Eq,
                value: literal.clone(),
            },
            true,
        ),
        (
            PolicyExpr::SessionInList {
                path: path("documents"),
                values: vec![literal.clone()],
            },
            true,
        ),
        (
            PolicyExpr::SessionIsNotNull {
                path: path("documents"),
            },
            true,
        ),
        (
            PolicyExpr::SessionIsNull {
                path: path("documents"),
            },
            false,
        ),
        (
            PolicyExpr::SessionContains {
                path: path("documents"),
                value: literal,
            },
            false,
        ),
    ];
    for (policy, visible) in cases {
        let mut snapshot = snapshot(schema("documents", policy));
        snapshot
            .genesis_physical_identities
            .tables
            .get_mut("documents")
            .unwrap()
            .id
            .0 = id;
        let db = authority(&snapshot);
        seed(&db, "documents", id, "row");
        spoof(&db, id);
        assert_eq!(ids(&db, "documents").len(), usize::from(visible));
        let unaccepted = open(1, snapshot.schemas[0].schema.clone());
        seed(&unaccepted, "documents", id, "row");
        spoof(&unaccepted, id);
        let prepared = unaccepted.prepare_query(&Query::from("documents")).unwrap();
        let error = block_on(unaccepted.all_for_identity(&prepared, ReadOpts::default(), alice()))
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert!(error.message.contains("CatalogueTableIdentityV1"));
    }
}

/// Malformed reserved paths and unknown tables must reject, even on an accepted
/// authority and even inside null predicates that would accept absent user data.
#[test]
fn malformed_and_unknown_intrinsics_never_fall_back_to_null() {
    for path in [
        vec!["__jazz_table_identity_v1".into()],
        vec![
            "__jazz_table_identity_v1".into(),
            "documents".into(),
            "extra".into(),
        ],
        path(""),
    ] {
        let public = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .policies(TablePolicies::new().with_select(PolicyExpr::SessionIsNull { path })),
            )
            .build();
        assert!(
            JazzSchema::new(&public).is_err(),
            "malformed paths must fail during schema admission"
        );
    }
    let db = authority(&snapshot(schema(
        "documents",
        PolicyExpr::SessionIsNull {
            path: path("unknown"),
        },
    )));
    seed(&db, "documents", uuid::Uuid::new_v4(), "row");
    let prepared = db.prepare_query(&Query::from("documents")).unwrap();
    let error = block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice())).unwrap_err();
    assert!(error.message.contains("CatalogueTableIdentityV1"));
    for table in ["", "unknown"] {
        let key = format!("{}{table}", jazz::query::TABLE_IDENTITY_OPERAND_PREFIX);
        let query = Query::from("documents").filter(jazz::query::eq(
            jazz::query::col("scopeId"),
            jazz::query::claim(&key),
        ));
        let error = db.prepare_query(&query).unwrap_err();
        assert!(error.message.contains("CatalogueTableIdentityV1"));
    }
}

/// Alice's inherited parent policy and EXISTS/relation evidence use the same
/// accepted scope IDs, including when the evidence table itself is unreadable.
#[test]
fn nested_inherited_and_relational_policies_bind_the_policy_schema() {
    for relational in [false, true] {
        let evidence = if relational {
            PolicyExpr::ExistsRel {
                rel: RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "grants".into(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::And(vec![
                        RelationPredicate::Cmp {
                            left: RelationColumnRef {
                                scope: None,
                                column: "scopeId".into(),
                            },
                            op: RelationCmpOp::Eq,
                            right: RelationValueRef::SessionRef(path("documents")),
                        },
                        RelationPredicate::Cmp {
                            left: RelationColumnRef {
                                scope: None,
                                column: "document".into(),
                            },
                            op: RelationCmpOp::Eq,
                            right: RelationValueRef::OuterColumn(RelationColumnRef {
                                scope: None,
                                column: "id".into(),
                            }),
                        },
                    ]),
                },
            }
        } else {
            common::exists(
                "grants",
                vec![intrinsic("documents"), common::outer_eq("document", "id")],
            )
        };
        let schema = common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("documents")
                        .column("text", ColumnType::Text)
                        .policies(TablePolicies::new().with_select(evidence)),
                )
                .table(
                    TableSchemaBuilder::new("grants")
                        .fk_column("document", "documents")
                        .column("scopeId", ColumnType::Uuid),
                )
                .table(
                    TableSchemaBuilder::new("children")
                        .fk_column("document", "documents")
                        .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                            operation: Operation::Select,
                            via_column: "document".into(),
                            max_depth: None,
                        })),
                )
                .build(),
        );
        let db = authority(&snapshot(schema));
        let scope = db.table_identity("documents").unwrap().unwrap().0;
        let good = block_on(db.insert(
            "documents",
            BTreeMap::from([("text".into(), Value::String("good".into()))]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid();
        let bad = block_on(db.insert(
            "documents",
            BTreeMap::from([("text".into(), Value::String("bad".into()))]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid();
        let mut child = None;
        for (document, scope_id) in [(good, scope), (bad, uuid::Uuid::new_v4())] {
            block_on(db.insert(
                "grants",
                BTreeMap::from([
                    ("document".into(), Value::Uuid(document.0)),
                    ("scopeId".into(), Value::Uuid(scope_id)),
                ]),
                Default::default(),
            ))
            .unwrap();
            let row = block_on(db.insert(
                "children",
                BTreeMap::from([("document".into(), Value::Uuid(document.0))]),
                Default::default(),
            ))
            .unwrap()
            .row_uuid();
            if document == good {
                child = Some(row);
            }
        }
        assert!(ids(&db, "grants").is_empty());
        assert_eq!(ids(&db, "documents"), vec![good]);
        assert_eq!(ids(&db, "children"), vec![child.unwrap()]);
    }
}

/// A cold Alice replica learns metadata through sync and its ordinary subscription
/// returns only authorized rows. Scope changes then remove/re-add those rows.
#[test]
fn cold_sync_and_subscription_updates_use_catalogue_policies() {
    let schema = schema("documents", intrinsic("documents"));
    let snapshot = snapshot(schema.clone());
    let server = authority(&snapshot);
    let scope = server.table_identity("documents").unwrap().unwrap().0;
    let good = seed(&server, "documents", scope, "good");
    seed(&server, "documents", uuid::Uuid::new_v4(), "bad");
    let alice_db = open(1, schema);
    connect(&alice_db, &server, alice());
    drive(
        &[&alice_db, &server],
        alice_db.cover_catalogue("documents", None),
    )
    .unwrap();
    let prepared = alice_db.prepare_query(&Query::from("documents")).unwrap();
    let mut stream =
        block_on(alice_db.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    for _ in 0..8 {
        turn(&[&alice_db, &server]);
    }
    let mut added = vec![];
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta { added: a, .. } = event {
            added.extend(a.iter().map(|r| r.row_uuid()));
        }
    }
    assert_eq!(added, vec![good]);
    block_on(server.update(
        "documents",
        good,
        BTreeMap::from([("scopeId".into(), Value::Uuid(uuid::Uuid::new_v4()))]),
        Default::default(),
    ))
    .unwrap();
    for _ in 0..6 {
        turn(&[&alice_db, &server]);
    }
    assert!(std::iter::from_fn(||stream.try_next_event()).any(|e|matches!(e,jazz::db::SubscriptionEvent::Delta{removed,..} if removed.iter().any(|r|r.row_uuid==good))));
    block_on(server.update(
        "documents",
        good,
        BTreeMap::from([("scopeId".into(), Value::Uuid(scope))]),
        Default::default(),
    ))
    .unwrap();
    for _ in 0..6 {
        turn(&[&alice_db, &server]);
    }
    assert!(std::iter::from_fn(||stream.try_next_event()).any(|e|matches!(e,jazz::db::SubscriptionEvent::Delta{added,..} if added.iter().any(|r|r.row_uuid()==good))));
}

/// Alice stays on the old names while Bob publishes and activates a rename.
/// Server policies name the new schema; a fixed local view still names the old.
#[test]
fn renamed_policy_schema_governs_old_client_views_and_live_subscriptions() {
    let source = schema("documents", intrinsic("documents"));
    let mut snapshot = snapshot(source.clone());
    let server = authority(&snapshot);
    let scope = server.table_identity("documents").unwrap().unwrap().0;
    let good = seed(&server, "documents", scope, "good");
    let fixed = block_on(server.register_schema_view(source.clone())).unwrap();
    let client = open(1, source.clone());
    connect(&client, &server, alice());
    drive(
        &[&client, &server],
        client.cover_catalogue("documents", None),
    )
    .unwrap();
    let query = client.prepare_query(&Query::from("documents")).unwrap();
    let mut stream =
        block_on(client.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    for _ in 0..6 {
        turn(&[&client, &server]);
    }
    while stream.try_next_event().is_some() {}
    let target = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("scopeId", ColumnType::Uuid)
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(intrinsic("tasks"))),
            )
            .build(),
    );
    let lens = MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: "documents".into(),
            target_table: "tasks".into(),
            ops: vec![LensOp::RenameColumn {
                from: "text".into(),
                to: "body".into(),
            }],
        }],
    )
    .unwrap();
    let publication = SchemaLineagePublication::author_from_prior(
        &source,
        &snapshot.genesis_physical_identities,
        SchemaVersion::new(target.clone()),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot.schemas.push(SchemaVersion::new(target.clone()));
    snapshot.lineages.push((1, publication));
    // The native runtime publishes the descriptor before activating its policies.
    block_on(server.admit_schema_with_lenses(target.clone(), std::slice::from_ref(&lens))).unwrap();
    server.install_authority_catalogue(&snapshot).unwrap();
    block_on(server.activate_schema_with_lenses(target.clone(), &[lens])).unwrap();
    snapshot.current_write_schema = CurrentWriteSchema {
        revision: 2,
        schema: target.version_id(),
    };
    server.install_authority_catalogue(&snapshot).unwrap();
    assert_eq!(ids(&server, "tasks"), vec![good]);
    assert_eq!(ids(&fixed, "documents"), vec![good]);
    drive(
        &[&client, &server],
        client.cover_catalogue("documents", None),
    )
    .unwrap();
    for _ in 0..6 {
        turn(&[&client, &server]);
    }
    while let Some(event) = stream.try_next_event() {
        assert!(
            !matches!(event, jazz::db::SubscriptionEvent::Rejected { .. }),
            "{event:?}"
        );
    }
    assert_eq!(ids(&client, "documents"), vec![good]);
    block_on(server.update(
        "tasks",
        good,
        BTreeMap::from([("scopeId".into(), Value::Uuid(uuid::Uuid::new_v4()))]),
        Default::default(),
    ))
    .unwrap();
    for _ in 0..6 {
        turn(&[&client, &server]);
    }
    assert!(std::iter::from_fn(||stream.try_next_event()).any(|e|matches!(e,jazz::db::SubscriptionEvent::Delta{removed,..} if removed.iter().any(|r|r.row_uuid==good))));
}

/// An unpublished local typed view has a valid schema but no accepted manifest.
/// Alice's policy checks reject until that exact descriptor is published.
#[test]
fn registering_an_unpublished_view_cannot_supply_policy_identities() {
    let db = authority(&snapshot(schema("documents", intrinsic("documents"))));
    let view = block_on(db.register_schema_view(schema("drafts", intrinsic("drafts")))).unwrap();
    seed(&view, "drafts", uuid::Uuid::new_v4(), "unpublished");
    let prepared = view.prepare_query(&Query::from("drafts")).unwrap();
    let error =
        block_on(view.all_for_identity(&prepared, ReadOpts::default(), alice())).unwrap_err();
    assert!(error.message.contains("CatalogueTableIdentityV1"));
}

/// Alice's nested document read uses the authority's accepted table identity
/// for both parent lookup and child policies, ignoring Mallory's spoofed claims.
#[test]
fn nested_reads_keep_the_accepted_catalogue_identity() {
    let db = authority(&snapshot(schema("documents", intrinsic("documents"))));
    let scope = db.table_identity("documents").unwrap().unwrap().0;
    let mut expected = vec![
        seed(&db, "documents", scope, "first"),
        seed(&db, "documents", scope, "second"),
    ];
    expected.sort();
    let forged = uuid::Uuid::new_v4();
    seed(&db, "documents", forged, "hidden");
    spoof(&db, forged);
    let query = Query::from("documents").array_subquery(
        jazz::query::ArraySubquery::new("related", "documents", "scopeId", "scopeId")
            .select(["text"]),
    );
    let prepared = db.prepare_query(&query).unwrap();
    let mut stream =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    let jazz::db::SubscriptionEvent::Delta { added: rows, .. } =
        block_on(stream.next_event()).unwrap()
    else {
        panic!("expected document subscription rows");
    };
    let mut parents = rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
    parents.sort();
    assert_eq!(parents, expected);
    for row in rows {
        let Value::Array(children) = row
            .cells()
            .find(|(name, _)| *name == "related")
            .unwrap()
            .1
            .unwrap()
        else {
            panic!("expected related documents");
        };
        let mut ids = children
            .iter()
            .map(|child| {
                let Value::Record(child) = child else {
                    panic!("expected document record")
                };
                let Value::Uuid(id) = child.get_idx(0).unwrap() else {
                    panic!("expected document id")
                };
                RowUuid(id)
            })
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, expected);
    }
}

/// Alice's published policy names CatalogueTableIdentityV1 with a stable path
/// and lowered operand. These byte checks are needed because query round trips
/// alone would not pin the reserved namespace used by persisted schemas.
#[test]
fn catalogue_identity_v1_pins_the_public_path_and_lowered_operand() {
    let policy = intrinsic("documents");
    assert_eq!(serde_json::to_vec(&policy).unwrap(), br#"{"type":"Cmp","column":"scopeId","op":"Eq","value":{"type":"SessionRef","path":["__jazz_table_identity_v1","documents"]}}"#);
    let key = format!("{}documents", jazz::query::TABLE_IDENTITY_OPERAND_PREFIX);
    let operand = jazz::query::claim(&key);
    // Existing Operand::Claim is tag 2, followed by the 33-byte UTF-8 key.
    let expected = b"\x02\x21\0jazz-table-identity-v1:documents";
    assert_eq!(postcard::to_allocvec(&operand).unwrap(), expected);
    let (decoded, rest) = postcard::take_from_bytes::<jazz::query::Operand>(expected).unwrap();
    assert_eq!(decoded, operand);
    assert!(rest.is_empty());
}

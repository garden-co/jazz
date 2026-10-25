//! Query inheritance uses the same parent capabilities as table policies.
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{InheritsOperation, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn bob() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "bob").unwrap()
}
fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn owner() -> PolicyExpr {
    PolicyExpr::eq_session(
        "owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    )
}
fn db() -> Db {
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("parents")
                .column("owner", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(owner())
                        .with_insert(PolicyExpr::False)
                        .with_update(Some(owner()), PolicyExpr::False)
                        .with_delete(PolicyExpr::False),
                ),
        )
        .table(
            TableSchemaBuilder::new("children")
                .fk_column("parent", "parents")
                .column("owner", ColumnType::Text)
                .policies(TablePolicies::new().with_select(owner())),
        )
        .table(
            TableSchemaBuilder::new("grandchildren")
                .fk_column("child", "children")
                .policies(TablePolicies::new().with_select(PolicyExpr::True)),
        )
        .table(
            TableSchemaBuilder::new("bundles")
                .array_fk_column("parents", "parents")
                .policies(TablePolicies::new().with_select(PolicyExpr::True)),
        )
        .table(
            TableSchemaBuilder::new("checks")
                .column("label", ColumnType::Text)
                .policies(TablePolicies::new().with_update(None, PolicyExpr::False)),
        )
        .table(
            TableSchemaBuilder::new("check_children")
                .fk_column("parent", "checks")
                .policies(TablePolicies::new().with_select(PolicyExpr::True)),
        )
        .table(
            TableSchemaBuilder::new("dependents")
                .fk_column("parent", "checks")
                .policies(TablePolicies::new().with_insert(PolicyExpr::Inherits {
                    operation: Operation::Update,
                    via_column: "parent".into(),
                    max_depth: None,
                })),
        )
        .table(
            TableSchemaBuilder::new("folders")
                .nullable_fk_column("parent", "folders")
                .column("owner", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::or(vec![
                    owner(),
                    PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "parent".into(),
                        max_depth: None,
                    },
                ]))),
        )
        .build();
    let db = block_on(Db::open(DbConfig::new(
        JazzSchema::new(&source).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x70; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for (id, owner) in [(1, "alice"), (2, "bob")] {
        put(
            &db,
            "parents",
            id,
            vec![("owner", Value::String(owner.into()))],
        );
    }
    for (id, parent, owner) in [
        (10, 1, "alice"),
        (11, 2, "alice"),
        (12, 1, "bob"),
        (13, 99, "alice"),
    ] {
        put(
            &db,
            "children",
            id,
            vec![
                ("parent", Value::Uuid(row(parent).0)),
                ("owner", Value::String(owner.into())),
            ],
        );
    }
    put(
        &db,
        "grandchildren",
        20,
        vec![("child", Value::Uuid(row(10).0))],
    );
    for (id, parents) in [
        (30, vec![1, 1, 2]),
        (31, vec![2]),
        (32, vec![]),
        (33, vec![99]),
    ] {
        put(
            &db,
            "bundles",
            id,
            vec![(
                "parents",
                Value::Array(
                    parents
                        .into_iter()
                        .map(|id| Value::Uuid(row(id).0))
                        .collect(),
                ),
            )],
        );
    }
    put(
        &db,
        "checks",
        40,
        vec![("label", Value::String("check-only update".into()))],
    );
    put(
        &db,
        "check_children",
        41,
        vec![("parent", Value::Uuid(row(40).0))],
    );
    for (id, parent, owner) in [
        (50, None, "alice"),
        (51, Some(50), "bob"),
        (52, Some(51), "bob"),
        (53, Some(54), "bob"),
        (54, Some(53), "bob"),
    ] {
        put(
            &db,
            "folders",
            id,
            vec![
                (
                    "parent",
                    parent.map_or(Value::Nullable(None), |parent| {
                        Value::Nullable(Some(Box::new(Value::Uuid(row(parent).0))))
                    }),
                ),
                ("owner", Value::String(owner.into())),
            ],
        );
    }
    db
}
fn put(db: &Db, table: &str, id: u8, cells: Vec<(&str, Value)>) {
    block_on(db.insert(
        table,
        cells.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        InsertOptions {
            row_id: Some(row(id)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn ids(db: &Db, query: Query, author: AuthorSubject) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&query).unwrap();
    let mut rows: Vec<_> = block_on(db.all_for_identity(&prepared, ReadOpts::default(), author))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    rows.sort();
    rows
}

/// Alice may read a child only when both its own policy and the requested
/// parent capability hold. Bob's parent, dangling references and forbidden
/// operations never match; UPDATE inherits USING, independently of CHECK.
#[test]
fn query_inherits_keeps_child_policy_and_parent_operation() {
    let db = db();
    assert_eq!(
        ids(&db, Query::from("children"), alice()),
        vec![row(10), row(11), row(13)]
    );
    assert_eq!(
        ids(&db, Query::from("children").inherits("parent"), alice()),
        vec![row(10)]
    );
    assert!(ids(&db, Query::from("children").inherits("parent"), bob()).is_empty());
    assert_eq!(
        ids(
            &db,
            Query::from("children").inherits_operation("parent", InheritsOperation::Update),
            alice()
        ),
        vec![row(10)]
    );
    for operation in [InheritsOperation::Insert, InheritsOperation::Delete] {
        assert!(
            ids(
                &db,
                Query::from("children").inherits_operation("parent", operation),
                alice()
            )
            .is_empty()
        );
    }
    assert_eq!(
        ids(
            &db,
            Query::from("check_children").inherits_operation("parent", InheritsOperation::Update),
            alice()
        ),
        vec![row(41)]
    );
    assert!(
        ids(
            &db,
            Query::from("check_children").inherits("parent"),
            alice()
        )
        .is_empty()
    );
    block_on(db.close()).unwrap();
}

/// Alice's bounded traversal stops before an unproven parent and terminates
/// cycles. UUID arrays use existential membership without duplicating children.
#[test]
fn query_inherits_honours_depth_arrays_and_cycles() {
    let db = db();
    assert!(
        ids(
            &db,
            Query::from("children").inherits_with_depth("parent", 0),
            alice()
        )
        .is_empty()
    );
    assert_eq!(
        ids(
            &db,
            Query::from("folders").inherits_with_depth("parent", 1),
            alice()
        ),
        vec![row(51)]
    );
    assert_eq!(
        ids(
            &db,
            Query::from("folders").inherits_with_depth("parent", 2),
            alice()
        ),
        vec![row(51), row(52)]
    );
    assert_eq!(
        ids(&db, Query::from("folders").inherits("parent"), alice()),
        vec![row(51), row(52)]
    );
    assert_eq!(
        ids(&db, Query::from("bundles").inherits("parents"), alice()),
        vec![row(30)]
    );
    assert_eq!(
        ids(&db, Query::from("bundles").inherits("parents"), bob()),
        vec![row(30), row(31)]
    );
    block_on(db.close()).unwrap();
}

/// Alice's maintained query loses and regains the child when Bob changes the
/// parent's owner, then loses it permanently on parent deletion. These changes
/// must invalidate the subscription even though no child bytes changed.
#[test]
fn inherited_query_subscription_tracks_parent_policy_changes() {
    let db = db();
    let prepared = db
        .prepare_query(&Query::from("children").inherits("parent"))
        .unwrap();
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    let SubscriptionEvent::Delta {
        reset: true, added, ..
    } = subscription.try_next_event().unwrap()
    else {
        panic!("initial reset");
    };
    assert_eq!(
        added.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
        vec![row(10)]
    );
    block_on(db.update(
        "parents",
        row(1),
        BTreeMap::from([("owner".into(), Value::String("bob".into()))]),
        Default::default(),
    ))
    .unwrap();
    let SubscriptionEvent::Delta {
        reset: false,
        removed,
        ..
    } = subscription.try_next_event().unwrap()
    else {
        panic!("revocation delta");
    };
    assert_eq!(
        removed.iter().map(|r| r.row_uuid).collect::<Vec<_>>(),
        vec![row(10)]
    );
    block_on(db.update(
        "parents",
        row(1),
        BTreeMap::from([("owner".into(), Value::String("alice".into()))]),
        Default::default(),
    ))
    .unwrap();
    let SubscriptionEvent::Delta {
        reset: false,
        added,
        ..
    } = subscription.try_next_event().unwrap()
    else {
        panic!("restored grant");
    };
    assert_eq!(
        added.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
        vec![row(10)]
    );
    block_on(db.delete("parents", row(1), Default::default())).unwrap();
    let SubscriptionEvent::Delta {
        reset: false,
        removed,
        ..
    } = subscription.try_next_event().unwrap()
    else {
        panic!("deleted parent");
    };
    assert_eq!(
        removed.iter().map(|r| r.row_uuid).collect::<Vec<_>>(),
        vec![row(10)]
    );
    assert!(subscription.try_next_event().is_none());
    drop(subscription);
    block_on(db.close()).unwrap();
}

/// Alice can use the accepted parent's CHECK-only UPDATE capability to create
/// a dependent. The parent's own edit still fails CHECK, and deleting the
/// parent removes the capability. This also covers ordinary write evaluation.
#[test]
fn check_only_update_capability_does_not_make_the_parent_mutable() {
    let db = db();
    let cells = BTreeMap::from([("parent".into(), Value::Uuid(row(40).0))]);
    let options = || InsertOptions {
        identity: jazz::db::WriteIdentity::Session(alice()),
        ..Default::default()
    };
    block_on(db.insert("dependents", cells.clone(), options())).unwrap();
    assert!(
        block_on(db.update(
            "checks",
            row(40),
            BTreeMap::from([("label".into(), Value::String("changed".into()))]),
            jazz::db::UpdateOptions {
                identity: jazz::db::WriteIdentity::Session(alice()),
                ..Default::default()
            }
        ))
        .is_err()
    );
    block_on(db.delete("checks", row(40), Default::default())).unwrap();
    assert!(block_on(db.insert("dependents", cells, options())).is_err());
    block_on(db.close()).unwrap();
}

/// Alice's authenticated query under old names must use Bob's newly activated
/// parent policy after a table/column rename, including its new field names.
/// A later new column reusing the original name cannot borrow the old epoch.
#[test]
fn inherited_query_uses_current_parent_policy_through_an_old_schema_view() {
    use jazz::protocol::{
        CatalogueSnapshot, CurrentWriteSchema, LensOp, MigrationLens, PhysicalIdentityManifest,
        SchemaLineagePublication, SchemaVersion, TableLens,
    };
    let make_schema = |renamed: bool| {
        let (parents, owner_column, parent_column) = if renamed {
            ("projects", "subject", "project")
        } else {
            ("parents", "owner", "parent")
        };
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new(parents)
                        .column(owner_column, ColumnType::Text)
                        .policies(TablePolicies::new().with_select(if renamed {
                            PolicyExpr::eq_session(
                                owner_column,
                                vec!["user".into(), "identity".into(), "subject".into()],
                            )
                        } else {
                            PolicyExpr::True
                        })),
                )
                .table(
                    TableSchemaBuilder::new("children")
                        .fk_column(parent_column, parents)
                        .policies(TablePolicies::new().with_select(PolicyExpr::True)),
                )
                .build(),
        )
        .unwrap()
    };
    let source = make_schema(false);
    let target = make_schema(true);
    let db = block_on(Db::open(DbConfig::new(
        source.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    let mut snapshot = CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&source),
        schemas: vec![SchemaVersion::new(source.clone())],
        lineages: Vec::new(),
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: source.version_id(),
        },
    };
    db.install_authority_catalogue(&snapshot).unwrap();
    for (parent, child, owner) in [(1, 10, "alice"), (2, 11, "bob")] {
        put(
            &db,
            "parents",
            parent,
            vec![("owner", Value::String(owner.into()))],
        );
        put(
            &db,
            "children",
            child,
            vec![("parent", Value::Uuid(row(parent).0))],
        );
    }
    let local_fixed = block_on(db.register_schema_view(source.clone())).unwrap();
    let client = block_on(Db::open(DbConfig::new(
        source.without_permissions(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x72; 16]),
            author: alice(),
        },
    )))
    .unwrap();
    client.set_non_durable_client();
    let (up, down) = jazz::db::duplex();
    block_on(client.connect_upstream(up));
    db.accept_subscriber(down, alice());
    let tick = || {
        block_on(client.tick()).unwrap();
        block_on(db.tick()).unwrap();
    };
    fn drive<T>(tick: impl Fn(), future: impl std::future::Future<Output = T>) -> T {
        use std::task::{Context, Poll, Waker};
        let mut future = std::pin::pin!(future);
        for _ in 0..100 {
            if let Poll::Ready(value) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                return value;
            }
            tick();
        }
        panic!("catalogue admission did not finish");
    }
    drive(tick, client.cover_catalogue("children", None)).unwrap();
    let mut streams = Vec::new();
    for table in ["parents", "children"] {
        let query = client.prepare_query(&Query::from(table)).unwrap();
        streams.push(
            block_on(client.subscribe(&query, ReadOpts::for_read_tier(jazz::db::ReadTier::Remote)))
                .unwrap(),
        );
    }
    for _ in 0..12 {
        tick();
    }
    let fixed = block_on(client.register_schema_view(source.without_permissions())).unwrap();
    let prepared = fixed
        .prepare_query(&Query::from("children").inherits("parent"))
        .unwrap();
    assert_eq!(
        ids(&fixed, Query::from("children").inherits("parent"), alice()),
        vec![row(10), row(11)]
    );
    let lens = MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![
            TableLens {
                source_table: "parents".into(),
                target_table: "projects".into(),
                ops: vec![LensOp::RenameColumn {
                    from: "owner".into(),
                    to: "subject".into(),
                }],
            },
            TableLens {
                source_table: "children".into(),
                target_table: "children".into(),
                ops: vec![LensOp::RenameColumn {
                    from: "parent".into(),
                    to: "project".into(),
                }],
            },
        ],
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
    block_on(db.admit_schema_with_lenses(target.clone(), std::slice::from_ref(&lens))).unwrap();
    db.install_authority_catalogue(&snapshot).unwrap();
    block_on(db.activate_schema_with_lenses(target.clone(), &[lens])).unwrap();
    snapshot.current_write_schema = CurrentWriteSchema {
        revision: 2,
        schema: target.version_id(),
    };
    db.install_authority_catalogue(&snapshot).unwrap();
    drive(tick, client.cover_catalogue("children", None)).unwrap();
    for _ in 0..12 {
        tick();
    }
    // A plain local view intentionally retains its original contract.
    assert_eq!(
        ids(
            &local_fixed,
            Query::from("children").inherits("parent"),
            alice()
        ),
        vec![row(10), row(11)]
    );
    assert_eq!(
        ids(&fixed, Query::from("children").inherits("parent"), alice()),
        vec![row(10)]
    );
    let rows = block_on(fixed.all_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![row(10)]
    );
    assert_eq!(
        ids(&db, Query::from("children").inherits("project"), alice()),
        vec![row(10)]
    );
    let next = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("subject", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(owner())),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("project", "projects")
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap();
    let addition = MigrationLens::new(
        target.version_id(),
        next.version_id(),
        vec![
            TableLens {
                source_table: "projects".into(),
                target_table: "projects".into(),
                ops: vec![LensOp::AddColumn {
                    column: "owner".into(),
                    default: Value::String("alice".into()),
                }],
            },
            TableLens {
                source_table: "children".into(),
                target_table: "children".into(),
                ops: Vec::new(),
            },
        ],
    )
    .unwrap();
    let prior = snapshot
        .lineages
        .last()
        .unwrap()
        .1
        .physical_identities
        .clone();
    let publication = SchemaLineagePublication::author_from_prior(
        &target,
        &prior,
        SchemaVersion::new(next.clone()),
        addition.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot.schemas.push(SchemaVersion::new(next.clone()));
    snapshot.lineages.push((2, publication));
    block_on(db.admit_schema_with_lenses(next.clone(), std::slice::from_ref(&addition))).unwrap();
    db.install_authority_catalogue(&snapshot).unwrap();
    block_on(db.activate_schema_with_lenses(next.clone(), &[addition])).unwrap();
    snapshot.current_write_schema = CurrentWriteSchema {
        revision: 3,
        schema: next.version_id(),
    };
    db.install_authority_catalogue(&snapshot).unwrap();
    drive(tick, client.cover_catalogue("children", None)).unwrap();
    for _ in 0..12 {
        tick();
    }
    let error =
        block_on(fixed.all_for_identity(&prepared, ReadOpts::default(), alice())).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Query);
    assert!(error.message.contains("owner"), "{error}");
    // The new authority column exists and grants both parents; the old client
    // lacks its epoch even though an old physical column spells `owner`.
    assert_eq!(
        ids(&db, Query::from("children").inherits("project"), alice()),
        vec![row(10), row(11)]
    );
    drop(streams);
    block_on(client.close()).unwrap();
    block_on(db.close()).unwrap();
}

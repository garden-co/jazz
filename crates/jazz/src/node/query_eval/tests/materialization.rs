//! materialization query-evaluation tests.

use super::*;

#[test]
fn authoritative_replacement_provenance_is_member_specific_in_a_mixed_batch() {
    let member = |row_byte, time| {
        ResultMemberEntry::row((
            groove::Intern::from("documents".to_owned()),
            RowUuid::from_bytes([row_byte; 16]),
            TxId::new(
                crate::time::TxTime::from(time),
                NodeUuid::from_bytes([0x91; 16]),
            ),
        ))
    };
    let stale_authority_member = member(0x11, 1);
    let authority_reentry = member(0x11, 2);
    let stable_ordinary_member = member(0x22, 3);
    let ordinary_content_update = member(0x22, 4);
    let provenance = BTreeSet::from([authority_reentry.clone()]);
    let mut result_set = BTreeSet::from([
        stale_authority_member.clone(),
        stable_ordinary_member.clone(),
    ]);
    let mut payloads = BTreeMap::new();

    for added in [&authority_reentry, &ordinary_content_update] {
        replace_stale_authoritative_occurrence_member(
            &mut result_set,
            &mut payloads,
            &provenance,
            added,
            "documents",
            false,
        )
        .expect("reduce mixed authoritative and ordinary additions");
        result_set.insert(added.clone());
    }

    assert!(!result_set.contains(&stale_authority_member));
    assert!(result_set.contains(&authority_reentry));
    assert!(result_set.contains(&stable_ordinary_member));
    assert!(result_set.contains(&ordinary_content_update));
}

#[test]
fn required_cell_guard_resolves_a_later_projected_column_by_name() {
    let table = TableSchema::new(
        "items",
        [
            ColumnSchema::new("first", ColumnType::String),
            ColumnSchema::new("second", ColumnType::String),
            ColumnSchema::new("third", ColumnType::String),
        ],
    );
    let row_id = RowUuid(uuid::Uuid::from_u128(1));
    let complete = current_row_from_cells(
        &table,
        row_id,
        &BTreeMap::from([
            ("first".to_owned(), Value::String("one".to_owned())),
            ("second".to_owned(), Value::String("two".to_owned())),
            ("third".to_owned(), Value::String("three".to_owned())),
        ]),
    )
    .expect("build complete row")
    .project(&table, &["third".to_owned()])
    .expect("project later column");
    assert!(current_row_has_required_subscription_cells(
        &complete,
        &table,
        Some(&["third".to_owned()]),
    ));

    let missing = current_row_from_cells(
        &table,
        row_id,
        &BTreeMap::from([
            ("first".to_owned(), Value::String("one".to_owned())),
            ("second".to_owned(), Value::String("two".to_owned())),
        ]),
    )
    .expect("build row missing projected required cell")
    .project(&table, &["third".to_owned()])
    .expect("project missing later column");
    assert!(!current_row_has_required_subscription_cells(
        &missing,
        &table,
        Some(&["third".to_owned()]),
    ));
}

#[test]
fn unordered_array_windows_materialize_per_parent_row_id_order() {
    let windows =
        NodeState::<RocksDbStorage>::relation_snapshot_no_order_windows(&[ArraySubquery::new(
            "comments", "comments", "todo_id", "id",
        )
        .offset(1)
        .limit(2)]);
    assert_eq!(
        windows
            .get("comments")
            .map(|window| (window.offset, window.limit)),
        Some((1, Some(2)))
    );
}

#[test]
fn authoritative_reset_version_uses_non_base_partition_descriptor() {
    let (_dir, mut node, evolved_table, todo, tx_id) = evolved_todos_version();
    let table = node.table("todos").unwrap().clone();
    let row = node
        .materialize_authoritative_reset_version_row("todos", todo, tx_id, None)
        .unwrap()
        .expect("stored evolved version");
    assert_eq!(
        row.cell(&table, "title"),
        Some(Value::String("partition-title".to_owned()))
    );
    let alias = *node
        .node_aliases
        .get(&tx_id.node)
        .expect("local node alias");
    let version = node
        .query_version_by_alias("todos", todo, VersionLayer::Content, tx_id.time, alias)
        .unwrap()
        .expect("non-base partition version");
    assert_eq!(version.tx_time(), tx_id.time);
    assert_eq!(version.tx_node_alias(), alias);
    assert_eq!(
        version.cell(&evolved_table, "body").unwrap(),
        Some(Value::String("partition-body".to_owned()))
    );
}

#[test]
fn relation_edge_target_uses_non_base_partition_descriptor() {
    let (_dir, mut node, evolved_table, todo, tx_id) = evolved_todos_version();
    let table = node.table("todos").unwrap().clone();
    let alias = *node
        .node_aliases
        .get(&tx_id.node)
        .expect("local node alias");
    let row = node
        .materialize_relation_edge_target_row(
            &ReadViewSpec::default(),
            node.catalogue.current_schema_version_id,
            "todos",
            todo,
            tx_id.time,
            alias,
        )
        .unwrap();
    assert_eq!(
        row.cell(&table, "title"),
        Some(Value::String("partition-title".to_owned()))
    );
    let version = node
        .query_version_by_alias("todos", todo, VersionLayer::Content, tx_id.time, alias)
        .unwrap()
        .expect("non-base partition version");
    assert_eq!(version.tx_time(), tx_id.time);
    assert_eq!(version.tx_node_alias(), alias);
    assert_eq!(
        version.cell(&evolved_table, "body").unwrap(),
        Some(Value::String("partition-body".to_owned()))
    );
}

#[test]
fn relation_edge_target_projects_old_witness_into_read_schema() {
    let base = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text)),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xe4; 16]), base.clone());
    let todo = row(0xe5);
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", todo, 0xe6).cells(BTreeMap::from([(
                "title".to_owned(),
                Value::String("written-by-alice".to_owned()),
            )])),
        )
        .expect("commit v1 todo");

    let evolved_schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text),
        ),
    );
    let evolved_table = evolved_schema.tables[0].clone();
    let evolved = SchemaVersion::new(evolved_schema);
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(
            node.author_schema_lineage_publication(
                evolved.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved.id,
                    vec![TableLens {
                        source_table: "todos".to_owned(),
                        target_table: "todos".to_owned(),
                        ops: vec![LensOp::AddColumn {
                            column: "body".to_owned(),
                            default: Value::String("from-lens-default".to_owned()),
                        }],
                    }],
                )
                .expect("valid migration lens"),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap(),
        ),
    })
    .expect("publish v2 lens");

    let alias = *node
        .node_aliases
        .get(&tx_id.node)
        .expect("local node alias");
    let row = node
        .materialize_relation_edge_target_row(
            &ReadViewSpec::default(),
            evolved.id,
            "todos",
            todo,
            tx_id.time,
            alias,
        )
        .expect("render projected relation target");
    assert_eq!(
        row.cell(&evolved_table, "title"),
        Some(Value::String("written-by-alice".to_owned()))
    );
    assert_eq!(
        row.cell(&evolved_table, "body"),
        Some(Value::String("from-lens-default".to_owned()))
    );
}

#[test]
fn authoritative_reset_relation_target_projects_old_renamed_witness() {
    let base = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text)),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xe7; 16]), base.clone());
    let user = row(0xe8);
    let tx_id =
        node.commit_mergeable_settled(MergeableCommit::new("users", user, 0xe9).cells(
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        ))
        .expect("commit v1 user");

    let evolved_schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("people")
                .column("name", PublicColumnType::Text)
                .column("label", PublicColumnType::Text),
        ),
    );
    let people = evolved_schema.tables[0].clone();
    let evolved = SchemaVersion::new(evolved_schema);
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(
            node.author_schema_lineage_publication(
                evolved.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved.id,
                    vec![TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![
                            LensOp::RenameTable {
                                from: "users".to_owned(),
                                to: "people".to_owned(),
                            },
                            LensOp::AddColumn {
                                column: "label".to_owned(),
                                default: Value::String("migrated".to_owned()),
                            },
                        ],
                    }],
                )
                .expect("valid migration lens"),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap(),
        ),
    })
    .expect("publish people lens");

    let target_version = RowVersionRefEntry {
        tx: tx_id,
        schema_version: None,
        layer: ResultRowLayer::Content,
        batch: None,
        branch_or_prefix: None,
        row_digest: None,
    };
    let row = node
        // The wire fact names the canonical authored table.  The receiver
        // must lens it to the v2 read table before materializing it.
        .materialize_authoritative_reset_relation_edge_target(
            evolved.id,
            "users",
            user,
            &target_version,
        )
        .expect("render authority relation target")
        .expect("authority has stored target witness");
    assert_eq!(row.table(), "people");
    assert_eq!(
        row.cell(&people, "name"),
        Some(Value::String("alice".to_owned()))
    );
    assert_eq!(
        row.cell(&people, "label"),
        Some(Value::String("migrated".to_owned()))
    );

    let canonical_edge = RelationEdgeEntry {
        path: "author".to_owned(),
        // The root is already expressed in Bob's result schema; only the
        // related witness needs the lineage translation here.
        source_table: groove::Intern::new("people".to_owned()),
        source_row: user,
        target_table: groove::Intern::new("users".to_owned()),
        target_row: user,
        kind: None,
        source_version: None,
        target_version: Some(target_version),
        depth: None,
        edge_id: None,
        branch: None,
        role: None,
        order: None,
        hole_state: None,
    };
    let read_edge = node
        .project_relation_edge_through_read_schema(&canonical_edge, evolved.id)
        .expect("project canonical edge identity for reset index");
    assert_eq!(canonical_edge.target_table.as_str(), "users");
    assert_eq!(read_edge.target_table, "people");
    assert_eq!(read_edge.target_row, user);
}

#[test]
fn authoritative_reset_relation_target_projects_two_hop_canonical_witness() {
    let v1 = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text)),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xf0; 16]), v1.clone());
    let user = row(0xf1);
    let tx_id =
        node.commit_mergeable_settled(MergeableCommit::new("users", user, 0xf2).cells(
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        ))
        .expect("commit v1 user");

    let v2 = SchemaVersion::new(public_query_eval_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("people").column("name", PublicColumnType::Text),
    )));
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(
            node.author_schema_lineage_publication(
                v2.clone(),
                MigrationLens::new(
                    v1.version_id(),
                    v2.id,
                    vec![TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![LensOp::RenameTable {
                            from: "users".to_owned(),
                            to: "people".to_owned(),
                        }],
                    }],
                )
                .expect("valid migration lens"),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap(),
        ),
    })
    .expect("publish v2 rename");

    let v3_schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("members")
                .column("display_name", PublicColumnType::Text)
                .column("origin", PublicColumnType::Text),
        ),
    );
    let members = v3_schema.tables[0].clone();
    let v3 = SchemaVersion::new(v3_schema);
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 2,
        publication: Box::new(
            node.author_schema_lineage_publication(
                v3.clone(),
                MigrationLens::new(
                    v2.id,
                    v3.id,
                    vec![TableLens {
                        source_table: "people".to_owned(),
                        target_table: "members".to_owned(),
                        ops: vec![
                            LensOp::RenameTable {
                                from: "people".to_owned(),
                                to: "members".to_owned(),
                            },
                            LensOp::RenameColumn {
                                from: "name".to_owned(),
                                to: "display_name".to_owned(),
                            },
                            LensOp::AddColumn {
                                column: "origin".to_owned(),
                                default: Value::String("v1".to_owned()),
                            },
                        ],
                    }],
                )
                .expect("valid migration lens"),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap(),
        ),
    })
    .expect("publish v3 rename");

    let target_version = RowVersionRefEntry {
        tx: tx_id,
        schema_version: Some(v1.version_id()),
        layer: ResultRowLayer::Content,
        batch: None,
        branch_or_prefix: None,
        row_digest: None,
    };
    let edge = RelationEdgeEntry {
        path: "author".to_owned(),
        source_table: groove::Intern::new("members".to_owned()),
        source_row: user,
        target_table: groove::Intern::new("users".to_owned()),
        target_row: user,
        kind: None,
        source_version: None,
        target_version: Some(target_version.clone()),
        depth: None,
        edge_id: None,
        branch: None,
        role: None,
        order: None,
        hole_state: None,
    };
    let projected_edge = node
        .project_relation_edge_through_read_schema(&edge, v3.id)
        .expect("project canonical edge through both lenses");
    assert_eq!(projected_edge.target_table, "members");

    let query = Query::from("members")
        .validate(&v3.schema)
        .expect("validate v3 members query");
    let binding = query.bind(BTreeMap::new()).expect("bind members query");
    let binding_view = BindingViewKey {
        shape_id: query.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };
    node.query
        .settled_result_sets
        .insert(binding_view, BTreeSet::new());
    node.query.settled_program_facts.insert(
        binding_view,
        BTreeSet::from([ProgramFactEntry::RelationEdge(edge.clone())]),
    );
    let settled_rows = node
        .settled_binding_view_source_rows(
            "members",
            v3.id,
            binding_view,
            SettledBindingRows::ResultMembers,
        )
        .expect("project canonical settled relation source through both lenses");
    assert_eq!(settled_rows.len(), 1);
    assert_eq!(settled_rows[0].table(), "members");

    let row = node
        .materialize_authoritative_reset_relation_edge_target(v3.id, "users", user, &target_version)
        .expect("render canonical relation witness through v3")
        .expect("stored target witness");
    assert_eq!(row.table(), "members");
    assert_eq!(
        row.cell(&members, "display_name"),
        Some(Value::String("alice".to_owned()))
    );
    assert_eq!(
        row.cell(&members, "origin"),
        Some(Value::String("v1".to_owned()))
    );
}

#[test]
fn flat_join_correlates_projected_v1_sources_across_table_rename() {
    let v1 = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("users")
                    .column("id", PublicColumnType::Uuid)
                    .column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("posts")
                    .column("id", PublicColumnType::Uuid)
                    .column("author_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text),
            ),
    );
    let v2 = SchemaVersion::new(public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("people")
                    .column("id", PublicColumnType::Uuid)
                    .column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("posts")
                    .column("id", PublicColumnType::Uuid)
                    .column("author_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text),
            ),
    ));
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xf6; 16]), v1.clone());
    let (_client_dir, mut client) =
        open_node_with_uuid(NodeUuid::from_bytes([0xf9; 16]), v1.clone());
    client
        .apply_trusted_catalogue_snapshot_settled(node.catalogue_snapshot().unwrap())
        .expect("install authority genesis identities before incremental lineage");
    let author = row(0xf7);
    let post = row(0xf8);
    let mismatched_author_row = row(0xf9);
    let mismatched_author_id = row(0xfa);
    let mismatched_post = row(0xfb);
    let author_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("users", author, 1).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(author.0)),
                ("name".to_owned(), Value::String("alice".to_owned())),
            ])),
        )
        .expect("commit v1 author");
    node.apply_fate_update(
        author_tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .expect("settle v1 author");
    let post_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("posts", post, 2).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(post.0)),
                ("author_id".to_owned(), Value::Uuid(author.0)),
                ("title".to_owned(), Value::String("hello".to_owned())),
            ])),
        )
        .expect("commit v1 post");
    node.apply_fate_update(
        post_tx,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .expect("settle v1 post");
    let mismatched_author_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("users", mismatched_author_row, 3).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(mismatched_author_id.0)),
                ("name".to_owned(), Value::String("unmatched".to_owned())),
            ])),
        )
        .expect("commit v1 author with distinct row identity");
    node.apply_fate_update(
        mismatched_author_tx,
        Fate::Accepted,
        Some(GlobalTime(3)),
        Some(DurabilityTier::Global),
    )
    .expect("settle mismatched v1 author");
    let mismatched_post_tx = node
        .commit_mergeable_settled(MergeableCommit::new("posts", mismatched_post, 4).cells(
            BTreeMap::from([
                ("id".to_owned(), Value::Uuid(mismatched_post.0)),
                ("author_id".to_owned(), Value::Uuid(mismatched_author_id.0)),
                (
                    "title".to_owned(),
                    Value::String("must not join".to_owned()),
                ),
            ]),
        ))
        .expect("commit v1 post whose foreign key is not the author row identity");
    node.apply_fate_update(
        mismatched_post_tx,
        Fate::Accepted,
        Some(GlobalTime(4)),
        Some(DurabilityTier::Global),
    )
    .expect("settle mismatched v1 post");
    let publication = node
        .author_schema_lineage_publication(
            v2.clone(),
            MigrationLens::new(
                v1.version_id(),
                v2.id,
                vec![
                    TableLens {
                        source_table: "users".to_owned(),
                        target_table: "people".to_owned(),
                        ops: vec![LensOp::RenameTable {
                            from: "users".to_owned(),
                            to: "people".to_owned(),
                        }],
                    },
                    TableLens {
                        source_table: "posts".to_owned(),
                        target_table: "posts".to_owned(),
                        ops: Vec::new(),
                    },
                ],
            )
            .expect("valid migration lens"),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(publication.clone()),
    })
    .expect("publish users to people lens");
    client
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        })
        .expect("publish users to people lens to client");
    node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: v2.id,
        },
    })
    .expect("activate v2 read schema");
    client
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: v2.id,
            },
        })
        .expect("activate v2 client read schema");

    for table in ["people", "posts"] {
        let shape = Query::from(table)
            .validate(&v2.schema)
            .expect("validate source");
        let binding = shape.bind(BTreeMap::new()).expect("bind source");
        assert_eq!(
            node.query_rows_at(&shape, &binding, GlobalTime(4))
                .expect("read projected source")
                .len(),
            2,
            "{table} must independently project its v1 row"
        );
    }
    let query = Query::from("posts").flat_join("people", "posts.author_id", "people._id");
    let shape = query.validate(&v2.schema).expect("validate v2 flat join");
    let binding = shape.bind(BTreeMap::new()).expect("bind v2 flat join");
    let rows = node
        .query_rows_at(&shape, &binding, GlobalTime(4))
        .expect("evaluate v2 flat join");
    assert_eq!(
        rows.len(),
        1,
        "flat joins must use the explicit source row identity alias, not an arbitrary stored id cell"
    );

    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        ..RegisterShapeOptions::default()
    };
    register_query_shape(&mut node, &shape, opts.clone());
    subscribe_query_binding_with_opts(&mut node, &shape, &binding, opts.clone());
    register_query_shape(&mut client, &shape, opts.clone());
    subscribe_query_binding_with_opts(&mut client, &shape, &binding, opts.clone());
    let binding_view =
        BindingViewKey::new(shape.shape_id(), binding.binding_id(), opts.read_view_key());
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };
    let mut peer = PeerState::edge_client(AuthorSubject::SYSTEM);
    let known_author = RowVersionRef::new("users", author, author_tx);
    peer.declare_known_state(
        subscription,
        Some(KnownStateDeclaration::ExactVersionSet {
            versions: vec![known_author.clone()],
        }),
    );
    let update = peer
        .rehydrate_query_with_opts(&mut node, &shape, &binding, opts.clone())
        .expect("rehydrate maintained v2 flat join");
    let missing = client
        .missing_known_state_row_version_refs(&update)
        .expect("detect omitted canonical contributor body");
    assert_eq!(missing, vec![known_author]);
    let repair = peer
        .handle_row_versions_fetch(
            &mut node,
            SyncMessage::FetchRowVersions {
                requests: missing.clone(),
            },
        )
        .expect("serve canonical contributor repair");
    let [SyncMessage::RowVersionPayloads { version_bundles }] = repair.as_slice() else {
        panic!("known contributor repair must carry row-version payloads");
    };
    client
        .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
        .expect("apply canonical contributor repair");
    client
        .apply_sync_message_settled(update.clone())
        .expect("apply maintained v2 flat join on client");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        ..
    }) = update
    else {
        panic!("flat join rehydrate must emit a view update");
    };
    assert!(reset_result_set);
    assert_eq!(
        result_member_adds.len(),
        1,
        "maintained v2 flat join must retain the projected source tuple"
    );
    let snapshot = client
        .authoritative_reset_snapshot_for_binding_view(&shape, binding_view)
        .expect("materialize applied flat-join authority snapshot")
        .expect("applied flat-join authority snapshot");
    assert_eq!(snapshot.root_count, 1);
    // The authority payload can render this tuple, but the receiver's
    // local IVM must instead rebuild it from canonical source versions.
    assert_eq!(
        client
            .query_rows_for_client(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorSubject::SYSTEM
            )
            .expect("read applied v2 flat join on client")
            .len(),
        1,
        "the client must retain the authority-maintained flat join tuple"
    );

    let updated_author_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("people", author, 5).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(author.0)),
                ("name".to_owned(), Value::String("alice".to_owned())),
            ])),
        )
        .expect("update renamed author");
    node.apply_fate_update(
        updated_author_tx,
        Fate::Accepted,
        Some(GlobalTime(5)),
        Some(DurabilityTier::Global),
    )
    .expect("settle renamed author update");
    let replacement = peer
        .query_update_for_subscription_with_opts(
            &mut node,
            subscription,
            &shape,
            &binding,
            opts.clone(),
        )
        .expect("publish flat tuple replacement")
        .expect("expected view update");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        version_carriers,
        version_bundles,
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = &replacement
    else {
        panic!("flat tuple replacement must emit a view update");
    };
    assert!(
        !reset_result_set,
        "unchanged result membership must take the non-reset rehydrate path"
    );
    assert!(
        result_member_adds.is_empty() && result_member_removes.is_empty(),
        "a no-op source version must retain the same result member"
    );
    let outgoing_contributor_adds = program_fact_adds
        .iter()
        .filter(|fact| {
            matches!(
                fact,
                ProgramFactEntry::ContributingMembers(contribution)
                    if contribution
                        .role
                        .as_deref()
                        .is_some_and(|role| role.starts_with("flat_tuple_source:"))
            )
        })
        .count();
    let outgoing_contributor_removes = program_fact_removes
        .iter()
        .filter(|fact| {
            matches!(
                fact,
                ProgramFactEntry::ContributingMembers(contribution)
                    if contribution
                        .role
                        .as_deref()
                        .is_some_and(|role| role.starts_with("flat_tuple_source:"))
            )
        })
        .count();
    assert_eq!(outgoing_contributor_adds, 1);
    assert_eq!(outgoing_contributor_removes, 1);
    let mut replacement_bundles = version_bundles.clone();
    replacement_bundles.extend(
        crate::protocol::expand_version_carriers(version_carriers)
            .expect("expand replacement contributor bundles"),
    );
    assert_eq!(
        replacement_bundles
            .iter()
            .filter(|bundle| bundle.tx.tx_id == updated_author_tx)
            .flat_map(|bundle| &bundle.versions)
            .count(),
        1,
        "the changed canonical contributor must ship exactly one body"
    );
    assert!(program_fact_removes.iter().any(|fact| {
        matches!(
            fact,
            ProgramFactEntry::ContributingMembers(contribution)
                if contribution
                    .role
                    .as_deref()
                    .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                    && contribution
                        .contributor
                        .as_real_row()
                        .and_then(RealRowMemberEntry::row_projection)
                        .is_some_and(|(table, row, tx)| table.to_string() == "users" && row == author && tx == author_tx)
        )
    }));
    assert!(program_fact_adds.iter().any(|fact| {
        matches!(
            fact,
            ProgramFactEntry::ContributingMembers(contribution)
                if contribution
                    .role
                    .as_deref()
                    .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                    && contribution
                        .contributor
                        .as_real_row()
                        .and_then(RealRowMemberEntry::row_projection)
                        .is_some_and(|(table, row, tx)| table.to_string() == "people" && row == author && tx == updated_author_tx)
        )
    }));
    client
        .apply_sync_message_settled(replacement)
        .expect("apply flat tuple replacement");
    let active_contributors = client
        .query
        .settled_program_facts
        .get(&binding_view)
        .expect("flat tuple facts remain scoped to the binding view")
        .iter()
        .filter(|fact| {
            matches!(
                fact,
                ProgramFactEntry::ContributingMembers(contribution)
                    if contribution
                        .role
                        .as_deref()
                        .is_some_and(|role| role.starts_with("flat_tuple_source:"))
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(active_contributors.len(), 1);
    assert!(matches!(
        active_contributors[0],
        ProgramFactEntry::ContributingMembers(contribution)
            if contribution
                .contributor
                .as_real_row()
                .and_then(RealRowMemberEntry::row_projection)
                .is_some_and(|(table, row, tx)| table.to_string() == "people" && row == author && tx == updated_author_tx)
    ));
    assert_eq!(
        client
            .query_rows_for_client(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorSubject::SYSTEM
            )
            .expect("read retained flat tuple after no-op source version")
            .len(),
        1
    );
}

// Schema-lineage staging, activation, crash recovery, and live registry growth.

#[test]
fn physical_identity_mapping_and_live_id_recovery_are_durable_catalogue_metadata() {
    // Physical topology is intentionally not public API, so this internal test
    // verifies the catalogue/recovery and local-allocation invariants directly.
    let schema = schema();
    let schema_version = schema.version_id();
    let (left_dir, left) = open_node_with_schema(node(0x2d), schema.clone());

    let left_mapping = left.catalogue.physical_mappings[&schema_version].clone();
    let todos = &left_mapping.tables["todos"];
    assert_ne!(todos.table_id.0, 0);
    assert_ne!(todos.columns["title"].0, 0);
    drop(left);

    let mut reopened = reopen_node_at(&left_dir, node(0x2d), schema.clone());
    assert_eq!(
        reopened.catalogue.physical_mappings[&schema_version],
        left_mapping
    );

    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    publish_schema_lineage(
        &mut reopened,
        evolved.clone(),
        MigrationLens::new(
            schema.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let next = &reopened.catalogue.physical_mappings[&evolved.id].tables["todos"];
    assert_eq!(next.table_id, todos.table_id);
    assert_eq!(next.columns["title"], todos.columns["title"]);
    assert!(next.columns["body"].0 > todos.columns["title"].0);
}

#[test]
fn non_genesis_schema_activates_only_with_its_ordered_lineage_bundle() {
    // Physical topology and the staged catalogue state are not public API, so
    // this internal test pins their atomic admission boundary directly.
    let base = schema();
    let source_id = base.version_id();
    let target = SchemaVersion::new(catalogue_evolved_schema());
    let lens = MigrationLens::new(
        source_id,
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let (dir, mut core) = open_node_with_schema(node(0x2e), base.clone());
    let publication = core.author_schema_lineage_publication(
        target.clone(),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();

    let standalone = core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchema {
        author: AuthorSubject::SYSTEM,
        schema: Box::new(target.clone()),
    });
    assert!(matches!(
        standalone,
        Err(Error::InvalidCatalogueUpdate(
            "non-genesis schema requires lineage publication"
        ))
    ));
    assert!(!core.catalogue_schemas().contains_key(&target.id));

    let ack = core
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication.clone()),
        })
        .unwrap();
    assert!(matches!(
        ack.as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            schema: Some(schema),
            lens: Some(published_lens),
            applied: true,
        })] if *schema == target.id && *published_lens == lens.id
    ));
    let source = &core.catalogue.physical_mappings[&source_id].tables["todos"];
    let activated = &core.catalogue.physical_mappings[&target.id].tables["todos"];
    assert_eq!(activated.table_id, source.table_id);
    assert_eq!(activated.columns["title"], source.columns["title"]);

    let duplicate = core
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        })
        .unwrap();
    assert!(matches!(
        duplicate.as_slice(),
        [SyncMessage::CatalogueAck(_)]
    ));
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x2e), base);
    assert!(reopened.catalogue_schemas().contains_key(&target.id));
    assert_eq!(
        reopened.catalogue.physical_mappings[&target.id].tables["todos"].table_id,
        reopened.catalogue.physical_mappings[&source_id].tables["todos"].table_id
    );
}

#[test]
fn durable_genesis_rejects_reopen_with_a_different_schema() {
    let base = schema();
    let (dir, core) = open_node_with_schema(node(0x2a), base);
    drop(core);

    let different = catalogue_evolved_schema();
    let cfs = different.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = NodeState::new(node(0x2a), different, storage).resolve();
    assert!(matches!(
        reopened,
        Err(Error::InvalidStoredValue(
            "opened schema is absent from the durable catalogue"
        ))
    ));
}

#[test]
fn pending_lineage_reserves_its_target_and_sequence() {
    let base = schema();
    let target = SchemaVersion::new(catalogue_evolved_schema());
    let lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let (_dir, mut core) = open_node_with_schema(node(0x2b), base.clone());
    let publication = core
        .author_schema_lineage_publication(
            target.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    let conflicting_lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String("different semantic default".to_owned()),
            }],
        }],
    );
    let conflict = core
        .author_schema_lineage_publication(
            target,
            conflicting_lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();

    assert!(
        core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(publication.clone()),
        })
        .unwrap()
        .is_empty()
    );
    assert!(matches!(
        core.apply_sync_message_with_ingest_context(
            SyncMessage::PublishSchemaWithLens {
                author: AuthorSubject::SYSTEM,
                catalogue_seq: 2,
                publication: Box::new(publication),
            },
            Some(CommitUnitIngestContext {
                identity: user(0x71),
                trust: CommitUnitTrust::Session,
                edge_authority: false,
            }),
        ).resolve(),
        Err(Error::UnauthorizedCatalogueUpdate)
    ));
    assert!(matches!(
        core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(conflict.clone()),
        }),
        Err(Error::InvalidCatalogueUpdate(
            "schema lineage catalogue sequence conflict"
        ))
    ));
    assert!(matches!(
        core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 3,
            publication: Box::new(conflict),
        }),
        Err(Error::InvalidCatalogueUpdate(
            "schema lineage target is already reserved"
        ))
    ));
}

#[test]
fn lineage_operations_must_exhaustively_reproduce_target_columns_before_staging() {
    let base = schema();
    let target = SchemaVersion::new(catalogue_evolved_schema());
    let incomplete_lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: Vec::new(),
        }],
    );
    let (_dir, mut core) = open_node_with_schema(node(0x29), base);
    let publication = core.author_schema_lineage_publication(
        target.clone(),
        incomplete_lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();
    let next_table = core.catalogue.next_physical_table_id;
    let next_column = core.catalogue.next_physical_column_id;

    assert!(matches!(
        core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        }),
        Err(Error::InvalidCatalogueUpdate(
            "lens operations do not reproduce target columns"
        ))
    ));
    assert!(!core.catalogue_schemas().contains_key(&target.id));
    assert!(core.catalogue.staged_lineages.is_empty());
    assert_eq!(core.catalogue.next_physical_table_id, next_table);
    assert_eq!(core.catalogue.next_physical_column_id, next_column);

    let correction = core.author_schema_lineage_publication(
        target.clone(),
        MigrationLens::new(
            core.catalogue.schema.version_id(),
            target.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(correction),
    })
    .unwrap();
    assert!(core.catalogue_schemas().contains_key(&target.id));
}

#[test]
fn schema_lineage_gaps_and_inactive_sources_park_durably_then_drain_in_order() {
    let v1 = schema();
    let v2 = SchemaVersion::new(catalogue_evolved_schema());
    let v3 = SchemaVersion::new(catalogue_v3_schema());
    let lens_12 = MigrationLens::new(
        v1.version_id(),
        v2.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let lens_23 = MigrationLens::new(
        v2.id,
        v3.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "archived".to_owned(),
                default: Value::Bool(false),
            }],
        }],
    );
    let (dir, mut core) = open_node_with_schema(node(0x2c), v1.clone());
    let publication_1 = core.author_schema_lineage_publication(
        v2.clone(),
        lens_12,
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();
    let publication_2 = SchemaLineagePublication::author_from_prior(
        &publication_1.schema.schema,
        &publication_1.physical_identities,
        v3.clone(),
        lens_23,
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();

    let parked = core
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(publication_2),
        })
        .unwrap();
    assert!(parked.is_empty());
    assert!(!core.catalogue_schemas().contains_key(&v2.id));
    assert!(!core.catalogue_schemas().contains_key(&v3.id));
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x2c), v1);
    assert!(!reopened.catalogue_schemas().contains_key(&v3.id));
    let drained = reopened
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication_1),
        })
        .unwrap();
    assert_eq!(
        drained
            .iter()
            .filter(|message| matches!(message, SyncMessage::CatalogueAck(_)))
            .count(),
        2
    );
    assert!(reopened.catalogue_schemas().contains_key(&v2.id));
    assert!(reopened.catalogue_schemas().contains_key(&v3.id));
    // This is the ordering evidence enum lowering consumes: the out-of-order
    // seq-2 envelope did not allocate an alias when it was merely parked.
    // After seq-1 activates, the drained child receives the later alias, so a
    // physical enum registry can append rather than reinterpret an existing
    // local tag according to network receipt order.
    assert_eq!(
        reopened.catalogue.schema_version_aliases[&v2.id],
        SchemaVersionAlias(2)
    );
    assert_eq!(
        reopened.catalogue.schema_version_aliases[&v3.id],
        SchemaVersionAlias(3)
    );
}

#[test]
fn malformed_unknown_source_bundle_is_quarantined_when_parent_arrives() {
    let v1 = schema();
    let v2 = SchemaVersion::new(catalogue_evolved_schema());
    let v3 = SchemaVersion::new(catalogue_v3_schema());
    let (dir, mut core) = open_node_with_schema(node(0x2d), v1.clone());
    let parent = core.author_schema_lineage_publication(
        v2.clone(),
        MigrationLens::new(
            v1.version_id(),
            v2.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();
    let malformed = SchemaLineagePublication::author_from_prior(
        &parent.schema.schema,
        &parent.physical_identities,
        v3.clone(),
        MigrationLens::new(
            v2.id,
            v3.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: Vec::new(),
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();
    let valid = SchemaLineagePublication::author_from_prior(
        &parent.schema.schema,
        &parent.physical_identities,
        v3.clone(),
        MigrationLens::new(
            v2.id,
            v3.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "archived".to_owned(),
                    default: Value::Bool(false),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();

    assert!(
        core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(malformed),
        })
        .unwrap()
        .is_empty()
    );
    core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(parent),
    })
    .unwrap();
    assert!(core.catalogue_schemas().contains_key(&v2.id));
    assert!(!core.catalogue_schemas().contains_key(&v3.id));
    assert!(!core.catalogue.pending_lineages.contains_key(&2));
    drop(core);

    let mut core = reopen_node_at(&dir, node(0x2d), v1);
    assert!(!core.catalogue.pending_lineages.contains_key(&2));

    core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 2,
        publication: Box::new(valid),
    })
    .unwrap();
    assert!(core.catalogue_schemas().contains_key(&v3.id));
}

#[test]
fn staged_lineage_resumes_after_each_activation_crash_boundary() {
    for (byte, failpoint) in [
        (0x25, CatalogueActivationFailpoint::AfterStaged),
        (0x26, CatalogueActivationFailpoint::AfterRegistration),
    ] {
        let base = schema();
        let target = SchemaVersion::new(catalogue_evolved_schema());
        let lens = MigrationLens::new(
            base.version_id(),
            target.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        );
        let (dir, mut core) = open_node_with_schema(node(byte), base.clone());
        let publication = core.author_schema_lineage_publication(
            target.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        ).unwrap();
        core.set_catalogue_activation_failpoint(failpoint);

        assert!(matches!(
            core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
                author: AuthorSubject::SYSTEM,
                catalogue_seq: 1,
                publication: Box::new(publication),
            }),
            Err(Error::CatalogueActivationFailed)
        ));
        assert!(!core.catalogue_schemas().contains_key(&target.id));
        assert!(matches!(
            core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchema {
                author: AuthorSubject::SYSTEM,
                schema: Box::new(target.clone()),
            }),
            Err(Error::CatalogueActivationFailed)
        ));
        drop(core);

        let reopened = reopen_node_at(&dir, node(byte), base);
        assert!(reopened.catalogue_schemas().contains_key(&target.id));
        assert_eq!(reopened.active_catalogue_seq(), 1);
    }
}

#[test]
fn publishing_lens_reconciles_target_table_and_column_identities_durably() {
    // Physical topology is intentionally not public API, so this internal test
    // verifies identity reconciliation and live-mapping recovery directly.
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let source_id = base.version_id();
    let target = SchemaVersion::new(evolved);
    let (dir, mut core) = open_node_with_schema(node(0x2f), base.clone());
    let source_table = core.catalogue.physical_mappings[&source_id].tables["todos"].clone();

    let lens = MigrationLens::new(
        source_id,
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    publish_schema_lineage(
        &mut core,
        target.clone(),
        lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();

    let reconciled = core.catalogue.physical_mappings[&target.id].tables["todos"].clone();
    assert_eq!(reconciled.table_id, source_table.table_id);
    assert_eq!(reconciled.columns["title"], source_table.columns["title"]);
    assert_ne!(reconciled.columns["body"], source_table.columns["title"]);
    let mapping = core.catalogue.physical_mappings[&target.id].clone();
    let max_live_table_id = mapping
        .tables
        .values()
        .map(|table| table.table_id.0)
        .max()
        .unwrap();
    let max_live_column_id = mapping
        .tables
        .values()
        .flat_map(|table| table.columns.values())
        .map(|column| column.0)
        .max()
        .unwrap();
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x2f), base);
    assert_eq!(reopened.catalogue.physical_mappings[&target.id], mapping);
    let later = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("notes").column("text", PublicColumnType::Text),
        ),
    ));
    publish_schema_lineage(
        &mut reopened,
        later.clone(),
        MigrationLens::new(target.id, later.id, vec![]),
        ["notes"],
        ["todos"],
    )
    .unwrap();
    let later_table = &reopened.catalogue.physical_mappings[&later.id].tables["notes"];
    assert!(later_table.table_id.0 > max_live_table_id);
    assert!(later_table.columns["text"].0 > max_live_column_id);
}

#[test]
fn active_history_projection_accepts_a_new_schema_variant_without_rebuild() {
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let (dir, mut core) = open_node_with_schema(node(0x2e), base.clone());
    let subscription = core.subscribe_history("todos").unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x44), 900).cells(title_cells("old-title")),
    )
    .unwrap();
    assert_eq!(
        subscription
            .recv()
            .unwrap()
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .count(),
        1,
    );
    let runtime = core.groove_runtime_token();

    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    })
    .unwrap();
    assert_ne!(
        core.groove_runtime_token(),
        runtime,
        "widening a shared current-row descriptor invalidates prepared graphs"
    );

    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x45), 1_000).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("new-title".to_owned())),
            ("body".to_owned(), Value::String("new-body".to_owned())),
        ])),
    )
    .unwrap();

    let deltas = subscription.recv().unwrap();
    let rows = deltas
        .iter()
        .filter(|(_, weight)| *weight > 0)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].0.descriptor(),
        base.tables[0].history_storage_table().record_schema(),
    );
    assert_eq!(
        core.version_storage_sources_for_layer("todos", VersionLayer::Content)
            .unwrap()
            .len(),
        1,
    );

    drop(subscription);
    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0x2e), base);
    let versions = reopened.query_table_versions("todos").unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(
        versions
            .iter()
            .map(VersionRow::schema_version_alias)
            .collect::<BTreeSet<_>>()
            .len(),
        2,
    );
}

/// A cold runtime may rebuild its projection registry after pruning a dropped
/// raw receiver, and the rebuilt registry must admit the new enum variant.
///
/// Actors: alice writes and drops a history stream; mallory then publishes an
/// append-only enum case through the trusted catalogue lane.
///
/// ```text
/// alice --drop history--> core --prune/rebuild--> v2 registry
/// mallory --publish v2--------------------------> new v2 write
/// ```
#[test]
fn dropped_history_receiver_allows_cold_registry_rebuild() {
    let schema = enum_projection_schema;
    let base = schema(&["open"]);
    let evolved = SchemaVersion::new(schema(&["open", "archived"]));
    let evolved_id = evolved.id;
    let (_dir, mut core) = open_node_with_schema(node(0x6c), base.clone());
    core.commit_mergeable_settled(
        MergeableCommit::new("items", row(0x6c), 900).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("before".to_owned())),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    let subscription = core.subscribe_history("items").unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
    drop(subscription);
    assert_eq!(core.runtime_stats_for_test().active_subscriptions, 1);
    let runtime = core.groove_runtime_token();

    core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(core.author_schema_lineage_publication(
            evolved,
            MigrationLens::new(
                base.version_id(),
                evolved_id,
                vec![TableLens {
                    source_table: "items".to_owned(),
                    target_table: "items".to_owned(),
                    ops: vec![LensOp::TransformColumn {
                        column: "status".to_owned(),
                        transform: "jazz.identity".to_owned(),
                    }],
                }],
            ),
            Vec::<String>::new(),
            Vec::<String>::new(),
        ).unwrap()),
    })
    .unwrap();

    assert_eq!(core.runtime_stats_for_test().active_subscriptions, 0);
    assert_ne!(core.groove_runtime_token(), runtime);
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_id,
        },
    })
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("items", row(0x6d), 1_000).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("after".to_owned())),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    assert_eq!(core.query_table_versions("items").unwrap().len(), 2);
}

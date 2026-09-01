// Scalar, payload, and nested enum projection across schema evolution.

fn independent_enum_schema(a: &[&str], b: &[&str]) -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("items")
            .column("a", public_scalar_enum("a", a))
            .column("b", public_scalar_enum("b", b)),
    ))
}

fn enum_projection_schema(statuses: &[&str]) -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("items")
            .column("title", PublicColumnType::Text)
            .column("status", public_scalar_enum("status", statuses)),
    ))
}

fn public_scalar_enum(name: &str, variants: &[&str]) -> PublicColumnType {
    PublicColumnType::ScalarEnum {
        name: name.to_owned(),
        variants: variants
            .iter()
            .map(|variant| (*variant).to_owned())
            .collect(),
    }
}

fn public_payload_enum(name: &str, cases: &[&str], field: &str) -> PublicColumnType {
    PublicColumnType::CatalogueEnumPayload {
        name: name.to_owned(),
        cases: cases
            .iter()
            .map(|case| PublicEnumCaseDescriptor {
                name: (*case).to_owned(),
                fields: vec![PublicColumnDescriptor::new(field, PublicColumnType::Text)],
            })
            .collect(),
    }
}

fn enum_identity_lens(source: SchemaVersionId, target: SchemaVersionId) -> MigrationLens {
    MigrationLens::new(
        source,
        target,
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![LensOp::TransformColumn {
                column: "status".to_owned(),
                transform: "jazz.identity".to_owned(),
            }],
        }],
    ).expect("valid migration lens")
}

/// Earlier catalogue introductions retain their physical scalar-enum tags
/// when a later sibling introduces a shallower authored ordinal.
///
/// alice publishes `base -> A -> A2`; bob later publishes `base -> B`.
///
/// ```text
/// base ──► A (+ a) ──► A2 (+ a2)
///   └────────────────► B (+ b)
/// ```
///
/// The node must append `b` after `a2`, activate the registry, and recover
/// the same mapping after reopen. (Sorting observes the local read schema;
/// the shared physical ordering is asserted below.)
#[test]
fn scalar_enum_later_sibling_appends_without_retagging_deeper_cases() {
    let base = enum_projection_schema(&["base"]);
    let a = SchemaVersion::new(enum_projection_schema(&["base", "a"]));
    let a2 = SchemaVersion::new(enum_projection_schema(&["base", "a", "a2"]));
    let b = SchemaVersion::new(enum_projection_schema(&["base", "b"]));
    let (dir, mut core) = open_node_with_schema(node(0x78), base.clone());

    publish_schema_lineage(
        &mut core,
        a.clone(),
        enum_identity_lens(base.version_id(), a.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    publish_schema_lineage(
        &mut core,
        a2.clone(),
        enum_identity_lens(a.id, a2.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: a2.id,
        },
    })
    .unwrap();

    let base_row = row(0x78);
    let a_row = row(0x79);
    let a2_row = row(0x7a);
    for (row_uuid, title, status, tx_time) in [
        (a2_row, "a2", 2, 1),
        (base_row, "base", 0, 2),
        (a_row, "a", 1, 3),
    ] {
        core.commit_mergeable_settled(
            MergeableCommit::new("items", row_uuid, tx_time).cells(BTreeMap::from([
                ("title".to_owned(), v(title)),
                ("status".to_owned(), Value::EnumTag(status)),
            ])),
        )
        .unwrap();
    }
    // `b` has local ordinal 1, but it is catalogue-later than A2's ordinal
    // 2. Ordinal-first physical ordering would attempt to insert it before
    // A2, which Groove correctly rejects as a retagging evolution.
    publish_schema_lineage(
        &mut core,
        b.clone(),
        enum_identity_lens(base.version_id(), b.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let b_mapping = &core.catalogue.physical_mappings[&b.id].tables["items"];
    let physical_cases = core
        .physical_scalar_enum_cases(b_mapping.table_id, b_mapping.columns["status"])
        .unwrap();
    let case = |schema, ordinal| GlobalScalarEnumCaseId {
        id: core.catalogue.physical_mappings[&schema].identities.tables["items"].columns
            ["status"]
            .enum_variants["root"][ordinal],
        introducing_schema: schema,
        introducing_ordinal: ordinal as u8,
    };
    let expected = vec![
        case(base.version_id(), 0),
        case(a.id, 1),
        case(a2.id, 2),
        case(b.id, 1),
    ];
    assert_eq!(physical_cases, expected);
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: b.id,
        },
    })
    .unwrap();
    let b_row = row(0x7b);
    core.commit_mergeable_settled(
        MergeableCommit::new("items", b_row, 4).cells(BTreeMap::from([
            ("title".to_owned(), v("b")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x78), base.clone());
    let titles = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        reopened
            .query_rows(
                &titles,
                &titles.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Local,
            )
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([base_row, a_row, a2_row, b_row]),
    );
}

fn payload_enum_projection_schema(cases: &[&str]) -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("items")
            .column("title", PublicColumnType::Text)
            .column("status", public_payload_enum("status", cases, "note")),
    ))
}

fn nested_scalar_enum_projection_schema(statuses: &[&str]) -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("items")
            .column("title", PublicColumnType::Text)
            .column(
                "statuses",
                PublicColumnType::Array {
                    element: Box::new(public_scalar_enum("status", statuses)),
                },
            ),
    ))
}

fn nested_payload_enum_projection_schema(cases: &[&str]) -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("items")
            .column("title", PublicColumnType::Text)
            .column(
                "statuses",
                PublicColumnType::Array {
                    element: Box::new(public_payload_enum("status", cases, "note")),
                },
            ),
    ))
}

/// This is an internal physical-activation regression: public clients cannot
/// directly observe Groove's live descriptors, but a payload-enum append must
/// activate and survive recovery before a newer client can write its new case.
#[test]
fn direct_payload_enum_append_activates_and_recovers() {
    let base = payload_enum_projection_schema(&["draft"]);
    let evolved_schema = payload_enum_projection_schema(&["draft", "published"]);
    let evolved = SchemaVersion::new(evolved_schema.clone());
    let (dir, mut core) = open_node_with_schema(node(0x76), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ).expect("valid migration lens"),
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
    let published_payload = groove::records::RecordDescriptor::new([(
        "note",
        groove::records::ValueType::String,
    )]);
    core.commit_mergeable_settled(
        MergeableCommit::new("items", row(0x76), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("new-case")),
            (
                "status".to_owned(),
                Value::Enum(groove::records::EnumValue::create(
                    1,
                    published_payload,
                    &[v("written after activation")],
                )
                .unwrap()),
            ),
        ])),
    )
    .unwrap();
    let table_id = core.catalogue.physical_mappings[&evolved.id].tables["items"].table_id;
    let physical_history = physical_history_table_name(table_id);
    assert_eq!(core.query_table_versions("items").unwrap().len(), 1);
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x76), base);
    assert_eq!(reopened.query_table_versions("items").unwrap().len(), 1);
    let table = reopened.database.table_schema(&physical_history).unwrap();
    assert!(table.value_variant_registries.values().any(|registry| {
        matches!(
            registry,
            groove::records::VariantRegistry::Enum { cases }
                if cases.len() == 2
        )
    }));
}

/// Replacing a scalar enum rather than append-extending it starts a fresh
/// column epoch.  The old enum UUIDs must not leak into that replacement, but
/// the fresh epoch still has to activate and survive recovery normally.
#[test]
fn incompatible_scalar_enum_epoch_activates_and_recovers() {
    let base = enum_projection_schema(&["draft", "published"]);
    let evolved_schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("items")
            .column("title", PublicColumnType::Text)
            .column(
                "status_replacement",
                public_scalar_enum("status", &["archived"]),
            ),
    ));
    let evolved = SchemaVersion::new(evolved_schema);
    let (dir, mut core) = open_node_with_schema(node(0x7b), base.clone());
    let source_column = core.catalogue.physical_mappings[&base.version_id()].tables["items"]
        .columns["status"];
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![
                    LensOp::DropColumn {
                        column: "status".to_owned(),
                        backwards_default: Value::EnumTag(0),
                    },
                    LensOp::AddColumn {
                        column: "status_replacement".to_owned(),
                        default: Value::EnumTag(0),
                    },
                ],
            }],
        ).expect("valid migration lens"),
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
    let target_column = core.catalogue.physical_mappings[&evolved.id].tables["items"].columns
        ["status_replacement"];
    assert_ne!(target_column, source_column, "replacement gets a fresh epoch");
    core.commit_mergeable_settled(
        MergeableCommit::new("items", row(0x7b), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("fresh enum epoch")),
            ("status_replacement".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x7b), base);
    assert_eq!(reopened.current_write_schema().unwrap().schema, evolved.id);
    assert_eq!(reopened.query_table_versions("items").unwrap().len(), 1);
    assert_eq!(
        reopened.catalogue.physical_mappings[&evolved.id].tables["items"].columns
            ["status_replacement"],
        target_column,
        "the activated fresh epoch remains stable across reopen"
    );
}

/// A catalogue envelope that reuses a retired enum UUID is quarantined before
/// it can expose its target schema or create durable physical state.  The
/// original schema remains the complete recovery result.
#[test]
fn retired_scalar_enum_uuid_is_quarantined_without_catalogue_mutation() {
    let base = enum_projection_schema(&["draft", "published"]);
    let evolved = SchemaVersion::new(build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("items")
            .column("title", PublicColumnType::Text)
            .column(
                "status_replacement",
                public_scalar_enum("status", &["archived"]),
            ),
    )));
    let lens = MigrationLens::new(
        base.version_id(),
        evolved.id,
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![
                LensOp::DropColumn {
                    column: "status".to_owned(),
                    backwards_default: Value::EnumTag(0),
                },
                LensOp::AddColumn {
                    column: "status_replacement".to_owned(),
                    default: Value::EnumTag(0),
                },
            ],
        }],
    ).expect("valid migration lens");
    let (dir, mut core) = open_node_with_schema(node(0x7c), base.clone());
    let source_variant = core.catalogue.physical_mappings[&base.version_id()].identities.tables
        ["items"]
        .columns["status"]
        .enum_variants["root"][0];
    let mut forged = core
        .author_schema_lineage_publication(
            evolved.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    forged.physical_identities.tables.get_mut("items").unwrap().columns.get_mut(
        "status_replacement",
    ).unwrap().enum_variants.get_mut("root").unwrap()[0] = source_variant;
    forged.id = forged.content_id();

    assert!(matches!(
        core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(forged),
        }),
        Err(Error::InvalidCatalogueUpdate("physical retired identity reused across lineage"))
    ));
    assert_eq!(core.active_catalogue_seq(), 0);
    assert!(!core.catalogue_schemas().contains_key(&evolved.id));
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x7c), base);
    assert_eq!(reopened.active_catalogue_seq(), 0);
    assert!(!reopened.catalogue_schemas().contains_key(&evolved.id));
}

#[test]
fn payload_enum_unknown_case_is_ignored_only_when_unselected() {
    let schema = |extra| {
        let cases = if extra {
            vec!["open", "closed"]
        } else {
            vec!["open"]
        };
        build_public_test_schema(PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .column("title", PublicColumnType::Text)
                .column("status", public_payload_enum("status", &cases, "x")),
        ))
    };
    let base = schema(false); let evolved = SchemaVersion::new(schema(true));
    let (_dir, mut core) = open_node_with_schema(node(0x76), base.clone());
    publish_schema_lineage(&mut core, evolved.clone(), MigrationLens::new(base.version_id(), evolved.id, vec![TableLens { source_table: "items".into(), target_table: "items".into(), ops: vec![LensOp::TransformColumn { column: "status".into(), transform: "jazz.identity".into() }] }]).expect("valid migration lens"), Vec::<String>::new(), Vec::<String>::new()).unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema { author: AuthorSubject::SYSTEM, pointer: CurrentWriteSchema { revision: 1, schema: evolved.id } }).unwrap();
    let payload = groove::records::RecordDescriptor::new([("x", groove::records::ValueType::String)]);
    let unknown = row(0x76);
    core.commit_mergeable_settled(MergeableCommit::new("items", unknown, 1).cells(BTreeMap::from([
        ("title".into(), v("ok")), ("status".into(), Value::Enum(groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap()))
    ]))).unwrap();
    let known = row(0x77);
    let known_payload = groove::records::RecordDescriptor::new([("x", groove::records::ValueType::String)]);
    core.commit_mergeable_settled(MergeableCommit::new("items", known, 2).cells(BTreeMap::from([
        ("title".into(), v("known")), ("status".into(), Value::Enum(groove::records::EnumValue::create(0, known_payload, &[v("open")]).unwrap()))
    ]))).unwrap();
    let title = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert!(core.query_rows(&title, &title.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local).is_ok());
    let all = Query::from("items").validate(&base).unwrap();
    assert_eq!(
        core.query_rows(&all, &all.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
}

/// Old-schema reads omit only rows whose nested enum occurrence has an
/// unrepresentable case; Alice's compatible row remains visible beside Bob's
/// later case.
///
/// ```text
/// alice ──writes open──► old reader ──► visible
/// bob   ──writes closed► old reader ──► omitted
/// ```
#[test]
fn nested_scalar_enum_unknown_case_omits_only_that_row() {
    let base = nested_scalar_enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(nested_scalar_enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x78), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "statuses".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ).expect("valid migration lens"),
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
    let unknown = row(0x78);
    let known = row(0x79);
    for (row_uuid, title, status, tx_time) in [
        (unknown, "new nested case", 1, 1),
        (known, "known nested case", 0, 2),
    ] {
        core.commit_mergeable_settled(
            MergeableCommit::new("items", row_uuid, tx_time).cells(BTreeMap::from([
                ("title".to_owned(), v(title)),
                (
                    "statuses".to_owned(),
                    Value::Array(vec![Value::EnumTag(status)]),
                ),
            ])),
        )
        .unwrap();
    }

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        core.query_rows(
            &title_only,
            &title_only.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .len(),
        2,
    );
    let all = Query::from("items").validate(&base).unwrap();
    assert_eq!(
        core.query_rows(&all, &all.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
}

/// A nested payload enum follows the same source-local compatibility rule as
/// a scalar enum: Bob's later case is omitted while Alice's older case stays
/// visible to an old-schema whole-row read.
#[test]
fn nested_payload_enum_unknown_case_omits_only_that_row() {
    let base = nested_payload_enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(nested_payload_enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7a), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "statuses".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ).expect("valid migration lens"),
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
    let payload = groove::records::RecordDescriptor::new([(
        "note",
        groove::records::ValueType::String,
    )]);
    let unknown = row(0x7a);
    let known = row(0x7b);
    for (row_uuid, title, status, note, tx_time) in [
        (unknown, "new nested case", 1, "closed", 1),
        (known, "known nested case", 0, "open", 2),
    ] {
        core.commit_mergeable_settled(
            MergeableCommit::new("items", row_uuid, tx_time).cells(BTreeMap::from([
                ("title".to_owned(), v(title)),
                (
                    "statuses".to_owned(),
                    Value::Array(vec![Value::Enum(
                        groove::records::EnumValue::create(status, payload, &[v(note)]).unwrap(),
                    )]),
                ),
            ])),
        )
        .unwrap();
    }

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        core.query_rows(
            &title_only,
            &title_only.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .len(),
        2,
    );
    let all = Query::from("items").validate(&base).unwrap();
    assert_eq!(
        core.query_rows(&all, &all.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
}

#[test]
fn maintained_old_enum_subscriptions_omit_rows_that_require_new_cases() {
    // This is an internal subscription-boundary regression. PeerState is the
    // server-side maintained subscription driver; public clients receive its
    // ViewUpdates, but cannot themselves install a catalogue lineage.
    let base = enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7b), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();

    let known = row(0x7b);
    accept_global(
        &mut core,
        MergeableCommit::new("items", known, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("known case")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    let title_binding = title_only.bind(BTreeMap::new()).unwrap();
    let mut title_peer = PeerState::new();
    let initial = title_peer
        .rehydrate_query(&mut core, &title_only, &title_binding)
        .expect("old-schema title subscription opens over known case");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { reset_result_set, result_member_adds, .. }) = initial else {
        panic!("expected initial maintained view update");
    };
    assert!(reset_result_set);
    assert_eq!(result_member_adds.len(), 1);

    // Recompiling exactly the same target must leave the maintained graph in
    // place: target registration is idempotent, not a hidden reset mechanism.
    let unchanged = title_peer
        .query_update(&mut core, &title_only, &title_binding)
        .expect("identical projection target remains registered");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        ..
    }) = unchanged else {
        panic!("expected maintained view update");
    };
    assert!(!reset_result_set, "idempotent target registration must not reset");
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());

    let unknown = row(0x7c);
    let unknown_tx = accept_global(
        &mut core,
        MergeableCommit::new("items", unknown, 2).cells(BTreeMap::from([
            ("title".to_owned(), v("new case is harmless when unselected")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    );
    let update = title_peer
        .query_update(&mut core, &title_only, &title_binding)
        .expect("unused unknown enum must not break maintained title output");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { reset_result_set, result_member_adds, .. }) = update else {
        panic!("expected maintained view update");
    };
    assert!(!reset_result_set);
    assert!(result_member_adds.iter().any(|member| {
        member.as_row().is_some_and(|(_, row_uuid, tx_id)| row_uuid == unknown && tx_id == unknown_tx)
    }));

    // A separate old-schema subscription that semantically consumes status
    // must omit the same physical row. In particular, it may not reinterpret
    // the new tag as `open`, and must never error just because a newer client
    // introduced an additive case.
    let status_required = Query::from("items").validate(&base).unwrap();
    let status_binding = status_required.bind(BTreeMap::new()).unwrap();
    let status_options = crate::protocol::RegisterShapeOptions {
        tier: DurabilityTier::Local,
        read_view: Default::default(),
        ..crate::protocol::RegisterShapeOptions::default()
    };
    let status_subscription = SubscriptionKey {
        shape_id: status_required.shape_id(),
        binding_id: status_binding.binding_id(),
        read_view: status_options.read_view_key(),
    };
    let whole_rows = core.query_rows(&status_required, &status_binding, DurabilityTier::Local)
        .expect("one-shot whole-row read omits only the unknown row");
    assert_eq!(
        whole_rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
    let mut status_peer = PeerState::new();
    let update = status_peer
        .rehydrate_query_with_opts(
            &mut core,
            &status_required,
            &status_binding,
            status_options.clone(),
        )
        .expect("required unknown enum case is a row exclusion");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_adds, .. }) = update else {
        panic!("expected initial maintained view update");
    };
    assert_eq!(result_member_adds.len(), 1);
    assert!(result_member_adds.iter().all(|member| {
        member.as_row().is_some_and(|(_, row_uuid, _)| row_uuid == known)
    }));

    // Maintained membership follows the same compatibility boundary on every
    // delta: a newer local/Ahead unknown winner retracts the older Global row,
    // then a newer known winner re-adds it without a subscription error.
    core.commit_mergeable_settled(
        MergeableCommit::new("items", known, 3).cells(BTreeMap::from([
            ("title".to_owned(), v("now incompatible")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    let update = status_peer
        .query_update_for_subscription_with_opts(
            &mut core,
            status_subscription,
            &status_required,
            &status_binding,
            status_options.clone(),
        )
        .expect("newly incompatible delta removes the row")
        .expect("expected view update");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_removes, .. }) = update else {
        panic!("expected maintained view update");
    };
    assert!(result_member_removes.iter().any(|member| {
        member.as_row().is_some_and(|(_, row_uuid, _)| row_uuid == known)
    }));
    core.commit_mergeable_settled(
        MergeableCommit::new("items", known, 4).cells(BTreeMap::from([
            ("title".to_owned(), v("compatible again")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    let update = status_peer
        .query_update_for_subscription_with_opts(
            &mut core,
            status_subscription,
            &status_required,
            &status_binding,
            status_options,
        )
        .expect("newly compatible delta re-adds the row")
        .expect("expected view update");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_adds, .. }) = update else {
        panic!("expected maintained view update");
    };
    assert!(result_member_adds.iter().any(|member| {
        member.as_row().is_some_and(|(_, row_uuid, _)| row_uuid == known)
    }));
}

#[test]
fn maintained_old_payload_enum_subscription_omits_new_case_without_aliasing() {
    let base = payload_enum_projection_schema(&["draft"]);
    let evolved = SchemaVersion::new(payload_enum_projection_schema(&["draft", "published"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7d), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();
    let payload = groove::records::RecordDescriptor::new([(
        "note",
        groove::records::ValueType::String,
    )]);
    accept_global(
        &mut core,
        MergeableCommit::new("items", row(0x7d), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("new payload case")),
            (
                "status".to_owned(),
                Value::Enum(
                    groove::records::EnumValue::create(1, payload, &[v("published")]).unwrap(),
                ),
            ),
        ])),
    );

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    let binding = title_only.bind(BTreeMap::new()).unwrap();
    let mut title_peer = PeerState::new();
    assert!(title_peer.rehydrate_query(&mut core, &title_only, &binding).is_ok());

    let required = Query::from("items").validate(&base).unwrap();
    let binding = required.bind(BTreeMap::new()).unwrap();
    let mut required_peer = PeerState::new();
    let update = required_peer
        .rehydrate_query(&mut core, &required, &binding)
        .expect("unknown payload case is a row exclusion");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_adds, .. }) = update else {
        panic!("expected initial maintained view update");
    };
    assert!(result_member_adds.is_empty());
}

#[test]
fn old_enum_schema_only_decodes_cases_required_by_the_query() {
    // This is an internal current-source boundary test.  The public query API
    // supplies the requirement closure; the old read schema must not decode a
    // physical case it neither returns nor uses semantically.
    let base = enum_projection_schema(&["open"]);
    let evolved_schema = enum_projection_schema(&["open", "closed"]);
    let evolved = SchemaVersion::new(evolved_schema.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x75), base.clone());
    // Requirement-none auxiliary sources still need a physical row shape for
    // relation closure, but their unrequested enum cell must be typed-null.
    assert!(core
        .ensure_physical_current_projection_for_enum_columns(
            base.version_id(),
            "items",
            &BTreeSet::new(),
        )
        .is_ok());
    let enum_lens = MigrationLens::new(
        base.version_id(),
        evolved.id,
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![LensOp::TransformColumn {
                column: "status".to_owned(),
                transform: "jazz.identity".to_owned(),
            }],
        }],
    ).expect("valid migration lens");
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        enum_lens,
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
    let closed = row(0x75);
    core.commit_mergeable_settled(
        MergeableCommit::new("items", closed, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("still-readable")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    let open = row(0x76);
    core.commit_mergeable_settled(
        MergeableCommit::new("items", open, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("still-compatible")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        core.query_rows(
            &title_only,
            &title_only.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (closed, BTreeMap::from([("title".to_owned(), v("still-readable"))])),
            (open, BTreeMap::from([("title".to_owned(), v("still-compatible"))])),
        ])
    );

    // Whole-row output semantically consumes `status`, so its source boundary
    // omits this incompatible row rather than inventing an old value or
    // turning an additive schema change into an old-client query failure.
    let whole_row = Query::from("items").validate(&base).unwrap();
    assert_eq!(core
        .query_rows(
            &whole_row,
            &whole_row.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>(),
        BTreeSet::from([open]));

    // The closure is semantic rather than merely output-shaped: a hidden
    // predicate or order key must force the same source-local exclusion before
    // those operators, including their pagination/aggregation consumers.
    for query in [
        Query::from("items")
            .select(["title"])
            .filter(eq(col("status"), lit(Value::EnumTag(0))))
            .validate(&base)
            .unwrap(),
        Query::from("items")
            .select(["title"])
            .order_by("status", crate::query::OrderDirection::Asc)
            .validate(&base)
            .unwrap(),
    ] {
        assert_eq!(core
            .query_rows(
                &query,
                &query.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Local,
            )
            .unwrap()
            .len(),
            1,
            "known row survives a semantic enum boundary",
        );
    }
    let grouped = Query::from("items").count().group_by("status").validate(&base).unwrap();
    assert_eq!(core
        .query_rows(&grouped, &grouped.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
        .unwrap()
        .len(),
        1,
    );
}

#[test]
fn enum_projection_requirement_closure_includes_hidden_policy_fields() {
    // A policy field is not part of the public output, but it still decides
    // whether the row exists. It therefore excludes an incompatible row
    // fail-closed rather than being treated like an unused cell.
    let schema = |statuses: &[&str]| {
        build_public_test_schema(PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .column("title", PublicColumnType::Text)
                .column("status", public_scalar_enum("status", statuses))
                .policies(PublicTablePolicies::new().with_select(public_literal_eq(
                    "status",
                    PublicValue::Text("open".to_owned()),
                ))),
        ))
    };
    let base = schema(&["open"]);
    let evolved_schema = schema(&["open", "closed"]);
    let evolved = SchemaVersion::new(evolved_schema);
    let (_dir, mut core) = open_node_with_schema(node(0x77), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();
    let item = row(0x77);
    accept_global(
        &mut core,
        MergeableCommit::new("items", item, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("globally allowed")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );
    core.commit_mergeable_settled(
        MergeableCommit::new("items", item, 2).cells(BTreeMap::from([
            ("title".to_owned(), v("new local case")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    let binding = title_only.bind(BTreeMap::new()).unwrap();
    let result = core.query_rows_for_link(
        &title_only,
        &binding,
        DurabilityTier::Local,
        user(0x77),
    );
    assert!(result.unwrap().is_empty(), "policy must hide incompatible row");

    // The same post-winner boundary applies before windowing and aggregation:
    // an unknown newer local version cannot leak the older global winner into
    // a page or count.
    let whole = Query::from("items").validate(&base).unwrap();
    assert!(core
        .query_rows(
            &whole,
            &whole.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .is_empty());
    let page = Query::from("items")
        .select(["title"])
        .order_by("status", crate::query::OrderDirection::Asc)
        .offset(0)
        .limit(1)
        .validate(&base)
        .unwrap();
    assert!(core
        .query_rows(
            &page,
            &page.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .is_empty());
    let count = Query::from("items")
        .count()
        .group_by("status")
        .validate(&base)
        .unwrap();
    assert!(core
        .query_rows(
            &count,
            &count.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .is_empty());

    core.commit_mergeable_settled(
        MergeableCommit::new("items", item, 3).cells(BTreeMap::from([
            ("title".to_owned(), v("local known again")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    assert_eq!(
        core.query_rows(
            &whole,
            &whole.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .len(),
        1,
    );
}

#[test]
fn old_enum_schema_omits_unknown_rows_from_materialized_query_sources() {
    // Contract: an old reader must receive an empty result—not an encoding
    // failure—when a selected winner uses a later enum case, even along source
    // paths that materialize projected rows before lowering.
    //
    // Actors: an evolved writer publishes `closed`; an old reader queries the
    // same physical row through history, a schema projection, and deleted-row
    // inspection.
    //
    // ```text
    // evolved writer: closed ──► old reader: omitted
    //                         ├─ historical cut
    //                         ├─ branch overlay
    //                         └─ including-deleted source
    // ```
    let base = enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_history_complete_node_with_schema(node(0x7b), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        enum_identity_lens(base.version_id(), evolved.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();

    let old_shape = Query::from("items").validate(&base).unwrap();
    let old_binding = old_shape.bind(BTreeMap::new()).unwrap();
    let global_row = row(0x7b);
    accept_global(
        &mut core,
        MergeableCommit::new("items", global_row, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("later global case")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    );

    assert!(core
        .projected_historical_current_rows("items", base.version_id(), GlobalTime(0))
        .unwrap()
        .is_empty());

    assert!(core
        .query_rows_at(&old_shape, &old_binding, GlobalTime(0))
        .unwrap()
        .is_empty());

    core.commit_mergeable_settled(
        MergeableCommit::new("items", global_row, 2).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    assert!(core
        .query_rows_including_deleted_in_authorization_mode(
            &old_shape,
            &old_binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap()
        .is_empty());
}

#[test]
fn old_enum_winner_projection_refreshes_after_later_registry_append() {
    // Contract: a raw current-winner projection installed for an old reader
    // survives later append-only enum registry growth without accepting a
    // changed projection mapping or type.
    //
    // Actors: an old reader opens while `closed` is the newest case; a latest
    // writer subsequently introduces and writes `archived`.
    //
    // ```text
    // base(open) ──► middle(+closed) ──► latest(+archived)
    // old query installs raw winner target        old query skips archived
    // ```
    let base = enum_projection_schema(&["open"]);
    let middle = SchemaVersion::new(enum_projection_schema(&["open", "closed"]));
    let latest = SchemaVersion::new(enum_projection_schema(&["open", "closed", "archived"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7d), base.clone());
    publish_schema_lineage(
        &mut core,
        middle.clone(),
        enum_identity_lens(base.version_id(), middle.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: middle.id },
    })
    .unwrap();

    let item = row(0x7d);
    accept_global(
        &mut core,
        MergeableCommit::new("items", item, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("compatible middle case")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );
    let old_shape = Query::from("items").validate(&base).unwrap();
    let old_binding = old_shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows(&old_shape, &old_binding, DurabilityTier::Local)
            .unwrap()
            .len(),
        1,
        "old read installs a current-winner projection before later growth",
    );
    publish_schema_lineage(
        &mut core,
        latest.clone(),
        enum_identity_lens(middle.id, latest.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema { revision: 2, schema: latest.id },
    })
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("items", item, 2).cells(BTreeMap::from([
            ("title".to_owned(), v("later archived case")),
            ("status".to_owned(), Value::EnumTag(2)),
        ])),
    )
    .unwrap();

    assert!(core
        .query_rows(&old_shape, &old_binding, DurabilityTier::Local)
        .unwrap()
        .is_empty());
}

#[test]
fn old_enum_index_read_uses_global_index_before_post_winner_omission() {
    // Contract: an indexed Global read retains its physical index access path
    // while the old-schema compatibility boundary still runs after the chosen
    // winner. The unknown row must be omitted, not force a full table scan.
    let schema = |statuses: &[&str]| {
        build_public_test_schema(PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .column("title", PublicColumnType::Text)
                .column("status", public_scalar_enum("status", statuses))
                .index_only(["title"]),
        ))
    };
    let base = schema(&["open"]);
    let evolved = SchemaVersion::new(schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7e), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        enum_identity_lens(base.version_id(), evolved.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();
    accept_global(
        &mut core,
        MergeableCommit::new("items", row(0x7e), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("indexed")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    );

    let query = Query::from("items")
        .filter(eq(col("title"), lit("indexed")))
        .validate(&base)
        .unwrap();
    let binding = query.bind(BTreeMap::new()).unwrap();
    core.reset_query_engine_read_metrics();
    assert!(core
        .query_rows(&query, &binding, DurabilityTier::Global)
        .unwrap()
        .is_empty());
    let metrics = core.query_engine_read_metrics();
    assert_eq!(metrics.source_index_probes, 1);
    assert_eq!(metrics.source_full_scans, 0);
}

#[test]
fn enum_projection_requirement_none_allows_unused_relation_enum() {
    let schema = |states: &[&str]| {
        build_public_test_schema(
            PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("items")
                        .column("title", PublicColumnType::Text)
                        .fk_column("state", "states"),
                )
                .table(
                    PublicTableSchemaBuilder::new("states")
                        .column("status", public_scalar_enum("status", states)),
                ),
        )
    };
    let base = schema(&["open"]);
    let evolved = SchemaVersion::new(schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x78), base.clone());
    publish_schema_lineage(&mut core, evolved.clone(), MigrationLens::new(
        base.version_id(), evolved.id, vec![
            TableLens { source_table: "items".into(), target_table: "items".into(), ops: vec![] },
            TableLens { source_table: "states".into(), target_table: "states".into(), ops: vec![LensOp::TransformColumn { column: "status".into(), transform: "jazz.identity".into() }] },
        ],
    ).expect("valid migration lens"), Vec::<String>::new(), Vec::<String>::new()).unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema { author: AuthorSubject::SYSTEM, pointer: CurrentWriteSchema { revision: 1, schema: evolved.id } }).unwrap();
    let state = row(0x79);
    core.commit_mergeable_settled(MergeableCommit::new("states", state, 1).cells(BTreeMap::from([("status".into(), Value::EnumTag(1))]))).unwrap();
    core.commit_mergeable_settled(MergeableCommit::new("items", row(0x7a), 2).cells(BTreeMap::from([
        ("title".into(), v("root remains readable")), ("state".into(), Value::Uuid(state.0)),
    ]))).unwrap();

    let root_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert!(core.query_rows(&root_only, &root_only.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local).is_ok());
    // Includes are hydrated by a separate path; this compilation path still
    // proves the implicit relation source has no accidental enum dependency.
    let included = Query::from("items").select(["title"]).include("state").validate(&base).unwrap();
    assert!(core.query_rows(&included, &included.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local).is_ok());
}

#[test]
fn independent_column_enum_registries_evolve_additively_across_reopen() {
    // Registry allocation is physical catalogue metadata, so this internal
    // test asserts both user-visible decoding and the non-Cartesian boundary.
    let base = independent_enum_schema(&["a0"], &["b0"]);
    let a_evolved = SchemaVersion::new(independent_enum_schema(&["a0", "a1"], &["b0"]));
    let b_evolved = SchemaVersion::new(independent_enum_schema(&["a0", "a1"], &["b0", "b1"]));
    let (dir, mut core) = open_node_with_schema(node(0x72), base.clone());
    core.commit_mergeable_settled(
        MergeableCommit::new("items", row(0x72), 1).cells(BTreeMap::from([
            ("a".to_owned(), Value::EnumTag(0)),
            ("b".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    let enum_lens = |source, target, column: &str| {
        MigrationLens::new(
            source,
            target,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: column.to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ).expect("valid migration lens")
    };
    publish_schema_lineage(
        &mut core,
        a_evolved.clone(),
        enum_lens(base.version_id(), a_evolved.id, "a"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: a_evolved.id,
        },
    })
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("items", row(0x73), 2).cells(BTreeMap::from([
            ("a".to_owned(), Value::EnumTag(1)),
            ("b".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    let table_id = core.catalogue.physical_mappings[&a_evolved.id].tables["items"].table_id;
    let enum_registry_ids = core.catalogue.physical_mappings[&a_evolved.id].tables["items"]
        .columns
        .values()
        .map(|column| {
            groove::records::variant_registry_id_for_path(&format!(
                "physical-column/{}/nullable",
                column.0
            ))
        })
        .collect::<BTreeSet<_>>();
    let physical_name = physical_history_table_name(table_id);
    let after_a = core.database.table_schema(&physical_name).unwrap();
    let after_a_sizes = after_a
        .value_variant_registries
        .iter()
        .filter(|(id, _)| enum_registry_ids.contains(id))
        .map(|(_, registry)| match registry {
            groove::records::VariantRegistry::EnumTag { variants } => variants.len(),
            groove::records::VariantRegistry::Enum { cases } => cases.len(),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(after_a_sizes, BTreeSet::from([1, 2]));
    publish_schema_lineage(
        &mut core,
        b_evolved.clone(),
        enum_lens(a_evolved.id, b_evolved.id, "b"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: b_evolved.id,
        },
    })
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("items", row(0x74), 3).cells(BTreeMap::from([
            ("a".to_owned(), Value::EnumTag(1)),
            ("b".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    assert_eq!(core.query_table_versions("items").unwrap().len(), 3);

    let base_mapping = &core.catalogue.physical_mappings[&base.version_id()].tables["items"];
    let a_mapping = &core.catalogue.physical_mappings[&a_evolved.id].tables["items"];
    let b_mapping = &core.catalogue.physical_mappings[&b_evolved.id].tables["items"];
    assert_eq!(base_mapping.columns, a_mapping.columns);
    assert_eq!(a_mapping.columns, b_mapping.columns);
    assert_eq!(base_mapping.table_id, b_mapping.table_id);

    let table = core.database.table_schema(&physical_name).unwrap();
    let user_registries = table
        .value_variant_registries
        .iter()
        .filter(|(id, _)| enum_registry_ids.contains(id))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(user_registries.len(), 2);
    assert_eq!(table.variants.len(), 3);
    let stable_tags = table
        .variants
        .iter()
        .map(|variant| variant.tag)
        .collect::<Vec<_>>();
    assert_eq!(
        user_registries
            .values()
            .map(|registry| match registry {
                groove::records::VariantRegistry::EnumTag { variants } => variants.len(),
                groove::records::VariantRegistry::Enum { .. } => 0,
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([2])
    );
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x72), base);
    let table = reopened.database.table_schema(&physical_name).unwrap();
    assert_eq!(table.variants.len(), 3);
    assert_eq!(
        table
            .variants
            .iter()
            .map(|variant| variant.tag)
            .collect::<Vec<_>>(),
        stable_tags
    );
    assert_eq!(
        table
            .value_variant_registries
            .keys()
            .filter(|id| enum_registry_ids.contains(id))
            .count(),
        2
    );
    assert_eq!(reopened.query_table_versions("items").unwrap().len(), 3);
}

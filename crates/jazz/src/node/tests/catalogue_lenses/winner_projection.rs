// Current-winner projection across rename, copy, durability, and enum boundaries.

#[test]
fn current_winner_projects_rename_copy_chains_across_durability_tiers() {
    let base = schema();
    let evolved = SchemaVersion::new(evolved_todos_name_body_schema());
    let (_dir, mut core) = open_node_with_schema(node(0x5c), base.clone());
    let old_row = row(0x5c);
    accept_global(
        &mut core,
        MergeableCommit::new("todos", old_row, 1).cells(title_cells("old title")),
    );
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::CopyColumn {
                        from: "name".to_owned(),
                        to: "body".to_owned(),
                    },
                ],
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
    let new_row = row(0x5d);
    accept_global(
        &mut core,
        MergeableCommit::new("todos", new_row, 2).cells(BTreeMap::from([
            ("name".to_owned(), v("new name")),
            ("body".to_owned(), v("new body")),
        ])),
    );

    let shape = Query::from("todos").validate(&evolved.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = BTreeMap::from([
        (
            old_row,
            BTreeMap::from([
                ("name".to_owned(), v("old title")),
                ("body".to_owned(), v("old title")),
            ]),
        ),
        (
            new_row,
            BTreeMap::from([
                ("name".to_owned(), v("new name")),
                ("body".to_owned(), v("new body")),
            ]),
        ),
    ]);
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        assert_eq!(
            core.query_rows(&shape, &binding, tier)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            expected,
            "Rename/Copy paths preserve old physical fields in {tier:?} reads",
        );
    }
}

fn assert_current_winner_copied_enum_remap(
    marker: u8,
    source_type: PublicColumnType,
    copied_type: PublicColumnType,
    latest_type: PublicColumnType,
    old_value: Value,
    unknown_value: Value,
) {
    let base = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items").column("source", source_type),
        ),
    );
    let copied = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .column("status", copied_type.clone())
                .column("status_copy", copied_type.clone()),
        ),
    ));
    let latest = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .column("status", latest_type)
                .column("status_copy", copied_type),
        ),
    ));
    let (_dir, mut core) = open_node_with_schema(node(marker), base.clone());
    let old_row = row(marker);
    accept_global(
        &mut core,
        MergeableCommit::new("items", old_row, 1)
            .cells(BTreeMap::from([("source".to_owned(), old_value.clone())])),
    );
    publish_schema_lineage(
        &mut core,
        copied.clone(),
        MigrationLens::new(
            base.version_id(),
            copied.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "source".to_owned(),
                        to: "status".to_owned(),
                    },
                    LensOp::CopyColumn {
                        from: "status".to_owned(),
                        to: "status_copy".to_owned(),
                    },
                ],
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
            schema: copied.id,
        },
    })
    .unwrap();
    let shape = Query::from("items").validate(&copied.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = BTreeMap::from([(
        old_row,
        BTreeMap::from([
            ("status".to_owned(), old_value.clone()),
            ("status_copy".to_owned(), old_value.clone()),
        ]),
    )]);
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        assert_eq!(
            core.query_rows(&shape, &binding, tier)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            expected,
            "copied enum row remaps through distinct registries in {tier:?}",
        );
    }
    publish_schema_lineage(
        &mut core,
        latest.clone(),
        MigrationLens::new(
            copied.id,
            latest.id,
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
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: latest.id,
        },
    })
    .unwrap();
    accept_global(
        &mut core,
        MergeableCommit::new("items", old_row, 2).cells(BTreeMap::from([
            ("status".to_owned(), unknown_value),
            ("status_copy".to_owned(), old_value),
        ])),
    );
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        assert_eq!(
            core.query_rows(&shape, &binding, tier)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::new(),
            "the later unknown winner omits the same row after arg-max in {tier:?}",
        );
    }
}

#[test]
fn current_winner_remaps_copied_scalar_enums_and_omits_later_winners() {
    assert_current_winner_copied_enum_remap(
        0x5e,
        public_scalar_enum("status", &["open", "selected"]),
        public_scalar_enum("status", &["open", "selected"]),
        public_scalar_enum("status", &["open", "selected", "closed"]),
        Value::EnumTag(1),
        Value::EnumTag(2),
    );
}

#[test]
fn current_winner_remaps_copied_payload_enums_and_omits_later_winners() {
    let payload = groove::records::RecordDescriptor::new([("note", ColumnType::String)]);
    assert_current_winner_copied_enum_remap(
        0x61,
        public_payload_enum("status", &["open"], "note"),
        public_payload_enum("status", &["open"], "note"),
        public_payload_enum("status", &["open", "closed"], "note"),
        Value::Enum(groove::records::EnumValue::create(0, payload.clone(), &[v("open")]).unwrap()),
        Value::Enum(groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap()),
    );
}

#[test]
fn current_winner_remaps_copied_nested_scalar_enums_and_omits_later_winners() {
    let scalar_array = |variants: &[&str]| PublicColumnType::Array {
        element: Box::new(public_scalar_enum("status", variants)),
    };
    assert_current_winner_copied_enum_remap(
        0x63,
        scalar_array(&["open"]),
        scalar_array(&["open"]),
        scalar_array(&["open", "closed"]),
        Value::Array(vec![Value::EnumTag(0)]),
        Value::Array(vec![Value::EnumTag(1)]),
    );
}

#[test]
fn current_winner_remaps_copied_nested_payload_enums_and_omits_later_winners() {
    let payload_array = |cases: &[&str]| PublicColumnType::Array {
        element: Box::new(public_payload_enum("status", cases, "note")),
    };
    let payload = groove::records::RecordDescriptor::new([("note", ColumnType::String)]);
    assert_current_winner_copied_enum_remap(
        0x65,
        payload_array(&["open"]),
        payload_array(&["open"]),
        payload_array(&["open", "closed"]),
        Value::Array(vec![Value::Enum(
            groove::records::EnumValue::create(0, payload.clone(), &[v("open")]).unwrap(),
        )]),
        Value::Array(vec![Value::Enum(
            groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap(),
        )]),
    );
}

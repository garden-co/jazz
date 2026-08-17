// Current-winner projection across rename, copy, durability, and enum boundaries.

#[test]
fn current_winner_projects_rename_copy_chains_across_durability_tiers() {
    let base = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let evolved = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]));
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
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
    source_type: ColumnType,
    copied_type: ColumnType,
    latest_type: ColumnType,
    old_value: Value,
    unknown_value: Value,
) {
    let base = JazzSchema::new([TableSchema::new(
        "items",
        [ColumnSchema::new("source", source_type)],
    )]);
    let copied = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("status", copied_type.clone()),
            ColumnSchema::new("status_copy", copied_type),
        ],
    )]));
    let latest = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("status", latest_type),
            ColumnSchema::new("status_copy", copied.schema.tables[0].columns[1].column_type.clone()),
        ],
    )]));
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
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
    let base = enum_projection_schema(&["open", "selected"]);
    let latest = enum_projection_schema(&["open", "selected", "closed"]);
    assert_current_winner_copied_enum_remap(
        0x5e,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::EnumTag(1),
        Value::EnumTag(2),
    );
}

#[test]
fn current_winner_remaps_copied_payload_enums_and_omits_later_winners() {
    let base = payload_enum_projection_schema(&["open"]);
    let latest = payload_enum_projection_schema(&["open", "closed"]);
    let payload = groove::records::RecordDescriptor::new([("note", ColumnType::String)]);
    assert_current_winner_copied_enum_remap(
        0x61,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::Enum(groove::records::EnumValue::create(0, payload.clone(), &[v("open")]).unwrap()),
        Value::Enum(groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap()),
    );
}

#[test]
fn current_winner_remaps_copied_nested_scalar_enums_and_omits_later_winners() {
    let base = nested_scalar_enum_projection_schema(&["open"]);
    let latest = nested_scalar_enum_projection_schema(&["open", "closed"]);
    assert_current_winner_copied_enum_remap(
        0x63,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::Array(vec![Value::EnumTag(0)]),
        Value::Array(vec![Value::EnumTag(1)]),
    );
}

#[test]
fn current_winner_remaps_copied_nested_payload_enums_and_omits_later_winners() {
    let base = nested_payload_enum_projection_schema(&["open"]);
    let latest = nested_payload_enum_projection_schema(&["open", "closed"]);
    let payload = groove::records::RecordDescriptor::new([("note", ColumnType::String)]);
    assert_current_winner_copied_enum_remap(
        0x65,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::Array(vec![Value::Enum(
            groove::records::EnumValue::create(0, payload.clone(), &[v("open")]).unwrap(),
        )]),
        Value::Array(vec![Value::Enum(
            groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap(),
        )]),
    );
}


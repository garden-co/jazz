#[cfg(test)]
mod variant_case_tests {
    use super::*;

    fn schema(byte: u8) -> SchemaVersionId {
        SchemaVersionId(uuid::Uuid::from_bytes([byte; 16]))
    }

    fn case(schema: SchemaVersionId, ordinal: u8) -> GlobalScalarEnumCaseId {
        GlobalScalarEnumCaseId {
            introducing_schema: schema,
            introducing_ordinal: ordinal,
        }
    }

    fn mapping(table_id: u64, columns: &[(&str, u64)]) -> SchemaPhysicalMapping {
        SchemaPhysicalMapping {
            tables: BTreeMap::from([(
                "entries".to_owned(),
                TablePhysicalMapping {
                    table_id: PhysicalTableId(table_id),
                    columns: columns
                        .iter()
                        .map(|(name, id)| (name.to_string(), PhysicalColumnId(*id)))
                        .collect(),
                    variant_cases: Vec::new(),
                    scalar_enum_cases: BTreeMap::new(),
                    payload_enum_cases: BTreeMap::new(),
                    nested_scalar_enum_cases: BTreeMap::new(),
                    nested_payload_enum_cases: BTreeMap::new(),
                },
            )]),
        }
    }

    fn fields(edited: bool) -> BTreeSet<String> {
        let mut fields = BTreeSet::from(["id".to_owned(), "body".to_owned()]);
        if edited {
            fields.insert("edited".to_owned());
        }
        fields
    }

    #[test]
    fn schema_layout_cases_allocate_durably_without_collisions() {
        let v1 = schema(1);
        let v2 = schema(2);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1)), (v2, SchemaVersionAlias(2))]);
        let mut mappings =
            BTreeMap::from([(v1, mapping(7, &[("id", 1), ("body", 2), ("url", 3)]))]);

        let first =
            allocate_physical_variant_cases(&mut mappings, &aliases, v1, "entries", fields(false))
                .unwrap();
        mappings.insert(
            v2,
            mapping(7, &[("id", 1), ("body", 2), ("url", 3), ("edited", 4)]),
        );
        let second =
            allocate_physical_variant_cases(&mut mappings, &aliases, v2, "entries", fields(true))
                .unwrap();
        assert_eq!(first.iter().map(|case| case.tag).collect::<Vec<_>>(), [1]);
        assert_eq!(second.iter().map(|case| case.tag).collect::<Vec<_>>(), [2]);
        validate_physical_variant_cases(&mappings, &aliases).unwrap();

        // The mapping is the payload durably written in jazz_schema_versions;
        // a JSON round trip models close/reopen of the catalogue row.
        let encoded = serde_json::to_vec(&mappings).unwrap();
        let reopened: BTreeMap<SchemaVersionId, SchemaPhysicalMapping> =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(reopened, mappings);
        validate_physical_variant_cases(&reopened, &aliases).unwrap();
    }

    #[test]
    fn reopen_validation_rejects_a_cross_layout_tag_collision() {
        let v1 = schema(1);
        let v2 = schema(2);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1)), (v2, SchemaVersionAlias(2))]);
        let mut first = mapping(7, &[("id", 1)]);
        first.tables.get_mut("entries").unwrap().variant_cases = vec![PhysicalVariantCase {
            tag: 9,
            fields: BTreeSet::from(["id".to_owned()]),
        }];
        let mut second = mapping(7, &[("id", 1)]);
        second.tables.get_mut("entries").unwrap().variant_cases = vec![PhysicalVariantCase {
            tag: 9,
            fields: BTreeSet::from(["id".to_owned()]),
        }];
        let mappings = BTreeMap::from([(v1, first), (v2, second)]);
        assert!(matches!(
            validate_physical_variant_cases(&mappings, &aliases),
            Err(Error::InvalidStoredValue(
                "physical table variant tag collision"
            ))
        ));
    }

    #[test]
    fn reopen_validation_rejects_duplicate_physical_column_ids() {
        let v1 = schema(1);
        let aliases = BTreeMap::from([(v1, SchemaVersionAlias(1))]);
        let mappings = BTreeMap::from([(v1, mapping(7, &[("id", 1), ("body", 1)]))]);

        assert!(matches!(
            validate_physical_variant_cases(&mappings, &aliases),
            Err(Error::InvalidStoredValue(
                "physical table maps multiple columns to one id"
            ))
        ));
    }

    #[test]
    fn nested_enum_epoch_accepts_only_append_only_case_growth() {
        let value_type = |variants: &[&str]| {
            records::ValueType::EnumTag(
                records::ScalarEnumSchema::new("state", variants.iter().copied()).unwrap(),
            )
        };
        let old = value_type(&["new", "done"]);
        assert!(physical_value_epoch_is_compatible(
            &old,
            &value_type(&["new", "done", "archived"]),
        ));
        assert!(!physical_value_epoch_is_compatible(
            &old,
            &value_type(&["done", "new"]),
        ));
        assert!(!physical_value_epoch_is_compatible(
            &old,
            &value_type(&["new"]),
        ));
    }

    #[test]
    fn later_sibling_with_a_shallower_ordinal_appends_after_deeper_introduction() {
        // This is an internal lowering invariant. The same ordering primitive
        // builds scalar, direct-payload, and nested-enum physical registries;
        // their compact tags are not publicly observable on their own.
        //
        // base ──► A (+ ordinal 1) ──► A2 (+ ordinal 2)
        //   └────────────────────────► B (+ ordinal 1)
        //
        // B is published later in the dense catalogue, so it must append
        // after A2 rather than retag A2 merely because B's local ordinal is
        // shallower. The test is sensitive to restoring ordinal-first order.
        let base = schema(1);
        let a = schema(2);
        let a2 = schema(3);
        let b = schema(4);
        let aliases = BTreeMap::from([
            (base, SchemaVersionAlias(1)),
            (a, SchemaVersionAlias(2)),
            (a2, SchemaVersionAlias(3)),
            (b, SchemaVersionAlias(4)),
        ]);
        let base_case = case(base, 0);
        let a_case = case(a, 1);
        let a2_case = case(a2, 2);
        let b_case = case(b, 1);

        for registry_kind in ["scalar", "direct payload", "nested"] {
            let mut registry = vec![
                base_case.clone(),
                a_case.clone(),
                a2_case.clone(),
                b_case.clone(),
            ];
            registry.sort_by(|left, right| compare_scalar_enum_cases(&aliases, left, right));
            assert_eq!(
                registry,
                vec![
                    base_case.clone(),
                    a_case.clone(),
                    a2_case.clone(),
                    b_case.clone()
                ],
                "{registry_kind} registry"
            );
        }
    }

    #[test]
    fn concurrent_scalar_enum_merge_preserves_established_prefix_and_distinct_cases() {
        // This is deliberately an internal lowering test: the failure happens
        // before a public row can be decoded. Two concurrent authored schemas
        // both use ordinal 2, so accepting the raw tags as one physical tag
        // would alias `archived` and `snoozed`.
        let schema = |variants: &[&str]| {
            records::ValueType::EnumTag(
                records::ScalarEnumSchema::new("status", variants.iter().copied())
                    .unwrap()
                    .with_registry_id(91),
            )
        };
        let archived = schema(&["draft", "published", "archived"]);
        let snoozed = schema(&["draft", "published", "snoozed"]);

        let merged_ab = merge_physical_value_type(&archived, &snoozed)
            .expect("concurrent enum cases must coexist in one physical registry");
        let merged_ba = merge_physical_value_type(&snoozed, &archived)
            .expect("the opposite established prefix also accepts its sibling");

        // This compatibility helper operates on an already-established local
        // physical descriptor. It is intentionally directional: canonical
        // catalogue ordering has already happened before this point, so
        // sorting or rebuilding this descriptor would retag stored values.
        // The schema-qualified physical lowering path supplies that canonical
        // order; this helper must only append a distinct sibling case.
        let records::ValueType::EnumTag(merged_ab) = merged_ab else {
            panic!("expected scalar enum registry");
        };
        let records::ValueType::EnumTag(merged_ba) = merged_ba else {
            panic!("expected scalar enum registry");
        };
        assert_eq!(
            merged_ab.variants,
            vec!["draft", "published", "archived", "snoozed"],
            "left registry stays an exact physical prefix"
        );
        assert_eq!(
            merged_ba.variants,
            vec!["draft", "published", "snoozed", "archived"],
            "the reverse call preserves its own established prefix"
        );
        assert_eq!(merged_ab.variants.len(), 4);
        assert_eq!(merged_ba.variants.len(), 4);
    }

    #[test]
    fn concurrent_scalar_enum_write_remap_never_aliases_sibling_ordinals() {
        let base = schema(1);
        let archived = schema(2);
        let snoozed = schema(3);
        let base_cases = [
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 0,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 1,
            },
        ];
        let archived_cases = base_cases
            .iter()
            .cloned()
            .chain(std::iter::once(GlobalScalarEnumCaseId {
                introducing_schema: archived,
                introducing_ordinal: 2,
            }))
            .collect::<Vec<_>>();
        let snoozed_cases = base_cases
            .iter()
            .cloned()
            .chain(std::iter::once(GlobalScalarEnumCaseId {
                introducing_schema: snoozed,
                introducing_ordinal: 2,
            }))
            .collect::<Vec<_>>();
        let physical_cases = archived_cases
            .iter()
            .cloned()
            .chain(snoozed_cases.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let archived_tag = remap_authored_scalar_enum_value(
            Value::Nullable(Some(Box::new(Value::EnumTag(2)))),
            &archived_cases,
            &physical_cases,
        )
        .unwrap();
        let snoozed_tag = remap_authored_scalar_enum_value(
            Value::Nullable(Some(Box::new(Value::EnumTag(2)))),
            &snoozed_cases,
            &physical_cases,
        )
        .unwrap();
        assert_ne!(archived_tag, snoozed_tag);
    }

    #[test]
    fn concurrent_payload_enum_additions_preserve_distinct_case_layouts() {
        let schema = |cases: Vec<records::EnumCase>| {
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new("status", cases)
                    .unwrap()
                    .with_registry_id(92),
            ))
        };
        let payload = |name| records::RecordDescriptor::new([(name, records::ValueType::String)]);
        let archived = schema(vec![
            records::EnumCase::new("draft", payload("label")),
            records::EnumCase::new("published", payload("label")),
            records::EnumCase::new("archived", payload("reason")),
        ]);
        let snoozed = schema(vec![
            records::EnumCase::new("draft", payload("label")),
            records::EnumCase::new("published", payload("label")),
            records::EnumCase::new("snoozed", payload("until")),
        ]);
        let merged = merge_physical_value_type(&archived, &snoozed)
            .expect("concurrent payload cases must coexist");
        let records::ValueType::Enum(registry) = merged else {
            panic!("expected payload enum registry");
        };
        assert_eq!(registry.cases.len(), 4);
        assert!(registry.cases.iter().any(|case| case.name == "archived"));
        assert!(registry.cases.iter().any(|case| case.name == "snoozed"));
    }

    #[test]
    fn concurrent_same_named_payload_case_must_not_merge_incompatibly() {
        let schema = |payload| {
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new("status", [records::EnumCase::new("draft", payload)])
                    .unwrap()
                    .with_registry_id(93),
            ))
        };
        let left = schema(records::RecordDescriptor::new([(
            "label",
            records::ValueType::String,
        )]));
        let right = schema(records::RecordDescriptor::new([(
            "label",
            records::ValueType::U64,
        )]));
        assert!(merge_physical_value_type(&left, &right).is_err());
    }

    #[test]
    fn concurrent_payload_enum_write_remap_never_aliases_sibling_ordinals() {
        let descriptor = records::RecordDescriptor::new([("value", records::ValueType::String)]);
        let authored = |new_case| {
            records::EnumSchema::new(
                "status",
                [
                    records::EnumCase::new("draft", descriptor.clone()),
                    records::EnumCase::new("published", descriptor.clone()),
                    records::EnumCase::new(new_case, descriptor.clone()),
                ],
            )
            .unwrap()
        };
        let archived = authored("archived");
        let snoozed = authored("snoozed");
        let base = schema(1);
        let archived_schema = schema(2);
        let snoozed_schema = schema(3);
        let archived_cases = vec![
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 0,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: base,
                introducing_ordinal: 1,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: archived_schema,
                introducing_ordinal: 2,
            },
        ];
        let snoozed_cases = vec![
            archived_cases[0].clone(),
            archived_cases[1].clone(),
            GlobalScalarEnumCaseId {
                introducing_schema: snoozed_schema,
                introducing_ordinal: 2,
            },
        ];
        let physical_cases = archived_cases
            .iter()
            .cloned()
            .chain(snoozed_cases.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let payload =
            records::EnumValue::create(2, descriptor.clone(), &[Value::String("x".to_owned())])
                .unwrap();
        let archived_value = remap_authored_payload_enum_value(
            Value::Enum(payload.clone()),
            &archived,
            &archived_cases,
            &physical_cases,
        )
        .unwrap();
        let snoozed_value = remap_authored_payload_enum_value(
            Value::Enum(payload),
            &snoozed,
            &snoozed_cases,
            &physical_cases,
        )
        .unwrap();
        assert_ne!(archived_value, snoozed_value);
    }

    #[test]
    fn nested_scalar_enum_remap_rewrites_array_and_nullable_tags() {
        let authored_enum = records::ValueType::EnumTag(
            records::ScalarEnumSchema::new("state", ["draft", "archived"]).unwrap(),
        );
        let physical_enum = records::ValueType::EnumTag(
            records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"]).unwrap(),
        );
        let authored = records::ValueType::Tuple(vec![
            records::ValueType::Array(Box::new(authored_enum.clone())),
            records::ValueType::Nullable(Box::new(authored_enum)),
        ]);
        let physical = records::ValueType::Tuple(vec![
            records::ValueType::Array(Box::new(physical_enum.clone())),
            records::ValueType::Nullable(Box::new(physical_enum)),
        ]);
        let remaps = EnumOccurrenceRemaps {
            scalar: BTreeMap::from([
                ("root/tuple/0/array".to_owned(), vec![Some(0), Some(2)]),
                ("root/tuple/1/nullable".to_owned(), vec![Some(0), Some(2)]),
            ]),
            payload: BTreeMap::new(),
            payload_children: BTreeMap::new(),
        };
        let remapped = remap_nested_enum_value(
            Value::Tuple(vec![
                Value::Array(vec![Value::EnumTag(1)]),
                Value::Nullable(Some(Box::new(Value::EnumTag(1)))),
            ]),
            &authored,
            &physical,
            &remaps,
            "root",
        )
        .unwrap();
        assert_eq!(
            remapped,
            Value::Tuple(vec![
                Value::Array(vec![Value::EnumTag(2)]),
                Value::Nullable(Some(Box::new(Value::EnumTag(2)))),
            ])
        );
    }

    #[test]
    fn nested_scalar_registry_reconciliation_preserves_inherited_cases() {
        let base = schema(1);
        let child = schema(2);
        let nested = |variants: &[&str]| {
            records::ValueType::Array(Box::new(records::ValueType::Nullable(Box::new(
                records::ValueType::EnumTag(
                    records::ScalarEnumSchema::new("state", variants.iter().copied()).unwrap(),
                ),
            ))))
        };
        let mut cases = BTreeMap::new();
        hydrate_nested_scalar_enum_cases(
            &nested(&["draft", "published"]),
            base,
            "root",
            &mut cases,
        )
        .unwrap();
        reconcile_nested_scalar_enum_cases(
            &nested(&["draft", "published", "archived"]),
            child,
            "root",
            &mut cases,
        )
        .unwrap();
        assert_eq!(cases["root/array/nullable"].len(), 3);
        assert_eq!(cases["root/array/nullable"][0].introducing_schema, base);
        assert_eq!(cases["root/array/nullable"][2].introducing_schema, child);
    }

    #[test]
    fn nested_payload_descriptor_unions_siblings_by_global_parent_identity() {
        // Two concurrent parent cases both occupy authored ordinal 1. Their
        // nested payload enum layouts must stay under separate global parent
        // paths, and the physical descriptor must retain both after a reopen.
        let base = schema(1);
        let archived = schema(2);
        let snoozed = schema(3);
        let root = "root/record/event";
        let base_case = GlobalScalarEnumCaseId {
            introducing_schema: base,
            introducing_ordinal: 0,
        };
        let archived_case = GlobalScalarEnumCaseId {
            introducing_schema: archived,
            introducing_ordinal: 1,
        };
        let snoozed_case = GlobalScalarEnumCaseId {
            introducing_schema: snoozed,
            introducing_ordinal: 1,
        };
        let inner = |name: &str| {
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new(
                    format!("inner-{name}"),
                    [records::EnumCase::new(
                        name,
                        records::RecordDescriptor::new([("value", records::ValueType::String)]),
                    )],
                )
                .unwrap(),
            ))
        };
        let payload = |name: &str| records::RecordDescriptor::new([("detail", inner(name))]);
        let outer = records::ValueType::Record(Box::new(records::RecordDescriptor::new([(
            "event",
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new(
                    "authored-event",
                    [
                        records::EnumCase::new("base", payload("base")),
                        records::EnumCase::new("archived", payload("archived")),
                    ],
                )
                .unwrap(),
            )),
        )])));

        let mut payload_registries = BTreeMap::from([(
            root.to_owned(),
            vec![
                base_case.clone(),
                archived_case.clone(),
                snoozed_case.clone(),
            ],
        )]);
        for (parent, child) in [
            (&base_case, base),
            (&archived_case, archived),
            (&snoozed_case, snoozed),
        ] {
            payload_registries.insert(
                format!("{}/record/detail", global_case_path(root, parent)),
                vec![GlobalScalarEnumCaseId {
                    introducing_schema: child,
                    introducing_ordinal: 0,
                }],
            );
        }
        let mut layouts = BTreeMap::from([
            ((root.to_owned(), base_case.clone()), payload("base")),
            (
                (root.to_owned(), archived_case.clone()),
                payload("archived"),
            ),
            ((root.to_owned(), snoozed_case.clone()), payload("snoozed")),
        ]);
        for parent in [&base_case, &archived_case, &snoozed_case] {
            layouts.insert(
                (
                    format!("{}/record/detail", global_case_path(root, parent)),
                    GlobalScalarEnumCaseId {
                        introducing_schema: parent.introducing_schema,
                        introducing_ordinal: 0,
                    },
                ),
                records::RecordDescriptor::new([("value", records::ValueType::String)]),
            );
        }
        let physical = physical_nested_enum_value_type(
            &outer,
            "root",
            &BTreeMap::new(),
            &payload_registries,
            &layouts,
            PhysicalColumnId(9),
        )
        .unwrap();
        let records::ValueType::Record(record) = physical else {
            panic!("physical record expected");
        };
        let records::ValueType::Enum(events) = &record.fields()[0].value_type else {
            panic!("physical payload enum expected");
        };
        assert_eq!(events.cases.len(), 3);
        assert_ne!(
            events.cases[1].name, events.cases[2].name,
            "concurrent ordinal-one parents must not collide"
        );
        for case in &events.cases {
            let records::ValueType::Enum(detail) = &case.payload.fields()[0].value_type else {
                panic!("recursively lowered payload enum expected");
            };
            assert_eq!(detail.cases.len(), 1);
        }
    }

    #[test]
    fn physical_large_scalar_kind_is_schema_derived_authenticated_and_json_is_not_text() {
        let text = ColumnSchema::new("body", records::ValueType::String);
        let mut json = ColumnSchema::new("body", records::ValueType::String);
        json.large_value_kind = crate::schema::LargeValueSemanticKind::Json;

        assert_eq!(
            physical_storage_value_type(&text),
            records::ValueType::String
        );
        assert_eq!(
            physical_storage_value_type(&json),
            groove::large_values::physical_storage_value_type(
                groove::large_values::LargeValueKind::Json,
            )
        );
        assert_ne!(physical_storage_value_type(&text), physical_storage_value_type(&json));
        assert!(physical_storage_value_type(&json).is_internal_storage_type());
        assert!(
            std::panic::catch_unwind(|| {
                ColumnSchema::new("not_public", physical_storage_value_type(&json))
            })
            .is_err(),
            "the physical descriptor constructor cannot be smuggled back into a public Jazz schema"
        );

        let text_cell = records::RecordDescriptor::new([(
            "cell",
            physical_storage_value_type(&text),
        )]);
        let json_cell = records::RecordDescriptor::new([(
            "cell",
            physical_storage_value_type(&json),
        )]);
        let same_json_shaped_bytes = Value::String(r#"{"title":"same bytes"}"#.to_owned());
        assert_eq!(
            text_cell.create(std::slice::from_ref(&same_json_shaped_bytes)).unwrap(),
            json_cell.create(std::slice::from_ref(&same_json_shaped_bytes)).unwrap(),
            "inline payloads stay compact because the containing schema supplies their kind"
        );

        let json_prepared = groove::large_values::prepare(
            groove::large_values::LargeValueKind::Json,
            br#"{"title":"same bytes"}"#,
        )
        .unwrap();
        let json_root = json_prepared.value_ref.clone();
        assert!(
            json_cell.create(&[Value::Large(json_root.clone())]).is_ok(),
            "the JSON physical descriptor accepts its schema-derived large value"
        );
        assert!(
            text_cell.create(&[Value::Large(json_root.clone())]).is_err(),
            "a JSON descriptor must not enter text physical storage"
        );

        let json_record = json_cell
            .create(&[Value::Large(json_root.clone())])
            .expect("encode JSON physical cell");
        let replayed_values = text_cell.bind(&json_record).to_values().unwrap();
        let [Value::Large(replayed)] = replayed_values.as_slice() else {
            panic!("chunked physical arm must decode")
        };
        let root = json_prepared
            .staged_chunks
            .iter()
            .find(|chunk| chunk.node_ref == json_root.root)
            .unwrap();
        assert_eq!(
            groove::large_values::decode_node(
                replayed.kind,
                root.node_ref.object_hash,
                &root.encoded,
            ),
            Err(groove::large_values::Error::DescriptorMismatch),
            "the independently addressed root authenticates its semantic kind"
        );
    }
}

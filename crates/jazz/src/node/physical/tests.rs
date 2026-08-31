#[cfg(test)]
mod variant_case_tests {
    use super::*;
    use crate::legacy_test_future::SettledNodeTestExt as _;
    use crate::protocol::TableLens;
    use crate::query::Query as JazzQuery;
    use crate::tools::public_schema::{
        ColumnDescriptor as PublicColumnDescriptor, ColumnType as PublicColumnType,
        EnumCaseDescriptor as PublicEnumCaseDescriptor, SchemaBuilder as PublicSchemaBuilder,
        TableSchemaBuilder as PublicTableSchemaBuilder,
    };
    use jazz_storage_rocksdb::RocksDbStorage;
    use std::path::Path;

    fn schema(byte: u8) -> SchemaVersionId {
        SchemaVersionId(uuid::Uuid::from_bytes([byte; 16]))
    }

    fn case(schema: SchemaVersionId, ordinal: u8) -> GlobalScalarEnumCaseId {
        let mut bytes = *schema.0.as_bytes();
        bytes[15] ^= ordinal;
        GlobalScalarEnumCaseId {
            id: crate::ids::GlobalPhysicalEnumVariantId(uuid::Uuid::from_bytes(bytes)),
            introducing_schema: schema,
            introducing_ordinal: ordinal,
        }
    }

    fn payload_case(schema: SchemaVersionId, ordinal: u32) -> GlobalEnumCaseId {
        let mut bytes = *schema.0.as_bytes();
        for (target, source) in bytes[12..].iter_mut().zip(ordinal.to_le_bytes()) {
            *target ^= source;
        }
        GlobalEnumCaseId {
            id: crate::ids::GlobalPhysicalEnumVariantId(uuid::Uuid::from_bytes(bytes)),
            introducing_schema: schema,
            introducing_ordinal: ordinal,
        }
    }

    fn mapping(table_id: u64, columns: &[(&str, u64)]) -> SchemaPhysicalMapping {
        SchemaPhysicalMapping {
            identities: PhysicalIdentityManifest {
                tables: BTreeMap::from([(
                    "entries".to_owned(),
                    PhysicalTableIdentity {
                        id: crate::ids::GlobalPhysicalTableId(uuid::Uuid::from_u128(table_id as u128 + 1)),
                        columns: columns
                            .iter()
                            .map(|(name, id)| {
                                (
                                    name.to_string(),
                                    PhysicalColumnIdentity {
                                        id: crate::ids::GlobalPhysicalColumnId(uuid::Uuid::from_u128(*id as u128 + 100)),
                                        enum_variants: BTreeMap::new(),
                                    },
                                )
                            })
                            .collect(),
                    },
                )]),
            },
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

        // The mapping is the canonical typed payload durably written in
        // jazz_schema_versions; its exact round trip models close/reopen.
        let reopened = mappings
            .iter()
            .map(|(version, mapping)| {
                Ok((*version, codec::decode_physical_mapping(&codec::encode_physical_mapping(mapping)?)?))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()
            .unwrap();
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
    fn reopen_validation_rejects_nil_global_enum_case_identity() {
        // Internal recovery-boundary test: no public catalogue operation can
        // author a registry case without its schema, but a corrupt durable
        // payload must never turn that unknown identity into a local tag.
        let known = schema(1);
        let mut corrupt = mapping(7, &[("state", 1)]);
        corrupt.tables.get_mut("entries").unwrap().scalar_enum_cases.insert(
            PhysicalColumnId(1),
            vec![GlobalScalarEnumCaseId {
                id: crate::ids::GlobalPhysicalEnumVariantId(uuid::Uuid::nil()),
                introducing_schema: known,
                introducing_ordinal: 0,
            }],
        );
        assert!(matches!(
            validate_physical_mapping_registries(
                &BTreeMap::from([(known, corrupt)]),
                &BTreeMap::from([(known, SchemaVersionAlias(1))]),
            ),
            Err(Error::InvalidStoredValue(
                "physical enum registry contains a nil global identity"
            ))
        ));

        let identity = case(known, 0);
        let mut duplicate = mapping(8, &[("state", 2)]);
        duplicate.tables.get_mut("entries").unwrap().scalar_enum_cases.insert(
            PhysicalColumnId(2),
            vec![
                identity.clone(),
                GlobalScalarEnumCaseId {
                    id: identity.id,
                    introducing_schema: schema(2),
                    introducing_ordinal: 9,
                },
            ],
        );
        assert!(matches!(
            validate_physical_mapping_registries(
                &BTreeMap::from([(known, duplicate)]),
                &BTreeMap::from([
                    (known, SchemaVersionAlias(1)),
                    (schema(2), SchemaVersionAlias(2)),
                ]),
            ),
            Err(Error::InvalidStoredValue(
                "physical enum registry repeats a case identity"
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
            registry
                .sort_by(|left, right| compare_scalar_enum_cases(&aliases, left, right));
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
            case(base, 0),
            case(base, 1),
        ];
        let archived_cases = base_cases
            .iter()
            .cloned()
            .chain(std::iter::once(case(archived, 2)))
            .collect::<Vec<_>>();
        let snoozed_cases = base_cases
            .iter()
            .cloned()
            .chain(std::iter::once(case(snoozed, 2)))
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
            payload_case(base, 0),
            payload_case(base, 1),
            payload_case(archived_schema, 2),
        ];
        let snoozed_cases = vec![
            archived_cases[0].clone(),
            archived_cases[1].clone(),
            payload_case(snoozed_schema, 2),
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
        let base_ids = BTreeMap::from([(
            "root/array/nullable".to_owned(),
            vec![case(base, 0).id, case(base, 1).id],
        )]);
        let mut cases = BTreeMap::new();
        hydrate_nested_scalar_enum_cases(
            &nested(&["draft", "published"]),
            &base_ids,
            base,
            "root",
            "root",
            &mut cases,
        )
        .unwrap();
        let evolved_ids = BTreeMap::from([(
            "root/array/nullable".to_owned(),
            vec![case(base, 0).id, case(base, 1).id, case(child, 2).id],
        )]);
        hydrate_nested_scalar_enum_cases(
            &nested(&["draft", "published", "archived"]),
            &evolved_ids,
            child,
            "root",
            "root",
            &mut cases,
        )
        .unwrap();
        assert_eq!(cases["root/array/nullable"].len(), 3);
        assert_eq!(cases["root/array/nullable"][0].id, case(base, 0).id);
        assert_eq!(cases["root/array/nullable"][2].id, case(child, 2).id);
    }

    #[test]
    fn payload_enum_catalogue_mapping_preserves_root_and_nested_u32_ordinals() {
        let empty_payload = || {
            records::RecordDescriptor::new(Vec::<(String, records::ValueType)>::new())
        };
        let nested_payload = records::ValueType::Enum(Box::new(
            records::EnumSchema::new(
                "detail",
                (0..257).map(|ordinal| {
                    let payload = if ordinal == 256 {
                        records::RecordDescriptor::new([(
                            "message",
                            records::ValueType::String,
                        )])
                    } else {
                        empty_payload()
                    };
                    records::EnumCase::new(format!("detail-{ordinal}"), payload)
                }),
            )
            .unwrap(),
        ));
        let payload_enum = |case_count: usize| {
            records::ValueType::Enum(Box::new(
                records::EnumSchema::new(
                    "wide",
                    (0..case_count).map(|ordinal| {
                        let payload = if ordinal == 256 {
                            records::RecordDescriptor::new([(
                                "detail",
                                nested_payload.clone(),
                            )])
                        } else {
                            empty_payload()
                        };
                        records::EnumCase::new(format!("case-{ordinal}"), payload)
                    }),
                )
                .unwrap(),
            ))
        };

        let introducing = schema(1);
        let nested_authored_path = "root/case/256/record/detail";
        let inherited_root_ids = (0..257)
            .map(|ordinal| payload_case(introducing, ordinal).id)
            .collect::<Vec<_>>();
        let nested_ids = (0..257)
            .map(|ordinal| payload_case(schema(4), ordinal).id)
            .collect::<Vec<_>>();
        let identities = BTreeMap::from([
            ("root".to_owned(), inherited_root_ids.clone()),
            (nested_authored_path.to_owned(), nested_ids.clone()),
        ]);
        let mut registries = BTreeMap::new();
        hydrate_nested_payload_enum_cases(
            &payload_enum(257),
            &identities,
            introducing,
            "root",
            "root",
            &mut registries,
        )
        .unwrap();
        assert_eq!(registries["root"][0], payload_case(introducing, 0));
        assert_eq!(
            registries["root"][256],
            payload_case(introducing, 256)
        );
        let nested_path = format!(
            "{}/record/detail",
            global_case_path("root", &payload_case(introducing, 256))
        );
        assert_eq!(
            registries[&nested_path][256].id,
            payload_case(schema(4), 256).id
        );

        let introducing_append = schema(2);
        let evolved = payload_enum(258);
        let mut evolved_root_ids = inherited_root_ids;
        evolved_root_ids.push(payload_case(introducing_append, 257).id);
        let evolved_identities = BTreeMap::from([
            ("root".to_owned(), evolved_root_ids),
            (nested_authored_path.to_owned(), nested_ids),
        ]);
        hydrate_nested_payload_enum_cases(
            &evolved,
            &evolved_identities,
            introducing_append,
            "root",
            "root",
            &mut registries,
        )
        .unwrap();
        assert_eq!(
            registries["root"][257],
            payload_case(introducing_append, 257)
        );
        let reopened: BTreeMap<String, Vec<GlobalEnumCaseId>> =
            serde_json::from_slice(&serde_json::to_vec(&registries).unwrap()).unwrap();
        assert_eq!(
            reopened["root"][256],
            payload_case(introducing, 256)
        );
        assert_eq!(
            reopened[&nested_path][256].id,
            payload_case(schema(4), 256).id
        );

        let records::ValueType::Enum(evolved_schema) = &evolved else {
            unreachable!("payload enum helper returns an enum");
        };
        let mut physical_cases = vec![payload_case(schema(3), 0)];
        physical_cases.extend(registries["root"].iter().cloned());
        let remapped = remap_authored_payload_enum_value(
            Value::Enum(records::EnumValue::create(257, empty_payload(), &[]).unwrap()),
            evolved_schema,
            &registries["root"],
            &physical_cases,
        )
        .unwrap();
        let Value::Enum(remapped) = remapped else {
            panic!("remapped payload enum expected");
        };
        assert_eq!(remapped.tag(), 258);

        let mut layouts = BTreeMap::new();
        collect_nested_payload_enum_layouts(&evolved, "root", &registries, &mut layouts).unwrap();
        let nested_prefix = payload_case(schema(0), 0);
        layouts.insert(
            (nested_path.clone(), nested_prefix.clone()),
            empty_payload(),
        );
        let mut physical_registries = registries.clone();
        physical_registries
            .get_mut(&nested_path)
            .unwrap()
            .insert(0, nested_prefix);
        let projected = physical_nested_enum_value_type(
            &evolved,
            "root",
            &BTreeMap::new(),
            &physical_registries,
            &layouts,
            PhysicalColumnId(9),
        )
        .unwrap();
        let records::ValueType::Enum(projected_schema) = &projected else {
            panic!("physical payload enum expected");
        };
        assert_eq!(projected_schema.cases.len(), 258);
        assert_eq!(
            projected_schema.cases[257].name,
            physical_enum_case_name(&payload_case(introducing_append, 257))
        );
        let records::ValueType::Enum(projected_nested) =
            &projected_schema.cases[256].payload.fields()[0].value_type
        else {
            panic!("nested physical payload enum expected");
        };
        assert_eq!(projected_nested.cases.len(), 258);

        // Exercise the same occurrence remap carrier assembled from catalogue
        // mappings on production writes. The nested authored tag 256 shifts
        // past an independently introduced physical case without narrowing.
        let tags = |authored: &[GlobalEnumCaseId], physical: &[GlobalEnumCaseId]| {
            authored
                .iter()
                .map(|identity| {
                    physical
                        .iter()
                        .position(|candidate| candidate == identity)
                        .map(|tag| u32::try_from(tag).unwrap())
                })
                .collect::<Vec<_>>()
        };
        let children = |path: &str, authored: &[GlobalEnumCaseId]| {
            authored
                .iter()
                .map(|identity| Some(global_case_path(path, identity)))
                .collect::<Vec<_>>()
        };
        let remaps = EnumOccurrenceRemaps {
            scalar: BTreeMap::new(),
            payload: BTreeMap::from([
                (
                    "root".to_owned(),
                    tags(&registries["root"], &physical_registries["root"]),
                ),
                (
                    nested_path.clone(),
                    tags(
                        &registries[&nested_path],
                        &physical_registries[&nested_path],
                    ),
                ),
            ]),
            payload_children: BTreeMap::from([
                (
                    "root".to_owned(),
                    children("root", &registries["root"]),
                ),
                (
                    nested_path.clone(),
                    children(&nested_path, &registries[&nested_path]),
                ),
            ]),
        };
        let authored_outer_case = evolved_schema.case(256).unwrap();
        let records::ValueType::Enum(authored_nested_schema) =
            &authored_outer_case.payload.fields()[0].value_type
        else {
            panic!("nested authored payload enum expected");
        };
        let nested_value = records::EnumValue::create(
            256,
            authored_nested_schema.cases[256].payload.clone(),
            &[Value::String("nested-wide-payload".to_owned())],
        )
        .unwrap();
        let outer_value = records::EnumValue::create(
            256,
            authored_outer_case.payload.clone(),
            &[Value::Enum(nested_value)],
        )
        .unwrap();
        let remapped = remap_nested_enum_value(
            Value::Enum(outer_value),
            &evolved,
            &projected,
            &remaps,
            "root",
        )
        .unwrap();
        let Value::Enum(remapped_outer) = remapped else {
            panic!("remapped outer payload enum expected");
        };
        assert_eq!(remapped_outer.tag(), 256);
        let mut remapped_outer_values = remapped_outer.record().to_values().unwrap();
        let Value::Enum(remapped_nested) = remapped_outer_values.remove(0) else {
            panic!("remapped nested payload enum expected");
        };
        assert_eq!(remapped_nested.tag(), 257);
        assert_eq!(
            remapped_nested.record().to_values().unwrap(),
            vec![Value::String("nested-wide-payload".to_owned())]
        );
    }

    #[test]
    fn scalar_enum_physical_encoding_rejects_more_than_u8_tags() {
        let introducing = schema(1);
        let physical_cases = (0..=u8::MAX)
            .map(|ordinal| case(introducing, ordinal))
            .chain(std::iter::once(case(schema(2), 0)))
            .collect::<Vec<_>>();

        assert!(matches!(
            physical_scalar_enum_schema(PhysicalColumnId(1), &physical_cases),
            Err(Error::InvalidStoredValue(
                "invalid physical scalar enum registry"
            ))
        ));
        assert!(matches!(
            remap_authored_scalar_enum_value(
                Value::EnumTag(0),
                &[case(schema(2), 0)],
                &physical_cases,
            ),
            Err(Error::InvalidStoredValue(
                "physical scalar enum tag exhausted"
            ))
        ));
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
        let base_case = payload_case(base, 0);
        let archived_case = payload_case(archived, 1);
        let snoozed_case = payload_case(snoozed, 1);
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
                vec![payload_case(child, 0)],
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
        for (parent, child) in [
            (&base_case, base),
            (&archived_case, archived),
            (&snoozed_case, snoozed),
        ] {
            layouts.insert(
                (
                    format!("{}/record/detail", global_case_path(root, parent)),
                    payload_case(child, 0),
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

    fn public_wide_payload_schema(case_count: usize) -> JazzSchema {
        let cases = (0..case_count)
            .map(|ordinal| PublicEnumCaseDescriptor {
                name: format!("case-{ordinal}"),
                fields: if ordinal == 256 {
                    vec![PublicColumnDescriptor::new(
                        "detail",
                        PublicColumnType::Text,
                    )]
                } else {
                    Vec::new()
                },
            })
            .collect();
        let public = PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("events").column(
                "event",
                PublicColumnType::CatalogueEnumPayload {
                    name: "event".to_owned(),
                    cases,
                },
            ))
            .build();
        JazzSchema::new(&public).expect("wide payload schema compiles")
    }

    fn public_wide_scalar_schema(last_case: Option<&str>) -> JazzSchema {
        let mut variants = (0..255)
            .map(|ordinal| format!("case-{ordinal}"))
            .collect::<Vec<_>>();
        variants.extend(last_case.map(str::to_owned));
        let public = PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("events").column(
                "status",
                PublicColumnType::ScalarEnum {
                    name: "status".to_owned(),
                    variants,
                },
            ))
            .build();
        JazzSchema::new(&public).expect("wide scalar schema compiles")
    }

    fn wide_scalar_sibling_publication(
        base: &JazzSchema,
        source_identities: &PhysicalIdentityManifest,
        target: SchemaVersion,
    ) -> SchemaLineagePublication {
        SchemaLineagePublication::author_from_prior(
            base,
            source_identities,
            target.clone(),
            MigrationLens::new(
                base.version_id(),
                target.id,
                vec![TableLens {
                    source_table: "events".to_owned(),
                    target_table: "events".to_owned(),
                    ops: vec![LensOp::TransformColumn {
                        column: "status".to_owned(),
                        transform: "jazz.identity".to_owned(),
                    }],
                }],
            )
            .expect("valid wide scalar lens"),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .expect("author wide scalar lineage")
    }

    fn open_receipt_node(
        path: &Path,
        node_uuid: NodeUuid,
        genesis: &JazzSchema,
    ) -> NodeState<RocksDbStorage> {
        let column_families = genesis.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let storage = RocksDbStorage::open(path, &refs).expect("open receipt storage");
        crate::db::block_on(NodeState::new(node_uuid, genesis.clone(), storage))
            .expect("open receipt node")
    }

    fn payload_case_256(schema: &JazzSchema) -> Value {
        let table = &schema.tables[0];
        let event = table
            .columns
            .iter()
            .find(|column| column.name == "event")
            .expect("event column");
        let records::ValueType::Enum(event_schema) = &event.column_type else {
            panic!("event payload enum expected");
        };
        let payload = event_schema.cases[256].payload.clone();
        Value::Enum(
            records::EnumValue::create(
                256,
                payload,
                &[Value::String("wide-payload".to_owned())],
            )
            .expect("create case 256 payload"),
        )
    }

    fn assert_payload_case_256(value: Value) {
        let Value::Enum(payload) = value else {
            panic!("payload enum expected");
        };
        assert_eq!(payload.tag(), 256);
        assert_eq!(
            payload.record().to_values().expect("decode payload"),
            vec![Value::String("wide-payload".to_owned())]
        );
    }

    fn assert_wide_payload_receipt(
        node: &mut NodeState<RocksDbStorage>,
        schema: &JazzSchema,
        shape: &crate::query::ValidatedQuery,
        binding: &crate::query::Binding,
        expected_row: RowUuid,
    ) {
        let current = crate::db::block_on(node.current_rows_for_schema(
            "events",
            schema.version_id(),
            DurabilityTier::Local,
        ))
        .expect("read projected current rows");
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].row_uuid(), expected_row);
        assert_payload_case_256(
            current[0]
                .cell(&schema.tables[0], "event")
                .expect("current event cell"),
        );

        let queried =
            crate::db::block_on(node.query_rows(shape, binding, DurabilityTier::Local))
                .expect("read projected query rows");
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].row_uuid(), expected_row);
        assert_payload_case_256(
            queried[0]
                .cell(&schema.tables[0], "event")
                .expect("query event cell"),
        );
    }

    #[test]
    fn node_lifecycle_preserves_case_256_and_nested_payload_through_lens_storage_and_reopen() {
        let base = public_wide_payload_schema(256);
        let evolved_schema = public_wide_payload_schema(257);
        let evolved = SchemaVersion::new(evolved_schema.clone());
        let node_uuid = NodeUuid::from_bytes([0x91; 16]);
        let row_uuid = RowUuid::from_bytes([0x92; 16]);
        let dir = tempfile::tempdir().expect("create receipt directory");
        let mut node = open_receipt_node(dir.path(), node_uuid, &base);

        let source_identities = node.catalogue.physical_mappings[&base.version_id()]
            .identities
            .clone();
        let publication = SchemaLineagePublication::author_from_prior(
            &base,
            &source_identities,
            evolved.clone(),
            MigrationLens::new(
                base.version_id(),
                evolved.id,
                vec![TableLens {
                    source_table: "events".to_owned(),
                    target_table: "events".to_owned(),
                    ops: vec![LensOp::TransformColumn {
                        column: "event".to_owned(),
                        transform: "jazz.identity".to_owned(),
                    }],
                }],
            )
            .expect("valid wide payload lens"),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .expect("author wide payload lineage");
        let expected_case_id =
            publication.physical_identities.tables["events"].columns["event"].enum_variants
                ["root"][256];
        node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        })
        .expect("publish wide payload lineage");
        node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
        })
        .expect("select evolved write schema");

        let physical = &node.catalogue.physical_mappings[&evolved.id].tables["events"];
        let event_column = physical.columns["event"];
        assert_eq!(physical.payload_enum_cases[&event_column].len(), 257);
        assert_eq!(
            physical.payload_enum_cases[&event_column][256].id,
            expected_case_id
        );
        assert_eq!(
            physical.payload_enum_cases[&event_column][256].introducing_ordinal,
            256
        );

        node.commit_mergeable_settled(
            MergeableCommit::new("events", row_uuid, 1)
                .cell("event", payload_case_256(&evolved_schema)),
        )
        .expect("persist case 256");
        let shape = JazzQuery::from("events")
            .filter(crate::query::Predicate::EnumMatch {
                column: "event".to_owned(),
                case: "case-256".to_owned(),
                payload: Box::new(crate::query::Predicate::All(Vec::new())),
            })
            .validate(&evolved_schema)
            .expect("validate wide payload query");
        let binding = shape.bind(BTreeMap::new()).expect("bind wide payload query");
        assert_wide_payload_receipt(&mut node, &evolved_schema, &shape, &binding, row_uuid);

        crate::db::block_on(node.close()).expect("close durable receipt storage");
        drop(node);
        let mut reopened = open_receipt_node(dir.path(), node_uuid, &base);
        let reopened_physical =
            &reopened.catalogue.physical_mappings[&evolved.id].tables["events"];
        let reopened_event_column = reopened_physical.columns["event"];
        assert_eq!(
            reopened_physical.payload_enum_cases[&reopened_event_column][256].id,
            expected_case_id
        );
        assert_eq!(
            reopened_physical.payload_enum_cases[&reopened_event_column][256]
                .introducing_ordinal,
            256
        );
        assert_wide_payload_receipt(
            &mut reopened,
            &evolved_schema,
            &shape,
            &binding,
            row_uuid,
        );
        crate::db::block_on(reopened.close()).expect("close reopened receipt storage");
    }

    #[test]
    fn scalar_lineage_union_above_u8_is_rejected_without_activation_or_persistence() {
        let base = public_wide_scalar_schema(None);
        let sibling_a = SchemaVersion::new(public_wide_scalar_schema(Some("sibling-a")));
        let sibling_b = SchemaVersion::new(public_wide_scalar_schema(Some("sibling-b")));
        let node_uuid = NodeUuid::from_bytes([0x93; 16]);
        let dir = tempfile::tempdir().expect("create scalar receipt directory");
        let mut node = open_receipt_node(dir.path(), node_uuid, &base);
        let source_identities = node.catalogue.physical_mappings[&base.version_id()]
            .identities
            .clone();
        let publication = |target| {
            wide_scalar_sibling_publication(&base, &source_identities, target)
        };

        node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication(sibling_a.clone())),
        })
        .expect("publish first scalar sibling");
        let next_table = node.catalogue.next_physical_table_id;
        let next_column = node.catalogue.next_physical_column_id;
        let error = node
            .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
                author: AuthorSubject::SYSTEM,
                catalogue_seq: 2,
                publication: Box::new(publication(sibling_b.clone())),
            })
            .expect_err("257th physical scalar case must be rejected");
        assert!(matches!(
            error,
            Error::InvalidCatalogueUpdate(
                "physical scalar enum registry exceeds u8 capacity"
            )
        ));
        assert_eq!(node.active_catalogue_seq(), 1);
        assert!(!node.catalogue.catalogue_schemas.contains_key(&sibling_b.id));
        assert!(node.catalogue.pending_lineages.is_empty());
        assert!(node.catalogue.staged_lineages.is_empty());
        assert_eq!(node.catalogue.next_physical_table_id, next_table);
        assert_eq!(node.catalogue.next_physical_column_id, next_column);

        crate::db::block_on(node.close()).expect("close scalar receipt storage");
        drop(node);
        let mut reopened = open_receipt_node(dir.path(), node_uuid, &base);
        assert_eq!(reopened.active_catalogue_seq(), 1);
        assert!(reopened.catalogue.catalogue_schemas.contains_key(&sibling_a.id));
        assert!(!reopened.catalogue.catalogue_schemas.contains_key(&sibling_b.id));
        assert!(reopened.catalogue.pending_lineages.is_empty());
        assert!(reopened.catalogue.staged_lineages.is_empty());
        assert_eq!(reopened.catalogue.next_physical_table_id, next_table);
        assert_eq!(reopened.catalogue.next_physical_column_id, next_column);
        crate::db::block_on(reopened.close()).expect("close reopened scalar receipt storage");
    }

    #[test]
    fn parked_scalar_sibling_is_revalidated_and_durably_removed_at_active_sequence() {
        let base = public_wide_scalar_schema(None);
        let sibling_a = SchemaVersion::new(public_wide_scalar_schema(Some("sibling-a")));
        let sibling_b = SchemaVersion::new(public_wide_scalar_schema(Some("sibling-b")));
        let node_uuid = NodeUuid::from_bytes([0x94; 16]);
        let dir = tempfile::tempdir().expect("create parked scalar receipt directory");
        let mut node = open_receipt_node(dir.path(), node_uuid, &base);
        let source_identities = node.catalogue.physical_mappings[&base.version_id()]
            .identities
            .clone();
        let publication_a =
            wide_scalar_sibling_publication(&base, &source_identities, sibling_a.clone());
        let publication_b =
            wide_scalar_sibling_publication(&base, &source_identities, sibling_b.clone());
        let initial_next_table = node.catalogue.next_physical_table_id;
        let initial_next_column = node.catalogue.next_physical_column_id;

        let parked = node
            .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
                author: AuthorSubject::SYSTEM,
                catalogue_seq: 2,
                publication: Box::new(publication_b),
            })
            .expect("park out-of-order scalar sibling");
        assert!(parked.is_empty());
        assert_eq!(node.active_catalogue_seq(), 0);
        assert!(node.catalogue.pending_lineages.contains_key(&2));
        assert_eq!(node.catalogue.next_physical_table_id, initial_next_table);
        assert_eq!(node.catalogue.next_physical_column_id, initial_next_column);

        crate::db::block_on(node.close()).expect("close parked scalar receipt storage");
        drop(node);
        let mut node = open_receipt_node(dir.path(), node_uuid, &base);
        assert!(node.catalogue.pending_lineages.contains_key(&2));

        let drained = node
            .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
                author: AuthorSubject::SYSTEM,
                catalogue_seq: 1,
                publication: Box::new(publication_a),
            })
            .expect("activate first sibling and reject parked overflow");
        assert_eq!(
            drained
                .iter()
                .filter(|message| matches!(message, SyncMessage::CatalogueAck(_)))
                .count(),
            1
        );
        assert_eq!(node.active_catalogue_seq(), 1);
        assert!(node.catalogue.catalogue_schemas.contains_key(&sibling_a.id));
        assert!(!node.catalogue.catalogue_schemas.contains_key(&sibling_b.id));
        assert!(node.catalogue.pending_lineages.is_empty());
        assert!(node.catalogue.staged_lineages.is_empty());
        let next_table = node.catalogue.next_physical_table_id;
        let next_column = node.catalogue.next_physical_column_id;
        assert_eq!(next_table, initial_next_table);
        assert_eq!(next_column, initial_next_column);

        crate::db::block_on(node.close()).expect("close cleaned scalar receipt storage");
        drop(node);
        let mut reopened = open_receipt_node(dir.path(), node_uuid, &base);
        assert_eq!(reopened.active_catalogue_seq(), 1);
        assert!(reopened.catalogue.catalogue_schemas.contains_key(&sibling_a.id));
        assert!(!reopened.catalogue.catalogue_schemas.contains_key(&sibling_b.id));
        assert!(reopened.catalogue.pending_lineages.is_empty());
        assert!(reopened.catalogue.staged_lineages.is_empty());
        assert_eq!(reopened.catalogue.next_physical_table_id, next_table);
        assert_eq!(reopened.catalogue.next_physical_column_id, next_column);
        crate::db::block_on(reopened.close())
            .expect("close reopened cleaned scalar receipt storage");
    }
}

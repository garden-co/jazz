//! Schema admission, variants, enum registries, row encoding, and validation.

use super::*;

#[futures_test::test]
async fn live_variant_table_evolves_direct_payload_enum_registry_before_new_layout() {
    let old_state = payload_enum_type(41, [("draft", ValueType::U64)]).nullable();
    let next_state = payload_enum_type(
        41,
        [("draft", ValueType::U64), ("published", ValueType::String)],
    )
    .nullable();
    let schema = DatabaseSchema::new([live_variant_enum_table(old_state)]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage).await.unwrap();

    database
        .evolve_table_variant_registries("items", &[ColumnSchema::new("state", next_state.clone())])
        .unwrap();
    database
        .register_table_variant_with_columns(
            "items",
            [],
            TableVariant::with_payload(
                2,
                [
                    TableVariantField::shared("id", ColumnType::U64, "id"),
                    TableVariantField::shared("state", next_state.clone(), "state"),
                ],
            ),
        )
        .unwrap();

    let table = database.table_schema("items").unwrap();
    assert_eq!(
        table
            .columns
            .iter()
            .find(|column| column.name == "state")
            .unwrap()
            .column_type,
        next_state
    );
    for tag in [1, 2] {
        let field = table
            .variant(tag)
            .unwrap()
            .payload_fields
            .iter()
            .find(|field| field.shared_column.as_deref() == Some("state"))
            .unwrap();
        assert_eq!(
            field.value_type, next_state,
            "variant {tag} retained a stale enum descriptor"
        );
    }
}

/// Nested payload and scalar enum occurrences advance through nullable,
/// array, record, and tuple wrappers without replacing their physical registry
/// identities.
#[futures_test::test]
async fn live_table_evolves_nested_payload_and_scalar_enum_registries() {
    let old_payload = payload_enum_type(42, [("draft", ValueType::U64)]);
    let next_payload = payload_enum_type(
        42,
        [("draft", ValueType::U64), ("published", ValueType::String)],
    );
    let old_scalar = ValueType::EnumTag(
        ScalarEnumSchema::new("phase", ["one", "two"])
            .unwrap()
            .with_registry_id(43),
    );
    let next_scalar = ValueType::EnumTag(
        ScalarEnumSchema::new("phase", ["one", "two", "three"])
            .unwrap()
            .with_registry_id(43),
    );
    let old_nested_payload = ValueType::Nullable(Box::new(ValueType::Array(Box::new(old_payload))));
    let next_nested_payload =
        ValueType::Nullable(Box::new(ValueType::Array(Box::new(next_payload))));
    let old_nested_scalar = ValueType::Record(Box::new(RecordDescriptor::new([(
        "phase",
        old_scalar.clone(),
    )])));
    let next_nested_scalar = ValueType::Record(Box::new(RecordDescriptor::new([(
        "phase",
        next_scalar.clone(),
    )])));
    let old_tuple_scalar = ValueType::Tuple(vec![
        ValueType::EnumTag(
            ScalarEnumSchema::new("tuple_phase", ["one", "two"])
                .unwrap()
                .with_registry_id(46),
        ),
        ValueType::U64,
    ]);
    let next_tuple_scalar = ValueType::Tuple(vec![
        ValueType::EnumTag(
            ScalarEnumSchema::new("tuple_phase", ["one", "two", "three"])
                .unwrap()
                .with_registry_id(46),
        ),
        ValueType::U64,
    ]);
    let schema = DatabaseSchema::new([TableSchema::new_with_bound_registries(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", old_nested_payload),
            ColumnSchema::new("scalar", old_nested_scalar),
            ColumnSchema::new("tuple_scalar", old_tuple_scalar),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage).await.unwrap();

    database
        .evolve_table_variant_registries(
            "items",
            &[
                ColumnSchema::new("payload", next_nested_payload.clone()),
                ColumnSchema::new("scalar", next_nested_scalar.clone()),
                ColumnSchema::new("tuple_scalar", next_tuple_scalar.clone()),
            ],
        )
        .unwrap();

    let table = database.table_schema("items").unwrap();
    assert_eq!(
        table
            .columns
            .iter()
            .find(|column| column.name == "payload")
            .unwrap()
            .column_type,
        next_nested_payload
    );
    assert_eq!(
        table
            .columns
            .iter()
            .find(|column| column.name == "scalar")
            .unwrap()
            .column_type,
        next_nested_scalar
    );
    assert_eq!(
        table
            .columns
            .iter()
            .find(|column| column.name == "tuple_scalar")
            .unwrap()
            .column_type,
        next_tuple_scalar
    );
}

/// A registry may only append cases. Reordering, renaming, changing an
/// existing payload, changing its arity, or replacing a registry identity must
/// leave the live descriptor untouched and fail before a new layout is staged.
#[futures_test::test]
async fn live_table_rejects_non_additive_enum_registry_mutations() {
    let old_payload = payload_enum_type(44, [("draft", ValueType::U64)]).nullable();
    let schema = DatabaseSchema::new([live_variant_enum_table(old_payload.clone())]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage).await.unwrap();
    let incompatible = [
        payload_enum_type(44, [("renamed", ValueType::U64)]).nullable(),
        payload_enum_type(44, [("draft", ValueType::String)]).nullable(),
        ValueType::Enum(Box::new(
            EnumSchema::new(
                "state",
                [EnumCase::new(
                    "draft",
                    RecordDescriptor::new([("value", ValueType::U64), ("extra", ValueType::Bool)]),
                )],
            )
            .unwrap()
            .with_registry_id(44),
        ))
        .nullable(),
        payload_enum_type(45, [("draft", ValueType::U64)]).nullable(),
    ];

    for next in incompatible {
        assert!(matches!(
            database.evolve_table_variant_registries("items", &[ColumnSchema::new("state", next)]),
            Err(Error::TableFieldDefinitionMismatch { .. })
        ));
    }

    let table = database.table_schema("items").unwrap();
    assert_eq!(
        table
            .columns
            .iter()
            .find(|column| column.name == "state")
            .unwrap()
            .column_type,
        old_payload
    );
}

#[test]
fn enum_registry_identity_is_owned_by_each_physical_column_occurrence() {
    let supplied = ScalarEnumSchema::new("state", ["new", "done"])
        .unwrap()
        .with_registry_id(7);
    let table = TableSchema::new(
        "items",
        [
            ColumnSchema::new("a", ColumnType::EnumTag(supplied.clone())),
            ColumnSchema::new("b", ColumnType::EnumTag(supplied)),
        ],
    );
    assert_eq!(table.value_variant_registries.len(), 2);
    let ids = table
        .columns
        .iter()
        .map(|column| match &column.column_type {
            ColumnType::EnumTag(schema) => schema.registry_id(),
            _ => unreachable!(),
        })
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 2);
    assert!(!ids.contains(&7));
}

#[test]
fn ordinary_enum_registry_ids_cannot_claim_the_reserved_system_marker() {
    let supplied = ScalarEnumSchema::new("state", ["new", "done"])
        .unwrap()
        .with_registry_id(1 << 63);
    let table = TableSchema::new(
        "items",
        [
            ColumnSchema::new("a", ColumnType::EnumTag(supplied.clone())),
            ColumnSchema::new("b", ColumnType::EnumTag(supplied)),
        ],
    );
    let ids = table
        .columns
        .iter()
        .map(|column| match &column.column_type {
            ColumnType::EnumTag(schema) => schema.registry_id(),
            _ => unreachable!(),
        })
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| id & (1 << 63) == 0));
}

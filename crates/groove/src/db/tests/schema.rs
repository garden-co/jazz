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

const JAZZ_CLASS_V1_PHYSICAL_NAMES: [&str; 7] = [
    "__groove_class_history",
    "__groove_class_register",
    "__groove_class_global_current",
    "__groove_class_ahead_current",
    "__groove_class_changes",
    "__groove_class_indices",
    "__groove_class_meta",
];

fn named_table(name: &str) -> TableSchema {
    TableSchema::new(name, [ColumnSchema::new("id", ColumnType::U64)])
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

fn named_direct_store(name: &str) -> DirectRecordStoreSchema {
    DirectRecordStoreSchema::new(
        name,
        RecordDescriptor::new([("id", ValueType::U64)]),
        RecordDescriptor::new([("payload", ValueType::Bytes)]),
    )
}

fn assert_column_family_name_conflict<T>(
    result: Result<T, Error>,
    name: &str,
    existing_owner: &'static str,
    requested_owner: &'static str,
) {
    let error = match result {
        Ok(_) => panic!("conflicting column family {name} was admitted"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        format!(
            "column family name conflict for {name}: existing owner {existing_owner}, requested owner {requested_owner}"
        )
    );
}

fn controlled_storage_for(
    schema: &DatabaseSchema,
    layout: &StorageLayout,
) -> (TestStorage, crate::storage::TestStorageControl) {
    let logical_families = schema.column_families();
    let physical_families = layout.physical_column_families(logical_families.iter().copied());
    let physical_family_refs = physical_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    TestStorage::controlled(&physical_family_refs)
}

async fn assert_initial_column_family_conflict(
    schema: DatabaseSchema,
    layout: StorageLayout,
    name: &str,
    existing_owner: &'static str,
    requested_owner: &'static str,
) {
    let (storage, control) = controlled_storage_for(&schema, &layout);
    let result = Database::new_with_storage_layout(schema, storage, layout).await;

    assert_column_family_name_conflict(result, name, existing_owner, requested_owner);
    assert!(
        control.observed().is_empty(),
        "schema rejection touched storage: {:?}",
        control.observed()
    );
}

#[futures_test::test]
async fn schema_admission_rejects_duplicate_application_column_families_before_storage_io() {
    let duplicate_tables = DatabaseSchema::new([named_table("shared"), named_table("shared")]);
    assert_initial_column_family_conflict(
        duplicate_tables,
        StorageLayout::Identity,
        "shared",
        "table",
        "table",
    )
    .await;

    let duplicate_direct_stores = DatabaseSchema::new([])
        .with_direct_record_store(named_direct_store("shared"))
        .with_direct_record_store(named_direct_store("shared"));
    assert_initial_column_family_conflict(
        duplicate_direct_stores,
        StorageLayout::Identity,
        "shared",
        "direct record store",
        "direct record store",
    )
    .await;

    let table_and_direct_store = DatabaseSchema::new([named_table("shared")])
        .with_direct_record_store(named_direct_store("shared"));
    assert_initial_column_family_conflict(
        table_and_direct_store,
        StorageLayout::Identity,
        "shared",
        "table",
        "direct record store",
    )
    .await;
}

#[futures_test::test]
async fn schema_admission_reserves_large_value_metadata_before_storage_io() {
    for (schema, requested_owner) in [
        (
            DatabaseSchema::new([named_table(LARGE_VALUE_METADATA_CF)]),
            "table",
        ),
        (
            DatabaseSchema::new([])
                .with_direct_record_store(named_direct_store(LARGE_VALUE_METADATA_CF)),
            "direct record store",
        ),
    ] {
        assert_initial_column_family_conflict(
            schema,
            StorageLayout::Identity,
            LARGE_VALUE_METADATA_CF,
            "large-value metadata",
            requested_owner,
        )
        .await;
    }
}

#[futures_test::test]
async fn jazz_class_layout_reserves_every_exact_physical_family_before_marker_io() {
    for name in JAZZ_CLASS_V1_PHYSICAL_NAMES {
        assert_initial_column_family_conflict(
            DatabaseSchema::new([named_table(name)]),
            StorageLayout::jazz_class_v1(),
            name,
            "JazzClassV1 storage layout",
            "table",
        )
        .await;
    }
}

#[futures_test::test]
async fn live_table_registration_reserves_every_exact_jazz_class_physical_family() {
    let schema = DatabaseSchema::new([named_table("application")]);
    let layout = StorageLayout::jazz_class_v1();
    let (storage, control) = controlled_storage_for(&schema, &layout);
    let mut database = Database::new_with_storage_layout(schema, storage, layout)
        .await
        .unwrap();
    control.take_observed();

    for name in JAZZ_CLASS_V1_PHYSICAL_NAMES {
        let result = database.register_table(named_table(name));
        assert_column_family_name_conflict(result, name, "JazzClassV1 storage layout", "table");
        assert!(
            control.observed().is_empty(),
            "live schema rejection touched storage for {name}: {:?}",
            control.take_observed()
        );
    }
}

#[futures_test::test]
async fn identity_layout_permits_jazz_class_physical_names_at_open_and_registration() {
    let initial_schema = DatabaseSchema::new(JAZZ_CLASS_V1_PHYSICAL_NAMES.map(named_table));
    let initial_storage = MemoryStorage::new(&initial_schema.column_families());
    Database::new(initial_schema, initial_storage)
        .await
        .unwrap();

    let mut dynamic_families = JAZZ_CLASS_V1_PHYSICAL_NAMES.to_vec();
    dynamic_families.push(LARGE_VALUE_METADATA_CF);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&dynamic_families),
    )
    .await
    .unwrap();
    for name in JAZZ_CLASS_V1_PHYSICAL_NAMES {
        database.register_table(named_table(name)).unwrap();
    }
}

#[futures_test::test]
async fn distinct_table_and_direct_store_families_remain_valid() {
    let schema = DatabaseSchema::new([named_table("albums")])
        .with_direct_record_store(named_direct_store("artwork"));
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).await.unwrap();

    assert_eq!(database.table_schema("albums").unwrap().name, "albums");
    assert!(database.direct_record_store("artwork").is_ok());
}

#[futures_test::test]
async fn live_duplicate_table_keeps_the_existing_table_already_exists_error() {
    let schema = DatabaseSchema::new([named_table("albums")]);
    let (storage, control) = controlled_storage_for(&schema, &StorageLayout::Identity);
    let mut database = Database::new(schema, storage).await.unwrap();
    control.take_observed();

    assert!(matches!(
        database.register_table(named_table("albums")),
        Err(Error::TableAlreadyExists(name)) if name == "albums"
    ));
    assert!(
        control.observed().is_empty(),
        "duplicate live-table rejection touched storage: {:?}",
        control.observed()
    );
}

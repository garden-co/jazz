use super::*;
use std::collections::HashSet;
use uuid::Uuid;

fn test_row_provenance() -> crate::metadata::RowProvenance {
    crate::metadata::RowProvenance::for_insert("jazz:test", 1)
}

#[test]
fn column_type_fixed_sizes() {
    assert_eq!(ColumnType::Integer.fixed_size(), Some(4));
    assert_eq!(ColumnType::BigInt.fixed_size(), Some(8));
    assert_eq!(ColumnType::Boolean.fixed_size(), Some(1));
    assert_eq!(ColumnType::Timestamp.fixed_size(), Some(8));
    assert_eq!(ColumnType::Uuid.fixed_size(), Some(16));
    assert_eq!(ColumnType::BatchId.fixed_size(), Some(16));
    assert_eq!(ColumnType::Text.fixed_size(), None);
    assert_eq!(ColumnType::Bytea.fixed_size(), None);
    assert_eq!(
        ColumnType::Enum {
            variants: vec!["a".to_string()]
        }
        .fixed_size(),
        Some(1)
    );
}

#[test]
fn column_descriptor_builder() {
    let col = ColumnDescriptor::new("email", ColumnType::Text)
        .nullable()
        .references("users")
        .default(Value::Text("unknown@example.com".into()));

    assert_eq!(col.name, "email");
    assert_eq!(col.column_type, ColumnType::Text);
    assert!(col.nullable);
    assert_eq!(col.references, Some(TableName::new("users")));
    assert_eq!(col.default, Some(Value::Text("unknown@example.com".into())));
}

#[test]
fn column_descriptor_deserializes_payload_without_default() {
    let col: ColumnDescriptor = serde_json::from_str(
        r#"{
            "name":"email",
            "column_type":{"type":"Text"},
            "nullable":true,
            "references":"users"
        }"#,
    )
    .expect("deserialize column descriptor without default");

    assert_eq!(col.name, "email");
    assert_eq!(col.column_type, ColumnType::Text);
    assert!(col.nullable);
    assert_eq!(col.references, Some(TableName::new("users")));
    assert_eq!(col.default, None);
}

#[test]
fn column_descriptor_deserializes_payload_with_default() {
    let col: ColumnDescriptor = serde_json::from_str(
        r#"{
            "name":"email",
            "column_type":{"type":"Text"},
            "nullable":true,
            "references":"users",
            "default":{"type":"Text","value":"unknown@example.com"}
        }"#,
    )
    .expect("deserialize column descriptor without default");

    assert_eq!(col.name, "email");
    assert_eq!(col.column_type, ColumnType::Text);
    assert!(col.nullable);
    assert_eq!(col.references, Some(TableName::new("users")));
    assert_eq!(col.default, Some(Value::Text("unknown@example.com".into())));
}

#[test]
fn column_descriptor_deserializes_payload_with_merge_strategy() {
    let col: ColumnDescriptor = serde_json::from_str(
        r#"{
            "name":"count",
            "column_type":{"type":"Integer"},
            "nullable":false,
            "merge_strategy":"Counter"
        }"#,
    )
    .expect("deserialize column descriptor with merge strategy");

    assert_eq!(col.name, "count");
    assert_eq!(col.column_type, ColumnType::Integer);
    assert!(!col.nullable);
    assert_eq!(col.merge_strategy, Some(ColumnMergeStrategy::Counter));
}

#[test]
fn row_descriptor_column_lookup() {
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Uuid),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("age", ColumnType::Integer),
    ]);

    assert_eq!(descriptor.column_index("id"), Some(0));
    assert_eq!(descriptor.column_index("name"), Some(1));
    assert_eq!(descriptor.column_index("age"), Some(2));
    assert_eq!(descriptor.column_index("unknown"), None);

    assert_eq!(descriptor.fixed_column_count(), 2); // id (uuid) + age (integer)
    assert_eq!(descriptor.variable_column_count(), 1); // name (text)
}

#[test]
fn column_type_row_serializes_columns_as_array() {
    let column_type = ColumnType::Row {
        columns: Box::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])),
    };

    let json = serde_json::to_value(&column_type).expect("serialize row column type");
    assert_eq!(json["type"], "Row");
    assert!(json["columns"].is_array());
    assert_eq!(json["columns"][0]["name"], "id");
    assert_eq!(json["columns"][1]["name"], "name");
}

#[test]
fn value_column_type() {
    assert_eq!(Value::Integer(42).column_type(), Some(ColumnType::Integer));
    assert_eq!(Value::BigInt(42).column_type(), Some(ColumnType::BigInt));
    assert_eq!(
        Value::Boolean(true).column_type(),
        Some(ColumnType::Boolean)
    );
    assert_eq!(
        Value::Text("hello".into()).column_type(),
        Some(ColumnType::Text)
    );
    assert_eq!(
        Value::Timestamp(123).column_type(),
        Some(ColumnType::Timestamp)
    );
    assert_eq!(
        Value::Uuid(crate::object::ObjectId::from_uuid(Uuid::nil())).column_type(),
        Some(ColumnType::Uuid)
    );
    assert_eq!(
        Value::BatchId([7; 16]).column_type(),
        Some(ColumnType::BatchId)
    );
    assert_eq!(
        Value::Bytea(vec![0, 1, 2, 3]).column_type(),
        Some(ColumnType::Bytea)
    );
    assert_eq!(Value::Null.column_type(), None);
}

#[test]
fn value_deserializes_timestamp_from_integral_float() {
    let value: Value = serde_json::from_str(r#"{"type":"Timestamp","value":1773285322816.0}"#)
        .expect("deserialize timestamp");

    assert_eq!(value, Value::Timestamp(1773285322816));
}

#[test]
fn value_rejects_fractional_float_timestamp() {
    let error = serde_json::from_str::<Value>(r#"{"type":"Timestamp","value":1.5}"#)
        .expect_err("fractional timestamp should be rejected");

    assert!(error.to_string().contains("timestamp must be an integer"));
}

#[test]
fn row_descriptor_hash_changes_when_merge_strategy_changes() {
    let lww = RowDescriptor::new(vec![ColumnDescriptor::new("count", ColumnType::Integer)]);
    let counter = RowDescriptor::new(vec![
        ColumnDescriptor::new("count", ColumnType::Integer)
            .merge_strategy(ColumnMergeStrategy::Counter),
    ]);

    assert_ne!(
        lww.content_hash(),
        counter.content_hash(),
        "changing only the merge strategy should change the schema hash"
    );
}

#[test]
fn combined_row_descriptor_single() {
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Uuid),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);

    let combined = CombinedRowDescriptor::single("users", descriptor);

    assert_eq!(combined.table_count(), 1);
    assert_eq!(combined.resolve_column("users", "id"), Some((0, 0)));
    assert_eq!(combined.resolve_column("users", "name"), Some((0, 1)));
    assert_eq!(combined.resolve_unqualified("name"), Some((0, 1)));
}

#[test]
fn combined_row_descriptor_join() {
    let users_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Uuid),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let posts_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Uuid),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Uuid),
    ]);

    let combined = CombinedRowDescriptor::new(
        vec!["users".to_string(), "posts".to_string()],
        vec![users_desc, posts_desc],
    );

    assert_eq!(combined.table_count(), 2);
    assert_eq!(combined.total_column_count(), 5);

    // Qualified lookups
    assert_eq!(combined.resolve_column("users", "id"), Some((0, 0)));
    assert_eq!(combined.resolve_column("users", "name"), Some((0, 1)));
    assert_eq!(combined.resolve_column("posts", "id"), Some((1, 0)));
    assert_eq!(combined.resolve_column("posts", "title"), Some((1, 1)));
    assert_eq!(combined.resolve_column("posts", "author_id"), Some((1, 2)));

    // Unqualified lookup (first match wins)
    // "id" exists in both tables, should return users.id
    assert_eq!(combined.resolve_unqualified("id"), Some((0, 0)));
    // "title" only exists in posts
    assert_eq!(combined.resolve_unqualified("title"), Some((1, 1)));
}

// ========================================================================
// Schema Hash Tests
// ========================================================================

#[test]
fn schema_hash_deterministic() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let hash1 = SchemaHash::compute(&schema);
    let hash2 = SchemaHash::compute(&schema);
    assert_eq!(hash1, hash2);
}

#[test]
fn schema_hash_column_order_sensitive() {
    // Schema with columns in different order should have a different hash so
    // physical row layouts can be bootstrapped precisely from catalogue schemas.
    let schema1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("age", ColumnType::Integer),
        )
        .build();

    // Build same schema with different column order
    let schema2: Schema = [(
        TableName::new("users"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("age", ColumnType::Integer),
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])),
    )]
    .into_iter()
    .collect();

    let hash1 = SchemaHash::compute(&schema1);
    let hash2 = SchemaHash::compute(&schema2);
    assert_ne!(hash1, hash2, "Column order should affect schema hash");
}

#[test]
fn schema_hash_table_order_independent() {
    // Build with tables in different orders
    let schema1 = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .table(TableSchema::builder("posts").column("id", ColumnType::Uuid))
        .build();

    let schema2 = SchemaBuilder::new()
        .table(TableSchema::builder("posts").column("id", ColumnType::Uuid))
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    let hash1 = SchemaHash::compute(&schema1);
    let hash2 = SchemaHash::compute(&schema2);
    assert_eq!(hash1, hash2, "Table order should not affect hash");
}

#[test]
fn schema_hash_enum_variant_order_independent() {
    let schema1 = SchemaBuilder::new()
        .table(TableSchema::builder("todos").column(
            "status",
            ColumnType::Enum {
                variants: vec![
                    "done".to_string(),
                    "in_progress".to_string(),
                    "todo".to_string(),
                ],
            },
        ))
        .build();

    let schema2 = SchemaBuilder::new()
        .table(TableSchema::builder("todos").column(
            "status",
            ColumnType::Enum {
                variants: vec![
                    "todo".to_string(),
                    "done".to_string(),
                    "in_progress".to_string(),
                ],
            },
        ))
        .build();

    let hash1 = SchemaHash::compute(&schema1);
    let hash2 = SchemaHash::compute(&schema2);
    assert_eq!(hash1, hash2, "Enum variant order should not affect hash");
}

#[test]
fn schema_hash_different_schemas() {
    let schema1 = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    let schema2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let hash1 = SchemaHash::compute(&schema1);
    let hash2 = SchemaHash::compute(&schema2);
    assert_ne!(
        hash1, hash2,
        "Different schemas should have different hashes"
    );
}

#[test]
fn schema_hash_ignores_policies() {
    let schema_without_policies = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("owner_id", ColumnType::Uuid),
        )
        .build();

    let schema_with_policies = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("owner_id", ColumnType::Uuid)
                .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                    "owner_id",
                    vec!["user_id".to_string()],
                ))),
        )
        .build();

    assert_eq!(
        SchemaHash::compute(&schema_without_policies),
        SchemaHash::compute(&schema_with_policies),
        "Policy-only changes should not affect schema identity",
    );
}

#[test]
fn schema_hash_preserves_historical_default_index_behavior() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build();

    assert_eq!(
        SchemaHash::compute(&schema).to_string(),
        "bfd77d25b0696da75df2ca82ab129c6289432decaaad8b86adcb31a366bdd217",
    );
}

#[test]
fn schema_hash_changes_when_indexed_columns_override_changes() {
    let default_indexes = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build();

    let explicit_subset = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .index_only(["done"]),
        )
        .build();

    assert_ne!(
        SchemaHash::compute(&default_indexes),
        SchemaHash::compute(&explicit_subset),
    );
}

#[test]
fn schema_hash_distinguishes_explicit_empty_index_override() {
    let schema_without_override = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build();

    let schema_with_empty_override = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .index_only(std::iter::empty::<&str>()),
        )
        .build();

    assert_ne!(
        SchemaHash::compute(&schema_without_override),
        SchemaHash::compute(&schema_with_empty_override),
        "an explicit empty index override changes indexing semantics",
    );
}

#[test]
fn schema_hash_changes_when_column_default_changes() {
    let schema1 = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("role", ColumnType::Text))
        .build();

    let schema2 = SchemaBuilder::new()
        .table(TableSchema::builder("users").column_with_default(
            "role",
            ColumnType::Text,
            Value::Text("member".into()),
        ))
        .build();

    let hash1 = SchemaHash::compute(&schema1);
    let hash2 = SchemaHash::compute(&schema2);

    assert_ne!(
        hash1, hash2,
        "Changing only a column default should change the schema hash"
    );
}

#[test]
fn schema_hash_short() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    let hash = SchemaHash::compute(&schema);
    let short = hash.short();

    assert_eq!(short.len(), 12, "Short hash should be 12 hex chars");
    assert!(short.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn schema_hash_to_object_id_deterministic() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    let hash = SchemaHash::compute(&schema);
    let oid1 = hash.to_object_id();
    let oid2 = hash.to_object_id();

    assert_eq!(oid1, oid2, "Same hash should produce same ObjectId");
}

#[test]
fn schema_hash_to_object_id_different_hashes() {
    let schema1 = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();
    let schema2 = SchemaBuilder::new()
        .table(TableSchema::builder("posts").column("id", ColumnType::Uuid))
        .build();

    let hash1 = SchemaHash::compute(&schema1);
    let hash2 = SchemaHash::compute(&schema2);

    assert_ne!(
        hash1.to_object_id(),
        hash2.to_object_id(),
        "Different hashes should produce different ObjectIds"
    );
}

#[test]
fn table_schema_builder() {
    let (name, schema) = TableSchema::builder("users")
        .column("id", ColumnType::Uuid)
        .column_with_default("role", ColumnType::Text, Value::Text("member".into()))
        .nullable_column("email", ColumnType::Text)
        .fk_column("org_id", "orgs")
        .nullable_fk_column("manager_id", "users")
        .build_named();

    assert_eq!(name.as_str(), "users");
    assert_eq!(schema.columns.columns.len(), 5);

    let id_col = schema.columns.column("id").unwrap();
    assert_eq!(id_col.column_type, ColumnType::Uuid);
    assert!(!id_col.nullable);

    let role_col = schema.columns.column("role").unwrap();
    assert_eq!(role_col.column_type, ColumnType::Text);
    assert_eq!(role_col.default, Some(Value::Text("member".into())));
    assert!(!role_col.nullable);

    let email_col = schema.columns.column("email").unwrap();
    assert_eq!(email_col.column_type, ColumnType::Text);
    assert!(email_col.nullable);

    let org_col = schema.columns.column("org_id").unwrap();
    assert_eq!(org_col.column_type, ColumnType::Uuid);
    assert!(!org_col.nullable);
    assert_eq!(org_col.references, Some(TableName::new("orgs")));

    let manager_col = schema.columns.column("manager_id").unwrap();
    assert!(manager_col.nullable);
    assert_eq!(manager_col.references, Some(TableName::new("users")));
}

#[test]
fn row_descriptor_content_hash() {
    let desc1 = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Uuid),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);

    let desc2 = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("id", ColumnType::Uuid),
    ]);

    // Same columns, different order -> different hash because physical layout changes.
    assert_ne!(desc1.content_hash(), desc2.content_hash());

    // Different columns -> different hash
    let desc3 = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Uuid)]);
    assert_ne!(desc1.content_hash(), desc3.content_hash());

    let desc4 = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Uuid),
        ColumnDescriptor::new("name", ColumnType::Text).default(Value::Text("anonymous".into())),
    ]);
    assert_ne!(
        desc1.content_hash(),
        desc4.content_hash(),
        "Column defaults should affect row descriptor content hash"
    );
}

#[test]
fn cloned_row_descriptor_recomputes_content_hash_after_mutation() {
    let desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Uuid),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let original_hash = desc.content_hash();

    let mut cloned = desc.clone();
    cloned
        .columns
        .push(ColumnDescriptor::new("$canEdit", ColumnType::Boolean));

    assert_ne!(
        original_hash,
        cloned.content_hash(),
        "A cloned descriptor must not reuse a stale cached hash after its columns change"
    );
}

// ========================================================================
// ComposedBranchName Tests
// ========================================================================

#[test]
fn composed_branch_name_format() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    let composed = ComposedBranchName::from_schema("dev", &schema, "main");
    let branch_name = composed.to_branch_name();
    let s = branch_name.as_str();

    // Should be in format: dev-XXXXXXXX-main
    assert!(s.starts_with("dev-"));
    assert!(s.ends_with("-main"));
    assert_eq!(s.matches('-').count(), 2);
}

#[test]
fn composed_branch_name_parse() {
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    let original = ComposedBranchName::from_schema("prod", &schema, "feature-x");
    let branch_name = original.to_branch_name();
    let parsed = ComposedBranchName::parse(&branch_name).unwrap();

    assert_eq!(parsed.env, "prod");
    assert_eq!(parsed.user_branch, "feature-x");
    // Note: full hash can't be recovered from 12 chars, but short() should match
    assert_eq!(parsed.schema_hash.short(), original.schema_hash.short());
}

#[test]
fn composed_branch_name_parse_invalid() {
    use crate::object::BranchName;

    // Too few parts
    let name = BranchName::new("just-one");
    assert!(ComposedBranchName::parse(&name).is_none());

    // Hash not 12 chars
    let name = BranchName::new("dev-abc-main");
    assert!(ComposedBranchName::parse(&name).is_none());

    // Hash not hex
    let name = BranchName::new("dev-gggggggggggg-main");
    assert!(ComposedBranchName::parse(&name).is_none());
}

#[test]
fn composed_branch_name_matches() {
    let hash = SchemaHash::from_bytes([0xab; 32]);
    let composed = ComposedBranchName::new("dev", hash, "main");

    assert!(composed.matches_env_and_branch("dev", "main"));
    assert!(!composed.matches_env_and_branch("prod", "main"));
    assert!(!composed.matches_env_and_branch("dev", "feature"));
}

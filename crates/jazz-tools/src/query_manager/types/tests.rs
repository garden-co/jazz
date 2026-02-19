use super::*;
use std::collections::HashSet;
use uuid::Uuid;

#[test]
fn column_type_fixed_sizes() {
    assert_eq!(ColumnType::Integer.fixed_size(), Some(4));
    assert_eq!(ColumnType::BigInt.fixed_size(), Some(8));
    assert_eq!(ColumnType::Boolean.fixed_size(), Some(1));
    assert_eq!(ColumnType::Timestamp.fixed_size(), Some(8));
    assert_eq!(ColumnType::Uuid.fixed_size(), Some(16));
    assert_eq!(ColumnType::Text.fixed_size(), None);
}

#[test]
fn column_descriptor_builder() {
    let col = ColumnDescriptor::new("email", ColumnType::Text)
        .nullable()
        .references("users");

    assert_eq!(col.name, "email");
    assert_eq!(col.column_type, ColumnType::Text);
    assert!(col.nullable);
    assert_eq!(col.references, Some(TableName::new("users")));
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
    assert_eq!(Value::Null.column_type(), None);
}

// ========================================================================
// Tuple Model Tests
// ========================================================================

fn make_commit_id(n: u8) -> crate::commit::CommitId {
    crate::commit::CommitId([n; 32])
}

#[test]
fn tuple_element_id() {
    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));
    let elem = TupleElement::Id(id);

    assert_eq!(elem.id(), id);
    assert!(!elem.is_materialized());
    assert!(elem.content().is_none());
    assert!(elem.commit_id().is_none());
}

#[test]
fn tuple_element_row() {
    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));
    let content = vec![1, 2, 3];
    let commit_id = make_commit_id(1);
    let elem = TupleElement::Row {
        id,
        content: content.clone(),
        commit_id,
    };

    assert_eq!(elem.id(), id);
    assert!(elem.is_materialized());
    assert_eq!(elem.content(), Some(content.as_slice()));
    assert_eq!(elem.commit_id(), Some(commit_id));
}

#[test]
fn tuple_element_from_row() {
    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));
    let row = Row::new(id, vec![1, 2, 3], make_commit_id(1));
    let elem = TupleElement::from_row(&row);

    assert_eq!(elem.id(), id);
    assert!(elem.is_materialized());
    assert_eq!(elem.content(), Some(&[1u8, 2, 3][..]));
}

#[test]
fn tuple_from_id() {
    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));
    let tuple = Tuple::from_id(id);

    assert_eq!(tuple.len(), 1);
    assert_eq!(tuple.first_id(), Some(id));
    assert!(!tuple.is_fully_materialized());
}

#[test]
fn tuple_from_row() {
    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));
    let row = Row::new(id, vec![1, 2, 3], make_commit_id(1));
    let tuple = Tuple::from_row(&row);

    assert_eq!(tuple.len(), 1);
    assert_eq!(tuple.first_id(), Some(id));
    assert!(tuple.is_fully_materialized());
}

#[test]
fn tuple_equality_based_on_ids() {
    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));

    // Two tuples with same ID but different content should be equal
    let tuple1 = Tuple::from_id(id);
    let tuple2 = Tuple::new(vec![TupleElement::Row {
        id,
        content: vec![1, 2, 3],
        commit_id: make_commit_id(1),
    }]);

    assert_eq!(tuple1, tuple2);
}

#[test]
fn tuple_hash_based_on_ids() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));

    let tuple1 = Tuple::from_id(id);
    let tuple2 = Tuple::new(vec![TupleElement::Row {
        id,
        content: vec![1, 2, 3],
        commit_id: make_commit_id(1),
    }]);

    let mut hasher1 = DefaultHasher::new();
    let mut hasher2 = DefaultHasher::new();
    tuple1.hash(&mut hasher1);
    tuple2.hash(&mut hasher2);

    assert_eq!(hasher1.finish(), hasher2.finish());
}

#[test]
fn tuple_in_hashset() {
    let id1 = crate::object::ObjectId::from_uuid(Uuid::from_u128(1));
    let id2 = crate::object::ObjectId::from_uuid(Uuid::from_u128(2));

    let mut set = HashSet::new();
    set.insert(Tuple::from_id(id1));
    set.insert(Tuple::from_id(id2));

    // Same ID with different content should be found
    let tuple_with_content = Tuple::new(vec![TupleElement::Row {
        id: id1,
        content: vec![1, 2, 3],
        commit_id: make_commit_id(1),
    }]);
    assert!(set.contains(&tuple_with_content));
}

#[test]
fn tuple_delta_to_row_delta() {
    let id = crate::object::ObjectId::from_uuid(Uuid::from_u128(42));
    let row = Row::new(id, vec![1, 2, 3], make_commit_id(1));
    let tuple = Tuple::from_row(&row);

    let tuple_delta = TupleDelta {
        added: vec![tuple],
        removed: vec![],
        updated: vec![],
    };

    let row_delta = tuple_delta.to_row_delta().unwrap();
    assert_eq!(row_delta.added.len(), 1);
    assert_eq!(row_delta.added[0].id, id);
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
// TupleDescriptor Tests
// ========================================================================

#[test]
fn tuple_descriptor_single_table() {
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);

    let td = TupleDescriptor::single("users", descriptor);

    assert_eq!(td.element_count(), 1);
    assert_eq!(td.total_columns(), 2);
    assert_eq!(td.resolve_column(0), Some((0, 0))); // column 0 -> element 0, local 0
    assert_eq!(td.resolve_column(1), Some((0, 1))); // column 1 -> element 0, local 1
    assert_eq!(td.resolve_column(2), None); // out of range
}

#[test]
fn tuple_descriptor_join() {
    let users_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let posts_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
    ]);

    let td = TupleDescriptor::from_tables(&[
        ("users".to_string(), users_desc),
        ("posts".to_string(), posts_desc),
    ]);

    assert_eq!(td.element_count(), 2);
    assert_eq!(td.total_columns(), 5);

    // users columns (0-1)
    assert_eq!(td.resolve_column(0), Some((0, 0))); // users.id
    assert_eq!(td.resolve_column(1), Some((0, 1))); // users.name

    // posts columns (2-4)
    assert_eq!(td.resolve_column(2), Some((1, 0))); // posts.id
    assert_eq!(td.resolve_column(3), Some((1, 1))); // posts.title
    assert_eq!(td.resolve_column(4), Some((1, 2))); // posts.author_id

    assert_eq!(td.resolve_column(5), None); // out of range
}

#[test]
fn tuple_descriptor_elements_for_columns() {
    let users_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let posts_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
    ]);

    let td = TupleDescriptor::from_tables(&[
        ("users".to_string(), users_desc),
        ("posts".to_string(), posts_desc),
    ]);

    // Only need users.id (column 0) -> need element 0 only
    let cols: HashSet<usize> = [0].into_iter().collect();
    let elements = td.elements_for_columns(&cols);
    assert_eq!(elements, [0].into_iter().collect());

    // Only need posts.title (column 3) -> need element 1 only
    let cols: HashSet<usize> = [3].into_iter().collect();
    let elements = td.elements_for_columns(&cols);
    assert_eq!(elements, [1].into_iter().collect());

    // Need both users.name and posts.title -> need both elements
    let cols: HashSet<usize> = [1, 3].into_iter().collect();
    let elements = td.elements_for_columns(&cols);
    assert_eq!(elements, [0, 1].into_iter().collect());
}

#[test]
fn tuple_descriptor_combined_descriptor() {
    let users_desc = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Integer)]);
    let posts_desc = RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)]);

    let td = TupleDescriptor::from_tables(&[
        ("users".to_string(), users_desc),
        ("posts".to_string(), posts_desc),
    ]);

    let combined = td.combined_descriptor();
    assert_eq!(combined.columns.len(), 2);
    assert_eq!(combined.columns[0].name, "id");
    assert_eq!(combined.columns[1].name, "title");
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
fn schema_hash_column_order_independent() {
    // Schema with columns in different order should have same hash
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
    assert_eq!(hash1, hash2, "Column order should not affect hash");
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
        .nullable_column("email", ColumnType::Text)
        .fk_column("org_id", "orgs")
        .nullable_fk_column("manager_id", "users")
        .build_named();

    assert_eq!(name.as_str(), "users");
    assert_eq!(schema.descriptor.columns.len(), 4);

    let id_col = schema.descriptor.column("id").unwrap();
    assert_eq!(id_col.column_type, ColumnType::Uuid);
    assert!(!id_col.nullable);

    let email_col = schema.descriptor.column("email").unwrap();
    assert_eq!(email_col.column_type, ColumnType::Text);
    assert!(email_col.nullable);

    let org_col = schema.descriptor.column("org_id").unwrap();
    assert_eq!(org_col.column_type, ColumnType::Uuid);
    assert!(!org_col.nullable);
    assert_eq!(org_col.references, Some(TableName::new("orgs")));

    let manager_col = schema.descriptor.column("manager_id").unwrap();
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

    // Same columns, different order -> same hash (order-independent)
    assert_eq!(desc1.content_hash(), desc2.content_hash());

    // Different columns -> different hash
    let desc3 = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Uuid)]);
    assert_ne!(desc1.content_hash(), desc3.content_hash());
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

use crate::JazzClient;

use super::*;

fn insert_file(client: &JazzClient, owner_id: &str, name: &str) -> ObjectId {
    client
        .insert(
            "files",
            crate::row_input!("owner_id" => owner_id, "name" => name),
        )
        .expect("insert file")
        .0
}

fn insert_todo_with_image(
    client: &JazzClient,
    owner_id: &str,
    title: &str,
    image: impl Into<Value>,
) -> ObjectId {
    let image = image.into();
    client
        .insert(
            "todos",
            crate::row_input!("owner_id" => owner_id, "title" => title, "image" => image),
        )
        .expect("insert todo")
        .0
}

fn insert_todo_with_images(
    client: &JazzClient,
    owner_id: &str,
    title: &str,
    images: Vec<Value>,
) -> ObjectId {
    client
        .insert(
            "todos",
            crate::row_input!(
                "owner_id" => owner_id,
                "title" => title,
                "images" => Value::Array(images),
            ),
        )
        .expect("insert todo")
        .0
}

async fn query_ids_as(client: &JazzClient, table: &str, user_id: &str) -> HashSet<ObjectId> {
    client
        .for_session(Session::new(user_id))
        .query(QueryBuilder::new(table).build(), None)
        .await
        .expect("query rows")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// Verifies that declared reverse-FK inheritance can grant SELECT on a target
/// row when the current session owns a row that references it.
#[tokio::test]
async fn rebac_declared_fk_inheritance_grants_select_access() {
    let schema = declared_file_inheritance_schema(false);
    let client = JazzClient::test_client(schema).await;

    let file_id = insert_file(&client, "bob", "bob-file");
    let _todo_id = insert_todo_with_image(&client, "alice", "todo", file_id);

    let visible_ids = query_ids_as(&client, "files", "alice").await;

    assert!(
        visible_ids.contains(&file_id),
        "alice should see file via allowedTo.readReferencing(policy.todos, \"image\")"
    );
}

/// Verifies that declared reverse-FK inheritance can grant UPDATE on a target
/// row through a visible referencing row.
#[tokio::test]
async fn rebac_declared_fk_inheritance_grants_update_access() {
    let schema = declared_file_inheritance_schema(false);
    let client = JazzClient::test_client(schema).await;

    let file_id = insert_file(&client, "bob", "bob-file");
    let _todo_id = insert_todo_with_image(&client, "alice", "todo", file_id);

    let update = client.for_session(Session::new("alice")).update(
        file_id,
        vec![
            ("owner_id".into(), Value::Text("bob".into())),
            ("name".into(), Value::Text("updated by alice".into())),
        ],
    );
    assert!(
        update.is_ok(),
        "alice should update file via declared inherited access from todos row"
    );
}

/// Verifies that declared reverse-FK inheritance also works for UUID-array
/// reference columns, including duplicate target ids in the array.
#[tokio::test]
async fn rebac_declared_fk_inheritance_array_membership_grants_access() {
    let schema = declared_file_inheritance_schema(true);
    let client = JazzClient::test_client(schema).await;

    let file_id = insert_file(&client, "bob", "array-file");
    let _todo_id = insert_todo_with_images(
        &client,
        "alice",
        "todo",
        vec![Value::Uuid(file_id), Value::Uuid(file_id)],
    );

    let visible_ids = query_ids_as(&client, "files", "alice").await;

    assert!(
        visible_ids.contains(&file_id),
        "array FK membership should grant inherited access when target id is present"
    );
}

/// Verifies that cyclic declared reverse-FK inheritance fails closed instead
/// of recursively granting access through the cycle.
#[tokio::test]
async fn rebac_declared_fk_inheritance_cycle_fails_closed() {
    let a_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session("user_id")),
            pe::allowed_to_read_referencing("table_b", "a_id"),
        ]));
    });
    let b_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session("user_id")),
            pe::allowed_to_read_referencing("table_a", "b_id"),
        ]));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("table_a")
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("b_id", "table_b")
                .policies(a_policies),
        )
        .table(
            TableSchema::builder("table_b")
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("a_id", "table_a")
                .policies(b_policies),
        )
        .build();
    let client = JazzClient::test_client(schema).await;

    let a_id = client
        .insert(
            "table_a",
            crate::row_input!("owner_id" => "bob", "b_id" => Value::Null),
        )
        .expect("insert table_a")
        .0;
    let b_id = client
        .insert(
            "table_b",
            crate::row_input!("owner_id" => "carol", "a_id" => a_id),
        )
        .expect("insert table_b")
        .0;

    client
        .update(a_id, vec![("b_id".into(), Value::Uuid(b_id))])
        .expect("link table_a");

    let visible_ids = query_ids_as(&client, "table_a", "alice").await;

    assert!(
        visible_ids.is_empty(),
        "cycle path should fail closed and not grant access"
    );
}

/// Verifies that access through a declared reverse-FK path is re-evaluated
/// when the referencing FK column changes from NULL to a target id.
#[tokio::test]
async fn rebac_declared_fk_inheritance_reacts_to_fk_updates() {
    let schema = declared_file_inheritance_schema(false);
    let client = JazzClient::test_client(schema).await;

    let file_id = insert_file(&client, "bob", "delayed-link");
    let todo_id = insert_todo_with_image(&client, "alice", "todo", Value::Null);

    let initially_visible = query_ids_as(&client, "files", "alice").await;
    assert!(
        !initially_visible.contains(&file_id),
        "file should be hidden before an inheriting reference exists"
    );

    client
        .update(todo_id, vec![("image".into(), Value::Uuid(file_id))])
        .expect("link todo image");

    let visible_after_link = query_ids_as(&client, "files", "alice").await;
    assert!(
        visible_after_link.contains(&file_id),
        "updating referencing FK should re-evaluate and grant access to linked target row"
    );
}

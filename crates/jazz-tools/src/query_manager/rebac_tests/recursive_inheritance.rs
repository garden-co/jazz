use crate::JazzClient;

use super::*;

fn insert_folder(
    client: &JazzClient,
    owner_id: &str,
    name: &str,
    parent_id: Option<ObjectId>,
) -> ObjectId {
    client
        .insert(
            "folders",
            crate::row_input!("owner_id" => owner_id, "name" => name, "parent_id" => parent_id),
            None,
        )
        .expect("insert folder")
        .0
}

async fn query_folder_ids_as(client: &JazzClient, user_id: &str) -> HashSet<ObjectId> {
    client
        .for_session(Session::new(user_id))
        .query(QueryBuilder::new("folders").build(), None)
        .await
        .expect("query folders")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

async fn query_folder_name_as(
    client: &JazzClient,
    user_id: &str,
    folder_id: ObjectId,
) -> Option<String> {
    client
        .for_session(Session::new(user_id))
        .query(
            QueryBuilder::new("folders")
                .filter_eq("id", Value::Uuid(folder_id))
                .select(&["name"])
                .build(),
            None,
        )
        .await
        .expect("query folders")
        .first()
        .map(|(_, values)| {
            let Some(Value::Text(name)) = values.first() else {
                panic!("folder name should be selected as text");
            };
            name.clone()
        })
}

#[tokio::test]
async fn rebac_recursive_inherits_allows_ancestor_access() {
    let schema = recursive_folders_schema(None);
    let client = JazzClient::test_client(schema).await;

    let root = insert_folder(&client, "alice", "Root", None);
    let child = insert_folder(&client, "bob", "Child", Some(root));
    let grand = insert_folder(&client, "carol", "Grandchild", Some(child));

    let result_ids = query_folder_ids_as(&client, "alice").await;

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible via recursive INHERITS"
    );
    assert!(
        result_ids.contains(&grand),
        "Grandchild should be visible via recursive INHERITS"
    );
}

#[tokio::test]
async fn rebac_recursive_inherits_respects_depth_override() {
    let schema = recursive_folders_schema(Some(1));
    let client = JazzClient::test_client(schema).await;

    let root = insert_folder(&client, "alice", "Root", None);
    let child = insert_folder(&client, "bob", "Child", Some(root));
    let grand = insert_folder(&client, "carol", "Grandchild", Some(child));

    let result_ids = query_folder_ids_as(&client, "alice").await;

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible at depth=1"
    );
    assert!(
        !result_ids.contains(&grand),
        "Grandchild should be hidden when max_depth=1"
    );
}

async fn run_recursive_folder_update(max_depth: Option<usize>) -> (bool, bool) {
    let schema = recursive_folders_schema(max_depth);
    let client = JazzClient::test_client(schema).await;

    let root = insert_folder(&client, "alice", "Root", None);
    let child = insert_folder(&client, "bob", "Child", Some(root));
    let grand = insert_folder(&client, "bob", "Grandchild", Some(child));

    let result = client.for_session(Session::new("alice")).update(
        grand,
        vec![("name".to_string(), Value::Text("Renamed by Alice".into()))],
    );

    let name = query_folder_name_as(&client, "bob", grand)
        .await
        .expect("bob should be able to see his folder");

    (result.is_err(), name == "Renamed by Alice")
}

#[tokio::test]
async fn rebac_recursive_inherits_write_checks_allow_and_deny() {
    let (denied_shallow, applied_shallow) = run_recursive_folder_update(Some(1)).await;
    assert!(
        denied_shallow,
        "Update should be denied when recursive INHERITS max depth is too shallow"
    );
    assert!(
        !applied_shallow,
        "Denied update must not be applied to the row"
    );

    let (denied_deep, applied_deep) = run_recursive_folder_update(Some(2)).await;
    assert!(
        !denied_deep,
        "Update should be allowed when max depth reaches the ancestor owner"
    );
    assert!(applied_deep, "Allowed update should be applied");
}

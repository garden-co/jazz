//! Documentation snippet sources compiled with the example crate.
#![allow(dead_code)]

use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use jazz_tools::query_manager::policy::{Operation, PolicyExpr};
use jazz_tools::query_manager::types::TablePolicies;
use jazz_tools::{
    DurabilityTier, JazzClient, ObjectId, QueryBuilder, Session, SessionClient, Value,
};
use serde_json::json;

fn verify_jwt_and_extract_claims(_token: &str) -> (String, serde_json::Value) {
    // Replace with your auth provider's JWT verification logic.
    ("replace-with-verified-sub".to_string(), json!({}))
}

fn todo_values(
    title: impl Into<String>,
    description: impl Into<String>,
) -> std::collections::HashMap<String, Value> {
    jazz_tools::row_input!("title" => title.into(), "done" => false, "description" => description.into())
}

// #region backend-request-session-rust
pub fn requester_session_from_headers(headers: &HeaderMap) -> Result<Session, StatusCode> {
    let auth = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = auth
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let (user_id, claims) = verify_jwt_and_extract_claims(token);
    Ok(Session::new(user_id).with_claims(claims))
}
// #endregion backend-request-session-rust

// #region backend-request-scoped-client-rust
pub fn scoped_client_for_session<'a>(
    client: &'a JazzClient,
    session: Session,
) -> SessionClient<'a> {
    client.for_session(session)
}
// #endregion backend-request-scoped-client-rust

// #region backend-request-handler-rust
pub async fn list_todos_for_request(
    headers: &HeaderMap,
    client: &JazzClient,
) -> Result<usize, StatusCode> {
    let user_client = client.for_session(requester_session_from_headers(headers)?);
    let query = QueryBuilder::new("todos").build();
    let rows = user_client
        .query(query, None)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(rows.len())
}
// #endregion backend-request-handler-rust

// #region permissions-simple-rust
pub fn simple_owner_policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_update(
            Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        )
}
// #endregion permissions-simple-rust

// #region permissions-inherits-rust
pub fn inherits_select_policy() -> TablePolicies {
    TablePolicies::new().with_select(PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::inherits(Operation::Select, "folder_id"),
    ]))
}
// #endregion permissions-inherits-rust

// #region permissions-combinators-rust
pub fn combinator_policy() -> TablePolicies {
    TablePolicies::new().with_select(PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::and(vec![
            PolicyExpr::True,
            PolicyExpr::inherits(Operation::Select, "project"),
        ]),
    ]))
}
// #endregion permissions-combinators-rust

// #region permissions-recursive-inherits-rust
pub fn recursive_inherits_policy() -> TablePolicies {
    TablePolicies::new().with_select(PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::inherits_with_depth(Operation::Select, "parent", 10),
    ]))
}
// #endregion permissions-recursive-inherits-rust

// #region reading-oneshot-rust
pub async fn read_todos_oneshot(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos").build();
    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-oneshot-rust

// #region reading-subscriptions-rust
pub async fn subscribe_todos(
    client: &JazzClient,
) -> jazz_tools::Result<jazz_tools::SubscriptionStream> {
    let query = QueryBuilder::new("todos").build();
    client.subscribe(query).await
}
// #endregion reading-subscriptions-rust

// #region reading-durability-tier-rust
pub async fn read_todos_at_edge_durability(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos").build();
    let rows = client
        .query(query, Some(DurabilityTier::EdgeServer))
        .await?;
    Ok(rows.len())
}
// #endregion reading-durability-tier-rust

// #region reading-filters-rust
pub async fn read_todos_with_filters(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .build();

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-filters-rust

// #region reading-sorting-rust
pub async fn read_todos_sorted(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .order_by("title")
        .build();

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-sorting-rust

// #region reading-pagination-rust
pub async fn read_todo_page(
    client: &JazzClient,
    page_size: usize,
    page: usize,
) -> jazz_tools::Result<usize> {
    let offset = page.saturating_sub(1) * page_size;
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .order_by("title")
        .limit(page_size)
        .offset(offset)
        .build();

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-pagination-rust

// #region reading-includes-rust
pub async fn read_todos_with_project(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .join("projects")
        .on("todos.project_id", "projects._id")
        .build();

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-includes-rust

// #region reading-reverse-relation-rust
pub fn build_projects_with_todos_query() -> jazz_tools::Query {
    QueryBuilder::new("projects")
        .with_array("todos_via_project", |sub| {
            sub.from("todos")
                .correlate("project_id", "_id")
                .filter_eq("done", Value::Boolean(false))
        })
        .build()
}
// #endregion reading-reverse-relation-rust

// #region reading-require-includes-rust
pub fn build_todos_with_required_project() -> jazz_tools::Query {
    QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .with_array("project", |sub| {
            sub.from("projects")
                .correlate("_id", "project_id")
                .require_result()
        })
        .build()
}
// #endregion reading-require-includes-rust

// #region reading-select-rust
pub async fn read_todo_titles(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos")
        .select(&["title", "done"])
        .build();

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-select-rust

// #region reading-magic-columns-rust
pub async fn read_todos_with_permissions(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos")
        .select(&["title", "$canRead", "$canEdit", "$canDelete"])
        .build();

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-magic-columns-rust

// #region reading-recursive-rust
pub fn build_todo_lineage_query() -> jazz_tools::Query {
    QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .with_recursive(|r| {
            r.from("todos")
                .correlate("id", "parent_id")
                .hop("todos", "parent_id")
                .max_depth(10)
        })
        .build()
}
// #endregion reading-recursive-rust

// #region writing-crud-rust
pub async fn write_todo_crud(client: &JazzClient, existing_id: ObjectId) -> jazz_tools::Result<()> {
    let values = todo_values("Write docs", "");

    let _new_row = client.create("todos", values).await?;
    client
        .update(
            existing_id,
            vec![("done".to_string(), Value::Boolean(true))],
        )
        .await?;
    client.delete(existing_id).await?;
    Ok(())
}
// #endregion writing-crud-rust

// #region writing-durability-tier-rust
pub async fn write_todo_with_default_durability(
    client: &JazzClient,
) -> jazz_tools::Result<ObjectId> {
    let (id, _row_values) = client
        .create(
            "todos",
            todo_values("Write docs with default durability behavior", ""),
        )
        .await?;

    // Rust currently does not expose per-write durability tier arguments.
    // Writes apply locally first, then sync asynchronously to higher tiers.
    Ok(id)
}
// #endregion writing-durability-tier-rust

pub async fn where_operator_examples(client: &JazzClient) -> jazz_tools::Result<()> {
    let search_term = "milk";

    // #region where-eq-ne-rust
    // Exact match
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .build();
    let incomplete_todos = client.query(query, None).await?;

    // Not equal
    let query = QueryBuilder::new("todos")
        .filter_ne("title", Value::Text("Draft".into()))
        .build();
    let non_draft_todos = client.query(query, None).await?;
    // #endregion where-eq-ne-rust

    // #region where-numeric-rust
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let one_week_ago = Value::Timestamp(now_ms - 7 * 24 * 60 * 60 * 1000);

    let query = QueryBuilder::new("todos")
        .filter_gt("created_at", one_week_ago)
        .build();
    let recent_todos = client.query(query, None).await?;

    let query = QueryBuilder::new("todos")
        .filter_ge("priority", Value::Integer(3))
        .build();
    let high_priority = client.query(query, None).await?;

    let query = QueryBuilder::new("todos")
        .filter_lt("priority", Value::Integer(10))
        .build();
    let low_priority = client.query(query, None).await?;
    // #endregion where-numeric-rust

    // #region where-contains-rust
    // Substring match (case-sensitive)
    let query = QueryBuilder::new("todos")
        .filter_contains("title", Value::Text(search_term.into()))
        .build();
    let matches = client.query(query, None).await?;
    // #endregion where-contains-rust

    // #region where-null-rust
    // Rows where the optional ref is not set
    let query = QueryBuilder::new("todos").filter_is_null("parent").build();
    let unlinked_todos = client.query(query, None).await?;

    // Rows where it is set
    let query = QueryBuilder::new("todos")
        .filter_is_not_null("parent")
        .build();
    let linked_todos = client.query(query, None).await?;
    // #endregion where-null-rust

    // #region where-and-rust
    // Multiple filter calls are AND-combined
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(true))
        .filter_is_not_null("project")
        .build();
    let done_with_project = client.query(query, None).await?;
    // #endregion where-and-rust

    // #region where-order-limit-rust
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .order_by("created_at")
        .limit(50)
        .build();
    let recent_incomplete = client.query(query, None).await?;
    // #endregion where-order-limit-rust

    // #region where-subscription-rust
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .build();
    let pending = client.subscribe(query).await?;
    // #endregion where-subscription-rust

    let _ = (
        incomplete_todos,
        non_draft_todos,
        recent_todos,
        high_priority,
        low_priority,
        matches,
        unlinked_todos,
        linked_todos,
        done_with_project,
        recent_incomplete,
        pending,
    );
    Ok(())
}

// #region reading-composing-queries-rust
pub fn composing_queries() {
    // Build two views from the same base conditions.
    let by_title = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .order_by("title")
        .limit(20)
        .build();
    let by_newest = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .order_by_desc("id")
        .build();

    let _ = (by_title, by_newest);
}
// #endregion reading-composing-queries-rust

// #region writing-nullable-update-rust
pub async fn clear_nullable_fields(
    client: &JazzClient,
    todo_id: ObjectId,
) -> jazz_tools::Result<()> {
    // Set a nullable column to null
    client
        .update(todo_id, vec![("owner_id".to_string(), Value::Null)])
        .await?;

    // Only the specified columns are changed; omitted columns are left as-is.
    Ok(())
}
// #endregion writing-nullable-update-rust

const CHUNK_SIZE: usize = 64 * 1024;

// #region files-create-from-bytes-rust
pub async fn create_file_from_bytes(
    client: &JazzClient,
    data: &[u8],
    name: Option<&str>,
    mime_type: &str,
) -> jazz_tools::Result<ObjectId> {
    let mut part_ids = Vec::new();
    let mut part_sizes = Vec::new();

    for chunk in data.chunks(CHUNK_SIZE) {
        let (part_id, _) = client
            .create(
                "file_parts",
                jazz_tools::row_input!("data" => chunk.to_vec()),
            )
            .await?;
        part_ids.push(Value::Uuid(part_id));
        part_sizes.push(Value::Integer(chunk.len() as i32));
    }

    let mut file_values = jazz_tools::row_input!(
        "mimeType" => mime_type,
        "partIds" => part_ids,
        "partSizes" => part_sizes,
    );
    if let Some(name) = name {
        file_values.insert("name".to_string(), name.into());
    }

    let (file_id, _) = client.create("files", file_values).await?;
    Ok(file_id)
}
// #endregion files-create-from-bytes-rust

// #region files-create-upload-rust
pub async fn create_upload_from_bytes(
    client: &JazzClient,
    data: &[u8],
    owner_id: &str,
) -> jazz_tools::Result<ObjectId> {
    let file_id = create_file_from_bytes(client, data, Some("photo.jpg"), "image/jpeg").await?;

    let (upload_id, _) = client
        .create(
            "uploads",
            jazz_tools::row_input!(
                "owner_id" => owner_id,
                "label" => "Profile photo",
                "fileId" => file_id,
            ),
        )
        .await?;

    Ok(upload_id)
}
// #endregion files-create-upload-rust

// #region files-load-rust
pub async fn load_file_bytes(
    client: &JazzClient,
    upload_id: ObjectId,
) -> jazz_tools::Result<Option<Vec<u8>>> {
    let uploads = client
        .query(
            QueryBuilder::new("uploads")
                .select(&["fileId"])
                .filter_eq("_id", Value::Uuid(upload_id))
                .build(),
            Some(DurabilityTier::EdgeServer),
        )
        .await?;

    let Some((_, row)) = uploads.first() else {
        return Ok(None);
    };
    let Value::Uuid(file_id) = &row[0] else {
        return Ok(None);
    };

    let files = client
        .query(
            QueryBuilder::new("files")
                .select(&["partIds"])
                .filter_eq("_id", Value::Uuid(*file_id))
                .build(),
            Some(DurabilityTier::EdgeServer),
        )
        .await?;

    let Some((_, row)) = files.first() else {
        return Ok(None);
    };
    let Value::Array(part_ids) = &row[0] else {
        return Ok(None);
    };

    let mut data = Vec::new();
    for part_ref in part_ids {
        let Value::Uuid(part_id) = part_ref else {
            continue;
        };
        let parts = client
            .query(
                QueryBuilder::new("file_parts")
                    .select(&["data"])
                    .filter_eq("_id", Value::Uuid(*part_id))
                    .build(),
                Some(DurabilityTier::EdgeServer),
            )
            .await?;
        if let Some((_, row)) = parts.first()
            && let Value::Bytea(chunk) = &row[0]
        {
            data.extend_from_slice(chunk);
        }
    }

    Ok(Some(data))
}
// #endregion files-load-rust

// #region files-delete-rust
pub async fn delete_upload_with_file(
    client: &JazzClient,
    upload_id: ObjectId,
) -> jazz_tools::Result<()> {
    let uploads = client
        .query(
            QueryBuilder::new("uploads")
                .select(&["fileId"])
                .filter_eq("_id", Value::Uuid(upload_id))
                .build(),
            Some(DurabilityTier::EdgeServer),
        )
        .await?;

    let Some((_, row)) = uploads.first() else {
        return Ok(());
    };
    let Value::Uuid(file_id) = &row[0] else {
        return Ok(());
    };

    let files = client
        .query(
            QueryBuilder::new("files")
                .select(&["partIds"])
                .filter_eq("_id", Value::Uuid(*file_id))
                .build(),
            Some(DurabilityTier::EdgeServer),
        )
        .await?;

    if let Some((file_row_id, row)) = files.first() {
        if let Value::Array(part_ids) = &row[0] {
            // Delete chunks while the parent file row still exists.
            for part_ref in part_ids {
                if let Value::Uuid(part_id) = part_ref {
                    client.delete(*part_id).await?;
                }
            }
        }
        client.delete(*file_row_id).await?;
    }

    client.delete(upload_id).await?;
    Ok(())
}
// #endregion files-delete-rust

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
        .join("projects")
        .on("todos.project_id", "projects.id")
        .build();

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-includes-rust

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
        .update(todo_id, vec![("ownerId".to_string(), Value::Null)])
        .await?;

    // Only the specified columns are changed; omitted columns are left as-is.
    Ok(())
}
// #endregion writing-nullable-update-rust

// #region writing-crud-rust
pub async fn write_todo_crud(client: &JazzClient, existing_id: ObjectId) -> jazz_tools::Result<()> {
    let values = vec![
        Value::Text("Write docs".to_string()),
        Value::Boolean(false),
        Value::Text(String::new()),
        Value::Null,
        Value::Null,
    ];

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
// JazzClient.create/update/delete use the default durability tier
// (edge for server-connected clients, worker for browser clients).
//
// Explicit tier control is available on RuntimeCore via
// insert_persisted / update_persisted / delete_persisted,
// but not yet exposed on JazzClient.
pub async fn write_todo_with_defaults(client: &JazzClient) -> jazz_tools::Result<ObjectId> {
    let values = vec![
        Value::Text("Write docs".to_string()),
        Value::Boolean(false),
        Value::Text(String::new()),
        Value::Null,
        Value::Null,
    ];

    // Uses the default tier (edge for server-connected clients).
    let (id, _row_values) = client.create("todos", values).await?;
    Ok(id)
}
// #endregion writing-durability-tier-rust

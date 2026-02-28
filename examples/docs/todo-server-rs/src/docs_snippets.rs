//! Documentation snippet sources compiled with the example crate.
#![allow(dead_code)]

use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use jazz_tools::query_manager::policy::{Operation, PolicyExpr};
use jazz_tools::query_manager::types::TablePolicies;
use jazz_tools::{
    JazzClient, ObjectId, PersistenceTier, QueryBuilder, Session, SessionClient, Value,
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

// #region reading-settled-tier-rust
pub async fn read_todos_settled_edge(client: &JazzClient) -> jazz_tools::Result<usize> {
    let query = QueryBuilder::new("todos").build();
    let rows = client
        .query(query, Some(PersistenceTier::EdgeServer))
        .await?;
    Ok(rows.len())
}
// #endregion reading-settled-tier-rust

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
pub async fn read_todos_with_related_rows(client: &JazzClient) -> jazz_tools::Result<usize> {
    // Rust currently composes related data using additional queries.
    // If rows carry foreign keys, query related tables and join in application code.
    let rows = client
        .query(QueryBuilder::new("todos").build(), None)
        .await?;
    Ok(rows.len())
}
// #endregion reading-includes-rust

// #region writing-crud-rust
pub async fn write_todo_crud(client: &JazzClient, existing_id: ObjectId) -> jazz_tools::Result<()> {
    let values = vec![
        Value::Text("Write docs".to_string()),
        Value::Boolean(false),
        Value::Text(String::new()),
        Value::Null,
        Value::Null,
    ];

    let _new_id = client.create("todos", values).await?;
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

// #region writing-ack-tier-rust
pub async fn write_todo_with_default_ack(client: &JazzClient) -> jazz_tools::Result<ObjectId> {
    let id = client
        .create(
            "todos",
            vec![
                Value::Text("Write docs with default ack behavior".to_string()),
                Value::Boolean(false),
                Value::Text(String::new()),
                Value::Null,
                Value::Null,
            ],
        )
        .await?;

    // Rust currently does not expose per-write ack tier arguments.
    // Writes apply locally first, then sync asynchronously to higher tiers.
    Ok(id)
}
// #endregion writing-ack-tier-rust

//! Documentation snippet sources compiled with the example crate.

use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use jazz_rs::{JazzClient, ObjectId, PersistenceTier, QueryBuilder, Session, SessionClient, Value};
use groove::query_manager::policy::{Operation, PolicyExpr};
use groove::query_manager::types::TablePolicies;
use serde_json::json;

fn verify_jwt_and_extract_claims(_token: &str) -> (String, serde_json::Value) {
    // Replace with your auth provider's JWT verification logic.
    ("replace-with-verified-sub".to_string(), json!({}))
}

// #region backend-request-session-rust
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn scoped_client_for_session<'a>(client: &'a JazzClient, session: Session) -> SessionClient<'a> {
    client.for_session(session)
}
// #endregion backend-request-scoped-client-rust

// #region backend-request-handler-rust
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn inherits_select_policy() -> TablePolicies {
    TablePolicies::new().with_select(PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::inherits(Operation::Select, "folder_id"),
    ]))
}
// #endregion permissions-inherits-rust

// #region reading-oneshot-rust
#[allow(dead_code)]
pub async fn read_todos_oneshot(client: &JazzClient) -> jazz_rs::Result<usize> {
    let query = QueryBuilder::new("todos").build();
    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-oneshot-rust

// #region reading-subscriptions-rust
#[allow(dead_code)]
pub async fn subscribe_todos(client: &JazzClient) -> jazz_rs::Result<jazz_rs::SubscriptionStream> {
    let query = QueryBuilder::new("todos").build();
    client.subscribe(query).await
}
// #endregion reading-subscriptions-rust

// #region reading-settled-tier-rust
#[allow(dead_code)]
pub async fn read_todos_settled_edge(client: &JazzClient) -> jazz_rs::Result<usize> {
    let query = QueryBuilder::new("todos").build();
    let rows = client
        .query(query, Some(PersistenceTier::EdgeServer))
        .await?;
    Ok(rows.len())
}
// #endregion reading-settled-tier-rust

// #region writing-crud-rust
#[allow(dead_code)]
pub async fn write_todo_crud(client: &JazzClient, existing_id: ObjectId) -> jazz_rs::Result<()> {
    let values = vec![
        Value::Text("Write docs".to_string()),
        Value::Boolean(false),
        Value::Text(String::new()),
    ];

    let _new_id = client.create("todos", values).await?;
    client
        .update(
            existing_id,
            vec![("completed".to_string(), Value::Boolean(true))],
        )
        .await?;
    client.delete(existing_id).await?;
    Ok(())
}
// #endregion writing-crud-rust

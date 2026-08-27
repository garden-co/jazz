//! Documentation snippet sources compiled with the example crate.
#![allow(dead_code)]

use std::collections::BTreeMap;

use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use jazz::db::{Db, ExclusiveTxOps, MergeableTxOps};
use jazz::groove::records::Value as DbValue;
use jazz::ids::RowUuid;
use jazz::query::{
    ArraySubquery, ArraySubqueryRequirement, Gather, OrderDirection, Query, col, contains, eq, gt,
    gte, is_null, lit, lt, ne, not,
};
use jazz::tools::{
    DurabilityTier, JazzClient, ObjectId, Operation, PolicyExpr, Session, TablePolicies, Value,
};
use jazz_storage_rocksdb::RocksDbStorage;
use serde_json::json;

fn verify_jwt_and_extract_claims(_token: &str) -> (String, String, serde_json::Value) {
    // Replace with your auth provider's JWT verification logic.
    ("replace-with-verified-sub".to_string(), json!({}))
}

fn todo_values(
    title: impl Into<String>,
    description: impl Into<String>,
) -> std::collections::HashMap<String, Value> {
    jazz::row_input!("title" => title.into(), "done" => false, "description" => description.into())
}

fn transaction_todo_values(title: impl Into<String>) -> jazz::db::RowCells {
    BTreeMap::from([
        ("title".to_string(), DbValue::String(title.into())),
        ("done".to_string(), DbValue::Bool(false)),
        ("description".to_string(), DbValue::String(String::new())),
    ])
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

    let (issuer, user_id, claims) = verify_jwt_and_extract_claims(token);
    Ok(Session::new(issuer, user_id).with_claims(claims))
}
// #endregion backend-request-session-rust

// #region backend-request-scoped-client-rust
pub fn scoped_client_for_session(client: &JazzClient, session: Session) -> JazzClient {
    client.for_session(session)
}
// #endregion backend-request-scoped-client-rust

// #region backend-request-handler-rust
pub async fn list_todos_for_request(
    headers: &HeaderMap,
    client: &JazzClient,
) -> Result<usize, StatusCode> {
    let user_client = client.for_session(requester_session_from_headers(headers)?);
    let query = Query::from("todos");
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
pub async fn read_todos_oneshot(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos");
    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-oneshot-rust

// #region reading-subscriptions-rust
pub async fn subscribe_todos(
    client: &JazzClient,
) -> jazz::tools::Result<jazz::tools::SubscriptionStream> {
    let query = Query::from("todos");
    client.subscribe(query).await
}
// #endregion reading-subscriptions-rust

// #region reading-durability-tier-rust
pub async fn read_todos_at_edge_durability(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos");
    let rows = client
        .query(query, Some(DurabilityTier::EdgeServer))
        .await?;
    Ok(rows.len())
}
// #endregion reading-durability-tier-rust

// #region reading-filters-rust
pub async fn read_todos_with_filters(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos").filter(eq(col("done"), lit(false)));

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-filters-rust

// #region reading-sorting-rust
pub async fn read_todos_sorted(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .order_by("title", OrderDirection::Asc);

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-sorting-rust

// #region reading-pagination-rust
pub async fn read_todo_page(
    client: &JazzClient,
    page_size: usize,
    page: usize,
) -> jazz::tools::Result<usize> {
    let offset = page.saturating_sub(1) * page_size;
    let query = Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .order_by("title", OrderDirection::Asc)
        .order_by("id", OrderDirection::Asc)
        .limit(page_size)
        .offset(offset);

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-pagination-rust

// #region reading-includes-rust
pub async fn read_todos_with_project(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .flat_join("projects", "todos.project_id", "projects.id");

    let rows = client.query_results(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-includes-rust

// #region reading-reverse-relation-rust
pub fn build_projects_with_todos_query() -> Query {
    Query::from("projects").array_subquery(
        ArraySubquery::new("todos_via_project", "todos", "project_id", "id")
            .filter(eq(col("done"), lit(false))),
    )
}
// #endregion reading-reverse-relation-rust

// #region reading-require-includes-rust
pub fn build_todos_with_required_project() -> Query {
    Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .array_subquery(
            ArraySubquery::new("project", "projects", "id", "project_id")
                .requirement(ArraySubqueryRequirement::AtLeastOne),
        )
}
// #endregion reading-require-includes-rust

// #region reading-select-rust
pub async fn read_todo_titles(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos").select(["title", "done"]);

    let rows = client.query(query, None).await?;
    Ok(rows.len())
}
// #endregion reading-select-rust

// #region reading-recursive-rust
pub fn build_todo_lineage_query() -> Query {
    Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .gather(
            Gather::from("todos")
                .where_current("id")
                .hop_to("parent_id")
                .max_depth(10),
        )
}
// #endregion reading-recursive-rust

// #region writing-crud-rust
pub async fn write_todo_crud(
    client: &JazzClient,
    existing_id: ObjectId,
) -> jazz::tools::Result<()> {
    let values = todo_values("Write docs", "");

    let _new_row = client.insert("todos", values)?;
    client.update(
        existing_id,
        vec![("done".to_string(), Value::Boolean(true))],
    )?;
    client.delete(existing_id)?;
    Ok(())
}
// #endregion writing-crud-rust

// #region writing-durability-tier-rust
pub async fn write_todo_with_default_durability(
    client: &JazzClient,
) -> jazz::tools::Result<ObjectId> {
    let (id, _row_values, _batch_id) = client.insert(
        "todos",
        todo_values("Write docs with default durability behavior", ""),
    )?;

    // Rust currently does not expose per-write durability tier arguments.
    // Writes apply locally first, then sync asynchronously to higher tiers.
    Ok(id)
}
// #endregion writing-durability-tier-rust

// #region writing-transaction-rust
pub fn group_todo_writes(
    db: &Db<RocksDbStorage>,
    existing_todo_id: RowUuid,
) -> Result<RowUuid, jazz::db::Error> {
    let (created_id, _transaction_id) = db.transaction(|tx| {
        let created_id = tx.insert("todos", transaction_todo_values("Write transaction docs"))?;
        tx.update(
            "todos",
            existing_todo_id,
            BTreeMap::from([("done".to_string(), DbValue::Bool(true))]),
        )?;

        let _staged = tx.read("todos", created_id)?;

        Ok(created_id)
    })?;

    Ok(created_id)
}
// #endregion writing-transaction-rust

// #region writing-exclusive-transaction-rust
pub fn finish_todo_exclusively(
    db: &Db<RocksDbStorage>,
    todo_id: RowUuid,
) -> Result<(), jazz::db::Error> {
    let tx = db.exclusive_tx()?;
    let _todo = tx.read("todos", todo_id)?;

    tx.update(
        "todos",
        todo_id,
        BTreeMap::from([("done".to_string(), DbValue::Bool(true))]),
    )?;
    let _transaction_id = tx.commit()?;
    Ok(())
}
// #endregion writing-exclusive-transaction-rust

pub async fn where_operator_examples(client: &JazzClient) -> jazz::tools::Result<()> {
    let search_term = "milk";

    // #region where-eq-ne-rust
    // Exact match
    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let incomplete_todos = client.query(query, None).await?;

    // Not equal
    let query = Query::from("todos").filter(ne(col("title"), lit("Draft")));
    let non_draft_todos = client.query(query, None).await?;
    // #endregion where-eq-ne-rust

    // #region where-numeric-rust
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let one_week_ago = now_ms - 7 * 24 * 60 * 60 * 1000;

    let query = Query::from("todos").filter(gt(col("$createdAt"), lit(one_week_ago)));
    let recent_todos = client.query(query, None).await?;

    let query = Query::from("todos").filter(gte(col("priority"), lit(3)));
    let high_priority = client.query(query, None).await?;

    let query = Query::from("todos").filter(lt(col("priority"), lit(10)));
    let low_priority = client.query(query, None).await?;
    // #endregion where-numeric-rust

    // #region where-contains-rust
    // Substring match (case-sensitive)
    let query = Query::from("todos").filter(contains(col("title"), lit(search_term)));
    let matches = client.query(query, None).await?;
    // #endregion where-contains-rust

    // #region where-null-rust
    // Rows where the optional ref is not set
    let query = Query::from("todos").filter(is_null(col("parent")));
    let unlinked_todos = client.query(query, None).await?;

    // Rows where it is set
    let query = Query::from("todos").filter(not(is_null(col("parent"))));
    let linked_todos = client.query(query, None).await?;
    // #endregion where-null-rust

    // #region where-and-rust
    // Multiple filter calls are AND-combined
    let query = Query::from("todos")
        .filter(eq(col("done"), lit(true)))
        .filter(not(is_null(col("project"))));
    let done_with_project = client.query(query, None).await?;
    // #endregion where-and-rust

    // #region where-order-limit-rust
    let query = Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .order_by("$createdAt", OrderDirection::Asc)
        .limit(50);
    let recent_incomplete = client.query(query, None).await?;
    // #endregion where-order-limit-rust

    // #region where-subscription-rust
    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
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
    let by_title = Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .order_by("title", OrderDirection::Asc)
        .limit(20);
    let by_newest = Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .order_by("id", OrderDirection::Desc);

    let _ = (by_title, by_newest);
}
// #endregion reading-composing-queries-rust

// #region writing-nullable-update-rust
pub async fn clear_nullable_fields(
    client: &JazzClient,
    todo_id: ObjectId,
) -> jazz::tools::Result<()> {
    // Set a nullable column to null
    client.update(todo_id, vec![("owner_id".to_string(), Value::Null)])?;

    // Only the specified columns are changed; omitted columns are left as-is.
    Ok(())
}
// #endregion writing-nullable-update-rust

// #region files-create-from-bytes-rust
pub async fn create_file_from_bytes(
    client: &JazzClient,
    data: &[u8],
    name: Option<&str>,
    mime_type: &str,
) -> jazz::tools::Result<ObjectId> {
    let mut file_values = jazz::row_input!(
        "mime_type" => mime_type,
        "data" => data.to_vec(),
    );
    if let Some(name) = name {
        file_values.insert("name".to_string(), name.into());
    }

    let (file_id, _, _) = client.insert("files", file_values)?;
    Ok(file_id)
}
// #endregion files-create-from-bytes-rust

// #region files-create-upload-rust
pub async fn create_upload_from_bytes(
    client: &JazzClient,
    data: &[u8],
    owner_id: &str,
) -> jazz::tools::Result<ObjectId> {
    let file_id = create_file_from_bytes(client, data, Some("photo.jpg"), "image/jpeg").await?;

    let (upload_id, _, _) = client.insert(
        "uploads",
        jazz::row_input!(
            "owner_id" => owner_id,
            "label" => "Profile photo",
            "fileId" => file_id,
        ),
    )?;

    Ok(upload_id)
}
// #endregion files-create-upload-rust

// #region files-load-rust
pub async fn load_file_bytes(
    client: &JazzClient,
    upload_id: ObjectId,
) -> jazz::tools::Result<Option<Vec<u8>>> {
    let uploads = client
        .query(
            Query::from("uploads")
                .select(["fileId"])
                .filter(eq(col("id"), lit(*upload_id.uuid()))),
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
            Query::from("files")
                .select(["data"])
                .filter(eq(col("id"), lit(*file_id.uuid()))),
            Some(DurabilityTier::EdgeServer),
        )
        .await?;

    let Some((_, row)) = files.first() else {
        return Ok(None);
    };

    match &row[0] {
        Value::Bytea(data) => Ok(Some(data.clone())),
        _ => Ok(None),
    }
}
// #endregion files-load-rust

// #region files-delete-rust
pub async fn delete_upload_with_file(
    client: &JazzClient,
    upload_id: ObjectId,
) -> jazz::tools::Result<()> {
    let uploads = client
        .query(
            Query::from("uploads")
                .select(["fileId"])
                .filter(eq(col("id"), lit(*upload_id.uuid()))),
            Some(DurabilityTier::EdgeServer),
        )
        .await?;

    let Some((_, row)) = uploads.first() else {
        return Ok(());
    };
    let Value::Uuid(file_id) = &row[0] else {
        return Ok(());
    };

    client.delete(*file_id)?;

    client.delete(upload_id)?;
    Ok(())
}
// #endregion files-delete-rust

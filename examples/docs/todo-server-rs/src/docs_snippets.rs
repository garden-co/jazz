//! Documentation snippet sources compiled with the example crate.
//!
//! The docs include each `#region` by name. Most use the typed API the
//! generated bindings (`crate::jazz_app`) give the [`Jazz`] handle. What it
//! doesn't cover yet (per-request sessions, includes, select, recursive
//! queries, files, serializable transactions with reads) uses the
//! low-level `JazzClient`, either directly or through [`Jazz::with_client`].
//! Permissions are the app's `jazz.permissions.json`.
#![allow(dead_code)]

use std::collections::BTreeMap;

use ::jazz_app::jazz;
use ::jazz_app::{Jazz, ObjectId, Order, ReadTier, Subscription, Timestamp};
use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use jazz::db::{Db, UpdateOptions};
use jazz::ids::RowUuid;
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, Gather, Query, col, eq, lit};
use jazz::records::Value as DbValue;
use jazz::tools::{JazzClient, Session, Value};

use crate::jazz_app::{NewTodo, Todo, TodoUpdate, projects, todos};

fn verify_jwt_and_extract_claims(_token: &str) -> (String, String, serde_json::Value) {
    unimplemented!("replace with your auth provider's JWT verification logic")
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
pub async fn list_todos_for_request(headers: &HeaderMap, jazz: &Jazz) -> Result<usize, StatusCode> {
    let session = requester_session_from_headers(headers)?;
    // The typed handle acts as the backend's own user; a request's user
    // reads through the low-level client, scoped to their session.
    jazz.with_client(move |client| async move {
        let user_client = client.for_session(session);
        let rows = user_client
            .query(todos::query().into_untyped(), ReadTier::LocalFirst)
            .await?;
        Ok(rows.len())
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
// #endregion backend-request-handler-rust

// #region reading-oneshot-rust
pub async fn read_todos_oneshot(jazz: &Jazz) -> ::jazz_app::Result<Vec<Todo>> {
    jazz.all(todos::query()).await
}
// #endregion reading-oneshot-rust

// #region reading-subscriptions-rust
pub async fn subscribe_todos(jazz: &Jazz) -> ::jazz_app::Result<Subscription<Todo>> {
    // A stream of deltas, starting with the rows the query matches now;
    // `Rows::apply` folds them into the current list.
    jazz.subscribe(todos::query()).await
}
// #endregion reading-subscriptions-rust

// #region reading-durability-tier-rust
pub async fn read_todos_at_remote_tier(jazz: &Jazz) -> ::jazz_app::Result<Vec<Todo>> {
    jazz.all_with(todos::query(), ReadTier::Remote).await
}
// #endregion reading-durability-tier-rust

// #region reading-filters-rust
pub async fn read_todos_with_filters(jazz: &Jazz) -> ::jazz_app::Result<Vec<Todo>> {
    let query = todos::query().filter(todos::done().eq(false));

    jazz.all(query).await
}
// #endregion reading-filters-rust

// #region reading-sorting-rust
pub async fn read_todos_sorted(jazz: &Jazz) -> ::jazz_app::Result<Vec<Todo>> {
    let query = todos::query()
        .filter(todos::done().eq(false))
        .order_by(todos::title(), Order::Asc);

    jazz.all(query).await
}
// #endregion reading-sorting-rust

// #region reading-pagination-rust
pub async fn read_todo_page(
    jazz: &Jazz,
    page_size: usize,
    page: usize,
) -> ::jazz_app::Result<Vec<Todo>> {
    let offset = page.saturating_sub(1) * page_size;
    let query = todos::query()
        .filter(todos::done().eq(false))
        .order_by(todos::title(), Order::Asc)
        // The id breaks ties, so pages never overlap.
        .order_by(todos::id(), Order::Asc)
        .limit(page_size)
        .offset(offset);

    jazz.all(query).await
}
// #endregion reading-pagination-rust

// #region reading-includes-rust
pub async fn read_todos_with_project(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .flat_join("projects", "todos.project", "projects.id");

    let rows = client.query(query, ReadTier::LocalFirst).await?;
    Ok(rows.len())
}
// #endregion reading-includes-rust

// #region reading-reverse-relation-rust
pub async fn read_open_todos_of_project(
    jazz: &Jazz,
    project_id: ObjectId,
) -> ::jazz_app::Result<Vec<Todo>> {
    // `projects.todos`: the todos whose `project` references this project.
    let query = projects::todos()
        .of(project_id)
        .filter(todos::done().eq(false));

    jazz.all(query).await
}
// #endregion reading-reverse-relation-rust

// #region reading-require-includes-rust
pub fn build_todos_with_required_project() -> Query {
    Query::from("todos")
        .filter(eq(col("done"), lit(false)))
        .array_subquery(
            ArraySubquery::new("project", "projects", "id", "project")
                .requirement(ArraySubqueryRequirement::AtLeastOne),
        )
}
// #endregion reading-require-includes-rust

// #region reading-select-rust
pub async fn read_todo_titles(client: &JazzClient) -> jazz::tools::Result<usize> {
    let query = Query::from("todos").select(["title", "done"]);

    let rows = client.query(query, ReadTier::LocalFirst).await?;
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
                .hop_to("parent")
                .max_depth(10),
        )
}
// #endregion reading-recursive-rust

// #region writing-crud-rust
pub async fn write_todo_crud(jazz: &Jazz, existing_id: ObjectId) -> ::jazz_app::Result<()> {
    let _new_todo = jazz.insert(NewTodo::new("Write docs", false)).await?;
    jazz.update(
        existing_id,
        TodoUpdate {
            done: Some(true),
            ..Default::default()
        },
    )
    .await?;
    jazz.delete::<Todo>(existing_id).await?;
    Ok(())
}
// #endregion writing-crud-rust

// #region writing-durability-tier-rust
pub async fn write_todo_with_default_durability(jazz: &Jazz) -> ::jazz_app::Result<ObjectId> {
    let todo = jazz
        .insert(NewTodo::new(
            "Write docs with default durability behavior",
            false,
        ))
        .await?;

    // Rust currently does not expose per-write durability tier arguments.
    // Writes apply locally first, then sync asynchronously to higher tiers.
    Ok(todo.id)
}
// #endregion writing-durability-tier-rust

// #region writing-transaction-rust
pub async fn group_todo_writes(
    jazz: &Jazz,
    existing_todo_id: ObjectId,
) -> ::jazz_app::Result<ObjectId> {
    // Committed together if the closure returns `Ok`, rolled back if it
    // returns `Err`.
    jazz.transaction(move |tx| {
        let created = tx.insert(NewTodo::new("Write transaction docs", false))?;
        tx.update(
            existing_todo_id,
            TodoUpdate {
                done: Some(true),
                ..Default::default()
            },
        )?;
        Ok(created.id)
    })
    .await
}
// #endregion writing-transaction-rust

// #region writing-serializable-transaction-rust
pub async fn finish_todo_serializably(db: &Db, todo_id: RowUuid) -> Result<(), jazz::db::Error> {
    let tx = db.serializable_tx().await?;
    let _todo = tx.read("todos", todo_id).await?;

    tx.update(
        "todos",
        todo_id,
        BTreeMap::from([("done".to_string(), DbValue::Bool(true))]),
        UpdateOptions::default(),
    )
    .await?;
    let _transaction_id = tx.commit().await?;
    Ok(())
}
// #endregion writing-serializable-transaction-rust

pub async fn where_operator_examples(jazz: &Jazz) -> ::jazz_app::Result<()> {
    let search_term = "milk";

    // #region where-eq-ne-rust
    // Exact match
    let query = todos::query().filter(todos::done().eq(false));
    let incomplete_todos = jazz.all(query).await?;

    // Not equal
    let query = todos::query().filter(todos::title().ne("Draft"));
    let non_draft_todos = jazz.all(query).await?;
    // #endregion where-eq-ne-rust

    // #region where-numeric-rust
    let query = todos::query().filter(todos::priority().gte(3));
    let high_priority = jazz.all(query).await?;

    let query = todos::query().filter(todos::priority().lt(10));
    let low_priority = jazz.all(query).await?;

    // Every table has `created_at()` and `updated_at()` columns.
    let one_week_ago = Timestamp(Timestamp::now().0 - 7 * 24 * 60 * 60 * 1000);
    let query = todos::query().filter(::jazz_app::created_at().gt(one_week_ago));
    let recent_todos = jazz.all(query).await?;
    // #endregion where-numeric-rust

    // #region where-contains-rust
    // Substring match (case-sensitive)
    let query = todos::query().filter(todos::title().contains(search_term));
    let matches = jazz.all(query).await?;
    // #endregion where-contains-rust

    // #region where-null-rust
    // Rows where the optional ref is not set
    let query = todos::query().filter(todos::parent().is_null());
    let unlinked_todos = jazz.all(query).await?;

    // Rows where it is set
    let query = todos::query().filter(todos::parent().is_not_null());
    let linked_todos = jazz.all(query).await?;
    // #endregion where-null-rust

    // #region where-and-rust
    // Multiple filter calls are AND-combined
    let query = todos::query()
        .filter(todos::done().eq(true))
        .filter(todos::project().is_not_null());
    let done_with_project = jazz.all(query).await?;
    // #endregion where-and-rust

    // #region where-order-limit-rust
    let query = todos::query()
        .filter(todos::done().eq(false))
        .order_by(::jazz_app::created_at(), Order::Asc)
        .limit(50);
    let recent_incomplete = jazz.all(query).await?;
    // #endregion where-order-limit-rust

    // #region where-subscription-rust
    let query = todos::query().filter(todos::done().eq(false));
    let pending = jazz.subscribe(query).await?;
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
    // Build two views from the same base conditions: queries are values.
    let open = todos::query().filter(todos::done().eq(false));
    let newest = open
        .clone()
        .order_by(::jazz_app::created_at(), Order::Desc)
        .limit(20);
    let by_priority = open.order_by(todos::priority(), Order::Desc);

    let _ = (newest, by_priority);
}
// #endregion reading-composing-queries-rust

// #region writing-nullable-update-rust
pub async fn clear_nullable_fields(jazz: &Jazz, todo_id: ObjectId) -> ::jazz_app::Result<()> {
    // `Some(None)` sets a nullable column to null; `None` leaves it as-is.
    jazz.update(
        todo_id,
        TodoUpdate {
            parent: Some(None),
            ..Default::default()
        },
    )
    .await?;

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
            ReadTier::Remote,
        )
        .await?;

    let Some(row) = uploads.first() else {
        return Ok(None);
    };
    let Some(Value::Uuid(file_id)) = row.get("fileId") else {
        return Ok(None);
    };

    let files = client
        .query(
            Query::from("files")
                .select(["data"])
                .filter(eq(col("id"), lit(*file_id.uuid()))),
            ReadTier::Remote,
        )
        .await?;

    let Some(row) = files.first() else {
        return Ok(None);
    };

    match row.get("data") {
        Some(Value::Bytea(data)) => Ok(Some(data.clone())),
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
            ReadTier::Remote,
        )
        .await?;

    let Some(row) = uploads.first() else {
        return Ok(());
    };
    let Some(Value::Uuid(file_id)) = row.get("fileId") else {
        return Ok(());
    };

    client.delete("files", *file_id)?;

    client.delete("uploads", upload_id)?;
    Ok(())
}
// #endregion files-delete-rust

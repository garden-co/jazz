//! HTTP routes for the todo API.

use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{
        IntoResponse,
        sse::{Event, Sse},
    },
    routing::{delete, get, post, put},
};
use futures_util::StreamExt as FuturesStreamExt;
use futures_util::stream::Stream;
use jazz_tools::{ObjectId, QueryBuilder, Value};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::AppState;

/// Todo item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Todo {
    pub id: Uuid,
    pub title: String,
    pub done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Request to create a new todo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTodoRequest {
    pub title: String,
    pub description: Option<String>,
}

/// Request to update a todo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTodoRequest {
    pub title: Option<String>,
    pub done: Option<bool>,
    pub description: Option<String>,
}

/// Create the router with all routes.
pub fn create_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/todos", get(list_todos))
        .route("/todos", post(create_todo))
        .route("/todos/live", get(todos_live))
        .route("/todos/:id", get(get_todo))
        .route("/todos/:id", put(update_todo))
        .route("/todos/:id", delete(delete_todo))
        .route("/health", get(health))
}

/// Broadcast current todos to all SSE connections.
async fn broadcast_todos(state: &AppState) {
    let query = QueryBuilder::new("todos").build();
    if let Ok(rows) = state.client.query(query, None).await {
        let todos: Vec<Todo> = rows
            .iter()
            .filter_map(|(id, values)| row_to_todo(*id, values))
            .collect();
        // Ignore send errors (no receivers is fine)
        let _ = state.sse_tx.send(todos);
    }
}

/// Convert a query result row to a Todo.
fn row_to_todo(object_id: ObjectId, values: &[Value]) -> Option<Todo> {
    if values.len() < 2 {
        return None;
    }
    let title = match &values[0] {
        Value::Text(s) => s.clone(),
        _ => return None,
    };
    let done = match &values[1] {
        Value::Boolean(b) => *b,
        _ => return None,
    };
    let description = values.get(2).and_then(|v| match v {
        Value::Text(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    });
    Some(Todo {
        id: *object_id.uuid(),
        title,
        done,
        description,
    })
}

fn todo_values(title: String, description: String) -> std::collections::HashMap<String, Value> {
    jazz_tools::row_input!("title" => title, "done" => false, "description" => description)
}

/// List all todos.
async fn list_todos(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let query = QueryBuilder::new("todos").build();
    match state.client.query(query, None).await {
        Ok(rows) => {
            let todos: Vec<Todo> = rows
                .iter()
                .filter_map(|(id, values)| row_to_todo(*id, values))
                .collect();
            Json(todos).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Create a new todo.
async fn create_todo(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateTodoRequest>,
) -> impl IntoResponse {
    let description = request.description.clone().unwrap_or_default();
    let values = todo_values(request.title.clone(), description.clone());

    match state.client.create("todos", values).await {
        Ok((row_id, row_values)) => {
            let todo = row_to_todo(row_id, &row_values);

            // Broadcast to SSE connections
            broadcast_todos(&state).await;

            (StatusCode::CREATED, Json(todo)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Get a single todo.
async fn get_todo(State(state): State<Arc<AppState>>, Path(id): Path<Uuid>) -> impl IntoResponse {
    let query = QueryBuilder::new("todos").build();

    match state.client.query(query, None).await {
        Ok(rows) => {
            // Find the todo with matching id
            for (object_id, values) in &rows {
                if *object_id.uuid() != id {
                    continue;
                }
                if let Some(todo) = row_to_todo(*object_id, values) {
                    return Json(todo).into_response();
                }
            }
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "Todo not found" })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Update a todo.
async fn update_todo(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateTodoRequest>,
) -> impl IntoResponse {
    let object_id = ObjectId::from_uuid(id);

    // Build partial updates
    let mut updates = Vec::new();
    if let Some(title) = request.title {
        updates.push(("title".to_string(), Value::Text(title)));
    }
    if let Some(done) = request.done {
        updates.push(("done".to_string(), Value::Boolean(done)));
    }
    if let Some(description) = request.description {
        updates.push(("description".to_string(), Value::Text(description)));
    }

    if updates.is_empty() {
        // No changes, fetch and return current
        let query = QueryBuilder::new("todos").build();
        if let Ok(rows) = state.client.query(query, None).await {
            for (oid, values) in &rows {
                if *oid.uuid() != id {
                    continue;
                }
                if let Some(todo) = row_to_todo(*oid, values) {
                    return Json(todo).into_response();
                }
            }
        }
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Todo not found" })),
        )
            .into_response();
    }

    match state.client.update(object_id, updates).await {
        Ok(()) => {
            // Broadcast to SSE connections
            broadcast_todos(&state).await;

            // Re-fetch the updated todo
            let query = QueryBuilder::new("todos").build();
            match state.client.query(query, None).await {
                Ok(rows) => {
                    for (oid, values) in &rows {
                        if *oid.uuid() != id {
                            continue;
                        }
                        if let Some(todo) = row_to_todo(*oid, values) {
                            return Json(todo).into_response();
                        }
                    }
                    (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({ "error": "Todo not found after update" })),
                    )
                        .into_response()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response(),
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Delete a todo.
async fn delete_todo(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    let object_id = ObjectId::from_uuid(id);

    match state.client.delete(object_id).await {
        Ok(()) => {
            // Broadcast to SSE connections
            broadcast_todos(&state).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("NotFound") || err_str.contains("ObjectNotFound") {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "Todo not found" })),
                )
                    .into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": err_str })),
                )
                    .into_response()
            }
        }
    }
}

/// Health check.
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "healthy" }))
}

/// SSE endpoint streaming all todos on changes.
async fn todos_live(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe to broadcast channel
    let rx = state.sse_tx.subscribe();

    // Get initial state
    let query = QueryBuilder::new("todos").build();
    let initial_todos: Vec<Todo> = state
        .client
        .query(query, None)
        .await
        .map(|rows| {
            rows.iter()
                .filter_map(|(id, values)| row_to_todo(*id, values))
                .collect()
        })
        .unwrap_or_default();

    // Create stream that yields initial state then updates
    let initial_event = futures_util::stream::once(async move {
        Ok::<_, Infallible>(
            Event::default().data(serde_json::to_string(&initial_todos).unwrap_or_default()),
        )
    });

    let update_stream = BroadcastStream::new(rx).filter_map(|result| async move {
        match result {
            Ok(todos) => Some(Ok::<_, Infallible>(
                Event::default().data(serde_json::to_string(&todos).unwrap_or_default()),
            )),
            Err(_) => None, // Ignore lagged errors
        }
    });

    Sse::new(initial_event.chain(update_stream))
}

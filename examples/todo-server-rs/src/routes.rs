//! HTTP routes for the todo API.

use std::convert::Infallible;

use ::jazz_app::{Jazz, ObjectId, Rows};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::get,
};
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use futures_util::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::jazz_app::{self, NewTodo, TodoUpdate, todos};

/// A todo, as the API serves it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Todo {
    pub id: Uuid,
    pub title: String,
    pub done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl From<jazz_app::Todo> for Todo {
    fn from(todo: jazz_app::Todo) -> Self {
        Self {
            id: *todo.id.uuid(),
            title: todo.title,
            done: todo.done,
            description: todo.description,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTodoRequest {
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateTodoRequest {
    pub title: Option<String>,
    pub done: Option<bool>,
    pub description: Option<String>,
}

/// Resolves when the service is stopping: the live streams end then, so a
/// graceful shutdown isn't held open by them.
pub type Stopping = Shared<BoxFuture<'static, ()>>;

#[derive(Clone)]
struct AppState {
    jazz: Jazz,
    stopping: Stopping,
}

impl axum::extract::FromRef<AppState> for Jazz {
    fn from_ref(state: &AppState) -> Jazz {
        state.jazz.clone()
    }
}

/// The API over the open app, serving until the process ends.
pub fn router(jazz: Jazz) -> Router {
    router_until(jazz, std::future::pending().boxed().shared())
}

/// The API over the open app; its live streams end once `stopping` resolves.
pub fn router_until(jazz: Jazz, stopping: Stopping) -> Router {
    Router::new()
        .route("/todos", get(list_todos).post(create_todo))
        .route("/todos/live", get(todos_live))
        .route(
            "/todos/:id",
            get(get_todo).put(update_todo).delete(delete_todo),
        )
        .route("/health", get(health))
        .with_state(AppState { jazz, stopping })
}

fn failed(error: ::jazz_app::Error) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": "Todo not found" })),
    )
        .into_response()
}

async fn list_todos(State(jazz): State<Jazz>) -> Response {
    match jazz.all(todos::query()).await {
        Ok(rows) => Json(rows.into_iter().map(Todo::from).collect::<Vec<_>>()).into_response(),
        Err(error) => failed(error),
    }
}

async fn create_todo(State(jazz): State<Jazz>, Json(request): Json<CreateTodoRequest>) -> Response {
    let new = NewTodo {
        description: request.description,
        ..NewTodo::new(request.title, false)
    };
    match jazz.insert(new).await {
        Ok(todo) => (StatusCode::CREATED, Json(Todo::from(todo))).into_response(),
        Err(error) => failed(error),
    }
}

async fn get_todo(State(jazz): State<Jazz>, Path(id): Path<Uuid>) -> Response {
    match jazz.get::<jazz_app::Todo>(ObjectId::from_uuid(id)).await {
        Ok(Some(todo)) => Json(Todo::from(todo)).into_response(),
        Ok(None) => not_found(),
        Err(error) => failed(error),
    }
}

async fn update_todo(
    State(jazz): State<Jazz>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateTodoRequest>,
) -> Response {
    let id = ObjectId::from_uuid(id);
    match jazz.get::<jazz_app::Todo>(id).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found(),
        Err(error) => return failed(error),
    }
    let update = TodoUpdate {
        title: request.title,
        done: request.done,
        description: request.description.map(Some),
        ..Default::default()
    };
    if update != TodoUpdate::default()
        && let Err(error) = jazz.update(id, update).await
    {
        return failed(error);
    }
    get_todo(State(jazz), Path(*id.uuid())).await
}

async fn delete_todo(State(jazz): State<Jazz>, Path(id): Path<Uuid>) -> Response {
    let id = ObjectId::from_uuid(id);
    match jazz.get::<jazz_app::Todo>(id).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found(),
        Err(error) => return failed(error),
    }
    match jazz.delete::<jazz_app::Todo>(id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failed(error),
    }
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "healthy" }))
}

/// Every todo, sent again whenever one changes: the subscription's deltas
/// applied to the list it holds.
async fn todos_live(
    State(state): State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Response> {
    let changes = state.jazz.subscribe(todos::query()).await.map_err(failed)?;
    let lists = changes
        .scan(Rows::default(), |rows, delta| {
            let list = delta.map(|delta| {
                rows.apply(delta);
                rows.as_slice()
                    .iter()
                    .cloned()
                    .map(Todo::from)
                    .collect::<Vec<_>>()
            });
            // A failed subscription ends the stream.
            std::future::ready(list.ok())
        })
        .map(|list| Ok(Event::default().data(serde_json::to_string(&list).unwrap_or_default())))
        .take_until(state.stopping);
    Ok(Sse::new(lists))
}

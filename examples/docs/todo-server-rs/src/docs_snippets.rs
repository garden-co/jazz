//! Documentation snippet sources compiled with the example crate.
//!
//! The docs include each `#region` by name. Each region is one function over
//! the docs todo app (`todos` and `projects`, from the TypeScript example's
//! `schema.ts`), doing what its TypeScript twin does. The files snippets
//! still use the low-level `JazzClient`.
//! Permissions are the app's `jazz.permissions.json`.
#![allow(dead_code)]

use std::ops::ControlFlow;

use ::jazz_app_support::jazz;
use futures_util::StreamExt;
use jazz::query::{Query as UntypedQuery, col, eq, lit};
use jazz::tools::{JazzClient, Value};
use uuid::Uuid;

use crate::jazz_app::{
    self, Advice, DurabilityTier, Jazz, NewTodo, ObjectId, Order, Partial, Project, Query,
    ReadTier, Rows, Subscription, Timestamp, Todo, TodoUpdate, projects, todos,
};
use crate::required_references;

/// The example's owner and project ids, as the TypeScript snippets'
/// `EXAMPLE_OWNER_ID` and `EXAMPLE_PROJECT_ID`.
const EXAMPLE_OWNER_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00a1);
const EXAMPLE_PROJECT_ID: Uuid = Uuid::nil();

// #region reading-oneshot-rust
pub async fn read_todos_oneshot(jazz: &Jazz) -> jazz_app::Result<Vec<Todo>> {
    jazz.all(todos::query().filter(todos::done().eq(false)))
        .await
}
// #endregion reading-oneshot-rust

// #region reading-one-rust
pub async fn read_todo(jazz: &Jazz, id: ObjectId) -> jazz_app::Result<Option<Todo>> {
    // The row, or `None` if there is none or you can't read it.
    jazz.get::<Todo>(id).await
}
// #endregion reading-one-rust

// #region reading-subscriptions-rust
pub async fn subscribe_todos(
    jazz: &Jazz,
    mut on_change: impl FnMut(&[Todo]) -> ControlFlow<()>,
) -> jazz_app::Result<()> {
    let mut changes = jazz
        .subscribe(todos::query().filter(todos::done().eq(false)))
        .await?;
    // The subscription yields deltas; `Rows` folds each into the whole result.
    let mut todos = Rows::default();
    while let Some(delta) = changes.next().await {
        todos.apply(delta?);
        if on_change(todos.as_slice()).is_break() {
            break;
        }
    }
    // Dropping the subscription unsubscribes.
    drop(changes);
    Ok(())
}
// #endregion reading-subscriptions-rust

// #region reading-durability-tier-rust
pub async fn read_todos_at_remote_tier(jazz: &Jazz) -> jazz_app::Result<Vec<Todo>> {
    jazz.all_with(
        todos::query().filter(todos::done().eq(false)),
        ReadTier::Remote,
    )
    .await
}
// #endregion reading-durability-tier-rust

// #region reading-filters-rust
pub async fn read_todos_with_filters(jazz: &Jazz) -> jazz_app::Result<Vec<Todo>> {
    let query = todos::query()
        .filter(todos::done().eq(false))
        .filter(todos::title().contains("docs"));

    jazz.all(query).await
}
// #endregion reading-filters-rust

// #region reading-sorting-rust
pub async fn read_todos_sorted_by_title(jazz: &Jazz) -> jazz_app::Result<Vec<Todo>> {
    let query = todos::query()
        .filter(todos::done().eq(false))
        .order_by(todos::title(), Order::Asc);

    jazz.all(query).await
}
// #endregion reading-sorting-rust

// #region reading-pagination-rust
pub async fn read_todo_page(
    jazz: &Jazz,
    page: usize,
    page_size: usize,
) -> jazz_app::Result<Vec<Todo>> {
    let offset = page.saturating_sub(1) * page_size;
    let query = todos::query()
        .filter(todos::done().eq(false))
        .order_by(todos::title(), Order::Asc)
        .order_by(todos::id(), Order::Asc)
        .limit(page_size)
        .offset(offset);

    jazz.all(query).await
}
// #endregion reading-pagination-rust

// #region reading-includes-rust
/// Each todo with its project, and its parent with the parent's project.
pub type TodoWithProjects = (Todo, Option<Project>, Option<(Todo, Option<Project>)>);

pub async fn read_todos_with_includes(jazz: &Jazz) -> jazz_app::Result<Vec<TodoWithProjects>> {
    let query = todos::query()
        .filter(todos::done().eq(false))
        .include(todos::project())
        .include(todos::parent().with(todos::query().include(todos::project())));

    jazz.all(query).await
}
// #endregion reading-includes-rust

// #region reading-reverse-relation-rust
pub async fn read_projects_with_todos(jazz: &Jazz) -> jazz_app::Result<Vec<(Project, Vec<Todo>)>> {
    let open_todos = todos::query().filter(todos::done().eq(false));
    let query = projects::query().include(projects::todos().with(open_todos));

    jazz.all(query).await
}
// #endregion reading-reverse-relation-rust

// #region reading-require-includes-rust
pub async fn read_orders_with_required_customer(
    jazz: &Jazz,
) -> jazz_app::Result<Vec<(required_references::Order, required_references::Customer)>> {
    // `require` keeps only the orders whose customer is there, and includes
    // a `Customer` rather than an `Option<Customer>`.
    let query = required_references::orders::query()
        .include(required_references::orders::customer().require());

    jazz.all(query).await
}
// #endregion reading-require-includes-rust

// #region reading-select-rust
pub async fn read_todo_titles_with_selected_project(
    jazz: &Jazz,
) -> jazz_app::Result<Vec<(Partial<Todo, String>, Option<Partial<Project, String>>)>> {
    // `select` reads only some columns: each row is a `Partial`, with its id.
    let query = todos::query()
        .select(todos::title())
        .filter(todos::done().eq(false))
        .include(todos::project().with(projects::query().select(projects::name())));

    jazz.all(query).await
}
// #endregion reading-select-rust

// #region reading-recursive-rust
pub fn build_todo_lineage_query() -> Query<Todo> {
    todos::query()
        .filter(todos::done().eq(false))
        .gather(todos::parent(), 10)
}
// #endregion reading-recursive-rust

// #region reading-composing-queries-rust
pub fn compose_todo_queries() -> [Query<Todo>; 3] {
    // Store a base query and reuse it for different views.
    let open_todos = todos::query().filter(todos::done().eq(false));

    let by_newest = open_todos.clone().order_by(todos::id(), Order::Desc);
    let by_title = open_todos
        .clone()
        .order_by(todos::title(), Order::Asc)
        .limit(20);
    let urgent = open_todos.filter(todos::title().contains("urgent"));

    [by_newest, by_title, urgent]
}
// #endregion reading-composing-queries-rust

pub async fn where_operator_examples(
    jazz: &Jazz,
    search_term: &str,
    todo_id_a: ObjectId,
    todo_id_b: ObjectId,
) -> jazz_app::Result<()> {
    // #region where-eq-ne-rust
    // Exact match
    let incomplete_todos = jazz
        .all(todos::query().filter(todos::done().eq(false)))
        .await?;

    // Not equal
    let non_draft_todos = jazz
        .all(todos::query().filter(todos::title().ne("Draft")))
        .await?;

    // One of a set
    let selected_todos = jazz
        .all(todos::query().filter(todos::id().is_in([todo_id_a, todo_id_b])))
        .await?;
    // #endregion where-eq-ne-rust

    // #region where-numeric-rust
    // Every table has `created_at()` and `updated_at()` columns.
    let one_week_ago = Timestamp(Timestamp::now().0 - 7 * 24 * 60 * 60 * 1000);

    let recent_todos = jazz
        .all(todos::query().filter(jazz_app::created_at().gt(one_week_ago)))
        .await?;
    let high_priority = jazz
        .all(todos::query().filter(todos::priority().gte(3)))
        .await?;
    let low_priority = jazz
        .all(todos::query().filter(todos::priority().lt(10)))
        .await?;
    // #endregion where-numeric-rust

    // #region where-contains-rust
    // Substring match (case-sensitive)
    let matches = jazz
        .all(todos::query().filter(todos::title().contains(search_term)))
        .await?;
    // #endregion where-contains-rust

    // #region where-null-rust
    // Rows where the optional ref is not set
    let unlinked_todos = jazz
        .all(todos::query().filter(todos::parent_id().is_null()))
        .await?;

    // Rows where it is set
    let linked_todos = jazz
        .all(todos::query().filter(todos::parent_id().is_not_null()))
        .await?;
    // #endregion where-null-rust

    // #region where-and-rust
    // Multiple filter calls are AND-combined: done AND assigned to a project
    let done_with_project = jazz
        .all(
            todos::query()
                .filter(todos::done().eq(true))
                .filter(todos::project_id().is_not_null()),
        )
        .await?;
    // #endregion where-and-rust

    // #region where-order-limit-rust
    let recent_incomplete = jazz
        .all(
            todos::query()
                .filter(todos::done().eq(false))
                .order_by(jazz_app::created_at(), Order::Asc)
                .limit(50),
        )
        .await?;
    // #endregion where-order-limit-rust

    let _ = (
        incomplete_todos,
        non_draft_todos,
        selected_todos,
        recent_todos,
        high_priority,
        low_priority,
        matches,
        unlinked_todos,
        linked_todos,
        done_with_project,
        recent_incomplete,
    );
    Ok(())
}

// #region where-subscription-rust
pub async fn subscribe_open_todos(jazz: &Jazz) -> jazz_app::Result<Subscription<Todo>> {
    // Dropping the subscription unsubscribes.
    jazz.subscribe(todos::query().filter(todos::done().eq(false)))
        .await
}
// #endregion where-subscription-rust

// #region writing-crud-rust
pub async fn write_todo_crud(jazz: &Jazz, todo_id: ObjectId) -> jazz_app::Result<()> {
    jazz.insert(NewTodo {
        owner_id: Some(ObjectId::from_uuid(EXAMPLE_OWNER_ID)),
        project_id: Some(ObjectId::from_uuid(EXAMPLE_PROJECT_ID)),
        ..NewTodo::new("Write docs", false)
    })
    .await?;
    jazz.update(
        todo_id,
        TodoUpdate {
            done: Some(true),
            ..Default::default()
        },
    )
    .await?;
    jazz.delete::<Todo>(todo_id).await
}
// #endregion writing-crud-rust

// #region writing-nullable-update-rust
pub async fn clear_nullable_todo_fields(jazz: &Jazz, todo_id: ObjectId) -> jazz_app::Result<()> {
    jazz.update(
        todo_id,
        TodoUpdate {
            // `Some(None)` sets a nullable column to null.
            owner_id: Some(None),
            // `None` leaves a column unchanged.
            description: None,
            ..Default::default()
        },
    )
    .await
}
// #endregion writing-nullable-update-rust

// #region writing-upsert-rust
pub async fn upsert_todo(jazz: &Jazz, imported_todo_id: ObjectId) -> jazz_app::Result<Todo> {
    // A durable handle's writes return once that tier has them.
    jazz.durable(DurabilityTier::GlobalServer)
        .upsert(imported_todo_id, NewTodo::new("Imported task", false))
        .await
}
// #endregion writing-upsert-rust

// #region writing-durability-tier-rust
pub async fn write_todo_with_durability_tiers(jazz: &Jazz) -> jazz_app::Result<()> {
    // A durable handle's writes return once the server accepted them, and
    // fail if it refused them.
    let global = jazz.durable(DurabilityTier::GlobalServer);

    let todo = global
        .insert(NewTodo {
            owner_id: Some(ObjectId::from_uuid(EXAMPLE_OWNER_ID)),
            project_id: Some(ObjectId::from_uuid(EXAMPLE_PROJECT_ID)),
            ..NewTodo::new("Write docs with durability tier", false)
        })
        .await?;
    global
        .update(
            todo.id,
            TodoUpdate {
                done: Some(true),
                ..Default::default()
            },
        )
        .await?;
    global.delete::<Todo>(todo.id).await
}
// #endregion writing-durability-tier-rust

// #region writing-transaction-rust
pub async fn group_todo_writes(
    jazz: &Jazz,
    existing_todo_id: ObjectId,
) -> jazz_app::Result<ObjectId> {
    // Committed together if the closure returns `Ok`, rolled back if it
    // returns `Err`. On a durable handle it returns once the commit reached
    // that tier.
    jazz.durable(DurabilityTier::GlobalServer)
        .transaction(move |tx| async move {
            let created = tx.insert(NewTodo {
                owner_id: Some(ObjectId::from_uuid(EXAMPLE_OWNER_ID)),
                project_id: Some(ObjectId::from_uuid(EXAMPLE_PROJECT_ID)),
                ..NewTodo::new("Write transaction docs", false)
            })?;
            tx.update(
                existing_todo_id,
                TodoUpdate {
                    done: Some(true),
                    ..Default::default()
                },
            )?;

            // Reads inside the transaction see its staged writes.
            let staged = tx
                .get::<Todo>(created.id)
                .await?
                .ok_or(jazz_app::Error::NotFound {
                    table: "todos",
                    id: created.id.to_string(),
                })?;
            Ok(staged.id)
        })
        .await
}
// #endregion writing-transaction-rust

// #region writing-mutation-errors-rust
pub async fn insert_todo_and_wait(jazz: &Jazz) -> jazz_app::Result<Option<Todo>> {
    // A durable write returns once the server accepted it, and fails with
    // its verdict if it refused it.
    let inserted = jazz
        .durable(DurabilityTier::GlobalServer)
        .insert(NewTodo {
            owner_id: Some(ObjectId::from_uuid(EXAMPLE_OWNER_ID)),
            project_id: Some(ObjectId::from_uuid(EXAMPLE_PROJECT_ID)),
            ..NewTodo::new("Ship review fixes", false)
        })
        .await;

    match inserted {
        Ok(todo) => Ok(Some(todo)),
        // `code` is TypeScript's: `permission_denied`, `write_rejected`, …
        Err(jazz_app::Error::WriteRejected { code, reason, .. }) => {
            eprintln!("{code}: {reason}");
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
// #endregion writing-mutation-errors-rust

// #region writing-transaction-errors-rust
pub async fn complete_todo_in_transaction(jazz: &Jazz, todo_id: ObjectId) -> jazz_app::Result<()> {
    let committed = jazz
        .durable(DurabilityTier::GlobalServer)
        .transaction(move |tx| async move {
            tx.update(
                todo_id,
                TodoUpdate {
                    done: Some(true),
                    ..Default::default()
                },
            )
        })
        .await;

    match committed {
        Ok(()) => Ok(()),
        Err(jazz_app::Error::WriteRejected { code, reason, .. }) => {
            eprintln!("{code}: {reason}");
            Ok(())
        }
        Err(error) => Err(error),
    }
}
// #endregion writing-transaction-errors-rust

// #region writing-serializable-transaction-rust
pub async fn finish_todo_serializably(
    jazz: &Jazz,
    todo_id: ObjectId,
) -> jazz_app::Result<ObjectId> {
    // Reads see the data as of the transaction's start; the server applies
    // the commit only if nothing it read or wrote changed meanwhile, and
    // refuses it otherwise.
    jazz.serializable_transaction(move |tx| async move {
        let todo = tx
            .get::<Todo>(todo_id)
            .await?
            .ok_or(jazz_app::Error::NotFound {
                table: "todos",
                id: todo_id.to_string(),
            })?;

        tx.update(
            todo.id,
            TodoUpdate {
                done: Some(true),
                ..Default::default()
            },
        )?;
        Ok(todo.id)
    })
    .await
}
// #endregion writing-serializable-transaction-rust

// #region writing-restore-rust
pub async fn restore_deleted_todo(jazz: &Jazz, todo_id: ObjectId) -> jazz_app::Result<Todo> {
    jazz.delete::<Todo>(todo_id).await?;

    // `include_deleted` reads deleted rows too.
    let deleted = jazz
        .one(todos::query().id(todo_id).include_deleted())
        .await?;
    if deleted.is_none() {
        return Err(jazz_app::Error::NotFound {
            table: "todos",
            id: todo_id.to_string(),
        });
    }

    // A restore replaces the whole row, so it takes a full `NewTodo`.
    jazz.restore_with(
        todo_id,
        NewTodo {
            owner_id: Some(ObjectId::from_uuid(EXAMPLE_OWNER_ID)),
            project_id: Some(ObjectId::from_uuid(EXAMPLE_PROJECT_ID)),
            ..NewTodo::new("Restored task", false)
        },
    )
    .await
}
// #endregion writing-restore-rust

// #region writing-manual-transaction-rust
pub async fn stage_todo_across_steps(jazz: &Jazz, should_cancel: bool) -> jazz_app::Result<()> {
    // On a durable handle, `commit` returns once the commit reached that tier.
    let tx = jazz
        .durable(DurabilityTier::GlobalServer)
        .begin_transaction()
        .await?;

    tx.insert(NewTodo {
        owner_id: Some(ObjectId::from_uuid(EXAMPLE_OWNER_ID)),
        project_id: Some(ObjectId::from_uuid(EXAMPLE_PROJECT_ID)),
        ..NewTodo::new("Review staged changes", false)
    })
    .await?;

    if should_cancel {
        return tx.rollback().await;
    }

    tx.commit().await
}
// #endregion writing-manual-transaction-rust

// #region dry-run-permissions-rust
pub async fn can_read_todo(jazz: &Jazz, todo_id: ObjectId) -> jazz_app::Result<Advice> {
    jazz.can_read::<Todo>(todo_id).await
}

pub async fn read_todos_with_delete_permission(jazz: &Jazz) -> jazz_app::Result<Vec<Todo>> {
    let todos = jazz
        .all(todos::query().order_by(todos::title(), Order::Asc))
        .await?;
    let mut deletable = Vec::new();
    for todo in todos {
        if jazz.can_delete::<Todo>(todo.id).await? == Advice::Allowed {
            deletable.push(todo);
        }
    }
    Ok(deletable)
}

pub async fn read_editable_todos(jazz: &Jazz) -> jazz_app::Result<Vec<Todo>> {
    let todos = jazz
        .all(todos::query().order_by(todos::title(), Order::Asc))
        .await?;
    let mut editable = Vec::new();
    for todo in todos {
        let update = TodoUpdate {
            title: Some(todo.title.clone()),
            ..Default::default()
        };
        if jazz.can_update(todo.id, update).await? == Advice::Allowed {
            editable.push(todo);
        }
    }
    Ok(editable)
}

pub async fn can_create_todo(jazz: &Jazz, title: &str) -> jazz_app::Result<Advice> {
    jazz.can_insert(NewTodo::new(title, false)).await
}
// #endregion dry-run-permissions-rust

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
            UntypedQuery::from("uploads")
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
            UntypedQuery::from("files")
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
            UntypedQuery::from("uploads")
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

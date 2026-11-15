//! Computed fields, transforms, enums, defaults and keyword names, through
//! the bindings generated for tests/fixtures/tasks.

#[path = "bindings/tasks.rs"]
mod bindings;

use bindings::{Computed, NewTask, Task, TaskPriority, TaskUpdate, tasks};
use jazz_app::{AppId, Config, Error, Json, Timestamp};

/// Due dates as `YYYY-MM-DD`, stored as midnight UTC.
struct App;

const DAY_MS: u64 = 86_400_000;

impl Computed for App {
    fn tasks_urgent(&self, score: i32, priority: TaskPriority) -> bool {
        score > 5 || priority == TaskPriority::High
    }

    fn iso_date_view(&self, stored: Timestamp) -> String {
        // Days since 1970-01-01 is all this test needs.
        format!("day-{}", stored.0 / DAY_MS)
    }

    fn iso_date_stored(&self, view: String) -> Timestamp {
        let day: u64 = view.trim_start_matches("day-").parse().unwrap();
        Timestamp(day * DAY_MS)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn computed_fields_and_transforms() {
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();

    let task = jazz
        .insert(NewTask {
            r#type: Some("chore".into()),
            due_at: Some("day-20000".into()),
            meta: Some(Json(serde_json::json!({ "owner": "ops" }))),
            ..NewTask::new("Ship it", 3, vec!["release".to_owned()])
        })
        .await
        .unwrap();
    assert_eq!(task.priority, TaskPriority::Low, "the column's default");
    assert!(!task.urgent);
    assert_eq!(task.due_at.as_deref(), Some("day-20000"));
    assert_eq!(task.tags, ["release"]);

    jazz.update(
        task.id,
        TaskUpdate {
            priority: Some(TaskPriority::InProgress),
            score: Some(9),
            due_at: Some(None),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let task: Task = jazz.get(task.id).await.unwrap().unwrap();
    assert_eq!(task.priority, TaskPriority::InProgress);
    assert!(task.urgent, "recomputed from the new score");
    assert_eq!(task.due_at, None);
    assert_eq!(task.r#type.as_deref(), Some("chore"));

    // Transformed columns compare as stored.
    jazz.insert(NewTask {
        due_at: Some("day-20100".into()),
        ..NewTask::new("Later", 0, Vec::new())
    })
    .await
    .unwrap();
    let late = jazz
        .all(tasks::query().filter(tasks::due_at().gt(Timestamp(20_050 * DAY_MS))))
        .await
        .unwrap();
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].title, "Later");

    let in_progress = jazz
        .all(tasks::query().filter(tasks::priority().eq(TaskPriority::InProgress)))
        .await
        .unwrap();
    assert_eq!(in_progress, vec![task]);
}

#[tokio::test]
async fn computed_fields_need_their_implementation() {
    let jazz = jazz_app::Jazz::open(
        jazz_app::AppFiles::new(bindings::APP, None),
        Config::new(AppId::random()),
    )
    .await
    .unwrap();
    let refused = jazz
        .insert(NewTask::new("No implementation", 0, Vec::new()))
        .await;
    assert!(matches!(refused, Err(Error::Config(_))), "{refused:?}");
}

/// A column named `query` keeps its field and gets `query_column()`.
#[tokio::test]
async fn a_column_named_query() {
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let labelled = jazz
        .insert(bindings::NewBox {
            query: Some("where?".into()),
            ..bindings::NewBox::new("left")
        })
        .await
        .unwrap();
    assert_eq!(labelled.query.as_deref(), Some("where?"));
    let found = jazz
        .all(bindings::boxes::query().filter(bindings::boxes::query_column().is_not_null()))
        .await
        .unwrap();
    assert_eq!(found, vec![labelled]);
}

/// A text-merge column's edit made from an older value merges over the
/// writes the editor didn't see; on an unchanged value it is a plain set.
#[tokio::test]
async fn text_edits_merge() {
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let task = jazz
        .insert(bindings::NewTask {
            notes: Some("hello world".into()),
            ..NewTask::new("Notes", 0, Vec::new())
        })
        .await
        .unwrap();
    let notes = || async {
        jazz.get::<Task>(task.id)
            .await
            .unwrap()
            .unwrap()
            .notes
            .unwrap()
    };

    // Someone else changes the notes...
    jazz.update(
        task.id,
        bindings::TaskUpdate {
            notes: Some(Some("hello brave world".into())),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // ...while an editor, who still shows "hello world", appends "!".
    jazz.edit_text(task.id, tasks::notes(), "hello world", "hello world!")
        .await
        .unwrap();
    assert_eq!(notes().await, "hello brave world!");

    // From the current value, an edit is just the new value.
    jazz.edit_text(task.id, tasks::notes(), "hello brave world!", "bye")
        .await
        .unwrap();
    assert_eq!(notes().await, "bye");

    // And inside a transaction.
    let id = task.id;
    jazz.transaction(move |tx| async move { tx.edit_text(id, tasks::notes(), "bye", "bye now") })
        .await
        .unwrap();
    assert_eq!(notes().await, "bye now");

    // A column without "merge": "text" is refused.
    let plain = jazz
        .edit_text(task.id, tasks::title(), "Notes", "Renamed")
        .await;
    assert!(plain.is_err(), "{plain:?}");
    // A row that isn't there.
    let missing = jazz
        .edit_text(jazz_app::ObjectId::new(), tasks::notes(), "", "x")
        .await;
    assert!(
        matches!(missing, Err(Error::NotFound { .. })),
        "{missing:?}"
    );
    // A stale base at the other end merges too: the edit keeps the other
    // writer's change at the start.
    jazz.update(
        task.id,
        bindings::TaskUpdate {
            notes: Some(Some("well, bye now".into())),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    jazz.edit_text(task.id, tasks::notes(), "bye now", "bye now, friend")
        .await
        .unwrap();
    assert_eq!(notes().await, "well, bye now, friend");
}

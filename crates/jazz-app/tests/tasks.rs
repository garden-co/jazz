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
            title: "Ship it".into(),
            score: 3,
            r#type: Some("chore".into()),
            due_at: Some("day-20000".into()),
            tags: vec!["release".into()],
            meta: Some(Json(serde_json::json!({ "owner": "ops" }))),
            ..Default::default()
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
        title: "Later".into(),
        due_at: Some("day-20100".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let late = jazz
        .all(tasks::query().filter(tasks::due_at().gt(Some(Timestamp(20_050 * DAY_MS)))))
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
        .insert(NewTask {
            title: "No implementation".into(),
            ..Default::default()
        })
        .await;
    assert!(matches!(refused, Err(Error::Config(_))), "{refused:?}");
}

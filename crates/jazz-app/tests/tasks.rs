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

    fn tasks_title_len(&self, title: String) -> i32 {
        title.chars().count() as i32
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

/// A query can read a large value only in part: a text range, or a json
/// subtree, of the full row or of a selection.
#[tokio::test]
async fn partial_reads() {
    use jazz_app::ValuePart;
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let task = jazz
        .insert(NewTask {
            meta: Some(Json(
                serde_json::json!({ "owner": { "name": "ops" }, "tags": ["a"] }),
            )),
            r#type: Some("Partial reads".into()),
            ..NewTask::new("héllo wörld", 0, Vec::new())
        })
        .await
        .unwrap();
    let head = jazz
        .one(
            tasks::query()
                .id(task.id)
                .part(tasks::r#type(), ValuePart::TextUtf8 { from: 0, to: 7 }),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.r#type.as_deref(), Some("Partial"));

    // A column a computed field is computed from is read whole, so the
    // computed field is right (as TS re-reads it in full).
    let computed = jazz
        .one(
            tasks::query()
                .id(task.id)
                .part(tasks::title(), ValuePart::TextUtf8 { from: 0, to: 3 }),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (computed.title.as_str(), computed.title_len),
        ("héllo wörld", 11)
    );

    // Inside an include, the included rows' column is read in part.
    let boxed = jazz
        .insert(bindings::NewBox {
            task: Some(task.id),
            ..bindings::NewBox::new("crate")
        })
        .await
        .unwrap();
    let (_, included) = jazz
        .one(
            bindings::boxes::query()
                .id(boxed.id)
                .include(bindings::boxes::task_relation().with(
                    tasks::query().part(tasks::r#type(), ValuePart::TextUtf8 { from: 0, to: 4 }),
                )),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(included.unwrap().r#type.as_deref(), Some("Part"));
    let owner = jazz
        .one(
            tasks::query()
                .id(task.id)
                .select(tasks::meta())
                .part(tasks::meta(), ValuePart::JsonPointer("/owner".into())),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        owner.values.unwrap().0,
        serde_json::json!({ "name": "ops" })
    );
}

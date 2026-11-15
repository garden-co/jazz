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

/// Large values: a bytes cell inserted from a stream, spliced in place, and a json cell edited by pointer.
#[tokio::test]
async fn large_values() {
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    let task = jazz
        .insert_streaming(
            NewTask {
                meta: Some(Json(serde_json::json!({ "owner": "ops", "tags": ["a"] }))),
                ..NewTask::new("Upload", 0, Vec::new())
            },
            tasks::attachment(),
            chunked(&data, 64 * 1024),
        )
        .await
        .unwrap();
    assert_eq!(task.attachment.as_deref(), Some(data.as_slice()));
    jazz.splice(
        task.id,
        tasks::attachment(),
        jazz_app::Page::Bytes { from: 0, to: 4 },
        vec![jazz_app::Splice {
            at: 1,
            delete: 2,
            insert: vec![7, 7, 7],
        }],
    )
    .await
    .unwrap();
    let attachment = jazz
        .get::<Task>(task.id)
        .await
        .unwrap()
        .unwrap()
        .attachment
        .unwrap();
    assert_eq!(attachment[..6], [data[0], 7, 7, 7, data[3], data[4]]);
    assert_eq!(attachment.len(), data.len() + 1);

    jazz.json_set(
        task.id,
        tasks::meta(),
        [("/owner".to_owned(), serde_json::json!("dev"))],
    )
    .await
    .unwrap();
    let meta = jazz
        .get::<Task>(task.id)
        .await
        .unwrap()
        .unwrap()
        .meta
        .unwrap();
    assert_eq!(meta.0, serde_json::json!({ "owner": "dev", "tags": ["a"] }));
}

/// `data` as a stream of `size`-byte chunks.
fn chunked(
    data: &[u8],
    size: usize,
) -> impl futures::Stream<Item = Result<Vec<u8>, std::convert::Infallible>> + use<> {
    let chunks: Vec<_> = data.chunks(size).map(|chunk| Ok(chunk.to_vec())).collect();
    futures::stream::iter(chunks)
}

/// A slow stream doesn't hold up the client's other calls: each chunk is
/// stored as it arrives, in its own turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_upload_lets_other_calls_run() {
    use futures::StreamExt;
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let slow = futures::stream::iter(0..4).then(|i| async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Ok::<_, std::convert::Infallible>(vec![i as u8; 1024])
    });
    let reader = jazz.clone();
    let (uploaded, read) = tokio::join!(
        jazz.insert_streaming(
            NewTask::new("Slow", 0, Vec::new()),
            tasks::attachment(),
            slow
        ),
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let started = std::time::Instant::now();
            reader.all(tasks::query()).await.unwrap();
            started.elapsed()
        }
    );
    assert_eq!(uploaded.unwrap().attachment.unwrap().len(), 4 * 1024);
    assert!(
        read < std::time::Duration::from_millis(150),
        "a read waited {read:?}"
    );
}

/// What goes wrong with a large value fails cleanly and writes nothing.
#[tokio::test]
async fn large_value_errors() {
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();

    // The stream fails: the upload is abandoned.
    let failing = futures::stream::iter([Ok(vec![1u8; 10]), Err("disk on fire")]);
    let failed = jazz
        .insert_streaming(
            NewTask::new("Broken", 0, Vec::new()),
            tasks::attachment(),
            failing,
        )
        .await;
    assert!(
        matches!(&failed, Err(Error::Upload(message)) if message == "disk on fire"),
        "{failed:?}"
    );
    assert!(jazz.all(tasks::query()).await.unwrap().is_empty());

    // The streamed column is the stream's to set.
    let both = jazz
        .insert_streaming(
            NewTask {
                attachment: Some(vec![1, 2, 3]),
                ..NewTask::new("Both", 0, Vec::new())
            },
            tasks::attachment(),
            chunked(&[4, 5, 6], 2),
        )
        .await;
    assert!(matches!(both, Err(Error::Config(_))), "{both:?}");

    let task = jazz
        .insert(NewTask {
            attachment: Some(vec![0; 8]),
            meta: Some(Json(serde_json::json!({ "owner": "ops" }))),
            ..NewTask::new("Edits", 0, Vec::new())
        })
        .await
        .unwrap();
    // A pointer to a member that isn't there.
    let pointer = jazz
        .json_set(
            task.id,
            tasks::meta(),
            [("/missing/deep".to_owned(), serde_json::json!(1))],
        )
        .await;
    assert!(pointer.is_err(), "{pointer:?}");
    // A page past the value's end.
    let page = jazz
        .splice(
            task.id,
            tasks::attachment(),
            jazz_app::Page::Bytes { from: 100, to: 200 },
            vec![jazz_app::Splice {
                at: 0,
                delete: 1,
                insert: vec![9],
            }],
        )
        .await;
    assert!(page.is_err(), "{page:?}");
    let missing = jazz
        .json_set(
            jazz_app::ObjectId::new(),
            tasks::meta(),
            [("/owner".to_owned(), serde_json::json!(1))],
        )
        .await;
    assert!(
        matches!(missing, Err(Error::NotFound { .. })),
        "{missing:?}"
    );
    let unchanged = jazz.get::<Task>(task.id).await.unwrap().unwrap();
    // In a transaction, with everything it staged.
    let id = task.id;
    jazz.transaction(move |tx| async move {
        tx.json_set(
            id,
            tasks::meta(),
            [("/owner".to_owned(), serde_json::json!("tx"))],
        )?;
        tx.splice(
            id,
            tasks::attachment(),
            jazz_app::Page::Bytes { from: 0, to: 8 },
            vec![jazz_app::Splice {
                at: 0,
                delete: 0,
                insert: vec![1],
            }],
        )
    })
    .await
    .unwrap();
    let edited = jazz.get::<Task>(task.id).await.unwrap().unwrap();
    assert_eq!(edited.meta.unwrap().0, serde_json::json!({ "owner": "tx" }));
    assert_eq!(edited.attachment.unwrap()[..2], [1, 0]);
    assert_eq!(unchanged.attachment, Some(vec![0; 8]));
    assert_eq!(
        unchanged.meta.unwrap().0,
        serde_json::json!({ "owner": "ops" })
    );

    // Streaming into an existing row, and upserting.
    let updated = jazz
        .update_streaming(
            task.id,
            bindings::TaskUpdate::default(),
            tasks::attachment(),
            chunked(&[7; 5], 2),
        )
        .await
        .unwrap();
    assert_eq!(updated.attachment, Some(vec![7; 5]));
    let id = jazz_app::ObjectId::new();
    let upserted = jazz
        .upsert_streaming(
            id,
            NewTask::new("Upserted", 0, Vec::new()),
            tasks::attachment(),
            chunked(&[3; 3], 1),
        )
        .await
        .unwrap();
    assert_eq!((upserted.id, upserted.attachment), (id, Some(vec![3; 3])));
}

/// A streaming call dropped mid-stream aborts its upload, and a chunk larger
/// than a push is split.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_upload_is_aborted() {
    use futures::StreamExt;
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let endless = futures::stream::iter(0..).then(|_| async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        Ok::<_, std::convert::Infallible>(vec![1u8; 100])
    });
    let upload = jazz.insert_streaming(
        NewTask::new("Endless", 0, Vec::new()),
        tasks::attachment(),
        endless,
    );
    let timed_out = tokio::time::timeout(std::time::Duration::from_millis(100), upload).await;
    assert!(timed_out.is_err());
    // The abort was sent when the call was dropped; it runs before this.
    assert_eq!(jazz.uploads_in_progress().await.unwrap(), 0);
    assert!(jazz.all(tasks::query()).await.unwrap().is_empty());

    // One 1 MiB chunk, pushed in pieces.
    let big = vec![5u8; 1024 * 1024];
    let task = jazz
        .insert_streaming(
            NewTask::new("Big", 0, Vec::new()),
            tasks::attachment(),
            chunked(&big, big.len()),
        )
        .await
        .unwrap();
    assert_eq!(task.attachment.unwrap().len(), big.len());
    assert_eq!(jazz.uploads_in_progress().await.unwrap(), 0);
}

/// Shutting down mid-upload ends the upload (`Stopped`) and leaves the
/// client thread exiting cleanly: the parked upload must not be dropped
/// with the thread's locals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_mid_upload() {
    use futures::StreamExt;
    let jazz = bindings::open(Config::new(AppId::random()), App)
        .await
        .unwrap();
    let endless = futures::stream::iter(0..).then(|_| async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        Ok::<_, std::convert::Infallible>(vec![1u8; 100])
    });
    let uploader = jazz.clone();
    let upload = tokio::spawn(async move {
        uploader
            .insert_streaming(
                NewTask::new("Endless", 0, Vec::new()),
                tasks::attachment(),
                endless,
            )
            .await
    });
    while jazz.uploads_in_progress().await.unwrap() == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    jazz.shutdown().await.unwrap();
    assert!(matches!(upload.await.unwrap(), Err(Error::Stopped)));
    // Let the client thread finish exiting (it aborted the process here).
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
}

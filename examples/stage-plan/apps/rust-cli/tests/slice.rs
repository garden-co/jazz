//! The StagePlan slice end to end, in-process: main-backend mode against
//! `jazz-server` stands in for the TypeScript main backend (the CLI itself is
//! a backend client: `examples/rust-backend-clients` runs it against
//! `createJazz`), acting as a signed-in user under StagePlan's own
//! permissions.

use std::time::Duration;

use ::jazz_app_support::{AppId, Backend, Config, Jazz, ReadTier, Storage};
use jazz_server::JazzServer;
use stage_plan_rust_cli::jazz_app::{self, TaskStatus, tasks};
use stage_plan_rust_cli::{ShowDetails, add_task, ensure_show, me, move_task, tasks_of};
use tempfile::TempDir;

const APP: &str = include_str!("../jazz.app.json");
const PERMISSIONS: &str = include_str!("../jazz.permissions.json");

async fn open(server: &JazzServer, app_id: AppId, user: &str, data: &TempDir) -> Jazz {
    let backend = Backend::new(server.backend_secret(), server.admin_secret(), user)
        .permissions(PERMISSIONS.as_bytes());
    jazz_app::open(
        Config::new(app_id)
            .server(server.base_url())
            .storage(Storage::Dir(data.path().to_path_buf()))
            .backend(backend),
    )
    .await
    .expect("open StagePlan as its backend")
}

fn details(name: &str) -> ShowDetails {
    ShowDetails {
        name: name.into(),
        venue: "The Roundhouse".into(),
        date: "2026-05-01".into(),
        doors: "19:00".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chief_plans_a_show() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let data = TempDir::new().unwrap();
    let jazz = open(&server, app_id, "alice", &data).await;

    // Create the show, once: the second call finds it.
    let show = ensure_show(&jazz, details("Spring tour")).await.unwrap();
    assert_eq!(show.chief_account, me(&jazz).unwrap());
    assert_eq!(
        ensure_show(&jazz, details("Spring tour")).await.unwrap(),
        show
    );

    // Tasks come back by rank; each lands at the end of its stage.
    let van = add_task(&jazz, show.id, "Book the van", TaskStatus::Todo)
        .await
        .unwrap();
    let setlist = add_task(&jazz, show.id, "Print setlists", TaskStatus::Todo)
        .await
        .unwrap();
    let soundcheck = add_task(&jazz, show.id, "Soundcheck", TaskStatus::Doing)
        .await
        .unwrap();
    assert_eq!((van.rank, setlist.rank, soundcheck.rank), (1.0, 2.0, 1.0));
    let titles =
        |tasks: &[jazz_app::Task]| tasks.iter().map(|t| t.title.clone()).collect::<Vec<_>>();
    assert_eq!(
        titles(&tasks_of(&jazz, show.id).await.unwrap()),
        ["Book the van", "Soundcheck", "Print setlists"]
    );

    // Moving a task puts it after the stage's last one.
    let moved = move_task(&jazz, van.id, TaskStatus::Doing).await.unwrap();
    assert_eq!((moved.status, moved.rank), (TaskStatus::Doing, 2.0));
    let doing = jazz
        .all(
            tasks::query()
                .filter(tasks::status().eq(TaskStatus::Doing))
                .order_by(tasks::rank(), ::jazz_app_support::Order::Asc),
        )
        .await
        .unwrap();
    assert_eq!(titles(&doing), ["Soundcheck", "Book the van"]);

    // The server accepted every write under StagePlan's permissions.
    let remote = tokio::time::timeout(
        Duration::from_secs(10),
        jazz.all_with(tasks::query(), ReadTier::Remote),
    )
    .await
    .expect("the server answers")
    .unwrap();
    assert_eq!(remote.len(), 3);

    // Another user sees none of it: tasks are for the show's crew.
    let other_data = TempDir::new().unwrap();
    let bob = open(&server, app_id, "bob", &other_data).await;
    let seen = bob
        .all_with(tasks::query(), ReadTier::Remote)
        .await
        .unwrap();
    assert!(seen.is_empty(), "{seen:?}");

    bob.shutdown().await.unwrap();
    jazz.shutdown().await.unwrap();
    server.shutdown().await;
}

/// The committed bindings are what `jazz-tools generate` writes from them.
#[test]
fn bindings_are_current() {
    assert_eq!(
        jazz_app::SOURCE,
        ::jazz_app_support::jazz::tools::deployment::artifact::source_hash(APP.as_bytes()),
        "src/jazz_app.rs is stale: run `npx jazz-tools generate`"
    );
}

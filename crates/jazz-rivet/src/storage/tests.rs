use super::Connection;
use jazz::{
    db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, duplex},
    ids::{AuthorSubject, NodeUuid},
    query::Query,
    records::Value,
    schema::JazzSchema,
    storage::Storage,
    tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder},
};
use rivetkit::{Actor, Ctx, Registry, Request, Response, ServeConfig, prelude::Result};
use std::{collections::BTreeMap, net::TcpListener, process::Command, sync::Arc, time::Duration};
use tokio::runtime::Handle;

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    let notes = TableSchemaBuilder::new("notes")
        .column("title", ColumnType::Text)
        .policies(
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_delete(PolicyExpr::True),
        );
    let schema = SchemaBuilder::new().table(notes).build();
    DbConfig::new(
        JazzSchema::new(&schema).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn title(s: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".into(), Value::String(s.into()))])
}

async fn titles(db: &Db) -> Vec<String> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let mut titles: Vec<_> = db
        .all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))
        .await
        .unwrap()
        .into_iter()
        .map(|row| match row.cell("title").unwrap() {
            Value::String(s) => s,
            other => panic!("expected text, got {other:?}"),
        })
        .collect();
    titles.sort();
    titles
}

async fn exercise(connection: Connection) {
    let core = Db::open_storage(
        config(1, AuthorSubject::SYSTEM),
        Storage::from_backend(connection.clone()),
    )
    .await
    .unwrap();
    let row = core
        .insert("notes", title("draft"), Default::default())
        .await
        .unwrap()
        .row_uuid();
    assert_eq!(titles(&core).await, ["draft"]);
    core.update("notes", row, title("published"), Default::default())
        .await
        .unwrap();
    assert_eq!(titles(&core).await, ["published"]);
    core.delete("notes", row, Default::default()).await.unwrap();
    assert!(titles(&core).await.is_empty());

    // Clients stage transactions locally and send the finished commit to the managed core.
    let alice = AuthorSubject::for_test_bytes([2; 16]);
    let client = Db::open(config(2, alice)).await.unwrap();
    let (upstream, downstream) = duplex();
    client.connect_upstream(upstream).await;
    core.accept_subscriber(downstream, alice).await.unwrap();
    {
        let aborted = client.mergeable_tx().await.unwrap();
        aborted
            .insert("notes", title("discarded"), Default::default())
            .await
            .unwrap();
        // Dropping an uncommitted Jazz transaction rolls it back.
    }
    let tx = client.mergeable_tx().await.unwrap();
    tx.insert("notes", title("first"), Default::default())
        .await
        .unwrap();
    tx.insert("notes", title("second"), Default::default())
        .await
        .unwrap();
    assert!(titles(&core).await.is_empty());
    tx.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            client.tick().await.unwrap();
            core.tick().await.unwrap();
            let rows = titles(&core).await;
            if !rows.is_empty() {
                assert_eq!(rows, ["first", "second"]);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("transaction reached the Rivet core");
    drop(core);
    let reopened = Db::open_storage(
        config(1, AuthorSubject::SYSTEM),
        Storage::from_backend(connection),
    )
    .await
    .unwrap();
    assert_eq!(titles(&reopened).await, ["first", "second"]);
}

struct TestActor;
#[async_trait::async_trait]
impl Actor for TestActor {
    type Input = ();
    type Actions = ();
    type Events = ();
    type ConnParams = ();
    type Action = rivetkit::action::Raw;
    async fn create(_: &Ctx<Self>) -> Result<Self> {
        Ok(Self)
    }

    async fn on_fetch(self: Arc<Self>, ctx: Ctx<Self>, _: Request) -> Result<Response> {
        let runtime = Handle::current();
        let connection = Connection::new(ctx.sql().clone(), runtime.clone(), move |reason| {
            let _ = ctx.stop_with_error(reason);
        });
        tokio::task::spawn_blocking(move || runtime.block_on(exercise(connection))).await?;
        Response::from_parts(200, Default::default(), vec![])
    }
}

/// Alice commits two rows and discards another. The actor's Db supports ordinary
/// CRUD and a reopened Db reads exactly the committed rows from actor SQLite.
/// Alice → commit → Rivet actor → reopen → read.
#[test]
fn jazz_db_operations_on_actor_sqlite() {
    // RivetKit takes its storage directory from the process environment. Set it
    // only for this subprocess so parallel tests and local runners stay isolated.
    const CHILD: &str = "JAZZ_RIVET_DB_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let temp = tempfile::tempdir().unwrap();
        let port = TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let binary = std::env::var_os("RIVET_ENGINE_BINARY_PATH").unwrap_or_else(|| {
            let output = Command::new("node")
                .args(["-p", "require('@rivetkit/engine-cli').getEnginePath()"])
                .current_dir(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../packages/jazz-tools"
                ))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "install workspace dependencies or set RIVET_ENGINE_BINARY_PATH: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap().trim().into()
        });
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::tests::jazz_db_operations_on_actor_sqlite",
                "--nocapture",
            ])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env(CHILD, "1")
            .env("RIVETKIT_STORAGE_PATH", temp.path())
            .env("RIVET_ENGINE_BINARY_PATH", binary)
            .env("RIVET_RUN_ENGINE_PORT", port.to_string())
            .current_dir(temp.path())
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut registry = Registry::new();
        registry.register_actor::<TestActor>("db-test");
        let config = ServeConfig::from_env().unwrap();
        let client = rivetkit::client::Client::new(
            rivetkit::client::ClientConfig::new(&config.endpoint)
                .token("default")
                .namespace("default")
                .pool_name("default"),
        );
        let shutdown = tokio_util::sync::CancellationToken::new();
        let serving = tokio::spawn(registry.serve_with_config(config, shutdown.clone()));
        let result = tokio::time::timeout(Duration::from_secs(90), async {
            let handle = client
                .get_or_create("db-test", vec!["db".into()], Default::default())
                .unwrap();
            let actor = loop {
                if let Ok(actor) = handle.resolve_handle().await {
                    break actor;
                }
                assert!(
                    !serving.is_finished(),
                    "Rivet registry exited during startup"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            };
            actor
                .fetch("/", "GET".parse().unwrap(), Default::default(), None)
                .await
        })
        .await;
        shutdown.cancel();
        serving.await.unwrap().unwrap();
        let response = result.unwrap().unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
    });
}

//! Per-column last-writer-wins by stamp.
//!
//! Core merges every accepted write into the row one column at a time. Each
//! plain column remembers the stamp of the write that last set it; a write
//! sets a column it authored only when its stamp is at least that stamp. A
//! write's stamp is its time, clamped by Core to when Core received it, so a
//! client with a clock running ahead cannot make its values immune to later
//! edits.
//!
//! Every write below carries an explicit physical timestamp through the public
//! `WriteContext::with_updated_at`, which is the client's clock for that write.

use jazz_testkit as support;

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jazz::query::Query;
use jazz::tools::test_support::{disconnect_client, reconnect_client};
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, SchemaBuilder, TableSchema, TransactionId,
    Value, WriteContext,
};
use jazz_server::JazzServer;
use support::{TestingClient, has_row, wait_for_rows};
use uuid::Uuid;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn todo_schema() -> jazz::tools::Schema {
    use jazz::tools::test_support::AllowAll;
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .allow_all()
        .build()
}

async fn connect(server: &JazzServer, user_id: &str) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(todo_schema())
        .with_user_id(user_id)
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the Unix epoch")
        .as_millis() as u64
}

/// A client whose clock reads `at_ms` for its next write.
fn at(client: &JazzClient, at_ms: u64) -> JazzClient {
    client.with_write_context(WriteContext::default().with_updated_at(at_ms))
}

async fn settle(client: &JazzClient, transaction_id: Option<TransactionId>, tier: DurabilityTier) {
    client
        .wait_for_transaction(
            transaction_id.expect("ordinary mutation commits immediately"),
            tier,
        )
        .await
        .expect("write settles");
}

fn todo(title: &str, done: bool) -> Vec<Value> {
    vec![Value::Text(title.to_owned()), Value::Boolean(done)]
}

/// alice creates the todo at `created_at` and both clients see it. Inserts
/// take no explicit timestamp, so the todo is created by an upsert.
async fn seed_todo(alice: &JazzClient, bob: &JazzClient, created_at: u64) -> ObjectId {
    // Each test runs its own server, so a fixed row id cannot collide.
    let row_uuid = Uuid::from_u128(0x5eed_70d0);
    let todo_id = ObjectId::from_uuid(row_uuid);
    let transaction_id = at(alice, created_at)
        .upsert(
            "todos",
            row_uuid,
            HashMap::from([
                ("title".to_owned(), Value::Text("draft".to_owned())),
                ("done".to_owned(), Value::Boolean(false)),
            ]),
        )
        .expect("alice creates the todo");
    settle(alice, transaction_id, DurabilityTier::GlobalServer).await;
    for (client, who) in [(alice, "alice"), (bob, "bob")] {
        wait_for_rows(
            client,
            Query::from("todos"),
            format!("{who} sees the seeded todo"),
            |rows| has_row(&rows, todo_id, &todo("draft", false)).then_some(()),
        )
        .await;
    }
    todo_id
}

async fn expect_todo(client: &JazzClient, who: &str, todo_id: ObjectId, expected: Vec<Value>) {
    wait_for_rows(
        client,
        Query::from("todos"),
        format!("{who} converges on {expected:?}"),
        |rows| has_row(&rows, todo_id, &expected).then_some(()),
    )
    .await;
}

/// Two offline edits to different columns of one row both survive.
///
/// Actors: alice and bob both go offline; alice retitles the todo, bob marks
/// it done. Whatever order Core receives them in, each column keeps its own
/// writer's value.
///
/// ```text
/// alice ─offline─ title="alice's title" ─┐
///                                        ├─reconnect─► server ─► {alice's title, done}
/// bob   ─offline─ done=true ─────────────┘
/// ```
#[tokio::test]
async fn offline_edits_to_different_columns_both_survive() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start_with_schema(todo_schema())
                .await
                .expect("start test server");
            let alice = connect(&server, "alice-column-stamps").await;
            let bob = connect(&server, "bob-column-stamps").await;
            // Every timestamp stays in Core's past, so none of them is clamped.
            let base = wall_ms() - 60_000;
            let todo_id = seed_todo(&alice, &bob, base).await;

            assert!(disconnect_client(&alice), "alice goes offline");
            assert!(disconnect_client(&bob), "bob goes offline");
            let alice_edit = at(&alice, base + 1_000)
                .update(
                    "todos",
                    todo_id,
                    vec![("title".to_owned(), Value::Text("alice's title".to_owned()))],
                )
                .expect("alice retitles offline");
            settle(&alice, alice_edit, DurabilityTier::Local).await;
            let bob_edit = at(&bob, base + 2_000)
                .update(
                    "todos",
                    todo_id,
                    vec![("done".to_owned(), Value::Boolean(true))],
                )
                .expect("bob marks done offline");
            settle(&bob, bob_edit, DurabilityTier::Local).await;

            // bob's newer write reaches Core first; alice's older one still
            // sets the column bob did not touch.
            assert!(reconnect_client(&bob).await.expect("bob reconnects"));
            settle(&bob, bob_edit, DurabilityTier::GlobalServer).await;
            assert!(reconnect_client(&alice).await.expect("alice reconnects"));
            settle(&alice, alice_edit, DurabilityTier::GlobalServer).await;

            for (client, who) in [(&alice, "alice"), (&bob, "bob")] {
                expect_todo(client, who, todo_id, todo("alice's title", true)).await;
            }

            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}

/// A late offline write loses the column a newer write already set, while
/// its other columns still apply.
///
/// Actors: alice goes offline and edits both the title and `done`; bob, later
/// in time, retitles the todo and reaches Core first. When alice reconnects
/// her title is older than bob's and loses; her `done` has no newer writer and
/// applies.
///
/// ```text
/// alice ─offline─ t+1s {title="alice (stale)", done=true} ─────reconnect──┐
/// bob   ────────── t+5s {title="bob (newer)"} ──► server                  │
///                                                   ◄────────────────────┘
///                                                   = {bob (newer), done}
/// ```
#[tokio::test]
async fn late_offline_write_loses_newer_column_but_applies_the_rest() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start_with_schema(todo_schema())
                .await
                .expect("start test server");
            let alice = connect(&server, "alice-late-offline").await;
            let bob = connect(&server, "bob-newer-online").await;
            // Every timestamp stays in Core's past, so none of them is clamped.
            let base = wall_ms() - 60_000;
            let todo_id = seed_todo(&alice, &bob, base).await;

            assert!(disconnect_client(&alice), "alice goes offline");
            let alice_edit = at(&alice, base + 1_000)
                .update(
                    "todos",
                    todo_id,
                    vec![
                        ("title".to_owned(), Value::Text("alice (stale)".to_owned())),
                        ("done".to_owned(), Value::Boolean(true)),
                    ],
                )
                .expect("alice edits offline");
            settle(&alice, alice_edit, DurabilityTier::Local).await;

            let bob_edit = at(&bob, base + 5_000)
                .update(
                    "todos",
                    todo_id,
                    vec![("title".to_owned(), Value::Text("bob (newer)".to_owned()))],
                )
                .expect("bob retitles");
            settle(&bob, bob_edit, DurabilityTier::GlobalServer).await;
            expect_todo(&bob, "bob", todo_id, todo("bob (newer)", false)).await;

            assert!(reconnect_client(&alice).await.expect("alice reconnects"));
            settle(&alice, alice_edit, DurabilityTier::GlobalServer).await;

            for (client, who) in [(&alice, "alice"), (&bob, "bob")] {
                expect_todo(client, who, todo_id, todo("bob (newer)", true)).await;
            }

            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}

/// A clock running ahead is clamped to Core's receive time, so a later write
/// from a correct clock still wins.
///
/// Actors: mallory's clock runs 20 s ahead (inside Core's skew tolerance, so
/// the write is accepted). Core stamps her title with the time it received it,
/// not her claimed time. bob, offline and so unaware of mallory's write, then
/// retitles the todo with his correct clock. His stamp is at least Core's
/// receive time of mallory's write, so his title wins. Without the clamp
/// mallory's claimed time would beat bob's for the next 20 seconds.
///
/// ```text
/// mallory ─(clock +20s) title="from the future"──► server  stamp = receive time
/// bob ─offline─ (clock now) title="from the present" ─reconnect─► server
///                                                   = "from the present"
/// ```
#[tokio::test]
async fn clock_ahead_is_clamped_so_a_later_write_still_wins() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start_with_schema(todo_schema())
                .await
                .expect("start test server");
            let mallory = connect(&server, "mallory-clock-ahead").await;
            let bob = connect(&server, "bob-clock-correct").await;
            let todo_id = seed_todo(&mallory, &bob, wall_ms()).await;

            assert!(disconnect_client(&bob), "bob goes offline");
            let future_edit = at(&mallory, wall_ms() + 20_000)
                .update(
                    "todos",
                    todo_id,
                    vec![(
                        "title".to_owned(),
                        Value::Text("from the future".to_owned()),
                    )],
                )
                .expect("mallory retitles with a clock ahead");
            settle(&mallory, future_edit, DurabilityTier::GlobalServer).await;
            expect_todo(&mallory, "mallory", todo_id, todo("from the future", false)).await;

            // Read bob's clock only after Core has accepted mallory's write.
            let present_edit = at(&bob, wall_ms())
                .update(
                    "todos",
                    todo_id,
                    vec![(
                        "title".to_owned(),
                        Value::Text("from the present".to_owned()),
                    )],
                )
                .expect("bob retitles offline");
            settle(&bob, present_edit, DurabilityTier::Local).await;
            assert!(reconnect_client(&bob).await.expect("bob reconnects"));
            settle(&bob, present_edit, DurabilityTier::GlobalServer).await;

            for (client, who) in [(&mallory, "mallory"), (&bob, "bob")] {
                expect_todo(client, who, todo_id, todo("from the present", false)).await;
            }

            mallory.shutdown().await.expect("shutdown mallory");
            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}

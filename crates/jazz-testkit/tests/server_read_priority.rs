//! The server takes a session's reads out of turn only when it has nothing
//! but reads queued: reads keep their place behind their own session's
//! commits while other sessions write, and an idle reader is answered
//! correctly beside them.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use jazz::query::{Query, col, eq, lit};
use jazz::records::Value as CoreValue;
use jazz::row_input;
use jazz::tools::sync::ReadTier;
use jazz::tools::{ColumnType, JazzClient, Schema, SchemaBuilder, TableSchema, permissions};
use jazz_server::JazzServer;
use jazz_testkit::TestingClient;

const TIMEOUT: Duration = Duration::from_secs(30);
const ROUNDS: usize = 20;

fn schema() -> Schema {
    let table = TableSchema::builder("notes")
        .column("body", ColumnType::Text)
        .policies(permissions(|p| {
            p.allow_insert().always();
            p.allow_read().always();
        }));
    SchemaBuilder::new().table(table).build()
}

/// Keep `writer` inserting notes until `stop` is set, without waiting for
/// their settlement, so its session always has commits queued.
fn flood(writer: Rc<JazzClient>, stop: Rc<Cell<bool>>) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while !stop.get() {
            writer
                .insert("notes", row_input!("body" => "flood"))
                .expect("flood insert");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
}

async fn read_note(reader: &JazzClient, note: uuid::Uuid) -> Vec<jazz::tools::QueryResult> {
    let query = Query::from("notes").filter(eq(col("id"), lit(CoreValue::Uuid(note))));
    tokio::time::timeout(TIMEOUT, reader.query(query, ReadTier::Remote))
        .await
        .expect("remote read answered in time")
        .expect("remote read")
}

/// Alice writes a note and reads it back from the server at once, while
/// bob and carol keep their sessions full of commits. Her read follows her
/// commit in her session's queue, so the server never takes it ahead of
/// the commit (INV-SYNC-49): every read sees the note just written.
///
/// ```text
/// bob, carol ──commit, commit, …──► server
/// alice ──commit(note)──► server
/// alice ──read(note)────► server ──► [note]
/// ```
#[tokio::test]
async fn a_read_after_its_own_commit_sees_it_while_others_write() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let connect = |user: &'static str| {
                TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema.clone())
                    .with_user_id(user)
                    .as_user()
                    .connect()
            };
            let alice = connect("alice").await;
            let bob = Rc::new(connect("bob").await);
            let carol = Rc::new(connect("carol").await);
            let stop = Rc::new(Cell::new(false));
            let floods = [
                flood(Rc::clone(&bob), Rc::clone(&stop)),
                flood(Rc::clone(&carol), Rc::clone(&stop)),
            ];

            for round in 0..ROUNDS {
                let body = format!("alice {round}");
                let (note, _, _) = alice
                    .insert("notes", row_input!("body" => body.as_str()))
                    .expect("alice inserts");
                let rows = read_note(&alice, *note.uuid()).await;
                assert_eq!(rows.len(), 1, "round {round}: alice reads her note");
            }

            stop.set(true);
            for task in floods {
                task.await.expect("flood task");
            }
            alice.shutdown().await.unwrap();
            for client in [bob, carol] {
                Rc::try_unwrap(client)
                    .ok()
                    .expect("flood released its client")
                    .shutdown()
                    .await
                    .unwrap();
            }
            server.shutdown().await;
        })
        .await;
}

/// The app's backend only reads: its point reads of alice's note are taken
/// out of turn while bob and carol keep their sessions full of commits, and
/// each is answered with the note, whole.
///
/// ```text
/// bob, carol ──commit, commit, …──► server
/// backend ──read(note)──► server ──► [note]   (×ROUNDS)
/// ```
#[tokio::test]
async fn an_idle_readers_point_reads_are_answered_while_others_write() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let connect = |user: &'static str| {
                TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema.clone())
                    .with_user_id(user)
                    .as_user()
                    .connect()
            };
            let alice = connect("alice").await;
            let (note, _, tx) = alice
                .insert("notes", row_input!("body" => "hello"))
                .expect("alice inserts");
            jazz_testkit::wait_for_global_txs(&alice, &[tx.expect("a committed insert")]).await;
            let backend = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("backend")
                .connect()
                .await;
            let bob = Rc::new(connect("bob").await);
            let carol = Rc::new(connect("carol").await);
            let stop = Rc::new(Cell::new(false));
            let floods = [
                flood(Rc::clone(&bob), Rc::clone(&stop)),
                flood(Rc::clone(&carol), Rc::clone(&stop)),
            ];

            for round in 0..ROUNDS {
                let rows = read_note(&backend, *note.uuid()).await;
                assert_eq!(rows.len(), 1, "round {round}: the backend reads the note");
                assert_eq!(
                    rows[0].get("body"),
                    Some(&jazz::tools::Value::Text("hello".to_owned())),
                    "round {round}"
                );
            }

            stop.set(true);
            for task in floods {
                task.await.expect("flood task");
            }
            alice.shutdown().await.unwrap();
            backend.shutdown().await.unwrap();
            for client in [bob, carol] {
                Rc::try_unwrap(client)
                    .ok()
                    .expect("flood released its client")
                    .shutdown()
                    .await
                    .unwrap();
            }
            server.shutdown().await;
        })
        .await;
}

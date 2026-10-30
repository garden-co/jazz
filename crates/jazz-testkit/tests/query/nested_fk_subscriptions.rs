use std::time::Duration;

use jazz::query::{ArraySubquery, Query};
use jazz::row_input;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, Row, SchemaBuilder, SubscriptionStream,
    SubscriptionStreamItem, TableSchema, Value,
};
use jazz_server::JazzServer;
use jazz_testkit::{TestingClient, wait_for_global_txs};

struct Fixture {
    server: JazzServer,
    alice: JazzClient,
    owner: ObjectId,
    list: ObjectId,
    item: ObjectId,
}

impl Fixture {
    async fn start() -> Self {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("owners").column("name", ColumnType::Text))
            .table(TableSchema::builder("lists").nullable_fk_column("owner_id", "owners"))
            .table(TableSchema::builder("items").fk_column("owner_id", "owners"))
            .allow_all()
            .build();
        let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
        let alice = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema)
            .with_user_id("alice")
            .ready_on("lists", Duration::from_secs(10))
            .connect()
            .await;
        let (owner, _, tx) = alice
            .insert("owners", row_input!("name" => "Alice"))
            .unwrap();
        wait_for_global_txs(&alice, &[tx.unwrap()]).await;
        let (list, _, tx) = alice
            .insert("lists", row_input!("owner_id" => owner))
            .unwrap();
        wait_for_global_txs(&alice, &[tx.unwrap()]).await;
        let (item, _, tx) = alice
            .insert("items", row_input!("owner_id" => owner))
            .unwrap();
        wait_for_global_txs(&alice, &[tx.unwrap()]).await;
        Self {
            server,
            alice,
            owner,
            list,
            item,
        }
    }

    async fn shutdown(self) {
        self.alice.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

fn nested_query() -> Query {
    Query::from("lists").array_subquery(
        ArraySubquery::new("owner", "owners", "id", "owner_id")
            .nested(ArraySubquery::new("items", "items", "owner_id", "id")),
    )
}

async fn next_list(stream: &mut SubscriptionStream) -> Row {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match stream.next().await.expect("subscription must remain open") {
                SubscriptionStreamItem::Rejected { reason } => {
                    panic!("subscription rejected: {reason:?}")
                }
                SubscriptionStreamItem::Delta(delta) => {
                    if let Some(row) = delta
                        .added
                        .into_iter()
                        .map(|change| change.row)
                        .chain(delta.updated.into_iter().filter_map(|change| change.row))
                        .next()
                    {
                        return row;
                    }
                }
            }
        }
    })
    .await
    .expect("subscription must deliver the list")
}

fn assert_owner_and_item(row: &Row, owner: ObjectId, name: &str, item: ObjectId) {
    let owners = row.get("owner").unwrap().as_array().unwrap();
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].row_id(), Some(owner));
    let values = owners[0].as_row().unwrap();
    assert_eq!(values[0], Value::Text(name.into()));
    let items = values[1].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].row_id(), Some(item));
}

/// Alice clears her list's FK after receiving its owner and that owner's item.
/// alice seeds -> server settles -> alice subscribes -> clears FK -> empty owner
#[tokio::test]
async fn clearing_owner_fk_removes_nested_owner() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let mut stream = f.alice.subscribe(nested_query()).await.unwrap();
            assert_owner_and_item(&next_list(&mut stream).await, f.owner, "Alice", f.item);

            f.alice
                .update("lists", f.list, vec![("owner_id".into(), Value::Null)])
                .unwrap();

            let row = next_list(&mut stream).await;
            assert_eq!(row.get("owner_id"), Some(&Value::Null));
            assert_eq!(row.get("owner"), Some(&Value::Array(vec![])));
            f.shutdown().await;
        })
        .await;
}

/// Alice switches her list from Alice's owner row to Bob's and receives Bob's item.
/// alice seeds both owners -> subscribes -> switches FK -> bob and his item
#[tokio::test]
async fn switching_owner_fk_replaces_nested_owner_and_items() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let (bob, _, tx) = f
                .alice
                .insert("owners", row_input!("name" => "Bob"))
                .unwrap();
            wait_for_global_txs(&f.alice, &[tx.unwrap()]).await;
            let (bob_item, _, tx) = f
                .alice
                .insert("items", row_input!("owner_id" => bob))
                .unwrap();
            wait_for_global_txs(&f.alice, &[tx.unwrap()]).await;
            let mut stream = f.alice.subscribe(nested_query()).await.unwrap();
            assert_owner_and_item(&next_list(&mut stream).await, f.owner, "Alice", f.item);

            f.alice
                .update("lists", f.list, vec![("owner_id".into(), Value::Uuid(bob))])
                .unwrap();

            assert_owner_and_item(&next_list(&mut stream).await, bob, "Bob", bob_item);
            f.shutdown().await;
        })
        .await;
}

/// Alice renames the included owner; the subscription retains its untouched item.
#[tokio::test]
async fn renaming_nested_owner_preserves_its_items() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let mut stream = f.alice.subscribe(nested_query()).await.unwrap();
            assert_owner_and_item(&next_list(&mut stream).await, f.owner, "Alice", f.item);

            f.alice
                .update(
                    "owners",
                    f.owner,
                    vec![("name".into(), Value::Text("Alice renamed".into()))],
                )
                .unwrap();

            assert_owner_and_item(
                &next_list(&mut stream).await,
                f.owner,
                "Alice renamed",
                f.item,
            );
            f.shutdown().await;
        })
        .await;
}

/// Control: Alice clears the same FK when subscribing to only one level of children.
#[tokio::test]
async fn clearing_owner_fk_preserves_shallow_subscription() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let query = Query::from("lists")
                .array_subquery(ArraySubquery::new("owner", "owners", "id", "owner_id"));
            let mut stream = f.alice.subscribe(query).await.unwrap();
            let initial = next_list(&mut stream).await;
            let owners = initial.get("owner").unwrap().as_array().unwrap();
            assert_eq!(owners.len(), 1);
            assert_eq!(owners[0].row_id(), Some(f.owner));

            f.alice
                .update("lists", f.list, vec![("owner_id".into(), Value::Null)])
                .unwrap();

            let row = next_list(&mut stream).await;
            assert_eq!(row.get("owner_id"), Some(&Value::Null));
            assert_eq!(row.get("owner"), Some(&Value::Array(vec![])));
            f.shutdown().await;
        })
        .await;
}

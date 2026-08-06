#![cfg(feature = "test")]

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use jazz::row_input;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ColumnType, DurabilityTier, ObjectId, OutputOccurrenceId, QueryBuilder, Schema, SchemaBuilder,
    SubscriptionStreamItem, TableSchema,
};
use support::TestingClient;
use uuid::Uuid;

fn todos_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build()
}

async fn next_delta(stream: &mut jazz::tools::SubscriptionStream) -> jazz::tools::OrderedRowDelta {
    let item = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("subscription delta arrives")
        .expect("subscription remains open");
    let SubscriptionStreamItem::Delta(delta) = item else {
        panic!("subscription was rejected")
    };
    delta
}

async fn next_delta_with_added(
    stream: &mut jazz::tools::SubscriptionStream,
) -> jazz::tools::OrderedRowDelta {
    for _ in 0..8 {
        let delta = next_delta(stream).await;
        if !delta.added.is_empty() {
            return delta;
        }
    }
    panic!("subscription did not emit an added output occurrence")
}

#[tokio::test(flavor = "current_thread")]
async fn plain_table_output_occurrence_is_root_stable_across_reset_and_join_order_is_positional() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todos_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000023")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;
            let query = QueryBuilder::new("todos").build();
            let mut initial_stream = client.subscribe(query.clone()).await.expect("subscribe");
            let empty_reset = next_delta(&mut initial_stream).await;
            assert!(
                empty_reset.added.is_empty(),
                "new subscription begins with a reset"
            );

            let (root, _, batch) = client
                .insert("todos", row_input!("title" => "draft", "done" => false))
                .expect("insert todo");
            client
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("todo settles locally");

            let one_shot = client
                .query(query.clone(), None)
                .await
                .expect("one-shot query");
            let one_shot_root: ObjectId = one_shot[0].0;
            assert_eq!(
                one_shot_root, root,
                "one-shot results retain their root id field"
            );
            let initial = next_delta_with_added(&mut initial_stream).await;
            let initial_id = initial.added[0].id.clone();

            assert_eq!(
                initial_id, root,
                "plain-table output remains root-compatible"
            );
            assert_eq!(initial_id.root(), root);
            assert!(initial_id.joined().is_empty());
            assert_eq!(
                initial_id.canonical_bytes().as_slice(),
                root.uuid().as_bytes()
            );

            drop(initial_stream);
            let mut reset_stream = client.subscribe(query).await.expect("resubscribe");
            let reset = next_delta_with_added(&mut reset_stream).await;
            assert_eq!(
                reset.added[0].id, initial_id,
                "reset preserves occurrence identity"
            );

            let first_join = ObjectId::from_uuid(Uuid::from_bytes([0x11; 16]));
            let second_join = ObjectId::from_uuid(Uuid::from_bytes([0x22; 16]));
            let ordered = OutputOccurrenceId::new(root, [first_join, second_join]);
            let reversed = OutputOccurrenceId::new(root, [second_join, first_join]);
            assert_ne!(
                ordered, reversed,
                "declared join order is part of the identity"
            );
            assert_eq!(
                ordered.contributing_rows().collect::<Vec<_>>(),
                vec![root, first_join, second_join]
            );
            assert_ne!(ordered.canonical_bytes(), reversed.canonical_bytes());
            let by_occurrence =
                BTreeMap::from([(ordered.clone(), "ordered"), (reversed, "reversed")]);
            assert_eq!(by_occurrence.get(&ordered), Some(&"ordered"));

            let joined_query = QueryBuilder::new("todos")
                .join("todos")
                .on("todos.id", "todos.id")
                .build();
            let error = match client.subscribe(joined_query).await {
                Ok(_) => panic!("joined maintained output cannot default to root identity"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("OutputOccurrenceId source tuples"),
                "joined subscription must fail loudly: {error}"
            );

            client.shutdown().await.expect("shutdown test client");
            server.shutdown().await;
        })
        .await;
}

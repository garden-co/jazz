mod common;

use jazz::account_registry::AccountId;
use jazz::query::Query;
use jazz::tools::sync::ReadTier;
use jazz::tools::{ColumnType, JazzClient, SchemaBuilder, Session, TableSchemaBuilder};

fn session(subject: &str, account: u128) -> Session {
    let mut session = Session::new("https://issuer.example", subject);
    session.account_id = Some(AccountId(uuid::Uuid::from_u128(account)));
    session
}

/// A trusted backend scopes writes and reads to registry-admitted sessions.
/// Linking shares ownership without erasing which identity authored the row.
#[tokio::test]
async fn linked_identities_share_account_ownership_but_not_exact_authorship() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("owned")
                .column("title", ColumnType::Text)
                .policies(common::read_and_allow_all_writes(common::session_eq(
                    "$createdBy.account",
                    &["user", "account"],
                ))),
        )
        .table(
            TableSchemaBuilder::new("exact")
                .column("title", ColumnType::Text)
                .policies(common::read_and_allow_all_writes(common::session_eq(
                    "$createdBy",
                    &["user"],
                ))),
        )
        .build();
    let client = JazzClient::test_client(schema).await;
    let alice = client.for_session(session("alice", 1));
    let linked = client.for_session(session("linked", 1));
    let unrelated = client.for_session(session("outsider", 2));
    for table in ["owned", "exact"] {
        alice
            .upsert(
                table,
                uuid::Uuid::from_u128(10),
                jazz::row_input!("title" => "private"),
            )
            .expect("author writes row");
    }
    let root_filtered = client
        .query_results_with_read_tier(
            Query::from("owned").filter(jazz::query::eq(
                jazz::query::col("$createdBy.account"),
                jazz::query::lit(jazz::groove::records::Value::Nullable(Some(Box::new(
                    jazz::groove::records::Value::Uuid(uuid::Uuid::from_u128(1)),
                )))),
            )),
            ReadTier::LocalFirst,
        )
        .await;
    assert_eq!(
        root_filtered
            .expect("filter structured account field")
            .len(),
        1
    );
    for (reader, owned_count, exact_count) in [(&alice, 1, 1), (&linked, 1, 0), (&unrelated, 0, 0)]
    {
        for (table, expected) in [("exact", exact_count), ("owned", owned_count)] {
            let rows = reader
                .query_results_with_read_tier(Query::from(table), ReadTier::LocalFirst)
                .await
                .expect("read admitted account scope");
            assert_eq!(rows.len(), expected, "{table} visibility for reader");
        }
    }
    client.shutdown().await.expect("close account fixture");
}

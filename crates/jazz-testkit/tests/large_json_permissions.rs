//! Owner-only policies must treat inline and indirect JSON identically.
use std::time::Duration;

use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, SchemaBuilder, TableSchema, Value, permissions, policy_expr as pe,
};
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs};

const ALICE: &str = "9750dcc2-516e-5ea0-8a26-54fa6ff6986b";
const BOB: &str = "756886b3-2033-583f-bd5a-a22f02fb5a6b";

fn payload(size: usize, character: char) -> String {
    let value = format!(
        "{{\"text\":\"{}\"}}",
        character.to_string().repeat(size - 11)
    );
    assert_eq!(value.len(), size);
    value
}

#[tokio::test(flavor = "current_thread")]
async fn owner_policy_accepts_json_across_inline_boundary() {
    run_matrix(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn owner_write_policy_with_public_reads_accepts_json_across_inline_boundary() {
    run_matrix(false).await;
}

async fn run_matrix(private_reads: bool) {
    tokio::task::LocalSet::new()
        .run_until(async {
            let owner = pe::eq("owner", pe::session(vec!["claims", "sub"]));
            let policy = permissions(|p| {
                if private_reads {
                    p.allow_read().where_(owner.clone());
                } else {
                    p.allow_read().always();
                }
                p.allow_insert().where_(owner.clone());
                p.allow_update().where_old(owner.clone()).where_new(owner);
            });
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("documents")
                        .column("owner", ColumnType::Text)
                        .column("payload", ColumnType::Json { schema: None })
                        .policies(policy),
                )
                .build();
            let server = jazz_server::JazzServer::start_with_schema(schema.clone())
                .await
                .unwrap();
            let ready = Duration::from_secs(30);
            let alice = connect_ready_user(&server, &schema, ALICE, "documents", ready).await;
            let bob = connect_ready_user(&server, &schema, BOB, "documents", ready).await;
            let backend =
                connect_ready_client(&server, &schema, "large-json-backend", "documents", ready)
                    .await;
            for size in [65_536, 65_537, 800_000] {
                for (client, label) in [(&alice, "owner"), (&backend, "backend")] {
                    eprintln!("{label} {size}: insert");
                    let (id, _, tx) = client
                        .insert(
                            "documents",
                            row_input!("owner" => ALICE, "payload" => payload(size, 'x')),
                        )
                        .unwrap();
                    wait_for_edge_txs(client, &[tx.unwrap()]).await;
                    eprintln!("{label} {size}: update");
                    let updated = payload(size, 'y');
                    let tx = client
                        .update(
                            "documents",
                            id,
                            vec![("payload".into(), Value::Text(updated.clone()))],
                        )
                        .unwrap()
                        .unwrap();
                    wait_for_edge_txs(client, &[tx]).await;
                    eprintln!("{label} {size}: deny takeover");
                    // Unknown/undisclosed rows may reject locally; if the
                    // client stages the write, authority settlement must deny it.
                    match bob.update(
                        "documents",
                        id,
                        vec![
                            ("owner".into(), Value::Text(BOB.into())),
                            ("payload".into(), Value::Text(payload(size, 'z'))),
                        ],
                    ) {
                        Err(error) => assert!(
                            error.to_string().contains("read policy denied UPDATE"),
                            "{error}"
                        ),
                        Ok(Some(tx)) => {
                            let error = tokio::time::timeout(
                                Duration::from_secs(15),
                                bob.wait_for_transaction(tx, DurabilityTier::GlobalServer),
                            )
                            .await
                            .unwrap()
                            .expect_err("another account cannot take over the row");
                            assert!(
                                error.to_string().ends_with("authorization_denied"),
                                "{error}"
                            );
                        }
                        Ok(None) => panic!("standalone update must commit or reject"),
                    }
                    if label == "owner" {
                        let tx = alice
                            .update(
                                "documents",
                                id,
                                vec![
                                    ("owner".into(), Value::Text(BOB.into())),
                                    ("payload".into(), Value::Text(payload(size, 'z'))),
                                ],
                            )
                            .unwrap()
                            .unwrap();
                        let error = tokio::time::timeout(
                            Duration::from_secs(15),
                            alice.wait_for_transaction(tx, DurabilityTier::GlobalServer),
                        )
                        .await
                        .unwrap()
                        .expect_err("new owner must still match the writer");
                        assert!(
                            error.to_string().ends_with("authorization_denied"),
                            "{error}"
                        );
                    }
                }
                let (_, _, tx) = bob
                    .insert(
                        "documents",
                        row_input!("owner" => ALICE, "payload" => payload(size, 'z')),
                    )
                    .unwrap();
                let error = tokio::time::timeout(
                    Duration::from_secs(15),
                    bob.wait_for_transaction(tx.unwrap(), DurabilityTier::GlobalServer),
                )
                .await
                .unwrap()
                .expect_err("another account cannot insert an Alice-owned row");
                assert!(
                    error.to_string().ends_with("authorization_denied"),
                    "{error}"
                );
            }
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
            backend.shutdown().await.unwrap();
            server.shutdown().await;
        })
        .await;
}

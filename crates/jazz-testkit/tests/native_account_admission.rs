use jazz::account_registry::AccountId;
use jazz::query::Query;
use jazz::tools::{ColumnType, ObjectId, SchemaBuilder, TableSchema, Value};
use jazz_server::{JazzServer, TEST_JWT_ISSUER};
use jazz_testkit::{connect, enroll_test_context, wait_for_edge_txs};

#[tokio::test]
async fn public_native_client_requires_enrollment_and_cannot_choose_another_account() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("notes").column("title", ColumnType::Text))
                .build();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let mut context = server.make_client_context_for_user(schema, "native-account-fixture");
            context.backend_secret = None;
            context.admin_secret = None;
            assert!(
                connect(context.clone()).await.is_err(),
                "bare JWT must not register an account"
            );
            enroll_test_context(&mut context)
                .await
                .expect("explicit fixture enrollment");
            let account = context.account_id.expect("registry assignment");
            let client = connect(context.clone())
                .await
                .expect("connect enrolled account");
            let (_, _, tx) = client
                .insert("notes", jazz::row_input!("title" => "registered author"))
                .unwrap();
            wait_for_edge_txs(&client, &[tx.unwrap()]).await;
            let rows = client
                .query(Query::from("notes").select(["title", "$createdBy"]), None)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].1,
                vec![
                    Value::Text("registered author".into()),
                    Value::Row {
                        id: None,
                        values: vec![
                            Value::Uuid(ObjectId::from_uuid(account.0)),
                            Value::Row {
                                id: None,
                                values: vec![
                                    Value::Text(TEST_JWT_ISSUER.into()),
                                    Value::Text("native-account-fixture".into()),
                                ]
                            },
                        ]
                    },
                ]
            );
            client.shutdown().await.unwrap();
            let mut forged = context.clone();
            forged.account_id = Some(AccountId(uuid::Uuid::new_v4()));
            assert!(
                connect(forged).await.is_err(),
                "registry must reject a copied account identifier"
            );
            let reconnected = connect(context)
                .await
                .expect("same enrolled context reconnects");
            reconnected.shutdown().await.unwrap();
            server.shutdown().await;
        })
        .await;
}

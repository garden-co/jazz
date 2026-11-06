use jazz::account_registry::AccountId;
use jazz::query::Query;
use jazz::tools::identity;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, ObjectId, SchemaBuilder, TableSchema, Value};
use jazz_server::JazzServer;
use jazz_testkit::{connect, wait_for_global_txs};

#[tokio::test]
async fn a_relayed_guest_authors_as_its_founding_account_and_cannot_choose_another() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("notes").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            // The guest's own proof, relayed by the app's backend: the core
            // founds the account its key derives and binds the proof to it.
            let seed = [0x4e; 32];
            let subject = identity::derive_user_id(&seed).to_string();
            let mut context = server.make_client_context_for_user(schema, &subject);
            context.jwt_token = Some(
                identity::mint_jazz_self_signed_token(
                    &seed,
                    identity::LOCAL_FIRST_ISSUER,
                    &server.app_id().to_string(),
                    3600,
                )
                .expect("mint guest proof"),
            );
            let account =
                jazz::account_registry::local_first_account_id(*server.app_id().uuid(), &subject);
            context.account_id = Some(account);
            let client = connect(context.clone())
                .await
                .expect("connect enrolled account");
            let (_, _, tx) = client
                .insert("notes", jazz::row_input!("title" => "registered author"))
                .unwrap();
            wait_for_global_txs(&client, &[tx.unwrap()]).await;
            let rows = client
                .query(
                    Query::from("notes").select(["title", "$createdBy"]),
                    jazz::tools::ReadTier::LocalFirst,
                )
                .await
                .map(jazz::tools::test_support::ordinary_rows)
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
                                    Value::Text(identity::LOCAL_FIRST_ISSUER.into()),
                                    Value::Text(subject.clone()),
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

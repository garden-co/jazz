mod support;

use jazz_db::db::{Db, DbConfig, DbIdentity, Error, ErrorCode};
use jazz_model::{
    model::public_schema::{
        ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    },
    query::Query,
    schema::JazzSchema,
};
use jazz_types::{
    ids::{AuthorSubject, NodeUuid},
    records::Value,
};
use std::{collections::BTreeMap, sync::Arc};
use support::DatabaseFixture;
use support::open_jazz;

fn config() -> DbConfig {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap();
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}
fn cells(body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(body.into()))])
}
async fn bodies(db: &Db) -> Result<Vec<String>, Error> {
    let query = db.prepare_query(&Query::from("notes"))?;
    Ok(db
        .read(&query)
        .await?
        .into_iter()
        .map(|row| match row.cell("body") {
            Some(Value::String(body)) => body,
            other => panic!("expected body, got {other:?}"),
        })
        .collect())
}

// Close the logical database and always drain its provider, even if the final tick fails.
async fn close(db: Db) -> Result<(), Error> {
    let result = db.close().await;
    let drained = db.close_worker().await;
    result.and(drained)
}

/// Alice writes a large value and rolls back a second write. Bob reopens the
/// same actor database and sees only Alice's committed value.
#[tokio::test(flavor = "multi_thread")]
async fn full_database_reopens_committed_values() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let pages = Arc::new(DatabaseFixture::default());
        let actor = uuid::Uuid::new_v4().to_string();
        let db = open_jazz(pages.clone(), actor.clone(), 1, config()).await?;
        let body = "large é value ".repeat(10_000);
        db.insert("notes", cells(&body), Default::default()).await?;
        let error = db
            .transaction(async |tx| {
                tx.insert("notes", cells("rolled back"), Default::default())
                    .await?;
                Err::<(), _>(Error::new(ErrorCode::Storage, "deliberate rollback"))
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("deliberate rollback"));
        assert_eq!(bodies(&db).await?, vec![body.clone()]);
        close(db).await?;
        let reopened = open_jazz(pages, actor, 1, config()).await?;
        assert_eq!(bodies(&reopened).await?, vec![body]);
        close(reopened).await?;
        anyhow::Ok(())
    })
    .await?
}

#[path = "support/duplex.rs"]
mod duplex;

/// Alice's replica uploads a large value to Bob's authority, both backed by
/// independent Rivet workers. Bob reopens and reads the settled value.
#[tokio::test(flavor = "multi_thread")]
async fn replicas_sync_through_worker_storage() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let pages = Arc::new(DatabaseFixture::default());
        let actor = uuid::Uuid::new_v4().to_string();
        let mut server_config = config();
        server_config.identity.node = NodeUuid::from_bytes([2; 16]);
        let server = open_jazz(pages.clone(), actor.clone(), 1, server_config).await?;
        server.set_authority().await;
        let client = open_jazz(
            Arc::new(DatabaseFixture::default()),
            uuid::Uuid::new_v4().to_string(),
            1,
            config(),
        )
        .await?;
        client.become_replica().await;
        let body = "sync large é value ".repeat(10_000);
        client
            .insert("notes", cells(&body), Default::default())
            .await?;
        let (upstream, subscriber) = duplex::duplex();
        client.connect_upstream(upstream).await;
        server
            .accept_subscriber(subscriber, AuthorSubject::SYSTEM)
            .await?;
        for _ in 0..20 {
            client.tick().await?;
            server.tick().await?;
        }
        assert_eq!(
            bodies(&server).await?,
            vec![body.clone()],
            "authority must receive the large value"
        );
        close(client).await?;
        close(server).await?;
        let mut server_config = config();
        server_config.identity.node = NodeUuid::from_bytes([2; 16]);
        let reopened = open_jazz(pages, actor, 1, server_config).await?;
        assert_eq!(bodies(&reopened).await?, vec![body]);
        close(reopened).await?;
        anyhow::Ok(())
    })
    .await?
}

/// Alice receives an error for refused and indeterminate commits. After a new
/// worker opens the authoritative pages, Bob sees exactly the persisted result.
#[tokio::test(flavor = "multi_thread")]
async fn database_commit_failures_preserve_persisted_outcome() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for failure in [1, 2] {
            let pages = Arc::new(DatabaseFixture::default());
            let actor = uuid::Uuid::new_v4().to_string();
            let db = open_jazz(pages.clone(), actor.clone(), 1, config()).await?;
            pages
                .fail_next
                .store(failure, std::sync::atomic::Ordering::SeqCst);
            assert!(
                db.insert("notes", cells("alice"), Default::default())
                    .await
                    .is_err()
            );
            // A failed final tick must still drain the provider before reopening.
            let _ = close(db).await;
            let reopened = open_jazz(pages, actor, 1, config()).await?;
            assert_eq!(
                bodies(&reopened).await?,
                if failure == 1 {
                    vec![]
                } else {
                    vec!["alice".to_owned()]
                }
            );
            close(reopened).await?;
        }
        anyhow::Ok(())
    })
    .await?
}

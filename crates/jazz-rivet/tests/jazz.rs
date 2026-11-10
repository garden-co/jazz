mod support;

use futures::task::{LocalFutureObj, LocalSpawn, SpawnError};
use jazz_db::db::{Db, DbConfig, DbIdentity, DbOwner, Error, ErrorCode};
use jazz_model::{
    model::public_schema::{
        ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    },
    query::Query,
    schema::JazzSchema,
};
use jazz_rivet::open_jazz;
use jazz_types::{
    ids::{AuthorSubject, NodeUuid},
    records::Value,
};
use std::{collections::BTreeMap, sync::Arc};
use support::Pages;

struct Local;
impl LocalSpawn for Local {
    fn spawn_local_obj(&self, future: LocalFutureObj<'static, ()>) -> Result<(), SpawnError> {
        tokio::task::spawn_local(future);
        Ok(())
    }
}
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

/// Alice writes a large value and rolls back a second write. Bob reopens the
/// same persisted pages through a new real Rivet worker and sees only Alice's
/// committed value. The page transport is deterministic, not a live cluster.
#[tokio::test(flavor = "multi_thread")]
async fn full_database_reopens_committed_values() -> anyhow::Result<()> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::task::LocalSet::new().run_until(async {
            let pages = Arc::new(Pages::default());
            let actor = uuid::Uuid::new_v4().to_string();
            let owner = DbOwner::spawn(
                open_jazz(pages.clone(), actor.clone(), 1, config()).await?,
                4,
                &Local,
            )?;
            let body = "large é value ".repeat(10_000);
            let written = body.clone();
            owner
                .call(async move |db| {
                    db.insert("notes", cells(&written), Default::default())
                        .await?;
                    Ok(())
                })
                .await?;
            let error = owner
                .call(async |db| {
                    db.transaction(async |tx| {
                        tx.insert("notes", cells("rolled back"), Default::default())
                            .await?;
                        Err::<(), _>(Error::new(ErrorCode::Storage, "deliberate rollback"))
                    })
                    .await
                    .map(|_| ())
                })
                .await
                .unwrap_err();
            assert!(error.to_string().contains("deliberate rollback"));
            assert_eq!(
                owner.call(async |db| bodies(db).await).await?,
                vec![body.clone()]
            );
            owner.close().await?;
            let reopened = DbOwner::spawn(open_jazz(pages, actor, 1, config()).await?, 4, &Local)?;
            assert_eq!(
                reopened.call(async |db| bodies(db).await).await?,
                vec![body]
            );
            reopened.close().await?;
            anyhow::Ok(())
        }),
    )
    .await?
}

/// Alice disconnects while the real SQLite worker waits for commit. The owner
/// finishes her write before Bob reads it, and shutdown drains independently.
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_caller_does_not_cancel_database_write() -> anyhow::Result<()> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::task::LocalSet::new().run_until(async {
            let pages = Arc::new(Pages::default());
            let owner = DbOwner::spawn(
                open_jazz(pages.clone(), uuid::Uuid::new_v4().to_string(), 1, config()).await?,
                4,
                &Local,
            )?;
            let pause = pages.pause_next_commit();
            let alice = owner.clone();
            let request = tokio::task::spawn_local(async move {
                alice
                    .call(async |db| {
                        db.insert("notes", cells("alice"), Default::default())
                            .await?;
                        Ok(())
                    })
                    .await
            });
            pages.commit_entered.notified().await;
            request.abort();
            assert!(request.await.unwrap_err().is_cancelled());
            drop(pause);
            assert_eq!(
                owner.call(async |db| bodies(db).await).await?,
                vec!["alice"]
            );
            drop(owner.close());
            owner.close().await?;
            anyhow::Ok(())
        }),
    )
    .await?
}

#[path = "support/duplex.rs"]
mod duplex;

/// Alice's replica uploads a large value to Bob's authority, both backed by
/// independent Rivet workers. Bob reopens and reads the settled value.
#[tokio::test(flavor = "multi_thread")]
async fn replicas_sync_through_worker_storage() -> anyhow::Result<()> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::task::LocalSet::new().run_until(async {
            let pages = Arc::new(Pages::default());
            let actor = uuid::Uuid::new_v4().to_string();
            let mut server_config = config();
            server_config.identity.node = NodeUuid::from_bytes([2; 16]);
            let server = DbOwner::spawn(
                open_jazz(pages.clone(), actor.clone(), 1, server_config).await?,
                4,
                &Local,
            )?;
            server
                .call(async |db| {
                    db.set_authority().await;
                    Ok(())
                })
                .await?;
            let client = DbOwner::spawn(
                open_jazz(
                    Arc::new(Pages::default()),
                    uuid::Uuid::new_v4().to_string(),
                    1,
                    config(),
                )
                .await?,
                4,
                &Local,
            )?;
            client
                .call(async |db| {
                    db.become_replica().await;
                    Ok(())
                })
                .await?;
            let body = "sync large é value ".repeat(10_000);
            let written = body.clone();
            client
                .call(async move |db| {
                    db.insert("notes", cells(&written), Default::default())
                        .await?;
                    Ok(())
                })
                .await?;
            let (upstream, subscriber) = duplex::duplex();
            client
                .call(async move |db| {
                    db.connect_upstream(upstream).await;
                    Ok(())
                })
                .await?;
            server
                .call(async move |db| {
                    db.accept_subscriber(subscriber, AuthorSubject::SYSTEM)
                        .await?;
                    Ok(())
                })
                .await?;
            for _ in 0..20 {
                client.call(async |db| db.tick().await).await?;
                server.call(async |db| db.tick().await).await?;
            }
            assert!(
                server.call(async |db| bodies(db).await).await? == vec![body.clone()],
                "authority must receive the large value"
            );
            client.close().await?;
            server.close().await?;
            let mut server_config = config();
            server_config.identity.node = NodeUuid::from_bytes([2; 16]);
            let reopened =
                DbOwner::spawn(open_jazz(pages, actor, 1, server_config).await?, 4, &Local)?;
            assert_eq!(
                reopened.call(async |db| bodies(db).await).await?,
                vec![body]
            );
            reopened.close().await?;
            anyhow::Ok(())
        }),
    )
    .await?
}

/// Alice receives an error for refused and indeterminate commits. After a new
/// worker opens the authoritative pages, Bob sees exactly the persisted result.
#[tokio::test(flavor = "multi_thread")]
async fn database_commit_failures_preserve_persisted_outcome() -> anyhow::Result<()> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::task::LocalSet::new().run_until(async {
            for failure in [1, 2] {
                let pages = Arc::new(Pages::default());
                let actor = uuid::Uuid::new_v4().to_string();
                let owner = DbOwner::spawn(
                    open_jazz(pages.clone(), actor.clone(), 1, config()).await?,
                    4,
                    &Local,
                )?;
                pages
                    .fail_next
                    .store(failure, std::sync::atomic::Ordering::SeqCst);
                assert!(
                    owner
                        .call(async |db| {
                            db.insert("notes", cells("alice"), Default::default())
                                .await?;
                            Ok(())
                        })
                        .await
                        .is_err()
                );
                // An indeterminate commit can also make final tick fail. Close must
                // still drain the worker before the replacement opens its pages.
                let _ = owner.close().await;
                let reopened =
                    DbOwner::spawn(open_jazz(pages, actor, 1, config()).await?, 4, &Local)?;
                assert_eq!(
                    reopened.call(async |db| bodies(db).await).await?,
                    if failure == 1 {
                        vec![]
                    } else {
                        vec!["alice".to_owned()]
                    }
                );
                reopened.close().await?;
            }
            anyhow::Ok(())
        }),
    )
    .await?
}

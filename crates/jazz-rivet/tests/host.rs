mod support;
use jazz_db::backend::{WorkerCell, WorkerSql};
use jazz_sql::Param;
use std::sync::Arc;
use support::DatabaseFixture;

/// Alice's SQL bindings preserve exact SQLite types. A provider-level test is
/// necessary to exercise i64 extremes, embedded NULs and invalid TEXT bytes.
#[tokio::test(flavor = "multi_thread")]
async fn scalar_parameters_preserve_sqlite_types() -> anyhow::Result<()> {
    let pages = Arc::new(DatabaseFixture::default());
    let db = pages.database("scalars").await?;
    let connection = jazz_rivet::test_connection(db).await?;
    let result = connection
        .query(
            "SELECT ?1, ?2, ?3, ?4, ?5, ?6",
            &[
                Param::Null,
                Param::Integer(i64::MAX),
                Param::Integer(i64::MIN),
                Param::Real(1.25),
                Param::Text("a\0é"),
                Param::Blob(&[0, 255]),
            ],
        )
        .await?;
    assert!(matches!(result.rows[0][0], WorkerCell::Null));
    assert!(matches!(result.rows[0][1], WorkerCell::Integer(i64::MAX)));
    assert!(matches!(result.rows[0][2], WorkerCell::Integer(i64::MIN)));
    assert!(matches!(result.rows[0][3], WorkerCell::Real(1.25)));
    assert!(matches!(&result.rows[0][4], WorkerCell::Text(v) if v == "a\0é".as_bytes()));
    assert!(matches!(&result.rows[0][5], WorkerCell::Blob(v) if v == &[0, 255]));
    assert!(
        connection
            .query("SELECT ?", &[Param::TextBytes(&[255])])
            .await
            .is_err()
    );
    connection.close().await?;
    Ok(())
}

/// Alice rolls back an inner savepoint while retaining her outer write. Bob's
/// ordinary Rivet SQL must wait for Alice's complete Jazz transaction.
/// This provider-level test checks coordination with non-Jazz actor SQL.
#[tokio::test(flavor = "multi_thread")]
async fn nested_savepoints_coordinate_with_rivet_sql() -> anyhow::Result<()> {
    let pages = Arc::new(DatabaseFixture::default());
    let db = pages.database("nested").await?;
    let connection = jazz_rivet::test_connection(db.clone()).await?;
    connection
        .execute_script("CREATE TABLE probe (v INTEGER)")
        .await?;
    connection.begin_savepoint("outer_write").await?;
    connection
        .execute("INSERT INTO probe VALUES (1)", &[])
        .await?;
    connection.begin_savepoint("inner_write").await?;
    connection
        .execute("INSERT INTO probe VALUES (2)", &[])
        .await?;
    connection.end_savepoint("inner_write", false).await?;
    let mut bob = Box::pin(db.query("SELECT v FROM probe", None));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut bob)
            .await
            .is_err()
    );
    connection.end_savepoint("outer_write", true).await?;
    let rows = tokio::time::timeout(std::time::Duration::from_secs(10), bob).await??;
    assert_eq!(rows.rows.len(), 1);
    assert!(matches!(rows.rows[0][0], rivetkit::ColumnValue::Integer(1)));
    connection.close().await?;
    // Jazz releasing its lease must not close Rivet's actor database.
    assert_eq!(db.query("SELECT v FROM probe", None).await?.rows.len(), 1);
    Ok(())
}

/// Alice triggers SQLite automatic rollback. Jazz must report an error, refuse
/// further use of that adapter, and allow a replacement to see only durable data.
/// A provider-level trigger is needed to force SQLite's RAISE(ROLLBACK) path.
#[tokio::test(flavor = "multi_thread")]
async fn automatic_rollback_fails_closed() -> anyhow::Result<()> {
    let pages = Arc::new(DatabaseFixture::default());
    let db = pages.database("rollback").await?;
    let connection = jazz_rivet::test_connection(db.clone()).await?;
    connection.execute_script("CREATE TABLE probe (v INTEGER); CREATE TRIGGER refuse BEFORE INSERT ON probe WHEN NEW.v = 2 BEGIN SELECT RAISE(ROLLBACK, 'refused'); END;").await?;
    connection.begin_savepoint("write").await?;
    connection
        .execute("INSERT INTO probe VALUES (1)", &[])
        .await?;
    assert!(
        connection
            .execute("INSERT INTO probe VALUES (2)", &[])
            .await
            .is_err()
    );
    assert!(connection.query("SELECT * FROM probe", &[]).await.is_err());
    let _ = connection.close().await;
    assert!(db.query("SELECT * FROM probe", None).await?.rows.is_empty());
    Ok(())
}

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rusqlite::{Connection, params};

const ROWS: i64 = 100_000;
const BRANCHES: i64 = 1_000;
const OVERRIDES_PER_BRANCH: i64 = 100;
const PAGE_SIZE: i64 = 50;

fn configure(conn: &Connection) {
    conn.pragma_update(None, "journal_mode", "MEMORY").unwrap();
    conn.pragma_update(None, "synchronous", "OFF").unwrap();
    conn.pragma_update(None, "temp_store", "MEMORY").unwrap();
    conn.pragma_update(None, "cache_size", -200_000).unwrap();
}

fn create_current_schema(conn: &Connection) {
    conn.execute_batch(
        r#"
        CREATE TABLE todos__schema_v1_current (
          "$rowId" INTEGER NOT NULL,
          "$branchId" TEXT NOT NULL,
          "$visibleTxId" INTEGER NOT NULL,
          "$isDeleted" INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          "$createdBy" TEXT NOT NULL,
          "$createdAt" INTEGER NOT NULL,
          "$updatedBy" TEXT NOT NULL,
          "$updatedAt" INTEGER NOT NULL,
          "$editMetadataJson" TEXT NOT NULL,
          PRIMARY KEY ("$rowId", "$branchId")
        );

        CREATE INDEX todos_current_done_created
          ON todos__schema_v1_current("$branchId", done, "$createdAt" DESC);
        "#,
    )
    .unwrap();
}

fn seed_current(conn: &mut Connection, rows: i64) {
    let tx = conn.transaction().unwrap();
    {
        let mut insert = tx
            .prepare(
                r#"
                INSERT INTO todos__schema_v1_current (
                  "$rowId", "$branchId", "$visibleTxId", "$isDeleted",
                  title, done, "$createdBy", "$createdAt", "$updatedBy", "$updatedAt",
                  "$editMetadataJson"
                ) VALUES (?1, 'main', ?1, 0, ?2, ?3, 'alice', ?4, 'alice', ?4, '{}')
                "#,
            )
            .unwrap();
        for row_id in 0..rows {
            let title = format!("todo {row_id}");
            let done = if row_id % 3 == 0 { 1 } else { 0 };
            insert
                .execute(params![row_id, title, done, row_id])
                .unwrap();
        }
    }
    tx.commit().unwrap();
}

fn current_db(rows: i64) -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    configure(&conn);
    create_current_schema(&conn);
    seed_current(&mut conn, rows);
    conn
}

fn query_current_page(conn: &Connection, since: i64, limit: i64) -> usize {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT "$rowId", title, done, "$createdAt"
            FROM todos__schema_v1_current
            WHERE "$branchId" = 'main'
              AND "$isDeleted" = 0
              AND done = 0
              AND "$createdAt" > ?1
            ORDER BY "$createdAt" DESC
            LIMIT ?2
            "#,
        )
        .unwrap();
    let mut rows = stmt.query(params![since, limit]).unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().unwrap() {
        let row_id: i64 = row.get(0).unwrap();
        let title: String = row.get(1).unwrap();
        let done: i64 = row.get(2).unwrap();
        let created_at: i64 = row.get(3).unwrap();
        black_box((row_id, title, done, created_at));
        count += 1;
    }
    count
}

fn query_current_scope_json(conn: &Connection, since: i64, limit: i64) -> usize {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
              "$rowId",
              title,
              done,
              "$createdAt",
              json_array(json_object(
                'kind', 'result',
                'table', 'todos',
                'schema', 'schema_v1',
                'branch', "$branchId",
                'rowId', "$rowId",
                'txId', "$visibleTxId"
              )) AS "$resultScopeJson"
            FROM todos__schema_v1_current
            WHERE "$branchId" = 'main'
              AND "$isDeleted" = 0
              AND done = 0
              AND "$createdAt" > ?1
            ORDER BY "$createdAt" DESC
            LIMIT ?2
            "#,
        )
        .unwrap();
    let mut rows = stmt.query(params![since, limit]).unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().unwrap() {
        let row_id: i64 = row.get(0).unwrap();
        let scope: String = row.get(4).unwrap();
        black_box((row_id, scope));
        count += 1;
    }
    count
}

fn create_snapshot_schema(conn: &Connection) {
    conn.execute_batch(
        r#"
        CREATE TABLE jazz_tx (
          "$txId" INTEGER PRIMARY KEY,
          "$globalEpoch" INTEGER,
          "$siteId" TEXT NOT NULL,
          "$siteTx" INTEGER NOT NULL,
          "$status" TEXT NOT NULL
        );

        CREATE TABLE jazz_branch (
          "$branchId" TEXT PRIMARY KEY,
          "$baseGlobalEpoch" INTEGER NOT NULL
        );

        CREATE TABLE jazz_branch_tx (
          "$branchId" TEXT NOT NULL,
          "$txId" INTEGER NOT NULL,
          PRIMARY KEY ("$branchId", "$txId")
        );

        CREATE TABLE todos__schema_v1_history (
          "$rowId" INTEGER NOT NULL,
          "$txId" INTEGER NOT NULL,
          "$op" TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          "$createdAt" INTEGER NOT NULL,
          "$isDeleted" INTEGER NOT NULL,
          PRIMARY KEY ("$rowId", "$txId")
        );

        CREATE INDEX todos_history_tx_row
          ON todos__schema_v1_history("$txId", "$rowId");
        CREATE INDEX todos_history_row_tx
          ON todos__schema_v1_history("$rowId", "$txId" DESC);
        CREATE INDEX todos_history_done_created_tx_row
          ON todos__schema_v1_history(done, "$createdAt" DESC, "$txId", "$rowId");
        CREATE INDEX tx_global_epoch
          ON jazz_tx("$globalEpoch", "$txId");

        CREATE TABLE todos__schema_v1_base_current (
          "$rowId" INTEGER PRIMARY KEY,
          "$visibleTxId" INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          "$createdAt" INTEGER NOT NULL,
          "$isDeleted" INTEGER NOT NULL
        );

        CREATE TABLE todos__schema_v1_branch_delta (
          "$branchId" TEXT NOT NULL,
          "$rowId" INTEGER NOT NULL,
          "$visibleTxId" INTEGER NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL,
          "$createdAt" INTEGER NOT NULL,
          "$isDeleted" INTEGER NOT NULL,
          PRIMARY KEY ("$branchId", "$rowId")
        );

        CREATE INDEX todos_base_done_created
          ON todos__schema_v1_base_current(done, "$createdAt" DESC);
        CREATE INDEX todos_branch_delta_done_created
          ON todos__schema_v1_branch_delta("$branchId", done, "$createdAt" DESC);
        "#,
    )
    .unwrap();
}

fn seed_snapshot(conn: &mut Connection, rows: i64, branches: i64, overrides_per_branch: i64) {
    let tx = conn.transaction().unwrap();
    {
        let mut insert_tx = tx
            .prepare(
                r#"
                INSERT INTO jazz_tx ("$txId", "$globalEpoch", "$siteId", "$siteTx", "$status")
                VALUES (?1, ?1, 'core', ?1, 'global_durable')
                "#,
            )
            .unwrap();
        let mut insert_history = tx
            .prepare(
                r#"
                INSERT INTO todos__schema_v1_history (
                  "$rowId", "$txId", "$op", title, done, "$createdAt", "$isDeleted"
                ) VALUES (?1, ?1, 'insert', ?2, ?3, ?1, 0)
                "#,
            )
            .unwrap();
        let mut insert_base_current = tx
            .prepare(
                r#"
                INSERT INTO todos__schema_v1_base_current (
                  "$rowId", "$visibleTxId", title, done, "$createdAt", "$isDeleted"
                ) VALUES (?1, ?1, ?2, ?3, ?1, 0)
                "#,
            )
            .unwrap();
        for row_id in 0..rows {
            let title = format!("base todo {row_id}");
            let done = if row_id % 3 == 0 { 1 } else { 0 };
            insert_tx.execute(params![row_id]).unwrap();
            insert_history
                .execute(params![row_id, title, done])
                .unwrap();
            insert_base_current
                .execute(params![row_id, format!("base todo {row_id}"), done])
                .unwrap();
        }
    }
    {
        let mut insert_branch = tx
            .prepare(
                r#"
                INSERT INTO jazz_branch ("$branchId", "$baseGlobalEpoch")
                VALUES (?1, ?2)
                "#,
            )
            .unwrap();
        let mut insert_tx = tx
            .prepare(
                r#"
                INSERT INTO jazz_tx ("$txId", "$globalEpoch", "$siteId", "$siteTx", "$status")
                VALUES (?1, NULL, ?2, ?3, 'local_pending')
                "#,
            )
            .unwrap();
        let mut insert_branch_tx = tx
            .prepare(
                r#"
                INSERT INTO jazz_branch_tx ("$branchId", "$txId")
                VALUES (?1, ?2)
                "#,
            )
            .unwrap();
        let mut insert_history = tx
            .prepare(
                r#"
                INSERT INTO todos__schema_v1_history (
                  "$rowId", "$txId", "$op", title, done, "$createdAt", "$isDeleted"
                ) VALUES (?1, ?2, 'update', ?3, ?4, ?5, 0)
                "#,
            )
            .unwrap();
        let mut insert_branch_delta = tx
            .prepare(
                r#"
                INSERT INTO todos__schema_v1_branch_delta (
                  "$branchId", "$rowId", "$visibleTxId", title, done, "$createdAt", "$isDeleted"
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)
                "#,
            )
            .unwrap();

        for branch_idx in 0..branches {
            let branch_id = format!("branch_{branch_idx}");
            insert_branch.execute(params![branch_id, rows - 1]).unwrap();
            for override_idx in 0..overrides_per_branch {
                let tx_id = rows + branch_idx * overrides_per_branch + override_idx;
                let row_id = (branch_idx * 97 + override_idx * 997) % rows;
                let title = format!("branch {branch_idx} override {override_idx}");
                let done = if override_idx % 5 == 0 { 1 } else { 0 };
                let created_at = rows + override_idx;
                insert_tx
                    .execute(params![tx_id, format!("site_{branch_idx}"), override_idx])
                    .unwrap();
                insert_branch_tx.execute(params![branch_id, tx_id]).unwrap();
                insert_history
                    .execute(params![row_id, tx_id, title, done, created_at])
                    .unwrap();
                insert_branch_delta
                    .execute(params![
                        branch_id,
                        row_id,
                        tx_id,
                        format!("branch {branch_idx} override {override_idx}"),
                        done,
                        created_at
                    ])
                    .unwrap();
            }
        }
    }
    tx.commit().unwrap();
}

fn snapshot_db(rows: i64, branches: i64, overrides_per_branch: i64) -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    configure(&conn);
    create_snapshot_schema(&conn);
    seed_snapshot(&mut conn, rows, branches, overrides_per_branch);
    conn
}

fn query_snapshot_window(conn: &Connection, branch_id: &str, limit: i64) -> usize {
    let mut stmt = conn
        .prepare(
            r#"
            WITH branch_visible_tx AS (
              SELECT "$txId"
              FROM jazz_tx
              WHERE "$globalEpoch" <= (
                SELECT "$baseGlobalEpoch" FROM jazz_branch WHERE "$branchId" = ?1
              )
              UNION ALL
              SELECT "$txId"
              FROM jazz_branch_tx
              WHERE "$branchId" = ?1
            ),
            ranked AS (
              SELECT
                h."$rowId",
                h."$txId",
                h.title,
                h.done,
                h."$createdAt",
                h."$isDeleted",
                row_number() OVER (
                  PARTITION BY h."$rowId"
                  ORDER BY h."$txId" DESC
                ) AS rn
              FROM todos__schema_v1_history h
              JOIN branch_visible_tx visible ON visible."$txId" = h."$txId"
            )
            SELECT "$rowId", "$txId", title, done, "$createdAt"
            FROM ranked
            WHERE rn = 1
              AND "$isDeleted" = 0
              AND done = 0
            ORDER BY "$createdAt" DESC
            LIMIT ?2
            "#,
        )
        .unwrap();
    let mut rows = stmt.query(params![branch_id, limit]).unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().unwrap() {
        let row_id: i64 = row.get(0).unwrap();
        let tx_id: i64 = row.get(1).unwrap();
        black_box((row_id, tx_id));
        count += 1;
    }
    count
}

fn query_snapshot_not_exists(conn: &Connection, branch_id: &str, limit: i64) -> usize {
    let mut stmt = conn
        .prepare(
            r#"
            WITH branch AS (
              SELECT "$baseGlobalEpoch" AS base FROM jazz_branch WHERE "$branchId" = ?1
            )
            SELECT h."$rowId", h."$txId", h.title, h.done, h."$createdAt"
            FROM todos__schema_v1_history h
            JOIN jazz_tx tx ON tx."$txId" = h."$txId"
            CROSS JOIN branch b
            WHERE (
                tx."$globalEpoch" <= b.base
                OR EXISTS (
                  SELECT 1 FROM jazz_branch_tx bt
                  WHERE bt."$branchId" = ?1 AND bt."$txId" = h."$txId"
                )
              )
              AND h."$isDeleted" = 0
              AND h.done = 0
              AND NOT EXISTS (
                SELECT 1
                FROM todos__schema_v1_history newer
                JOIN jazz_tx newer_tx ON newer_tx."$txId" = newer."$txId"
                WHERE newer."$rowId" = h."$rowId"
                  AND (
                    newer_tx."$globalEpoch" <= b.base
                    OR EXISTS (
                      SELECT 1 FROM jazz_branch_tx newer_bt
                      WHERE newer_bt."$branchId" = ?1
                        AND newer_bt."$txId" = newer."$txId"
                    )
                  )
                  AND newer."$txId" > h."$txId"
              )
            ORDER BY h."$createdAt" DESC
            LIMIT ?2
            "#,
        )
        .unwrap();
    let mut rows = stmt.query(params![branch_id, limit]).unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().unwrap() {
        let row_id: i64 = row.get(0).unwrap();
        let tx_id: i64 = row.get(1).unwrap();
        black_box((row_id, tx_id));
        count += 1;
    }
    count
}

fn query_snapshot_candidate_index(conn: &Connection, branch_id: &str, limit: i64) -> usize {
    let mut stmt = conn
        .prepare(
            r#"
            WITH branch AS (
              SELECT "$baseGlobalEpoch" AS base FROM jazz_branch WHERE "$branchId" = ?1
            )
            SELECT h."$rowId", h."$txId", h.title, h.done, h."$createdAt"
            FROM todos__schema_v1_history h INDEXED BY todos_history_done_created_tx_row
            JOIN jazz_tx tx ON tx."$txId" = h."$txId"
            CROSS JOIN branch b
            WHERE h.done = 0
              AND h."$isDeleted" = 0
              AND (
                tx."$globalEpoch" <= b.base
                OR EXISTS (
                  SELECT 1 FROM jazz_branch_tx bt
                  WHERE bt."$branchId" = ?1 AND bt."$txId" = h."$txId"
                )
              )
              AND NOT EXISTS (
                SELECT 1
                FROM todos__schema_v1_history newer INDEXED BY todos_history_row_tx
                JOIN jazz_tx newer_tx ON newer_tx."$txId" = newer."$txId"
                WHERE newer."$rowId" = h."$rowId"
                  AND newer."$txId" > h."$txId"
                  AND (
                    newer_tx."$globalEpoch" <= b.base
                    OR EXISTS (
                      SELECT 1 FROM jazz_branch_tx newer_bt
                      WHERE newer_bt."$branchId" = ?1
                        AND newer_bt."$txId" = newer."$txId"
                    )
                  )
              )
            ORDER BY h."$createdAt" DESC
            LIMIT ?2
            "#,
        )
        .unwrap();
    let mut rows = stmt.query(params![branch_id, limit]).unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().unwrap() {
        let row_id: i64 = row.get(0).unwrap();
        let tx_id: i64 = row.get(1).unwrap();
        black_box((row_id, tx_id));
        count += 1;
    }
    count
}

fn query_snapshot_candidate_index_overfetch(
    conn: &Connection,
    branch_id: &str,
    limit: i64,
) -> usize {
    let overfetch = limit * 20;
    let mut stmt = conn
        .prepare(
            r#"
            WITH branch AS (
              SELECT "$baseGlobalEpoch" AS base FROM jazz_branch WHERE "$branchId" = ?1
            ),
            candidates AS (
              SELECT h."$rowId", h."$txId", h.title, h.done, h."$createdAt", h."$isDeleted"
              FROM todos__schema_v1_history h INDEXED BY todos_history_done_created_tx_row
              JOIN jazz_tx tx ON tx."$txId" = h."$txId"
              CROSS JOIN branch b
              WHERE h.done = 0
                AND h."$isDeleted" = 0
                AND (
                  tx."$globalEpoch" <= b.base
                  OR EXISTS (
                    SELECT 1 FROM jazz_branch_tx bt
                    WHERE bt."$branchId" = ?1 AND bt."$txId" = h."$txId"
                  )
                )
              ORDER BY h."$createdAt" DESC
              LIMIT ?2
            )
            SELECT c."$rowId", c."$txId", c.title, c.done, c."$createdAt"
            FROM candidates c
            CROSS JOIN branch b
            WHERE NOT EXISTS (
              SELECT 1
              FROM todos__schema_v1_history newer INDEXED BY todos_history_row_tx
              JOIN jazz_tx newer_tx ON newer_tx."$txId" = newer."$txId"
              WHERE newer."$rowId" = c."$rowId"
                AND newer."$txId" > c."$txId"
                AND (
                  newer_tx."$globalEpoch" <= b.base
                  OR EXISTS (
                    SELECT 1 FROM jazz_branch_tx newer_bt
                    WHERE newer_bt."$branchId" = ?1
                      AND newer_bt."$txId" = newer."$txId"
                  )
                )
            )
            ORDER BY c."$createdAt" DESC
            LIMIT ?3
            "#,
        )
        .unwrap();
    let mut rows = stmt.query(params![branch_id, overfetch, limit]).unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().unwrap() {
        let row_id: i64 = row.get(0).unwrap();
        let tx_id: i64 = row.get(1).unwrap();
        black_box((row_id, tx_id));
        count += 1;
    }
    count
}

fn query_sparse_branch_overlay(conn: &Connection, branch_id: &str, limit: i64) -> usize {
    let mut stmt = conn
        .prepare(
            r#"
            WITH changed AS (
              SELECT "$rowId"
              FROM todos__schema_v1_branch_delta
              WHERE "$branchId" = ?1
            ),
            overlay AS (
              SELECT
                "$rowId",
                "$visibleTxId",
                title,
                done,
                "$createdAt",
                "$isDeleted"
              FROM todos__schema_v1_branch_delta
              WHERE "$branchId" = ?1
                AND "$isDeleted" = 0
                AND done = 0

              UNION ALL

              SELECT
                base."$rowId",
                base."$visibleTxId",
                base.title,
                base.done,
                base."$createdAt",
                base."$isDeleted"
              FROM todos__schema_v1_base_current base
              WHERE base."$isDeleted" = 0
                AND base.done = 0
                AND NOT EXISTS (
                  SELECT 1
                  FROM changed
                  WHERE changed."$rowId" = base."$rowId"
                )
            )
            SELECT "$rowId", "$visibleTxId", title, done, "$createdAt"
            FROM overlay
            ORDER BY "$createdAt" DESC
            LIMIT ?2
            "#,
        )
        .unwrap();
    let mut rows = stmt.query(params![branch_id, limit]).unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().unwrap() {
        let row_id: i64 = row.get(0).unwrap();
        let tx_id: i64 = row.get(1).unwrap();
        black_box((row_id, tx_id));
        count += 1;
    }
    count
}

fn bench_current_reads(c: &mut Criterion) {
    let conn = current_db(ROWS);
    let since = ROWS - 20_000;
    assert_eq!(
        query_current_page(&conn, since, PAGE_SIZE),
        PAGE_SIZE as usize
    );
    assert_eq!(
        query_current_scope_json(&conn, since, PAGE_SIZE),
        PAGE_SIZE as usize
    );

    let mut group = c.benchmark_group("sqlite_shapes/current_projection");
    group.measurement_time(Duration::from_secs(8));
    group.bench_function("filter_user_and_system_order_system_limit_50", |b| {
        b.iter(|| query_current_page(black_box(&conn), black_box(since), black_box(PAGE_SIZE)));
    });
    group.bench_function("same_query_with_json_result_scope_limit_50", |b| {
        b.iter(|| {
            query_current_scope_json(black_box(&conn), black_box(since), black_box(PAGE_SIZE))
        });
    });
    group.finish();
}

fn bench_branch_snapshots(c: &mut Criterion) {
    let conn = snapshot_db(ROWS, BRANCHES, OVERRIDES_PER_BRANCH);
    let branch_id = "branch_777";
    assert_eq!(
        query_snapshot_window(&conn, branch_id, PAGE_SIZE),
        PAGE_SIZE as usize
    );
    assert_eq!(
        query_snapshot_not_exists(&conn, branch_id, PAGE_SIZE),
        PAGE_SIZE as usize
    );
    assert_eq!(
        query_snapshot_candidate_index(&conn, branch_id, PAGE_SIZE),
        PAGE_SIZE as usize
    );
    assert_eq!(
        query_snapshot_candidate_index_overfetch(&conn, branch_id, PAGE_SIZE),
        PAGE_SIZE as usize
    );
    assert_eq!(
        query_sparse_branch_overlay(&conn, branch_id, PAGE_SIZE),
        PAGE_SIZE as usize
    );

    let mut group = c.benchmark_group("sqlite_shapes/branch_snapshot");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("history_window_100k_rows_1k_branches_limit_50", |b| {
        b.iter(|| {
            query_snapshot_window(black_box(&conn), black_box(branch_id), black_box(PAGE_SIZE))
        });
    });
    group.bench_function("history_not_exists_100k_rows_1k_branches_limit_50", |b| {
        b.iter(|| {
            query_snapshot_not_exists(black_box(&conn), black_box(branch_id), black_box(PAGE_SIZE))
        });
    });
    group.bench_function(
        "history_candidate_index_100k_rows_1k_branches_limit_50",
        |b| {
            b.iter(|| {
                query_snapshot_candidate_index(
                    black_box(&conn),
                    black_box(branch_id),
                    black_box(PAGE_SIZE),
                )
            });
        },
    );
    group.bench_function(
        "history_candidate_index_overfetch_100k_rows_1k_branches_limit_50",
        |b| {
            b.iter(|| {
                query_snapshot_candidate_index_overfetch(
                    black_box(&conn),
                    black_box(branch_id),
                    black_box(PAGE_SIZE),
                )
            });
        },
    );
    group.bench_function("sparse_overlay_100k_rows_1k_branches_limit_50", |b| {
        b.iter(|| {
            query_sparse_branch_overlay(
                black_box(&conn),
                black_box(branch_id),
                black_box(PAGE_SIZE),
            )
        });
    });
    group.finish();
}

fn bench_branch_seed(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_shapes/seed");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("seed_snapshot_100k_rows_1k_branches_100_overrides", |b| {
        b.iter_batched(
            || (),
            |_| snapshot_db(ROWS, BRANCHES, OVERRIDES_PER_BRANCH),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_current_reads,
    bench_branch_snapshots,
    bench_branch_seed
);
criterion_main!(benches);

//! Shared helpers for the Jazz topologies: schema, row cells, batched writes,
//! point lookups, and the server's permission / edge-readiness plumbing.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, PolicyExpr, QueryBuilder, Schema, SchemaBuilder,
    SchemaHash, TablePolicies, TableSchema, Value,
};

use crate::config::{BATCH, PROBE};
use crate::dataset::{COLUMNS, Plant, TABLE};

pub(crate) fn schema() -> Schema {
    let mut table = TableSchema::builder(TABLE);
    for column in COLUMNS {
        table = table.column(column, ColumnType::Text);
    }
    SchemaBuilder::new().table(table).build()
}

/// A plant as Jazz cells, keyed by [`COLUMNS`].
fn cells(plant: &Plant) -> HashMap<String, Value> {
    let values = [
        &plant.id,
        &plant.fields[0],
        &plant.fields[1],
        &plant.fields[2],
        &plant.fields[3],
        &plant.fields[4],
    ];
    COLUMNS
        .iter()
        .zip(values)
        .map(|(col, val)| ((*col).to_string(), Value::Text(val.clone())))
        .collect()
}

/// Insert every plant in batched transactions. With `wait_tier`, block on server
/// settlement after each batch; otherwise just commit locally. Returns write time.
pub(crate) async fn write_plants(
    client: &JazzClient,
    plants: &[Plant],
    wait_tier: Option<DurabilityTier>,
    progress: bool,
) -> Duration {
    let n = plants.len() as u64;
    let t = Instant::now();
    let mut i = 0u64;
    while i < n {
        let t_commit = Instant::now();
        let tx = client.begin_transaction().expect("begin transaction");
        for _ in 0..BATCH.min(n - i) {
            tx.insert(TABLE, cells(&plants[i as usize]))
                .expect("staged insert");
            i += 1;
        }
        let batch = tx.commit().expect("commit transaction");
        let commit = t_commit.elapsed();
        let mut wait = Duration::ZERO;
        if let Some(tier) = wait_tier {
            let t_wait = Instant::now();
            client
                .wait_for_batch(batch, tier)
                .await
                .expect("wait for batch");
            wait = t_wait.elapsed();
        }
        if progress {
            eprintln!(
                "    batch {i}/{n}: commit {:.0} ms + wait {:.0} ms  (total elapsed {:.1}s)",
                commit.as_secs_f64() * 1e3,
                wait.as_secs_f64() * 1e3,
                t.elapsed().as_secs_f64()
            );
        }
    }
    t.elapsed()
}

/// Average latency of a single point query by `plant_id` at `tier`, over a small
/// sub-sample (Jazz reads are full scans, so `SAMPLE` would be too slow).
pub(crate) async fn point_lookup(
    client: &JazzClient,
    ids: &[String],
    tier: DurabilityTier,
) -> Duration {
    let mut count = 0u32;
    let t = Instant::now();
    for id in ids.iter().take(PROBE) {
        let query = QueryBuilder::new(TABLE)
            .filter_eq("plant_id", Value::Text(id.clone()))
            .build();
        let _ = client.query(query, Some(tier)).await.expect("point query");
        count += 1;
    }
    t.elapsed() / count.max(1)
}

/// Rows the client sees for the table at `tier` (a full-table query).
pub(crate) async fn row_count(client: &JazzClient, tier: DurabilityTier) -> usize {
    client
        .query(QueryBuilder::new(TABLE).build(), Some(tier))
        .await
        .expect("count query")
        .len()
}

/// Publish an allow-all permission policy via the server's admin HTTP endpoint,
/// retrying until the server catalogue is ready.
pub(crate) async fn publish_allow_all(
    base_url: &str,
    app_id: &str,
    admin_secret: &str,
    schema: &Schema,
) {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let body = serde_json::json!({
        "schemaHash": SchemaHash::compute(schema).to_string(),
        "permissions": { TABLE: serde_json::to_value(&policies).expect("serialize policies") },
        "expectedParentBundleObjectId": Option::<String>::None,
    });
    let http = reqwest::Client::new();
    let url = format!("{base_url}/apps/{app_id}/admin/permissions");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let resp = http
            .post(&url)
            .header("X-Jazz-Admin-Secret", admin_secret)
            .json(&body)
            .send()
            .await
            .expect("publish permissions request");
        match resp.status() {
            reqwest::StatusCode::CREATED => return,
            reqwest::StatusCode::NOT_FOUND if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            status => {
                let text = resp.text().await.unwrap_or_default();
                panic!("publish permissions failed: {status} {text}");
            }
        }
    }
}

/// Block until an `EdgeServer`-tier query against the table succeeds.
pub(crate) async fn wait_edge_ready(client: &JazzClient) {
    let query = QueryBuilder::new(TABLE).build();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client
            .query(query.clone(), Some(DurabilityTier::EdgeServer))
            .await
            .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "edge query never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

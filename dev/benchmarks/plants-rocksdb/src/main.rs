use std::collections::HashMap;
use std::time::{Duration, Instant};

use jazz_tools::server::JazzServer;
use jazz_tools::{
    ClientStorage, ColumnType, DurabilityTier, JazzClient, PolicyExpr, QueryBuilder, Schema,
    SchemaBuilder, SchemaHash, TablePolicies, TableSchema, Value,
};
use uuid::Uuid;

/// Parse one RFC-4180-ish line from the USDA file (every field double-quoted).
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => fields.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
        }
    }
    fields.push(cur);
    fields
}

/// Load up to `limit` real plant records (5 fields each) from the USDA dataset.
fn load_plants(limit: usize) -> Vec<[String; 5]> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/data/plantlst.txt");
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e}\nrun scripts/setup.sh first"));
    let mut out = Vec::new();
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let mut c = parse_csv_line(line);
        c.resize(5, String::new());
        out.push([
            std::mem::take(&mut c[0]),
            std::mem::take(&mut c[1]),
            std::mem::take(&mut c[2]),
            std::mem::take(&mut c[3]),
            std::mem::take(&mut c[4]),
        ]);
        if out.len() >= limit {
            break;
        }
    }
    out
}

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("plants")
                .column("plant_id", ColumnType::Text)
                .column("symbol", ColumnType::Text)
                .column("synonym_symbol", ColumnType::Text)
                .column("scientific_name", ColumnType::Text)
                .column("common_name", ColumnType::Text)
                .column("family", ColumnType::Text),
        )
        .build()
}

async fn publish_allow_all(base_url: &str, app_id: &str, admin_secret: &str, schema: &Schema) {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let policies_json = serde_json::to_value(&policies).expect("serialize policies");
    let body = serde_json::json!({
        "schemaHash": SchemaHash::compute(schema).to_string(),
        "permissions": { "plants": policies_json },
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
        let status = resp.status();
        if status == reqwest::StatusCode::CREATED {
            return;
        }
        if status == reqwest::StatusCode::NOT_FOUND && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let text = resp.text().await.unwrap_or_default();
        panic!("publish permissions failed: {status} {text}");
    }
}

async fn wait_edge_ready(client: &JazzClient) {
    let q = QueryBuilder::new("plants").build();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client
            .query(q.clone(), Some(DurabilityTier::EdgeServer))
            .await
            .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "edge query never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn run() {
    let schema = schema();
    let server_dir = tempfile::tempdir().expect("server tempdir");
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .with_rocksdb_storage()
        .with_data_dir(server_dir.path())
        .start()
        .await;

    publish_allow_all(
        &server.base_url(),
        &server.app_id().to_string(),
        server.admin_secret(),
        &schema,
    )
    .await;

    let mut ctx = server.make_client_context_for_user(schema.clone(), "bench");
    ctx.storage = ClientStorage::Persistent;
    let client_dir = ctx.data_dir.clone();
    let client = JazzClient::connect(ctx).await.expect("connect client");

    wait_edge_ready(&client).await;

    const N: u64 = 7500;
    const BATCH: u64 = 1000;
    let plants = load_plants(N as usize);
    let n = plants.len() as u64;
    // Logical payload: assigned id (36-byte uuid) + every field byte.
    let logical_bytes: u64 = plants
        .iter()
        .map(|f| 36 + f.iter().map(String::len).sum::<usize>() as u64)
        .sum();

    let mut ids = Vec::new();
    let t_write = Instant::now();
    let mut i = 0u64;
    while i < n {
        let tx = client.begin_transaction().expect("begin transaction");
        for _ in 0..BATCH.min(n - i) {
            let p = &plants[i as usize];
            let uuid = Uuid::from_u128((i + 1) as u128).to_string();
            let values: HashMap<String, Value> = HashMap::from([
                ("plant_id".to_string(), Value::Text(uuid.clone())),
                ("symbol".to_string(), Value::Text(p[0].clone())),
                ("synonym_symbol".to_string(), Value::Text(p[1].clone())),
                ("scientific_name".to_string(), Value::Text(p[2].clone())),
                ("common_name".to_string(), Value::Text(p[3].clone())),
                ("family".to_string(), Value::Text(p[4].clone())),
            ]);
            tx.insert("plants", values).expect("staged insert");
            ids.push(uuid);
            i += 1;
        }
        let batch = tx.commit().expect("commit transaction");
        client
            .wait_for_batch(batch, DurabilityTier::EdgeServer)
            .await
            .expect("wait for batch to reach edge");
        println!(
            "    batch {i}/{n} synced  elapsed={:.1}s",
            t_write.elapsed().as_secs_f64()
        );
    }
    let write = t_write.elapsed();

    // Point lookups by the plant_id data column, at both tiers, timed.
    for (label, tier) in [
        ("Local", DurabilityTier::Local),
        ("EdgeServer", DurabilityTier::EdgeServer),
    ] {
        let probe: Vec<&String> = ids
            .iter()
            .step_by((n as usize / 20).max(1))
            .take(20)
            .collect();
        let t = Instant::now();
        let mut found = 0;
        for id in &probe {
            let q = QueryBuilder::new("plants")
                .filter_eq("plant_id", Value::Text((*id).clone()))
                .build();
            let rows = client.query(q, Some(tier)).await.expect("point query");
            if !rows.is_empty() {
                found += 1;
            }
        }
        let per = t.elapsed() / probe.len().max(1) as u32;
        println!(
            "  {label:<10} point lookup by plant_id: {:.3} ms/lookup  ({}/{} found)",
            per.as_secs_f64() * 1e3,
            found,
            probe.len()
        );
    }

    let server_bytes = dir_size(server.data_dir());
    let client_bytes = dir_size(&client_dir);
    let logical_mb = logical_bytes as f64 / 1e6;
    println!(
        "\nwrote+synced {n} rows in {:.1}s  ({:.0} rows/s)\n  \
         logical payload   {:.2} MB\n  \
         server on-disk    {:.1} MB  ({:.1}× logical)\n  \
         client on-disk    {:.1} MB  ({:.1}× logical)",
        write.as_secs_f64(),
        n as f64 / write.as_secs_f64(),
        logical_mb,
        server_bytes as f64 / 1e6,
        server_bytes as f64 / logical_bytes.max(1) as f64,
        client_bytes as f64 / 1e6,
        client_bytes as f64 / logical_bytes.max(1) as f64,
    );
    server.shutdown().await;
}

fn dir_size(path: &std::path::Path) -> u64 {
    let Ok(md) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if md.is_dir() {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| dir_size(&e.path()))
            .sum()
    } else {
        md.len()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::task::LocalSet::new().run_until(run()).await;
}

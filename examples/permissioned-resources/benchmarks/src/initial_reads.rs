//! Initial SELECTs over a reopened authority, with exact membership and encoded
//! result receipts. Seeding, reopening, and result hashing are outside read time.

use super::*;

pub(super) fn run(schema: &JazzSchema, seeded: &Seeded, config: &Config) {
    let tables = std::env::var("JAZZ_CUSTOMER_ONLY_TABLE")
        .map(|table| vec![table])
        .unwrap_or_else(|_| subscription_tables());
    let limit = std::env::var("JAZZ_CUSTOMER_QUERY_LIMIT")
        .ok()
        .map(|value| value.parse::<usize>().expect("valid query limit"));
    let repetitions = std::env::var("JAZZ_CUSTOMER_INITIAL_SELECT_REPETITIONS")
        .ok()
        .map_or(1, |value| {
            value.parse::<usize>().expect("valid repetitions")
        });
    let same_node = std::env::var_os("JAZZ_CUSTOMER_REOPEN_SEEDED_NODE").is_some();
    let reopen_node = node(if same_node { 1 } else { 6 });
    let dir = tempfile::tempdir().expect("initial SELECT store directory");
    let started = Instant::now();
    copy_dir_contents(seeded._core_dir.path(), dir.path()).expect("copy seeded store");
    let copy_us = started.elapsed().as_micros();
    let started = Instant::now();
    let server = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage: BoxedStorage::new(open_storage(dir.path(), schema)),
        identity: DbIdentity {
            node: reopen_node,
            author: config.client_author(seeded),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node_uuid_seed(
            reopen_node,
        )))),
    }))
    .expect("open authority for initial SELECTs");
    let open_us = started.elapsed().as_micros();
    let visible = expected_visible_rows(seeded, config.identity);
    for repetition in 0..repetitions {
        let mut per_table = Vec::new();
        let mut total_read_us = 0;
        let mut total_prepare_us = 0;
        let mut total_rows = 0;
        for table in &tables {
            let mut query = Query::from(table.as_str());
            if let Some(limit) = limit {
                query = query.limit(limit);
            }
            let started = Instant::now();
            let prepared = server
                .prepare_query(&query)
                .expect("prepare initial SELECT");
            let prepare_us = started.elapsed().as_micros();
            #[cfg(feature = "cold-settle-attribution")]
            jazz_sim::phase_attribution::reset();
            let started = Instant::now();
            let rows = block_on(server.all_for_identity(
                &prepared,
                ReadOpts {
                    tier: jazz::tx::DurabilityTier::Global,
                    ..ReadOpts::default()
                },
                config.client_author(seeded),
            ))
            .expect("initial SELECT");
            let read_us = started.elapsed().as_micros();
            #[cfg(feature = "cold-settle-attribution")]
            let phase_trace = jazz_sim::phase_attribution::snapshot();
            #[cfg(not(feature = "cold-settle-attribution"))]
            let phase_trace = JsonValue::Null;
            let expected = visible[table]
                .iter()
                .copied()
                .take(limit.unwrap_or(usize::MAX))
                .collect::<Vec<_>>();
            let actual = rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "exact authorized UUID-ordered SELECT: {table}"
            );
            let encoded = jazz::binding_codec::encode_rows(&rows).expect("encode result receipt");
            let mut digest = std::collections::hash_map::DefaultHasher::new();
            encoded.hash(&mut digest);
            total_read_us += read_us;
            total_prepare_us += prepare_us;
            total_rows += rows.len();
            per_table.push(json!({
                "table": table,
                "rows": rows.len(),
                "prepare_us": prepare_us,
                "read_us": read_us,
                "encoded_bytes": encoded.len(),
                "encoded_hash": digest.finish(),
                "phase_trace": phase_trace,
            }));
        }
        println!(
            "{}",
            json!({
                "benchmark": "permissioned_resources_initial_selects",
                "query_limit": limit,
                "read_iteration": repetition,
                "reopen_seeded_node": same_node,
                "copy_us": copy_us,
                "open_us": open_us,
                "queries": per_table.len(),
                "rows": total_rows,
                "prepare_us": total_prepare_us,
                "read_us": total_read_us,
                "per_table": per_table,
                "storage": "rocks",
                "seed_cache_hit": seeded.seed_cache_hit,
            })
        );
    }
    block_on(server.close()).expect("close initial SELECT authority");
}

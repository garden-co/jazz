use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;
use jazz_testkit::duplex_transport::duplex;

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("route", ColumnType::U32),
            ColumnSchema::new("label", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn open_client(seed: u8, schema: JazzSchema) -> Db<MemoryStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed; 16]),
                author: AuthorId::from_bytes([seed; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(seed as u64)),
    ))
    .expect("open client")
}

fn open_server(schema: JazzSchema) -> Db<MemoryStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x5e; 16]),
                author: AuthorId::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x5e)),
    ))
    .expect("open server")
}

fn global_read_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        ..ReadOpts::default()
    }
}

fn row(seed: u64) -> RowUuid {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7200_u64.to_be_bytes());
    bytes[8..].copy_from_slice(&seed.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn measure_unrelated_coverage_group_refresh(group_count: usize) -> Duration {
    let schema = schema();
    let server = open_server(schema.clone());
    let client = open_client(0xc1, schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, AuthorId::from_bytes([0xc1; 16]));

    let attachments = (0..group_count)
        .map(|route| {
            let prepared = client
                .prepare_query(
                    &Query::from("items").filter(eq(col("route"), lit(Value::U32(route as u32)))),
                )
                .unwrap_or_else(|error| panic!("prepare coverage group {route}: {error}"));
            client
                .attach_query_with_opts(&prepared, global_read_opts())
                .unwrap_or_else(|error| panic!("attach coverage group {route}: {error}"))
        })
        .collect::<Vec<_>>();

    for _ in 0..100 {
        client.tick().expect("send coverage groups");
        server.tick().expect("serve coverage groups");
        client.tick().expect("receive coverage groups");
        if attachments
            .iter()
            .all(|attachment| client.query_attachment_is_covered(attachment))
        {
            break;
        }
    }
    assert!(
        attachments
            .iter()
            .all(|attachment| client.query_attachment_is_covered(attachment)),
        "coverage groups did not become covered"
    );

    server
        .seed_settled_mergeable_for_bootstrap(
            "items",
            row(1_000_000 + group_count as u64),
            AuthorId::SYSTEM,
            BTreeMap::from([
                ("route".to_owned(), Value::U32(u32::MAX)),
                ("label".to_owned(), Value::String("unrelated".to_owned())),
            ]),
        )
        .expect("write unrelated row");

    let start = Instant::now();
    server.tick().expect("serve unrelated row change");
    let elapsed = start.elapsed();

    assert_eq!(attachments.len(), group_count);
    elapsed
}

#[test]
fn unrelated_coverage_group_refresh_is_linear_in_groups_not_flushes() {
    let small = measure_unrelated_coverage_group_refresh(100);
    let large = measure_unrelated_coverage_group_refresh(1_000);
    let group_ratio = large.as_secs_f64() / small.as_secs_f64().max(0.000_001);

    eprintln!(
        "coverage-group refresh small={small:?} large={large:?} group_ratio={group_ratio:.2}"
    );
    assert!(
        group_ratio <= 15.0,
        "one-row refresh grew superlinearly with unrelated coverage groups: \
         small={small:?}, large={large:?}, group_ratio={group_ratio:.2}"
    );
}

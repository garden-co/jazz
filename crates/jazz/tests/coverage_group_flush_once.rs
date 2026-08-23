use std::collections::BTreeMap;
use std::time::{Duration, Instant};

mod common;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, QueryAttachment, ReadOpts,
    SeededRowIdSource,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_testkit::duplex_transport::duplex;

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("route", ColumnType::BigInt)
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_client(seed: u8, schema: JazzSchema) -> Db<TestStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed; 16]),
                author: AuthorSubject::for_test_bytes([seed; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(seed as u64)),
    ))
    .expect("open client")
}

fn open_server(schema: JazzSchema) -> Db<TestStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x5e; 16]),
                author: AuthorSubject::SYSTEM,
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

struct CoverageGroupFixture {
    _client: Db<TestStorage>,
    server: Db<TestStorage>,
    _attachments: Vec<QueryAttachment>,
    next_row: u64,
}

fn prepare_coverage_group_fixture(group_count: usize) -> CoverageGroupFixture {
    let schema = schema();
    let server = open_server(schema.clone());
    let client = open_client(0xc1, schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = jazz::db::block_on(client.connect_upstream(client_transport));
    let _subscriber =
        server.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0xc1; 16]));

    let attachments = (0..group_count)
        .map(|route| {
            let prepared = client
                .prepare_query(
                    &Query::from("items").filter(eq(col("route"), lit(Value::I64(route as i64)))),
                )
                .unwrap_or_else(|error| panic!("prepare coverage group {route}: {error}"));
            client
                .attach_query_with_opts(&prepared, global_read_opts())
                .unwrap_or_else(|error| panic!("attach coverage group {route}: {error}"))
        })
        .collect::<Vec<_>>();

    for _ in 0..100 {
        block_on(client.tick()).expect("send coverage groups");
        block_on(server.tick()).expect("serve coverage groups");
        block_on(client.tick()).expect("receive coverage groups");
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

    CoverageGroupFixture {
        _client: client,
        server,
        _attachments: attachments,
        next_row: 1_000_000 + group_count as u64 * 10,
    }
}

impl CoverageGroupFixture {
    fn measure_unrelated_refresh(&mut self) -> Duration {
        self.server
            .seed_settled_mergeable_for_bootstrap(
                "items",
                row(self.next_row),
                AuthorSubject::SYSTEM,
                BTreeMap::from([
                    ("route".to_owned(), Value::I64(i64::from(u32::MAX))),
                    ("label".to_owned(), Value::String("unrelated".to_owned())),
                ]),
            )
            .expect("write unrelated row");
        self.next_row += 1;

        let start = Instant::now();
        block_on(self.server.tick()).expect("serve unrelated row change");
        start.elapsed()
    }
}

#[test]
fn unrelated_coverage_group_refresh_is_linear_in_groups_not_flushes() {
    const PAIRS: usize = 5;
    let mut small_fixture = prepare_coverage_group_fixture(100);
    let mut large_fixture = prepare_coverage_group_fixture(1_000);

    // Each pair runs the two already-prepared topologies back-to-back, and we
    // alternate which topology goes first. This prevents a one-off scheduler
    // pause or cache effect from making the single 100-group control look
    // implausibly cheap. The median keeps the original 15x ceiling while
    // discarding the two noisiest paired observations.
    let samples: [(Duration, Duration); PAIRS] = std::array::from_fn(|pair| {
        if pair % 2 == 0 {
            let small = small_fixture.measure_unrelated_refresh();
            let large = large_fixture.measure_unrelated_refresh();
            (small, large)
        } else {
            let large = large_fixture.measure_unrelated_refresh();
            let small = small_fixture.measure_unrelated_refresh();
            (small, large)
        }
    });
    let mut ratios =
        samples.map(|(small, large)| large.as_secs_f64() / small.as_secs_f64().max(0.000_001));
    ratios.sort_by(f64::total_cmp);
    let group_ratio = ratios[PAIRS / 2];

    eprintln!(
        "coverage-group refresh pairs={samples:?} sorted_ratios={ratios:?} median_ratio={group_ratio:.2}"
    );
    assert!(
        group_ratio <= 15.0,
        "one-row refresh grew superlinearly with unrelated coverage groups: \
         pairs={samples:?}, sorted_ratios={ratios:?}, median_ratio={group_ratio:.2}"
    );
}

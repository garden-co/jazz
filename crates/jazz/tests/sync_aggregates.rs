//! Synced aggregates: a remote aggregate read subscribes to its input rows
//! (the rows the reader may read that match its filters), and the client
//! aggregates them as it would local rows. AVG stays finite for large
//! finite inputs. Not ported from Jazz.

mod common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Aggregate, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{compile_schema, read_and_allow_all_writes, session_eq};

const ME: [&str; 3] = ["user", "identity", "subject"];

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("metrics")
                    .column("owner", ColumnType::Text)
                    .column("kind", ColumnType::Text)
                    .column("value", ColumnType::Double)
                    .policies(read_and_allow_all_writes(session_eq("owner", &ME))),
            )
            .build(),
    )
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x70 + n; 16])
}

fn owner(n: u8) -> Value {
    Value::String(user(n).principal_parts().1)
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([9, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn metric(db: &Db, n: u8, who: u8, kind: &str, value: f64) {
    db.seed_settled_mergeable_for_bootstrap(
        "metrics",
        id(n),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("owner".to_owned(), owner(who)),
            ("kind".to_owned(), Value::String(kind.to_owned())),
            ("value".to_owned(), Value::F64(value)),
        ]),
    )
    .unwrap();
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

/// Each output row's cells, by name.
fn outputs(db: &Db, query: &Query, tier: ReadTier) -> Vec<BTreeMap<String, Value>> {
    let prepared = db.prepare_query(query).unwrap();
    let opts = ReadOpts {
        tier,
        ..ReadOpts::default()
    };
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| {
            row.cells()
                .filter_map(|(name, value)| Some((name.to_owned(), value?.clone())))
                .collect()
        })
        .collect()
}

fn cell(row: &BTreeMap<String, Value>, name: &str) -> Value {
    match row.get(name) {
        Some(Value::Nullable(Some(inner))) => (**inner).clone(),
        Some(value) => value.clone(),
        None => panic!("no `{name}` in {row:?}"),
    }
}

#[test]
fn a_synced_aggregate_reads_only_the_rows_its_reader_may_read() {
    let server = open(0, AuthorSubject::SYSTEM);
    metric(&server, 1, 1, "cpu", 1.0);
    metric(&server, 2, 1, "cpu", 3.0);
    metric(&server, 3, 2, "cpu", 100.0);
    metric(&server, 4, 1, "mem", 7.0);
    let client = open(1, user(1));
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, user(1));

    let query = Query::from("metrics")
        .filter(eq(col("kind"), lit("cpu")))
        .aggregate([Aggregate::count(), Aggregate::sum("value")]);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let rows = outputs(&client, &query, ReadTier::LocalOnly);
    assert_eq!(rows.len(), 1);
    assert_eq!(cell(&rows[0], "count"), Value::U64(2));
    assert_eq!(cell(&rows[0], "sum_value"), Value::F64(4.0));

    // A new input row reaches the replica and the aggregate.
    metric(&server, 5, 1, "cpu", 5.0);
    pump(&server, &client);
    let rows = outputs(&client, &query, ReadTier::Remote);
    assert_eq!(cell(&rows[0], "count"), Value::U64(3));
    assert_eq!(cell(&rows[0], "sum_value"), Value::F64(9.0));
}

#[test]
fn avg_of_large_finite_values_stays_finite() {
    let db = open(0, AuthorSubject::SYSTEM);
    metric(&db, 1, 1, "big", f64::MAX);
    metric(&db, 2, 1, "big", f64::MAX);
    metric(&db, 3, 1, "mixed", f64::MAX);
    metric(&db, 4, 1, "mixed", -f64::MAX);
    metric(&db, 5, 1, "mixed", 6.0);
    let query = Query::from("metrics")
        .aggregate([Aggregate::avg("value")])
        .group_by("kind")
        .order_by("kind", jazz::query::OrderDirection::Asc);
    let rows = outputs(&db, &query, ReadTier::LocalOnly);
    assert_eq!(cell(&rows[0], "avg_value"), Value::F64(f64::MAX));
    assert_eq!(cell(&rows[1], "avg_value"), Value::F64(2.0));
}

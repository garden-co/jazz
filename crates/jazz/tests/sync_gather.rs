//! Gathers on SQLite: recursive reachability seeded by root rows, read
//! locally and synced. A synced gather ships its result plus the edges of
//! its closure (transitively from each result row), and watches each
//! frontier value so an edge that extends the closure reaches the replica.
//! Not ported from Jazz.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Gather, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("name", ColumnType::Text)
                    .nullable_fk_column("parent_id", "teams")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("team_edges")
                    .fk_column("child_team", "teams")
                    .fk_column("parent_team", "teams")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn team(db: &Db, n: u8, name: &str, parent: Option<u8>) {
    let parent = parent.map_or(Value::Nullable(None), |p| Value::Uuid(id(1, p).0));
    db.seed_settled_mergeable_for_bootstrap(
        "teams",
        id(1, n),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("name".to_owned(), Value::String(name.to_owned())),
            ("parent_id".to_owned(), parent),
        ]),
    )
    .unwrap();
}

fn edge(db: &Db, n: u8, child: u8, parent: u8) {
    db.seed_settled_mergeable_for_bootstrap(
        "team_edges",
        id(2, n),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("child_team".to_owned(), Value::Uuid(id(1, child).0)),
            ("parent_team".to_owned(), Value::Uuid(id(1, parent).0)),
        ]),
    )
    .unwrap();
}

fn names(db: &Db, query: &Query, tier: ReadTier) -> Vec<String> {
    let prepared = db.prepare_query(query).unwrap();
    let opts = ReadOpts {
        tier,
        ..ReadOpts::default()
    };
    let mut names: Vec<String> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .filter_map(|row| {
            row.cells().find_map(|(name, value)| match (name, value) {
                ("name", Some(Value::String(s))) => Some(s.clone()),
                _ => None,
            })
        })
        .collect();
    names.sort();
    names
}

fn edges_from(seed: &str) -> Query {
    Query::from("teams")
        .filter(eq(col("name"), lit(seed)))
        .gather(
            Gather::from("team_edges")
                .where_current("child_team")
                .hop_to("parent_team")
                .max_depth(10),
        )
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn connect(server: &Db) -> Db {
    let client = open(1);
    let (client_side, server_side) = duplex();
    let _ = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    client
}

#[test]
fn gather_follows_an_edge_table_and_a_self_reference() {
    let db = open(0);
    team(&db, 1, "root", None);
    team(&db, 2, "mid", Some(1));
    team(&db, 3, "leaf", Some(2));
    team(&db, 4, "other", None);
    edge(&db, 1, 3, 2);
    edge(&db, 2, 2, 1);
    assert_eq!(
        names(&db, &edges_from("leaf"), ReadTier::LocalOnly),
        ["leaf", "mid", "root"]
    );
    let parents = Query::from("teams")
        .filter(eq(col("name"), lit("leaf")))
        .gather(
            Gather::from("teams")
                .where_current("id")
                .hop_to("parent_id")
                .max_depth(10),
        );
    assert_eq!(
        names(&db, &parents, ReadTier::LocalOnly),
        ["leaf", "mid", "root"]
    );
    // The depth bound counts hops from the seed.
    let one_hop = Query::from("teams")
        .filter(eq(col("name"), lit("leaf")))
        .gather(
            Gather::from("team_edges")
                .where_current("child_team")
                .hop_to("parent_team")
                .max_depth(1),
        );
    assert_eq!(names(&db, &one_hop, ReadTier::LocalOnly), ["leaf", "mid"]);
}

#[test]
fn a_cycle_ends_the_gather() {
    let db = open(0);
    team(&db, 1, "a", None);
    team(&db, 2, "b", None);
    edge(&db, 1, 1, 2);
    edge(&db, 2, 2, 1);
    let fixpoint = Query::from("teams")
        .filter(eq(col("name"), lit("a")))
        .gather(
            Gather::from("team_edges")
                .where_current("child_team")
                .hop_to("parent_team"),
        );
    assert_eq!(names(&db, &fixpoint, ReadTier::LocalOnly), ["a", "b"]);
    assert_eq!(
        names(&db, &edges_from("a"), ReadTier::LocalOnly),
        ["a", "b"]
    );
}

#[test]
fn synced_gather_ships_its_edges_and_follows_new_ones() {
    let server = open(0);
    team(&server, 1, "team-1", None);
    team(&server, 2, "team-2", None);
    team(&server, 3, "team-3", None);
    edge(&server, 1, 1, 2);
    let client = connect(&server);
    let query = edges_from("team-1");
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        names(&client, &query, ReadTier::LocalOnly),
        ["team-1", "team-2"]
    );

    // An edge from a closure row extends the closure on the replica.
    edge(&server, 2, 2, 3);
    pump(&server, &client);
    assert_eq!(
        names(&client, &query, ReadTier::LocalOnly),
        ["team-1", "team-2", "team-3"]
    );
    assert_eq!(
        names(&client, &query, ReadTier::Remote),
        ["team-1", "team-2", "team-3"]
    );
    // The replica holds the closure's edges, and only them.
    edge(&server, 3, 4, 1);
    team(&server, 4, "team-4", None);
    pump(&server, &client);
    assert_eq!(held(&client, "team_edges"), [id(2, 1), id(2, 2)]);
}

fn held(db: &Db, table: &str) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut rows: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    rows.sort();
    rows
}

#[test]
fn a_synced_gather_with_filters_after_it_is_refused() {
    let server = open(0);
    let client = connect(&server);
    let query = edges_from("team-1").filter(eq(col("name"), lit("team-2")));
    let prepared = client.prepare_query(&query).unwrap();
    let error = match block_on(client.subscribe(&prepared, ReadOpts::default())) {
        Err(error) => error,
        Ok(_) => panic!("a filtered gather must not sync silently"),
    };
    assert!(
        error.message.contains("not supported yet"),
        "{}",
        error.message
    );
}

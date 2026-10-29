//! Junction traversals (`join_via`): a root row matches when some
//! joined row does, at every nested level, and a joined row counts only
//! when the reader may read it. Synced, the server ships each level's
//! correlated rows with the result.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{JoinCorrelation, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};

const ME: [&str; 3] = ["user", "identity", "subject"];

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("orgs")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("teams")
                    .fk_column("org_id", "orgs")
                    .column("kind", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("team_id", "teams")
                    .column("member", ColumnType::Text)
                    .column("kind", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq("member", &ME))),
            )
            .build(),
    )
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x50 + n; 16])
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

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn seed(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, Value)]) {
    let cells = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    db.seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, cells)
        .unwrap();
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn member(n: u8) -> Value {
    Value::String(user(n).principal_parts().1)
}

/// Orgs 1 and 2, one team each; user 1 is in team 1, user 2 in team 2.
fn server() -> Db {
    let db = open(0, AuthorSubject::SYSTEM);
    for n in 1..=2 {
        seed(
            &db,
            "orgs",
            id(1, n),
            &[("name", text(&format!("org {n}")))],
        );
        seed(
            &db,
            "teams",
            id(2, n),
            &[("org_id", Value::Uuid(id(1, n).0)), ("kind", text("eng"))],
        );
        seed(
            &db,
            "memberships",
            id(3, n),
            &[
                ("team_id", Value::Uuid(id(2, n).0)),
                ("member", member(n)),
                ("kind", text("eng")),
            ],
        );
    }
    db
}

fn ids(db: &Db, query: &Query, tier: ReadTier) -> Vec<RowUuid> {
    let prepared = db.prepare_query(query).unwrap();
    let opts = ReadOpts {
        tier,
        ..ReadOpts::default()
    };
    let mut ids: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    ids.sort();
    ids
}

fn orgs_with_members() -> Query {
    let membership = Query::from("teams")
        .join_via("memberships", "team_id", [])
        .joins
        .remove(0);
    Query::from("orgs").join_via_with_nested_joins("teams", "org_id", [], [membership])
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn connect(server: &Db, author: AuthorSubject) -> Db {
    let client = open(author.principal_parts().1.len() as u8 % 200 + 1, author);
    let (client_side, server_side) = duplex();
    let _ = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, author);
    client
}

#[test]
fn nested_joins_match_at_every_level() {
    let db = server();
    assert_eq!(
        ids(&db, &orgs_with_members(), ReadTier::LocalOnly),
        [id(1, 1), id(1, 2)]
    );
    // A team without members does not carry its org.
    seed(&db, "orgs", id(1, 3), &[("name", text("org 3"))]);
    seed(
        &db,
        "teams",
        id(2, 3),
        &[("org_id", Value::Uuid(id(1, 3).0)), ("kind", text("eng"))],
    );
    assert_eq!(
        ids(&db, &orgs_with_members(), ReadTier::LocalOnly),
        [id(1, 1), id(1, 2)]
    );
}

#[test]
fn correlated_join_filters_compare_with_the_source_row() {
    let db = server();
    // Membership kinds that match their team's kind.
    seed(
        &db,
        "memberships",
        id(3, 9),
        &[
            ("team_id", Value::Uuid(id(2, 2).0)),
            ("member", member(1)),
            ("kind", text("ops")),
        ],
    );
    block_on(db.update(
        "memberships",
        id(3, 2),
        BTreeMap::from([("kind".to_owned(), text("ops"))]),
        Default::default(),
    ))
    .unwrap();
    let query = Query::from("teams").join_via_column_with_correlations(
        "memberships",
        "team_id",
        "id",
        [JoinCorrelation {
            join_column: "kind".to_owned(),
            source_column: "kind".to_owned(),
        }],
        [],
    );
    assert_eq!(ids(&db, &query, ReadTier::LocalOnly), [id(2, 1)]);
}

#[test]
fn a_synced_join_counts_only_readable_joined_rows() {
    let server = server();
    // User 2 is in team 1 too: hidden from user 1.
    seed(
        &server,
        "memberships",
        id(3, 4),
        &[
            ("team_id", Value::Uuid(id(2, 1).0)),
            ("member", member(2)),
            ("kind", text("eng")),
        ],
    );
    let client = connect(&server, user(1));
    let query = orgs_with_members();
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, &query, ReadTier::LocalOnly), [id(1, 1)]);
    assert_eq!(ids(&client, &query, ReadTier::Remote), [id(1, 1)]);
    assert_eq!(
        ids(&client, &Query::from("memberships"), ReadTier::LocalOnly),
        [id(3, 1)],
        "only her own membership reaches her replica"
    );

    // She leaves team 1: user 2's hidden membership must not keep org 1.
    block_on(server.delete("memberships", id(3, 1), Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, &query, ReadTier::LocalOnly), []);

    // She joins team 2: its org arrives with the membership.
    seed(
        &server,
        "memberships",
        id(3, 5),
        &[
            ("team_id", Value::Uuid(id(2, 2).0)),
            ("member", member(1)),
            ("kind", text("eng")),
        ],
    );
    pump(&server, &client);
    assert_eq!(ids(&client, &query, ReadTier::LocalOnly), [id(1, 2)]);
}

#[test]
fn a_synced_uncorrelated_join_is_refused() {
    let server = server();
    let client = connect(&server, user(1));
    let mut query = Query::from("orgs").join_via("teams", "org_id", [eq(col("kind"), lit("eng"))]);
    query.joins[0].target = jazz::query::JoinTarget::Uncorrelated;
    query.joins[0].on_column = String::new();
    let error = match client.prepare_query(&query) {
        Err(error) => error,
        Ok(prepared) => match block_on(client.subscribe(&prepared, ReadOpts::default())) {
            Err(error) => error,
            Ok(_) => panic!("an uncorrelated join must not sync silently"),
        },
    };
    assert!(
        error.message.contains("not supported yet"),
        "{}",
        error.message
    );
}

#[test]
fn a_joined_row_the_reader_cannot_read_does_not_match() {
    let db = server();
    let prepared = db.prepare_query(&orgs_with_members()).unwrap();
    for (n, org) in [(1, id(1, 1)), (2, id(1, 2))] {
        let rows = block_on(db.all_for_identity(&prepared, ReadOpts::default(), user(n))).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
            [org],
            "the other org is reachable only through the other user's membership"
        );
    }
}

/// Bundles hold their parts in an array of references.
fn bundles_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parts")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("bundles")
                    .column(
                        "items",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Uuid),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_bundles(node: u8) -> Db {
    let config = DbConfig::new(
        bundles_schema(),
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

fn bundles() -> Db {
    let db = open_bundles(0);
    for (n, label) in [(1, "A"), (2, "B"), (3, "C")] {
        seed(&db, "parts", id(4, n), &[("label", text(label))]);
    }
    let holding =
        |parts: &[u8]| Value::Array(parts.iter().map(|&n| Value::Uuid(id(4, n).0)).collect());
    seed(&db, "bundles", id(5, 1), &[("items", holding(&[2, 1]))]);
    seed(&db, "bundles", id(5, 2), &[("items", holding(&[3]))]);
    db
}

fn parts_of_bundle_one() -> Query {
    Query::from("parts").join_via(
        "bundles",
        "items",
        [eq(col("id"), lit(Value::Uuid(id(5, 1).0)))],
    )
}

#[test]
fn a_join_through_an_array_of_references_matches_the_rows_it_holds() {
    let db = bundles();
    // Parts some bundle holds (a reverse hop): the parts of bundle 1.
    assert_eq!(
        ids(&db, &parts_of_bundle_one(), ReadTier::LocalOnly),
        [id(4, 1), id(4, 2)]
    );
    // Bundles holding a part (a forward hop through the array).
    let bundles = Query::from("bundles").join_via_column(
        "parts",
        "id",
        "items",
        [eq(col("label"), lit("C"))],
    );
    assert_eq!(ids(&db, &bundles, ReadTier::LocalOnly), [id(5, 2)]);
}

#[test]
fn a_synced_join_through_an_array_ships_the_rows_holding_the_result() {
    let server = bundles();
    let client = open_bundles(9);
    let (client_side, server_side) = duplex();
    let _ = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let prepared = client.prepare_query(&parts_of_bundle_one()).unwrap();
    let subscription = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    drop(subscription);
    // The replica holds the bundle the join goes through, so it answers
    // the read itself.
    assert_eq!(
        ids(&client, &parts_of_bundle_one(), ReadTier::LocalOnly),
        [id(4, 1), id(4, 2)]
    );
}

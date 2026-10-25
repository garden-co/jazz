//! Remote recursive queries hydrate readable witnesses through normal sync.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, RecursionBound, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn value(n: u8) -> Value {
    Value::Uuid(id(n).0)
}
fn text(s: &str) -> Value {
    Value::String(s.into())
}
fn cells(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    values
        .iter()
        .map(|(k, v)| ((*k).into(), v.clone()))
        .collect()
}
fn schema() -> JazzSchema {
    let readable = TablePolicies::new().with_select(PolicyExpr::eq_session(
        "owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    ));
    common::compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("groups").policies(common::allow_all_policies()))
            .table(
                TableSchemaBuilder::new("docs")
                    .column("body", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("doc", "docs")
                    .fk_column("team", "groups")
                    .column("owner", ColumnType::Text)
                    .policies(readable.clone()),
            )
            .table(
                TableSchemaBuilder::new("edges")
                    .fk_column("member", "groups")
                    .fk_column("parent", "groups")
                    .column("owner", ColumnType::Text)
                    .policies(readable.clone()),
            )
            .table(
                TableSchemaBuilder::new("unindexed_access")
                    .column("doc", ColumnType::Uuid)
                    .fk_column("team", "groups")
                    .policies(common::allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("seeds")
                    .fk_column("team", "groups")
                    .column("user", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(readable),
            )
            .build(),
    )
}
fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: if node == 0 {
                AuthorSubject::SYSTEM
            } else {
                alice()
            },
        },
    );
    let db = if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap();
    db.set_identity_claims(alice(), BTreeMap::from([("sub".into(), text("alice"))]));
    db
}
fn put(db: &Db, table: &str, row: u8, values: &[(&str, Value)]) {
    db.seed_settled_mergeable_for_bootstrap(table, id(row), AuthorSubject::SYSTEM, cells(values))
        .unwrap();
}
fn fixture() -> (Db, Db) {
    let server = open(0);
    for n in 11..=14 {
        put(&server, "groups", n, &[]);
    }
    for n in 1..=3 {
        put(&server, "docs", n, &[("body", text(&format!("doc {n}")))]);
    }
    for (n, member, parent) in [(31, 11, 12), (32, 12, 13)] {
        put(
            &server,
            "edges",
            n,
            &[
                ("member", value(member)),
                ("parent", value(parent)),
                ("owner", text("alice")),
            ],
        );
    }
    for (n, doc, team, owner) in [
        (41, 1, 13, "alice"),
        (42, 2, 14, "alice"),
        (43, 3, 12, "bob"),
    ] {
        put(
            &server,
            "access",
            n,
            &[
                ("doc", value(doc)),
                ("team", value(team)),
                ("owner", text(owner)),
            ],
        );
    }
    for (n, team, user) in [(51, 11, "alice"), (52, 12, "bob")] {
        put(
            &server,
            "seeds",
            n,
            &[
                ("team", value(team)),
                ("user", text(user)),
                ("owner", text(user)),
            ],
        );
    }
    let client = open(1);
    let (up, down) = duplex_transport::duplex();
    let _ = block_on(client.connect_upstream(up));
    server.accept_subscriber_with_claims(
        down,
        alice(),
        Some(BTreeMap::from([("sub".into(), text("alice"))])),
    );
    (server, client)
}
fn query(claim: &str) -> Query {
    Query::from("docs")
        .reachable_via(
            "access",
            "doc",
            "team",
            lit(value(11)),
            "edges",
            "member",
            "parent",
            [],
        )
        .seeded_by("seeds", "user", claim, "team")
}
fn pump(server: &Db, client: &Db) {
    for _ in 0..8 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}
fn local(db: &Db, query: &Query) -> Vec<RowUuid> {
    let prepared = db.prepare_query(query).unwrap();
    let mut rows: Vec<_> = block_on(db.all_for_identity(
        &prepared,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        alice(),
    ))
    .unwrap()
    .iter()
    .map(|r| r.row_uuid())
    .collect();
    rows.sort();
    rows
}
fn cached(db: &Db, table: &str) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let mut ids: Vec<_> = block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    ids.sort();
    ids
}
fn drive<T>(server: &Db, client: &Db, future: impl Future<Output = T>) -> T {
    drive_with(|| pump(server, client), future)
}
fn drive_with<T>(tick: impl Fn(), future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        tick();
    }
    panic!("remote query did not finish");
}

fn remote(server: &Db, client: &Db, query: &Query) -> Vec<RowUuid> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let result = drive(
        server,
        client,
        client.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let jazz::db::SerializedReadResult::Rows(rows) = result else {
        panic!("row result")
    };
    let mut ids: Vec<_> = rows.iter().map(|row| row.row_uuid()).collect();
    ids.sort();
    ids
}

/// Alice's seeded query must use her admitted identity, including when provider
/// input tries to substitute Bob's reserved identity claim.
#[test]
fn seeded_identity_path_is_derived_from_admitted_author() {
    let (server, _) = fixture();
    assert_eq!(local(&server, &query("sub")), vec![id(1)]);
    assert_eq!(local(&server, &query("user.identity.subject")), vec![id(1)]);
    server.set_identity_claims(
        alice(),
        BTreeMap::from([("user.identity.subject".into(), text("bob"))]),
    );
    assert_eq!(local(&server, &query("user.identity.subject")), vec![id(1)]);
}

/// Alice receives only readable graph witnesses. A new alternative path must
/// reach her replica even when the document result does not change, so removing
/// the old path cannot incorrectly remove her document.
#[test]
fn recursive_subscription_hydrates_and_tracks_alternative_paths() {
    let (server, client) = fixture();
    let query = query("sub");
    let prepared = client.prepare_query(&query).unwrap();
    let _stream =
        block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, &query), vec![id(1)]);
    assert_eq!(cached(&client, "access"), vec![id(41)]);
    assert_eq!(cached(&client, "edges"), vec![id(31), id(32)]);
    assert_eq!(cached(&client, "seeds"), vec![id(51)]);

    block_on(server.insert(
        "edges",
        cells(&[
            ("member", value(11)),
            ("parent", value(13)),
            ("owner", text("alice")),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(id(33)),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(cached(&client, "edges"), vec![id(31), id(32), id(33)]);
    block_on(server.delete("edges", id(32), Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, &query), vec![id(1)]);
    block_on(server.update(
        "edges",
        id(33),
        cells(&[("owner", text("bob"))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    assert!(remote(&server, &client, &query).is_empty());
    block_on(server.update(
        "edges",
        id(33),
        cells(&[("owner", text("alice"))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, &query), vec![id(1)]);
}

/// Alice's one-shot read hydrates the same bounded recursive graph as a live
/// subscription, and a maximum depth excludes paths that are too long.
#[test]
fn recursive_fetch_respects_depth_and_hydrates_without_subscription() {
    let (server, client) = fixture();
    let mut query = query("sub");
    query.reachable[0].bound = RecursionBound::MaxDepth(1);
    assert!(remote(&server, &client, &query).is_empty());
    query.reachable[0].bound = RecursionBound::MaxDepth(2);
    assert_eq!(remote(&server, &client, &query), vec![id(1)]);
    assert_eq!(local(&client, &query), vec![id(1)]);
}

/// Alice's filtered fixpoint traversal terminates through a cycle and respects
/// access/edge/seed filters. An unrelated readable access row is not hydrated.
#[test]
fn recursive_fixpoint_preserves_filters_and_cycles() {
    let (server, client) = fixture();
    put(
        &server,
        "edges",
        34,
        &[
            ("member", value(12)),
            ("parent", value(11)),
            ("owner", text("alice")),
        ],
    );
    let mut query = query("user.identity.subject");
    query.reachable[0].bound = RecursionBound::Fixpoint;
    query.reachable[0]
        .edge_filters
        .push(eq(col("owner"), lit("alice")));
    query.reachable[0]
        .access_filters
        .push(eq(col("owner"), lit("alice")));
    query.reachable[0]
        .seed
        .as_mut()
        .unwrap()
        .filters
        .push(eq(col("owner"), lit("alice")));
    assert_eq!(remote(&server, &client, &query), vec![id(1)]);
    assert_eq!(cached(&client, "access"), vec![id(41)]);
    assert_eq!(cached(&client, "edges"), vec![id(31), id(32), id(34)]);
    query.reachable[0]
        .edge_filters
        .push(eq(col("id"), lit(value(31))));
    assert!(remote(&server, &client, &query).is_empty());
}

/// Alice's empty remote query must still refuse an unindexed access lookup;
/// that refusal must leave ordinary reads on the connection usable.
#[test]
fn recursive_empty_unindexed_shape_is_refused_before_settlement() {
    let (server, client) = fixture();
    let mut query = query("sub");
    query.reachable[0].access_table = "unindexed_access".into();
    let bytes = postcard::to_allocvec(&query).unwrap();
    let error = drive(
        &server,
        &client,
        client.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Query);
    assert!(error.message.contains("indexed scalar UUID"), "{error}");
    assert_eq!(
        remote(&server, &client, &Query::from("docs")),
        vec![id(1), id(2), id(3)]
    );
}

/// Alice pins an authority transaction before Bob changes a graph edge. Cold
/// hydration must use the old edge at that cut, while a new remote read uses
/// the changed graph and cannot return the formerly reachable document.
#[test]
fn recursive_authority_snapshot_hydrates_the_original_graph() {
    let (server, client) = fixture();
    remote(&server, &client, &Query::from("docs").limit(0));
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(client.begin_serializable(tx)).unwrap();
    let empty = client.prepare_query(&Query::from("docs").limit(0)).unwrap();
    drive(
        &server,
        &client,
        client.serializable_tx_ref(tx).all_prepared(&empty),
    )
    .unwrap();
    block_on(server.update(
        "edges",
        id(32),
        cells(&[("parent", value(14))]),
        Default::default(),
    ))
    .unwrap();
    let query = query("sub");
    let prepared = client.prepare_query(&query).unwrap();
    let result = drive(
        &server,
        &client,
        client.serializable_tx_ref(tx).all_prepared(&prepared),
    )
    .unwrap();
    assert_eq!(
        result.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![id(1)]
    );
    assert_eq!(remote(&server, &client, &query), vec![id(2)]);
    client.abandon_transaction_handle(tx).unwrap();
}

/// Alice's ordinary and private relays forward supporting rows and real
/// authority settlement. A revoked path cannot be resurrected from a relay's
/// retained local cache while another path still grants the document.
#[test]
fn recursive_support_remains_coherent_through_relays() {
    for private in [false, true] {
        let (server, _) = fixture();
        let relay = open(2);
        if private {
            relay.set_scope_isolated_storage_owner().unwrap();
        }
        let client = open(3);
        for (client, server) in [(&relay, &server), (&client, &relay)] {
            let (up, down) = duplex_transport::duplex();
            block_on(client.connect_upstream(up));
            server.accept_subscriber_with_claims(
                down,
                alice(),
                Some(BTreeMap::from([("sub".into(), text("alice"))])),
            );
        }
        let tick = || {
            for _ in 0..10 {
                for db in [&client, &relay, &server, &relay, &client] {
                    block_on(db.tick()).unwrap();
                }
            }
        };
        let query = query("sub");
        let prepared = client.prepare_query(&query).unwrap();
        let _stream =
            block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap();
        tick();
        assert_eq!(local(&client, &query), vec![id(1)], "private={private}");
        block_on(server.insert(
            "edges",
            cells(&[
                ("member", value(11)),
                ("parent", value(13)),
                ("owner", text("alice")),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(id(33)),
                ..Default::default()
            },
        ))
        .unwrap();
        tick();
        block_on(server.update(
            "edges",
            id(32),
            cells(&[("owner", text("bob"))]),
            Default::default(),
        ))
        .unwrap();
        tick();
        assert_eq!(
            block_on(client.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap()
                .len(),
            1
        );
        block_on(server.delete("edges", id(33), Default::default())).unwrap();
        tick();
        assert!(
            block_on(client.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap()
                .is_empty(),
            "private={private}: revoked old path resurrected"
        );
    }
}

/// Alice's whole-author query operands compare provenance field by field and
/// ignore forged identity records supplied as claims. Bob's rows remain out.
#[test]
fn whole_author_query_claims_use_admitted_provenance() {
    let db = open(0);
    let alice = AuthorSubject::for_test_bytes([7; 16]);
    let bob = AuthorSubject::for_test_bytes([8; 16]);
    for (row, author) in [(61, alice), (62, bob)] {
        block_on(db.insert(
            "docs",
            cells(&[("body", text("provenance"))]),
            jazz::db::InsertOptions {
                row_id: Some(id(row)),
                identity: jazz::db::WriteIdentity::Session(author),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    db.set_identity_claims(alice, BTreeMap::from([("user".into(), bob.to_value())]));
    for (field, claim) in [
        ("$createdBy", "user"),
        ("$createdBy.identity", "user.identity"),
    ] {
        for reverse in [false, true] {
            let (left, right) = (col(field), jazz::query::claim(claim));
            let filter = if reverse {
                eq(right, left)
            } else {
                eq(left, right)
            };
            let prepared = db
                .prepare_query(&Query::from("docs").filter(filter))
                .unwrap();
            let rows =
                block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice)).unwrap();
            assert_eq!(
                rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
                vec![id(61)]
            );
        }
    }
}

/// Alice's graph exceeds the bounded witness budget. The authority must refuse
/// her subscription without publishing partial roots or inputs; other reads on
/// that same connection remain usable.
#[test]
fn recursive_witness_budget_refuses_without_partial_delivery() {
    let (server, client) = fixture();
    for _ in 0..17_000 {
        block_on(server.insert(
            "edges",
            cells(&[
                ("member", value(11)),
                ("parent", value(13)),
                ("owner", text("alice")),
            ]),
            Default::default(),
        ))
        .unwrap();
    }
    let prepared = client.prepare_query(&query("sub")).unwrap();
    let mut stream =
        block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    pump(&server, &client);
    assert!(
        matches!(stream.try_next_event(), Some(jazz::db::SubscriptionEvent::Rejected { reason })
        if reason.contains("limit") || reason.contains("budget"))
    );
    for table in ["docs", "access", "edges", "seeds"] {
        assert!(cached(&client, table).is_empty(), "partial {table}");
    }
    assert_eq!(
        remote(&server, &client, &Query::from("docs")),
        vec![id(1), id(2), id(3)]
    );
}

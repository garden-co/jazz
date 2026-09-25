use std::collections::{BTreeMap, BTreeSet};

mod common;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, PreparedQuery, Propagation, ReadOpts,
    SeededRowIdSource, SubscriptionEvent, SubscriptionStream,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, all_of, claim, col, eq, param, provider_claim_key};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("team", ColumnType::Uuid)
                    .column("updated_at", ColumnType::Timestamp)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("owned_documents")
                    .column("team", ColumnType::Uuid)
                    .column("owner", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_db() -> Db<TestStorage> {
    open_db_as(AuthorSubject::SYSTEM)
}

fn open_db_as(author: AuthorSubject) -> Db<TestStorage> {
    let schema = schema();
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&column_family_refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x7100)),
    ))
    .expect("open parameterized routing db")
}

fn row(seed: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000_u64.to_be_bytes());
    bytes[8..].copy_from_slice(&seed.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn insert_document(db: &Db<TestStorage>, document: RowUuid, team: RowUuid, updated_at: u64) {
    block_on(db.insert(
        "documents",
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("updated_at".to_owned(), Value::U64(updated_at)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(document),
            ..Default::default()
        },
    ))
    .expect("insert document");
}

fn local_read_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn take_initial_reset(label: &str, stream: &mut SubscriptionStream) -> BTreeSet<RowUuid> {
    let event = stream
        .try_next_event()
        .unwrap_or_else(|| panic!("{label} subscription did not emit an initial reset"));
    match event {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(
                removed.is_empty(),
                "{label} initial reset unexpectedly removed rows"
            );
            added
                .into_iter()
                .chain(updated)
                .map(|row| row.row_uuid())
                .collect()
        }
        other => panic!("{label} expected an initial reset, got {other:?}"),
    }
}

/// The installed maintained graph owns its execution. Neither the public
/// prepared handle nor a sibling binding is its lifetime owner.
#[test]
fn fresh_subscription_owns_its_graph_after_prepared_handles_are_dropped() {
    let db = open_db();
    let team_a = row(100);
    let team_b = row(200);
    insert_document(&db, row(1), team_a, 1);
    insert_document(&db, row(2), team_b, 2);
    let query = Query::from("documents").filter(eq(col("team"), param("team")));
    let open = |team: RowUuid| {
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".into(), Value::Uuid(team.0))]),
            )
            .expect("prepare bound subscription");
        // `prepared` is dropped before the caller receives the stream.
        block_on(db.subscribe(&prepared, local_read_opts())).expect("open subscription")
    };
    let mut a = open(team_a);
    let mut b = open(team_b);
    let mut a_rows = take_initial_reset("A", &mut a);
    let mut b_rows = take_initial_reset("B", &mut b);
    assert_eq!(a_rows, BTreeSet::from([row(1)]));
    assert_eq!(b_rows, BTreeSet::from([row(2)]));
    insert_document(&db, row(3), team_a, 3);
    apply_pending_events("A", &mut a, &mut a_rows);
    apply_pending_events("B", &mut b, &mut b_rows);
    assert_eq!(a_rows, BTreeSet::from([row(1), row(3)]));
    assert_eq!(b_rows, BTreeSet::from([row(2)]));
    drop(a);
    block_on(db.tick()).expect("retire A without retiring B");
    insert_document(&db, row(4), team_b, 4);
    apply_pending_events("B after A dropped", &mut b, &mut b_rows);
    assert_eq!(b_rows, BTreeSet::from([row(2), row(4)]));
    drop(b);
    block_on(db.close()).expect("close subscription fixture");
}

#[derive(Debug, Default, PartialEq, Eq)]
struct AppliedEvents {
    count: usize,
    resets: usize,
    added: BTreeSet<RowUuid>,
    updated: BTreeSet<RowUuid>,
    removed: BTreeSet<RowUuid>,
}

fn apply_pending_events(
    label: &str,
    stream: &mut SubscriptionStream,
    rows: &mut BTreeSet<RowUuid>,
) -> AppliedEvents {
    let mut applied = AppliedEvents::default();
    while let Some(event) = stream.try_next_event() {
        applied.count += 1;
        match event {
            SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } => {
                if reset {
                    applied.resets += 1;
                    rows.clear();
                }
                for removed in removed {
                    applied.removed.insert(removed.row_uuid);
                    rows.remove(&removed.row_uuid);
                }
                for row in added {
                    applied.added.insert(row.row_uuid());
                    rows.insert(row.row_uuid());
                }
                for row in updated {
                    applied.updated.insert(row.row_uuid());
                    rows.insert(row.row_uuid());
                }
            }
            SubscriptionEvent::Rejected { reason } => {
                panic!("{label} subscription was rejected: {reason:?}")
            }
            SubscriptionEvent::Closed => panic!("{label} subscription closed unexpectedly"),
        }
    }
    applied
}

fn assert_ordered_rows(
    db: &Db<TestStorage>,
    prepared: &PreparedQuery,
    expected: &[RowUuid],
    label: &str,
) {
    let actual = block_on(db.all(prepared, local_read_opts()))
        .unwrap_or_else(|error| panic!("{label} one-shot read failed: {error}"))
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{label} returned the wrong row order");
}

/// One maintained shape serves two bound team subscriptions:
///
/// ```text
/// documents(team = A) ──> binding A ──> Top 2 for A
/// documents(team = B) ──> binding B ──> Top 2 for B
/// ```
///
/// Binding and mutation deltas must never make either window global.
#[test]
fn parameterized_top_by_is_partitioned_per_active_binding() {
    let db = open_db();
    let team_a = row(1);
    let team_b = row(2);

    for (document, team, updated_at) in [
        (row(101), team_a, 10),
        (row(102), team_a, 11),
        (row(103), team_a, 12),
        (row(201), team_b, 20),
        (row(202), team_b, 21),
        (row(203), team_b, 22),
    ] {
        insert_document(&db, document, team, updated_at);
    }

    let query = Query::from("documents")
        .filter(eq(col("team"), param("team")))
        .order_by("updated_at", OrderDirection::Desc)
        .limit(2);
    let prepared_a = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
        )
        .expect("prepare team A binding");
    let prepared_b = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team_b.0))]),
        )
        .expect("prepare team B binding");

    assert_eq!(
        prepared_a.shape().shape_id(),
        prepared_b.shape().shape_id(),
        "bindings must share the same maintained query shape"
    );
    assert_ne!(
        prepared_a.binding().binding_id(),
        prepared_b.binding().binding_id(),
        "bindings must remain independently routable"
    );

    let mut stream_a =
        block_on(db.subscribe(&prepared_a, local_read_opts())).expect("subscribe team A binding");
    let mut stream_b =
        block_on(db.subscribe(&prepared_b, local_read_opts())).expect("subscribe team B binding");
    let mut rows_a = take_initial_reset("team A", &mut stream_a);
    let mut rows_b = take_initial_reset("team B", &mut stream_b);
    apply_pending_events("team A after both binds", &mut stream_a, &mut rows_a);
    apply_pending_events("team B after both binds", &mut stream_b, &mut rows_b);

    assert_eq!(rows_a, BTreeSet::from([row(102), row(103)]));
    assert_eq!(rows_b, BTreeSet::from([row(202), row(203)]));
    assert_ordered_rows(&db, &prepared_a, &[row(103), row(102)], "team A initial");
    assert_ordered_rows(&db, &prepared_b, &[row(203), row(202)], "team B initial");

    insert_document(&db, row(104), team_a, 30);
    let team_a_delta = apply_pending_events("team A mutation", &mut stream_a, &mut rows_a);
    assert_eq!(
        team_a_delta,
        AppliedEvents {
            count: 1,
            resets: 0,
            added: BTreeSet::from([row(104)]),
            updated: BTreeSet::new(),
            removed: BTreeSet::from([row(102)]),
        },
        "team A insert must incrementally rotate team A's TopBy window"
    );
    let team_b_after_a =
        apply_pending_events("team B after team A mutation", &mut stream_b, &mut rows_b);
    assert_eq!(rows_a, BTreeSet::from([row(103), row(104)]));
    assert_eq!(rows_b, BTreeSet::from([row(202), row(203)]));
    assert_eq!(
        team_b_after_a.count, 0,
        "team A insert must not notify the team B subscription"
    );
    assert_ordered_rows(
        &db,
        &prepared_a,
        &[row(104), row(103)],
        "team A after team A insert",
    );
    assert_ordered_rows(
        &db,
        &prepared_b,
        &[row(203), row(202)],
        "team B after team A insert",
    );

    insert_document(&db, row(204), team_b, 31);
    let team_b_delta = apply_pending_events("team B mutation", &mut stream_b, &mut rows_b);
    assert_eq!(
        team_b_delta,
        AppliedEvents {
            count: 1,
            resets: 0,
            added: BTreeSet::from([row(204)]),
            updated: BTreeSet::new(),
            removed: BTreeSet::from([row(202)]),
        },
        "team B insert must incrementally rotate team B's TopBy window"
    );
    let team_a_after_b =
        apply_pending_events("team A after team B mutation", &mut stream_a, &mut rows_a);
    assert_eq!(rows_a, BTreeSet::from([row(103), row(104)]));
    assert_eq!(rows_b, BTreeSet::from([row(203), row(204)]));
    assert_eq!(
        team_a_after_b.count, 0,
        "team B insert must not notify the team A subscription"
    );
    assert_ordered_rows(
        &db,
        &prepared_a,
        &[row(104), row(103)],
        "team A after team B insert",
    );
    assert_ordered_rows(
        &db,
        &prepared_b,
        &[row(204), row(203)],
        "team B after team B insert",
    );
}

/// Local-tier bindings of one shape stay exact under churn:
///
/// ```text
/// documents ──> team = $team ──> binding per team ──> subscriber per team
///               (unbounded, and Top 3 by updated_at)
/// ```
///
/// Rows are inserted, moved between teams, reordered and deleted, and
/// bindings are dropped and re-opened, while every maintained subscription
/// is compared against a one-shot read of the same binding after each step.
#[test]
fn local_bindings_of_one_shape_match_one_shot_reads_under_churn() {
    churn_differential(open_db());
}

/// The same differential for an ordinary (non-System) author, whose reads
/// carry a session rather than bypassing policy entirely.
#[test]
fn local_author_bindings_of_one_shape_match_one_shot_reads_under_churn() {
    churn_differential(open_db_as(AuthorSubject::for_test_uuid(uuid::uuid!(
        "71000000-0000-0000-0000-0000000000b2"
    ))));
}

fn churn_differential(db: Db<TestStorage>) {
    const TEAMS: u64 = 5;
    const STEPS: u64 = 160;

    let team = |index: u64| row(1_000 + index % TEAMS);
    let unbounded = Query::from("documents").filter(eq(col("team"), param("team")));
    let top = Query::from("documents")
        .filter(eq(col("team"), param("team")))
        .order_by("updated_at", OrderDirection::Desc)
        .limit(3);
    let prepare = |query: &Query, team: RowUuid| {
        db.prepare_query_bound(
            query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
        )
        .expect("prepare team binding")
    };

    struct Live {
        label: String,
        prepared: PreparedQuery,
        stream: SubscriptionStream,
        rows: BTreeSet<RowUuid>,
    }
    let open = |label: String, prepared: PreparedQuery| {
        let mut stream =
            block_on(db.subscribe(&prepared, local_read_opts())).expect("subscribe team binding");
        let mut rows = take_initial_reset(&label, &mut stream);
        apply_pending_events(&label, &mut stream, &mut rows);
        Live {
            label,
            prepared,
            stream,
            rows,
        }
    };
    let expected = |live: &Live| {
        block_on(db.all(&live.prepared, local_read_opts()))
            .unwrap_or_else(|error| panic!("{} one-shot read failed: {error}", live.label))
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>()
    };

    for seed in 0..12 {
        insert_document(&db, row(seed), team(seed), seed * 7 % 23);
    }
    let mut live = (0..TEAMS)
        .flat_map(|index| {
            [
                (format!("all/{index}"), prepare(&unbounded, team(index))),
                (format!("top/{index}"), prepare(&top, team(index))),
            ]
        })
        .map(|(label, prepared)| open(label, prepared))
        .collect::<Vec<_>>();
    for subscription in &live {
        assert_eq!(
            subscription.rows,
            expected(subscription),
            "{} initial",
            subscription.label
        );
    }

    let mut documents = (0..12).collect::<Vec<u64>>();
    let mut next_document = 12;
    for step in 0..STEPS {
        let pick = documents[(step as usize * 5) % documents.len()];
        match step % 6 {
            0 | 3 => {
                insert_document(&db, row(next_document), team(step * 3), step % 29);
                documents.push(next_document);
                next_document += 1;
            }
            1 => {
                block_on(db.update(
                    "documents",
                    row(pick),
                    BTreeMap::from([("team".to_owned(), Value::Uuid(team(step + pick).0))]),
                    Default::default(),
                ))
                .expect("move document to another team");
            }
            2 | 4 => {
                block_on(db.update(
                    "documents",
                    row(pick),
                    BTreeMap::from([("updated_at".to_owned(), Value::U64(step * 11 % 31))]),
                    Default::default(),
                ))
                .expect("reorder document");
            }
            _ => {
                block_on(db.delete("documents", row(pick), Default::default()))
                    .expect("delete document");
                documents.retain(|document| *document != pick);
            }
        }
        if step % 40 == 39 {
            // Drop one binding and re-open it while its siblings stay live.
            let index = (step / 40) as usize % live.len();
            let Live {
                label,
                prepared,
                stream,
                ..
            } = live.swap_remove(index);
            drop(stream);
            block_on(db.tick()).expect("retire dropped binding");
            live.push(open(format!("{label}/reopened"), prepared));
        }
        for subscription in &mut live {
            apply_pending_events(
                &subscription.label,
                &mut subscription.stream,
                &mut subscription.rows,
            );
            assert_eq!(
                subscription.rows,
                expected(subscription),
                "{} diverged from its one-shot read after step {step}",
                subscription.label
            );
        }
    }
    drop(live);
    block_on(db.close()).expect("close churn fixture");
}

fn team_binding(db: &Db<TestStorage>, query: &Query, team: RowUuid) -> PreparedQuery {
    db.prepare_query_bound(
        query,
        BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
    )
    .expect("prepare team binding")
}

/// Internal work-bound check: exact results are covered by the churn
/// differential below, but only runtime stats can show whether bindings share
/// one prepared binding source or each inline their own graph. Local-tier
/// bindings of one shape share the source's arrangements, so adding bindings
/// adds no arrangement; an inlined binding would add one per binding.
#[test]
fn local_bindings_of_one_shape_share_one_prepared_source() {
    let db = open_db();
    for seed in 0..8 {
        insert_document(&db, row(seed), row(1_000 + seed % 4), seed);
    }
    let query = Query::from("documents")
        .filter(eq(col("team"), param("team")))
        .order_by("updated_at", OrderDirection::Desc)
        .limit(2);
    let subscribe = |team: u64| {
        let prepared = team_binding(&db, &query, row(1_000 + team));
        let mut stream =
            block_on(db.subscribe(&prepared, local_read_opts())).expect("subscribe team binding");
        let rows = take_initial_reset("team", &mut stream);
        assert_eq!(rows.len(), 2, "team {team} initial window");
        stream
    };

    // The first subscriber keeps its literal graph; the second prepares the
    // shared shape. From then on, bindings only attach to that shape.
    let first = subscribe(0);
    let second = subscribe(1);
    let two = db.runtime_stats_for_test();
    let rest = (2..4).map(subscribe).collect::<Vec<_>>();
    let four = db.runtime_stats_for_test();
    assert_eq!(four.active_subscriptions, two.active_subscriptions + 2);
    assert_eq!(four.active_shape_params, two.active_shape_params + 2);
    assert_eq!(
        four.arrangement_count, two.arrangement_count,
        "later bindings of one Local-tier shape must reuse its prepared source"
    );
    drop((first, second, rest));
    block_on(db.close()).expect("close sharing fixture");
}

/// A claim the query itself reads is bound from the reader's session on the
/// shared Local-tier path, like an ordinary parameter:
///
/// ```text
/// owned_documents ──> team = $team AND owner = claims.owner ──> per team
/// ```
///
/// Each team binding sees only the reader's own documents, both initially and
/// as matching and non-matching rows arrive.
#[test]
fn local_bindings_bind_query_claims_from_the_readers_session() {
    let reader = AuthorSubject::for_test_uuid(uuid::uuid!("71000000-0000-0000-0000-0000000000a1"));
    let db = open_db_as(reader);
    db.set_identity_claims(
        reader,
        BTreeMap::from([(
            provider_claim_key("owner"),
            Value::String("alice".to_owned()),
        )]),
    );
    let insert = |document: u64, team: u64, owner: &str| {
        block_on(db.insert(
            "owned_documents",
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(row(1_000 + team).0)),
                ("owner".to_owned(), Value::String(owner.to_owned())),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(row(document)),
                ..Default::default()
            },
        ))
        .expect("insert owned document");
    };
    insert(1, 0, "alice");
    insert(2, 0, "bob");
    insert(3, 1, "alice");
    insert(4, 1, "bob");

    let query = Query::from("owned_documents").filter(all_of([
        eq(col("team"), param("team")),
        // `session.claims["owner"]`, as the public schema DSL lowers it.
        eq(col("owner"), claim(provider_claim_key("owner"))),
    ]));
    let mut live = (0..2)
        .map(|team| {
            let label = format!("team {team}");
            let prepared = team_binding(&db, &query, row(1_000 + team));
            let mut stream = block_on(db.subscribe(&prepared, local_read_opts()))
                .expect("subscribe claim-filtered binding");
            let rows = take_initial_reset(&label, &mut stream);
            (label, prepared, stream, rows)
        })
        .collect::<Vec<_>>();
    let check = |live: &mut Vec<(String, PreparedQuery, SubscriptionStream, BTreeSet<RowUuid>)>,
                 expected: [&[u64]; 2]| {
        for ((label, prepared, stream, rows), expected) in live.iter_mut().zip(expected) {
            apply_pending_events(label, stream, rows);
            let expected = expected.iter().copied().map(row).collect::<BTreeSet<_>>();
            let one_shot = block_on(db.all(prepared, local_read_opts()))
                .expect("one-shot claim-filtered read")
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>();
            assert_eq!(one_shot, expected, "{label} one-shot read");
            assert_eq!(*rows, expected, "{label} maintained subscription");
        }
    };
    check(&mut live, [&[1], &[3]]);

    insert(5, 0, "alice");
    insert(6, 1, "bob");
    check(&mut live, [&[1, 5], &[3]]);

    drop(live);
    block_on(db.close()).expect("close claim fixture");
}

/// Six live bindings of one claim-reading shape, for one author: two teams
/// under request-scoped claims for alice, two for bob, and two under the
/// session's own claims (carol). Bindings that differ only by claim scope must
/// never see each other's rows, before or after writes.
///
/// ```text
/// owned_documents ──> team = $team AND owner = claims.owner
///                        ├── alice (request) t0, t1
///                        ├── bob   (request) t0, t1
///                        └── carol (session) t0, t1
/// ```
#[test]
fn local_bindings_keep_request_and_session_claim_scopes_apart() {
    let reader = AuthorSubject::for_test_uuid(uuid::uuid!("71000000-0000-0000-0000-0000000000a1"));
    let db = open_db_as(reader);
    let owner_claims = |owner: &str| {
        BTreeMap::from([(provider_claim_key("owner"), Value::String(owner.to_owned()))])
    };
    db.set_identity_claims(reader, owner_claims("carol"));
    let insert = |document: u64, team: u64, owner: &str| {
        block_on(db.insert(
            "owned_documents",
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(row(1_000 + team).0)),
                ("owner".to_owned(), Value::String(owner.to_owned())),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(row(document)),
                ..Default::default()
            },
        ))
        .expect("insert owned document");
    };
    for (document, team, owner) in [
        (1, 0, "alice"),
        (2, 0, "bob"),
        (3, 1, "alice"),
        (4, 1, "bob"),
        (5, 0, "carol"),
        (6, 1, "carol"),
    ] {
        insert(document, team, owner);
    }

    let query = Query::from("owned_documents").filter(all_of([
        eq(col("team"), param("team")),
        eq(col("owner"), claim(provider_claim_key("owner"))),
    ]));
    let bindings = [
        (0, Some("alice")),
        (1, Some("alice")),
        (0, Some("bob")),
        (1, Some("bob")),
        (0, None),
        (1, None),
    ];
    let mut live = bindings
        .into_iter()
        .map(|(team, owner)| {
            let label = format!("team {team} claims {}", owner.unwrap_or("session"));
            let prepared = team_binding(&db, &query, row(1_000 + team));
            let prepared = match owner {
                Some(owner) => prepared.with_identity_claims(reader, owner_claims(owner)),
                None => prepared,
            };
            let mut stream = block_on(db.subscribe(&prepared, local_read_opts()))
                .expect("subscribe claim-scoped binding");
            let rows = take_initial_reset(&label, &mut stream);
            (label, prepared, stream, rows)
        })
        .collect::<Vec<_>>();
    let check = |live: &mut Vec<(String, PreparedQuery, SubscriptionStream, BTreeSet<RowUuid>)>,
                 expected: [&[u64]; 6]| {
        for ((label, prepared, stream, rows), expected) in live.iter_mut().zip(expected) {
            apply_pending_events(label, stream, rows);
            let expected = expected.iter().copied().map(row).collect::<BTreeSet<_>>();
            let one_shot = block_on(db.all(prepared, local_read_opts()))
                .expect("one-shot claim-scoped read")
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>();
            assert_eq!(one_shot, expected, "{label} one-shot read");
            assert_eq!(*rows, expected, "{label} maintained subscription");
        }
    };
    check(&mut live, [&[1], &[3], &[2], &[4], &[5], &[6]]);

    insert(10, 0, "alice");
    insert(11, 0, "bob");
    insert(12, 1, "carol");
    insert(13, 1, "bob");
    check(
        &mut live,
        [&[1, 10], &[3], &[2, 11], &[4, 13], &[5], &[6, 12]],
    );

    drop(live);
    block_on(db.close()).expect("close claim-scope fixture");
}

/// Two live subscriptions of the same binding on the shared shape: each sees
/// writes, and closing one leaves the other delivering.
///
/// ```text
/// documents ──> team = $team
///                 ├── team 0 (lone, literal)
///                 ├── team 1 ─┐ one binding, two subscribers
///                 └── team 1 ─┘ on the shared shape
/// ```
#[test]
fn local_duplicate_bindings_on_a_shared_shape_each_deliver() {
    let db = open_db();
    insert_document(&db, row(1), row(1_000), 1);
    insert_document(&db, row(2), row(1_001), 2);
    let query = Query::from("documents").filter(eq(col("team"), param("team")));
    let open = |team: u64, label: &str| {
        let prepared = team_binding(&db, &query, row(1_000 + team));
        let mut stream =
            block_on(db.subscribe(&prepared, local_read_opts())).expect("subscribe team binding");
        let rows = take_initial_reset(label, &mut stream);
        (label.to_owned(), prepared, stream, rows)
    };
    let check = |live: &mut [(String, PreparedQuery, SubscriptionStream, BTreeSet<RowUuid>)],
                 expected: &[&[u64]]| {
        for ((label, prepared, stream, rows), expected) in live.iter_mut().zip(expected) {
            apply_pending_events(label, stream, rows);
            let expected = expected.iter().copied().map(row).collect::<BTreeSet<_>>();
            let one_shot = block_on(db.all(prepared, local_read_opts()))
                .expect("one-shot team read")
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>();
            assert_eq!(one_shot, expected, "{label} one-shot read");
            assert_eq!(*rows, expected, "{label} maintained subscription");
        }
    };

    let mut live = vec![
        open(0, "team 0"),
        open(1, "team 1 first"),
        open(1, "team 1 second"),
    ];
    check(&mut live, &[&[1], &[2], &[2]]);

    insert_document(&db, row(3), row(1_001), 3);
    check(&mut live, &[&[1], &[2, 3], &[2, 3]]);

    let dropped = live.remove(1);
    drop(dropped);
    block_on(db.tick()).expect("tick after closing one duplicate");
    insert_document(&db, row(4), row(1_001), 4);
    check(&mut live, &[&[1], &[2, 3, 4]]);

    drop(live);
    block_on(db.close()).expect("close duplicate-binding fixture");
}

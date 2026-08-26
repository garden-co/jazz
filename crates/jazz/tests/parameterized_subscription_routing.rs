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
use jazz::query::{OrderDirection, Query, col, eq, param};
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
            .build(),
    )
}

fn open_db() -> Db<TestStorage> {
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
                author: AuthorSubject::SYSTEM,
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

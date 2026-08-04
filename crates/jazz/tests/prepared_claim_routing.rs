use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, claim, col, eq, param};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

const DOCUMENTS: &str = "documents";
const MEMBERSHIPS: &str = "memberships";
const TEAMS: &str = "teams";
const WRITER: AuthorId = AuthorId(uuid::uuid!("82000000-0000-0000-0000-000000000001"));
const USER_A: AuthorId = AuthorId(uuid::uuid!("82000000-0000-0000-0000-000000000002"));
const USER_B: AuthorId = AuthorId(uuid::uuid!("82000000-0000-0000-0000-000000000003"));

type BenchDb = Db<MemoryStorage>;

fn row(tag: u8) -> RowUuid {
    RowUuid::from_bytes([tag; 16])
}

fn schema() -> JazzSchema {
    schema_with_membership_policy(Policy::public())
}

fn schema_with_membership_policy(membership_policy: Option<Query>) -> JazzSchema {
    let policy = Query::from(DOCUMENTS).join_via_column(
        MEMBERSHIPS,
        "team",
        "team",
        [
            eq(col("user"), claim("sub")),
            eq(col("region"), claim("region")),
        ],
    );
    JazzSchema::new([
        TableSchema::new(TEAMS, [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            MEMBERSHIPS,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("region", ColumnType::String),
            ],
        )
        .with_reference("team", TEAMS)
        .with_read_policy(membership_policy)
        .with_write_policy(Policy::public()),
        TableSchema::new(
            DOCUMENTS,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("updated_at", ColumnType::U64),
            ],
        )
        .with_reference("team", TEAMS)
        .with_read_policy(Policy::shape(policy))
        .with_write_policy(Policy::public()),
    ])
}

fn open_db() -> BenchDb {
    open_db_with_schema(schema())
}

fn open_db_with_schema(schema: JazzSchema) -> BenchDb {
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&family_refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x82; 16]),
                author: WRITER,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x8200)),
    ))
    .expect("open prepared claim routing db")
}

fn opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn seed(db: &BenchDb, team_a: RowUuid, team_b: RowUuid, region_a: &str, region_b: &str) {
    for (team, name) in [(team_a, "Team A"), (team_b, "Team B")] {
        db.insert_with_id(
            TEAMS,
            team,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        )
        .expect("seed team");
    }
    for (membership, team, user, region) in [
        (row(0x31), team_a, USER_A, region_a),
        (row(0x32), team_b, USER_B, region_b),
    ] {
        db.insert_with_id(
            MEMBERSHIPS,
            membership,
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team.0)),
                ("user".to_owned(), Value::Uuid(user.0)),
                ("region".to_owned(), Value::String(region.to_owned())),
            ]),
        )
        .expect("seed membership");
    }
    for (document, team, updated_at) in [(row(0x41), team_a, 10), (row(0x42), team_b, 20)] {
        db.insert_with_id(
            DOCUMENTS,
            document,
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team.0)),
                ("updated_at".to_owned(), Value::U64(updated_at)),
            ]),
        )
        .expect("seed document");
    }
}

fn assert_call_order(
    first: (AuthorId, RowUuid, RowUuid),
    second: (AuthorId, RowUuid, RowUuid),
    region_a: &str,
    region_b: &str,
) {
    let db = open_db();
    let team_a = row(0x11);
    let team_b = row(0x12);
    seed(&db, team_a, team_b, region_a, region_b);
    db.set_identity_claims(
        USER_A,
        BTreeMap::from([("region".to_owned(), Value::String(region_a.to_owned()))]),
    );
    db.set_identity_claims(
        USER_B,
        BTreeMap::from([("region".to_owned(), Value::String(region_b.to_owned()))]),
    );
    let query = Query::from(DOCUMENTS)
        .filter(eq(col("team"), param("team")))
        .order_by("updated_at", OrderDirection::Desc)
        .limit(2);

    let mut shape_id = None;
    for (identity, team, expected) in [first, second] {
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
            )
            .expect("prepare identity/team binding");
        if let Some(shape_id) = shape_id {
            assert_eq!(prepared.shape().shape_id(), shape_id);
        } else {
            shape_id = Some(prepared.shape().shape_id());
        }
        let rows = block_on(db.all_for_identity(&prepared, opts(), identity))
            .expect("read identity/team binding")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![expected],
            "prepared result depended on call order"
        );
    }
}

#[test]
fn prepared_policy_claims_route_per_identity_and_application_binding() {
    let team_a = row(0x11);
    let team_b = row(0x12);
    let document_a = row(0x41);
    let document_b = row(0x42);

    assert_call_order(
        (USER_A, team_a, document_a),
        (USER_B, team_b, document_b),
        "region-a",
        "region-b",
    );
    assert_call_order(
        (USER_B, team_b, document_b),
        (USER_A, team_a, document_a),
        "region-a",
        "region-b",
    );
}

fn assert_retained_subscription_regions(region_a: &str, region_b: &str) {
    let db = open_db();
    let team_a = row(0x11);
    let team_b = row(0x12);
    seed(&db, team_a, team_b, region_a, region_b);
    db.set_identity_claims(
        USER_A,
        BTreeMap::from([("region".to_owned(), Value::String(region_a.to_owned()))]),
    );
    db.set_identity_claims(
        USER_B,
        BTreeMap::from([("region".to_owned(), Value::String(region_b.to_owned()))]),
    );
    let query = Query::from(DOCUMENTS)
        .filter(eq(col("team"), param("team")))
        .order_by("updated_at", OrderDirection::Desc)
        .limit(2);
    let prepared = |team: RowUuid| {
        db.prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
        )
        .expect("prepare team binding")
    };
    let mut stream_a = block_on(db.subscribe_for_identity(&prepared(team_a), opts(), USER_A))
        .expect("subscribe team A");
    let mut stream_b = block_on(db.subscribe_for_identity(&prepared(team_b), opts(), USER_B))
        .expect("subscribe team B");

    let initial = |event| match event {
        SubscriptionEvent::Delta {
            reset: true, added, ..
        } => added
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        other => panic!("expected initial reset, got {other:?}"),
    };
    assert_eq!(
        initial(stream_a.try_next_event().expect("team A reset")),
        vec![row(0x41)]
    );
    assert_eq!(
        initial(stream_b.try_next_event().expect("team B reset")),
        vec![row(0x42)]
    );
    assert!(
        stream_a.try_next_event().is_none(),
        "adding team B's binding must not change team A"
    );
    assert!(stream_b.try_next_event().is_none());
}

#[test]
fn prepared_policy_claims_route_retained_subscriptions() {
    assert_retained_subscription_regions("region-a", "region-b");
}

#[test]
fn prepared_policy_claims_support_equal_custom_string_values() {
    assert_retained_subscription_regions("shared-region", "shared-region");
}

#[test]
fn prepared_binding_includes_claims_from_auxiliary_source_policies() {
    let membership_policy = Policy::shape(
        Query::from(MEMBERSHIPS).filter(eq(col("region"), claim("membership_region"))),
    );
    let db = open_db_with_schema(schema_with_membership_policy(membership_policy));
    let team_a = row(0x11);
    let team_b = row(0x12);
    seed(&db, team_a, team_b, "region-a", "region-b");
    db.set_identity_claims(
        USER_A,
        BTreeMap::from([
            ("region".to_owned(), Value::String("region-a".to_owned())),
            (
                "membership_region".to_owned(),
                Value::String("region-a".to_owned()),
            ),
        ]),
    );
    db.set_identity_claims(
        USER_B,
        BTreeMap::from([
            ("region".to_owned(), Value::String("region-b".to_owned())),
            (
                "membership_region".to_owned(),
                Value::String("region-b".to_owned()),
            ),
        ]),
    );
    let query = Query::from(DOCUMENTS).filter(eq(col("team"), param("team")));

    for (identity, team, expected) in [(USER_A, team_a, row(0x41)), (USER_B, team_b, row(0x42))] {
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
            )
            .expect("prepare auxiliary-policy binding");
        let rows = block_on(db.all_for_identity(&prepared, opts(), identity))
            .expect("read through auxiliary policy")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![expected]);
    }
}

#[test]
fn prepared_binding_rejects_conflicting_claim_types_across_policies() {
    let root_policy = Query::from(DOCUMENTS)
        .filter(eq(col("team"), claim("shared_scope")))
        .join_via_column(MEMBERSHIPS, "team", "team", [eq(col("user"), claim("sub"))]);
    let membership_policy =
        Query::from(MEMBERSHIPS).filter(eq(col("region"), claim("shared_scope")));
    let schema = JazzSchema::new([
        TableSchema::new(TEAMS, [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            MEMBERSHIPS,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("region", ColumnType::String),
            ],
        )
        .with_reference("team", TEAMS)
        .with_read_policy(membership_policy),
        TableSchema::new(DOCUMENTS, [ColumnSchema::new("team", ColumnType::Uuid)])
            .with_reference("team", TEAMS)
            .with_read_policy(root_policy),
    ]);
    let db = open_db_with_schema(schema);
    db.set_identity_claims(
        USER_A,
        BTreeMap::from([("shared_scope".to_owned(), Value::Uuid(row(0x11).0))]),
    );
    let prepared = db
        .prepare_query(&Query::from(DOCUMENTS))
        .expect("prepare conflicting-policy shape");

    let error = block_on(db.all_for_identity(&prepared, opts(), USER_A))
        .expect_err("conflicting policy claim types must fail explicitly");
    assert!(
        error.to_string().contains("conflicting policy types"),
        "unexpected error: {error}"
    );
}

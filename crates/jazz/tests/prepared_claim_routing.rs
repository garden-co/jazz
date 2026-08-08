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
use jazz::protocol::{CurrentWriteSchema, SchemaVersion};
use jazz::query::{OrderDirection, Query, claim, col, eq, lit, param};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

const DOCUMENTS: &str = "documents";
const MEMBERSHIPS: &str = "memberships";
const TEAMS: &str = "teams";
const PROJECTS: &str = "projects";
const PROJECT_ACCESS: &str = "project_access";
const GROUPS: &str = "groups";
const GROUP_MEMBERS: &str = "group_members";
const GROUP_EDGES: &str = "group_edges";
const CYCLE_A: &str = "cycle_a";
const CYCLE_B: &str = "cycle_b";
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

fn two_hop_seeded_policy_schema() -> JazzSchema {
    let document_policy = Query::from(DOCUMENTS).join_via_column(MEMBERSHIPS, "document", "id", []);
    let membership_policy = Query::from(MEMBERSHIPS).join_via_row_id(PROJECTS, "project", []);
    let project_policy = Query::from(PROJECTS)
        .reachable_via(
            PROJECT_ACCESS,
            "project",
            "group",
            lit("seeded"),
            GROUP_EDGES,
            "member",
            "parent",
            [],
        )
        .seeded_by(GROUP_MEMBERS, "user", "sub", "group");

    JazzSchema::new([
        TableSchema::new(
            DOCUMENTS,
            [
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("updated_at", ColumnType::U64),
            ],
        )
        .with_reference("project", PROJECTS)
        .with_read_policy(Policy::shape(document_policy))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            MEMBERSHIPS,
            [
                ColumnSchema::new("document", ColumnType::Uuid),
                ColumnSchema::new("project", ColumnType::Uuid),
            ],
        )
        .with_reference("document", DOCUMENTS)
        .with_reference("project", PROJECTS)
        .with_read_policy(Policy::shape(membership_policy))
        .with_write_policy(Policy::public()),
        TableSchema::new(PROJECTS, [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::shape(project_policy))
            .with_write_policy(Policy::public()),
        TableSchema::new(GROUPS, [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            PROJECT_ACCESS,
            [
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("group", ColumnType::Uuid),
            ],
        )
        .with_reference("project", PROJECTS)
        .with_reference("group", GROUPS)
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            GROUP_MEMBERS,
            [
                ColumnSchema::new("group", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
            ],
        )
        .with_reference("group", GROUPS)
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            GROUP_EDGES,
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
            ],
        )
        .with_reference("member", GROUPS)
        .with_reference("parent", GROUPS)
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn policy_proof_cycle_schema() -> JazzSchema {
    let a_policy = Query::from(CYCLE_A).join_via_column(CYCLE_B, "id", "b", []);
    let b_policy = Query::from(CYCLE_B).join_via_column(CYCLE_A, "id", "a", []);
    JazzSchema::new([
        TableSchema::new(CYCLE_A, [ColumnSchema::new("b", ColumnType::Uuid)])
            .with_reference("b", CYCLE_B)
            .with_read_policy(Policy::shape(a_policy))
            .with_write_policy(Policy::public()),
        TableSchema::new(CYCLE_B, [ColumnSchema::new("a", ColumnType::Uuid)])
            .with_reference("a", CYCLE_A)
            .with_read_policy(Policy::shape(b_policy))
            .with_write_policy(Policy::public()),
    ])
}

fn evolved_schema() -> JazzSchema {
    let mut schema = schema();
    schema
        .tables
        .iter_mut()
        .find(|table| table.name == DOCUMENTS)
        .expect("documents table")
        .columns
        .push(
            jazz::schema::ColumnSchema::new("generation", ColumnType::U64)
                .with_default(Value::U64(0)),
        );
    schema
}

fn open_db() -> BenchDb {
    open_db_with_schema(schema())
}

fn open_db_with_schema(schema: JazzSchema) -> BenchDb {
    open_db_with_schema_as(schema, WRITER)
}

fn open_db_with_schema_as(schema: JazzSchema, author: AuthorId) -> BenchDb {
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&family_refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x82; 16]),
                author,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x8200)),
    ))
    .expect("open prepared claim routing db")
}

fn row_ids_for_identity(
    db: &BenchDb,
    prepared: &jazz::db::PreparedQuery,
    identity: AuthorId,
) -> Vec<RowUuid> {
    block_on(db.all_for_identity(prepared, opts(), identity))
        .expect("read prepared query for identity")
        .into_iter()
        .map(|row| row.row_uuid())
        .collect()
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

fn seed_two_hop_reachability_policy(db: &BenchDb) -> (RowUuid, RowUuid, RowUuid, RowUuid) {
    let project_a = row(0x51);
    let project_b = row(0x52);
    let document_a = row(0x61);
    let document_b = row(0x62);
    let group_a = row(0x71);
    let group_b = row(0x72);

    for (project, name) in [(project_a, "project-a"), (project_b, "project-b")] {
        db.insert_with_id(
            PROJECTS,
            project,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        )
        .expect("seed project");
    }
    for (group, name) in [(group_a, "group-a"), (group_b, "group-b")] {
        db.insert_with_id(
            GROUPS,
            group,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        )
        .expect("seed group");
    }
    for (document, project, updated_at) in
        [(document_a, project_a, 10), (document_b, project_b, 20)]
    {
        db.insert_with_id(
            DOCUMENTS,
            document,
            BTreeMap::from([
                ("project".to_owned(), Value::Uuid(project.0)),
                ("updated_at".to_owned(), Value::U64(updated_at)),
            ]),
        )
        .expect("seed document");
    }
    for (membership, document, project) in [
        (row(0x81), document_a, project_a),
        (row(0x82), document_b, project_b),
    ] {
        db.insert_with_id(
            MEMBERSHIPS,
            membership,
            BTreeMap::from([
                ("document".to_owned(), Value::Uuid(document.0)),
                ("project".to_owned(), Value::Uuid(project.0)),
            ]),
        )
        .expect("seed membership");
    }
    for (access, project, group) in [
        (row(0x91), project_a, group_a),
        (row(0x92), project_b, group_b),
    ] {
        db.insert_with_id(
            PROJECT_ACCESS,
            access,
            BTreeMap::from([
                ("project".to_owned(), Value::Uuid(project.0)),
                ("group".to_owned(), Value::Uuid(group.0)),
            ]),
        )
        .expect("seed project access");
    }
    for (membership, group, user) in [(row(0xa1), group_a, USER_A), (row(0xa2), group_b, USER_B)] {
        db.insert_with_id(
            GROUP_MEMBERS,
            membership,
            BTreeMap::from([
                ("group".to_owned(), Value::Uuid(group.0)),
                ("user".to_owned(), Value::Uuid(user.0)),
            ]),
        )
        .expect("seed reachability seed membership");
    }

    (project_a, project_b, document_a, document_b)
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
#[ignore = "prepared claim routing is unlanded; tracked separately from INV-RLS-21."]
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

// REMOVED for the tests-only PR: prepared_policy_claim_predicate_routing_rejects_unsupported_shapes
// asserted ErrorCode::PreparedClaimPredicateRoutingUnsupported, a fail-closed rejection that is
// part of the unlanded fix rather than of INV-RLS-21 itself. It returns with the implementation.

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
fn policy_dependency_reads_do_not_expose_dependency_rows() {
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
                Value::String("not-region-a".to_owned()),
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
    for (identity, other_team) in [(USER_A, team_b), (USER_B, team_a)] {
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(other_team.0))]),
            )
            .expect("prepare cross-principal dependency binding");
        assert!(
            row_ids_for_identity(&db, &prepared, identity).is_empty(),
            "raw evidence must not bypass the outer policy's authenticated user and region predicates"
        );
    }

    let memberships = db
        .prepare_query(&Query::from(MEMBERSHIPS))
        .expect("prepare direct membership read");
    assert!(
        row_ids_for_identity(&db, &memberships, USER_A).is_empty(),
        "raw dependency evidence must not make the dependency row directly visible"
    );
    assert_eq!(
        row_ids_for_identity(&db, &memberships, USER_B),
        vec![row(0x32)],
        "ordinary dependency-table reads must still enforce its own policy"
    );
}

#[test]
fn dependency_policies_are_not_recursively_composed_into_outer_policy() {
    let db = open_db_with_schema(two_hop_seeded_policy_schema());
    let (project_a, project_b, document_a, document_b) = seed_two_hop_reachability_policy(&db);
    let query = Query::from(DOCUMENTS).filter(eq(col("project"), param("project")));
    let prepared_a = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("project".to_owned(), Value::Uuid(project_a.0))]),
        )
        .expect("prepare project A binding");
    let prepared_b = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("project".to_owned(), Value::Uuid(project_b.0))]),
        )
        .expect("prepare project B binding");

    assert_eq!(prepared_a.shape().shape_id(), prepared_b.shape().shape_id());
    assert_eq!(
        row_ids_for_identity(&db, &prepared_a, USER_A),
        vec![document_a],
        "the outer document policy requires a matching membership row; the membership table's own policy is not recursively composed"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_a, USER_B),
        vec![document_a],
        "the dependency table's nested project policy must not narrow the outer policy"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_b, USER_A),
        vec![document_b],
        "raw policy evidence remains available regardless of the dependency table's own read policy"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_b, USER_B),
        vec![document_b],
        "principal B must receive only project B through the seeded policy chain"
    );
}

#[test]
fn policy_proof_implicit_and_outer_include_sources_do_not_reenter_policy_compilation() {
    let db = open_db_with_schema(two_hop_seeded_policy_schema());
    let (project_a, _, document_a, _) = seed_two_hop_reachability_policy(&db);
    let query = Query::from(DOCUMENTS)
        .filter(eq(col("project"), param("project")))
        .include("project");
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("project".to_owned(), Value::Uuid(project_a.0))]),
        )
        .expect("prepare included project binding");

    assert_eq!(
        row_ids_for_identity(&db, &prepared, USER_A),
        vec![document_a],
        "policy and outer include sources must not re-enter the protected policy"
    );
}

#[test]
fn mutually_referential_dependency_policies_do_not_recurse() {
    let db = open_db_with_schema(policy_proof_cycle_schema());
    let a = row(0xb1);
    let b = row(0xb2);
    db.insert_with_id(
        CYCLE_A,
        a,
        BTreeMap::from([("b".to_owned(), Value::Uuid(b.0))]),
    )
    .expect("seed cycle A");
    db.insert_with_id(
        CYCLE_B,
        b,
        BTreeMap::from([("a".to_owned(), Value::Uuid(a.0))]),
    )
    .expect("seed cycle B");

    let prepared = db
        .prepare_query(&Query::from(CYCLE_A))
        .expect("prepare cyclic policy query");
    assert_eq!(
        row_ids_for_identity(&db, &prepared, USER_A),
        vec![a],
        "each policy reads the other table as raw evidence instead of recursively applying its policy"
    );
}

#[test]
#[ignore = "prepared claim routing is unlanded; tracked separately from INV-RLS-21."]
fn prepared_binding_reprepares_claim_routing_after_schema_change() {
    let db = open_db_with_schema_as(schema(), AuthorId::SYSTEM);
    let team_a = row(0x11);
    let team_b = row(0x12);
    let document_a = row(0x41);
    let document_b = row(0x42);
    seed(&db, team_a, team_b, "region-a", "region-b");
    db.set_identity_claims(
        USER_A,
        BTreeMap::from([("region".to_owned(), Value::String("region-a".to_owned()))]),
    );
    db.set_identity_claims(
        USER_B,
        BTreeMap::from([("region".to_owned(), Value::String("region-b".to_owned()))]),
    );
    let query = Query::from(DOCUMENTS).filter(eq(col("team"), param("team")));
    let prepared_v1 = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
        )
        .expect("prepare v1 team A binding");
    assert_eq!(
        row_ids_for_identity(&db, &prepared_v1, USER_A),
        vec![document_a]
    );
    assert!(row_ids_for_identity(&db, &prepared_v1, USER_B).is_empty());

    let v2 = SchemaVersion::new(evolved_schema());
    db.publish_schema(v2.clone()).expect("publish v2 schema");
    db.set_current_write_schema(CurrentWriteSchema {
        revision: 1,
        schema: v2.id,
    })
    .expect("select v2 schema");

    let prepared_v2_a = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
        )
        .expect("reprepare v2 team A binding");
    let prepared_v2_b = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team_b.0))]),
        )
        .expect("reprepare v2 team B binding");

    assert_ne!(
        prepared_v1.shape().shape_id(),
        prepared_v2_a.shape().shape_id()
    );
    assert_eq!(
        prepared_v2_a.shape().shape_id(),
        prepared_v2_b.shape().shape_id()
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_v2_a, USER_A),
        vec![document_a]
    );
    assert!(
        row_ids_for_identity(&db, &prepared_v2_a, USER_B).is_empty(),
        "principal B's claim must not select principal A's row after re-preparation"
    );
    assert!(
        row_ids_for_identity(&db, &prepared_v2_b, USER_A).is_empty(),
        "principal A's claim must not select principal B's row after re-preparation"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_v2_b, USER_B),
        vec![document_b]
    );
}

#[test]
#[ignore = "prepared claim routing is unlanded; tracked separately from INV-RLS-21."]
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
    let error = db
        .prepare_query(&Query::from(DOCUMENTS))
        .expect_err("conflicting policy claim types must fail during preparation");
    assert!(
        error.to_string().contains("conflicting policy types"),
        "unexpected error: {error}"
    );
}

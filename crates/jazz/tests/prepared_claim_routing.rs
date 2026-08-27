use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{
    CurrentWriteSchema, LensOp, MigrationLens, SchemaLineagePublication, SchemaVersion, TableLens,
};
use jazz::query::{OrderDirection, Query, col, eq, param};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    CmpOp as PublicCmpOp, PolicyExpr as PublicPolicyExpr, PolicyValue as PublicPolicyValue,
    RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelProjectColumn, RelProjectExpr, RelRecursionBound, RelValueRef,
    RowIdRef as RelRowIdRef, Value as PublicValue,
};
use jazz::tools::{
    ColumnType as PublicColumnType, Schema as PublicSchema, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};
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
fn writer() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("82000000-0000-0000-0000-000000000001"))
}

fn user_a() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("82000000-0000-0000-0000-000000000002"))
}

fn user_b() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("82000000-0000-0000-0000-000000000003"))
}

fn provider_claim(name: &str) -> String {
    // `Db::set_identity_claims` receives the post-admission representation.
    // Public policy still addresses these values through `session.claims[name]`.
    format!("\0claims:{name}")
}

fn test_user_claims(region: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim("region"), Value::String(region.to_owned()))])
}

type BenchDb = Db<TestStorage>;

fn row(tag: u8) -> RowUuid {
    RowUuid::from_bytes([tag; 16])
}

fn compile_schema(source: &PublicSchema) -> JazzSchema {
    jazz::schema::JazzSchema::new(source).expect("prepared claim routing source schema compiles")
}

fn allow_all_policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PublicPolicyExpr::True)
        .with_insert(PublicPolicyExpr::True)
        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
        .with_delete(PublicPolicyExpr::True)
}

fn read_and_allow_all_writes(read: PublicPolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(read)
        .with_insert(PublicPolicyExpr::True)
        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
        .with_delete(PublicPolicyExpr::True)
}

fn session_value(path: &[&str]) -> PublicPolicyValue {
    PublicPolicyValue::SessionRef(path.iter().map(|segment| (*segment).to_owned()).collect())
}

fn session_eq(column: &str, path: &[&str]) -> PublicPolicyExpr {
    PublicPolicyExpr::Cmp {
        column: column.to_owned(),
        op: PublicCmpOp::Eq,
        value: session_value(path),
    }
}

fn outer_eq(column: &str, outer_column: &str) -> PublicPolicyExpr {
    session_eq(column, &["__jazz_outer_row", outer_column])
}

fn text_eq(column: &str, value: &str) -> PublicPolicyExpr {
    PublicPolicyExpr::Cmp {
        column: column.to_owned(),
        op: PublicCmpOp::Eq,
        value: PublicPolicyValue::Literal(PublicValue::Text(value.to_owned())),
    }
}

fn exists(table: &str, conditions: Vec<PublicPolicyExpr>) -> PublicPolicyExpr {
    PublicPolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PublicPolicyExpr::And(conditions)),
    }
}

fn schema() -> JazzSchema {
    schema_with_membership_policy(Some(PublicPolicyExpr::True))
}

fn schema_with_membership_policy(membership_policy: Option<PublicPolicyExpr>) -> JazzSchema {
    let document_policy = exists(
        MEMBERSHIPS,
        vec![
            outer_eq("team", "team"),
            session_eq("user", &["user"]),
            session_eq("region", &["claims", "region"]),
        ],
    );
    let membership_policies = membership_policy
        .map(read_and_allow_all_writes)
        .unwrap_or_else(|| {
            TablePolicies::new()
                .with_insert(PublicPolicyExpr::True)
                .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                .with_delete(PublicPolicyExpr::True)
        });
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(TEAMS)
                    .column("name", PublicColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(MEMBERSHIPS)
                    .fk_column("team", TEAMS)
                    .column("user", PublicColumnType::Text)
                    .column("region", PublicColumnType::Text)
                    .policies(membership_policies),
            )
            .table(
                TableSchemaBuilder::new(DOCUMENTS)
                    .fk_column("team", TEAMS)
                    .column("updated_at", PublicColumnType::Timestamp)
                    .policies(read_and_allow_all_writes(document_policy)),
            )
            .build(),
    )
}

fn two_hop_seeded_policy_schema() -> JazzSchema {
    let document_policy = exists(MEMBERSHIPS, vec![outer_eq("document", "id")]);
    let membership_policy = exists(PROJECTS, vec![outer_eq("id", "project")]);
    let seed = RelExpr::Project {
        input: Box::new(RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: GROUP_MEMBERS.into(),
                alias: None,
            }),
            predicate: RelPredicateExpr::Cmp {
                left: RelColumnRef {
                    scope: None,
                    column: "user".to_owned(),
                },
                op: RelPredicateCmpOp::Eq,
                right: RelValueRef::SessionRef(vec!["user".to_owned()]),
            },
        }),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(RelColumnRef {
                scope: None,
                column: "group".to_owned(),
            }),
        }],
    };
    let step = RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: GROUP_EDGES.into(),
                    alias: Some("edges".to_owned()),
                }),
                predicate: RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: Some("edges".to_owned()),
                        column: "member".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::RowId(RelRowIdRef::Frontier),
                },
            }),
            right: Box::new(RelExpr::TableScan {
                table: GROUPS.into(),
                alias: Some("target".to_owned()),
            }),
            on: vec![RelJoinCondition {
                left: RelColumnRef {
                    scope: Some("edges".to_owned()),
                    column: "parent".to_owned(),
                },
                right: RelColumnRef {
                    scope: Some("target".to_owned()),
                    column: "id".to_owned(),
                },
            }],
            join_kind: RelJoinKind::Inner,
        }),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(RelColumnRef {
                scope: Some("target".to_owned()),
                column: "id".to_owned(),
            }),
        }],
    };
    let project_policy = PublicPolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Gather {
                    seed: Box::new(seed),
                    step: Box::new(step),
                    frontier_key: RelKeyRef::RowId(RelRowIdRef::Current),
                    bound: RelRecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelKeyRef::RowId(RelRowIdRef::Current)],
                }),
                right: Box::new(RelExpr::TableScan {
                    table: PROJECT_ACCESS.into(),
                    alias: Some("access".to_owned()),
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "group".to_owned(),
                    },
                }],
                join_kind: RelJoinKind::Inner,
            }),
            predicate: RelPredicateExpr::Cmp {
                left: RelColumnRef {
                    scope: Some("access".to_owned()),
                    column: "project".to_owned(),
                },
                op: RelPredicateCmpOp::Eq,
                right: RelValueRef::RowId(RelRowIdRef::Outer),
            },
        },
    };

    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(DOCUMENTS)
                    .fk_column("project", PROJECTS)
                    .column("updated_at", PublicColumnType::Timestamp)
                    .policies(read_and_allow_all_writes(document_policy)),
            )
            .table(
                TableSchemaBuilder::new(MEMBERSHIPS)
                    .fk_column("document", DOCUMENTS)
                    .fk_column("project", PROJECTS)
                    .policies(read_and_allow_all_writes(membership_policy)),
            )
            .table(
                TableSchemaBuilder::new(PROJECTS)
                    .column("name", PublicColumnType::Text)
                    .policies(read_and_allow_all_writes(project_policy)),
            )
            .table(
                TableSchemaBuilder::new(GROUPS)
                    .column("name", PublicColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(PROJECT_ACCESS)
                    .fk_column("project", PROJECTS)
                    .fk_column("group", GROUPS)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(GROUP_MEMBERS)
                    .fk_column("group", GROUPS)
                    .column("user", PublicColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(GROUP_EDGES)
                    .fk_column("member", GROUPS)
                    .fk_column("parent", GROUPS)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn policy_proof_cycle_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(CYCLE_A)
                    .fk_column("b", CYCLE_B)
                    .policies(read_and_allow_all_writes(exists(
                        CYCLE_B,
                        vec![outer_eq("id", "b")],
                    ))),
            )
            .table(
                TableSchemaBuilder::new(CYCLE_B)
                    .fk_column("a", CYCLE_A)
                    .policies(read_and_allow_all_writes(exists(
                        CYCLE_A,
                        vec![outer_eq("id", "a")],
                    ))),
            )
            .build(),
    )
}

fn evolved_schema() -> JazzSchema {
    let mut source = schema().public_schema().clone();
    source
        .get_mut(&DOCUMENTS.into())
        .expect("documents table")
        .columns
        .columns
        .push(
            jazz::tools::ColumnDescriptor::new("generation", PublicColumnType::Timestamp)
                .default(PublicValue::Timestamp(0)),
        );
    compile_schema(&source)
}

fn public_join_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(TEAMS)
                    .column("name", PublicColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new(DOCUMENTS)
                    .fk_column("team", TEAMS)
                    .column("updated_at", PublicColumnType::Timestamp)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn evolved_public_join_schema() -> JazzSchema {
    let mut source = public_join_schema().public_schema().clone();
    source
        .get_mut(&DOCUMENTS.into())
        .expect("documents table")
        .columns
        .columns
        .push(
            jazz::tools::ColumnDescriptor::new("generation", PublicColumnType::Timestamp)
                .default(PublicValue::Timestamp(0)),
        );
    compile_schema(&source)
}

fn open_db() -> BenchDb {
    open_db_with_schema(schema())
}

fn open_db_with_schema(schema: JazzSchema) -> BenchDb {
    open_db_with_schema_as(schema, writer())
}

fn open_db_with_schema_as(schema: JazzSchema, author: AuthorSubject) -> BenchDb {
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&family_refs),
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
    identity: AuthorSubject,
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
        block_on(db.insert(
            TEAMS,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(team),
                ..Default::default()
            },
        ))
        .expect("seed team");
    }
    for (membership, team, user, region) in [
        (row(0x31), team_a, user_a(), region_a),
        (row(0x32), team_b, user_b(), region_b),
    ] {
        block_on(db.insert(
            MEMBERSHIPS,
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team.0)),
                (
                    "user".to_owned(),
                    Value::String(user.canonical().to_owned()),
                ),
                ("region".to_owned(), Value::String(region.to_owned())),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(membership),
                ..Default::default()
            },
        ))
        .expect("seed membership");
    }
    for (document, team, updated_at) in [(row(0x41), team_a, 10), (row(0x42), team_b, 20)] {
        block_on(db.insert(
            DOCUMENTS,
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team.0)),
                ("updated_at".to_owned(), Value::U64(updated_at)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(document),
                ..Default::default()
            },
        ))
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
        block_on(db.insert(
            PROJECTS,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(project),
                ..Default::default()
            },
        ))
        .expect("seed project");
    }
    for (group, name) in [(group_a, "group-a"), (group_b, "group-b")] {
        block_on(db.insert(
            GROUPS,
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(group),
                ..Default::default()
            },
        ))
        .expect("seed group");
    }
    for (document, project, updated_at) in
        [(document_a, project_a, 10), (document_b, project_b, 20)]
    {
        block_on(db.insert(
            DOCUMENTS,
            BTreeMap::from([
                ("project".to_owned(), Value::Uuid(project.0)),
                ("updated_at".to_owned(), Value::U64(updated_at)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(document),
                ..Default::default()
            },
        ))
        .expect("seed document");
    }
    for (membership, document, project) in [
        (row(0x81), document_a, project_a),
        (row(0x82), document_b, project_b),
    ] {
        block_on(db.insert(
            MEMBERSHIPS,
            BTreeMap::from([
                ("document".to_owned(), Value::Uuid(document.0)),
                ("project".to_owned(), Value::Uuid(project.0)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(membership),
                ..Default::default()
            },
        ))
        .expect("seed membership");
    }
    for (access, project, group) in [
        (row(0x91), project_a, group_a),
        (row(0x92), project_b, group_b),
    ] {
        block_on(db.insert(
            PROJECT_ACCESS,
            BTreeMap::from([
                ("project".to_owned(), Value::Uuid(project.0)),
                ("group".to_owned(), Value::Uuid(group.0)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(access),
                ..Default::default()
            },
        ))
        .expect("seed project access");
    }
    for (membership, group, user) in [
        (row(0xa1), group_a, user_a()),
        (row(0xa2), group_b, user_b()),
    ] {
        block_on(db.insert(
            GROUP_MEMBERS,
            BTreeMap::from([
                ("group".to_owned(), Value::Uuid(group.0)),
                (
                    "user".to_owned(),
                    Value::String(user.canonical().to_owned()),
                ),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(membership),
                ..Default::default()
            },
        ))
        .expect("seed reachability seed membership");
    }

    (project_a, project_b, document_a, document_b)
}

fn assert_call_order(
    first: (AuthorSubject, RowUuid, RowUuid),
    second: (AuthorSubject, RowUuid, RowUuid),
    region_a: &str,
    region_b: &str,
) {
    let db = open_db();
    let team_a = row(0x11);
    let team_b = row(0x12);
    seed(&db, team_a, team_b, region_a, region_b);
    db.set_identity_claims(user_a(), test_user_claims(region_a));
    db.set_identity_claims(user_b(), test_user_claims(region_b));
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
        (user_a(), team_a, document_a),
        (user_b(), team_b, document_b),
        "region-a",
        "region-b",
    );
    assert_call_order(
        (user_b(), team_b, document_b),
        (user_a(), team_a, document_a),
        "region-a",
        "region-b",
    );
}

// REMOVED for the tests-only PR: prepared_policy_claim_predicate_routing_rejects_unsupported_shapes
// asserted ErrorCode::PreparedClaimPredicateRoutingUnsupported, a fail-closed rejection that is
// part of the unlanded fix rather than of INV-RLS-21 itself. It returns with the implementation.
#[test]
fn prepared_policy_claim_routing_preserves_claimless_union_branches() {
    let policy = PublicPolicyExpr::Or(vec![
        text_eq("visibility", "public"),
        session_eq("owner", &["user"]),
        session_eq("region", &["claims", "region"]),
    ]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(DOCUMENTS)
                    .column("visibility", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Text)
                    .column("region", PublicColumnType::Text)
                    .policies(read_and_allow_all_writes(policy)),
            )
            .build(),
    );
    let db = open_db_with_schema(schema);
    let public = row(0x51);
    let private = row(0x52);
    let regional = row(0x53);
    db.set_identity_claims(user_a(), test_user_claims("region-a"));
    db.set_identity_claims(user_b(), test_user_claims("region-b"));
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("visibility".to_owned(), Value::String("public".to_owned())),
            (
                "owner".to_owned(),
                Value::String(writer().canonical().to_owned()),
            ),
            ("region".to_owned(), Value::String("other".to_owned())),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(public),
            ..Default::default()
        },
    ))
    .expect("seed public document");
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("visibility".to_owned(), Value::String("private".to_owned())),
            (
                "owner".to_owned(),
                Value::String(user_a().canonical().to_owned()),
            ),
            ("region".to_owned(), Value::String("other".to_owned())),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(private),
            ..Default::default()
        },
    ))
    .expect("seed private document");
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("visibility".to_owned(), Value::String("private".to_owned())),
            (
                "owner".to_owned(),
                Value::String(writer().canonical().to_owned()),
            ),
            ("region".to_owned(), Value::String("region-a".to_owned())),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(regional),
            ..Default::default()
        },
    ))
    .expect("seed regional document");

    let prepared = db
        .prepare_query(&Query::from(DOCUMENTS).order_by("visibility", OrderDirection::Asc))
        .expect("prepare mixed claim and claimless policy");
    let mut rows = block_on(db.all_for_identity(&prepared, opts(), user_a()))
        .expect("read mixed claim and claimless policy")
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    rows.sort_unstable();
    let mut expected = vec![public, private, regional];
    expected.sort_unstable();
    assert_eq!(rows, expected);

    let rows = block_on(db.all_for_identity(&prepared, opts(), user_b()))
        .expect("read claimless policy branch")
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![public]);

    let windowed = db
        .prepare_query(
            &Query::from(DOCUMENTS)
                .order_by("visibility", OrderDirection::Desc)
                .limit(1),
        )
        .expect("prepare finite mixed-policy window");
    for identity in [user_a(), user_b()] {
        let rows = block_on(db.all_for_identity(&windowed, opts(), identity))
            .expect("read finite claimless policy branch")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![public]);
    }
}

fn assert_retained_subscription_regions(region_a: &str, region_b: &str) {
    let db = open_db();
    let team_a = row(0x11);
    let team_b = row(0x12);
    seed(&db, team_a, team_b, region_a, region_b);
    db.set_identity_claims(user_a(), test_user_claims(region_a));
    db.set_identity_claims(user_b(), test_user_claims(region_b));
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
    let mut stream_a = block_on(db.subscribe_for_identity(&prepared(team_a), opts(), user_a()))
        .expect("subscribe team A");
    let mut stream_b = block_on(db.subscribe_for_identity(&prepared(team_b), opts(), user_b()))
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
fn prepared_nested_claim_routes_keep_two_bindings_isolated_through_live_membership() {
    const CHATS: &str = "chats";
    const CHAT_MEMBERS: &str = "chat_members";

    let chat_policy = PublicPolicyExpr::Or(vec![
        text_eq("name", "never-visible"),
        session_eq("joinCode", &["claims", "join_code"]),
        exists(
            CHAT_MEMBERS,
            vec![outer_eq("chatId", "id"), session_eq("userId", &["user"])],
        ),
    ]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(CHATS)
                    .column("name", PublicColumnType::Text)
                    .nullable_column("joinCode", PublicColumnType::Text)
                    .policies(read_and_allow_all_writes(chat_policy)),
            )
            .table(
                TableSchemaBuilder::new(CHAT_MEMBERS)
                    .fk_column("chatId", CHATS)
                    .column("userId", PublicColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq("userId", &["user"]))),
            )
            .build(),
    );
    let db = open_db_with_schema(schema);
    let chat_a = row(0xc1);
    let chat_b = row(0xc2);
    let join_code_a = "invite-a";
    let join_code_b = "invite-b";
    for (chat, name, join_code) in [
        (chat_a, "chat-a", join_code_a),
        (chat_b, "chat-b", join_code_b),
    ] {
        block_on(db.insert(
            CHATS,
            BTreeMap::from([
                ("name".to_owned(), Value::String(name.to_owned())),
                (
                    "joinCode".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String(format!("stored-{join_code}"))))),
                ),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(chat),
                ..Default::default()
            },
        ))
        .expect("seed invite chat");
    }
    for (identity, join_code) in [(user_a(), join_code_a), (user_b(), join_code_b)] {
        db.set_identity_claims(
            identity,
            BTreeMap::from([(
                provider_claim("join_code"),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            )]),
        );
    }
    let query = Query::from(CHATS).filter(eq(col("id"), param("id")));
    let prepared = |chat: RowUuid| {
        db.prepare_query_bound(
            &query,
            BTreeMap::from([("id".to_owned(), Value::Uuid(chat.0))]),
        )
        .expect("prepare chat binding")
    };
    let mut stream_a = block_on(db.subscribe_for_identity(&prepared(chat_a), opts(), user_a()))
        .expect("subscribe invite binding A");
    let mut stream_b = block_on(db.subscribe_for_identity(&prepared(chat_b), opts(), user_b()))
        .expect("subscribe invite binding B");
    let initial_rows = |event| match event {
        SubscriptionEvent::Delta {
            reset: true, added, ..
        } => added
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        other => panic!("expected initial reset, got {other:?}"),
    };
    assert!(
        initial_rows(stream_a.try_next_event().expect("invite A reset")).is_empty(),
        "a chat without its membership must not be visible"
    );
    assert!(
        initial_rows(stream_b.try_next_event().expect("invite B reset")).is_empty(),
        "a chat without its membership must not be visible"
    );

    block_on(db.insert(
        CHAT_MEMBERS,
        BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat_a.0)),
            (
                "userId".to_owned(),
                Value::String(user_a().canonical().to_owned()),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row(0xc3)),
            ..Default::default()
        },
    ))
    .expect("commit membership for binding A");
    let added_rows = |event| match event {
        SubscriptionEvent::Delta {
            reset: false,
            added,
            ..
        } => added
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        other => panic!("expected live membership delta, got {other:?}"),
    };
    assert_eq!(
        added_rows(
            stream_a
                .try_next_event()
                .expect("binding A membership delta")
        ),
        vec![chat_a],
        "the matching binding receives its live membership CommitUnit"
    );
    assert!(
        stream_b.try_next_event().is_none(),
        "binding A's CommitUnit must not leak into binding B"
    );

    block_on(db.insert(
        CHAT_MEMBERS,
        BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat_b.0)),
            (
                "userId".to_owned(),
                Value::String(user_b().canonical().to_owned()),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row(0xc4)),
            ..Default::default()
        },
    ))
    .expect("commit membership for binding B");
    assert_eq!(
        added_rows(
            stream_b
                .try_next_event()
                .expect("binding B membership delta")
        ),
        vec![chat_b],
        "the other nullable invite binding receives only its own CommitUnit"
    );
    assert!(
        stream_a.try_next_event().is_none(),
        "binding B's CommitUnit must not leak back into binding A"
    );
}

#[test]
fn policy_dependency_reads_do_not_expose_dependency_rows() {
    let membership_policy = session_eq("region", &["claims", "membership_region"]);
    let db = open_db_with_schema(schema_with_membership_policy(Some(membership_policy)));
    let team_a = row(0x11);
    let team_b = row(0x12);
    seed(&db, team_a, team_b, "region-a", "region-b");
    db.set_identity_claims(
        user_a(),
        BTreeMap::from([
            (
                provider_claim("region"),
                Value::String("region-a".to_owned()),
            ),
            (
                provider_claim("membership_region"),
                Value::String("not-region-a".to_owned()),
            ),
        ]),
    );
    db.set_identity_claims(
        user_b(),
        BTreeMap::from([
            (
                provider_claim("region"),
                Value::String("region-b".to_owned()),
            ),
            (
                provider_claim("membership_region"),
                Value::String("region-b".to_owned()),
            ),
        ]),
    );
    let query = Query::from(DOCUMENTS).filter(eq(col("team"), param("team")));

    for (identity, team, expected) in [(user_a(), team_a, row(0x41)), (user_b(), team_b, row(0x42))]
    {
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
    for (identity, other_team) in [(user_a(), team_b), (user_b(), team_a)] {
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
        row_ids_for_identity(&db, &memberships, user_a()).is_empty(),
        "raw dependency evidence must not make the dependency row directly visible"
    );
    assert_eq!(
        row_ids_for_identity(&db, &memberships, user_b()),
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
        row_ids_for_identity(&db, &prepared_a, user_a()),
        vec![document_a],
        "the outer document policy requires a matching membership row; the membership table's own policy is not recursively composed"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_a, user_b()),
        vec![document_a],
        "the dependency table's nested project policy must not narrow the outer policy"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_b, user_a()),
        vec![document_b],
        "raw policy evidence remains available regardless of the dependency table's own read policy"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_b, user_b()),
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
        row_ids_for_identity(&db, &prepared, user_a()),
        vec![document_a],
        "policy and outer include sources must not re-enter the protected policy"
    );
}

#[test]
fn mutually_referential_dependency_policies_do_not_recurse() {
    let db = open_db_with_schema(policy_proof_cycle_schema());
    let a = row(0xb1);
    let b = row(0xb2);
    block_on(db.insert(
        CYCLE_A,
        BTreeMap::from([("b".to_owned(), Value::Uuid(b.0))]),
        jazz::db::InsertOptions {
            row_id: Some(a),
            ..Default::default()
        },
    ))
    .expect("seed cycle A");
    block_on(db.insert(
        CYCLE_B,
        BTreeMap::from([("a".to_owned(), Value::Uuid(a.0))]),
        jazz::db::InsertOptions {
            row_id: Some(b),
            ..Default::default()
        },
    ))
    .expect("seed cycle B");

    let prepared = db
        .prepare_query(&Query::from(CYCLE_A))
        .expect("prepare cyclic policy query");
    assert_eq!(
        row_ids_for_identity(&db, &prepared, user_a()),
        vec![a],
        "each policy reads the other table as raw evidence instead of recursively applying its policy"
    );
}

#[test]
fn prepared_binding_reprepares_claim_routing_after_schema_change() {
    let db = open_db_with_schema_as(schema(), AuthorSubject::SYSTEM);
    let team_a = row(0x11);
    let team_b = row(0x12);
    let document_a = row(0x41);
    let document_b = row(0x42);
    seed(&db, team_a, team_b, "region-a", "region-b");
    db.set_identity_claims(user_a(), test_user_claims("region-a"));
    db.set_identity_claims(user_b(), test_user_claims("region-b"));
    let query = Query::from(DOCUMENTS).filter(eq(col("team"), param("team")));
    let prepared_v1 = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
        )
        .expect("prepare v1 team A binding");
    assert_eq!(
        row_ids_for_identity(&db, &prepared_v1, user_a()),
        vec![document_a]
    );
    assert!(row_ids_for_identity(&db, &prepared_v1, user_b()).is_empty());
    let prepared_team_v1 = db
        .prepare_query_bound(
            &Query::from(TEAMS).filter(eq(col("name"), param("name"))),
            BTreeMap::from([("name".to_owned(), Value::String("Team A".to_owned()))]),
        )
        .expect("prepare v1 team binding");
    assert_eq!(
        db.read(&prepared_team_v1)
            .expect("read prepared v1 team before schema change")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![team_a]
    );

    let v2 = SchemaVersion::new(evolved_schema());
    let lens = MigrationLens::new(
        schema().version_id(),
        v2.id,
        vec![
            TableLens {
                source_table: DOCUMENTS.to_owned(),
                target_table: DOCUMENTS.to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "generation".to_owned(),
                    default: Value::U64(0),
                }],
            },
            TableLens {
                source_table: TEAMS.to_owned(),
                target_table: TEAMS.to_owned(),
                ops: Vec::new(),
            },
            TableLens {
                source_table: MEMBERSHIPS.to_owned(),
                target_table: MEMBERSHIPS.to_owned(),
                ops: Vec::new(),
            },
        ],
    );
    block_on(db.publish_schema_with_lens(
        1,
        SchemaLineagePublication::new(v2.clone(), lens, Vec::<String>::new(), Vec::<String>::new()),
    ))
    .expect("publish v1-to-v2 lineage");
    block_on(db.set_current_write_schema(CurrentWriteSchema {
        revision: 1,
        schema: v2.id,
    }))
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
        row_ids_for_identity(&db, &prepared_v2_a, user_a()),
        vec![document_a]
    );
    let projected = block_on(db.all_for_identity(&prepared_v2_a, opts(), user_a()))
        .expect("read v2 defaulted document");
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].cell_at(2), Some(Value::U64(0)));
    assert_eq!(
        db.read(&prepared_team_v1)
            .expect("invalidate stale v1 Groove handle after catalogue rebuild")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![team_a]
    );
    assert!(
        row_ids_for_identity(&db, &prepared_v2_a, user_b()).is_empty(),
        "principal B's claim must not select principal A's row after re-preparation"
    );
    assert!(
        row_ids_for_identity(&db, &prepared_v2_b, user_a()).is_empty(),
        "principal A's claim must not select principal B's row after re-preparation"
    );
    assert_eq!(
        row_ids_for_identity(&db, &prepared_v2_b, user_b()),
        vec![document_b]
    );
}

#[cfg(feature = "testing")]
#[test]
/// A rebuilt subscription releases only its own replacement runtime handle.
///
/// Alice and Bob hold distinct prepared, claim-scoped subscriptions. After a
/// catalogue rebuild, Alice drops her stream; Bob's rebuilt stream must retain
/// its handle and deliver both its already-pending update and a later insert.
///
/// ```text
/// alice ──subscribe──► rebuilt runtime ◄──subscribe── bob
/// alice ──drop───────► release Alice only ──write──► Bob receives row
/// ```
fn rebuilt_subscription_drop_releases_rehydrated_handle_without_touching_peer() {
    let db = open_db_with_schema_as(schema(), AuthorSubject::SYSTEM);
    let team_a = row(0x11);
    let team_b = row(0x12);
    seed(&db, team_a, team_b, "region-a", "region-b");
    db.set_identity_claims(user_a(), test_user_claims("region-a"));
    db.set_identity_claims(user_b(), test_user_claims("region-b"));
    let query = Query::from(DOCUMENTS).filter(eq(col("team"), param("team")));
    let prepared = |team: RowUuid| {
        db.prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
        )
        .expect("prepare subscription binding")
    };
    let mut stream_a = block_on(db.subscribe_for_identity(&prepared(team_a), opts(), user_a()))
        .expect("subscribe A");
    let mut stream_b = block_on(db.subscribe_for_identity(&prepared(team_b), opts(), user_b()))
        .expect("subscribe B");
    stream_a.try_next_event().expect("A reset");
    stream_b.try_next_event().expect("B reset");

    let v2 = SchemaVersion::new(evolved_schema());
    let lens = MigrationLens::new(
        schema().version_id(),
        v2.id,
        vec![
            TableLens {
                source_table: DOCUMENTS.to_owned(),
                target_table: DOCUMENTS.to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "generation".to_owned(),
                    default: Value::U64(0),
                }],
            },
            TableLens {
                source_table: TEAMS.to_owned(),
                target_table: TEAMS.to_owned(),
                ops: Vec::new(),
            },
            TableLens {
                source_table: MEMBERSHIPS.to_owned(),
                target_table: MEMBERSHIPS.to_owned(),
                ops: Vec::new(),
            },
        ],
    );
    block_on(db.publish_schema_with_lens(
        1,
        SchemaLineagePublication::new(v2.clone(), lens, Vec::<String>::new(), Vec::<String>::new()),
    ))
    .expect("publish v1-to-v2 lineage");
    block_on(db.set_current_write_schema(CurrentWriteSchema {
        revision: 1,
        schema: v2.id,
    }))
    .expect("activate v2");
    block_on(db.update(
        DOCUMENTS,
        row(0x41),
        BTreeMap::from([("updated_at".to_owned(), Value::U64(11))]),
        Default::default(),
    ))
    .expect("trigger A subscription rehydration");
    block_on(db.update(
        DOCUMENTS,
        row(0x42),
        BTreeMap::from([("updated_at".to_owned(), Value::U64(21))]),
        Default::default(),
    ))
    .expect("trigger B subscription rehydration");
    assert!(matches!(
        stream_a.try_next_event(),
        Some(SubscriptionEvent::Delta { reset: true, .. })
    ));
    assert!(matches!(
        stream_b.try_next_event(),
        Some(SubscriptionEvent::Delta { reset: true, .. })
    ));
    // The second trigger updates Bob's pre-existing row after the shared
    // catalogue rebuild. It is a real FIFO delta, not evidence that the later
    // insert was lost.
    assert!(matches!(
        stream_b.try_next_event(),
        Some(SubscriptionEvent::Delta { updated, .. })
            if updated.iter().any(|output| output.row_uuid() == row(0x42))
    ));
    assert_eq!(db.active_groove_subscriptions_for_test(), 2);

    drop(stream_a);
    // Stream Drop is deliberately non-blocking. The successor runtime handle
    // must be retired by the next node owner turn, without touching Bob's.
    block_on(db.tick()).expect("drain A finalization after runtime rebuild");
    assert_eq!(db.active_groove_subscriptions_for_test(), 1);
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team_b.0)),
            ("updated_at".to_owned(), Value::U64(30)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row(0x43)),
            ..Default::default()
        },
    ))
    .expect("write after dropping A");
    assert!(matches!(
        stream_b.try_next_event(),
        Some(SubscriptionEvent::Delta { added, .. }) if added.iter().any(|output| output.row_uuid() == row(0x43))
    ));
}

#[test]
/// A prepared join owned by Alice survives a catalogue-driven Groove rebuild.
///
/// Alice publishes a compatible schema lineage after preparing the v1 join;
/// the test then occupies the rebuilt runtime with a distinct v2 handle before
/// reading Alice's old handle.
///
/// ```text
/// alice ──prepare v1 join──► runtime v1
/// alice ──publish lineage──► runtime v2 ──prepare conflicting v2 join──► read v1 handle
/// ```
fn prepared_join_handle_recompiles_after_catalogue_runtime_rebuild() {
    let v1 = public_join_schema();
    let db = open_db_with_schema_as(v1.clone(), AuthorSubject::SYSTEM);
    let team = row(0xa1);
    let document = row(0xa2);
    block_on(db.insert(
        TEAMS,
        BTreeMap::from([("name".to_owned(), Value::String("Team A".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(team),
            ..Default::default()
        },
    ))
    .expect("seed team");
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("updated_at".to_owned(), Value::U64(1)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(document),
            ..Default::default()
        },
    ))
    .expect("seed document");
    let join = Query::from(DOCUMENTS).join_via_column(
        TEAMS,
        "id",
        "team",
        [eq(col("name"), param("name"))],
    );
    let prepared_v1 = db
        .prepare_query_bound(
            &join,
            BTreeMap::from([("name".to_owned(), Value::String("Team A".to_owned()))]),
        )
        .expect("prepare v1 join");
    assert_eq!(
        db.read(&prepared_v1)
            .expect("read v1 prepared join")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![document]
    );

    let v2 = SchemaVersion::new(evolved_public_join_schema());
    let lens = MigrationLens::new(
        v1.version_id(),
        v2.id,
        vec![
            TableLens {
                source_table: TEAMS.to_owned(),
                target_table: TEAMS.to_owned(),
                ops: Vec::new(),
            },
            TableLens {
                source_table: DOCUMENTS.to_owned(),
                target_table: DOCUMENTS.to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "generation".to_owned(),
                    default: Value::U64(0),
                }],
            },
        ],
    );
    block_on(db.publish_schema_with_lens(
        1,
        SchemaLineagePublication::new(v2.clone(), lens, Vec::<String>::new(), Vec::<String>::new()),
    ))
    .expect("publish v2 with lineage lens");
    block_on(db.set_current_write_schema(CurrentWriteSchema {
        revision: 1,
        schema: v2.id,
    }))
    .expect("activate v2");

    // A distinct v2 prepared shape occupies the fresh runtime before the old
    // handle is read, so correctness cannot rely on an accidentally matching
    // runtime-local prepared ID.
    let conflicting_v2 = db
        .prepare_query_bound(
            &Query::from(DOCUMENTS).join_via_column(
                TEAMS,
                "id",
                "team",
                [eq(col("name"), param("other_name"))],
            ),
            BTreeMap::from([("other_name".to_owned(), Value::String("missing".to_owned()))]),
        )
        .expect("prepare conflicting v2 join shape");
    assert!(
        db.read(&conflicting_v2)
            .expect("execute conflicting v2 prepared shape")
            .is_empty()
    );
    assert_eq!(
        db.read(&prepared_v1)
            .expect("stale v1 handle recompiles through the active schema")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![document]
    );
}

#[test]
#[ignore = "#1760: future shared-prepared-descriptor claim-name/type collision guard; INV-RLS-21 keeps dependency-policy descriptors separate today"]
fn prepared_binding_rejects_conflicting_claim_types_across_policies() {
    let root_policy = PublicPolicyExpr::And(vec![
        session_eq("team", &["claims", "shared_scope"]),
        exists(
            MEMBERSHIPS,
            vec![outer_eq("team", "team"), session_eq("user", &["user"])],
        ),
    ]);
    let membership_policy = session_eq("region", &["claims", "shared_scope"]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new(TEAMS).column("name", PublicColumnType::Text))
            .table(
                TableSchemaBuilder::new(MEMBERSHIPS)
                    .fk_column("team", TEAMS)
                    .column("user", PublicColumnType::Text)
                    .column("region", PublicColumnType::Text)
                    .policies(TablePolicies::new().with_select(membership_policy)),
            )
            .table(
                TableSchemaBuilder::new(DOCUMENTS)
                    .fk_column("team", TEAMS)
                    .policies(TablePolicies::new().with_select(root_policy)),
            )
            .build(),
    );
    let db = open_db_with_schema(schema);
    db.set_identity_claims(
        user_a(),
        BTreeMap::from([(provider_claim("shared_scope"), Value::Uuid(row(0x11).0))]),
    );
    let error = db
        .prepare_query(&Query::from(DOCUMENTS))
        .expect_err("conflicting policy claim types must fail during preparation");
    assert!(
        error.to_string().contains("conflicting policy types"),
        "unexpected error: {error}"
    );
}

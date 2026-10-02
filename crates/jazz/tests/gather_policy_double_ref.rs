//! Recursive `gather` read policies over a same-table double-reference edge,
//! evaluated by a history-complete (Global-tier) node for an admitted session.
//!
//! Mirrors `packages/jazz-tools/src/runtime/permissions.repro.test.ts` ›
//! "evaluates explicit gather seeds through a same-table double-ref edge":
//! a backend Node runtime with `tier: "global"` writes the graph locally and
//! then reads through `forSession`.
//!
//! The lowered read graph reaches one input node along two paths. Hydration
//! used to clear its "visiting" mark inside `debug_assert!`, so release builds
//! (NAPI/WASM) reported that shared input as "graph contains a dependency
//! cycle". Debug test builds only catch that regression with groove's debug
//! assertions disabled:
//! `cargo test --config 'profile.dev.package.groove.debug-assertions=false' ...`.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, Propagation, ReadOpts, SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelProjectColumn, RelProjectExpr, RelRecursionBound, RelValueRef, RowIdRef,
};
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TableName, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;

use common::compile_schema;

const USER_UUID: uuid::Uuid = uuid::uuid!("82000000-0000-0000-0000-000000000001");
const UNUSED_UUID: uuid::Uuid = uuid::uuid!("82000000-0000-0000-0000-000000000002");

fn user() -> AuthorSubject {
    AuthorSubject::for_test_uuid(USER_UUID)
}

fn account_of(author: AuthorSubject) -> uuid::Uuid {
    author.account_id().expect("test author has an account").0
}

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn col(scope: &str, column: &str) -> RelColumnRef {
    RelColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    }
}

fn scan(table: &str, alias: Option<&str>) -> RelExpr {
    RelExpr::TableScan {
        table: TableName::new(table),
        alias: alias.map(str::to_owned),
    }
}

fn eq(left: RelColumnRef, right: RelValueRef) -> RelPredicateExpr {
    RelPredicateExpr::Cmp {
        left,
        op: RelPredicateCmpOp::Eq,
        right,
    }
}

fn literal(value: impl Into<jazz::tools::Value>) -> RelValueRef {
    RelValueRef::Literal(value.into())
}

fn project_id(input: RelExpr, column: RelColumnRef) -> RelExpr {
    RelExpr::Project {
        input: Box::new(input),
        columns: vec![RelProjectColumn {
            alias: "id".to_owned(),
            expr: RelProjectExpr::Column(column),
        }],
    }
}

/// The exact relation IR that the TypeScript permission DSL lowers
/// `doubleRefReproApp`'s dropdown read policy to:
///
/// ```text
/// directTeams = team_entry.where({ user_id: session.user.account }).hopTo("target")
/// reachable   = teams.gather({ start: directTeams,
///                              step: team_entry.where({ team_id: current,
///                                                       administrator: false })
///                                              .hopTo("target"),
///                              maxDepth: 8 })
/// allowRead(dropdown) = exists(reachable.hopTo("dropdowns_access_edgesViaTeam")
///                                .where({ resource_id: dropdown.id,
///                                         grant_role: { in: ["viewer"] },
///                                         administrator: false }))
/// ```
///
/// The seed hops `team_entry.target_id` into `teams`; the recursive step
/// projects the next edge's `target_id` directly as the frontier key. Both
/// scan `team_entry`, the table that holds two refs to `teams`. This is built
/// with the typed public relation IR because the fluent Rust relation builder
/// cannot express a filter above a hop join or an `IN` list.
fn dropdown_read_policy() -> PolicyExpr {
    let seed = project_id(
        RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(scan("team_entry", None)),
                right: Box::new(scan("teams", Some("__hop_0"))),
                on: vec![RelJoinCondition {
                    left: col("team_entry", "target_id"),
                    right: col("__hop_0", "id"),
                }],
                join_kind: RelJoinKind::Inner,
            }),
            predicate: eq(
                col("team_entry", "user_id"),
                RelValueRef::SessionRef(vec!["user".to_owned(), "account".to_owned()]),
            ),
        },
        col("__hop_0", "id"),
    );
    let step = project_id(
        RelExpr::Filter {
            input: Box::new(scan("team_entry", None)),
            predicate: RelPredicateExpr::And(vec![
                eq(col("team_entry", "administrator"), literal(false)),
                eq(
                    col("team_entry", "team_id"),
                    RelValueRef::RowId(RowIdRef::Frontier),
                ),
            ]),
        },
        col("team_entry", "target_id"),
    );
    let reachable = RelExpr::Gather {
        seed: Box::new(seed),
        step: Box::new(step),
        frontier_key: RelKeyRef::RowId(RowIdRef::Current),
        bound: RelRecursionBound::MaxDepth(8),
        dedupe_key: vec![RelKeyRef::RowId(RowIdRef::Current)],
    };
    let grants = RelExpr::Filter {
        input: Box::new(RelExpr::Join {
            left: Box::new(reachable),
            right: Box::new(scan("dropdowns_access_edges", Some("__recursive_join_0"))),
            on: vec![RelJoinCondition {
                left: col("teams", "id"),
                right: col("__recursive_join_0", "team_id"),
            }],
            join_kind: RelJoinKind::Inner,
        }),
        predicate: RelPredicateExpr::And(vec![
            eq(
                col("__recursive_join_0", "resource_id"),
                RelValueRef::OuterColumn(RelColumnRef::unscoped("id")),
            ),
            RelPredicateExpr::In {
                left: col("__recursive_join_0", "grant_role"),
                values: vec![literal("viewer")],
            },
            eq(col("__recursive_join_0", "administrator"), literal(false)),
        ]),
    };
    PolicyExpr::ExistsRel {
        rel: project_id(grants, col("__recursive_join_0", "id")),
    }
}

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("name", ColumnType::Text)
                    .policies(TablePolicies::new()),
            )
            .table(
                TableSchemaBuilder::new("team_entry")
                    .fk_column("team_id", "teams")
                    .fk_column("target_id", "teams")
                    .column("user_id", ColumnType::Uuid)
                    .column("administrator", ColumnType::Boolean)
                    .policies(TablePolicies::new()),
            )
            .table(
                TableSchemaBuilder::new("dropdowns")
                    .column("name", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(dropdown_read_policy())),
            )
            .table(
                TableSchemaBuilder::new("dropdowns_access_edges")
                    .fk_column("resource_id", "dropdowns")
                    .fk_column("team_id", "teams")
                    .column("grant_role", ColumnType::Text)
                    .column("administrator", ColumnType::Boolean)
                    .policies(TablePolicies::new()),
            )
            .build(),
    )
}

fn open_global_node() -> Db {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open_history_complete(DbConfig::new(
        schema,
        TestStorage::new(&family_refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x82; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open history-complete node")
}

fn insert(db: &Db, table: &str, id: RowUuid, cells: Vec<(&str, Value)>) {
    let handle = block_on(
        db.insert(
            table,
            cells
                .into_iter()
                .map(|(column, value)| (column.to_owned(), value))
                .collect::<BTreeMap<_, _>>(),
            InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        ),
    )
    .unwrap_or_else(|error| panic!("insert into {table}: {error:?}"));
    block_on(handle.wait(DurabilityTier::Local)).expect("local settlement");
}

fn local_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

/// The gather seed hops `team_entry.target_id`, and each step hops the next
/// edge's `target_id` from a frontier matched on `team_id`. A grant held by
/// the seed edge's *source* team (`team_id`) is never reachable. Both the
/// one-shot read and the subscription opening must evaluate, not fail.
///
/// Actors: the SYSTEM backend node seeds the graph; alice (`user()`, the
/// member named by the first edge's `user_id`) reads as an admitted session.
///
/// ```text
/// user ─entry(team=User, target=Direct)─► Direct
/// Direct ─entry(team=Direct, target=Nested)─► Nested ─grant(viewer)─► Visible
/// User ─grant(viewer)─► "Not reachable through target_id"
///
/// session read of dropdowns ─► {Visible}
/// ```
#[test]
fn gather_seed_hops_the_target_ref_of_a_same_table_double_ref_edge() {
    let db = open_global_node();
    let (user_team, direct, nested) = (row(0x01), row(0x02), row(0x03));
    let (visible, source_only) = (row(0x11), row(0x12));
    for (id, name) in [(user_team, "User"), (direct, "Direct"), (nested, "Nested")] {
        insert(&db, "teams", id, vec![("name", Value::String(name.into()))]);
    }
    insert(
        &db,
        "dropdowns",
        visible,
        vec![("name", Value::String("Visible".into()))],
    );
    insert(
        &db,
        "dropdowns",
        source_only,
        vec![(
            "name",
            Value::String("Not reachable through target_id".into()),
        )],
    );
    insert(
        &db,
        "team_entry",
        row(0x21),
        vec![
            ("team_id", Value::Uuid(user_team.0)),
            ("target_id", Value::Uuid(direct.0)),
            ("user_id", Value::Uuid(account_of(user()))),
            ("administrator", Value::Bool(false)),
        ],
    );
    insert(
        &db,
        "team_entry",
        row(0x22),
        vec![
            ("team_id", Value::Uuid(direct.0)),
            ("target_id", Value::Uuid(nested.0)),
            (
                "user_id",
                Value::Uuid(account_of(AuthorSubject::for_test_uuid(UNUSED_UUID))),
            ),
            ("administrator", Value::Bool(false)),
        ],
    );
    insert(
        &db,
        "dropdowns_access_edges",
        row(0x31),
        vec![
            ("resource_id", Value::Uuid(visible.0)),
            ("team_id", Value::Uuid(nested.0)),
            ("grant_role", Value::String("viewer".into())),
            ("administrator", Value::Bool(false)),
        ],
    );
    insert(
        &db,
        "dropdowns_access_edges",
        row(0x32),
        vec![
            ("resource_id", Value::Uuid(source_only.0)),
            ("team_id", Value::Uuid(user_team.0)),
            ("grant_role", Value::String("viewer".into())),
            ("administrator", Value::Bool(false)),
        ],
    );

    let prepared = db
        .prepare_query(&Query::from("dropdowns"))
        .expect("prepare dropdowns");
    let one_shot = block_on(db.all_for_identity(&prepared, local_opts(), user()))
        .expect("one-shot session read of dropdowns")
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(one_shot, BTreeSet::from([visible]));

    let mut stream = block_on(db.subscribe_client_for_identity(&prepared, local_opts(), user()))
        .expect("session subscription of dropdowns");
    let opened = match block_on(stream.next_event()).expect("subscription opening") {
        SubscriptionEvent::Delta {
            reset: true, added, ..
        } => added
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        event => panic!("expected an opening reset, got {event:?}"),
    };
    assert_eq!(opened, BTreeSet::from([visible]));
}

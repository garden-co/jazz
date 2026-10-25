//! SQLite port of the retired M3 maintained-versus-one-shot shape matrix.
//! Every shape has its own ordinary replica: another subscription cannot supply
//! missing policy or relation inputs. The authority's fresh query is the oracle.
//! Unsupported runtime shapes must fail; this port does not filter them out.

mod common;

use common::allow_all_policies as public_all_policies;
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, FileOptions, PreparedQuery, ReadOpts, ReadTier,
    RestoreOptions, SubscriptionEvent, SubscriptionStream, UpsertOptions, WriteIdentity, block_on,
    duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Aggregate, ArraySubquery, Query, RelationQuery, claim, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    RelationColumnRef, RelationExpr, RelationJoinCondition, RelationJoinKind,
    RelationProjectColumn, RelationProjectExpr,
};
use jazz::tools::{
    ColumnType as PublicColumnType, Operation, PolicyExpr, SchemaBuilder as PublicSchemaBuilder,
    TableSchemaBuilder as PublicTableSchemaBuilder, Value as PublicValue,
};
use std::collections::{BTreeMap, BTreeSet};

fn build_public_test_schema(builder: PublicSchemaBuilder) -> JazzSchema {
    common::compile_schema(&builder.build())
}
fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}
fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn generated_row(kind: u8, index: u64) -> RowUuid {
    let mut bytes = [kind; 16];
    bytes[8..].copy_from_slice(&index.to_be_bytes());
    RowUuid::from_bytes(bytes)
}
fn claims(author: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([("sub".into(), Value::String(author.principal_parts().1))])
}

struct Fixture {
    db: Db,
    schema: JazzSchema,
    _directory: Option<tempfile::TempDir>,
    // Fixture bookkeeping chooses the public restore API after a deletion.
    // This is not a substitute for any runtime observation or assertion.
    deleted: BTreeSet<(String, RowUuid)>,
}
impl Fixture {
    fn new() -> Self {
        Self::with_storage(true)
    }
    fn with_storage(durable: bool) -> Self {
        let schema = matrix_schema();
        let config = DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: AuthorSubject::SYSTEM,
            },
        );
        let directory = durable.then(|| tempfile::tempdir().unwrap());
        let db = match &directory {
            Some(directory) => block_on(Db::open_at(
                config,
                &directory.path().join("authority.sqlite"),
                FileOptions::default(),
            ))
            .unwrap(),
            None => block_on(Db::open(config)).unwrap(),
        };
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
        for author in [user(0xa1), user(0xb2)] {
            db.set_identity_claims(author, claims(author));
        }
        Self {
            db,
            schema,
            _directory: directory,
            deleted: BTreeSet::new(),
        }
    }
    fn put(&mut self, write: SeedWrite) {
        let key = (write.table.to_owned(), write.id);
        let identity = write
            .author
            .map_or(WriteIdentity::default(), WriteIdentity::Session);
        if self.deleted.contains(&key) {
            block_on(self.db.restore(
                write.table,
                write.id,
                Some(write.cells),
                RestoreOptions {
                    identity,
                    updated_at_ms: Some(write.at),
                    ..Default::default()
                },
            ))
            .unwrap();
            self.deleted.remove(&key);
        } else {
            block_on(self.db.upsert(
                write.table,
                write.id,
                write.cells,
                UpsertOptions {
                    identity,
                    updated_at_ms: Some(write.at),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
    }
    fn delete(&mut self, table: &str, id: RowUuid) {
        let key = (table.to_owned(), id);
        if self.deleted.contains(&key) {
            // Repeated random revocation leaves the same absence. Do not mask
            // unexpected errors from writes to currently live rows.
            return;
        }
        block_on(self.db.delete(table, id, Default::default())).unwrap();
        self.deleted.insert(key);
    }
}
struct SeedWrite {
    table: &'static str,
    id: RowUuid,
    at: u64,
    author: Option<AuthorSubject>,
    cells: BTreeMap<String, Value>,
}
impl SeedWrite {
    fn new(table: &'static str, id: RowUuid, at: u64) -> Self {
        Self {
            table,
            id,
            at,
            author: None,
            cells: BTreeMap::new(),
        }
    }
    fn made_by(mut self, author: AuthorSubject) -> Self {
        self.author = Some(author);
        self
    }
    fn cells(mut self, cells: BTreeMap<String, Value>) -> Self {
        self.cells = cells;
        self
    }
}
fn accept_global(core: &mut Fixture, write: SeedWrite) {
    core.put(write);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AggregateAgreement {
    Exact,
    F64Approx,
}
type AggregateSpec = (&'static str, Aggregate, &'static str, AggregateAgreement);
const F64_AGGREGATE_ERROR_FACTOR: f64 = 64.0;
struct Spec {
    name: &'static str,
    query: Query,
    approximate_output: Option<&'static str>,
}
fn row_specs(seed: u64) -> Vec<Spec> {
    [
        ("docs_plain", Query::from("docs")),
        (
            "docs_filtered",
            Query::from("docs").filter(eq(col("kind"), lit("match"))),
        ),
        (
            "docs_created_by",
            Query::from("docs").filter(eq(col("$createdBy"), claim("user"))),
        ),
        // Preserve the old ordinary createdAt filter; also cover real provenance.
        (
            "docs_created_at",
            Query::from("docs").filter(eq(col("createdAt"), lit(7_u64))),
        ),
        (
            "docs_provenance_created_at",
            Query::from("docs").filter(eq(col("$createdAt"), lit(10_u64 + seed % 3))),
        ),
        (
            "docs_edge_seeded_reachable",
            Query::from("docs")
                .reachable_via(
                    "doc_access",
                    "doc",
                    "team",
                    lit("edge-seed"),
                    "team_edges",
                    "member",
                    "parent",
                    [],
                )
                .seeded_by(
                    "group_access_edges",
                    "user_id",
                    "user.identity.subject",
                    "group_id",
                ),
        ),
        (
            "resources_same_table_seeded_reachable",
            Query::from("resources"),
        ),
        (
            "string_resources_same_table_seeded_reachable",
            Query::from("string_resources"),
        ),
        (
            "children_inherit_doc",
            Query::from("children").inherits("doc"),
        ),
        (
            "grandchildren_inherit_child",
            Query::from("grandchildren").inherits("child"),
        ),
        (
            "docs_projected_with_doc_access",
            Query::from("docs").select(["title"]).array_subquery(
                ArraySubquery::new("access", "doc_access", "doc", "id").select(["team"]),
            ),
        ),
        (
            "docs_relation_facade_direct_access",
            relation_doc_access_shape(),
        ),
    ]
    .into_iter()
    .map(|(name, query)| Spec {
        name,
        query,
        approximate_output: None,
    })
    .collect()
}
fn aggregate_specs() -> Vec<Spec> {
    aggregate_differential_specs()
        .into_iter()
        .map(|(name, aggregate, output, agreement)| Spec {
            name,
            query: Query::from("docs")
                .aggregate([aggregate])
                .group_by("bucket"),
            approximate_output: (agreement == AggregateAgreement::F64Approx).then_some(output),
        })
        .collect()
}

type Cells = BTreeMap<String, Option<Value>>;
type Rows = BTreeMap<RowUuid, Cells>;
fn selected(row: &CurrentRow) -> Cells {
    row.cells()
        .filter(|(name, _)| !name.starts_with('$'))
        .map(|(name, value)| (name.to_owned(), value.cloned()))
        .collect()
}
fn snapshot(rows: &[CurrentRow]) -> Rows {
    rows.iter()
        .map(|row| (row.row_uuid(), selected(row)))
        .collect()
}
fn value(value: &Value) -> &Value {
    match value {
        Value::Nullable(Some(inner)) => self::value(inner),
        _ => value,
    }
}
fn bucket(cells: &Cells) -> u64 {
    match value(cells["bucket"].as_ref().unwrap()) {
        Value::U64(n) => *n,
        other => panic!("bucket must be U64: {other:?}"),
    }
}
fn absolute_sums(db: &Db) -> BTreeMap<u64, f64> {
    let prepared = db
        .prepare_query(&Query::from("docs").select(["bucket", "f64_value"]))
        .unwrap();
    let mut sums = BTreeMap::new();
    for row in block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap() {
        let cells = selected(&row);
        let Value::F64(n) = value(cells["f64_value"].as_ref().unwrap()) else {
            panic!("F64 input")
        };
        *sums.entry(bucket(&cells)).or_default() += n.abs();
    }
    sums
}
fn assert_rows(
    actual: &Rows,
    expected: &Rows,
    spec: &Spec,
    sums: &BTreeMap<u64, f64>,
    updates: u64,
    context: &str,
) {
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "{context}: {} row/group membership",
        spec.name
    );
    for (id, expected_cells) in expected {
        let actual_cells = &actual[id];
        assert_eq!(
            actual_cells.keys().collect::<Vec<_>>(),
            expected_cells.keys().collect::<Vec<_>>(),
            "{context}: {} projected columns",
            spec.name
        );
        for (column, expected_value) in expected_cells {
            let actual_value = &actual_cells[column];
            if spec.approximate_output == Some(column.as_str())
                && let (Some(a), Some(e)) = (actual_value, expected_value)
                && let (Value::F64(a), Value::F64(e)) = (value(a), value(e))
            {
                let sum_abs = sums[&bucket(expected_cells)];
                let tolerance =
                    F64_AGGREGATE_ERROR_FACTOR * f64::EPSILON * (updates as f64 + 4.0) * sum_abs;
                let error = (a - e).abs();
                assert!(
                    a.is_finite() && e.is_finite() && error <= tolerance,
                    "{context}: {} {column} error={error:e} tolerance={tolerance:e} sum_abs={sum_abs:e} actual={a:e} expected={e:e}",
                    spec.name
                );
            } else {
                assert_eq!(
                    actual_value, expected_value,
                    "{context}: {} {column}",
                    spec.name
                );
            }
        }
    }
}

struct Replica {
    spec: Spec,
    db: Db,
    authority_query: PreparedQuery,
    stream: SubscriptionStream,
    rows: Rows,
    settled: bool,
}
struct Oracle {
    replicas: Vec<Replica>,
    updates: u64,
}
impl Oracle {
    fn open(fixture: &Fixture, specs: Vec<Spec>, seed: u64) -> Self {
        let author = user(0xa1);
        let replicas = specs
            .into_iter()
            .enumerate()
            .map(|(offset, spec)| {
                let db = block_on(Db::open(DbConfig::new(
                    fixture.schema.clone(),
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x80 + u8::try_from(offset).unwrap(); 16]),
                        author,
                    },
                )))
                .unwrap();
                db.set_non_durable_client();
                db.set_identity_claims(author, claims(author));
                let (up, down) = duplex();
                block_on(db.connect_upstream(up));
                fixture
                    .db
                    .accept_subscriber_with_claims(down, author, Some(claims(author)));
                let prepared = db
                    .prepare_query(&spec.query)
                    .unwrap_or_else(|e| panic!("seed={seed} {} prepare: {e}", spec.name));
                let authority_query = fixture.db.prepare_query(&spec.query).unwrap();
                let stream = block_on(db.subscribe_for_identity(
                    &prepared,
                    ReadOpts::for_read_tier(ReadTier::Remote),
                    author,
                ))
                .unwrap_or_else(|e| panic!("seed={seed} {} remote open: {e}", spec.name));
                Replica {
                    spec,
                    db,
                    authority_query,
                    stream,
                    rows: BTreeMap::new(),
                    settled: false,
                }
            })
            .collect();
        let mut oracle = Self {
            replicas,
            updates: 0,
        };
        oracle.check(fixture, seed, "initial");
        oracle
    }
    fn check(&mut self, fixture: &Fixture, seed: u64, checkpoint: &str) {
        // Normal semantic transport only; no injected row images or copied stores.
        for _ in 0..32 {
            for replica in &self.replicas {
                block_on(replica.db.tick()).unwrap();
            }
            block_on(fixture.db.tick()).unwrap();
        }
        let sums = absolute_sums(&fixture.db);
        for replica in &mut self.replicas {
            block_on(replica.db.tick()).unwrap();
            while let Some(event) = replica.stream.try_next_event() {
                let SubscriptionEvent::Delta {
                    reset,
                    added,
                    updated,
                    removed,
                    settled,
                    tier,
                    ..
                } = event
                else {
                    panic!(
                        "seed={seed} {checkpoint}: {} unexpected event {event:?}",
                        replica.spec.name
                    );
                };
                if settled {
                    assert_eq!(
                        tier,
                        jazz::tx::DurabilityTier::Global,
                        "authority settlement"
                    );
                    replica.settled = true;
                }
                if reset {
                    replica.rows.clear();
                }
                for removed in removed {
                    replica.rows.remove(&removed.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    replica.rows.insert(row.row_uuid(), selected(&row));
                }
            }
            let context = format!("seed={seed} {checkpoint}");
            assert!(
                replica.settled,
                "{context}: {} never settled",
                replica.spec.name
            );
            let expected = snapshot(
                &block_on(fixture.db.all_for_identity(
                    &replica.authority_query,
                    ReadOpts::for_read_tier(ReadTier::LocalOnly),
                    user(0xa1),
                ))
                .unwrap(),
            );
            assert_rows(
                &replica.rows,
                &expected,
                &replica.spec,
                &sums,
                self.updates,
                &context,
            );
        }
    }
}

fn matrix_schema() -> JazzSchema {
    let same_table_policy = jazz_model::test_public_schema::seeded_recursive_access_policy(
        "resource_access",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "team_edges",
        "member",
        "parent",
        &[("administrator", PublicValue::Boolean(false))],
        "teams",
        "identity_key",
        &["claims", "sub"],
        "id",
    );
    let string_same_table_policy = jazz_model::test_public_schema::seeded_recursive_access_policy(
        "string_resource_access",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "team_edges",
        "member",
        "parent",
        &[("administrator", PublicValue::Boolean(false))],
        "teams",
        "identity_key_text",
        &["claims", "sub"],
        "id",
    );

    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .column("kind", PublicColumnType::Text)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .column("bucket", PublicColumnType::Timestamp)
                    .column("f64_value", PublicColumnType::Double)
                    .nullable_column("nullable_f64_value", PublicColumnType::Double)
                    .column("i64_value", PublicColumnType::BigInt)
                    .column("u64_value", PublicColumnType::Timestamp)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("doc", "docs")
                    .column("status", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "doc".into(),
                        max_depth: Some(2),
                    })),
            )
            .table(
                PublicTableSchemaBuilder::new("grandchildren")
                    .fk_column("child", "children")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("teams")
                    .column("name", PublicColumnType::Text)
                    .column("identity_key", PublicColumnType::Text)
                    .column("identity_key_text", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("team_edges")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams")
                    .column("administrator", PublicColumnType::Boolean)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("group_access_edges")
                    .column("user_id", PublicColumnType::Text)
                    .fk_column("group_id", "teams")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("doc_access")
                    .fk_column("doc", "docs")
                    .fk_column("team", "teams")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("label", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(same_table_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("string_resources")
                    .column("label", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(string_same_table_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("resource_access")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams")
                    .column("administrator", PublicColumnType::Boolean)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("string_resource_access")
                    .fk_column("resource", "string_resources")
                    .fk_column("team", "teams")
                    .column("administrator", PublicColumnType::Boolean)
                    .policies(public_all_policies()),
            ),
    )
}

fn aggregate_differential_specs() -> Vec<AggregateSpec> {
    use jazz::query::Aggregate;

    vec![
        (
            "docs_count_by_bucket",
            Aggregate::count(),
            "count",
            AggregateAgreement::Exact,
        ),
        (
            "docs_f64_sum_by_bucket",
            Aggregate::sum("f64_value"),
            "sum_f64_value",
            AggregateAgreement::F64Approx,
        ),
        (
            "docs_f64_avg_by_bucket",
            Aggregate::avg("f64_value"),
            "avg_f64_value",
            AggregateAgreement::F64Approx,
        ),
        (
            "docs_f64_min_by_bucket",
            Aggregate::min("f64_value"),
            "min_f64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_f64_max_by_bucket",
            Aggregate::max("f64_value"),
            "max_f64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_sum_by_bucket",
            Aggregate::sum("i64_value"),
            "sum_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_avg_by_bucket",
            Aggregate::avg("i64_value"),
            "avg_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_min_by_bucket",
            Aggregate::min("i64_value"),
            "min_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_max_by_bucket",
            Aggregate::max("i64_value"),
            "max_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_sum_by_bucket",
            Aggregate::sum("u64_value"),
            "sum_u64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_avg_by_bucket",
            Aggregate::avg("u64_value"),
            "avg_u64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_min_by_bucket",
            Aggregate::min("u64_value"),
            "min_u64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_max_by_bucket",
            Aggregate::max("u64_value"),
            "max_u64_value",
            AggregateAgreement::Exact,
        ),
    ]
}

fn relation_doc_access_shape() -> Query {
    jazz::query::relation_query_to_query(&RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "docs".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "doc_access".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("docs".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "doc".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("docs".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("docs".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
            ],
        },
    })
    .expect("single-hop relation facade should normalize")
}

fn seed_base(core: &mut Fixture, seed: u64) {
    let alice = user(0xa1);
    let bob = user(0xb2);
    for (team, name, identity) in [
        (row(0x31), "alice-direct", alice),
        (row(0x32), "alice-parent", bob),
        (row(0x33), "bob", bob),
    ] {
        accept_global(
            core,
            SeedWrite::new("teams", team, 1).cells(BTreeMap::from([
                ("name".to_owned(), Value::String(name.to_owned())),
                (
                    "identity_key".to_owned(),
                    Value::String(identity.principal_parts().1),
                ),
                (
                    "identity_key_text".to_owned(),
                    Value::String(identity.principal_parts().1),
                ),
            ])),
        );
    }
    accept_global(
        core,
        team_edge_commit(row(0x41), row(0x31), row(0x32), false, 2),
    );
    accept_global(
        core,
        SeedWrite::new("group_access_edges", row(0x42), 3).cells(BTreeMap::from([
            (
                "user_id".to_owned(),
                Value::String(alice.principal_parts().1),
            ),
            ("group_id".to_owned(), Value::Uuid(row(0x31).0)),
        ])),
    );

    for (
        doc,
        title,
        kind,
        author,
        created_at,
        bucket,
        f64_value,
        nullable_f64_value,
        i64_value,
        u64_value,
    ) in [
        (
            row(0x11),
            "visible-direct",
            "match",
            alice,
            7,
            1,
            1.5,
            Some(1.5),
            -11,
            11,
        ),
        (
            row(0x12),
            "visible-transitive",
            "match",
            alice,
            7,
            1,
            -0.25,
            None,
            7,
            19,
        ),
        (
            row(0x13),
            "hidden",
            "match",
            bob,
            8,
            2,
            3.0,
            Some(3.0),
            -5,
            5,
        ),
        (
            row(0x14),
            "filtered-out",
            "other",
            alice,
            9,
            2,
            0.75,
            None,
            13,
            23,
        ),
    ] {
        accept_global(
            core,
            SeedWrite::new("docs", doc, 10 + seed % 3)
                .made_by(author)
                .cells(differential_doc_cells(
                    title,
                    kind,
                    author,
                    created_at,
                    bucket,
                    f64_value,
                    nullable_f64_value,
                    i64_value,
                    u64_value,
                )),
        );
    }
    for (edge, doc, team) in [
        (row(0x51), row(0x11), row(0x31)),
        (row(0x52), row(0x12), row(0x32)),
        (row(0x53), row(0x13), row(0x33)),
    ] {
        accept_global(
            core,
            SeedWrite::new("doc_access", edge, 20).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    for (resource, label) in [
        (row(0x61), "direct"),
        (row(0x62), "transitive"),
        (row(0x63), "hidden"),
    ] {
        accept_global(
            core,
            SeedWrite::new("resources", resource, 30).cells(BTreeMap::from([(
                "label".to_owned(),
                Value::String(label.to_owned()),
            )])),
        );
    }
    for (edge, resource, team) in [
        (row(0x64), row(0x61), row(0x31)),
        (row(0x65), row(0x62), row(0x32)),
        (row(0x66), row(0x63), row(0x33)),
    ] {
        accept_global(
            core,
            SeedWrite::new("resource_access", edge, 31).cells(BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
                ("administrator".to_owned(), Value::Bool(false)),
            ])),
        );
    }
    for (resource, label) in [
        (row(0x67), "string-direct"),
        (row(0x68), "string-transitive"),
        (row(0x69), "string-hidden"),
    ] {
        accept_global(
            core,
            SeedWrite::new("string_resources", resource, 32).cells(BTreeMap::from([(
                "label".to_owned(),
                Value::String(label.to_owned()),
            )])),
        );
    }
    for (edge, resource, team) in [
        (row(0x6a), row(0x67), row(0x31)),
        (row(0x6b), row(0x68), row(0x32)),
        (row(0x6c), row(0x69), row(0x33)),
    ] {
        accept_global(
            core,
            SeedWrite::new("string_resource_access", edge, 33).cells(BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
                ("administrator".to_owned(), Value::Bool(false)),
            ])),
        );
    }
    accept_global(
        core,
        SeedWrite::new("children", row(0x71), 40).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(0x11).0)),
            ("status".to_owned(), Value::String("open".to_owned())),
        ])),
    );
    accept_global(
        core,
        SeedWrite::new("grandchildren", row(0x81), 41).cells(BTreeMap::from([(
            "child".to_owned(),
            Value::Uuid(row(0x71).0),
        )])),
    );
}

#[allow(clippy::too_many_arguments)]
fn differential_doc_cells(
    title: &str,
    kind: &str,
    _author: AuthorSubject,
    created_at: u64,
    bucket: u64,
    f64_value: f64,
    nullable_f64_value: Option<f64>,
    i64_value: i64,
    u64_value: u64,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("kind".to_owned(), Value::String(kind.to_owned())),
        ("createdAt".to_owned(), Value::U64(created_at)),
        ("bucket".to_owned(), Value::U64(bucket)),
        ("f64_value".to_owned(), Value::F64(f64_value)),
        (
            "nullable_f64_value".to_owned(),
            Value::Nullable(nullable_f64_value.map(|value| Box::new(Value::F64(value)))),
        ),
        ("i64_value".to_owned(), Value::I64(i64_value)),
        ("u64_value".to_owned(), Value::U64(u64_value)),
    ])
}

fn team_edge_commit(
    row_uuid: RowUuid,
    member: RowUuid,
    parent: RowUuid,
    administrator: bool,
    made_at: u64,
) -> SeedWrite {
    SeedWrite::new("team_edges", row_uuid, made_at).cells(BTreeMap::from([
        ("member".to_owned(), Value::Uuid(member.0)),
        ("parent".to_owned(), Value::Uuid(parent.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ]))
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    match std::env::var(name) {
        Ok(text) => text
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a u64")),
        Err(std::env::VarError::NotPresent) => fallback,
        Err(error) => panic!("{name}: {error}"),
    }
}
fn seeds() -> Vec<u64> {
    if std::env::var_os("JAZZ_SEED").is_some() {
        return vec![env_u64("JAZZ_SEED", 0)];
    }
    let mut result = vec![11, 29, 47, 4_372_288, 7_777_013];
    let count = env_u64("JAZZ_SEED_COUNT", result.len() as u64).max(result.len() as u64);
    result.extend((0..count - result.len() as u64).map(|i| 9_000 + i * 7_919));
    result
}
fn churn_depths() -> Vec<u64> {
    let text =
        std::env::var("JAZZ_DIFFERENTIAL_CHURN_DEPTHS").unwrap_or_else(|_| "10,1000,100000".into());
    let depths: Vec<u64> = text
        .split(',')
        .map(|part| part.parse().expect("positive u64 churn depth"))
        .collect();
    assert!(
        !depths.is_empty() && depths[0] > 0 && depths.windows(2).all(|pair| pair[0] < pair[1]),
        "churn depths must be positive and strictly increasing"
    );
    depths
}
struct Rng(u64);
impl Rng {
    fn next(&mut self, count: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 % count
    }
}
fn mutate(fixture: &mut Fixture, operation: u64, step: u64) {
    match operation {
        0 => {
            let id = generated_row(0x90, step);
            fixture.put(
                SeedWrite::new("docs", id, 100 + step)
                    .made_by(user(0xa1))
                    .cells(differential_doc_cells(
                        "added",
                        "match",
                        user(0xa1),
                        7,
                        1,
                        0.5,
                        Some(0.5),
                        3,
                        3,
                    )),
            );
            fixture.put(
                SeedWrite::new("doc_access", generated_row(0xa0, step), 120 + step).cells(
                    BTreeMap::from([
                        ("doc".into(), Value::Uuid(id.0)),
                        ("team".into(), Value::Uuid(row(0x31).0)),
                    ]),
                ),
            );
        }
        1 => fixture.put(
            SeedWrite::new("docs", generated_row(0xb0, step), 140 + step)
                .made_by(user(0xb2))
                .cells(differential_doc_cells(
                    "hidden-added",
                    "match",
                    user(0xb2),
                    8,
                    2,
                    -0.5,
                    None,
                    -3,
                    5,
                )),
        ),
        2 => fixture.delete("group_access_edges", row(0x42)),
        3 => fixture.put(
            SeedWrite::new("group_access_edges", row(0x42), 180 + step).cells(BTreeMap::from([
                (
                    "user_id".into(),
                    Value::String(user(0xa1).principal_parts().1),
                ),
                ("group_id".into(), Value::Uuid(row(0x31).0)),
            ])),
        ),
        4 => fixture.delete("children", row(0x71)),
        5 => fixture.put(
            SeedWrite::new("children", row(0x71), 220 + step).cells(BTreeMap::from([
                ("doc".into(), Value::Uuid(row(0x11).0)),
                ("status".into(), Value::String("open".into())),
            ])),
        ),
        6 => fixture.put(SeedWrite::new("docs", row(0x14), 240 + step).cells(
            differential_doc_cells(
                "filtered-now-created-at",
                "match",
                user(0xa1),
                7,
                2,
                0.75,
                None,
                13,
                23,
            ),
        )),
        _ => unreachable!(),
    }
}
fn run_seed(seed: u64) {
    let mut fixture = Fixture::new();
    seed_base(&mut fixture, seed);
    let mut specs = row_specs(seed);
    specs.extend(aggregate_specs());
    let mut oracle = Oracle::open(&fixture, specs, seed);
    let mut rng = Rng(seed ^ 0x9e37_79b9);
    let steps = env_u64("JAZZ_DIFFERENTIAL_STEP_COUNT", 3);
    assert!(steps > 0, "the differential corpus must exercise writes");
    for step in 0..steps {
        let operation = rng.next(7);
        mutate(&mut fixture, operation, step);
        oracle.updates += 1;
        oracle.check(
            &fixture,
            seed,
            &format!("step={step} operation={operation}"),
        );
    }
    // Ensure the named two-level inheritance shape actually loses a grandchild
    // when its grandparent disappears, even if the random child ended deleted.
    fixture.put(
        SeedWrite::new("children", row(0x71), 1000 + steps).cells(BTreeMap::from([
            ("doc".into(), Value::Uuid(row(0x11).0)),
            ("status".into(), Value::String("open".into())),
        ])),
    );
    oracle.updates += 1;
    oracle.check(&fixture, seed, "inheritance-child-present");
    fixture.delete("docs", row(0x11));
    oracle.updates += 1;
    oracle.check(&fixture, seed, "inheritance-grandparent-deleted");
    fixture.put(
        SeedWrite::new("docs", row(0x11), 1001 + steps)
            .made_by(user(0xa1))
            .cells(differential_doc_cells(
                "visible-direct",
                "match",
                user(0xa1),
                7,
                1,
                1.5,
                Some(1.5),
                -11,
                11,
            )),
    );
    oracle.updates += 1;
    oracle.check(&fixture, seed, "inheritance-grandparent-restored");
}
fn run_churn() {
    let mut fixture = Fixture::with_storage(false);
    for (id, f64_value, nullable, i64_value, u64_value) in [
        (row(0x11), 1.0e16, Some(1.0e16), -11, 11),
        (row(0x12), -1.0e16, Some(-1.0e16), 7, 19),
        (row(0x14), 0.1, None, 13, 23),
    ] {
        fixture.put(
            SeedWrite::new("docs", id, 400).cells(differential_doc_cells(
                "churn-cancellation",
                "match",
                user(0xa1),
                7,
                1,
                f64_value,
                nullable,
                i64_value,
                u64_value,
            )),
        );
    }
    let specs = aggregate_specs()
        .into_iter()
        .filter(|spec| spec.approximate_output.is_some())
        .collect();
    let mut oracle = Oracle::open(&fixture, specs, 0);
    let mut previous = 0;
    for depth in churn_depths() {
        for operation in previous + 1..=depth {
            let id = generated_row(0xfe, (operation - 1) / 3);
            match operation % 3 {
                0 => fixture.delete("docs", id),
                phase => fixture.put(SeedWrite::new("docs", id, 1000 + operation).cells(
                    differential_doc_cells(
                        if phase == 1 {
                            "churn-transient"
                        } else {
                            "churn-transient-updated"
                        },
                        "match",
                        user(0xa1),
                        7,
                        1,
                        if phase == 1 { 0.3 } else { 0.4 },
                        Some(0.3),
                        1,
                        1,
                    ),
                )),
            }
        }
        oracle.updates = depth;
        oracle.check(&fixture, 0, &format!("churn={depth}"));
        previous = depth;
    }
}
fn numeric_sums(rows: &Rows) -> BTreeMap<u64, f64> {
    rows.values()
        .filter_map(|cells| {
            match cells
                .get("sum_nullable_f64_value")
                .and_then(Option::as_ref)
                .map(value)
            {
                Some(Value::F64(number)) => Some((bucket(cells), *number)),
                Some(Value::Nullable(None)) | None => None,
                other => panic!("numeric or NULL aggregate expected: {other:?}"),
            }
        })
        .collect()
}
fn run_null_semantics() {
    let mut fixture = Fixture::new();
    seed_base(&mut fixture, 0);
    fixture.put(
        SeedWrite::new("docs", row(0x15), 500).cells(differential_doc_cells(
            "all-null",
            "match",
            user(0xa1),
            7,
            3,
            4.0,
            None,
            4,
            4,
        )),
    );
    let mut oracle = Oracle::open(
        &fixture,
        vec![Spec {
            name: "docs_nullable_sum_by_bucket",
            query: Query::from("docs")
                .sum("nullable_f64_value")
                .group_by("bucket"),
            approximate_output: None,
        }],
        0,
    );
    assert!(
        !numeric_sums(&oracle.replicas[0].rows).contains_key(&3),
        "all-NULL group has no numeric value"
    );
    fixture.delete("docs", row(0x11));
    oracle.updates += 1;
    oracle.check(&fixture, 0, "nullable-to-all-null");
    assert!(
        !numeric_sums(&oracle.replicas[0].rows).contains_key(&1),
        "group becoming all-NULL loses its numeric value"
    );
    for id in [row(0x12), row(0x13), row(0x14)] {
        fixture.delete("docs", id);
    }
    oracle.updates += 3;
    oracle.check(&fixture, 0, "nullable-to-empty");
    assert!(
        !numeric_sums(&oracle.replicas[0].rows).contains_key(&2),
        "empty group has no numeric value"
    );
}

/// Alice's isolated replicas maintain every retired M3 row/aggregate shape
/// while the authority accepts seeded writes, grant changes and deletions.
/// Each settled result must agree with a fresh authority read as Alice.
///
/// ```text
/// seeded writes -> authority -> normal sync -> one replica per shape
///                      |                             |
///                  fresh query <--- compare ---> maintained result
/// ```
#[test]
#[ignore = "manual randomized matrix; the gate must select it with --ignored --exact"]
fn sqlite_maintained_one_shot_differential_oracle() {
    run_churn();
    run_null_semantics();
    for seed in seeds() {
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_seed(seed)))
        {
            eprintln!(
                "SQLITE DIFFERENTIAL SEED FAILED: {seed}; replay with JAZZ_SEED={seed}, preserving JAZZ_DIFFERENTIAL_STEP_COUNT and JAZZ_DIFFERENTIAL_CHURN_DEPTHS"
            );
            std::panic::resume_unwind(payload);
        }
    }
}

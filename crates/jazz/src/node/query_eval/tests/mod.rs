//! Query-evaluation tests that exercise several pipeline stages together.

use super::*;
use crate::legacy_test_future::{ResultFutureExt as _, SettledNodeTestExt as _};

mod authorization;
mod bindings;
mod execution;
mod lowering;
mod maintained_views;
mod materialization;
mod normalization;
mod read_sources;
mod subscriptions;

use std::collections::{BTreeMap, BTreeSet};

use groove::schema::{ColumnSchema, ColumnType};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};

use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
use crate::node::query_engine::{CoverageScope, FieldRequirement, ProgramFactOutput};
use crate::node::{MergeableCommit, NodeState};
use crate::peer::PeerState;
use crate::protocol::{
    CurrentWriteSchema, MigrationLens, ReadViewSpec, RealRowMemberEntry, RegisterShapeOptions,
    RelationEdgeEntry, ResultRowLayer, RowVersionRefEntry, SchemaVersion, ShapeAst, Subscribe,
    SyncMessage, TableLens,
};
use crate::query::{
    Aggregate, ArraySubquery, JoinSourceLookup, OrderDirection, Query, claim, col, eq, gt, in_list,
    lit, lte, param, table,
};
use crate::schema::{JazzSchema, TableSchema};
use crate::tools::public_schema::{
    CmpOp as PublicCmpOp, PolicyValue as PublicPolicyValue, RelColumnRef as PublicRelColumnRef,
    RelExpr as PublicRelExpr, RelJoinCondition as PublicRelJoinCondition,
    RelJoinKind as PublicRelJoinKind, RelKeyRef as PublicRelKeyRef,
    RelPredicateCmpOp as PublicRelPredicateCmpOp, RelPredicateExpr as PublicRelPredicateExpr,
    RelProjectColumn as PublicRelProjectColumn, RelProjectExpr as PublicRelProjectExpr,
    RelRecursionBound as PublicRelRecursionBound, RelValueRef as PublicRelValueRef,
    RowIdRef as PublicRelRowIdRef,
};
use crate::tools::{
    ColumnType as PublicColumnType, PolicyExpr as PublicPolicyExpr,
    SchemaBuilder as PublicSchemaBuilder, TablePolicies as PublicTablePolicies,
    TableSchemaBuilder as PublicTableSchemaBuilder,
};

/// A coalesced authority re-entry for Alice's document must replace only
/// that exact member; Bob's ordinary content update in the same batch must
/// retain update semantics.
///
/// authority ──re-admit alice──► replacement set
/// bob ──content update─────────► ordinary add (not replacement)
/// A real settled Edge ViewUpdate seeds the client's authority membership;
/// a later local content version of that occurrence remains an update when
/// the ClientLocal maintained graph drains.
///
/// server ──ViewUpdate(issue v1)──► client authority
/// client ──title v2──────────────► one maintained drain
// This lowerer-level assertion protects the case-local descriptor lookup;
// public one-shot and maintained behavior is exercised by the enum query
// integration coverage.
fn collect_binding_source_descriptor_fields(
    graph: &GraphBuilder,
    descriptors_by_shape: &mut BTreeMap<String, BTreeSet<BTreeSet<String>>>,
) {
    match graph {
        GraphBuilder::BindingSource { shape, output } => {
            let fields = output
                .fields()
                .iter()
                .map(|field| field.name.clone().expect("binding fields are named"))
                .collect();
            descriptors_by_shape
                .entry(shape.clone())
                .or_default()
                .insert(fields);
        }
        GraphBuilder::Recursive { seed, step, .. } => {
            collect_binding_source_descriptor_fields(seed, descriptors_by_shape);
            collect_binding_source_descriptor_fields(step, descriptors_by_shape);
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. }
        | GraphBuilder::StreamingChecksum { input, .. } => {
            collect_binding_source_descriptor_fields(input, descriptors_by_shape);
        }
        GraphBuilder::Union { inputs } => {
            for input in inputs {
                collect_binding_source_descriptor_fields(input, descriptors_by_shape);
            }
        }
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            collect_binding_source_descriptor_fields(left, descriptors_by_shape);
            collect_binding_source_descriptor_fields(right, descriptors_by_shape);
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => {}
    }
}

fn collect_binding_source_projected_fields(
    graph: &GraphBuilder,
    projected_by_shape: &mut BTreeMap<String, BTreeSet<BTreeSet<String>>>,
) {
    match graph {
        GraphBuilder::Project { input, fields } => {
            if let GraphBuilder::BindingSource { shape, .. } = input.as_ref() {
                projected_by_shape.entry(shape.clone()).or_default().insert(
                    fields
                        .iter()
                        .map(|field| field.output_name.clone())
                        .collect(),
                );
            }
            collect_binding_source_projected_fields(input, projected_by_shape);
        }
        GraphBuilder::Recursive { seed, step, .. } => {
            collect_binding_source_projected_fields(seed, projected_by_shape);
            collect_binding_source_projected_fields(step, projected_by_shape);
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. }
        | GraphBuilder::StreamingChecksum { input, .. } => {
            collect_binding_source_projected_fields(input, projected_by_shape);
        }
        GraphBuilder::Union { inputs } => {
            for input in inputs {
                collect_binding_source_projected_fields(input, projected_by_shape);
            }
        }
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            collect_binding_source_projected_fields(left, projected_by_shape);
            collect_binding_source_projected_fields(right, projected_by_shape);
        }
        GraphBuilder::BindingSource { .. }
        | GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => {}
    }
}

fn register_query_shape(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    opts: RegisterShapeOptions,
) {
    node.apply_sync_message_settled(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: ShapeAst::from_validated(shape),
        opts,
    })
    .unwrap();
}

fn subscribe_query_binding(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    subscribe_query_binding_with_opts(node, shape, binding, RegisterShapeOptions::default());
}

fn subscribe_query_binding_with_opts(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: RegisterShapeOptions,
) {
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    node.apply_sync_message_settled(SyncMessage::Subscribe(Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        },
        values,
        known_state: None,
        delegated_session: None,
    }))
    .unwrap();
}

fn register_shape_binding_for_receiver(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    register_query_shape(node, shape, RegisterShapeOptions::default());
    subscribe_query_binding(node, shape, binding);
}

fn lowered_current_app_rows_graph(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
    read_view: &ReadViewSpec,
) -> GraphBuilder {
    let program = node
        .compile_current_query_program_for_read_view(
            shape,
            binding,
            DurabilityTier::Local,
            identity,
            CurrentQueryProgramOutput::AppRows,
            read_view,
        )
        .expect("compile current query program");
    lowered_app_rows_graph(&program).expect("app rows graph")
}

fn schema() -> JazzSchema {
    public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("issues")
                    .column("title", PublicColumnType::Text)
                    .column("state", PublicColumnType::Text)
                    .fk_column("assignee", "users")
                    .column("priority", PublicColumnType::Timestamp),
            )
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("issue_members")
                    .fk_column("issue", "issues")
                    .fk_column("user", "users"),
            ),
    )
}

fn signed_metric_schema() -> JazzSchema {
    public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("metrics")
                .column("bucket", PublicColumnType::Text)
                .column("score", PublicColumnType::BigInt),
        ),
    )
}

fn owner_policy_schema() -> JazzSchema {
    public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("issues")
                .column("title", PublicColumnType::Text)
                .column("assignee", PublicColumnType::Uuid)
                .column("requiresAdmin", PublicColumnType::Boolean)
                .policies(
                    PublicTablePolicies::new().with_select(PublicPolicyExpr::eq_session(
                        "assignee",
                        vec!["claims".to_owned(), "sub".to_owned()],
                    )),
                ),
        ),
    )
}

fn public_query_eval_schema(builder: PublicSchemaBuilder) -> JazzSchema {
    crate::schema::JazzSchema::new(&builder.build())
        .expect("query-eval test public schema compiles")
}

fn public_claim_eq(column: &str, claim: &str) -> PublicPolicyExpr {
    PublicPolicyExpr::eq_session(column, vec!["claims".to_owned(), claim.to_owned()])
}

fn public_outer_exists(
    table: &str,
    join_column: &str,
    outer_column: &str,
    additional_conditions: impl IntoIterator<Item = PublicPolicyExpr>,
) -> PublicPolicyExpr {
    let mut conditions = vec![PublicPolicyExpr::Cmp {
        column: join_column.to_owned(),
        op: PublicCmpOp::Eq,
        value: PublicPolicyValue::SessionRef(vec![
            "__jazz_outer_row".to_owned(),
            outer_column.to_owned(),
        ]),
    }];
    conditions.extend(additional_conditions);
    PublicPolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PublicPolicyExpr::And(conditions)),
    }
}

fn public_seeded_recursive_access_policy(seed_claim: &str) -> PublicPolicyExpr {
    let column = |scope: &str, column: &str| PublicRelColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    };
    let eq =
        |scope: &str, column_name: &str, right: PublicRelValueRef| PublicRelPredicateExpr::Cmp {
            left: column(scope, column_name),
            op: PublicRelPredicateCmpOp::Eq,
            right,
        };
    let seed = PublicRelExpr::Project {
        input: Box::new(PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::TableScan {
                table: "teamSeeds".into(),
                alias: Some("seed".to_owned()),
            }),
            predicate: eq(
                "seed",
                "user",
                PublicRelValueRef::SessionRef(vec!["claims".to_owned(), seed_claim.to_owned()]),
            ),
        }),
        columns: vec![PublicRelProjectColumn {
            alias: "id".to_owned(),
            expr: PublicRelProjectExpr::Column(column("seed", "team")),
        }],
    };
    let step = PublicRelExpr::Project {
        input: Box::new(PublicRelExpr::Join {
            left: Box::new(PublicRelExpr::Filter {
                input: Box::new(PublicRelExpr::TableScan {
                    table: "teamMemberships".into(),
                    alias: Some("edge".to_owned()),
                }),
                predicate: eq(
                    "edge",
                    "member",
                    PublicRelValueRef::RowId(PublicRelRowIdRef::Frontier),
                ),
            }),
            right: Box::new(PublicRelExpr::TableScan {
                table: "teams".into(),
                alias: Some("team".to_owned()),
            }),
            on: vec![PublicRelJoinCondition {
                left: column("edge", "parent"),
                right: column("team", "id"),
            }],
            join_kind: PublicRelJoinKind::Inner,
        }),
        columns: vec![PublicRelProjectColumn {
            alias: "id".to_owned(),
            expr: PublicRelProjectExpr::Column(column("team", "id")),
        }],
    };
    let reachable = PublicRelExpr::Gather {
        seed: Box::new(seed),
        step: Box::new(step),
        frontier_key: PublicRelKeyRef::RowId(PublicRelRowIdRef::Current),
        bound: PublicRelRecursionBound::MaxDepth(8),
        dedupe_key: vec![PublicRelKeyRef::RowId(PublicRelRowIdRef::Current)],
    };
    PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(reachable),
                right: Box::new(PublicRelExpr::TableScan {
                    table: "resourceAccess".into(),
                    alias: Some("access".to_owned()),
                }),
                on: vec![PublicRelJoinCondition {
                    left: PublicRelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: column("access", "team"),
                }],
                join_kind: PublicRelJoinKind::Inner,
            }),
            predicate: eq(
                "access",
                "resource",
                PublicRelValueRef::RowId(PublicRelRowIdRef::Outer),
            ),
        },
    }
}

fn open_node() -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let schema = schema();
    open_node_with_uuid(NodeUuid::from_bytes([9; 16]), schema)
}

fn open_node_with_uuid(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .expect("open rocksdb");
    let node = NodeState::new(node_uuid, schema, storage).expect("node");
    (temp_dir, node)
}

/// Stores a version in the non-base schema partition. The extra `body`
/// cell makes using the base history descriptor observably wrong at the
/// native row-batch boundary.
fn evolved_todos_version() -> (
    tempfile::TempDir,
    NodeState<RocksDbStorage>,
    TableSchema,
    RowUuid,
    TxId,
) {
    let base = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text)),
    );
    let evolved_schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text),
        ),
    );
    let evolved_todos = evolved_schema.tables[0].clone();
    let evolved_payload = SchemaVersion::new(evolved_schema);
    let (dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xe1; 16]), base.clone());
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(
            node.author_schema_lineage_publication(
                evolved_payload.clone(),
                MigrationLens::new(
                    base.version_id(),
                    evolved_payload.id,
                    vec![TableLens {
                        source_table: "todos".to_owned(),
                        target_table: "todos".to_owned(),
                        ops: vec![LensOp::AddColumn {
                            column: "body".to_owned(),
                            default: Value::String("base-default".to_owned()),
                        }],
                    }],
                )
                .expect("valid migration lens"),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap(),
        ),
    })
    .unwrap();
    node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    let todo = row(0xe2);
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", todo, 0xe3).cells(BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("partition-title".to_owned()),
                ),
                (
                    "body".to_owned(),
                    Value::String("partition-body".to_owned()),
                ),
            ])),
        )
        .unwrap();
    (dir, node, evolved_todos, todo, tx_id)
}

/// Proves that an authoritative relation-edge witness is rendered through
/// the subscriber's newer read lens, rather than decoding the old
/// authored record with the new descriptor.
///
/// alice(v1 row) ──relation witness──► bob(v2 read lens)
///                                  └──► v2 defaulted cell
///
/// This is deliberately an internal seam test: the production relation
/// snapshot receives only an exact `(table, row, tx)` witness here; the
/// broader client/edge scenario remains the black-box catalogue test.
/// Proves that a remote authority reset renders an old joined target in
/// the subscription schema, including table rename and added-column lens
/// operations.
///
/// alice(v1 users row) ──authority reset witness──► bob(v2 people read)
///                                           └──► renamed table + default
///
/// The direct seam is necessary because this reset path receives only the
/// authority's exact relation-edge version tuple; end-to-end delivery is
/// covered by the catalogue integration scenarios.
/// A v2 flat join must correlate the lens-projected v1 post and author
/// cells, rather than only materializing each source independently.
///
/// alice ──v1 users/posts──► node ──users→people lens──► v2 flat join
/// A canonical relation witness can name a branch-only v1 row. Projection
/// and reset materialization must honor its branch discriminator, lens the
/// old `users` table to `people`, and never substitute root history.
fn recursive_schema() -> JazzSchema {
    public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("teams").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("resources").column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("teamSeeds")
                    .fk_column("team", "teams")
                    .column("kind", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("resourceAccess")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("teamTeamMemberships")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams")
                    .column("onlyAdmins", PublicColumnType::Boolean),
            ),
    )
}

fn open_recursive_node() -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    open_node_with_uuid(NodeUuid::from_bytes([9; 16]), recursive_schema())
}

fn missing_session_seed_policy_schema() -> JazzSchema {
    public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("teams").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("name", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(public_seeded_recursive_access_policy("session_id")),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("teamSeeds")
                    .fk_column("team", "teams")
                    .column("user", PublicColumnType::Uuid),
            )
            .table(
                PublicTableSchemaBuilder::new("resourceAccess")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("teamMemberships")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams"),
            ),
    )
}

fn row(idx: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn commit_global_cells(
    node: &mut NodeState<RocksDbStorage>,
    table: &str,
    row_uuid: RowUuid,
    cells: BTreeMap<String, Value>,
    now_ms: u64,
    global_time: u64,
) -> TxId {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new(table, row_uuid, now_ms)
                .made_by(AuthorSubject::SYSTEM)
                .cells(cells),
        )
        .expect("commit row");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(global_time)),
        Some(DurabilityTier::Global),
    )
    .expect("accept row");
    tx_id
}

fn current_titles(
    table: &TableSchema,
    rows: impl IntoIterator<Item = CurrentRow>,
) -> BTreeMap<RowUuid, Value> {
    rows.into_iter()
        .map(|row| {
            (
                row.row_uuid(),
                row.cell(table, "title")
                    .expect("test row should carry title"),
            )
        })
        .collect()
}

fn historical_titles_via_full_scan(
    node: &mut NodeState<RocksDbStorage>,
    table: &TableSchema,
    position: GlobalTime,
) -> BTreeMap<RowUuid, Value> {
    let table_id = node
        .physical_table_id_for_schema(node.catalogue.current_schema_version_id, &table.name)
        .expect("physical table id");
    let history_source = node
        .physical_history_source_graph(node.catalogue.current_schema_version_id, &table.name)
        .expect("physical history source");
    let deltas = node
        .database
        .query_graph(historical_current_graph_full_scan(
            table,
            table_id,
            position,
            history_source,
        ))
        .expect("full-scan historical graph");
    let rows = node
        .materialize_inline_current_query_rows(table, deltas)
        .expect("materialize full-scan historical graph");
    current_titles(table, rows)
}

fn delete_global(
    node: &mut NodeState<RocksDbStorage>,
    table: &str,
    row_uuid: RowUuid,
    now_ms: u64,
    global_time: u64,
) -> TxId {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new(table, row_uuid, now_ms)
                .made_by(AuthorSubject::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Deleted),
        )
        .expect("delete row");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(global_time)),
        Some(DurabilityTier::Global),
    )
    .expect("accept delete");
    tx_id
}

fn author(byte: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([byte; 16])
}

fn commit_issue(
    node: &mut NodeState<RocksDbStorage>,
    idx: usize,
    state: &str,
    assignee: AuthorSubject,
) {
    node.commit_mergeable_unit_settled(
        MergeableCommit::new("issues", row(idx), 1_000 + idx as u64)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String(format!("issue-{idx}"))),
                ("state".to_owned(), Value::String(state.to_owned())),
                ("assignee".to_owned(), Value::Uuid(assignee.test_uuid())),
                ("priority".to_owned(), Value::U64(idx as u64)),
            ])),
    )
    .expect("commit issue");
}

fn commit_signed_metric(
    node: &mut NodeState<RocksDbStorage>,
    idx: usize,
    bucket: &str,
    score: i64,
) {
    node.commit_mergeable_unit_settled(
        MergeableCommit::new("metrics", row(idx), 1_000 + idx as u64)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([
                ("bucket".to_owned(), Value::String(bucket.to_owned())),
                ("score".to_owned(), Value::I64(score)),
            ])),
    )
    .expect("commit signed metric");
}

fn commit_global_issue(
    node: &mut NodeState<RocksDbStorage>,
    idx: usize,
    state: &str,
    assignee: AuthorSubject,
    seq: u64,
) -> TxId {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("issues", row(idx), 1_000 + idx as u64)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String(format!("issue-{idx}"))),
                    ("state".to_owned(), Value::String(state.to_owned())),
                    ("assignee".to_owned(), Value::Uuid(assignee.test_uuid())),
                    ("priority".to_owned(), Value::U64(idx as u64)),
                ])),
        )
        .expect("commit issue");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(seq)),
        Some(DurabilityTier::Global),
    )
    .expect("accept issue");
    tx_id
}

fn commit_member(
    node: &mut NodeState<RocksDbStorage>,
    idx: usize,
    issue: RowUuid,
    user: AuthorSubject,
) {
    node.commit_mergeable_unit_settled(
        MergeableCommit::new("issue_members", row(10_000 + idx), 10_000 + idx as u64)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([
                ("issue".to_owned(), Value::Uuid(issue.0)),
                ("user".to_owned(), Value::Uuid(user.test_uuid())),
            ])),
    )
    .expect("commit member");
}

fn commit_global_user(
    node: &mut NodeState<RocksDbStorage>,
    user: AuthorSubject,
    name: &str,
    seq: u64,
) {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("users", RowUuid(user.test_uuid()), 2_000 + seq)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([(
                    "name".to_owned(),
                    Value::String(name.to_owned()),
                )])),
        )
        .expect("commit user");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(seq)),
        Some(DurabilityTier::Global),
    )
    .expect("accept user");
}

fn commit_global_member(
    node: &mut NodeState<RocksDbStorage>,
    idx: usize,
    issue: RowUuid,
    user: AuthorSubject,
    seq: u64,
) {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("issue_members", row(10_000 + idx), 3_000 + seq)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([
                    ("issue".to_owned(), Value::Uuid(issue.0)),
                    ("user".to_owned(), Value::Uuid(user.test_uuid())),
                ])),
        )
        .expect("commit member");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(seq)),
        Some(DurabilityTier::Global),
    )
    .expect("accept member");
}

/// A branch read renders a frozen root together with an overlay relation
/// through Groove's structured terminal, while its internal relation fact
/// retains the mixed canonical provenance needed for future deltas.
///
/// alice --accept issue--> core --freeze branch base--> branch view
/// branch --write user----► branch view --array correlation--> issue.assigneeRows
///
/// The public array payload is deliberately not assembled from
/// `RelationSnapshot::edges`: structured app rows are its sole owner. The
/// separate discriminator assertion pins the internal relation terminal so
/// a base root and overlay target cannot silently lose their correlation
/// witness while still returning an empty array.
fn recursive_shape(schema: &RuntimeSchema) -> ValidatedQuery {
    Query::from("resources")
        .reachable_via(
            "resourceAccess",
            "resource",
            "team",
            param("team"),
            "teamTeamMemberships",
            "member",
            "parent",
            [eq(col("onlyAdmins"), lit(false))],
        )
        .validate_runtime(schema)
        .unwrap()
}

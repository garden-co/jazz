//! Query-evaluation tests that exercise several pipeline stages together.

use super::*;
use crate::legacy_test_future::{
    FutureResolveExt as _, OptionFutureExt as _, ResultFutureExt as _, SettledNodeTestExt as _,
};

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

use crate::ids::{AuthorId, BranchId, NodeUuid, RowUuid};
use crate::node::query_engine::{CoverageScope, FieldRequirement, ProgramFactOutput};
use crate::node::{MergeableCommit, NodeState};
use crate::peer::PeerState;
use crate::protocol::{
    CurrentWriteSchema, MigrationLens, ReadViewSourceSpec, ReadViewSpec, RealRowMemberEntry,
    RegisterShapeOptions, RelationEdgeEntry, ResultRowLayer, RowVersionRefEntry, SchemaVersion,
    ShapeAst, Subscribe, SyncMessage, TableLens,
};
use crate::query::{
    Aggregate, ArraySubquery, JoinSourceLookup, OrderDirection, PolicyBranch, Query, claim, col,
    contains, eq, gt, in_list, lit, lte, param, table,
};
use crate::schema::{JazzSchema, Policy, TableSchema};

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
        | GraphBuilder::Aggregate { input, .. } => {
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
        | GraphBuilder::Aggregate { input, .. } => {
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
    identity: AuthorId,
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
    JazzSchema::new([
        TableSchema::new(
            "issues",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("state", ColumnType::String),
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("priority", ColumnType::U64),
            ],
        )
        .with_reference("assignee", "users"),
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "issue_members",
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
            ],
        )
        .with_reference("issue", "issues")
        .with_reference("user", "users"),
    ])
}

fn signed_metric_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "metrics",
        [
            ColumnSchema::new("bucket", ColumnType::String),
            ColumnSchema::new("score", ColumnType::I64),
        ],
    )])
}

fn owner_policy_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "issues",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("assignee", ColumnType::Uuid),
            ColumnSchema::new("requiresAdmin", ColumnType::Bool),
        ],
    )
    .with_read_policy(Query::from("issues").filter(eq(col("assignee"), claim("sub"))))])
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
    let base = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let evolved_todos = TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    );
    let evolved_payload = SchemaVersion::new(JazzSchema::new([evolved_todos.clone()]));
    let (dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xe1; 16]), base.clone());
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(SchemaLineagePublication::new(
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
            ),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )),
    })
    .unwrap();
    node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
    JazzSchema::new([
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new("resources", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "teamSeeds",
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("kind", ColumnType::String),
            ],
        )
        .with_reference("team", "teams"),
        TableSchema::new(
            "resourceAccess",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("resource", "resources")
        .with_reference("team", "teams"),
        TableSchema::new(
            "teamTeamMemberships",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("onlyAdmins", ColumnType::Bool),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
    ])
}

fn open_recursive_node() -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    open_node_with_uuid(NodeUuid::from_bytes([9; 16]), recursive_schema())
}

fn missing_session_seed_policy_schema() -> JazzSchema {
    let mut policy = Query::from("resources").reachable_via(
        "resourceAccess",
        "resource",
        "team",
        lit("seeded-by-session"),
        "teamMemberships",
        "member",
        "parent",
        [],
    );
    policy.reachable[0].seed = Some(crate::query::ReachableSeed {
        table: "teamSeeds".to_owned(),
        user_column: Some("user".to_owned()),
        user_claim: Some("session_id".to_owned()),
        team_column: "team".to_owned(),
        filters: Vec::new(),
    });
    JazzSchema::new([
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new("resources", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(policy),
        TableSchema::new(
            "teamSeeds",
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
            ],
        )
        .with_reference("team", "teams"),
        TableSchema::new(
            "resourceAccess",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("resource", "resources")
        .with_reference("team", "teams"),
        TableSchema::new(
            "teamMemberships",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
    ])
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
    global_seq: u64,
) -> TxId {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new(table, row_uuid, now_ms)
                .made_by(AuthorId::SYSTEM)
                .cells(cells),
        )
        .expect("commit row");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(global_seq)),
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
    position: GlobalSeq,
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
    global_seq: u64,
) -> TxId {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new(table, row_uuid, now_ms)
                .made_by(AuthorId::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Deleted),
        )
        .expect("delete row");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(global_seq)),
        Some(DurabilityTier::Global),
    )
    .expect("accept delete");
    tx_id
}

fn author(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}

fn commit_issue(node: &mut NodeState<RocksDbStorage>, idx: usize, state: &str, assignee: AuthorId) {
    node.commit_mergeable_unit_settled(
        MergeableCommit::new("issues", row(idx), 1_000 + idx as u64)
            .made_by(AuthorId::SYSTEM)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String(format!("issue-{idx}"))),
                ("state".to_owned(), Value::String(state.to_owned())),
                ("assignee".to_owned(), Value::Uuid(assignee.0)),
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
            .made_by(AuthorId::SYSTEM)
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
    assignee: AuthorId,
    seq: u64,
) -> TxId {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("issues", row(idx), 1_000 + idx as u64)
                .made_by(AuthorId::SYSTEM)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String(format!("issue-{idx}"))),
                    ("state".to_owned(), Value::String(state.to_owned())),
                    ("assignee".to_owned(), Value::Uuid(assignee.0)),
                    ("priority".to_owned(), Value::U64(idx as u64)),
                ])),
        )
        .expect("commit issue");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(seq)),
        Some(DurabilityTier::Global),
    )
    .expect("accept issue");
    tx_id
}

fn commit_member(node: &mut NodeState<RocksDbStorage>, idx: usize, issue: RowUuid, user: AuthorId) {
    node.commit_mergeable_unit_settled(
        MergeableCommit::new("issue_members", row(10_000 + idx), 10_000 + idx as u64)
            .made_by(AuthorId::SYSTEM)
            .cells(BTreeMap::from([
                ("issue".to_owned(), Value::Uuid(issue.0)),
                ("user".to_owned(), Value::Uuid(user.0)),
            ])),
    )
    .expect("commit member");
}

fn commit_global_user(node: &mut NodeState<RocksDbStorage>, user: AuthorId, name: &str, seq: u64) {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("users", RowUuid(user.0), 2_000 + seq)
                .made_by(AuthorId::SYSTEM)
                .cells(BTreeMap::from([(
                    "name".to_owned(),
                    Value::String(name.to_owned()),
                )])),
        )
        .expect("commit user");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(seq)),
        Some(DurabilityTier::Global),
    )
    .expect("accept user");
}

fn commit_global_member(
    node: &mut NodeState<RocksDbStorage>,
    idx: usize,
    issue: RowUuid,
    user: AuthorId,
    seq: u64,
) {
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("issue_members", row(10_000 + idx), 3_000 + seq)
                .made_by(AuthorId::SYSTEM)
                .cells(BTreeMap::from([
                    ("issue".to_owned(), Value::Uuid(issue.0)),
                    ("user".to_owned(), Value::Uuid(user.0)),
                ])),
        )
        .expect("commit member");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(seq)),
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
fn recursive_shape(schema: &JazzSchema) -> ValidatedQuery {
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
        .validate(schema)
        .unwrap()
}
